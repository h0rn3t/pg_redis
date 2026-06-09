#include "postgres.h"
#include "miscadmin.h"
#include "access/hash.h"
#include "common/hashfn.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/dsa.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "types.h"
#include "shmem.h"

static PgRedisSharedHeader *PgRedisShared = NULL;
static HTAB *PgRedisSharedKeyspace = NULL;
static LWLockPadded *PgRedisPartitionLocks = NULL;

/* Per-backend cached attach to the shared DSA segment. */
static dsa_area *cached_dsa = NULL;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* Forward decls for the dirty-ring slot array allocation — the ring itself
 * lives in src/dirty_ring.c (introduced in Group 4). We only reserve space
 * here so that startup_hook can wire the pointer. */
extern Size pg_redis_dirty_ring_size_bytes(int slot_count);
extern void pg_redis_dirty_ring_shmem_attach(void *slot_area, int slot_count);
extern void pg_redis_dirty_ring_shmem_init(void *slot_area, int slot_count);

/* Size of one keyspace HTAB entry. The entry struct lives in types.h. */
#define PG_REDIS_SHARED_ENTRY_SIZE sizeof(PgRedisSharedEntry)

/* Compute the expected number of HTAB entries. We size for `shared_max_memory
 * / average_entry_size`. The HTAB itself is small (entry headers); the bulk
 * of memory consumption is in the DSA segment for variable-size payloads.
 * 256MB / ~2KB per durable entry → ~130k entries default. Pick a healthy
 * upper bound and let HTAB grow within shmem if more slots are needed (PG18
 * shared HTAB does support growth up to the requested `max_size`). */
/* Average bytes per HTAB entry used for sizing. The HTAB itself holds only
 * fixed-size entry headers; the bulk of memory is the DSA payload segment. */
#define PG_REDIS_BYTES_PER_ENTRY 2048

static Size
estimate_max_entries(void)
{
	Size		est;

	/*
	 * Compute in Size (64-bit) arithmetic. The previous `int * 1024 * 1024`
	 * expression overflowed signed int once pg_redis.shared_max_memory_mb
	 * reached 2048, producing a negative value that silently clamped to the
	 * 1024 floor: the cluster then ran with a 1024-entry HTAB regardless of
	 * the configured size and PANIC'd the first time hash_search(HASH_ENTER)
	 * tried to grow the table past that bound. Casting the MB value to Size
	 * before the multiply keeps the computation exact up to the GUC ceiling.
	 */
	est = ((Size) pg_redis_shared_max_memory_mb) * 1024 * 1024 / PG_REDIS_BYTES_PER_ENTRY;
	if (est < 1024)
		est = 1024;
	return est;
}

void
pg_redis_shmem_request(void)
{
	Size		size = 0;
	int			ring_slots = pg_redis_dirty_ring_size;
	int			partitions = pg_redis_lock_partitions;
	Size		max_entries = estimate_max_entries();

	/* 1. Header. */
	size = add_size(size, MAXALIGN(sizeof(PgRedisSharedHeader)));

	/* 2. Dirty-ring slot area. Computed by the ring module so the slot
	 *    layout stays encapsulated. */
	size = add_size(size, MAXALIGN(pg_redis_dirty_ring_size_bytes(ring_slots)));

	/* 3. Shared HTAB for the keyspace (header + entries). */
	size = add_size(size, hash_estimate_size(max_entries, PG_REDIS_SHARED_ENTRY_SIZE));

	RequestAddinShmemSpace(size);

	/* Two LWLock tranches:
	 *   - "pg_redis" — the legacy snapshot lock (1.0/1.1 carryover), the
	 *     startup/DSA-publish lock, and the dirty-ring consumer lock (1.2).
	 *   - "pg_redis_partitions" — `lock_partitions` partition locks for
	 *     the shared keyspace.
	 * The named-tranche mechanism handles both naming and allocation. */
	RequestNamedLWLockTranche("pg_redis", 3);
	RequestNamedLWLockTranche("pg_redis_partitions", partitions);
}

void
pg_redis_shmem_startup(void)
{
	bool		found;
	HASHCTL		ctl;
	int			ring_slots = pg_redis_dirty_ring_size;
	Size		max_entries = estimate_max_entries();
	void	   *ring_area;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	/* 1. Header. */
	PgRedisShared = (PgRedisSharedHeader *)
		ShmemInitStruct("pg_redis shared header",
						sizeof(PgRedisSharedHeader),
						&found);
	if (!found)
	{
		LWLockPadded *legacy = GetNamedLWLockTranche("pg_redis");

		PgRedisShared->snapshot_lock = &legacy[0].lock;
		PgRedisShared->snapshot_in_progress = false;
		PgRedisShared->snapshot_id_inflight = 0;

		PgRedisShared->startup_lock = &legacy[1].lock;
		pg_atomic_init_u32(&PgRedisShared->loaded, 0);
		pg_atomic_init_u64(&PgRedisShared->last_load_attempt, 0);

		PgRedisShared->ring_consumer_lock = &legacy[2].lock;

		PgRedisShared->dsa = DSM_HANDLE_INVALID;
		PgRedisShared->lock_tranche_id = 0;	/* filled below */
		PgRedisShared->bgw_latch = NULL;

		pg_atomic_init_u64(&PgRedisShared->ring_write_head, 0);
		pg_atomic_init_u64(&PgRedisShared->ring_read_head, 0);
	}

	/* 2. Dirty-ring slot area. Attach (set per-backend statics) on every
	 *    invocation; first-time-init runs only on !found. */
	{
		bool		ring_found;

		ring_area = ShmemInitStruct("pg_redis dirty ring",
									pg_redis_dirty_ring_size_bytes(ring_slots),
									&ring_found);
		if (ring_area != NULL)
		{
			if (!ring_found)
				pg_redis_dirty_ring_shmem_init(ring_area, ring_slots);
			pg_redis_dirty_ring_shmem_attach(ring_area, ring_slots);
		}
	}

	/* 3. Partition LWLocks. */
	PgRedisPartitionLocks = GetNamedLWLockTranche("pg_redis_partitions");
	if (!found && PgRedisShared != NULL)
	{
		/* Lock tranche id is implicit in the named tranche; the locks are
		 * already initialized by RequestNamedLWLockTranche. We just need a
		 * stable identifier to publish — use the first lock's tranche id. */
		PgRedisShared->lock_tranche_id =
			PgRedisPartitionLocks[0].lock.tranche;
	}

	/* 4. Shared keyspace HTAB. Serialization is provided SOLELY by the
	 *    external partition LWLocks (pg_redis_partition_lock); HASH_PARTITION
	 *    is deliberately NOT used. dynahash's internal partitioning hashes the
	 *    key with string_hash, which does not match our external partition
	 *    function (hash_bytes) — combining the two let dynahash's internal
	 *    partition diverge from the lock the caller actually held, corrupting
	 *    the HTAB under concurrency (bug #6). Keys are NUL-terminated cstrings
	 *    (text args are truncated at the first NUL), so HASH_STRINGS is the
	 *    correct, call-site-safe choice; HASH_BLOBS would force every key to a
	 *    zero-padded fixed-size buffer and a 1025-byte memcmp on the hot path.
	 *    init = max so the table never resizes (and never hits dynahash's
	 *    single internal metadata lock) after construction. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(((PgRedisSharedEntry *) 0)->key);
	ctl.entrysize = PG_REDIS_SHARED_ENTRY_SIZE;
	PgRedisSharedKeyspace =
		ShmemInitHash("pg_redis shared keyspace",
					  max_entries,			/* init */
					  max_entries,			/* max */
					  &ctl,
					  HASH_ELEM | HASH_STRINGS);

	if (!found)
		elog(LOG, "pg_redis: shared keyspace sized for " UINT64_FORMAT
			 " entries (shared_max_memory=%d MB, ~%d bytes/entry)",
			 (uint64) max_entries, pg_redis_shared_max_memory_mb,
			 PG_REDIS_BYTES_PER_ENTRY);

	LWLockRelease(AddinShmemInitLock);

	/* 5. DSA segment for variable-size payloads. Created lazily — the very
	 *    first backend in shared mode does dsa_create() and publishes the
	 *    handle into PgRedisShared->dsa. Subsequent backends dsa_attach.
	 *    We can't create the DSA from inside the startup hook because DSA
	 *    requires a working backend (PGPROC slot). Handled in pg_redis_shmem_dsa(). */
}

void
pg_redis_shmem_install_hooks(void)
{
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pg_redis_shmem_startup;
}

/* --- Accessors --- */

PgRedisSharedHeader *
pg_redis_shmem_header(void)
{
	return PgRedisShared;
}

HTAB *
pg_redis_shmem_keyspace(void)
{
	return PgRedisSharedKeyspace;
}

/*
 * Eagerly create (first backend) or attach (subsequent backends) the shared
 * DSA segment for this backend. Must be called once per backend BEFORE any
 * partition LWLock is taken — that is why every SQL entry point routes through
 * ensure_loaded(), and the BGW calls this at startup. Idempotent: returns
 * immediately once cached_dsa is set.
 *
 * The previous design created/attached the DSA lazily from inside
 * pg_redis_shared_palloc(), whose first call is typically made while a
 * partition LWLock is already held. That meant acquiring startup_lock (and
 * dsa_create's internal DSM locks) under a partition lock — a four-lock chain
 * across two contended tranches (bug #5). Doing the attach eagerly, before any
 * partition lock, removes that hazard: pg_redis_shmem_dsa() is now a pure
 * accessor.
 *
 * startup_lock is held only briefly to serialize create-vs-attach (so a second
 * backend waits for the creator to publish the handle rather than creating a
 * second segment). NO SPI or other blocking work runs under it.
 */
void
pg_redis_shmem_attach_dsa(void)
{
	MemoryContext old_ctx;
	ResourceOwner old_owner;

	if (cached_dsa != NULL)
		return;					/* already attached in this backend */
	if (PgRedisShared == NULL)
		return;					/* shmem not initialized — session mode */

	/* dsa_create/dsa_attach palloc the backend-local dsa_area struct in
	 * CurrentMemoryContext and bind it to CurrentResourceOwner. During SQL
	 * function execution both are transient and get torn down at
	 * end-of-statement, leaving cached_dsa dangling on the next call. Pin the
	 * struct's lifetime to this backend by allocating in TopMemoryContext under
	 * a NULL resource owner. */
	old_ctx = MemoryContextSwitchTo(TopMemoryContext);
	old_owner = CurrentResourceOwner;
	CurrentResourceOwner = NULL;

	/* dsa_create / dsa_attach / dsa_pin / dsa_pin_mapping can ereport (OOM on
	 * mmap, dsm handle invalidated). Wrap in PG_TRY so a longjmp doesn't leave
	 * CurrentResourceOwner=NULL or startup_lock held. */
	LWLockAcquire(PgRedisShared->startup_lock, LW_EXCLUSIVE);

	PG_TRY();
	{
		if (PgRedisShared->dsa == DSM_HANDLE_INVALID)
		{
			dsa_area   *area = dsa_create(PgRedisShared->lock_tranche_id);

			/* DSA segments are session-pinned by default. Pin so the segment
			 * survives the creating backend and stays alive cluster-wide. */
			dsa_pin(area);
			dsa_pin_mapping(area);
			PgRedisShared->dsa = dsa_get_handle(area);
			cached_dsa = area;
			elog(LOG, "pg_redis: DSA created, handle=%u",
				 (unsigned) PgRedisShared->dsa);
		}
		else
		{
			cached_dsa = dsa_attach(PgRedisShared->dsa);
			dsa_pin_mapping(cached_dsa);
		}
	}
	PG_CATCH();
	{
		LWLockRelease(PgRedisShared->startup_lock);
		CurrentResourceOwner = old_owner;
		MemoryContextSwitchTo(old_ctx);
		PG_RE_THROW();
	}
	PG_END_TRY();

	LWLockRelease(PgRedisShared->startup_lock);

	CurrentResourceOwner = old_owner;
	MemoryContextSwitchTo(old_ctx);
}

/*
 * Pure accessor: return this backend's cached dsa_area, or NULL if the segment
 * has not been attached yet (session mode, or a code path that failed to call
 * pg_redis_shmem_attach_dsa() first — which the wrappers below Assert against).
 * This function NEVER creates or attaches; that is pg_redis_shmem_attach_dsa()'s
 * job, run before any partition lock is taken.
 */
dsa_area *
pg_redis_shmem_dsa(void)
{
	return cached_dsa;
}

/*
 * Debug-only invariant check: assert that the calling backend holds NONE of
 * the pg_redis LWLocks (every partition lock, plus the snapshot, startup, and
 * ring-consumer locks). Used to guard the no-SPI-/-no-blocking-work-under-a-
 * pg_redis-LWLock contract at the entry of SPI-bearing paths (cold-start load,
 * sync_flush drain, FLUSHALL TRUNCATE, ring collect/release). Compiles to
 * nothing in non-assert builds.
 */
void
pg_redis_assert_no_pg_redis_lwlock_held(void)
{
#ifdef USE_ASSERT_CHECKING
	int			i;

	if (PgRedisShared != NULL)
	{
		Assert(!LWLockHeldByMe(PgRedisShared->snapshot_lock));
		Assert(!LWLockHeldByMe(PgRedisShared->startup_lock));
		Assert(!LWLockHeldByMe(PgRedisShared->ring_consumer_lock));
	}
	if (PgRedisPartitionLocks != NULL && pg_redis_lock_partitions > 0)
		for (i = 0; i < pg_redis_lock_partitions; i++)
			Assert(!LWLockHeldByMe(&PgRedisPartitionLocks[i].lock));
#endif
}

LWLock *
pg_redis_partition_lock(const char *key, Size keylen)
{
	uint32		h;
	int			idx;

	if (PgRedisPartitionLocks == NULL || pg_redis_lock_partitions <= 0)
		return NULL;

	h = hash_bytes((const unsigned char *) key, (int) keylen);
	idx = h % pg_redis_lock_partitions;
	return &PgRedisPartitionLocks[idx].lock;
}

void
pg_redis_shmem_set_bgw_latch(Latch *latch)
{
	if (PgRedisShared != NULL)
		PgRedisShared->bgw_latch = latch;
}

void
pg_redis_shmem_acquire_all_partition_locks_exclusive(void)
{
	int			i;

	if (PgRedisPartitionLocks == NULL || pg_redis_lock_partitions <= 0)
		return;
	for (i = 0; i < pg_redis_lock_partitions; i++)
		LWLockAcquire(&PgRedisPartitionLocks[i].lock, LW_EXCLUSIVE);
}

void
pg_redis_shmem_release_all_partition_locks(void)
{
	int			i;

	if (PgRedisPartitionLocks == NULL || pg_redis_lock_partitions <= 0)
		return;
	/* Release in reverse acquisition order for hygiene. */
	for (i = pg_redis_lock_partitions - 1; i >= 0; i--)
		LWLockRelease(&PgRedisPartitionLocks[i].lock);
}

void
pg_redis_shmem_wake_bgw(void)
{
	Latch	   *l;

	if (PgRedisShared == NULL)
		return;
	l = PgRedisShared->bgw_latch;
	if (l != NULL)
		SetLatch(l);
}

/* DSA wrappers. The DSA is attached lazily by pg_redis_shmem_dsa(); these
 * wrappers funnel every shared-mode allocation through it so callers can
 * stay ignorant of the underlying API. */

dsa_pointer
pg_redis_shared_palloc(Size sz)
{
	dsa_area   *area = pg_redis_shmem_dsa();

	/* The DSA must have been attached eagerly (pg_redis_shmem_attach_dsa)
	 * before any shared-keyspace allocation. A NULL here means a caller
	 * reached a shared path without first routing through ensure_loaded() /
	 * the BGW attach — catch it loudly in assert builds. */
	Assert(area != NULL);
	if (area == NULL)
		return InvalidDsaPointer;
	return dsa_allocate_extended(area, sz, DSA_ALLOC_NO_OOM | DSA_ALLOC_ZERO);
}

void
pg_redis_shared_pfree(dsa_pointer p)
{
	dsa_area   *area;

	if (p == InvalidDsaPointer)
		return;
	area = pg_redis_shmem_dsa();
	Assert(area != NULL);
	if (area == NULL)
		return;
	dsa_free(area, p);
}

void *
pg_redis_shared_addr(dsa_pointer p)
{
	dsa_area   *area;

	if (p == InvalidDsaPointer)
		return NULL;
	area = pg_redis_shmem_dsa();
	Assert(area != NULL);
	if (area == NULL)
		return NULL;
	return dsa_get_address(area, p);
}

/* --- v1.0 snapshot coordination (preserved) --- */

bool
pg_redis_shmem_try_begin_snapshot(void)
{
	bool		ok;

	if (PgRedisShared == NULL)
		return true;			/* shmem not initialized — allow */

	LWLockAcquire(PgRedisShared->snapshot_lock, LW_EXCLUSIVE);
	if (PgRedisShared->snapshot_in_progress)
		ok = false;
	else
	{
		PgRedisShared->snapshot_in_progress = true;
		ok = true;
	}
	LWLockRelease(PgRedisShared->snapshot_lock);
	return ok;
}

void
pg_redis_shmem_end_snapshot(void)
{
	if (PgRedisShared == NULL)
		return;

	LWLockAcquire(PgRedisShared->snapshot_lock, LW_EXCLUSIVE);
	PgRedisShared->snapshot_in_progress = false;
	LWLockRelease(PgRedisShared->snapshot_lock);
}
