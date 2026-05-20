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
static int
estimate_max_entries(void)
{
	/* Approximate: 2KB average per entry (header + key + value pointer). */
	const int	bytes_per_entry = 2048;
	int			est;

	est = (pg_redis_shared_max_memory_mb * 1024 * 1024) / bytes_per_entry;
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
	int			max_entries = estimate_max_entries();

	/* 1. Header. */
	size = add_size(size, MAXALIGN(sizeof(PgRedisSharedHeader)));

	/* 2. Dirty-ring slot area. Computed by the ring module so the slot
	 *    layout stays encapsulated. */
	size = add_size(size, MAXALIGN(pg_redis_dirty_ring_size_bytes(ring_slots)));

	/* 3. Shared HTAB for the keyspace (header + entries). */
	size = add_size(size, hash_estimate_size(max_entries, PG_REDIS_SHARED_ENTRY_SIZE));

	RequestAddinShmemSpace(size);

	/* Two LWLock tranches:
	 *   - "pg_redis" — the single legacy snapshot lock (1.0/1.1 carryover)
	 *     plus the startup load lock.
	 *   - "pg_redis_partitions" — `lock_partitions` partition locks for
	 *     the shared keyspace.
	 * The named-tranche mechanism handles both naming and allocation. */
	RequestNamedLWLockTranche("pg_redis", 2);
	RequestNamedLWLockTranche("pg_redis_partitions", partitions);
}

void
pg_redis_shmem_startup(void)
{
	bool		found;
	HASHCTL		ctl;
	int			ring_slots = pg_redis_dirty_ring_size;
	int			partitions = pg_redis_lock_partitions;
	int			max_entries = estimate_max_entries();
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

	/* 4. Shared keyspace HTAB. Partitioned to align with our LWLock
	 *    partition scheme so HTAB-internal locking does not contend with
	 *    our explicit partition locks. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(((PgRedisSharedEntry *) 0)->key);
	ctl.entrysize = PG_REDIS_SHARED_ENTRY_SIZE;
	ctl.num_partitions = partitions;
	PgRedisSharedKeyspace =
		ShmemInitHash("pg_redis shared keyspace",
					  max_entries / 4,		/* init */
					  max_entries,			/* max */
					  &ctl,
					  HASH_ELEM | HASH_STRINGS | HASH_PARTITION);

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

dsa_area *
pg_redis_shmem_dsa(void)
{
	MemoryContext old_ctx;
	ResourceOwner old_owner;

	if (cached_dsa != NULL)
		return cached_dsa;
	if (PgRedisShared == NULL)
		return NULL;

	/* dsa_create/dsa_attach palloc the backend-local dsa_area struct in
	 * CurrentMemoryContext and bind it to CurrentResourceOwner. During SQL
	 * function execution both are transient (ExecutorState / per-transaction
	 * owner) and get torn down at end-of-statement, leaving cached_dsa
	 * dangling on the next call. Pin the struct's lifetime to this backend by
	 * allocating in TopMemoryContext under a NULL resource owner. */
	old_ctx = MemoryContextSwitchTo(TopMemoryContext);
	old_owner = CurrentResourceOwner;
	CurrentResourceOwner = NULL;

	/* Lazy create/attach: the first backend to touch DSA in shared mode
	 * creates the segment under the startup_lock; subsequent backends just
	 * attach to the published handle. */
	LWLockAcquire(PgRedisShared->startup_lock, LW_EXCLUSIVE);
	if (PgRedisShared->dsa == DSM_HANDLE_INVALID)
	{
		dsa_area   *area = dsa_create(PgRedisShared->lock_tranche_id);

		/* DSA segments are session-pinned by default. We need them to
		 * survive the creating backend's lifetime — pin so the segment
		 * stays alive for the cluster. */
		dsa_pin(area);
		dsa_pin_mapping(area);
		PgRedisShared->dsa = dsa_get_handle(area);
		cached_dsa = area;
		elog(LOG, "pg_redis: DSA created, handle=%u, area=%p",
			 (unsigned) PgRedisShared->dsa, (void *) area);
	}
	else
	{
		cached_dsa = dsa_attach(PgRedisShared->dsa);
		dsa_pin_mapping(cached_dsa);
		elog(LOG, "pg_redis: DSA attached, handle=%u, area=%p",
			 (unsigned) PgRedisShared->dsa, (void *) cached_dsa);
	}
	LWLockRelease(PgRedisShared->startup_lock);

	CurrentResourceOwner = old_owner;
	MemoryContextSwitchTo(old_ctx);

	return cached_dsa;
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
