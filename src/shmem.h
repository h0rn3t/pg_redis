#ifndef PG_REDIS_SHMEM_H
#define PG_REDIS_SHMEM_H

#include "postgres.h"
#include "port/atomics.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"
#include "utils/hsearch.h"

/*
 * Shared memory infrastructure.
 *
 * The v1.0/v1.1 keyspace is session-local: every backend keeps its own
 * in-memory keyspace inside its private MemoryContext. Starting in v1.2, when
 * `pg_redis.storage_mode = 'shared'` is set (and `shared_preload_libraries =
 * 'pg_redis'`), a shared HTAB + DSA segment hold the cluster-wide keyspace
 * visible to every backend.
 *
 * The header below is always allocated. The shared HTAB, partition LWLock
 * array, dirty-ring, and DSA are only meaningfully populated when shared mode
 * is enabled (size-zero allocations otherwise stay tiny).
 */

typedef struct PgRedisSharedHeader
{
	/* --- v1.0 snapshot coordination (still used in session mode) --- */
	LWLock	   *snapshot_lock;
	bool		snapshot_in_progress;
	int64		snapshot_id_inflight;

	/* --- v1.2 shared keyspace + async-table additions --- */

	/* Brief, non-SPI critical sections only (e.g. publishing the DSA handle
	 * after dsa_create). MUST NOT be held across any SPI call — the cold-start
	 * load is coordinated by the `loaded` atomic state machine below, not by
	 * this lock. */
	LWLock	   *startup_lock;

	/* Cold-start load coordination. 0 = unloaded, 1 = loading (one backend is
	 * running the SPI load with NO pg_redis LWLock held), 2 = loaded. Backends
	 * CAS 0->1 to become the loader; others poll via WaitLatch until 2. */
	pg_atomic_uint32 loaded;

	/* TimestampTz (as raw u64) of the most recent 0->1 transition. The
	 * watchdog lets a backend steal a `loading` state stuck for >30s (the
	 * loader died between writing 1 and 2) by CAS'ing 1->0 and retrying. */
	pg_atomic_uint64 last_load_attempt;

	/* DSA segment backing variable-size payloads. Created at startup hook
	 * (postmaster context); attached on demand by each backend. */
	dsa_handle	dsa;

	/* Tranche id for the partition LWLocks. `partition_locks` points into
	 * a separate shmem allocation sized at `lock_partitions`. */
	int			lock_tranche_id;

	/* BGW signals its own latch here at startup so producers can wake it
	 * on a full ring. NULL until the BGW starts; producers test for NULL. */
	Latch	   *bgw_latch;

	/* Dirty-ring atomic counters. Slots live in a separate shmem alloc
	 * sized at `dirty_ring_size` slots. */
	pg_atomic_uint64 ring_write_head;
	pg_atomic_uint64 ring_read_head;

	/* Serializes ALL dirty-ring drainers — the BGW and every sync_flush
	 * producer — so read_head is advanced by at most one drainer at a time and
	 * slot DRAINING transitions never race. A leaf lock: a sync_flush producer
	 * releases its partition LWLock before acquiring this (Decision 10), so the
	 * two are never held together in the drain path and no ordering cycle
	 * exists. (FLUSHALL's drop-all-pending may acquire it while holding
	 * partition locks; that partition->consumer nesting has no reverse edge.) */
	LWLock	   *ring_consumer_lock;
}			PgRedisSharedHeader;

/* shmem_request_hook entry point — reserves the header, ring, lock array,
 * and HTAB space. Called from _PG_init via the hook in pg_redis.c. */
extern void pg_redis_shmem_request(void);

/* shmem_startup_hook entry point — wires up the structs above. */
extern void pg_redis_shmem_startup(void);

/* Install the startup hook. Called from _PG_init. */
extern void pg_redis_shmem_install_hooks(void);

/* --- v1.0 snapshot coordination, preserved --- */
extern bool pg_redis_shmem_try_begin_snapshot(void);
extern void pg_redis_shmem_end_snapshot(void);

/* --- v1.2 accessors --- */

/* Return the global shared header, or NULL when shmem was not initialized
 * (e.g. extension not in shared_preload_libraries). Callers MUST handle NULL
 * gracefully — session mode still works without shared mem. */
extern PgRedisSharedHeader *pg_redis_shmem_header(void);

/* Return the shared HTAB pointer (keyed by text, entries PgRedisSharedEntry).
 * NULL when shared mode is not initialized. */
extern HTAB *pg_redis_shmem_keyspace(void);

/* Eagerly create (first backend) or attach (subsequent backends) the shared
 * DSA segment for this backend. MUST be called once per backend before any
 * partition LWLock is taken (every SQL entry point routes through
 * ensure_loaded(); the BGW calls it at startup). Idempotent. No-op in session
 * mode (shmem not initialized). */
extern void pg_redis_shmem_attach_dsa(void);

/* Pure accessor: return the calling backend's cached `dsa_area *`, or NULL if
 * pg_redis_shmem_attach_dsa() has not run yet. Never creates or attaches. */
extern dsa_area *pg_redis_shmem_dsa(void);

/* Return the LWLock for `hash_any(key, len) % lock_partitions`. */
extern LWLock *pg_redis_partition_lock(const char *key, Size keylen);

/*
 * Invariant guard: no pg_redis code path may run SPI, a transaction-state
 * mutation, dsa_create, blocking I/O, or snapshot-requiring catalog access
 * while holding ANY pg_redis LWLock (partition, snapshot, startup, or
 * ring-consumer lock). Call this (debug-only; a no-op in non-assert builds) at
 * the entry of every SPI-bearing path to catch regressions early.
 */
extern void pg_redis_assert_no_pg_redis_lwlock_held(void);

/* Helper: set or clear the BGW latch pointer in the header. Called by the
 * BGW main loop at startup/shutdown. */
extern void pg_redis_shmem_set_bgw_latch(Latch *latch);
extern void pg_redis_shmem_wake_bgw(void);

/* Acquire / release every partition LWLock exclusively. Used by FLUSHALL in
 * shared mode to take an exclusive snapshot of the whole keyspace while it
 * resets state. Idempotent: when locks are not configured (session mode or
 * shmem not initialized) this is a no-op. */
extern void pg_redis_shmem_acquire_all_partition_locks_exclusive(void);
extern void pg_redis_shmem_release_all_partition_locks(void);

/* DSA wrappers — allocate / free a chunk in the shared DSA segment. Returns
 * InvalidDsaPointer (0) on allocation failure (caller raises
 * ERRCODE_OUT_OF_MEMORY). NULL-safe on shared_dsa_pfree. */
extern dsa_pointer pg_redis_shared_palloc(Size sz);
extern void pg_redis_shared_pfree(dsa_pointer p);
extern void *pg_redis_shared_addr(dsa_pointer p);

#endif							/* PG_REDIS_SHMEM_H */
