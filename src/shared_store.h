#ifndef PG_REDIS_SHARED_STORE_H
#define PG_REDIS_SHARED_STORE_H

#include "postgres.h"

#include "types.h"

/*
 * Shared-keyspace CRUD using a scratch-copy contract.
 *
 * The shared HTAB stores PgRedisSharedEntry rows whose variable-size
 * payloads (string TLV, hash field map, list node chain) live in the
 * shared DSA segment. To preserve the existing command-function pattern
 * (call lookup → mutate the returned entry → call mark_dirty), each
 * `shared_store_*` routine materializes the shared row into a per-backend
 * palloc'd PgRedisEntry "scratch" under the partition lock, then releases
 * the lock. The caller mutates the scratch. On mark_dirty, the scratch is
 * written back to the shared HTAB under exclusive lock, freeing prior DSA
 * payloads as needed.
 *
 * This requires two lock acquisitions per write (lookup/upsert + writeback)
 * but avoids holding a lock across the command body, which is structurally
 * safer and matches PG's locking idioms.
 */

extern PgRedisEntry *pg_redis_shared_store_lookup(const char *key,
												  bool *expired);
extern PgRedisEntry *pg_redis_shared_store_lookup_raw(const char *key);
extern PgRedisEntry *pg_redis_shared_store_upsert(const char *key,
												  bool *is_new);
extern bool pg_redis_shared_store_remove(const char *key);

/* Write the scratch entry back to the shared HTAB. Called from
 * pg_redis_mark_dirty when storage_mode='shared'. Returns true if the
 * writeback succeeded; raises ERROR on DSA OOM. */
extern void pg_redis_shared_store_writeback(PgRedisEntry *scratch);

/* Free all shared-state payloads (DSA chunks) for a key. Called from
 * shared_store_remove and from FLUSHALL. */
extern void pg_redis_shared_entry_release_payloads(PgRedisSharedEntry *se);

/* FLUSHALL helper for shared mode. Iterates every entry in the shared HTAB,
 * frees its DSA payloads, and removes the entry. Caller MUST hold every
 * partition LWLock exclusively. Returns the number of entries removed. */
extern int	pg_redis_shared_store_reset_all(void);

#endif							/* PG_REDIS_SHARED_STORE_H */
