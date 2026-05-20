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

/* Outcome of a fast-path shared-mode read against a hash key. */
typedef enum PgRedisFastHashOutcome
{
	PG_REDIS_FAST_HASH_HIT,			/* field present; value/value_len set */
	PG_REDIS_FAST_HASH_FIELD_MISS,	/* key present, hash type, but no such field */
	PG_REDIS_FAST_HASH_KEY_MISS,	/* key absent — caller should fall through to slow path */
	PG_REDIS_FAST_HASH_KEY_EXPIRED, /* key present but TTL-expired — caller should fall through for lazy eviction */
	PG_REDIS_FAST_HASH_WRONGTYPE	/* key present but not a hash; *out_actual set */
} PgRedisFastHashOutcome;

/*
 * Fast read for HGET/HEXISTS in shared mode. Acquires the partition lock as
 * LW_SHARED, looks up the shared entry, dispatches based on type/TTL/field
 * presence. On HIT, copies the value bytes into CurrentMemoryContext via
 * *out_value / *out_value_len (caller pfrees). For FIELD_MISS / KEY_MISS /
 * KEY_EXPIRED / WRONGTYPE, out_value is NULL and out_value_len is 0.
 *
 * Pass want_value=false to skip the value copy (HEXISTS path); HIT/FIELD_MISS
 * still reflect field presence.
 */
extern PgRedisFastHashOutcome
pg_redis_shared_hash_lookup_field(const char *key, Size keylen,
								  const char *field, Size fieldlen,
								  bool want_value,
								  unsigned char **out_value, Size *out_value_len,
								  PgRedisValueType *out_actual);

/* Outcome of a fast-path mutation against a hash key. */
typedef enum PgRedisFastHashMutateOutcome
{
	PG_REDIS_FAST_HASH_MUTATE_OK_NEW,	/* HSET created a new field */
	PG_REDIS_FAST_HASH_MUTATE_OK_OVERWRITE, /* HSET overwrote an existing field */
	PG_REDIS_FAST_HASH_MUTATE_OK_DELETED,	/* HDEL removed an existing field */
	PG_REDIS_FAST_HASH_MUTATE_NOOP,			/* HDEL on a missing field or key */
	PG_REDIS_FAST_HASH_MUTATE_KEY_EXPIRED,	/* TTL expired — caller falls back for eviction */
	PG_REDIS_FAST_HASH_MUTATE_WRONGTYPE		/* existing key isn't a hash; *out_actual set */
} PgRedisFastHashMutateOutcome;

/* Atomic HSET in shared mode. Holds LW_EXCLUSIVE for the duration, mutates
 * the DSA hash table directly (no scratch materialize), and publishes the
 * matching persistence events when async_table mode is active. */
extern PgRedisFastHashMutateOutcome
pg_redis_shared_hash_set_atomic(const char *key, Size keylen,
								const char *field, Size fieldlen,
								const unsigned char *value, Size value_len,
								PgRedisValueType *out_actual);

/* Atomic HDEL in shared mode. Symmetric to set_atomic. Returns
 * MUTATE_OK_DELETED on hit, MUTATE_NOOP on miss. */
extern PgRedisFastHashMutateOutcome
pg_redis_shared_hash_del_atomic(const char *key, Size keylen,
								const char *field, Size fieldlen,
								PgRedisValueType *out_actual);

#endif							/* PG_REDIS_SHARED_STORE_H */
