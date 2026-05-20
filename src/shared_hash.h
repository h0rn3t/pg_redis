#ifndef PG_REDIS_SHARED_HASH_H
#define PG_REDIS_SHARED_HASH_H

#include "postgres.h"
#include "utils/dsa.h"

#include "types.h"

/*
 * DSA-backed hash field storage for `storage_mode = 'shared'`.
 *
 * Open-addressing hash table with linear probing. Each per-key hash lives in a
 * single DSA allocation laid out as `PgRedisSharedHashTable` header followed
 * by a contiguous bucket array of `bucket_count` `PgRedisSharedHashBucket`s.
 * Bucket array size is a power of two; lookup index = hash_bytes(field) &
 * (bucket_count - 1). Grow to 2x when (occupied + tombstones) > 0.7 * size;
 * shrink lazily to size/2 when occupied < 0.125 * size and size > 16.
 *
 * All accessors operate on a `dsa_pointer` to the table header stored on
 * `PgRedisSharedEntry.value.hash.table`. The set/del paths take a pointer to
 * that slot because grow/shrink rehashes can swap the table out for a new
 * allocation. Callers are responsible for holding the correct partition
 * LWLock around these calls (LW_EXCLUSIVE for set/del, LW_SHARED for read).
 */

/* Initial bucket count for a freshly allocated table. Must be a power of two. */
#define PG_REDIS_SHARED_HASH_INIT_BUCKETS	16

/* Free the entire table allocation (header + bucket array) and every DSA
 * chunk reachable from OCCUPIED buckets (field_dsa / value_dsa). */
extern void pg_redis_shared_hash_free(dsa_pointer table_dsa);

/* Set (or overwrite) a field. The table dsa_pointer may be replaced if the
 * call grows/lazy-allocates it; the caller's slot is updated through
 * *inout_table_dsa. *was_new is set true if a new field bucket was occupied
 * (i.e. the field was not already present). */
extern void pg_redis_shared_hash_set(dsa_pointer *inout_table_dsa,
									 const char *field, Size fieldlen,
									 const unsigned char *value, Size value_len,
									 bool *was_new);

/* Look up a field. On hit, returns true and writes the value bytes into a
 * freshly palloc'd local buffer in CurrentMemoryContext via *out_value /
 * *out_value_len. Caller must pfree. */
extern bool pg_redis_shared_hash_get(dsa_pointer table_dsa,
									 const char *field, Size fieldlen,
									 unsigned char **out_value, Size *out_value_len);

extern bool pg_redis_shared_hash_exists(dsa_pointer table_dsa,
										const char *field, Size fieldlen);

/* Remove a field. May shrink the table (and update *inout_table_dsa). Returns
 * true if a field was removed. */
extern bool pg_redis_shared_hash_del(dsa_pointer *inout_table_dsa,
									 const char *field, Size fieldlen);

/* Number of OCCUPIED buckets. O(1) — read from the header. */
extern int64 pg_redis_shared_hash_count(dsa_pointer table_dsa);

/* Materialize the table into a per-backend PgRedisHash in CurrentMemoryContext.
 * Used by the scratch-based mutation paths for keys whose entire payload
 * needs a local copy. Read-only paths (HGET/HEXISTS) call _get/_exists
 * directly to avoid this. */
extern struct PgRedisHash *pg_redis_shared_hash_materialize(dsa_pointer table_dsa);

#endif							/* PG_REDIS_SHARED_HASH_H */
