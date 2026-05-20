#ifndef PG_REDIS_SHARED_HASH_H
#define PG_REDIS_SHARED_HASH_H

#include "postgres.h"
#include "utils/dsa.h"

#include "types.h"

/*
 * DSA-backed hash field storage for `storage_mode = 'shared'`.
 *
 * The data structure is a singly-linked list of (field_name, value) nodes
 * allocated in the shared DSA segment. This trades O(N) lookup per field
 * for simplicity — pg_redis hashes are typically small and the cost of
 * carrying an HTAB header in DSA per hash is high relative to the small
 * field counts seen in cache workloads.
 *
 * All accessors operate on a `head` dsa_pointer (the chain head stored on
 * `PgRedisSharedEntry.value.hash.fields_head`). Callers are responsible for
 * holding the correct partition LWLock during these calls.
 */

/* Free every field node reachable from `head`. The DSA pointers freed include
 * the per-node field_dsa and value_dsa. */
extern void pg_redis_shared_hash_free(dsa_pointer head);

/* Set (or overwrite) a field. Returns the (possibly updated) chain head via
 * *out_head. Also sets *was_new=true if a new field node was added. */
extern void pg_redis_shared_hash_set(dsa_pointer *inout_head,
									 const char *field, Size fieldlen,
									 const unsigned char *value, Size value_len,
									 bool *was_new);

/* Look up a field. On hit, returns true and writes the value bytes into a
 * freshly palloc'd local buffer in CurrentMemoryContext via *out_value /
 * *out_value_len. Caller must pfree. */
extern bool pg_redis_shared_hash_get(dsa_pointer head,
									 const char *field, Size fieldlen,
									 unsigned char **out_value, Size *out_value_len);

extern bool pg_redis_shared_hash_exists(dsa_pointer head,
										const char *field, Size fieldlen);

/* Remove a field. Returns true if removed (updates *inout_head if the head
 * itself was removed). */
extern bool pg_redis_shared_hash_del(dsa_pointer *inout_head,
									 const char *field, Size fieldlen);

/* Count fields. O(N). */
extern int64 pg_redis_shared_hash_count(dsa_pointer head);

/* Iterator helper for materializing a shared hash into a per-backend
 * PgRedisHash (palloc'd) for the lookup-scratch pattern. */
extern struct PgRedisHash *pg_redis_shared_hash_materialize(dsa_pointer head);

#endif							/* PG_REDIS_SHARED_HASH_H */
