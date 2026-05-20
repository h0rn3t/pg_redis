#ifndef PG_REDIS_HASH_VALUE_H
#define PG_REDIS_HASH_VALUE_H

#include "postgres.h"
#include "types.h"

/* Create a hash whose HTAB and per-field values are owned by `mcxt`. Use this
 * variant from materialize_scratch with CurrentMemoryContext so a shared-mode
 * scratch hash dies with the SQL statement. */
extern PgRedisHash *pg_redis_hash_create_in(MemoryContext mcxt);

/* Backwards-compatible: create a hash in PgRedisMemoryContext (long-lived). */
extern PgRedisHash *pg_redis_hash_create(void);
extern void pg_redis_hash_free(PgRedisHash *h);

/* Returns true if a new field was inserted, false if an existing field was
 * overwritten. Value is copied into the hash's owning context. */
extern bool pg_redis_hash_set(PgRedisHash *h, const char *field,
							  const char *value, Size value_len);

/* Returns NULL if missing. Returned pointer is valid until next mutation of
 * this field (or context reset). */
extern const char *pg_redis_hash_get(PgRedisHash *h, const char *field,
									 Size *out_len);

extern bool pg_redis_hash_exists(PgRedisHash *h, const char *field);
extern bool pg_redis_hash_del(PgRedisHash *h, const char *field);

extern int64 pg_redis_hash_field_count(const PgRedisHash *h);
extern Size pg_redis_hash_memory(const PgRedisHash *h);

#endif							/* PG_REDIS_HASH_VALUE_H */
