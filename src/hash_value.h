#ifndef PG_REDIS_HASH_VALUE_H
#define PG_REDIS_HASH_VALUE_H

#include "postgres.h"
#include "types.h"

extern PgRedisHash *pg_redis_hash_create(void);
extern void pg_redis_hash_free(PgRedisHash *h);

/* Returns true if a new field was inserted, false if an existing field was
 * overwritten. Value is copied into PgRedisMemoryContext. */
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
