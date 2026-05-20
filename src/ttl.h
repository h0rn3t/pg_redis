#ifndef PG_REDIS_TTL_H
#define PG_REDIS_TTL_H

#include "postgres.h"
#include "types.h"

/* Return true if entry has an expire set and is now past expiration. */
extern bool pg_redis_entry_is_expired(const PgRedisEntry *e);

/* Set expire_at = now + seconds. seconds <= 0 means: delete now. Returns
 * true if the key existed and was processed, false if missing. */
extern bool pg_redis_set_expire_seconds(const char *key, int32 seconds);

/* TTL command semantics: -2 (no key), -1 (no expire), else seconds remaining. */
extern int32 pg_redis_ttl_seconds(const char *key);

/* Sweep the in-memory store, removing expired entries. Returns count removed. */
extern int64 pg_redis_ttl_sweep(void);

#endif							/* PG_REDIS_TTL_H */
