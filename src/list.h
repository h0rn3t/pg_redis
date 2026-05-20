#ifndef PG_REDIS_LIST_H
#define PG_REDIS_LIST_H

#include "postgres.h"
#include "types.h"

/* Create a list whose nodes and value buffers live in `mcxt`. */
extern PgRedisList *pg_redis_list_create_in(MemoryContext mcxt);

/* Backwards-compatible: create a list in PgRedisMemoryContext (long-lived). */
extern PgRedisList *pg_redis_list_create(void);
extern void pg_redis_list_free(PgRedisList *list);

extern int64 pg_redis_list_lpush(PgRedisList *list, const char *value, Size len);
extern int64 pg_redis_list_rpush(PgRedisList *list, const char *value, Size len);

/* Pop functions return a freshly palloc'd copy in CurrentMemoryContext so the
 * buffer dies with the SQL statement that called LPOP/RPOP. *out_len is set on
 * success. Returns NULL when the list is empty. */
extern char *pg_redis_list_lpop(PgRedisList *list, Size *out_len);
extern char *pg_redis_list_rpop(PgRedisList *list, Size *out_len);

extern int64 pg_redis_list_length(const PgRedisList *list);
extern Size pg_redis_list_memory(const PgRedisList *list);

#endif							/* PG_REDIS_LIST_H */
