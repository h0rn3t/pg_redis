#ifndef PG_REDIS_SHARED_LIST_H
#define PG_REDIS_SHARED_LIST_H

#include "postgres.h"
#include "utils/dsa.h"

#include "types.h"

/*
 * DSA-backed list storage for `storage_mode = 'shared'`.
 *
 * Doubly-linked list of (ord, value) nodes in the shared DSA segment. The
 * head and tail dsa_pointers live on PgRedisSharedEntry.value.list, along
 * with `min_ord`, `max_ord`, `length`, and `ord_initialized`.
 *
 * Callers must hold the partition LWLock for the parent key when mutating.
 */

typedef struct PgRedisSharedListMeta
{
	dsa_pointer head;
	dsa_pointer tail;
	int64		length;
	int64		min_ord;
	int64		max_ord;
	bool		ord_initialized;
}			PgRedisSharedListMeta;

/* Free every node reachable from `meta->head`. Resets meta to empty. */
extern void pg_redis_shared_list_free(PgRedisSharedListMeta *meta);

/* LPUSH / RPUSH. Returns the new length. Updates meta in place. */
extern int64 pg_redis_shared_list_lpush(PgRedisSharedListMeta *meta,
										const unsigned char *value, Size value_len);
extern int64 pg_redis_shared_list_rpush(PgRedisSharedListMeta *meta,
										const unsigned char *value, Size value_len);

/* LPOP / RPOP. On success, returns true, writes palloc'd value bytes via
 * *out_value and length via *out_len. Also writes the popped ord via
 * *out_ord — needed by the caller to publish an EVENT_LIST_ITEM_DELETE. */
extern bool pg_redis_shared_list_lpop(PgRedisSharedListMeta *meta,
									  unsigned char **out_value, Size *out_len,
									  int64 *out_ord);
extern bool pg_redis_shared_list_rpop(PgRedisSharedListMeta *meta,
									  unsigned char **out_value, Size *out_len,
									  int64 *out_ord);

/* Materialize the entire shared list into a per-backend PgRedisList for the
 * lookup-scratch pattern. */
extern struct PgRedisList *pg_redis_shared_list_materialize(const PgRedisSharedListMeta *meta);

#endif							/* PG_REDIS_SHARED_LIST_H */
