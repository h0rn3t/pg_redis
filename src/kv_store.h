#ifndef PG_REDIS_KV_STORE_H
#define PG_REDIS_KV_STORE_H

#include "postgres.h"
#include "types.h"

extern void pg_redis_store_init(void);
extern void pg_redis_store_reset(void);
extern HTAB *pg_redis_store_htab(void);

/* Look up an entry, applying lazy TTL expiration. Returns NULL if missing or
 * just-expired. If found_expired is non-NULL, set to true when caller-visible
 * effect was an expiration removal. */
extern PgRedisEntry *pg_redis_store_lookup(const char *key, bool *found_expired);

/* Same as lookup but ignores TTL. Used for internal management. */
extern PgRedisEntry *pg_redis_store_lookup_raw(const char *key);

/* Insert-or-update path. Returns the entry (existing or newly inserted)
 * with type unset on creation (caller must set). is_new is set accordingly. */
extern PgRedisEntry *pg_redis_store_upsert(const char *key, bool *is_new);

/* Free value substructures of entry and reset to a clean STRING-typed state
 * with no value attached. expire/version/dirty are preserved unless
 * also_reset_meta is true. */
extern void pg_redis_entry_release_value(PgRedisEntry *e, bool also_reset_meta);

/* Remove entry from the store, freeing all substructure. Returns true if
 * present. */
extern bool pg_redis_store_remove(const char *key);

/* Total approximate memory usage of the in-memory keyspace (entries). */
extern Size pg_redis_store_memory_usage(void);
extern int64 pg_redis_store_count(void);

/* Iterate keys. Initialize then call _next until it returns NULL. */
extern void pg_redis_store_seq_init(HASH_SEQ_STATUS *status);
extern PgRedisEntry *pg_redis_store_seq_next(HASH_SEQ_STATUS *status);

/* Type label helpers. */
extern const char *pg_redis_type_name(PgRedisValueType t);
extern bool pg_redis_type_from_name(const char *name, PgRedisValueType *out);

/* Per-type counters used by STATS. */
typedef struct PgRedisStoreStats
{
	int64		key_count;
	int64		string_count;
	int64		int_count;
	int64		hash_count;
	int64		list_count;
	int64		expiring_count;
	int64		memory_usage;
} PgRedisStoreStats;

extern void pg_redis_store_collect_stats(PgRedisStoreStats *out);

/* ------------------------------------------------------------------------- */
/* Dirty-set bookkeeping (per-backend) for batched pre-commit persistence.
 *
 * - Mutators call pg_redis_mark_dirty(e) after touching an entry; the entry
 *   is appended to a singly-linked list (idempotent: ignored if already in
 *   the set).
 * - DEL/TTL-eviction call pg_redis_mark_deleted(key) before the entry is
 *   freed; the key string is copied into the dirty-tracking context.
 * - The flush path takes ownership of the lists via pg_redis_dirty_head_take()
 *   and pg_redis_pending_deletes_take(); both clear the in-memory state. */

extern void pg_redis_mark_dirty(PgRedisEntry *e);
extern void pg_redis_mark_deleted(const char *key);

/* Walk the dirty-set, returning the head and resetting the in-memory set.
 * Caller owns the returned chain (linked via dirty_next) and must walk it,
 * clearing in_dirty_set as it goes. */
extern PgRedisEntry *pg_redis_dirty_head_take(void);

/* Drop the dirty-set without taking ownership. Resets in_dirty_set flags. */
extern void pg_redis_dirty_clear(void);

typedef struct PgRedisPendingDelete
{
	char	   *key;
	struct PgRedisPendingDelete *next;
} PgRedisPendingDelete;

/* Take and reset the pending-deletes list. Caller owns the returned chain. */
extern PgRedisPendingDelete *pg_redis_pending_deletes_take(void);

/* Drop pending deletes without taking ownership. */
extern void pg_redis_pending_deletes_clear(void);

/* WRONGTYPE diagnostic. */
extern void pg_redis_wrongtype(const char *key, PgRedisValueType actual,
							   PgRedisValueType expected);

#endif							/* PG_REDIS_KV_STORE_H */
