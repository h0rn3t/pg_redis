#ifndef PG_REDIS_PERSISTENCE_H
#define PG_REDIS_PERSISTENCE_H

#include "postgres.h"
#include "types.h"

/* One-time setup (per session). Currently a no-op placeholder; the extension
 * tables are created by the extension SQL script. */
extern void pg_redis_persistence_init(void);

/* Lazy-load: pull every non-expired row from pg_redis.store into the in-memory
 * store. Called on first access in this backend session, gated by an internal
 * flag. Safe to call multiple times; subsequent calls are no-ops. */
extern void pg_redis_persistence_load_if_needed(void);
extern void pg_redis_persistence_invalidate_load(void);

/* Legacy single-row APIs preserved as thin wrappers. They now just queue the
 * entry into the per-backend dirty-set; the durable write happens in the
 * batched pre-commit flush. Safe to call inside an SPI session (no SPI). */
extern void pg_redis_persistence_save_entry(PgRedisEntry *e);
extern void pg_redis_persistence_delete_key(const char *key);

/* Iterate dirty entries and persist a batch of up to max_batch. Returns count
 * actually flushed. */
extern int64 pg_redis_persistence_flush_dirty_batch(int max_batch);

/* Snapshot all current entries into pg_redis.snapshots. The snapshot row id
 * is generated server-side via uuidv7() (PG18+) and is not returned. */
extern void pg_redis_persistence_save_snapshot(void);

/* Append one AOF row (op, key, args). args must be a palloc'd cstring of valid
 * JSON or {} for empty. */
extern void pg_redis_persistence_append_aof(const char *op, const char *key,
											const char *args_json);

/* FLUSHALL: truncate the durable store in sync_table/async_table mode and mark
 * the per-backend lazy-load flag as "loaded" (so the next access does not
 * resurrect rows from a now-empty table — and avoids a needless SPI scan). */
extern void pg_redis_persistence_flushall(void);

/* Drain up to `max_events` events from the shared dirty-ring and persist
 * them via the cached array-form SPI plans. Caller owns the transaction
 * context (StartTransactionCommand/CommitTransactionCommand). Returns the
 * number of events actually persisted. Used by the BGW main loop and by
 * the sync_flush producer-side fallback. */
extern int pg_redis_persistence_async_drain(int max_events);

/* Convenience for the sync_flush producer-side fallback: opens its own
 * transaction, calls _async_drain, commits. Returns events drained. */
extern int pg_redis_persistence_sync_drain(void);

/* Per-backend counter of dirty-ring events published in the current
 * transaction. Bumped by mark_dirty/mark_deleted in async mode; reset by the
 * xact callback at PRE_COMMIT / ABORT. Used to emit a WARNING on rollback
 * (the writes have already been ack'd to the BGW and will be persisted, so
 * the user's ROLLBACK does not undo them). */
extern void pg_redis_persistence_note_async_publish(int n);

#endif							/* PG_REDIS_PERSISTENCE_H */
