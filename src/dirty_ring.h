#ifndef PG_REDIS_DIRTY_RING_H
#define PG_REDIS_DIRTY_RING_H

#include "postgres.h"
#include "port/atomics.h"
#include "utils/dsa.h"

#include "types.h"

/*
 * Shared multi-producer / single-consumer dirty-ring used when
 * `pg_redis.persistence_mode = 'async_table'`.
 *
 * Backends (producers) atomically claim a slot via the ring_write_head
 * counter in the shared header, fill in a PgRedisDirtyEvent, and return.
 * The single consumer (the pg_redis bgworker) reads slots in order from
 * ring_read_head and persists them via the v1.1 batched-array SPI plans,
 * then advances ring_read_head.
 *
 * Variable-size payloads (long keys, long field names, long values) that do
 * not fit inline get stored in the shared DSA segment; the slot carries a
 * dsa_pointer instead.
 */

#define PG_REDIS_DIRTY_EVENT_INLINE_BYTES 256

typedef enum PgRedisDirtyEventType
{
	PG_REDIS_EVENT_KEY_UPSERT = 1,
	PG_REDIS_EVENT_KEY_DELETE,
	PG_REDIS_EVENT_HASH_FIELD_UPSERT,
	PG_REDIS_EVENT_HASH_FIELD_DELETE,
	PG_REDIS_EVENT_LIST_ITEM_INSERT,
	PG_REDIS_EVENT_LIST_ITEM_DELETE
}			PgRedisDirtyEventType;

typedef struct PgRedisDirtyEvent
{
	/* Slot lifecycle, CAS'd by producers and the consumer:
	 *   SLOT_EMPTY(0) -> SLOT_WRITING(1) -> SLOT_READY(2) -> SLOT_DRAINING(3) -> SLOT_EMPTY
	 * A producer reserves EMPTY->WRITING, fills the payload, then publishes
	 * WRITING->READY (and only then advances write_head). The consumer's
	 * collect phase CAS's READY->DRAINING; the post-commit release phase flips
	 * DRAINING->EMPTY (success) or the abort path resets DRAINING->READY. */
	pg_atomic_uint32 state;

	/* GetCurrentTimestamp() stamped when the slot is reserved (EMPTY->WRITING).
	 * The consumer's reclamation pass uses it to reset slots stuck in WRITING
	 * (producer ereport'd between reserve and publish) after a grace period. */
	TimestampTz write_started_at;

	PgRedisDirtyEventType type;
	PgRedisValueType value_type;	/* for KEY_UPSERT: string vs int vs hash vs list */

	/* Identity. */
	uint16		key_len;
	uint16		field_len;		/* hash events only */
	int64		ord;			/* list events only */
	TimestampTz expire_at;
	bool		has_expire;
	uint64		version;

	/* Inline payload area: laid out as [key][field][value] when each fits.
	 * Lengths above describe the inline layout. Overflow goes to DSA. */
	uint16		value_len;		/* inline value length; 0 if not present */
	uint16		dsa_overflow;	/* nonzero when payload spilled to DSA */
	dsa_pointer dsa_payload;	/* valid only when dsa_overflow != 0 */

	char		inline_bytes[PG_REDIS_DIRTY_EVENT_INLINE_BYTES];
}			PgRedisDirtyEvent;

/* Shmem sizing — used by shmem.c during request_hook. */
extern Size pg_redis_dirty_ring_size_bytes(int slot_count);

/* Wire the slot array allocated in shmem to the module's static state.
 * Always called from shmem_startup_hook so every backend sets its local
 * pointer. */
extern void pg_redis_dirty_ring_shmem_attach(void *slot_area, int slot_count);

/* First-time-only initialization of the slot array (memset + atomic init).
 * Only the postmaster (or whichever process gets !found from ShmemInitStruct)
 * should call this. Backends that attach to existing shmem skip this. */
extern void pg_redis_dirty_ring_shmem_init(void *slot_area, int slot_count);

/* Result of a single (mechanism-only) publish attempt. */
typedef enum PgRedisRingPublishResult
{
	PG_REDIS_RING_OK = 0,		/* event reserved, written, and published */
	PG_REDIS_RING_FULL			/* no free slot — caller applies backpressure */
}			PgRedisRingPublishResult;

/*
 * Producer-side, mechanism only. Two-phase reservation: CAS a free slot
 * EMPTY->WRITING, write the payload, publish WRITING->READY, then advance
 * write_head. Returns PG_REDIS_RING_FULL (without touching shared state) when
 * the ring is full. NEVER drains and NEVER raises an error — the caller
 * applies the pg_redis.async_full_action backpressure policy (which may
 * require releasing a partition LWLock before draining). On PG_REDIS_RING_FULL
 * the event's DSA spill payload (if any) is still owned by the caller and must
 * be freed by it.
 */
extern PgRedisRingPublishResult pg_redis_dirty_ring_publish(const PgRedisDirtyEvent *ev);

/*
 * Producer-side, for callers holding NO pg_redis LWLock. Applies
 * pg_redis.async_full_action on a full ring: 'block' spins (waking the BGW)
 * up to the spin budget then raises insufficient_resources; 'sync_flush'
 * drains synchronously via pg_redis_persistence_sync_drain then retries.
 * Loops until the event is published or it raises. Frees the event's DSA spill
 * payload on the error path (hence a non-const event).
 */
extern void pg_redis_dirty_ring_publish_blocking(PgRedisDirtyEvent *ev);

/*
 * Consumer-side, two-phase drain (Decision 11). Phase 1 "collect": under
 * ring_consumer_lock, copy up to `max` READY events starting at read_head into
 * out_buf, CAS'ing each consumed slot READY->DRAINING. Does NOT advance
 * read_head and does NOT free slots. Stops at the first non-READY slot.
 * Returns the count and writes the consumed base position (= read_head at
 * entry) into *out_base. The caller MUST hold no partition LWLock.
 */
extern int	pg_redis_dirty_ring_collect(PgRedisDirtyEvent *out_buf, int max,
										uint64 *out_base);

/*
 * Phase 3 "release" (success path): after the batch's durable write has been
 * executed, advance read_head past the batch, flip the DRAINING slots to
 * EMPTY, and free each event's DSA spill payload exactly once. Under
 * ring_consumer_lock.
 */
extern void pg_redis_dirty_ring_release_batch(const PgRedisDirtyEvent *batch,
											  uint64 base, int n);

/*
 * Phase 3 "abort" (failure path): the durable write raised before commit.
 * Reset the batch's DRAINING slots back to READY and leave read_head
 * unchanged, so the events remain in the ring for a subsequent drain
 * (at-least-once). Does NOT free DSA payloads (the slots still own them).
 * Under ring_consumer_lock.
 */
extern void pg_redis_dirty_ring_abort_batch(const PgRedisDirtyEvent *batch,
											uint64 base, int n);

/*
 * Reclamation pass: scan [read_head, write_head] and reset any slot stuck in
 * SLOT_WRITING longer than pg_redis.ring_slot_stuck_timeout (producer ereport'd
 * between reserve and publish) back to SLOT_EMPTY, unblocking producers. Run by
 * the BGW every pg_redis.ring_reclaim_tick_interval drain ticks. Returns the
 * number of slots reclaimed. Takes ring_consumer_lock. */
extern int	pg_redis_dirty_ring_reclaim_stuck(void);

/* Discard every currently-pending event without persisting it. Frees any
 * DSA-overflow payloads so they don't leak. Returns the number of events
 * dropped. Used by FLUSHALL in async mode (the durable rows are being
 * TRUNCATE'd, so pending events would be redundant or worse, resurrecting
 * deleted keys). Takes ring_consumer_lock (it advances read_head); may be
 * called while holding partition LWLocks (FLUSHALL) — the partition->consumer
 * nesting has no reverse edge, so it cannot deadlock. */
extern int	pg_redis_dirty_ring_drop_all_pending(void);

/* Counters helpers. */
extern uint64 pg_redis_dirty_ring_pending(void);
extern bool pg_redis_dirty_ring_full(void);

/* Diagnostic snapshot of the ring (for the pgredis."RING_INSPECT"() SQL fn and
 * invariant tests). Reads atomics without the consumer lock, so the result is a
 * best-effort instantaneous view. Any out pointer may be NULL. */
extern void pg_redis_dirty_ring_inspect(uint64 *out_write_head,
										uint64 *out_read_head,
										uint64 *out_pending,
										int32 *out_stuck_writing);

/* --- Producer-side encoders ---
 *
 * Each `encode_*` function fills a PgRedisDirtyEvent from the current
 * in-memory state. For payloads that fit inline (key + value within
 * PG_REDIS_DIRTY_EVENT_INLINE_BYTES) the event is self-contained; oversize
 * payloads spill into the shared DSA segment and the event carries a
 * dsa_pointer. The caller passes the filled event to
 * pg_redis_dirty_ring_publish().
 *
 * `out_value_tlv` is a freshly-built TLV bytea body (no varlena header) for
 * string/int upserts. For hash/list events `out_value` is the raw bytes. */

extern void pg_redis_event_encode_key_upsert(PgRedisDirtyEvent *ev,
											 const PgRedisEntry *e,
											 const unsigned char *tlv,
											 Size tlv_len);
extern void pg_redis_event_encode_key_delete(PgRedisDirtyEvent *ev,
											 const char *key, Size keylen);
extern void pg_redis_event_encode_hash_field_upsert(PgRedisDirtyEvent *ev,
													const char *key, Size keylen,
													const char *field, Size fieldlen,
													const unsigned char *val,
													Size val_len);
extern void pg_redis_event_encode_hash_field_delete(PgRedisDirtyEvent *ev,
													const char *key, Size keylen,
													const char *field, Size fieldlen);
extern void pg_redis_event_encode_list_item_insert(PgRedisDirtyEvent *ev,
												   const char *key, Size keylen,
												   int64 ord,
												   const unsigned char *val,
												   Size val_len);
extern void pg_redis_event_encode_list_item_delete(PgRedisDirtyEvent *ev,
												   const char *key, Size keylen,
												   int64 ord);

/* --- Consumer-side decoders ---
 *
 * Each `decode_*` accessor returns pointers into the event's inline buffer
 * (or DSA-resolved pointer for spilled events). Pointers are valid only
 * until the event slot is released (the consumer must consume the data
 * before bumping read_head). The lengths are returned via the out params. */

extern void pg_redis_event_decode_key(const PgRedisDirtyEvent *ev,
									  const char **out_key, Size *out_keylen);
extern void pg_redis_event_decode_field(const PgRedisDirtyEvent *ev,
										const char **out_field, Size *out_fieldlen);
extern void pg_redis_event_decode_value(const PgRedisDirtyEvent *ev,
										const unsigned char **out_value,
										Size *out_value_len);

#endif							/* PG_REDIS_DIRTY_RING_H */
