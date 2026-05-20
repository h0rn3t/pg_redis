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
	pg_atomic_uint32 state;		/* 0 = empty, 1 = writing, 2 = ready, 3 = drained */
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

/* Producer-side API. Returns the slot it wrote into; raises ERROR on
 * insufficient_resources when in 'block' mode and the ring is full past the
 * spin budget. */
extern void pg_redis_dirty_ring_publish(const PgRedisDirtyEvent *ev);

/* Consumer (BGW) drain. Returns count copied (0 if nothing pending). */
extern int	pg_redis_dirty_ring_drain(PgRedisDirtyEvent *out_buf, int max);

/* Discard every currently-pending event without persisting it. Frees any
 * DSA-overflow payloads so they don't leak. Returns the number of events
 * dropped. Used by FLUSHALL in async mode (the durable rows are being
 * TRUNCATE'd, so pending events would be redundant or worse, resurrecting
 * deleted keys). Caller MUST hold every partition LWLock exclusively to
 * keep producers out for the duration of the drop. */
extern int	pg_redis_dirty_ring_drop_all_pending(void);

/* Counters helpers. */
extern uint64 pg_redis_dirty_ring_pending(void);
extern bool pg_redis_dirty_ring_full(void);

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
