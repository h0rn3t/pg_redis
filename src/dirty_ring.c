#include "postgres.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"

#include <string.h>

#include "types.h"
#include "shmem.h"
#include "dirty_ring.h"

/*
 * Shared dirty-ring backing store.
 *
 * The ring's atomic counters (write_head, read_head) live in PgRedisSharedHeader.
 * The slot array lives in its own shmem allocation, pointed to here once
 * pg_redis_shmem_startup() hands it off via pg_redis_dirty_ring_shmem_attach().
 *
 * Concurrency:
 *   - Producers do `slot_idx = pg_atomic_fetch_add_u64(&write_head, 1)`,
 *     then write into slots[slot_idx % ring_size].
 *   - The single consumer (the BGW) reads from slots[read_head % ring_size]
 *     onward up to write_head and bumps read_head.
 *   - Each slot has an `state` atomic so the consumer can spin briefly if
 *     a producer claimed the slot but hasn't filled it yet.
 *
 * Slot states: 0 = empty (or post-drain reset), 1 = writing, 2 = ready,
 *   3 = draining (consumer has the slot). The consumer flips 2 -> 3 -> 0.
 */

static PgRedisDirtyEvent *ring_slots = NULL;
static int	ring_capacity = 0;

#define SLOT_EMPTY     0u
#define SLOT_WRITING   1u
#define SLOT_READY     2u
#define SLOT_DRAINING  3u

Size
pg_redis_dirty_ring_size_bytes(int slot_count)
{
	return mul_size(sizeof(PgRedisDirtyEvent), slot_count);
}

void
pg_redis_dirty_ring_shmem_attach(void *slot_area, int slot_count)
{
	/* Set per-backend statics. Every backend's shmem_startup_hook must hit
	 * this so subsequent publish()/drain() calls see a valid pointer. */
	ring_slots = (PgRedisDirtyEvent *) slot_area;
	ring_capacity = slot_count;
}

void
pg_redis_dirty_ring_shmem_init(void *slot_area, int slot_count)
{
	/* First-time only: zero-initialize the slot array and set up the per-
	 * slot atomic state. ShmemInitStruct does not memset for us. */
	memset(slot_area, 0, mul_size(sizeof(PgRedisDirtyEvent), slot_count));
	for (int i = 0; i < slot_count; i++)
	{
		PgRedisDirtyEvent *slot = &((PgRedisDirtyEvent *) slot_area)[i];

		pg_atomic_init_u32(&slot->state, SLOT_EMPTY);
	}
}

uint64
pg_redis_dirty_ring_pending(void)
{
	PgRedisSharedHeader *h = pg_redis_shmem_header();
	uint64		w,
				r;

	if (h == NULL)
		return 0;
	w = pg_atomic_read_u64(&h->ring_write_head);
	r = pg_atomic_read_u64(&h->ring_read_head);
	return (w > r) ? (w - r) : 0;
}

bool
pg_redis_dirty_ring_full(void)
{
	if (ring_capacity <= 0)
		return false;
	return pg_redis_dirty_ring_pending() >= (uint64) ring_capacity;
}

/*
 * Producer path. Backpressure governed by pg_redis.async_full_action:
 *   - block: spin briefly (100 × 100µs = ~10ms), wake the BGW, then ERROR.
 *   - sync_flush: handled by the caller in [src/persistence.c] which drains
 *     locally before retrying — this function just signals "full" via ERROR
 *     so the caller can fall back. (Pure ring code stays mechanism-only.)
 *
 * Slot acquisition uses pg_atomic_fetch_add_u64 on write_head; the slot
 * index is the returned value modulo ring_capacity.
 */
/* Forward decl — defined in src/persistence.c. Called by publish() when the
 * ring is full and async_full_action='sync_flush'. The producer drains
 * synchronously in its own transaction so commits become slower instead of
 * failing. NULL-safe: returns 0 if persistence layer can't drain. */
extern int pg_redis_persistence_sync_drain(void);

void
pg_redis_dirty_ring_publish(const PgRedisDirtyEvent *ev)
{
	PgRedisSharedHeader *h = pg_redis_shmem_header();
	uint64		my_slot;
	int			spin;
	PgRedisDirtyEvent *target;
	uint32		expected;

	if (h == NULL || ring_slots == NULL || ring_capacity <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_redis: async dirty-ring not initialized"),
				 errdetail("This indicates persistence_mode='async_table' without "
						   "shared_preload_libraries='pg_redis'.")));

	/* Spin-wait for room. The window we watch is
	 *   write_head - read_head < ring_capacity. */
	for (spin = 0;; spin++)
	{
		uint64		w = pg_atomic_read_u64(&h->ring_write_head);
		uint64		r = pg_atomic_read_u64(&h->ring_read_head);

		if (w - r < (uint64) ring_capacity)
			break;

		/* Full — kick the BGW and back off briefly. */
		pg_redis_shmem_wake_bgw();
		if (spin >= 100)
		{
			if (pg_redis_async_full_action == PG_REDIS_ASYNC_FULL_SYNC_FLUSH)
			{
				/* Drain inline in this backend. Then retry the slot
				 * claim from the top. */
				(void) pg_redis_persistence_sync_drain();
				spin = 0;
				continue;
			}
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("pg_redis: async dirty-ring is full"),
					 errhint("Raise pg_redis.dirty_ring_size, or set "
							 "pg_redis.async_full_action='sync_flush' for inline drain.")));
		}
		pg_usleep(100);
	}

	my_slot = pg_atomic_fetch_add_u64(&h->ring_write_head, 1);
	target = &ring_slots[my_slot % ring_capacity];

	/* Wait for the slot to be empty (consumer may not have finished
	 * draining a prior wraparound). */
	for (spin = 0;; spin++)
	{
		expected = SLOT_EMPTY;
		if (pg_atomic_compare_exchange_u32(&target->state, &expected, SLOT_WRITING))
			break;
		if (spin >= 1000)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_redis: dirty-ring slot stuck — consumer lagging?")));
		pg_usleep(10);
	}

	/* Copy the event payload. */
	target->type = ev->type;
	target->value_type = ev->value_type;
	target->key_len = ev->key_len;
	target->field_len = ev->field_len;
	target->ord = ev->ord;
	target->expire_at = ev->expire_at;
	target->has_expire = ev->has_expire;
	target->version = ev->version;
	target->value_len = ev->value_len;
	target->dsa_overflow = ev->dsa_overflow;
	target->dsa_payload = ev->dsa_payload;
	memcpy(target->inline_bytes, ev->inline_bytes,
		   sizeof(target->inline_bytes));

	pg_write_barrier();
	pg_atomic_write_u32(&target->state, SLOT_READY);
}

/*
 * Consumer drain. Reads up to `max` ready slots starting at read_head,
 * copies them into out_buf, advances read_head and clears slot states.
 */
int
pg_redis_dirty_ring_drain(PgRedisDirtyEvent *out_buf, int max)
{
	PgRedisSharedHeader *h = pg_redis_shmem_header();
	uint64		w,
				r;
	int			drained = 0;

	if (h == NULL || ring_slots == NULL || ring_capacity <= 0)
		return 0;

	w = pg_atomic_read_u64(&h->ring_write_head);
	r = pg_atomic_read_u64(&h->ring_read_head);

	while (drained < max && r < w)
	{
		PgRedisDirtyEvent *slot = &ring_slots[r % ring_capacity];
		uint32		expected = SLOT_READY;

		if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, SLOT_DRAINING))
		{
			/* Producer hasn't finished filling this slot. Bail out and
			 * let the next drain pick it up. */
			break;
		}

		pg_read_barrier();
		out_buf[drained] = *slot;

		pg_atomic_write_u32(&slot->state, SLOT_EMPTY);
		r++;
		drained++;
	}

	if (drained > 0)
		pg_atomic_write_u64(&h->ring_read_head, r);

	return drained;
}

/* -------------------------------------------------------------------------
 * Event encoders / decoders.
 *
 * Inline layout (in PgRedisDirtyEvent.inline_bytes, starting at offset 0):
 *
 *   KEY_UPSERT:        [key (key_len bytes)] [TLV value (value_len bytes)]
 *   KEY_DELETE:        [key (key_len bytes)]
 *   HASH_FIELD_UPSERT: [key (key_len)] [field (field_len)] [value (value_len)]
 *   HASH_FIELD_DELETE: [key (key_len)] [field (field_len)]
 *   LIST_ITEM_INSERT:  [key (key_len)] [value (value_len)]
 *   LIST_ITEM_DELETE:  [key (key_len)]
 *
 * Total inline bytes used = key_len + field_len + value_len. When that sum
 * exceeds PG_REDIS_DIRTY_EVENT_INLINE_BYTES, the event sets dsa_overflow=1
 * and stores the concatenated bytes in a DSA chunk pointed to by
 * dsa_payload. The accessor functions transparently follow the pointer.
 *
 * (DSA spill is deferred — Group 4 lands the inline path; the spill path
 * raises ERROR on oversize. Keys+fields are length-capped to 1024 bytes
 * each by the GUCs, so the 256-byte inline budget covers small values; the
 * spill path becomes important for large list/hash values.)
 * ------------------------------------------------------------------------- */

static void
event_inline_write(PgRedisDirtyEvent *ev,
				   const char *part1, Size len1,
				   const char *part2, Size len2,
				   const char *part3, Size len3)
{
	Size		total = len1 + len2 + len3;
	char	   *p;

	if (total > PG_REDIS_DIRTY_EVENT_INLINE_BYTES)
	{
		/* Spill to DSA. Allocate a single contiguous chunk holding the same
		 * [key][field][value] concatenation that would otherwise fit inline.
		 * The consumer resolves via pg_redis_shared_addr(). */
		dsa_pointer dsa_p = pg_redis_shared_palloc(total);

		if (dsa_p == InvalidDsaPointer)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("pg_redis: out of shared memory for dirty-ring spill payload %zu",
							total),
					 errhint("Raise pg_redis.shared_max_memory.")));
		p = (char *) pg_redis_shared_addr(dsa_p);
		if (len1 > 0)
			memcpy(p, part1, len1);
		p += len1;
		if (len2 > 0)
			memcpy(p, part2, len2);
		p += len2;
		if (len3 > 0)
			memcpy(p, part3, len3);

		ev->dsa_overflow = 1;
		ev->dsa_payload = dsa_p;
		/* inline_bytes contents are don't-care when dsa_overflow is set. */
		return;
	}

	p = ev->inline_bytes;
	if (len1 > 0)
		memcpy(p, part1, len1);
	p += len1;
	if (len2 > 0)
		memcpy(p, part2, len2);
	p += len2;
	if (len3 > 0)
		memcpy(p, part3, len3);

	ev->dsa_overflow = 0;
	ev->dsa_payload = InvalidDsaPointer;
}

void
pg_redis_event_encode_key_upsert(PgRedisDirtyEvent *ev,
								 const PgRedisEntry *e,
								 const unsigned char *tlv, Size tlv_len)
{
	Size		keylen = strlen(e->key);

	memset(ev, 0, offsetof(PgRedisDirtyEvent, inline_bytes));
	ev->type = PG_REDIS_EVENT_KEY_UPSERT;
	ev->value_type = e->type;
	ev->key_len = (uint16) keylen;
	ev->field_len = 0;
	ev->ord = 0;
	ev->expire_at = e->expire_at;
	ev->has_expire = e->has_expire;
	ev->version = e->version;
	ev->value_len = (uint16) tlv_len;
	event_inline_write(ev,
					   e->key, keylen,
					   NULL, 0,
					   (const char *) tlv, tlv_len);
}

void
pg_redis_event_encode_key_delete(PgRedisDirtyEvent *ev,
								 const char *key, Size keylen)
{
	memset(ev, 0, offsetof(PgRedisDirtyEvent, inline_bytes));
	ev->type = PG_REDIS_EVENT_KEY_DELETE;
	ev->value_type = PG_REDIS_TYPE_STRING;	/* don't-care */
	ev->key_len = (uint16) keylen;
	event_inline_write(ev, key, keylen, NULL, 0, NULL, 0);
}

void
pg_redis_event_encode_hash_field_upsert(PgRedisDirtyEvent *ev,
										const char *key, Size keylen,
										const char *field, Size fieldlen,
										const unsigned char *val, Size val_len)
{
	memset(ev, 0, offsetof(PgRedisDirtyEvent, inline_bytes));
	ev->type = PG_REDIS_EVENT_HASH_FIELD_UPSERT;
	ev->value_type = PG_REDIS_TYPE_HASH;
	ev->key_len = (uint16) keylen;
	ev->field_len = (uint16) fieldlen;
	ev->value_len = (uint16) val_len;
	event_inline_write(ev,
					   key, keylen,
					   field, fieldlen,
					   (const char *) val, val_len);
}

void
pg_redis_event_encode_hash_field_delete(PgRedisDirtyEvent *ev,
										const char *key, Size keylen,
										const char *field, Size fieldlen)
{
	memset(ev, 0, offsetof(PgRedisDirtyEvent, inline_bytes));
	ev->type = PG_REDIS_EVENT_HASH_FIELD_DELETE;
	ev->value_type = PG_REDIS_TYPE_HASH;
	ev->key_len = (uint16) keylen;
	ev->field_len = (uint16) fieldlen;
	event_inline_write(ev, key, keylen, field, fieldlen, NULL, 0);
}

void
pg_redis_event_encode_list_item_insert(PgRedisDirtyEvent *ev,
									   const char *key, Size keylen,
									   int64 ord,
									   const unsigned char *val, Size val_len)
{
	memset(ev, 0, offsetof(PgRedisDirtyEvent, inline_bytes));
	ev->type = PG_REDIS_EVENT_LIST_ITEM_INSERT;
	ev->value_type = PG_REDIS_TYPE_LIST;
	ev->key_len = (uint16) keylen;
	ev->ord = ord;
	ev->value_len = (uint16) val_len;
	event_inline_write(ev, key, keylen, NULL, 0,
					   (const char *) val, val_len);
}

void
pg_redis_event_encode_list_item_delete(PgRedisDirtyEvent *ev,
									   const char *key, Size keylen,
									   int64 ord)
{
	memset(ev, 0, offsetof(PgRedisDirtyEvent, inline_bytes));
	ev->type = PG_REDIS_EVENT_LIST_ITEM_DELETE;
	ev->value_type = PG_REDIS_TYPE_LIST;
	ev->key_len = (uint16) keylen;
	ev->ord = ord;
	event_inline_write(ev, key, keylen, NULL, 0, NULL, 0);
}

/* --- Decoders --- */

static const char *
event_payload_base(const PgRedisDirtyEvent *ev)
{
	if (ev->dsa_overflow)
	{
		/* Resolve DSA pointer to local address. */
		extern void *pg_redis_shared_addr(dsa_pointer);

		return (const char *) pg_redis_shared_addr(ev->dsa_payload);
	}
	return ev->inline_bytes;
}

void
pg_redis_event_decode_key(const PgRedisDirtyEvent *ev,
						  const char **out_key, Size *out_keylen)
{
	const char *base = event_payload_base(ev);

	*out_key = base;
	*out_keylen = ev->key_len;
}

void
pg_redis_event_decode_field(const PgRedisDirtyEvent *ev,
							const char **out_field, Size *out_fieldlen)
{
	const char *base = event_payload_base(ev);

	*out_field = base + ev->key_len;
	*out_fieldlen = ev->field_len;
}

void
pg_redis_event_decode_value(const PgRedisDirtyEvent *ev,
							const unsigned char **out_value,
							Size *out_value_len)
{
	const char *base = event_payload_base(ev);

	*out_value = (const unsigned char *) (base + ev->key_len + ev->field_len);
	*out_value_len = ev->value_len;
}
