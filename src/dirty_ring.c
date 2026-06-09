#include "postgres.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/timestamp.h"

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

void
pg_redis_dirty_ring_inspect(uint64 *out_write_head, uint64 *out_read_head,
							uint64 *out_pending, int32 *out_stuck_writing)
{
	PgRedisSharedHeader *h = pg_redis_shmem_header();
	uint64		w = 0,
				r = 0,
				pos;
	int32		stuck = 0;

	if (h != NULL && ring_slots != NULL && ring_capacity > 0)
	{
		r = pg_atomic_read_u64(&h->ring_read_head);
		w = pg_atomic_read_u64(&h->ring_write_head);

		/* Scan [read_head, write_head] inclusive — a stuck WRITING slot lives
		 * at write_head under the advance-after-READY protocol. Best-effort,
		 * no lock held. */
		for (pos = r; pos <= w; pos++)
		{
			if (pg_atomic_read_u32(&ring_slots[pos % ring_capacity].state) == SLOT_WRITING)
				stuck++;
		}
	}

	if (out_write_head != NULL)
		*out_write_head = w;
	if (out_read_head != NULL)
		*out_read_head = r;
	if (out_pending != NULL)
		*out_pending = (w > r) ? (w - r) : 0;
	if (out_stuck_writing != NULL)
		*out_stuck_writing = stuck;
}

/* Forward decl — defined in src/persistence.c. Called by publish_blocking()
 * when the ring is full and async_full_action='sync_flush'. The producer
 * drains synchronously inside an internal subtransaction of the user's xact
 * (or a top-level transaction when no outer xact is active), so commits become
 * slower instead of failing. */
extern int	pg_redis_persistence_sync_drain(void);

/* Free an event's DSA spill payload (if any). Used on publish failure paths
 * where the event never made it into a slot, so the caller still owns the
 * payload and must release it to avoid leaking shared memory. */
static void
free_event_spill(PgRedisDirtyEvent *ev)
{
	if (ev->dsa_overflow && ev->dsa_payload != InvalidDsaPointer)
	{
		pg_redis_shared_pfree(ev->dsa_payload);
		ev->dsa_payload = InvalidDsaPointer;
		ev->dsa_overflow = 0;
	}
}

/*
 * Producer reservation + publish — two-phase, mechanism only (Decision 4).
 *
 * Reserve: read write_head/read_head; if the ring is full, return
 * PG_REDIS_RING_FULL without touching any shared state. Otherwise CAS
 * slots[w % cap] EMPTY->WRITING. On CAS failure (a racing producer owns the
 * slot, or it is mid-drain / awaiting reclaim) retry the reservation from the
 * top — write_head cannot advance past w until slots[w]'s owner publishes it.
 *
 * Write: fill the slot payload.
 *
 * Publish: pg_write_barrier(), CAS state WRITING->READY, then CAS write_head
 * w -> w+1. write_head advances ONLY past a slot already marked READY, so a
 * producer that ereports between WRITING and READY leaves write_head unchanged
 * and the slot in WRITING for the reclaim pass — it never punches a hole in
 * [read_head, write_head) that would permanently stall the consumer (bug #4).
 */
PgRedisRingPublishResult
pg_redis_dirty_ring_publish(const PgRedisDirtyEvent *ev)
{
	PgRedisSharedHeader *h = pg_redis_shmem_header();
	PgRedisDirtyEvent *target;
	uint64		w;
	uint32		expected;

	if (h == NULL || ring_slots == NULL || ring_capacity <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_redis: async dirty-ring not initialized"),
				 errdetail("This indicates persistence_mode='async_table' without "
						   "shared_preload_libraries='pg_redis'.")));

	/* Phase 1: reserve a slot. */
	for (;;)
	{
		uint64		r;

		w = pg_atomic_read_u64(&h->ring_write_head);
		r = pg_atomic_read_u64(&h->ring_read_head);

		if (w - r >= (uint64) ring_capacity)
			return PG_REDIS_RING_FULL;

		target = &ring_slots[w % ring_capacity];
		expected = SLOT_EMPTY;
		if (pg_atomic_compare_exchange_u32(&target->state, &expected, SLOT_WRITING))
			break;				/* reserved position w */

		CHECK_FOR_INTERRUPTS();
	}

	/* Phase 2: write payload. state/write_started_at are managed here, NOT
	 * copied from ev (ev's state field is meaningless on the local stack). */
	target->write_started_at = GetCurrentTimestamp();
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

	/* Phase 3: publish. The write barrier ensures the payload is visible
	 * before the consumer can observe SLOT_READY. */
	pg_write_barrier();
	expected = SLOT_WRITING;
	if (!pg_atomic_compare_exchange_u32(&target->state, &expected, SLOT_READY))
	{
		/* Our slot was reclaimed out from under us (we exceeded
		 * ring_slot_stuck_timeout between reserve and publish — pathological
		 * for a live producer). The payload write is void; the slot now belongs
		 * to whoever the reclaim handed it to. Tell the caller to retry, and
		 * since the slot took our dsa_payload pointer by copy we must NOT free
		 * it here (the reclaim left state EMPTY but never read the payload;
		 * the pointer is still valid and still owned by the caller). */
		return PG_REDIS_RING_FULL;
	}

	/* Advance write_head past the now-READY slot. We are the unique owner of
	 * position w, and write_head only advances one position at a time by the
	 * owner of that position, so write_head must still equal w here. */
	{
		uint64		ew = w;

		(void) pg_atomic_compare_exchange_u64(&h->ring_write_head, &ew, w + 1);
		Assert(ew == w);
	}

	return PG_REDIS_RING_OK;
}

/*
 * Producer-side blocking publish for callers holding NO pg_redis LWLock.
 * Applies pg_redis.async_full_action on a full ring and loops until the event
 * is published or it raises.
 */
void
pg_redis_dirty_ring_publish_blocking(PgRedisDirtyEvent *ev)
{
	int			spin = 0;

	for (;;)
	{
		if (pg_redis_dirty_ring_publish(ev) == PG_REDIS_RING_OK)
			return;

		/* Full ring. Wake the BGW and apply backpressure. No pg_redis LWLock
		 * is held here, so draining synchronously is safe. */
		pg_redis_shmem_wake_bgw();

		if (pg_redis_async_full_action == PG_REDIS_ASYNC_FULL_SYNC_FLUSH)
		{
			(void) pg_redis_persistence_sync_drain();
			spin = 0;
			continue;
		}

		/* block mode: spin ~10ms then give up. */
		if (++spin >= 100)
		{
			free_event_spill(ev);
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("pg_redis: async dirty-ring is full"),
					 errhint("Raise pg_redis.dirty_ring_size, or set "
							 "pg_redis.async_full_action='sync_flush' for inline drain.")));
		}
		pg_usleep(100);
		CHECK_FOR_INTERRUPTS();
	}
}

/*
 * Consumer drain, phase 1 "collect" (Decision 11). Under ring_consumer_lock,
 * copy up to `max` READY events starting at read_head into out_buf, CAS'ing
 * each consumed slot READY->DRAINING. Does NOT advance read_head and does NOT
 * free slots — that happens in release_batch() only after the durable write
 * has been executed. Stops at the first non-READY slot (a WRITING producer or
 * the empty tail). Records the consumed base (= read_head at entry) in
 * *out_base and returns the count.
 */
int
pg_redis_dirty_ring_collect(PgRedisDirtyEvent *out_buf, int max, uint64 *out_base)
{
	PgRedisSharedHeader *h = pg_redis_shmem_header();
	uint64		w,
				r;
	int			collected = 0;

	if (out_base != NULL)
		*out_base = 0;
	if (h == NULL || ring_slots == NULL || ring_capacity <= 0)
		return 0;

	/* A sync_flush producer MUST have released its partition lock before
	 * reaching here (Decision 10); the BGW holds nothing. */
	pg_redis_assert_no_pg_redis_lwlock_held();

	LWLockAcquire(h->ring_consumer_lock, LW_EXCLUSIVE);

	r = pg_atomic_read_u64(&h->ring_read_head);
	w = pg_atomic_read_u64(&h->ring_write_head);
	if (out_base != NULL)
		*out_base = r;

	while (collected < max && r + (uint64) collected < w)
	{
		PgRedisDirtyEvent *slot = &ring_slots[(r + (uint64) collected) % ring_capacity];
		uint32		expected = SLOT_READY;

		if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, SLOT_DRAINING))
			break;				/* first non-READY slot: stop (preserves order) */

		pg_read_barrier();
		out_buf[collected] = *slot;		/* copy incl. dsa_payload pointer */
		collected++;
	}

	LWLockRelease(h->ring_consumer_lock);
	return collected;
}

/*
 * Consumer drain, phase 3 "release" — success path. After the batch's durable
 * write has been executed, advance read_head past it, flip the DRAINING slots
 * to EMPTY, and free each event's DSA spill payload exactly once. Under
 * ring_consumer_lock. `batch` is unused (the slots are authoritative) but kept
 * for API symmetry.
 */
void
pg_redis_dirty_ring_release_batch(const PgRedisDirtyEvent *batch, uint64 base, int n)
{
	PgRedisSharedHeader *h = pg_redis_shmem_header();
	int			i;

	if (h == NULL || ring_slots == NULL || ring_capacity <= 0 || n <= 0)
		return;

	LWLockAcquire(h->ring_consumer_lock, LW_EXCLUSIVE);

	/* Only one drainer ever holds a given batch's DRAINING slots, and
	 * read_head advances only under this lock, so read_head must still equal
	 * base here. */
	Assert(pg_atomic_read_u64(&h->ring_read_head) == base);

	for (i = 0; i < n; i++)
	{
		PgRedisDirtyEvent *slot = &ring_slots[(base + (uint64) i) % ring_capacity];

		if (slot->dsa_overflow && slot->dsa_payload != InvalidDsaPointer)
		{
			pg_redis_shared_pfree(slot->dsa_payload);
			slot->dsa_payload = InvalidDsaPointer;
			slot->dsa_overflow = 0;
		}
		pg_atomic_write_u32(&slot->state, SLOT_EMPTY);
	}

	pg_atomic_write_u64(&h->ring_read_head, base + (uint64) n);

	LWLockRelease(h->ring_consumer_lock);
}

/*
 * Consumer drain, phase 3 "abort" — failure path. The durable write raised
 * before commit; reset the batch's DRAINING slots back to READY, leave
 * read_head unchanged, and keep the DSA spill payloads (the slots still own
 * them) so a subsequent drain re-collects and re-persists the events
 * (at-least-once). Under ring_consumer_lock.
 */
void
pg_redis_dirty_ring_abort_batch(const PgRedisDirtyEvent *batch, uint64 base, int n)
{
	PgRedisSharedHeader *h = pg_redis_shmem_header();
	int			i;

	if (h == NULL || ring_slots == NULL || ring_capacity <= 0 || n <= 0)
		return;

	LWLockAcquire(h->ring_consumer_lock, LW_EXCLUSIVE);
	for (i = 0; i < n; i++)
	{
		PgRedisDirtyEvent *slot = &ring_slots[(base + (uint64) i) % ring_capacity];

		pg_atomic_write_u32(&slot->state, SLOT_READY);
	}
	LWLockRelease(h->ring_consumer_lock);
}

/*
 * Reclamation pass. A producer that ereports between reserving a slot
 * (EMPTY->WRITING) and publishing it (WRITING->READY) leaves write_head
 * UNCHANGED, so the only place a stuck WRITING slot can sit is AT write_head
 * (the next-to-publish position). Check that slot; if it has been WRITING
 * longer than ring_slot_stuck_timeout, CAS it back to EMPTY so producers can
 * reserve it again. Returns the number reclaimed (0 or 1). Under
 * ring_consumer_lock.
 */
int
pg_redis_dirty_ring_reclaim_stuck(void)
{
	PgRedisSharedHeader *h = pg_redis_shmem_header();
	PgRedisDirtyEvent *slot;
	uint64		w;
	uint32		expected = SLOT_WRITING;
	int			reclaimed = 0;
	TimestampTz now;

	if (h == NULL || ring_slots == NULL || ring_capacity <= 0)
		return 0;

	LWLockAcquire(h->ring_consumer_lock, LW_EXCLUSIVE);
	w = pg_atomic_read_u64(&h->ring_write_head);
	slot = &ring_slots[w % ring_capacity];
	now = GetCurrentTimestamp();

	if (pg_atomic_read_u32(&slot->state) == SLOT_WRITING &&
		TimestampDifferenceExceeds(slot->write_started_at, now,
								   pg_redis_ring_slot_stuck_timeout_ms))
	{
		/* A live producer never lingers >timeout between reserve and publish,
		 * so this slot belongs to a crashed/aborted producer. CAS so we don't
		 * race a producer that just (re)reserved it. */
		if (pg_atomic_compare_exchange_u32(&slot->state, &expected, SLOT_EMPTY))
			reclaimed = 1;
	}

	LWLockRelease(h->ring_consumer_lock);
	return reclaimed;
}

int
pg_redis_dirty_ring_drop_all_pending(void)
{
	PgRedisSharedHeader *h = pg_redis_shmem_header();
	uint64		w,
				r;
	int			dropped = 0;

	if (h == NULL || ring_slots == NULL || ring_capacity <= 0)
		return 0;

	LWLockAcquire(h->ring_consumer_lock, LW_EXCLUSIVE);
	r = pg_atomic_read_u64(&h->ring_read_head);
	w = pg_atomic_read_u64(&h->ring_write_head);

	while (r < w)
	{
		PgRedisDirtyEvent *slot = &ring_slots[r % ring_capacity];
		uint32		st = pg_atomic_read_u32(&slot->state);

		/* Drop READY and already-DRAINING slots (freeing their spill payloads).
		 * Stop at a WRITING boundary slot — that belongs to a live producer
		 * mid-publish; the reclaim pass handles a stuck one. */
		if (st != SLOT_READY && st != SLOT_DRAINING)
			break;

		if (slot->dsa_overflow && slot->dsa_payload != InvalidDsaPointer)
		{
			pg_redis_shared_pfree(slot->dsa_payload);
			slot->dsa_payload = InvalidDsaPointer;
			slot->dsa_overflow = 0;
		}
		pg_atomic_write_u32(&slot->state, SLOT_EMPTY);
		r++;
		dropped++;
	}

	if (dropped > 0)
		pg_atomic_write_u64(&h->ring_read_head, r);

	LWLockRelease(h->ring_consumer_lock);
	return dropped;
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
