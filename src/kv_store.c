#include "postgres.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "varatt.h"
#include "storage/lwlock.h"

#include <string.h>

#include "types.h"
#include "kv_store.h"
#include "shmem.h"
#include "utils.h"
#include "ttl.h"
#include "list.h"
#include "hash_value.h"
#include "binval.h"
#include "dirty_ring.h"
#include "shared_store.h"
#include "persistence.h"

static HTAB *PgRedisStore = NULL;

/* Per-backend dirty-set: singly-linked list of mutated entries, plus a
 * companion list of keys whose entries were removed (DEL / TTL eviction).
 * Both live in pg_redis_memcxt() so they share the keyspace's lifecycle. */
static PgRedisEntry *dirty_head = NULL;
static PgRedisPendingDelete *pending_deletes_head = NULL;

static void
ensure_store_created(void)
{
	HASHCTL		ctl;

	if (PgRedisStore != NULL)
		return;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(((PgRedisEntry *) 0)->key);
	ctl.entrysize = sizeof(PgRedisEntry);
	ctl.hcxt = pg_redis_memcxt();

	PgRedisStore = hash_create("pg_redis_store",
							   1024,
							   &ctl,
							   HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
}

void
pg_redis_store_init(void)
{
	ensure_store_created();
}

void
pg_redis_store_reset(void)
{
	PgRedisStore = NULL;
	/* Both the dirty chain and the pending-deletes list live in the same
	 * memory context as the keyspace; the reset wipes them all. */
	dirty_head = NULL;
	pending_deletes_head = NULL;
	pg_redis_memcxt_reset();
	ensure_store_created();
}

HTAB *
pg_redis_store_htab(void)
{
	ensure_store_created();
	return PgRedisStore;
}

void
pg_redis_entry_release_value(PgRedisEntry *e, bool also_reset_meta)
{
	if (e == NULL)
		return;

	switch (e->type)
	{
		case PG_REDIS_TYPE_STRING:
			if (e->value.string_value != NULL)
			{
				pfree(e->value.string_value);
				e->value.string_value = NULL;
			}
			break;
		case PG_REDIS_TYPE_INT:
			e->value.int_value = 0;
			break;
		case PG_REDIS_TYPE_HASH:
			if (e->value.hash_value != NULL)
			{
				pg_redis_hash_free(e->value.hash_value);
				e->value.hash_value = NULL;
			}
			break;
		case PG_REDIS_TYPE_LIST:
			if (e->value.list_value != NULL)
			{
				pg_redis_list_free(e->value.list_value);
				e->value.list_value = NULL;
			}
			break;
	}

	e->string_len = 0;
	e->memory_usage = 0;
	e->type = PG_REDIS_TYPE_STRING;

	if (also_reset_meta)
	{
		e->expire_at = 0;
		e->has_expire = false;
		e->dirty = false;
		e->deleted = false;
		e->version = 0;
	}
}

/* -------------------------------------------------------------------------
 * Shared-mode dispatch — bodies live in src/shared_store.c.
 *
 * When `storage_mode = 'shared'` (and shared_preload_libraries is set), each
 * public CRUD function routes to its shared_store_* counterpart instead of
 * the per-backend HTAB path below. The shared path uses partitioned
 * LWLocks and DSA-backed payloads; the materialized scratch returned to
 * callers is a per-backend palloc'd PgRedisEntry valid until the next
 * mark_dirty / mark_deleted / end-of-statement.
 * ------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------
 * Public dispatch wrappers — route to the session-mode body below or to the
 * shared-mode stub above based on pg_redis_storage_is_shared(). */

PgRedisEntry *
pg_redis_store_lookup_raw(const char *key)
{
	bool		found;
	PgRedisEntry *e;

	if (pg_redis_storage_is_shared())
		return pg_redis_shared_store_lookup_raw(key);

	ensure_store_created();
	e = (PgRedisEntry *) hash_search(PgRedisStore, key, HASH_FIND, &found);
	if (!found)
		return NULL;
	return e;
}

PgRedisEntry *
pg_redis_store_lookup(const char *key, bool *found_expired)
{
	PgRedisEntry *e;

	if (found_expired)
		*found_expired = false;

	e = pg_redis_store_lookup_raw(key);
	if (e == NULL)
		return NULL;

	if (pg_redis_entry_is_expired(e))
	{
		pg_redis_store_remove(key);
		if (found_expired)
			*found_expired = true;
		return NULL;
	}
	return e;
}

PgRedisEntry *
pg_redis_store_upsert(const char *key, bool *is_new)
{
	bool		found;
	PgRedisEntry *e;

	if (pg_redis_storage_is_shared())
		return pg_redis_shared_store_upsert(key, is_new);

	ensure_store_created();
	e = (PgRedisEntry *) hash_search(PgRedisStore, key, HASH_ENTER, &found);

	if (!found)
	{
		/* hash_search has already copied the key into e->key (HASH_STRINGS).
		 * Initialize the rest. */
		e->type = PG_REDIS_TYPE_STRING;
		e->expire_at = 0;
		e->has_expire = false;
		e->dirty = false;
		e->deleted = false;
		e->in_dirty_set = false;
		e->dirty_next = NULL;
		e->version = 1;
		e->memory_usage = 0;
		e->string_len = 0;
		MemSet(&e->value, 0, sizeof(e->value));
	}
	else if (pg_redis_entry_is_expired(e))
	{
		/* Treat as fresh insert: clear the old value. */
		pg_redis_entry_release_value(e, true);
		e->type = PG_REDIS_TYPE_STRING;
		e->version = 1;
		found = false;
	}

	if (is_new)
		*is_new = !found;
	return e;
}

bool
pg_redis_store_remove(const char *key)
{
	bool		found;
	PgRedisEntry *e;

	if (pg_redis_storage_is_shared())
		return pg_redis_shared_store_remove(key);

	ensure_store_created();
	e = (PgRedisEntry *) hash_search(PgRedisStore, key, HASH_FIND, &found);
	if (!found)
		return false;

	/* If the entry was in the dirty-set, unlink it: the slot is about to be
	 * freed and the dirty chain must not retain a dangling pointer. */
	if (e->in_dirty_set)
	{
		PgRedisEntry **pp = &dirty_head;

		while (*pp != NULL)
		{
			if (*pp == e)
			{
				*pp = e->dirty_next;
				break;
			}
			pp = &(*pp)->dirty_next;
		}
		e->in_dirty_set = false;
		e->dirty_next = NULL;
	}

	/* Record the durable tombstone *before* the entry vanishes so that we
	 * keep a valid copy of the key for the pre-commit flush. */
	pg_redis_mark_deleted(key);

	pg_redis_entry_release_value(e, true);
	hash_search(PgRedisStore, key, HASH_REMOVE, &found);
	return true;
}

Size
pg_redis_store_memory_usage(void)
{
	HASH_SEQ_STATUS s;
	PgRedisEntry *e;
	Size		total = 0;

	ensure_store_created();
	hash_seq_init(&s, PgRedisStore);
	while ((e = (PgRedisEntry *) hash_seq_search(&s)) != NULL)
	{
		total += sizeof(PgRedisEntry);
		switch (e->type)
		{
			case PG_REDIS_TYPE_STRING:
				total += e->string_len;
				break;
			case PG_REDIS_TYPE_INT:
				break;
			case PG_REDIS_TYPE_HASH:
				if (e->value.hash_value)
					total += pg_redis_hash_memory(e->value.hash_value);
				break;
			case PG_REDIS_TYPE_LIST:
				if (e->value.list_value)
					total += pg_redis_list_memory(e->value.list_value);
				break;
		}
	}
	return total;
}

int64
pg_redis_store_count(void)
{
	ensure_store_created();
	return (int64) hash_get_num_entries(PgRedisStore);
}

void
pg_redis_store_seq_init(HASH_SEQ_STATUS *status)
{
	ensure_store_created();
	hash_seq_init(status, PgRedisStore);
}

PgRedisEntry *
pg_redis_store_seq_next(HASH_SEQ_STATUS *status)
{
	return (PgRedisEntry *) hash_seq_search(status);
}

const char *
pg_redis_type_name(PgRedisValueType t)
{
	switch (t)
	{
		case PG_REDIS_TYPE_STRING:
			return "string";
		case PG_REDIS_TYPE_INT:
			return "int";
		case PG_REDIS_TYPE_HASH:
			return "hash";
		case PG_REDIS_TYPE_LIST:
			return "list";
	}
	return "unknown";
}

bool
pg_redis_type_from_name(const char *name, PgRedisValueType *out)
{
	if (name == NULL)
		return false;
	if (strcmp(name, "string") == 0)
	{
		*out = PG_REDIS_TYPE_STRING;
		return true;
	}
	if (strcmp(name, "int") == 0)
	{
		*out = PG_REDIS_TYPE_INT;
		return true;
	}
	if (strcmp(name, "hash") == 0)
	{
		*out = PG_REDIS_TYPE_HASH;
		return true;
	}
	if (strcmp(name, "list") == 0)
	{
		*out = PG_REDIS_TYPE_LIST;
		return true;
	}
	return false;
}

void
pg_redis_wrongtype(const char *key, PgRedisValueType actual,
				   PgRedisValueType expected)
{
	ereport(ERROR,
			(errcode(ERRCODE_DATATYPE_MISMATCH),
			 errmsg("pg_redis: WRONGTYPE operation against key holding the wrong kind of value"),
			 errdetail("key \"%s\" has type %s, command expects %s",
					   key,
					   pg_redis_type_name(actual),
					   pg_redis_type_name(expected))));
}

/* -------------------------------------------------------------------------
 * Dirty-set bookkeeping
 * ------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------
 * Async-mode publishers: when effective persistence mode is async_table,
 * mark_dirty/mark_deleted publish PgRedisDirtyEvent's directly into the
 * shared dirty-ring instead of accumulating a per-backend dirty-set.
 * ------------------------------------------------------------------------- */

static void
publish_async_key_upsert(PgRedisEntry *e)
{
	PgRedisDirtyEvent ev;
	bytea	   *tlv;
	const unsigned char *body;
	Size		body_len;

	if (e->type == PG_REDIS_TYPE_STRING || e->type == PG_REDIS_TYPE_INT)
	{
		tlv = pg_redis_encode_value(e);
		if (tlv == NULL)
			return;
		body = (const unsigned char *) VARDATA(tlv);
		body_len = VARSIZE(tlv) - VARHDRSZ;
		pg_redis_event_encode_key_upsert(&ev, e, body, body_len);
		pg_redis_dirty_ring_publish_blocking(&ev);
		pg_redis_persistence_note_async_publish(1);
		pfree(tlv);
	}
	else
	{
		/* hash/list parent row: NULL value */
		pg_redis_event_encode_key_upsert(&ev, e, NULL, 0);
		pg_redis_dirty_ring_publish_blocking(&ev);
		pg_redis_persistence_note_async_publish(1);
	}
}

static void
publish_async_hash_deltas(PgRedisEntry *e)
{
	PgRedisHash *h = e->value.hash_value;
	HASH_SEQ_STATUS s;
	PgRedisHashField *f;
	PgRedisHashTombstone *t,
			   *tnext;
	Size		keylen = strlen(e->key);
	PgRedisDirtyEvent ev;

	if (h == NULL)
		return;

	hash_seq_init(&s, h->fields);
	while ((f = (PgRedisHashField *) hash_seq_search(&s)) != NULL)
	{
		if (!f->dirty)
			continue;
		pg_redis_event_encode_hash_field_upsert(&ev,
												e->key, keylen,
												f->field, strlen(f->field),
												(const unsigned char *) f->value,
												f->value_len);
		pg_redis_dirty_ring_publish_blocking(&ev);
		pg_redis_persistence_note_async_publish(1);
		f->dirty = false;
	}

	for (t = h->tombstones; t != NULL; t = tnext)
	{
		tnext = t->next;
		pg_redis_event_encode_hash_field_delete(&ev,
												e->key, keylen,
												t->field, strlen(t->field));
		pg_redis_dirty_ring_publish_blocking(&ev);
		pg_redis_persistence_note_async_publish(1);
		pfree(t->field);
		pfree(t);
	}
	h->tombstones = NULL;
}

static void
publish_async_list_deltas(PgRedisEntry *e)
{
	PgRedisList *l = e->value.list_value;
	PgRedisListNode *n;
	PgRedisOrdNode *o,
			   *onext;
	Size		keylen = strlen(e->key);
	PgRedisDirtyEvent ev;

	if (l == NULL)
		return;

	for (n = l->head; n != NULL; n = n->next)
	{
		if (!n->pending_insert)
			continue;
		pg_redis_event_encode_list_item_insert(&ev,
											   e->key, keylen,
											   n->ord,
											   (const unsigned char *) n->value,
											   n->value_len);
		pg_redis_dirty_ring_publish_blocking(&ev);
		pg_redis_persistence_note_async_publish(1);
		n->pending_insert = false;
	}

	for (o = l->pending_delete_ords; o != NULL; o = onext)
	{
		onext = o->next;
		pg_redis_event_encode_list_item_delete(&ev,
											   e->key, keylen,
											   o->ord);
		pg_redis_dirty_ring_publish_blocking(&ev);
		pg_redis_persistence_note_async_publish(1);
		pfree(o);
	}
	l->pending_delete_ords = NULL;
}

void
pg_redis_mark_dirty(PgRedisEntry *e)
{
	if (e == NULL)
		return;

	if (pg_redis_storage_is_shared())
	{
		/* Shared storage: write the scratch back to the shared HTAB under
		 * the exclusive partition lock. The async event publisher (if
		 * applicable) then queues durability work for the BGW. */
		pg_redis_shared_store_writeback(e);

		if (pg_redis_effective_persistence_mode() == PG_REDIS_PERSIST_ASYNC_TABLE)
		{
			publish_async_key_upsert(e);
			if (e->type == PG_REDIS_TYPE_HASH)
				publish_async_hash_deltas(e);
			else if (e->type == PG_REDIS_TYPE_LIST)
				publish_async_list_deltas(e);
		}
		return;
	}

	if (pg_redis_effective_persistence_mode() == PG_REDIS_PERSIST_ASYNC_TABLE)
	{
		/* Session storage + async_table is a misconfiguration that
		 * pg_redis_effective_persistence_mode would normally downgrade.
		 * Guard against being reached: in case the GUCs are mid-flux, fall
		 * back to sync per-backend dirty-set. */
	}

	if (e->in_dirty_set)
		return;
	e->in_dirty_set = true;
	e->dirty_next = dirty_head;
	dirty_head = e;
	/* Keep the legacy flag in sync for any remaining callers (snapshot, etc.). */
	e->dirty = true;
}

void
pg_redis_mark_deleted(const char *key)
{
	MemoryContext old;
	PgRedisPendingDelete *node;
	Size		len;

	if (key == NULL || *key == '\0')
		return;

	if (pg_redis_effective_persistence_mode() == PG_REDIS_PERSIST_ASYNC_TABLE)
	{
		PgRedisDirtyEvent ev;

		pg_redis_event_encode_key_delete(&ev, key, strlen(key));
		pg_redis_dirty_ring_publish_blocking(&ev);
		pg_redis_persistence_note_async_publish(1);
		return;
	}

	len = strlen(key);
	old = MemoryContextSwitchTo(pg_redis_memcxt());
	node = (PgRedisPendingDelete *) palloc(sizeof(PgRedisPendingDelete));
	node->key = (char *) palloc(len + 1);
	memcpy(node->key, key, len + 1);
	node->next = pending_deletes_head;
	pending_deletes_head = node;
	MemoryContextSwitchTo(old);
}

PgRedisEntry *
pg_redis_dirty_head_take(void)
{
	PgRedisEntry *h = dirty_head;

	dirty_head = NULL;
	return h;
}

void
pg_redis_dirty_clear(void)
{
	PgRedisEntry *e = dirty_head;

	while (e != NULL)
	{
		PgRedisEntry *next = e->dirty_next;

		e->in_dirty_set = false;
		e->dirty_next = NULL;
		e = next;
	}
	dirty_head = NULL;
}

PgRedisPendingDelete *
pg_redis_pending_deletes_take(void)
{
	PgRedisPendingDelete *h = pending_deletes_head;

	pending_deletes_head = NULL;
	return h;
}

void
pg_redis_pending_deletes_clear(void)
{
	/* Nodes live in pg_redis_memcxt(); resetting the head is enough — they
	 * become unreachable garbage until the context is reset. For long-lived
	 * sessions we explicitly pfree to keep memory bounded. */
	PgRedisPendingDelete *p = pending_deletes_head;

	while (p != NULL)
	{
		PgRedisPendingDelete *next = p->next;

		pfree(p->key);
		pfree(p);
		p = next;
	}
	pending_deletes_head = NULL;
}

void
pg_redis_store_collect_stats(PgRedisStoreStats *out)
{
	HASH_SEQ_STATUS s;
	PgRedisEntry *e;

	MemSet(out, 0, sizeof(*out));
	ensure_store_created();
	hash_seq_init(&s, PgRedisStore);
	while ((e = (PgRedisEntry *) hash_seq_search(&s)) != NULL)
	{
		out->key_count++;
		if (e->has_expire)
			out->expiring_count++;
		switch (e->type)
		{
			case PG_REDIS_TYPE_STRING:
				out->string_count++;
				out->memory_usage += sizeof(PgRedisEntry) + e->string_len;
				break;
			case PG_REDIS_TYPE_INT:
				out->int_count++;
				out->memory_usage += sizeof(PgRedisEntry);
				break;
			case PG_REDIS_TYPE_HASH:
				out->hash_count++;
				out->memory_usage += sizeof(PgRedisEntry) +
					(e->value.hash_value ? pg_redis_hash_memory(e->value.hash_value) : 0);
				break;
			case PG_REDIS_TYPE_LIST:
				out->list_count++;
				out->memory_usage += sizeof(PgRedisEntry) +
					(e->value.list_value ? pg_redis_list_memory(e->value.list_value) : 0);
				break;
		}
	}
}
