#include "postgres.h"
#include "utils/dsa.h"
#include "utils/elog.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "storage/lwlock.h"
#include "datatype/timestamp.h"

#include <string.h>

#include "types.h"
#include "kv_store.h"
#include "hash_value.h"
#include "list.h"
#include "shmem.h"
#include "shared_hash.h"
#include "shared_list.h"
#include "shared_store.h"
#include "dirty_ring.h"
#include "persistence.h"
#include "utils.h"

/* -------------------------------------------------------------------------
 * Helpers: materialize PgRedisSharedEntry → PgRedisEntry (palloc'd scratch).
 * Caller holds the partition lock on entry, releases on return.
 * ------------------------------------------------------------------------- */

static PgRedisEntry *
materialize_scratch(const PgRedisSharedEntry *se)
{
	PgRedisEntry *e;

	e = (PgRedisEntry *) palloc0(sizeof(PgRedisEntry));
	strncpy(e->key, se->key, sizeof(e->key) - 1);
	e->type = se->type;
	e->expire_at = se->expire_at;
	e->has_expire = se->has_expire;
	e->version = se->version;
	e->dirty = false;
	e->deleted = false;
	e->in_dirty_set = false;
	e->dirty_next = NULL;

	switch (se->type)
	{
		case PG_REDIS_TYPE_STRING:
			{
				const char *src;
				Size		n;

				n = se->value.scalar.value_len;
				src = (const char *) pg_redis_shared_addr(se->value.scalar.dsa_value);
				if (src != NULL && n > 0)
				{
					/* CurrentMemoryContext: scratch dies with the statement. */
					char	   *dst = (char *) palloc(n + 1);

					memcpy(dst, src, n);
					dst[n] = '\0';
					e->value.string_value = dst;
					e->string_len = n;
				}
				else
				{
					e->value.string_value = NULL;
					e->string_len = 0;
				}
				break;
			}
		case PG_REDIS_TYPE_INT:
			e->value.int_value = se->value.scalar.int_value;
			break;
		case PG_REDIS_TYPE_HASH:
			e->value.hash_value =
				pg_redis_shared_hash_materialize(se->value.hash.table);
			break;
		case PG_REDIS_TYPE_LIST:
			{
				PgRedisSharedListMeta meta = {
					.head = se->value.list.head,
					.tail = se->value.list.tail,
					.length = se->value.list.length,
					.min_ord = se->value.list.min_ord,
					.max_ord = se->value.list.max_ord,
					.ord_initialized = se->value.list.ord_initialized,
				};

				e->value.list_value = pg_redis_shared_list_materialize(&meta);
				break;
			}
	}
	return e;
}

/* -------------------------------------------------------------------------
 * Writeback: PgRedisEntry (scratch) → PgRedisSharedEntry.
 *
 * Frees any pre-existing DSA payloads on the shared entry, then allocates
 * new DSA chunks from the scratch's local state. Caller holds LW_EXCLUSIVE.
 * ------------------------------------------------------------------------- */

void
pg_redis_shared_entry_release_payloads(PgRedisSharedEntry *se)
{
	switch (se->type)
	{
		case PG_REDIS_TYPE_STRING:
			pg_redis_shared_pfree(se->value.scalar.dsa_value);
			se->value.scalar.dsa_value = InvalidDsaPointer;
			se->value.scalar.value_len = 0;
			break;
		case PG_REDIS_TYPE_INT:
			se->value.scalar.int_value = 0;
			break;
		case PG_REDIS_TYPE_HASH:
			pg_redis_shared_hash_free(se->value.hash.table);
			se->value.hash.table = InvalidDsaPointer;
			se->value.hash.field_count = 0;
			break;
		case PG_REDIS_TYPE_LIST:
			{
				PgRedisSharedListMeta meta = {
					.head = se->value.list.head,
					.tail = se->value.list.tail,
					.length = se->value.list.length,
					.min_ord = se->value.list.min_ord,
					.max_ord = se->value.list.max_ord,
					.ord_initialized = se->value.list.ord_initialized,
				};

				pg_redis_shared_list_free(&meta);
				se->value.list.head = InvalidDsaPointer;
				se->value.list.tail = InvalidDsaPointer;
				se->value.list.length = 0;
				se->value.list.min_ord = 0;
				se->value.list.max_ord = -1;
				se->value.list.ord_initialized = false;
				break;
			}
	}
}

static void
populate_shared_from_scratch(PgRedisSharedEntry *se, const PgRedisEntry *e)
{
	se->type = e->type;
	se->expire_at = e->expire_at;
	se->has_expire = e->has_expire;
	se->version = e->version;

	switch (e->type)
	{
		case PG_REDIS_TYPE_STRING:
			{
				Size		n = e->string_len;
				dsa_pointer p = (n > 0) ? pg_redis_shared_palloc(n)
				: InvalidDsaPointer;

				if (n > 0 && p == InvalidDsaPointer)
					ereport(ERROR,
							(errcode(ERRCODE_OUT_OF_MEMORY),
							 errmsg("pg_redis: out of shared memory in SET writeback"),
							 errhint("Raise pg_redis.shared_max_memory.")));
				if (n > 0)
					memcpy(pg_redis_shared_addr(p), e->value.string_value, n);
				se->value.scalar.dsa_value = p;
				se->value.scalar.value_len = n;
				se->value.scalar.int_value = 0;
				break;
			}
		case PG_REDIS_TYPE_INT:
			se->value.scalar.int_value = e->value.int_value;
			se->value.scalar.dsa_value = InvalidDsaPointer;
			se->value.scalar.value_len = 0;
			break;
		case PG_REDIS_TYPE_HASH:
			{
				PgRedisHash *h = e->value.hash_value;
				dsa_pointer table_dsa = InvalidDsaPointer;
				HASH_SEQ_STATUS s;
				PgRedisHashField *f;
				int64		count = 0;

				if (h != NULL && h->fields != NULL)
				{
					hash_seq_init(&s, h->fields);
					while ((f = (PgRedisHashField *) hash_seq_search(&s)) != NULL)
					{
						bool		was_new;

						pg_redis_shared_hash_set(&table_dsa,
												 f->field, strlen(f->field),
												 (const unsigned char *) f->value,
												 f->value_len,
												 &was_new);
						if (was_new)
							count++;
					}
				}
				se->value.hash.table = table_dsa;
				se->value.hash.field_count = count;
				break;
			}
		case PG_REDIS_TYPE_LIST:
			{
				PgRedisList *l = e->value.list_value;
				PgRedisSharedListMeta meta = {
					.head = InvalidDsaPointer,
					.tail = InvalidDsaPointer,
					.length = 0,
					.min_ord = 0,
					.max_ord = -1,
					.ord_initialized = false,
				};
				PgRedisListNode *n;

				if (l != NULL)
				{
					for (n = l->head; n != NULL; n = n->next)
						(void) pg_redis_shared_list_rpush(&meta,
														  (const unsigned char *) n->value,
														  n->value_len);
				}
				se->value.list.head = meta.head;
				se->value.list.tail = meta.tail;
				se->value.list.length = meta.length;
				se->value.list.min_ord = meta.min_ord;
				se->value.list.max_ord = meta.max_ord;
				se->value.list.ord_initialized = meta.ord_initialized;
				break;
			}
	}
}

/* -------------------------------------------------------------------------
 * Public CRUD
 * ------------------------------------------------------------------------- */

PgRedisEntry *
pg_redis_shared_store_lookup_raw(const char *key)
{
	HTAB	   *ks = pg_redis_shmem_keyspace();
	LWLock	   *plock = pg_redis_partition_lock(key, strlen(key));
	bool		found;
	PgRedisSharedEntry *se;
	PgRedisEntry *scratch = NULL;

	if (ks == NULL || plock == NULL)
		return NULL;

	LWLockAcquire(plock, LW_SHARED);
	se = (PgRedisSharedEntry *) hash_search(ks, key, HASH_FIND, &found);
	if (found)
		scratch = materialize_scratch(se);
	LWLockRelease(plock);

	return scratch;
}

PgRedisFastHashOutcome
pg_redis_shared_hash_lookup_field(const char *key, Size keylen,
								  const char *field, Size fieldlen,
								  bool want_value,
								  unsigned char **out_value, Size *out_value_len,
								  PgRedisValueType *out_actual)
{
	HTAB	   *ks = pg_redis_shmem_keyspace();
	LWLock	   *plock = pg_redis_partition_lock(key, keylen);
	bool		found;
	PgRedisSharedEntry *se;
	PgRedisFastHashOutcome outcome;
	dsa_pointer table_dsa = InvalidDsaPointer;
	bool		field_hit = false;

	if (out_value)
		*out_value = NULL;
	if (out_value_len)
		*out_value_len = 0;

	if (ks == NULL || plock == NULL)
		return PG_REDIS_FAST_HASH_KEY_MISS;

	LWLockAcquire(plock, LW_SHARED);
	se = (PgRedisSharedEntry *) hash_search(ks, key, HASH_FIND, &found);
	if (!found)
	{
		LWLockRelease(plock);
		return PG_REDIS_FAST_HASH_KEY_MISS;
	}
	if (se->has_expire && GetCurrentTimestamp() >= se->expire_at)
	{
		LWLockRelease(plock);
		return PG_REDIS_FAST_HASH_KEY_EXPIRED;
	}
	if (se->type != PG_REDIS_TYPE_HASH)
	{
		if (out_actual)
			*out_actual = se->type;
		LWLockRelease(plock);
		return PG_REDIS_FAST_HASH_WRONGTYPE;
	}

	table_dsa = se->value.hash.table;
	if (want_value)
		field_hit = pg_redis_shared_hash_get(table_dsa, field, fieldlen,
											 out_value, out_value_len);
	else
		field_hit = pg_redis_shared_hash_exists(table_dsa, field, fieldlen);
	outcome = field_hit ? PG_REDIS_FAST_HASH_HIT : PG_REDIS_FAST_HASH_FIELD_MISS;
	LWLockRelease(plock);
	return outcome;
}

PgRedisEntry *
pg_redis_shared_store_lookup(const char *key, bool *expired)
{
	PgRedisEntry *e;

	if (expired)
		*expired = false;

	e = pg_redis_shared_store_lookup_raw(key);
	if (e == NULL)
		return NULL;

	if (e->has_expire && GetCurrentTimestamp() >= e->expire_at)
	{
		/* Lazy eviction: drop the durable row under exclusive lock. */
		(void) pg_redis_shared_store_remove(key);
		if (expired)
			*expired = true;
		/* Scratch is local; just free it. */
		pfree(e);
		return NULL;
	}
	return e;
}

PgRedisEntry *
pg_redis_shared_store_upsert(const char *key, bool *is_new)
{
	HTAB	   *ks = pg_redis_shmem_keyspace();
	LWLock	   *plock = pg_redis_partition_lock(key, strlen(key));
	bool		found;
	PgRedisSharedEntry *se;
	PgRedisEntry *scratch;

	if (ks == NULL || plock == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_redis: shared keyspace not initialized")));

	LWLockAcquire(plock, LW_EXCLUSIVE);
	se = (PgRedisSharedEntry *) hash_search(ks, key, HASH_ENTER, &found);

	if (!found)
	{
		/* Initialize new entry. */
		MemSet(&se->value, 0, sizeof(se->value));
		se->type = PG_REDIS_TYPE_STRING;
		se->expire_at = 0;
		se->has_expire = false;
		se->version = 1;
		se->value.scalar.dsa_value = InvalidDsaPointer;
		se->value.hash.table = InvalidDsaPointer;
		se->value.list.head = InvalidDsaPointer;
		se->value.list.tail = InvalidDsaPointer;
		se->value.list.min_ord = 0;
		se->value.list.max_ord = -1;
		se->value.list.ord_initialized = false;
	}
	else if (se->has_expire && GetCurrentTimestamp() >= se->expire_at)
	{
		/* Treat expired entry as fresh — clear payloads, reset to default. */
		pg_redis_shared_entry_release_payloads(se);
		se->type = PG_REDIS_TYPE_STRING;
		se->expire_at = 0;
		se->has_expire = false;
		se->version = 1;
		found = false;
	}

	scratch = materialize_scratch(se);
	LWLockRelease(plock);

	if (is_new)
		*is_new = !found;
	return scratch;
}

int
pg_redis_shared_store_reset_all(void)
{
	HTAB	   *ks = pg_redis_shmem_keyspace();
	HASH_SEQ_STATUS s;
	PgRedisSharedEntry *se;
	int			removed = 0;
	char	  **keys_to_remove;
	int			capacity = 64;
	int			n = 0;
	int			i;
	bool		found;

	if (ks == NULL)
		return 0;

	/* Two-pass: collect keys first, then remove. hash_seq_search + HASH_REMOVE
	 * in the same loop is not safe per hsearch.h. */
	keys_to_remove = (char **) palloc(sizeof(char *) * capacity);
	hash_seq_init(&s, ks);
	while ((se = (PgRedisSharedEntry *) hash_seq_search(&s)) != NULL)
	{
		if (n == capacity)
		{
			capacity *= 2;
			keys_to_remove = (char **) repalloc(keys_to_remove,
												sizeof(char *) * capacity);
		}
		keys_to_remove[n++] = pstrdup(se->key);
		pg_redis_shared_entry_release_payloads(se);
	}

	for (i = 0; i < n; i++)
	{
		hash_search(ks, keys_to_remove[i], HASH_REMOVE, &found);
		if (found)
			removed++;
		pfree(keys_to_remove[i]);
	}
	pfree(keys_to_remove);

	return removed;
}

bool
pg_redis_shared_store_remove(const char *key)
{
	HTAB	   *ks = pg_redis_shmem_keyspace();
	LWLock	   *plock = pg_redis_partition_lock(key, strlen(key));
	bool		found;
	PgRedisSharedEntry *se;

	if (ks == NULL || plock == NULL)
		return false;

	LWLockAcquire(plock, LW_EXCLUSIVE);
	se = (PgRedisSharedEntry *) hash_search(ks, key, HASH_FIND, &found);
	if (found)
	{
		pg_redis_shared_entry_release_payloads(se);
		hash_search(ks, key, HASH_REMOVE, &found);
	}
	LWLockRelease(plock);

	return found;
}

/* Apply only dirty fields and tombstones from `h` to the shared entry's
 * existing DSA chain — avoids the O(N) release+rebuild that would otherwise
 * fire on every HSET/HDEL of a multi-field hash. The scratch's clean fields
 * are left exactly as they sit in DSA. */
static void
surgical_apply_hash(PgRedisSharedEntry *se, PgRedisHash *h)
{
	HASH_SEQ_STATUS s;
	PgRedisHashField *f;
	PgRedisHashTombstone *t;
	dsa_pointer table_dsa = se->value.hash.table;
	int64		count = se->value.hash.field_count;

	if (h != NULL && h->fields != NULL)
	{
		hash_seq_init(&s, h->fields);
		while ((f = (PgRedisHashField *) hash_seq_search(&s)) != NULL)
		{
			bool		was_new;

			if (!f->dirty)
				continue;
			pg_redis_shared_hash_set(&table_dsa,
									 f->field, strlen(f->field),
									 (const unsigned char *) f->value,
									 f->value_len,
									 &was_new);
			if (was_new)
				count++;
		}
	}

	if (h != NULL)
	{
		for (t = h->tombstones; t != NULL; t = t->next)
		{
			if (pg_redis_shared_hash_del(&table_dsa, t->field, strlen(t->field)))
				count--;
		}
	}

	se->value.hash.table = table_dsa;
	se->value.hash.field_count = count;
}

void
pg_redis_shared_store_writeback(PgRedisEntry *scratch)
{
	HTAB	   *ks;
	LWLock	   *plock;
	bool		found;
	PgRedisSharedEntry *se;

	if (scratch == NULL)
		return;
	ks = pg_redis_shmem_keyspace();
	plock = pg_redis_partition_lock(scratch->key, strlen(scratch->key));
	if (ks == NULL || plock == NULL)
		return;

	LWLockAcquire(plock, LW_EXCLUSIVE);
	se = (PgRedisSharedEntry *) hash_search(ks, scratch->key, HASH_ENTER, &found);

	/* Decide between surgical (cheap, type matches) and full rebuild. The
	 * surgical path is only safe when an existing entry of the same type is
	 * being mutated — type changes (SET on a key that previously held a
	 * hash) still need release+rebuild so we drop the stale DSA chunks. */
	if (!found)
	{
		MemSet(&se->value, 0, sizeof(se->value));
		se->value.scalar.dsa_value = InvalidDsaPointer;
		se->value.hash.table = InvalidDsaPointer;
		se->value.list.head = InvalidDsaPointer;
		se->value.list.tail = InvalidDsaPointer;
		populate_shared_from_scratch(se, scratch);
	}
	else if (se->type != scratch->type)
	{
		pg_redis_shared_entry_release_payloads(se);
		populate_shared_from_scratch(se, scratch);
	}
	else if (scratch->type == PG_REDIS_TYPE_HASH)
	{
		/* In-place surgical update — see surgical_apply_hash. */
		se->expire_at = scratch->expire_at;
		se->has_expire = scratch->has_expire;
		se->version = scratch->version;
		surgical_apply_hash(se, scratch->value.hash_value);
		LWLockRelease(plock);
		return;
	}
	else
	{
		/* STRING/INT/LIST: the cheap path isn't implemented yet, fall back
		 * to release+rebuild. Strings/ints are single-payload so the cost is
		 * O(1); lists are O(N) but mutations are rarer than the hash hot
		 * path that triggers production OOMs. */
		pg_redis_shared_entry_release_payloads(se);
		populate_shared_from_scratch(se, scratch);
	}

	LWLockRelease(plock);
}

/* -------------------------------------------------------------------------
 * Atomic HSET / HDEL helpers — bypass scratch materialization and operate
 * directly on the DSA hash table under LW_EXCLUSIVE. Each one performs the
 * full mutation (type check, eviction, mutate, persistence event) in a
 * single lock acquisition.
 * ------------------------------------------------------------------------- */

/* Build a transient stack-allocated PgRedisEntry that carries the metadata
 * (key, type, expire, version) `pg_redis_event_encode_key_upsert` needs. */
static void
fill_event_entry(PgRedisEntry *out,
				 const PgRedisSharedEntry *se,
				 const char *key, Size keylen)
{
	memset(out, 0, sizeof(*out));
	if (keylen > PG_REDIS_MAX_KEY_SIZE)
		keylen = PG_REDIS_MAX_KEY_SIZE;
	memcpy(out->key, key, keylen);
	out->key[keylen] = '\0';
	out->type = se->type;
	out->expire_at = se->expire_at;
	out->has_expire = se->has_expire;
	out->version = se->version;
}

PgRedisFastHashMutateOutcome
pg_redis_shared_hash_set_atomic(const char *key, Size keylen,
								const char *field, Size fieldlen,
								const unsigned char *value, Size value_len,
								PgRedisValueType *out_actual)
{
	HTAB	   *ks = pg_redis_shmem_keyspace();
	LWLock	   *plock = pg_redis_partition_lock(key, keylen);
	bool		found;
	PgRedisSharedEntry *se;
	bool		was_new = false;

	if (ks == NULL || plock == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_redis: shared keyspace not initialized")));

	LWLockAcquire(plock, LW_EXCLUSIVE);
	se = (PgRedisSharedEntry *) hash_search(ks, key, HASH_ENTER, &found);

	if (!found)
	{
		/* New entry — init as HASH directly. */
		MemSet(&se->value, 0, sizeof(se->value));
		se->expire_at = 0;
		se->has_expire = false;
		se->version = 0;
		se->type = PG_REDIS_TYPE_HASH;
		se->value.scalar.dsa_value = InvalidDsaPointer;
		se->value.hash.table = InvalidDsaPointer;
		se->value.hash.field_count = 0;
		se->value.list.head = InvalidDsaPointer;
		se->value.list.tail = InvalidDsaPointer;
	}
	else if (se->has_expire && GetCurrentTimestamp() >= se->expire_at)
	{
		/* TTL-expired — treat as fresh insert. */
		pg_redis_shared_entry_release_payloads(se);
		MemSet(&se->value, 0, sizeof(se->value));
		se->expire_at = 0;
		se->has_expire = false;
		se->version = 0;
		se->type = PG_REDIS_TYPE_HASH;
		se->value.scalar.dsa_value = InvalidDsaPointer;
		se->value.hash.table = InvalidDsaPointer;
		se->value.hash.field_count = 0;
		se->value.list.head = InvalidDsaPointer;
		se->value.list.tail = InvalidDsaPointer;
	}
	else if (se->type != PG_REDIS_TYPE_HASH)
	{
		PgRedisValueType actual = se->type;

		if (out_actual)
			*out_actual = actual;
		LWLockRelease(plock);
		return PG_REDIS_FAST_HASH_MUTATE_WRONGTYPE;
	}

	pg_redis_shared_hash_set(&se->value.hash.table,
							 field, fieldlen,
							 value, value_len,
							 &was_new);
	if (was_new)
		se->value.hash.field_count++;
	se->version++;

	if (pg_redis_effective_persistence_mode() == PG_REDIS_PERSIST_ASYNC_TABLE)
	{
		PgRedisEntry stub;
		PgRedisDirtyEvent ev;

		fill_event_entry(&stub, se, key, keylen);
		pg_redis_event_encode_key_upsert(&ev, &stub, NULL, 0);
		pg_redis_dirty_ring_publish(&ev);
		pg_redis_event_encode_hash_field_upsert(&ev, key, keylen,
												field, fieldlen,
												value, value_len);
		pg_redis_dirty_ring_publish(&ev);
		pg_redis_persistence_note_async_publish(2);
	}

	LWLockRelease(plock);
	return was_new ? PG_REDIS_FAST_HASH_MUTATE_OK_NEW
		: PG_REDIS_FAST_HASH_MUTATE_OK_OVERWRITE;
}

PgRedisFastHashMutateOutcome
pg_redis_shared_hash_del_atomic(const char *key, Size keylen,
								const char *field, Size fieldlen,
								PgRedisValueType *out_actual)
{
	HTAB	   *ks = pg_redis_shmem_keyspace();
	LWLock	   *plock = pg_redis_partition_lock(key, keylen);
	bool		found;
	PgRedisSharedEntry *se;
	bool		removed;

	if (ks == NULL || plock == NULL)
		return PG_REDIS_FAST_HASH_MUTATE_NOOP;

	LWLockAcquire(plock, LW_EXCLUSIVE);
	se = (PgRedisSharedEntry *) hash_search(ks, key, HASH_FIND, &found);
	if (!found)
	{
		LWLockRelease(plock);
		return PG_REDIS_FAST_HASH_MUTATE_NOOP;
	}
	if (se->has_expire && GetCurrentTimestamp() >= se->expire_at)
	{
		/* Drop the expired entry under the lock we already hold. */
		pg_redis_shared_entry_release_payloads(se);
		hash_search(ks, key, HASH_REMOVE, &found);
		LWLockRelease(plock);
		return PG_REDIS_FAST_HASH_MUTATE_NOOP;
	}
	if (se->type != PG_REDIS_TYPE_HASH)
	{
		PgRedisValueType actual = se->type;

		if (out_actual)
			*out_actual = actual;
		LWLockRelease(plock);
		return PG_REDIS_FAST_HASH_MUTATE_WRONGTYPE;
	}

	removed = pg_redis_shared_hash_del(&se->value.hash.table, field, fieldlen);
	if (!removed)
	{
		LWLockRelease(plock);
		return PG_REDIS_FAST_HASH_MUTATE_NOOP;
	}
	se->value.hash.field_count--;
	se->version++;

	if (pg_redis_effective_persistence_mode() == PG_REDIS_PERSIST_ASYNC_TABLE)
	{
		PgRedisDirtyEvent ev;

		pg_redis_event_encode_hash_field_delete(&ev, key, keylen, field, fieldlen);
		pg_redis_dirty_ring_publish(&ev);
		pg_redis_persistence_note_async_publish(1);
	}

	LWLockRelease(plock);
	return PG_REDIS_FAST_HASH_MUTATE_OK_DELETED;
}
