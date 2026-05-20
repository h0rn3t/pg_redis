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
				pg_redis_shared_hash_materialize(se->value.hash.fields_head);
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
			pg_redis_shared_hash_free(se->value.hash.fields_head);
			se->value.hash.fields_head = InvalidDsaPointer;
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
				dsa_pointer head = InvalidDsaPointer;
				HASH_SEQ_STATUS s;
				PgRedisHashField *f;
				int64		count = 0;

				if (h != NULL && h->fields != NULL)
				{
					hash_seq_init(&s, h->fields);
					while ((f = (PgRedisHashField *) hash_seq_search(&s)) != NULL)
					{
						bool		was_new;

						pg_redis_shared_hash_set(&head,
												 f->field, strlen(f->field),
												 (const unsigned char *) f->value,
												 f->value_len,
												 &was_new);
						if (was_new)
							count++;
					}
				}
				se->value.hash.fields_head = head;
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
		se->value.hash.fields_head = InvalidDsaPointer;
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
	dsa_pointer head = se->value.hash.fields_head;
	int64		count = se->value.hash.field_count;

	if (h != NULL && h->fields != NULL)
	{
		hash_seq_init(&s, h->fields);
		while ((f = (PgRedisHashField *) hash_seq_search(&s)) != NULL)
		{
			bool		was_new;

			if (!f->dirty)
				continue;
			pg_redis_shared_hash_set(&head,
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
			if (pg_redis_shared_hash_del(&head, t->field, strlen(t->field)))
				count--;
		}
	}

	se->value.hash.fields_head = head;
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
		se->value.hash.fields_head = InvalidDsaPointer;
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
