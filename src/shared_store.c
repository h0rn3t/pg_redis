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
				elog(LOG, "pg_redis: materialize STRING n=%zu dsa_value=%lu",
					 (size_t) n, (unsigned long) se->value.scalar.dsa_value);
				src = (const char *) pg_redis_shared_addr(se->value.scalar.dsa_value);
				elog(LOG, "pg_redis: materialize STRING shared_addr returned src=%p", (void *) src);
				if (src != NULL && n > 0)
				{
					e->value.string_value = pg_redis_palloc_string(src, n);
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

	elog(LOG, "pg_redis: shared_lookup_raw key=\"%s\" ks=%p plock=%p",
		 key ? key : "(null)", (void *) ks, (void *) plock);

	if (ks == NULL || plock == NULL)
		return NULL;

	LWLockAcquire(plock, LW_SHARED);
	se = (PgRedisSharedEntry *) hash_search(ks, key, HASH_FIND, &found);
	elog(LOG, "pg_redis: shared_lookup_raw hash_search returned se=%p found=%d",
		 (void *) se, (int) found);
	if (found)
	{
		elog(LOG, "pg_redis: materialize_scratch se->type=%d se->value.scalar.dsa_value=%lu se->value.scalar.value_len=%zu",
			 (int) se->type,
			 (unsigned long) se->value.scalar.dsa_value,
			 (size_t) se->value.scalar.value_len);
		scratch = materialize_scratch(se);
		elog(LOG, "pg_redis: materialize_scratch returned scratch=%p", (void *) scratch);
	}
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

void
pg_redis_shared_store_writeback(PgRedisEntry *scratch)
{
	HTAB	   *ks;
	LWLock	   *plock;
	bool		found;
	PgRedisSharedEntry *se;

	if (scratch == NULL)
		return;
	elog(LOG, "pg_redis: writeback key=\"%s\" type=%d string_len=%zu",
		 scratch->key, (int) scratch->type, (size_t) scratch->string_len);
	ks = pg_redis_shmem_keyspace();
	plock = pg_redis_partition_lock(scratch->key, strlen(scratch->key));
	if (ks == NULL || plock == NULL)
		return;

	LWLockAcquire(plock, LW_EXCLUSIVE);
	se = (PgRedisSharedEntry *) hash_search(ks, scratch->key, HASH_ENTER, &found);
	if (!found)
	{
		MemSet(&se->value, 0, sizeof(se->value));
		se->value.scalar.dsa_value = InvalidDsaPointer;
		se->value.hash.fields_head = InvalidDsaPointer;
		se->value.list.head = InvalidDsaPointer;
		se->value.list.tail = InvalidDsaPointer;
	}
	else
	{
		/* Free old payloads before installing new ones. If the type changed
		 * (e.g. SET on a key previously holding a hash) the old DSA chunks
		 * need to go regardless. */
		pg_redis_shared_entry_release_payloads(se);
	}

	populate_shared_from_scratch(se, scratch);
	LWLockRelease(plock);
}
