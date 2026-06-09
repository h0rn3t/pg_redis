#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"
#include "utils/hsearch.h"

#include <string.h>

#include "types.h"
#include "binval.h"
#include "kv_store.h"
#include "list.h"
#include "hash_value.h"
#include "persistence.h"
#include "ttl.h"
#include "utils.h"
#include "dirty_ring.h"
#include "shmem.h"
#include "shared_store.h"

/* Per-backend "we've loaded from store at least once" flag. */
static bool persistence_loaded = false;
static bool persistence_initialized = false;

/* Per-backend counter of async dirty-ring events published in the current
 * transaction. Bumped by mark_dirty/mark_deleted in async mode; consulted by
 * the xact callback on ABORT to warn that the writes have already been ack'd
 * to the BGW (the ROLLBACK does not undo them). Reset at PRE_COMMIT and
 * ABORT. */
static int async_published_in_xact = 0;

void
pg_redis_persistence_note_async_publish(int n)
{
	if (n > 0)
		async_published_in_xact += n;
}

/* Cached SPI plans (per-backend). Prepared lazily on first use with
 * SPI_keepplan so they survive SPI_finish. All plans take array parameters
 * and are invoked via unnest() so a single statement covers an entire batch. */
static SPIPlanPtr plan_store_upsert = NULL;
static SPIPlanPtr plan_store_delete = NULL;
static SPIPlanPtr plan_hash_field_upsert = NULL;
static SPIPlanPtr plan_hash_field_delete = NULL;
static SPIPlanPtr plan_list_item_insert = NULL;
static SPIPlanPtr plan_list_item_delete = NULL;

/*
 * A drain batch can contain multiple events for the same key (e.g. warmup
 * SET followed by measurement SET, or rapid INCR/SET on a counter). Postgres
 * rejects "ON CONFLICT DO UPDATE" if two input rows share the conflict key,
 * so dedupe in the SELECT keeping the last event by ordinality.
 */
static const char *SQL_STORE_UPSERT =
	"INSERT INTO pgredis.store (key, type, value, expire_at, version, updated_at) "
	"SELECT DISTINCT ON (k) k, t, v, ea, ver, now() "
	"  FROM unnest($1::text[], $2::text[], $3::bytea[], $4::timestamptz[], $5::bigint[]) "
	"       WITH ORDINALITY AS u(k, t, v, ea, ver, i) "
	" ORDER BY k, i DESC "
	"ON CONFLICT (key) DO UPDATE SET "
	"  type = EXCLUDED.type, "
	"  value = EXCLUDED.value, "
	"  expire_at = EXCLUDED.expire_at, "
	"  version = EXCLUDED.version, "
	"  updated_at = now()";

static const char *SQL_STORE_DELETE =
	"DELETE FROM pgredis.store WHERE key = ANY($1::text[])";

static const char *SQL_HASH_FIELD_UPSERT =
	"INSERT INTO pgredis.hash_fields (key, field, value) "
	"SELECT DISTINCT ON (k, f) k, f, v "
	"  FROM unnest($1::text[], $2::text[], $3::bytea[]) "
	"       WITH ORDINALITY AS u(k, f, v, i) "
	" ORDER BY k, f, i DESC "
	"ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value";

static const char *SQL_HASH_FIELD_DELETE =
	"DELETE FROM pgredis.hash_fields h "
	"USING unnest($1::text[], $2::text[]) AS u(k, f) "
	"WHERE h.key = u.k AND h.field = u.f";

static const char *SQL_LIST_ITEM_INSERT =
	"INSERT INTO pgredis.list_items (key, ord, value) "
	"SELECT k, o, v FROM unnest($1::text[], $2::bigint[], $3::bytea[]) AS u(k, o, v)";

static const char *SQL_LIST_ITEM_DELETE =
	"DELETE FROM pgredis.list_items l "
	"USING unnest($1::text[], $2::bigint[]) AS u(k, o) "
	"WHERE l.key = u.k AND l.ord = u.o";

static void pg_redis_xact_cb_persistence(XactEvent event, void *arg);
static void serialize_entry_value_json(StringInfo out, PgRedisEntry *e);
static bool persistence_writes_enabled(void);
static void run_flush(void);

/* Prepare and pin a plan if not already cached. */
static SPIPlanPtr
prepare_plan_if_needed(SPIPlanPtr *slot, const char *sql,
					   int nargs, Oid *argtypes)
{
	SPIPlanPtr	plan;

	if (*slot != NULL)
		return *slot;

	plan = SPI_prepare(sql, nargs, argtypes);
	if (plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_redis: SPI_prepare failed: %s",
						SPI_result_code_string(SPI_result))));

	if (SPI_keepplan(plan) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_redis: SPI_keepplan failed")));

	*slot = plan;
	return plan;
}

/* -------------------------------------------------------------------------
 * Initialization
 * ------------------------------------------------------------------------- */

void
pg_redis_persistence_init(void)
{
	if (persistence_initialized)
		return;
	RegisterXactCallback(pg_redis_xact_cb_persistence, NULL);
	persistence_initialized = true;
}

static void
pg_redis_xact_cb_persistence(XactEvent event, void *arg)
{
	PgRedisPersistenceMode mode = pg_redis_effective_persistence_mode();

	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
			if (mode == PG_REDIS_PERSIST_ASYNC_TABLE)
			{
				/* Async mode: events were published into the shared dirty-
				 * ring at command time. The BGW persists them. The
				 * pre-commit callback only clears any stray per-backend
				 * bookkeeping that might have leaked in from a sync path. */
				pg_redis_dirty_clear();
				pg_redis_pending_deletes_clear();
				async_published_in_xact = 0;
				break;
			}
			if (mode == PG_REDIS_PERSIST_SYNC_TABLE ||
				mode == PG_REDIS_PERSIST_ASYNC_TABLE)
			{
				run_flush();
			}
			else
			{
				/* Mode is 'none' (or snapshot/aof): drop tracking without
				 * issuing SPI. */
				pg_redis_dirty_clear();
				pg_redis_pending_deletes_clear();
			}
			break;

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			/* No SPI on abort: just drop in-memory tracking. */
			pg_redis_dirty_clear();
			pg_redis_pending_deletes_clear();
			if (mode == PG_REDIS_PERSIST_ASYNC_TABLE)
			{
				/* Shared keyspace is authoritative — in-memory writes
				 * acked before the abort remain visible to other backends.
				 * Do NOT invalidate the load flag (no re-load needed). */
				if (async_published_in_xact > 0)
					ereport(WARNING,
							(errcode(ERRCODE_WARNING),
							 errmsg("pg_redis: %d async event(s) already published to dirty-ring will NOT be rolled back",
									async_published_in_xact),
							 errhint("In async_table mode, mutations are durable independently of the user's transaction; ROLLBACK does not undo them.")));
				async_published_in_xact = 0;
				break;
			}
			async_published_in_xact = 0;
			/* Force a re-load next time so the in-memory view re-syncs with
			 * the (rolled-back) durable state. */
			persistence_loaded = false;
			break;

		default:
			break;
	}
}

void
pg_redis_persistence_invalidate_load(void)
{
	persistence_loaded = false;
}

static bool
persistence_writes_enabled(void)
{
	PgRedisPersistenceMode m = pg_redis_resolve_persistence_mode();

	return m == PG_REDIS_PERSIST_SYNC_TABLE ||
		m == PG_REDIS_PERSIST_ASYNC_TABLE; /* async falls back to sync in v0.1 */
}

/* -------------------------------------------------------------------------
 * Datum vectors — growable arrays used to assemble unnest() parameters.
 * ------------------------------------------------------------------------- */

typedef struct DatumVec
{
	Datum	   *vals;
	bool	   *nulls;
	int			n;
	int			cap;
} DatumVec;

static void
vec_init(DatumVec *v)
{
	v->cap = 16;
	v->n = 0;
	v->vals = (Datum *) palloc(v->cap * sizeof(Datum));
	v->nulls = (bool *) palloc(v->cap * sizeof(bool));
}

static void
vec_grow(DatumVec *v)
{
	v->cap *= 2;
	v->vals = (Datum *) repalloc(v->vals, v->cap * sizeof(Datum));
	v->nulls = (bool *) repalloc(v->nulls, v->cap * sizeof(bool));
}

static void
vec_push(DatumVec *v, Datum d, bool isnull)
{
	if (v->n == v->cap)
		vec_grow(v);
	v->vals[v->n] = d;
	v->nulls[v->n] = isnull;
	v->n++;
}

static ArrayType *
vec_to_array(DatumVec *v, Oid elmtype, int elmlen, bool elmbyval, char elmalign)
{
	int			dims[1];
	int			lbs[1] = {1};

	dims[0] = v->n;
	return construct_md_array(v->vals, v->nulls, 1, dims, lbs,
							  elmtype, elmlen, elmbyval, elmalign);
}

static ArrayType *
vec_to_text_array(DatumVec *v)
{
	return vec_to_array(v, TEXTOID, -1, false, TYPALIGN_INT);
}

static ArrayType *
vec_to_bytea_array(DatumVec *v)
{
	return vec_to_array(v, BYTEAOID, -1, false, TYPALIGN_INT);
}

static ArrayType *
vec_to_int8_array(DatumVec *v)
{
	return vec_to_array(v, INT8OID, 8, FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);
}

static ArrayType *
vec_to_timestamptz_array(DatumVec *v)
{
	return vec_to_array(v, TIMESTAMPTZOID, 8, FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);
}

/* -------------------------------------------------------------------------
 * Pre-commit batched flush
 * ------------------------------------------------------------------------- */

static void
run_flush(void)
{
	PgRedisEntry *dirty;
	PgRedisPendingDelete *deletes;
	DatumVec	store_keys,
				store_types,
				store_values,
				store_expires,
				store_versions;
	DatumVec	hf_up_keys,
				hf_up_fields,
				hf_up_values;
	DatumVec	hf_del_keys,
				hf_del_fields;
	DatumVec	li_ins_keys,
				li_ins_ords,
				li_ins_values;
	DatumVec	li_del_keys,
				li_del_ords;
	DatumVec	store_delete_keys;
	bool		any_work;
	PgRedisEntry *e;

	dirty = pg_redis_dirty_head_take();
	deletes = pg_redis_pending_deletes_take();
	any_work = (dirty != NULL) || (deletes != NULL);
	if (!any_work)
		return;

	/* XACT_EVENT_PRE_COMMIT runs after the active snapshot has been popped,
	 * so SPI_execute_plan would fail with "cannot execute SQL without an
	 * outer snapshot or portal". Push a fresh transaction snapshot for the
	 * duration of the flush, then pop it. */
	PushActiveSnapshot(GetTransactionSnapshot());

	if (pg_redis_spi_connect() != SPI_OK_CONNECT)
	{
		PopActiveSnapshot();
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_redis: SPI_connect failed in flush")));
	}

	vec_init(&store_keys);
	vec_init(&store_types);
	vec_init(&store_values);
	vec_init(&store_expires);
	vec_init(&store_versions);
	vec_init(&hf_up_keys);
	vec_init(&hf_up_fields);
	vec_init(&hf_up_values);
	vec_init(&hf_del_keys);
	vec_init(&hf_del_fields);
	vec_init(&li_ins_keys);
	vec_init(&li_ins_ords);
	vec_init(&li_ins_values);
	vec_init(&li_del_keys);
	vec_init(&li_del_ords);
	vec_init(&store_delete_keys);

	/* ----- Collect parent-row upserts and per-collection deltas ----- */
	for (e = dirty; e != NULL; e = e->dirty_next)
	{
		/* Parent row in pgredis.store: present for every dirty entry. */
		vec_push(&store_keys, CStringGetTextDatum(e->key), false);
		vec_push(&store_types,
				 CStringGetTextDatum(pg_redis_type_name(e->type)),
				 false);

		if (e->type == PG_REDIS_TYPE_STRING || e->type == PG_REDIS_TYPE_INT)
		{
			bytea	   *enc = pg_redis_encode_value(e);

			if (enc != NULL)
				vec_push(&store_values, PointerGetDatum(enc), false);
			else
				vec_push(&store_values, (Datum) 0, true);
		}
		else
		{
			/* hash/list parent row carries value = NULL */
			vec_push(&store_values, (Datum) 0, true);
		}

		if (e->has_expire)
			vec_push(&store_expires, TimestampTzGetDatum(e->expire_at), false);
		else
			vec_push(&store_expires, (Datum) 0, true);

		vec_push(&store_versions, Int64GetDatum((int64) e->version), false);

		if (e->type == PG_REDIS_TYPE_HASH && e->value.hash_value != NULL)
		{
			PgRedisHash *h = e->value.hash_value;
			PgRedisHashTombstone *t;
			HASH_SEQ_STATUS s;
			PgRedisHashField *f;

			/* Field-level upserts: every dirty field. */
			hash_seq_init(&s, h->fields);
			while ((f = (PgRedisHashField *) hash_seq_search(&s)) != NULL)
			{
				if (!f->dirty)
					continue;
				vec_push(&hf_up_keys, CStringGetTextDatum(e->key), false);
				vec_push(&hf_up_fields, CStringGetTextDatum(f->field), false);
				if (f->value != NULL)
				{
					bytea	   *b = (bytea *) palloc(VARHDRSZ + f->value_len);

					SET_VARSIZE(b, VARHDRSZ + f->value_len);
					if (f->value_len > 0)
						memcpy(VARDATA(b), f->value, f->value_len);
					vec_push(&hf_up_values, PointerGetDatum(b), false);
				}
				else
				{
					vec_push(&hf_up_values, (Datum) 0, true);
				}
				f->dirty = false;
			}

			/* Field-level deletes: tombstones recorded by HDEL. */
			for (t = h->tombstones; t != NULL; t = t->next)
			{
				vec_push(&hf_del_keys, CStringGetTextDatum(e->key), false);
				vec_push(&hf_del_fields, CStringGetTextDatum(t->field), false);
			}
		}
		else if (e->type == PG_REDIS_TYPE_LIST && e->value.list_value != NULL)
		{
			PgRedisList *l = e->value.list_value;
			PgRedisListNode *n;
			PgRedisOrdNode *o;

			for (n = l->head; n != NULL; n = n->next)
			{
				if (!n->pending_insert)
					continue;
				vec_push(&li_ins_keys, CStringGetTextDatum(e->key), false);
				vec_push(&li_ins_ords, Int64GetDatum(n->ord), false);
				if (n->value != NULL)
				{
					bytea	   *b = (bytea *) palloc(VARHDRSZ + n->value_len);

					SET_VARSIZE(b, VARHDRSZ + n->value_len);
					if (n->value_len > 0)
						memcpy(VARDATA(b), n->value, n->value_len);
					vec_push(&li_ins_values, PointerGetDatum(b), false);
				}
				else
				{
					vec_push(&li_ins_values, (Datum) 0, true);
				}
				n->pending_insert = false;
			}

			for (o = l->pending_delete_ords; o != NULL; o = o->next)
			{
				vec_push(&li_del_keys, CStringGetTextDatum(e->key), false);
				vec_push(&li_del_ords, Int64GetDatum(o->ord), false);
			}
		}
	}

	/* ----- Collect store-level deletes for DEL / TTL eviction.
	 *
	 * If a key was DEL'd and then re-created in the same transaction, both
	 * pending_deletes and dirty contain it. Dropping the tombstone here
	 * keeps the upsert from being cascade-wiped by the trailing delete. */
	{
		PgRedisPendingDelete *p;

		for (p = deletes; p != NULL; p = p->next)
		{
			PgRedisEntry *d;
			bool		in_dirty = false;

			for (d = dirty; d != NULL; d = d->dirty_next)
			{
				if (strcmp(d->key, p->key) == 0)
				{
					in_dirty = true;
					break;
				}
			}
			if (!in_dirty)
				vec_push(&store_delete_keys,
						 CStringGetTextDatum(p->key), false);
		}
	}

	/* ----- Execute plans in safe FK-order ----- */

	/* 1. Parent rows (must precede child inserts on first-time keys). */
	if (store_keys.n > 0)
	{
		Oid			argtypes[5] = {TEXTARRAYOID, TEXTARRAYOID, BYTEAARRAYOID,
		TIMESTAMPTZARRAYOID, INT8ARRAYOID};
		Datum		vals[5];
		char		nulls[5] = {' ', ' ', ' ', ' ', ' '};
		SPIPlanPtr	plan;

		vals[0] = PointerGetDatum(vec_to_text_array(&store_keys));
		vals[1] = PointerGetDatum(vec_to_text_array(&store_types));
		vals[2] = PointerGetDatum(vec_to_bytea_array(&store_values));
		vals[3] = PointerGetDatum(vec_to_timestamptz_array(&store_expires));
		vals[4] = PointerGetDatum(vec_to_int8_array(&store_versions));

		plan = prepare_plan_if_needed(&plan_store_upsert, SQL_STORE_UPSERT,
									  5, argtypes);
		if (SPI_execute_plan(plan, vals, nulls, false, 0) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_redis: SPI_execute_plan failed in store_upsert")));
	}

	/* 2. Hash field deletes (tombstones from HDEL). */
	if (hf_del_keys.n > 0)
	{
		Oid			argtypes[2] = {TEXTARRAYOID, TEXTARRAYOID};
		Datum		vals[2];
		char		nulls[2] = {' ', ' '};
		SPIPlanPtr	plan;

		vals[0] = PointerGetDatum(vec_to_text_array(&hf_del_keys));
		vals[1] = PointerGetDatum(vec_to_text_array(&hf_del_fields));

		plan = prepare_plan_if_needed(&plan_hash_field_delete,
									  SQL_HASH_FIELD_DELETE, 2, argtypes);
		if (SPI_execute_plan(plan, vals, nulls, false, 0) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_redis: SPI_execute_plan failed in hash_field_delete")));
	}

	/* 3. Hash field upserts. */
	if (hf_up_keys.n > 0)
	{
		Oid			argtypes[3] = {TEXTARRAYOID, TEXTARRAYOID, BYTEAARRAYOID};
		Datum		vals[3];
		char		nulls[3] = {' ', ' ', ' '};
		SPIPlanPtr	plan;

		vals[0] = PointerGetDatum(vec_to_text_array(&hf_up_keys));
		vals[1] = PointerGetDatum(vec_to_text_array(&hf_up_fields));
		vals[2] = PointerGetDatum(vec_to_bytea_array(&hf_up_values));

		plan = prepare_plan_if_needed(&plan_hash_field_upsert,
									  SQL_HASH_FIELD_UPSERT, 3, argtypes);
		if (SPI_execute_plan(plan, vals, nulls, false, 0) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_redis: SPI_execute_plan failed in hash_field_upsert")));
	}

	/* 4. List item deletes. */
	if (li_del_keys.n > 0)
	{
		Oid			argtypes[2] = {TEXTARRAYOID, INT8ARRAYOID};
		Datum		vals[2];
		char		nulls[2] = {' ', ' '};
		SPIPlanPtr	plan;

		vals[0] = PointerGetDatum(vec_to_text_array(&li_del_keys));
		vals[1] = PointerGetDatum(vec_to_int8_array(&li_del_ords));

		plan = prepare_plan_if_needed(&plan_list_item_delete,
									  SQL_LIST_ITEM_DELETE, 2, argtypes);
		if (SPI_execute_plan(plan, vals, nulls, false, 0) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_redis: SPI_execute_plan failed in list_item_delete")));
	}

	/* 5. List item inserts. */
	if (li_ins_keys.n > 0)
	{
		Oid			argtypes[3] = {TEXTARRAYOID, INT8ARRAYOID, BYTEAARRAYOID};
		Datum		vals[3];
		char		nulls[3] = {' ', ' ', ' '};
		SPIPlanPtr	plan;

		vals[0] = PointerGetDatum(vec_to_text_array(&li_ins_keys));
		vals[1] = PointerGetDatum(vec_to_int8_array(&li_ins_ords));
		vals[2] = PointerGetDatum(vec_to_bytea_array(&li_ins_values));

		plan = prepare_plan_if_needed(&plan_list_item_insert,
									  SQL_LIST_ITEM_INSERT, 3, argtypes);
		if (SPI_execute_plan(plan, vals, nulls, false, 0) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_redis: SPI_execute_plan failed in list_item_insert")));
	}

	/* 6. Parent deletes (CASCADE cleans child rows from any prior commits). */
	if (store_delete_keys.n > 0)
	{
		Oid			argtypes[1] = {TEXTARRAYOID};
		Datum		vals[1];
		char		nulls[1] = {' '};
		SPIPlanPtr	plan;

		vals[0] = PointerGetDatum(vec_to_text_array(&store_delete_keys));

		plan = prepare_plan_if_needed(&plan_store_delete, SQL_STORE_DELETE,
									  1, argtypes);
		if (SPI_execute_plan(plan, vals, nulls, false, 0) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_redis: SPI_execute_plan failed in store_delete")));
	}

	SPI_finish();
	PopActiveSnapshot();

	/* ----- Clear in-memory tracking now that durable state is up to date. */
	{
		PgRedisPendingDelete *p,
				   *next;

		for (p = deletes; p != NULL; p = next)
		{
			next = p->next;
			pfree(p->key);
			pfree(p);
		}
	}

	for (e = dirty; e != NULL;)
	{
		PgRedisEntry *next = e->dirty_next;

		e->in_dirty_set = false;
		e->dirty_next = NULL;
		e->dirty = false;

		if (e->type == PG_REDIS_TYPE_HASH && e->value.hash_value != NULL)
		{
			PgRedisHash *h = e->value.hash_value;
			PgRedisHashTombstone *t = h->tombstones;

			while (t != NULL)
			{
				PgRedisHashTombstone *tn = t->next;

				if (t->field)
					pfree(t->field);
				pfree(t);
				t = tn;
			}
			h->tombstones = NULL;
		}
		else if (e->type == PG_REDIS_TYPE_LIST && e->value.list_value != NULL)
		{
			PgRedisList *l = e->value.list_value;
			PgRedisOrdNode *o = l->pending_delete_ords;

			while (o != NULL)
			{
				PgRedisOrdNode *on = o->next;

				pfree(o);
				o = on;
			}
			l->pending_delete_ords = NULL;
		}

		e = next;
	}
}

/* -------------------------------------------------------------------------
 * Legacy single-row APIs — now thin wrappers around the dirty-set.
 * Callers in pg_redis.c are migrated to mark_dirty / mark_deleted directly,
 * but the public symbols stay so that any external consumer (or stale call
 * site) continues to compile. The actual durable write happens at flush. */

void
pg_redis_persistence_save_entry(PgRedisEntry *e)
{
	if (!persistence_writes_enabled())
		return;
	if (e == NULL || e->deleted)
		return;
	pg_redis_mark_dirty(e);
}

void
pg_redis_persistence_delete_key(const char *key)
{
	if (!persistence_writes_enabled())
		return;
	pg_redis_mark_deleted(key);
}

/* -------------------------------------------------------------------------
 * Lazy load on first access
 * ------------------------------------------------------------------------- */

static void
load_strings_and_ints(void)
{
	const char *sql =
		"SELECT key, type, value, expire_at "
		"  FROM pgredis.store "
		" WHERE type IN ('string', 'int') "
		"   AND (expire_at IS NULL OR expire_at > now())";
	int			ret;

	ret = SPI_execute(sql, true, 0);
	if (ret != SPI_OK_SELECT)
		return;

	for (uint64 i = 0; i < SPI_processed; i++)
	{
		HeapTuple	tup = SPI_tuptable->vals[i];
		TupleDesc	td = SPI_tuptable->tupdesc;
		char	   *key;
		char	   *type;
		bool		isnull;
		Datum		d;
		PgRedisEntry *e;
		bool		is_new;
		bytea	   *raw;

		key = SPI_getvalue(tup, td, 1);
		type = SPI_getvalue(tup, td, 2);
		if (key == NULL || type == NULL)
		{
			ereport(WARNING,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("pg_redis: ignoring row with NULL key or type in pgredis.store")));
			continue;
		}

		if (strlen(key) > PG_REDIS_MAX_KEY_SIZE)
		{
			ereport(WARNING,
					(errmsg("pg_redis: ignoring oversize key \"%s\" during load", key)));
			continue;
		}

		d = SPI_getbinval(tup, td, 3, &isnull);
		/* PG_DETOAST_DATUM (not the "packed" DatumGetByteaPP) so inline-
		 * compressed values are decompressed before the TLV decoder sees them;
		 * otherwise pg_redis_decode_* would read PGLZ bytes, fail the tag check,
		 * and silently drop the value. The detoasted copy lives in
		 * CurrentMemoryContext and is freed at SPI_finish / statement end. */
		raw = isnull ? NULL : (bytea *) PG_DETOAST_DATUM(d);

		e = pg_redis_store_upsert(key, &is_new);

		if (strcmp(type, "string") == 0)
		{
			char	   *s = NULL;
			Size		slen = 0;

			e->type = PG_REDIS_TYPE_STRING;
			if (raw != NULL && pg_redis_decode_string(raw, &s, &slen))
			{
				e->value.string_value = pg_redis_palloc_string(s, slen);
				e->string_len = slen;
				pfree(s);
			}
			else if (raw != NULL)
			{
				ereport(WARNING,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("pg_redis: skipping key \"%s\": binary tag does not match type 'string'",
								key)));
				pg_redis_store_remove(key);
				continue;
			}
		}
		else if (strcmp(type, "int") == 0)
		{
			int64		iv = 0;

			e->type = PG_REDIS_TYPE_INT;
			if (raw != NULL && pg_redis_decode_int(raw, &iv))
				e->value.int_value = iv;
			else if (raw != NULL)
			{
				ereport(WARNING,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("pg_redis: skipping key \"%s\": binary tag does not match type 'int'",
								key)));
				pg_redis_store_remove(key);
				continue;
			}
		}
		else
		{
			continue;
		}

		d = SPI_getbinval(tup, td, 4, &isnull);
		if (!isnull)
		{
			e->has_expire = true;
			e->expire_at = DatumGetTimestampTz(d);
		}
		else
		{
			e->has_expire = false;
		}
		e->dirty = false;

		/* In shared mode, the scratch was a per-backend copy. Push it back
		 * into the shared HTAB so other backends see the loaded data. */
		if (pg_redis_storage_is_shared())
			pg_redis_shared_store_writeback(e);
	}
}

static void
load_hashes(void)
{
	const char *sql =
		"SELECT s.key, h.field, h.value, s.expire_at "
		"  FROM pgredis.store s "
		"  JOIN pgredis.hash_fields h USING (key) "
		" WHERE s.type = 'hash' "
		"   AND (s.expire_at IS NULL OR s.expire_at > now())";

	if (SPI_execute(sql, true, 0) != SPI_OK_SELECT)
		return;

	for (uint64 i = 0; i < SPI_processed; i++)
	{
		HeapTuple	tup = SPI_tuptable->vals[i];
		TupleDesc	td = SPI_tuptable->tupdesc;
		char	   *key = SPI_getvalue(tup, td, 1);
		char	   *field = SPI_getvalue(tup, td, 2);
		bool		isnull;
		Datum		d;
		PgRedisEntry *e;
		bool		is_new;
		bytea	   *raw;
		const char *vbytes;
		Size		vlen;

		if (key == NULL || field == NULL)
			continue;
		if (strlen(key) > PG_REDIS_MAX_KEY_SIZE)
			continue;
		if (strlen(field) > PG_REDIS_MAX_FIELD_SIZE)
			continue;

		d = SPI_getbinval(tup, td, 3, &isnull);
		/* PG_DETOAST_DATUM (not the "packed" DatumGetByteaPP) so inline-
		 * compressed values are decompressed before the TLV decoder sees them;
		 * otherwise pg_redis_decode_* would read PGLZ bytes, fail the tag check,
		 * and silently drop the value. The detoasted copy lives in
		 * CurrentMemoryContext and is freed at SPI_finish / statement end. */
		raw = isnull ? NULL : (bytea *) PG_DETOAST_DATUM(d);
		vbytes = (raw != NULL) ? VARDATA_ANY(raw) : NULL;
		vlen = (raw != NULL) ? VARSIZE_ANY_EXHDR(raw) : 0;

		e = pg_redis_store_upsert(key, &is_new);
		if (is_new || e->type != PG_REDIS_TYPE_HASH)
		{
			pg_redis_entry_release_value(e, false);
			e->type = PG_REDIS_TYPE_HASH;
			e->value.hash_value = pg_redis_hash_create();
		}

		(void) pg_redis_hash_set(e->value.hash_value, field,
								 vbytes != NULL ? vbytes : "", vlen);
		/* hash_set marks the field dirty; we just loaded it from durable
		 * state — clear that so the next flush doesn't redundantly upsert. */
		{
			HASH_SEQ_STATUS s;
			PgRedisHashField *f;

			hash_seq_init(&s, e->value.hash_value->fields);
			while ((f = (PgRedisHashField *) hash_seq_search(&s)) != NULL)
				f->dirty = false;
		}

		d = SPI_getbinval(tup, td, 4, &isnull);
		if (!isnull)
		{
			e->has_expire = true;
			e->expire_at = DatumGetTimestampTz(d);
		}
		e->dirty = false;

		/* Shared mode: writeback the scratch so this field lands in the
		 * shared HTAB. Each row triggers two extra lock acquisitions, but
		 * the load happens once per postmaster lifetime so it's acceptable. */
		if (pg_redis_storage_is_shared())
			pg_redis_shared_store_writeback(e);
	}
}

static void
load_lists(void)
{
	const char *sql =
		"SELECT s.key, l.ord, l.value, s.expire_at "
		"  FROM pgredis.store s "
		"  JOIN pgredis.list_items l USING (key) "
		" WHERE s.type = 'list' "
		"   AND (s.expire_at IS NULL OR s.expire_at > now()) "
		" ORDER BY s.key, l.ord";

	if (SPI_execute(sql, true, 0) != SPI_OK_SELECT)
		return;

	for (uint64 i = 0; i < SPI_processed; i++)
	{
		HeapTuple	tup = SPI_tuptable->vals[i];
		TupleDesc	td = SPI_tuptable->tupdesc;
		char	   *key = SPI_getvalue(tup, td, 1);
		bool		isnull;
		Datum		d;
		PgRedisEntry *e;
		bool		is_new;
		int64		ord;
		bytea	   *raw;
		const char *vbytes;
		Size		vlen;
		PgRedisListNode *n;

		if (key == NULL)
			continue;
		if (strlen(key) > PG_REDIS_MAX_KEY_SIZE)
			continue;

		d = SPI_getbinval(tup, td, 2, &isnull);
		if (isnull)
			continue;
		ord = DatumGetInt64(d);

		d = SPI_getbinval(tup, td, 3, &isnull);
		/* PG_DETOAST_DATUM (not the "packed" DatumGetByteaPP) so inline-
		 * compressed values are decompressed before the TLV decoder sees them;
		 * otherwise pg_redis_decode_* would read PGLZ bytes, fail the tag check,
		 * and silently drop the value. The detoasted copy lives in
		 * CurrentMemoryContext and is freed at SPI_finish / statement end. */
		raw = isnull ? NULL : (bytea *) PG_DETOAST_DATUM(d);
		vbytes = (raw != NULL) ? VARDATA_ANY(raw) : NULL;
		vlen = (raw != NULL) ? VARSIZE_ANY_EXHDR(raw) : 0;

		e = pg_redis_store_upsert(key, &is_new);
		if (is_new || e->type != PG_REDIS_TYPE_LIST)
		{
			pg_redis_entry_release_value(e, false);
			e->type = PG_REDIS_TYPE_LIST;
			e->value.list_value = pg_redis_list_create();
		}

		(void) pg_redis_list_rpush(e->value.list_value,
								   vbytes != NULL ? vbytes : "", vlen);
		/* rpush assigns a synthetic ord and marks pending_insert; overwrite
		 * both with the durable ord. Rows arrive ordered by ord, so the head
		 * carries min and each subsequent row sets the new max. */
		n = e->value.list_value->tail;
		if (n != NULL)
		{
			n->ord = ord;
			n->pending_insert = false;
		}
		if (n == e->value.list_value->head)
			e->value.list_value->min_ord = ord;
		e->value.list_value->max_ord = ord;
		e->value.list_value->ord_initialized = true;

		d = SPI_getbinval(tup, td, 4, &isnull);
		if (!isnull)
		{
			e->has_expire = true;
			e->expire_at = DatumGetTimestampTz(d);
		}
		e->dirty = false;

		/* Shared mode: writeback per row (see load_hashes for rationale). */
		if (pg_redis_storage_is_shared())
			pg_redis_shared_store_writeback(e);
	}
}

/* Watchdog: how long the `loading` (state 1) phase may run before another
 * backend is allowed to steal it (CAS 1->0) and retry — covers the case where
 * the loader died between writing 1 and 2. */
#define PG_REDIS_LOAD_WATCHDOG_MS 30000

/* Run the SPI-bearing cold-start load: connect, repopulate strings/ints,
 * hashes and lists, finish. Caller MUST NOT hold any pg_redis LWLock. SPI
 * relies on the calling statement's active snapshot (every SQL entry point
 * has one). Re-throws on error after closing the SPI session. */
static void
pg_redis_persistence_do_load(void)
{
	if (pg_redis_spi_connect() != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_redis: SPI_connect failed in cold-start load")));

	PG_TRY();
	{
		load_strings_and_ints();
		load_hashes();
		load_lists();
	}
	PG_CATCH();
	{
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();

	SPI_finish();
}

void
pg_redis_persistence_load_if_needed(void)
{
	PgRedisPersistenceMode m;
	PgRedisSharedHeader *h;

	if (persistence_loaded)
		return;

	pg_redis_persistence_init();

	m = pg_redis_resolve_persistence_mode();
	if (m == PG_REDIS_PERSIST_NONE)
	{
		persistence_loaded = true;
		return;
	}

	h = pg_redis_shmem_header();

	/* Session mode (or shmem uninitialized): load directly, no cross-backend
	 * coordination needed. */
	if (!pg_redis_storage_is_shared() || h == NULL)
	{
		pg_redis_persistence_do_load();
		persistence_loaded = true;
		return;
	}

	/*
	 * Shared mode: coordinate the cold-start load across concurrent backends
	 * via the `loaded` atomic state machine (0 = unloaded, 1 = loading,
	 * 2 = loaded). The winner of the CAS 0->1 runs the SPI load with NO
	 * pg_redis LWLock held; the others poll via WaitLatch until they observe
	 * 2. This replaces the old "hold startup_lock across SPI" pattern, which
	 * violated the no-SPI-under-LWLock invariant and deadlocked under
	 * concurrent cold-start load.
	 */
	for (;;)
	{
		uint32		state = pg_atomic_read_u32(&h->loaded);
		uint32		expected;

		if (state == 2)
		{
			persistence_loaded = true;
			return;
		}

		if (state == 0)
		{
			/* Try to become the loader. */
			expected = 0;
			if (pg_atomic_compare_exchange_u32(&h->loaded, &expected, 1))
			{
				pg_atomic_write_u64(&h->last_load_attempt,
									(uint64) GetCurrentTimestamp());
				PG_TRY();
				{
					pg_redis_persistence_do_load();
				}
				PG_CATCH();
				{
					/* Load failed — reset to unloaded so another backend can
					 * retry instead of spinning on `loading` forever. */
					pg_atomic_write_u32(&h->loaded, 0);
					PG_RE_THROW();
				}
				PG_END_TRY();

				pg_atomic_write_u32(&h->loaded, 2);
				persistence_loaded = true;
				return;
			}
			/* Lost the CAS; re-read state. */
			continue;
		}

		/* state == 1: another backend is loading. */
		{
			TimestampTz attempt =
				(TimestampTz) pg_atomic_read_u64(&h->last_load_attempt);

			/* Watchdog: if `loading` has been stuck past the timeout (loader
			 * likely died), steal it and become the loader on the next pass. */
			if (attempt != 0 &&
				TimestampDifferenceExceeds(attempt, GetCurrentTimestamp(),
										   PG_REDIS_LOAD_WATCHDOG_MS))
			{
				expected = 1;
				if (pg_atomic_compare_exchange_u32(&h->loaded, &expected, 0))
					continue;
			}

			/* Poll: 10ms tick, honor cancellation and statement timeout. No
			 * pg_redis LWLock is held while we wait. */
			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 10L,
							 PG_WAIT_EXTENSION);
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}
	}
}

/* -------------------------------------------------------------------------
 * Snapshot — still JSON; out of the hot path.
 * ------------------------------------------------------------------------- */

static void
serialize_entry_value_json(StringInfo out, PgRedisEntry *e)
{
	switch (e->type)
	{
		case PG_REDIS_TYPE_STRING:
			appendStringInfoChar(out, '{');
			appendStringInfoString(out, "\"value\":");
			if (e->value.string_value == NULL)
				appendStringInfoString(out, "\"\"");
			else
				escape_json(out, e->value.string_value);
			appendStringInfoChar(out, '}');
			break;

		case PG_REDIS_TYPE_INT:
			appendStringInfo(out, "{\"value\":" INT64_FORMAT "}", e->value.int_value);
			break;

		case PG_REDIS_TYPE_HASH:
			{
				PgRedisHash *h = e->value.hash_value;
				HASH_SEQ_STATUS s;
				PgRedisHashField *f;
				bool		first = true;

				appendStringInfoString(out, "{\"fields\":{");
				if (h != NULL && h->fields != NULL)
				{
					hash_seq_init(&s, h->fields);
					while ((f = (PgRedisHashField *) hash_seq_search(&s)) != NULL)
					{
						if (!first)
							appendStringInfoChar(out, ',');
						escape_json(out, f->field);
						appendStringInfoChar(out, ':');
						escape_json(out, f->value != NULL ? f->value : "");
						first = false;
					}
				}
				appendStringInfoString(out, "}}");
				break;
			}

		case PG_REDIS_TYPE_LIST:
			{
				PgRedisList *l = e->value.list_value;
				PgRedisListNode *n;
				bool		first = true;

				appendStringInfoString(out, "{\"items\":[");
				if (l != NULL)
				{
					for (n = l->head; n != NULL; n = n->next)
					{
						if (!first)
							appendStringInfoChar(out, ',');
						escape_json(out, n->value != NULL ? n->value : "");
						first = false;
					}
				}
				appendStringInfoString(out, "]}");
				break;
			}
	}
}

void
pg_redis_persistence_save_snapshot(void)
{
	StringInfoData json;
	HASH_SEQ_STATUS s;
	PgRedisEntry *e;
	int64		count = 0;
	bool		first = true;
	Oid			argtypes[2] = {INT8OID, TEXTOID};
	Datum		values[2];
	char		nulls[2] = {' ', ' '};
	const char *sql =
		"INSERT INTO pgredis.snapshots(key_count, payload) "
		"VALUES ($1, $2::jsonb)";

	initStringInfo(&json);
	appendStringInfoChar(&json, '[');

	pg_redis_store_seq_init(&s);
	while ((e = pg_redis_store_seq_next(&s)) != NULL)
	{
		if (pg_redis_entry_is_expired(e))
			continue;

		if (!first)
			appendStringInfoChar(&json, ',');
		appendStringInfoChar(&json, '{');
		appendStringInfoString(&json, "\"key\":");
		escape_json(&json, e->key);
		appendStringInfoString(&json, ",\"type\":");
		escape_json(&json, pg_redis_type_name(e->type));
		appendStringInfoString(&json, ",\"value\":");
		serialize_entry_value_json(&json, e);
		if (e->has_expire)
		{
			char	   *iso = DatumGetCString(DirectFunctionCall1(timestamptz_out,
																  TimestampTzGetDatum(e->expire_at)));

			appendStringInfoString(&json, ",\"expire_at\":");
			escape_json(&json, iso);
			pfree(iso);
		}
		appendStringInfoChar(&json, '}');
		first = false;
		count++;
	}
	appendStringInfoChar(&json, ']');

	values[0] = Int64GetDatum(count);
	values[1] = CStringGetTextDatum(json.data);

	if (pg_redis_spi_connect() != SPI_OK_CONNECT)
	{
		pfree(json.data);
		pfree(DatumGetPointer(values[1]));
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_redis: SPI_connect failed in save_snapshot")));
	}

	if (SPI_execute_with_args(sql, 2, argtypes, values, nulls, false, 1) != SPI_OK_INSERT ||
		SPI_processed != 1)
	{
		SPI_finish();
		pfree(json.data);
		pfree(DatumGetPointer(values[1]));
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_redis: snapshot insert failed")));
	}

	SPI_finish();
	pfree(json.data);
	pfree(DatumGetPointer(values[1]));
}

/* -------------------------------------------------------------------------
 * Background batched flush — driven by jobs scheduler. With the dirty-set
 * model the synchronous flush already lives in the xact pre-commit hook, so
 * this entry point now just calls into the same code path. */

int64
pg_redis_persistence_flush_dirty_batch(int max_batch)
{
	(void) max_batch;
	if (!persistence_writes_enabled())
		return 0;
	run_flush();
	return 0;
}

/* -------------------------------------------------------------------------
 * Async-table drain: pulls events from the shared dirty-ring and persists
 * them via the cached array-form SPI plans. Called from:
 *   - the BGW main loop (periodic + watermark trigger)
 *   - the sync_flush producer fallback (via pg_redis_persistence_sync_drain)
 *
 * Reuses the same plan slots and DatumVec helpers as run_flush().
 * ------------------------------------------------------------------------- */

#define ASYNC_DRAIN_BATCH_MAX 1024

int
pg_redis_persistence_async_drain(int max_events)
{
	PgRedisDirtyEvent *events;
	int			n;
	int			batch_max;
	uint64		base = 0;
	bool		spi_connected = false;
	bool		snapshot_pushed = false;
	DatumVec	store_keys,
				store_types,
				store_values,
				store_expires,
				store_versions;
	DatumVec	hf_up_keys,
				hf_up_fields,
				hf_up_values;
	DatumVec	hf_del_keys,
				hf_del_fields;
	DatumVec	li_ins_keys,
				li_ins_ords,
				li_ins_values;
	DatumVec	li_del_keys,
				li_del_ords;
	DatumVec	store_delete_keys;

	if (!persistence_writes_enabled())
		return 0;

	batch_max = max_events > 0 ? max_events : ASYNC_DRAIN_BATCH_MAX;
	if (batch_max > ASYNC_DRAIN_BATCH_MAX)
		batch_max = ASYNC_DRAIN_BATCH_MAX;

	events = (PgRedisDirtyEvent *) palloc(sizeof(PgRedisDirtyEvent) * batch_max);

	/* Phase 1 (collect): under ring_consumer_lock, copy a batch and flip its
	 * slots READY -> DRAINING. read_head is NOT advanced and nothing is freed
	 * yet — that waits for the durable write to succeed (Decision 11). */
	n = pg_redis_dirty_ring_collect(events, batch_max, &base);
	if (n == 0)
	{
		pfree(events);
		return 0;
	}

	PushActiveSnapshot(GetTransactionSnapshot());
	snapshot_pushed = true;

	if (pg_redis_spi_connect() != SPI_OK_CONNECT)
	{
		/* Couldn't open SPI. Return the collected batch to the ring (slots back
		 * to READY, read_head unchanged) so a later drain retries it
		 * (at-least-once), then raise. */
		PopActiveSnapshot();
		snapshot_pushed = false;
		pg_redis_dirty_ring_abort_batch(events, base, n);
		pfree(events);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_redis: SPI_connect failed in async drain")));
	}
	spi_connected = true;

	/* Phase 2 (persist). On any error the PG_CATCH below pops the snapshot,
	 * finishes SPI (idempotently), and resets the batch's slots to READY so
	 * the events stay in the ring. */
	PG_TRY();
	{
	vec_init(&store_keys);
	vec_init(&store_types);
	vec_init(&store_values);
	vec_init(&store_expires);
	vec_init(&store_versions);
	vec_init(&hf_up_keys);
	vec_init(&hf_up_fields);
	vec_init(&hf_up_values);
	vec_init(&hf_del_keys);
	vec_init(&hf_del_fields);
	vec_init(&li_ins_keys);
	vec_init(&li_ins_ords);
	vec_init(&li_ins_values);
	vec_init(&li_del_keys);
	vec_init(&li_del_ords);
	vec_init(&store_delete_keys);

	/* Walk decoded events and dispatch into the right vec. */
	for (int i = 0; i < n; i++)
	{
		PgRedisDirtyEvent *ev = &events[i];
		const char *key;
		Size		keylen;
		const char *field;
		Size		fieldlen;
		const unsigned char *val;
		Size		val_len;
		text	   *key_text;

		pg_redis_event_decode_key(ev, &key, &keylen);
		key_text = cstring_to_text_with_len(key, keylen);

		switch (ev->type)
		{
			case PG_REDIS_EVENT_KEY_UPSERT:
				pg_redis_event_decode_value(ev, &val, &val_len);
				vec_push(&store_keys, PointerGetDatum(key_text), false);
				vec_push(&store_types,
						 CStringGetTextDatum(pg_redis_type_name(ev->value_type)),
						 false);
				if (val_len == 0 ||
					(ev->value_type != PG_REDIS_TYPE_STRING &&
					 ev->value_type != PG_REDIS_TYPE_INT))
				{
					vec_push(&store_values, (Datum) 0, true);
				}
				else
				{
					bytea	   *b = (bytea *) palloc(VARHDRSZ + val_len);

					SET_VARSIZE(b, VARHDRSZ + val_len);
					memcpy(VARDATA(b), val, val_len);
					vec_push(&store_values, PointerGetDatum(b), false);
				}
				if (ev->has_expire)
					vec_push(&store_expires,
							 TimestampTzGetDatum(ev->expire_at), false);
				else
					vec_push(&store_expires, (Datum) 0, true);
				vec_push(&store_versions,
						 Int64GetDatum((int64) ev->version), false);
				break;

			case PG_REDIS_EVENT_KEY_DELETE:
				vec_push(&store_delete_keys,
						 PointerGetDatum(key_text), false);
				break;

			case PG_REDIS_EVENT_HASH_FIELD_UPSERT:
				pg_redis_event_decode_field(ev, &field, &fieldlen);
				pg_redis_event_decode_value(ev, &val, &val_len);
				vec_push(&hf_up_keys, PointerGetDatum(key_text), false);
				vec_push(&hf_up_fields,
						 PointerGetDatum(cstring_to_text_with_len(field, fieldlen)),
						 false);
				{
					bytea	   *b = (bytea *) palloc(VARHDRSZ + val_len);

					SET_VARSIZE(b, VARHDRSZ + val_len);
					if (val_len > 0)
						memcpy(VARDATA(b), val, val_len);
					vec_push(&hf_up_values, PointerGetDatum(b), false);
				}
				break;

			case PG_REDIS_EVENT_HASH_FIELD_DELETE:
				pg_redis_event_decode_field(ev, &field, &fieldlen);
				vec_push(&hf_del_keys, PointerGetDatum(key_text), false);
				vec_push(&hf_del_fields,
						 PointerGetDatum(cstring_to_text_with_len(field, fieldlen)),
						 false);
				break;

			case PG_REDIS_EVENT_LIST_ITEM_INSERT:
				pg_redis_event_decode_value(ev, &val, &val_len);
				vec_push(&li_ins_keys, PointerGetDatum(key_text), false);
				vec_push(&li_ins_ords, Int64GetDatum(ev->ord), false);
				{
					bytea	   *b = (bytea *) palloc(VARHDRSZ + val_len);

					SET_VARSIZE(b, VARHDRSZ + val_len);
					if (val_len > 0)
						memcpy(VARDATA(b), val, val_len);
					vec_push(&li_ins_values, PointerGetDatum(b), false);
				}
				break;

			case PG_REDIS_EVENT_LIST_ITEM_DELETE:
				vec_push(&li_del_keys, PointerGetDatum(key_text), false);
				vec_push(&li_del_ords, Int64GetDatum(ev->ord), false);
				break;
		}
	}

	/* Execute plans in the same FK-safe order as run_flush:
	 *   1. parent upserts → 2. hash field deletes → 3. hash field upserts
	 *   → 4. list item deletes → 5. list item inserts → 6. parent deletes. */

	if (store_keys.n > 0)
	{
		Oid			argtypes[5] = {TEXTARRAYOID, TEXTARRAYOID, BYTEAARRAYOID,
		TIMESTAMPTZARRAYOID, INT8ARRAYOID};
		Datum		vals[5];
		char		nulls[5] = {' ', ' ', ' ', ' ', ' '};
		SPIPlanPtr	plan;

		vals[0] = PointerGetDatum(vec_to_text_array(&store_keys));
		vals[1] = PointerGetDatum(vec_to_text_array(&store_types));
		vals[2] = PointerGetDatum(vec_to_bytea_array(&store_values));
		vals[3] = PointerGetDatum(vec_to_timestamptz_array(&store_expires));
		vals[4] = PointerGetDatum(vec_to_int8_array(&store_versions));

		plan = prepare_plan_if_needed(&plan_store_upsert, SQL_STORE_UPSERT,
									  5, argtypes);
		if (SPI_execute_plan(plan, vals, nulls, false, 0) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_redis: SPI_execute_plan failed in async store_upsert")));
	}

	if (hf_del_keys.n > 0)
	{
		Oid			argtypes[2] = {TEXTARRAYOID, TEXTARRAYOID};
		Datum		vals[2];
		char		nulls[2] = {' ', ' '};
		SPIPlanPtr	plan;

		vals[0] = PointerGetDatum(vec_to_text_array(&hf_del_keys));
		vals[1] = PointerGetDatum(vec_to_text_array(&hf_del_fields));
		plan = prepare_plan_if_needed(&plan_hash_field_delete,
									  SQL_HASH_FIELD_DELETE, 2, argtypes);
		if (SPI_execute_plan(plan, vals, nulls, false, 0) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_redis: SPI_execute_plan failed in async hash_field_delete")));
	}

	if (hf_up_keys.n > 0)
	{
		Oid			argtypes[3] = {TEXTARRAYOID, TEXTARRAYOID, BYTEAARRAYOID};
		Datum		vals[3];
		char		nulls[3] = {' ', ' ', ' '};
		SPIPlanPtr	plan;

		vals[0] = PointerGetDatum(vec_to_text_array(&hf_up_keys));
		vals[1] = PointerGetDatum(vec_to_text_array(&hf_up_fields));
		vals[2] = PointerGetDatum(vec_to_bytea_array(&hf_up_values));
		plan = prepare_plan_if_needed(&plan_hash_field_upsert,
									  SQL_HASH_FIELD_UPSERT, 3, argtypes);
		if (SPI_execute_plan(plan, vals, nulls, false, 0) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_redis: SPI_execute_plan failed in async hash_field_upsert")));
	}

	if (li_del_keys.n > 0)
	{
		Oid			argtypes[2] = {TEXTARRAYOID, INT8ARRAYOID};
		Datum		vals[2];
		char		nulls[2] = {' ', ' '};
		SPIPlanPtr	plan;

		vals[0] = PointerGetDatum(vec_to_text_array(&li_del_keys));
		vals[1] = PointerGetDatum(vec_to_int8_array(&li_del_ords));
		plan = prepare_plan_if_needed(&plan_list_item_delete,
									  SQL_LIST_ITEM_DELETE, 2, argtypes);
		if (SPI_execute_plan(plan, vals, nulls, false, 0) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_redis: SPI_execute_plan failed in async list_item_delete")));
	}

	if (li_ins_keys.n > 0)
	{
		Oid			argtypes[3] = {TEXTARRAYOID, INT8ARRAYOID, BYTEAARRAYOID};
		Datum		vals[3];
		char		nulls[3] = {' ', ' ', ' '};
		SPIPlanPtr	plan;

		vals[0] = PointerGetDatum(vec_to_text_array(&li_ins_keys));
		vals[1] = PointerGetDatum(vec_to_int8_array(&li_ins_ords));
		vals[2] = PointerGetDatum(vec_to_bytea_array(&li_ins_values));
		plan = prepare_plan_if_needed(&plan_list_item_insert,
									  SQL_LIST_ITEM_INSERT, 3, argtypes);
		if (SPI_execute_plan(plan, vals, nulls, false, 0) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_redis: SPI_execute_plan failed in async list_item_insert")));
	}

	if (store_delete_keys.n > 0)
	{
		Oid			argtypes[1] = {TEXTARRAYOID};
		Datum		vals[1];
		char		nulls[1] = {' '};
		SPIPlanPtr	plan;

		vals[0] = PointerGetDatum(vec_to_text_array(&store_delete_keys));
		plan = prepare_plan_if_needed(&plan_store_delete, SQL_STORE_DELETE,
									  1, argtypes);
		if (SPI_execute_plan(plan, vals, nulls, false, 0) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_redis: SPI_execute_plan failed in async store_delete")));
	}

	SPI_finish();
	spi_connected = false;
	PopActiveSnapshot();
	snapshot_pushed = false;
	}
	PG_CATCH();
	{
		/* Idempotent cleanup (Group 9): pop the active snapshot and finish the
		 * SPI session if they are still open, in that order, before returning
		 * the batch to the ring. Tracking the two flags keeps repeated drain
		 * failures from deepening the SPI nesting or leaking snapshots. */
		if (snapshot_pushed)
		{
			PopActiveSnapshot();
			snapshot_pushed = false;
		}
		if (spi_connected)
		{
			SPI_finish();
			spi_connected = false;
		}
		/* Phase 3 (failure): leave read_head unchanged and reset the batch's
		 * slots to READY so the events are re-collected and re-persisted by a
		 * later drain (at-least-once). The DSA spill payloads stay owned by the
		 * slots — do NOT free them here. */
		pg_redis_dirty_ring_abort_batch(events, base, n);
		pfree(events);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Phase 3 (success): the durable write executed. Advance read_head past the
	 * batch, flip its slots to EMPTY, and free each event's DSA spill payload
	 * exactly once. */
	pg_redis_dirty_ring_release_batch(events, base, n);
	pfree(events);
	return n;
}

/*
 * Synchronous drain of the dirty-ring, used by the sync_flush producer
 * fallback in [src/dirty_ring.c] when the ring is full. Adapts its
 * transaction scope to the caller:
 *
 *   - Called from inside a user statement (the typical producer path,
 *     mid-HSET/SET/etc.), a transaction is already active. We MUST NOT
 *     call StartTransactionCommand in that state (it raises
 *     "unexpected state STARTED" and aborts the user's xact). Instead,
 *     run the drain inside an internal subtransaction so a drain failure
 *     stays scoped to itself and the outer user xact survives.
 *
 *   - Called from a context with no active transaction (no current caller,
 *     but kept stable for future non-xact callers), use a top-level
 *     StartTransactionCommand/CommitTransactionCommand pair.
 *
 * Returns the number of events drained. Re-throws on internal failure,
 * after releasing the subtransaction (or aborting the top-level xact) it
 * opened.
 */
int
pg_redis_persistence_sync_drain(void)
{
	int			drained;

	/* The sync_flush producer MUST release its partition LWLock before calling
	 * us (Decision 2): the drain opens a subtransaction and runs SPI, which is
	 * forbidden under any pg_redis LWLock. */
	pg_redis_assert_no_pg_redis_lwlock_held();

	if (IsTransactionState())
	{
		BeginInternalSubTransaction(NULL);
		PG_TRY();
		{
			drained = pg_redis_persistence_async_drain(0);
			ReleaseCurrentSubTransaction();
		}
		PG_CATCH();
		{
			RollbackAndReleaseCurrentSubTransaction();
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
	else
	{
		StartTransactionCommand();
		PG_TRY();
		{
			drained = pg_redis_persistence_async_drain(0);
		}
		PG_CATCH();
		{
			AbortCurrentTransaction();
			PG_RE_THROW();
		}
		PG_END_TRY();
		CommitTransactionCommand();
	}
	return drained;
}

/* -------------------------------------------------------------------------
 * FLUSHALL — clear durable store in sync/async modes
 * ------------------------------------------------------------------------- */

void
pg_redis_persistence_flushall(void)
{
	/* M1 (Decision 12): the TRUNCATE (SPI + heavyweight relation locks) MUST
	 * run with no pg_redis LWLock held. pg_redis_flushall() releases all
	 * partition locks before calling us. */
	pg_redis_assert_no_pg_redis_lwlock_held();

	if (persistence_writes_enabled())
	{
		if (pg_redis_spi_connect() == SPI_OK_CONNECT)
		{
			(void) SPI_execute("TRUNCATE TABLE pgredis.store CASCADE", false, 0);
			SPI_finish();
		}
	}
	persistence_loaded = true;

	/* Shared mode: the keyspace is now authoritative-empty across the
	 * cluster. Mark the shared loaded flag (2 = loaded) so other backends
	 * don't try to re-load from the (now empty) durable tables. */
	if (pg_redis_storage_is_shared())
	{
		PgRedisSharedHeader *h = pg_redis_shmem_header();

		if (h != NULL)
			pg_atomic_write_u32(&h->loaded, 2);
	}
}

/* -------------------------------------------------------------------------
 * AOF append (not replayed in v0.1)
 * ------------------------------------------------------------------------- */

void
pg_redis_persistence_append_aof(const char *op, const char *key,
								const char *args_json)
{
	Oid			argtypes[3] = {TEXTOID, TEXTOID, TEXTOID};
	Datum		values[3];
	char		nulls[3] = {' ', ' ', ' '};
	const char *sql =
		"INSERT INTO pgredis.aof(op, key, args) VALUES ($1, $2, $3::jsonb)";

	if (op == NULL)
		return;

	values[0] = CStringGetTextDatum(op);
	if (key != NULL)
		values[1] = CStringGetTextDatum(key);
	else
	{
		values[1] = (Datum) 0;
		nulls[1] = 'n';
	}
	values[2] = CStringGetTextDatum(args_json != NULL ? args_json : "{}");

	if (pg_redis_spi_connect() != SPI_OK_CONNECT)
		return;
	(void) SPI_execute_with_args(sql, 3, argtypes, values, nulls, false, 0);
	SPI_finish();

	pfree(DatumGetPointer(values[0]));
	if (key != NULL)
		pfree(DatumGetPointer(values[1]));
	pfree(DatumGetPointer(values[2]));
}
