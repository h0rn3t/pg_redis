#include "postgres.h"

#if PG_VERSION_NUM < 180000
#error "pg_redis requires PostgreSQL 18 or later"
#endif

#include "varatt.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

#include <string.h>

#include "types.h"
#include "utils.h"
#include "kv_store.h"
#include "ttl.h"
#include "list.h"
#include "hash_value.h"
#include "persistence.h"
#include "jobs.h"
#include "shmem.h"
#include "bgworker.h"

PG_MODULE_MAGIC;

/* -------------------------------------------------------------------------
 * GUC variables
 * ------------------------------------------------------------------------- */

char	   *pg_redis_persistence_mode = NULL;
char	   *pg_redis_storage_mode = NULL;
int			pg_redis_flush_interval_s = 5;
int			pg_redis_flush_batch_size = 1000;
int			pg_redis_ttl_cleanup_interval_s = 30;
int			pg_redis_max_key_size = 1024;
int			pg_redis_max_value_size = 1048576;
bool		pg_redis_enable_bgworker = false;
int			pg_redis_shared_max_memory_mb = 256;
int			pg_redis_dirty_ring_size = 65536;
int			pg_redis_lock_partitions = 16;
int			pg_redis_async_full_action = PG_REDIS_ASYNC_FULL_BLOCK;

/* Enum value table for pg_redis.async_full_action. */
static const struct config_enum_entry async_full_action_options[] = {
	{"block", PG_REDIS_ASYNC_FULL_BLOCK, false},
	{"sync_flush", PG_REDIS_ASYNC_FULL_SYNC_FLUSH, false},
	{NULL, 0, false}
};

static void pg_redis_xact_cb(XactEvent event, void *arg);
static void declare_shmem_request_hook(void);

static shmem_request_hook_type prev_shmem_request_hook = NULL;

static bool
check_persistence_mode(char **newval, void **extra, GucSource source)
{
	if (*newval == NULL)
		return true;
	if (strcmp(*newval, "none") == 0 ||
		strcmp(*newval, "sync_table") == 0 ||
		strcmp(*newval, "async_table") == 0 ||
		strcmp(*newval, "snapshot") == 0 ||
		strcmp(*newval, "aof") == 0)
		return true;
	GUC_check_errdetail("Allowed values are: none, sync_table, async_table, snapshot, aof.");
	return false;
}

/* Emitted whenever persistence_mode lands on async_table without a matching
 * storage_mode=shared. The runtime falls back to sync_table semantics via
 * pg_redis_effective_persistence_mode(); this hook just makes the
 * misconfiguration loud at GUC assignment time. */
static void
assign_persistence_mode(const char *newval, void *extra)
{
	if (newval != NULL &&
		strcmp(newval, "async_table") == 0 &&
		pg_redis_storage_mode != NULL &&
		strcmp(pg_redis_storage_mode, "shared") != 0)
	{
		ereport(WARNING,
				(errmsg("pg_redis: persistence_mode='async_table' requires storage_mode='shared'"),
				 errdetail("Falling back to sync_table semantics. "
						   "Set pg_redis.storage_mode='shared' in postgresql.conf "
						   "(also requires shared_preload_libraries='pg_redis').")));
	}
}

static bool
check_storage_mode(char **newval, void **extra, GucSource source)
{
	if (*newval == NULL)
		return true;
	if (strcmp(*newval, "session") == 0 || strcmp(*newval, "shared") == 0)
		return true;
	GUC_check_errdetail("Allowed values are: session, shared.");
	return false;
}

static void
pg_redis_shmem_request_hook(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
	pg_redis_shmem_request();
}

static void
declare_shmem_request_hook(void)
{
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pg_redis_shmem_request_hook;
}

void		_PG_init(void);

void
_PG_init(void)
{
	DefineCustomStringVariable("pg_redis.persistence_mode",
							   "Persistence mode for the pg_redis keyspace.",
							   "One of: none, sync_table, async_table, snapshot, aof. "
							   "async_table requires storage_mode='shared'; otherwise "
							   "the runtime falls back to sync_table.",
							   &pg_redis_persistence_mode,
							   "sync_table",
							   PGC_USERSET,
							   0,
							   check_persistence_mode,
							   assign_persistence_mode,
							   NULL);

	DefineCustomStringVariable("pg_redis.storage_mode",
							   "Storage mode for the pg_redis keyspace.",
							   "Either 'session' (per-backend memory) or 'shared'. v0.1 only "
							   "implements 'session'; 'shared' is reserved.",
							   &pg_redis_storage_mode,
							   "session",
							   PGC_SIGHUP,
							   0,
							   check_storage_mode,
							   NULL,
							   NULL);

	DefineCustomIntVariable("pg_redis.flush_interval",
							"Seconds between background flush ticks.",
							NULL,
							&pg_redis_flush_interval_s,
							5,
							1,
							3600,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_redis.flush_batch_size",
							"Max dirty entries flushed per background tick.",
							NULL,
							&pg_redis_flush_batch_size,
							1000,
							1,
							1000000,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_redis.ttl_cleanup_interval",
							"Seconds between TTL cleanup ticks (used by ttl_cleanup jobs).",
							NULL,
							&pg_redis_ttl_cleanup_interval_s,
							30,
							1,
							86400,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_redis.max_key_size",
							"Maximum length, in bytes, of a key.",
							NULL,
							&pg_redis_max_key_size,
							1024,
							1,
							PG_REDIS_MAX_KEY_SIZE,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_redis.max_value_size",
							"Maximum length, in bytes, of a single string/list/hash value.",
							NULL,
							&pg_redis_max_value_size,
							1048576,
							1,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_redis.enable_background_worker",
							 "Enable the pg_redis background worker (requires shared_preload_libraries).",
							 NULL,
							 &pg_redis_enable_bgworker,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	/* PGC_POSTMASTER GUCs can only be defined at postmaster startup, i.e.
	 * when this module is loaded via shared_preload_libraries. If the
	 * extension is loaded as a regular library (CREATE EXTENSION from a
	 * regular session), defining a PGC_POSTMASTER GUC raises:
	 *   "cannot create PGC_POSTMASTER variables after startup".
	 * The static defaults remain in effect either way; these GUCs are only
	 * meaningful when shared mode is also requested. */
	if (process_shared_preload_libraries_in_progress)
	{
		DefineCustomIntVariable("pg_redis.shared_max_memory",
								"Maximum memory, in MB, for the shared-mode keyspace DSA.",
								"Only used when storage_mode='shared'. Sized at postmaster start.",
								&pg_redis_shared_max_memory_mb,
								256,
								1,
								32768,
								PGC_POSTMASTER,
								GUC_UNIT_MB,
								NULL, NULL, NULL);

		DefineCustomIntVariable("pg_redis.dirty_ring_size",
								"Slot count for the shared async-flush dirty-ring.",
								"Only used when persistence_mode='async_table'.",
								&pg_redis_dirty_ring_size,
								65536,
								1024,
								1048576,
								PGC_POSTMASTER,
								0,
								NULL, NULL, NULL);

		DefineCustomIntVariable("pg_redis.lock_partitions",
								"Number of LWLock partitions on the shared keyspace.",
								"Only used when storage_mode='shared'.",
								&pg_redis_lock_partitions,
								16,
								1,
								128,
								PGC_POSTMASTER,
								0,
								NULL, NULL, NULL);
	}

	DefineCustomEnumVariable("pg_redis.async_full_action",
							 "Action when the async dirty-ring is full.",
							 "'block' raises an error after a brief spin; "
							 "'sync_flush' drains synchronously in the producer.",
							 &pg_redis_async_full_action,
							 PG_REDIS_ASYNC_FULL_BLOCK,
							 async_full_action_options,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	MarkGUCPrefixReserved("pg_redis");

	if (process_shared_preload_libraries_in_progress)
	{
		declare_shmem_request_hook();
		pg_redis_shmem_install_hooks();
		pg_redis_bgworker_register();
	}

	RegisterXactCallback(pg_redis_xact_cb, NULL);
}

static void
pg_redis_xact_cb(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
		pg_redis_persistence_invalidate_load();
}

/* -------------------------------------------------------------------------
 * Helpers shared by command implementations
 * ------------------------------------------------------------------------- */

static void
ensure_loaded(void)
{
	pg_redis_store_init();
	pg_redis_persistence_init();
	pg_redis_persistence_load_if_needed();
}

static char *
argtext_to_pstring(text *t, Size *out_len, bool check_key)
{
	Size		len = VARSIZE_ANY_EXHDR(t);
	char	   *p;

	if (check_key)
		pg_redis_check_key_len(NULL, len);
	else
		pg_redis_check_value_len(len);

	p = pg_redis_palloc_string(VARDATA_ANY(t), len);
	if (out_len)
		*out_len = len;
	return p;
}

/* -------------------------------------------------------------------------
 * Strings / generic
 * ------------------------------------------------------------------------- */

PG_FUNCTION_INFO_V1(pg_redis_set);
Datum
pg_redis_set(PG_FUNCTION_ARGS)
{
	text	   *key_t = PG_GETARG_TEXT_PP(0);
	text	   *val_t = PG_GETARG_TEXT_PP(1);
	char		keybuf[PG_REDIS_KEY_STACK_BUFSZ];
	char	   *key;
	char	   *val;
	Size		key_len,
				val_len;
	PgRedisEntry *e;
	bool		is_new;

	ensure_loaded();

	key = pg_redis_text_to_cstring_stack(key_t, keybuf, sizeof(keybuf),
										 &key_len, true, false);
	val = argtext_to_pstring(val_t, &val_len, false);

	e = pg_redis_store_upsert(key, &is_new);
	if (!is_new && e->type != PG_REDIS_TYPE_STRING)
		pg_redis_entry_release_value(e, false);

	if (e->type == PG_REDIS_TYPE_STRING && e->value.string_value != NULL)
	{
		pfree(e->value.string_value);
		e->value.string_value = NULL;
	}
	e->type = PG_REDIS_TYPE_STRING;
	e->value.string_value = val; /* owned by PgRedisMemoryContext */
	e->string_len = val_len;
	e->version++;
	e->dirty = true;
	e->deleted = false;

	pg_redis_mark_dirty(e);
	pg_redis_free_cstring_stack(keybuf, key);
	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(pg_redis_get);
Datum
pg_redis_get(PG_FUNCTION_ARGS)
{
	text	   *key_t = PG_GETARG_TEXT_PP(0);
	char		keybuf[PG_REDIS_KEY_STACK_BUFSZ];
	char	   *key;
	Size		key_len;
	PgRedisEntry *e;

	ensure_loaded();
	key = pg_redis_text_to_cstring_stack(key_t, keybuf, sizeof(keybuf),
										 &key_len, true, false);
	e = pg_redis_store_lookup(key, NULL);
	pg_redis_free_cstring_stack(keybuf, key);

	if (e == NULL)
		PG_RETURN_NULL();

	switch (e->type)
	{
		case PG_REDIS_TYPE_STRING:
			if (e->value.string_value == NULL)
				PG_RETURN_NULL();
			PG_RETURN_TEXT_P(cstring_to_text_with_len(e->value.string_value, e->string_len));

		case PG_REDIS_TYPE_INT:
			{
				Size		n;
				char	   *s = pg_redis_format_int64(e->value.int_value, &n);
				text	   *out = cstring_to_text_with_len(s, n);

				pfree(s);
				PG_RETURN_TEXT_P(out);
			}

		case PG_REDIS_TYPE_HASH:
			pg_redis_wrongtype(e->key, PG_REDIS_TYPE_HASH, PG_REDIS_TYPE_STRING);
			break;
		case PG_REDIS_TYPE_LIST:
			pg_redis_wrongtype(e->key, PG_REDIS_TYPE_LIST, PG_REDIS_TYPE_STRING);
			break;
	}
	PG_RETURN_NULL();			/* unreachable */
}

PG_FUNCTION_INFO_V1(pg_redis_del);
Datum
pg_redis_del(PG_FUNCTION_ARGS)
{
	text	   *key_t = PG_GETARG_TEXT_PP(0);
	char		keybuf[PG_REDIS_KEY_STACK_BUFSZ];
	char	   *key;
	Size		key_len;
	bool		removed;

	ensure_loaded();
	key = pg_redis_text_to_cstring_stack(key_t, keybuf, sizeof(keybuf),
										 &key_len, true, false);
	/* store_remove records a tombstone in pending_deletes; the durable row
	 * is removed by the pre-commit flush (CASCADE clears any children). */
	removed = pg_redis_store_remove(key);
	pg_redis_free_cstring_stack(keybuf, key);
	PG_RETURN_BOOL(removed);
}

PG_FUNCTION_INFO_V1(pg_redis_exists);
Datum
pg_redis_exists(PG_FUNCTION_ARGS)
{
	text	   *key_t = PG_GETARG_TEXT_PP(0);
	char		keybuf[PG_REDIS_KEY_STACK_BUFSZ];
	char	   *key;
	Size		key_len;
	PgRedisEntry *e;

	ensure_loaded();
	key = pg_redis_text_to_cstring_stack(key_t, keybuf, sizeof(keybuf),
										 &key_len, true, false);
	e = pg_redis_store_lookup(key, NULL);
	pg_redis_free_cstring_stack(keybuf, key);
	PG_RETURN_BOOL(e != NULL);
}

PG_FUNCTION_INFO_V1(pg_redis_expire);
Datum
pg_redis_expire(PG_FUNCTION_ARGS)
{
	text	   *key_t = PG_GETARG_TEXT_PP(0);
	int32		seconds = PG_GETARG_INT32(1);
	char		keybuf[PG_REDIS_KEY_STACK_BUFSZ];
	char	   *key;
	Size		key_len;
	bool		ok;
	PgRedisEntry *e;

	ensure_loaded();
	key = pg_redis_text_to_cstring_stack(key_t, keybuf, sizeof(keybuf),
										 &key_len, true, false);
	ok = pg_redis_set_expire_seconds(key, seconds);
	if (ok && seconds > 0)
	{
		/* For seconds <= 0, set_expire_seconds already removed the entry via
		 * store_remove (which appends to pending_deletes). */
		e = pg_redis_store_lookup_raw(key);
		if (e != NULL)
			pg_redis_mark_dirty(e);
	}
	pg_redis_free_cstring_stack(keybuf, key);
	PG_RETURN_BOOL(ok);
}

PG_FUNCTION_INFO_V1(pg_redis_ttl);
Datum
pg_redis_ttl(PG_FUNCTION_ARGS)
{
	text	   *key_t = PG_GETARG_TEXT_PP(0);
	char		keybuf[PG_REDIS_KEY_STACK_BUFSZ];
	char	   *key;
	Size		key_len;
	int32		out;

	ensure_loaded();
	key = pg_redis_text_to_cstring_stack(key_t, keybuf, sizeof(keybuf),
										 &key_len, true, false);
	out = pg_redis_ttl_seconds(key);
	pg_redis_free_cstring_stack(keybuf, key);
	PG_RETURN_INT32(out);
}

static int64
incrdecr(text *key_t, int64 delta)
{
	char		keybuf[PG_REDIS_KEY_STACK_BUFSZ];
	char	   *key;
	Size		key_len;
	PgRedisEntry *e;
	bool		is_new;
	int64		newval;

	ensure_loaded();
	key = pg_redis_text_to_cstring_stack(key_t, keybuf, sizeof(keybuf),
										 &key_len, true, false);
	e = pg_redis_store_upsert(key, &is_new);

	if (is_new)
	{
		e->type = PG_REDIS_TYPE_INT;
		e->value.int_value = 0;
	}
	else if (e->type == PG_REDIS_TYPE_STRING)
	{
		int64		parsed;

		if (e->value.string_value == NULL ||
			!pg_redis_parse_int64(e->value.string_value, e->string_len, &parsed))
		{
			pg_redis_free_cstring_stack(keybuf, key);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("pg_redis: value at key \"%s\" is not an integer",
							e->key)));
		}
		pfree(e->value.string_value);
		e->value.string_value = NULL;
		e->string_len = 0;
		e->type = PG_REDIS_TYPE_INT;
		e->value.int_value = parsed;
	}
	else if (e->type != PG_REDIS_TYPE_INT)
	{
		PgRedisValueType actual = e->type;

		pg_redis_free_cstring_stack(keybuf, key);
		pg_redis_wrongtype(e->key, actual, PG_REDIS_TYPE_INT);
	}

	newval = e->value.int_value + delta;
	e->value.int_value = newval;
	e->version++;

	pg_redis_mark_dirty(e);
	pg_redis_free_cstring_stack(keybuf, key);
	return newval;
}

PG_FUNCTION_INFO_V1(pg_redis_incr);
Datum
pg_redis_incr(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(incrdecr(PG_GETARG_TEXT_PP(0), 1));
}

PG_FUNCTION_INFO_V1(pg_redis_decr);
Datum
pg_redis_decr(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(incrdecr(PG_GETARG_TEXT_PP(0), -1));
}

/* -------------------------------------------------------------------------
 * Hashes
 * ------------------------------------------------------------------------- */

PG_FUNCTION_INFO_V1(pg_redis_hset);
Datum
pg_redis_hset(PG_FUNCTION_ARGS)
{
	text	   *key_t = PG_GETARG_TEXT_PP(0);
	text	   *field_t = PG_GETARG_TEXT_PP(1);
	text	   *val_t = PG_GETARG_TEXT_PP(2);
	char		keybuf[PG_REDIS_KEY_STACK_BUFSZ];
	char		fieldbuf[PG_REDIS_FIELD_STACK_BUFSZ];
	char	   *key,
			   *field,
			   *val;
	Size		key_len,
				field_len,
				val_len;
	PgRedisEntry *e;
	bool		is_new;
	bool		new_field;

	ensure_loaded();
	key = pg_redis_text_to_cstring_stack(key_t, keybuf, sizeof(keybuf),
										 &key_len, true, false);
	field = pg_redis_text_to_cstring_stack(field_t, fieldbuf, sizeof(fieldbuf),
										   &field_len, false, true);
	val = argtext_to_pstring(val_t, &val_len, false);

	e = pg_redis_store_upsert(key, &is_new);
	if (!is_new && e->type != PG_REDIS_TYPE_HASH)
	{
		PgRedisValueType actual = e->type;

		pg_redis_free_cstring_stack(keybuf, key);
		pg_redis_free_cstring_stack(fieldbuf, field);
		pfree(val);
		pg_redis_wrongtype(e->key, actual, PG_REDIS_TYPE_HASH);
	}
	if (is_new || e->value.hash_value == NULL)
	{
		e->type = PG_REDIS_TYPE_HASH;
		e->value.hash_value = pg_redis_hash_create();
	}

	new_field = pg_redis_hash_set(e->value.hash_value, field, val, val_len);
	e->version++;

	pg_redis_mark_dirty(e);

	pg_redis_free_cstring_stack(keybuf, key);
	pg_redis_free_cstring_stack(fieldbuf, field);
	pfree(val);
	PG_RETURN_BOOL(new_field);
}

PG_FUNCTION_INFO_V1(pg_redis_hget);
Datum
pg_redis_hget(PG_FUNCTION_ARGS)
{
	text	   *key_t = PG_GETARG_TEXT_PP(0);
	text	   *field_t = PG_GETARG_TEXT_PP(1);
	char		keybuf[PG_REDIS_KEY_STACK_BUFSZ];
	char		fieldbuf[PG_REDIS_FIELD_STACK_BUFSZ];
	char	   *key,
			   *field;
	Size		key_len,
				field_len;
	PgRedisEntry *e;
	const char *v;
	Size		vlen;

	ensure_loaded();
	key = pg_redis_text_to_cstring_stack(key_t, keybuf, sizeof(keybuf),
										 &key_len, true, false);
	field = pg_redis_text_to_cstring_stack(field_t, fieldbuf, sizeof(fieldbuf),
										   &field_len, false, true);

	e = pg_redis_store_lookup(key, NULL);
	if (e == NULL)
	{
		pg_redis_free_cstring_stack(keybuf, key);
		pg_redis_free_cstring_stack(fieldbuf, field);
		PG_RETURN_NULL();
	}
	if (e->type != PG_REDIS_TYPE_HASH)
	{
		PgRedisValueType actual = e->type;

		pg_redis_free_cstring_stack(keybuf, key);
		pg_redis_free_cstring_stack(fieldbuf, field);
		pg_redis_wrongtype(e->key, actual, PG_REDIS_TYPE_HASH);
	}
	v = pg_redis_hash_get(e->value.hash_value, field, &vlen);
	pg_redis_free_cstring_stack(keybuf, key);
	pg_redis_free_cstring_stack(fieldbuf, field);
	if (v == NULL)
		PG_RETURN_NULL();
	PG_RETURN_TEXT_P(cstring_to_text_with_len(v, vlen));
}

PG_FUNCTION_INFO_V1(pg_redis_hdel);
Datum
pg_redis_hdel(PG_FUNCTION_ARGS)
{
	text	   *key_t = PG_GETARG_TEXT_PP(0);
	text	   *field_t = PG_GETARG_TEXT_PP(1);
	char		keybuf[PG_REDIS_KEY_STACK_BUFSZ];
	char		fieldbuf[PG_REDIS_FIELD_STACK_BUFSZ];
	char	   *key,
			   *field;
	Size		key_len,
				field_len;
	PgRedisEntry *e;
	bool		removed;

	ensure_loaded();
	key = pg_redis_text_to_cstring_stack(key_t, keybuf, sizeof(keybuf),
										 &key_len, true, false);
	field = pg_redis_text_to_cstring_stack(field_t, fieldbuf, sizeof(fieldbuf),
										   &field_len, false, true);

	e = pg_redis_store_lookup(key, NULL);
	if (e == NULL)
	{
		pg_redis_free_cstring_stack(keybuf, key);
		pg_redis_free_cstring_stack(fieldbuf, field);
		PG_RETURN_BOOL(false);
	}
	if (e->type != PG_REDIS_TYPE_HASH)
	{
		PgRedisValueType actual = e->type;

		pg_redis_free_cstring_stack(keybuf, key);
		pg_redis_free_cstring_stack(fieldbuf, field);
		pg_redis_wrongtype(e->key, actual, PG_REDIS_TYPE_HASH);
	}
	removed = pg_redis_hash_del(e->value.hash_value, field);
	if (removed)
	{
		e->version++;
		pg_redis_mark_dirty(e);
	}
	pg_redis_free_cstring_stack(keybuf, key);
	pg_redis_free_cstring_stack(fieldbuf, field);
	PG_RETURN_BOOL(removed);
}

PG_FUNCTION_INFO_V1(pg_redis_hexists);
Datum
pg_redis_hexists(PG_FUNCTION_ARGS)
{
	text	   *key_t = PG_GETARG_TEXT_PP(0);
	text	   *field_t = PG_GETARG_TEXT_PP(1);
	char		keybuf[PG_REDIS_KEY_STACK_BUFSZ];
	char		fieldbuf[PG_REDIS_FIELD_STACK_BUFSZ];
	char	   *key,
			   *field;
	Size		key_len,
				field_len;
	PgRedisEntry *e;
	bool		exists;

	ensure_loaded();
	key = pg_redis_text_to_cstring_stack(key_t, keybuf, sizeof(keybuf),
										 &key_len, true, false);
	field = pg_redis_text_to_cstring_stack(field_t, fieldbuf, sizeof(fieldbuf),
										   &field_len, false, true);

	e = pg_redis_store_lookup(key, NULL);
	if (e == NULL)
	{
		pg_redis_free_cstring_stack(keybuf, key);
		pg_redis_free_cstring_stack(fieldbuf, field);
		PG_RETURN_BOOL(false);
	}
	if (e->type != PG_REDIS_TYPE_HASH)
	{
		PgRedisValueType actual = e->type;

		pg_redis_free_cstring_stack(keybuf, key);
		pg_redis_free_cstring_stack(fieldbuf, field);
		pg_redis_wrongtype(e->key, actual, PG_REDIS_TYPE_HASH);
	}
	exists = pg_redis_hash_exists(e->value.hash_value, field);
	pg_redis_free_cstring_stack(keybuf, key);
	pg_redis_free_cstring_stack(fieldbuf, field);
	PG_RETURN_BOOL(exists);
}

/* -------------------------------------------------------------------------
 * Lists
 * ------------------------------------------------------------------------- */

static int64
list_push(text *key_t, text *val_t, bool left)
{
	char		keybuf[PG_REDIS_KEY_STACK_BUFSZ];
	char	   *key,
			   *val;
	Size		key_len,
				val_len;
	PgRedisEntry *e;
	bool		is_new;
	int64		newlen;

	ensure_loaded();
	key = pg_redis_text_to_cstring_stack(key_t, keybuf, sizeof(keybuf),
										 &key_len, true, false);
	val = argtext_to_pstring(val_t, &val_len, false);

	e = pg_redis_store_upsert(key, &is_new);
	if (!is_new && e->type != PG_REDIS_TYPE_LIST)
	{
		PgRedisValueType actual = e->type;

		pg_redis_free_cstring_stack(keybuf, key);
		pfree(val);
		pg_redis_wrongtype(e->key, actual, PG_REDIS_TYPE_LIST);
	}
	if (is_new || e->value.list_value == NULL)
	{
		e->type = PG_REDIS_TYPE_LIST;
		e->value.list_value = pg_redis_list_create();
	}

	if (left)
		newlen = pg_redis_list_lpush(e->value.list_value, val, val_len);
	else
		newlen = pg_redis_list_rpush(e->value.list_value, val, val_len);
	e->version++;

	pg_redis_mark_dirty(e);
	pg_redis_free_cstring_stack(keybuf, key);
	pfree(val);
	return newlen;
}

PG_FUNCTION_INFO_V1(pg_redis_lpush);
Datum
pg_redis_lpush(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(list_push(PG_GETARG_TEXT_PP(0), PG_GETARG_TEXT_PP(1), true));
}

PG_FUNCTION_INFO_V1(pg_redis_rpush);
Datum
pg_redis_rpush(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(list_push(PG_GETARG_TEXT_PP(0), PG_GETARG_TEXT_PP(1), false));
}

static Datum
list_pop(PG_FUNCTION_ARGS, bool left)
{
	text	   *key_t = PG_GETARG_TEXT_PP(0);
	char		keybuf[PG_REDIS_KEY_STACK_BUFSZ];
	char	   *key;
	Size		key_len;
	PgRedisEntry *e;
	char	   *popped;
	Size		plen;
	text	   *out;

	ensure_loaded();
	key = pg_redis_text_to_cstring_stack(key_t, keybuf, sizeof(keybuf),
										 &key_len, true, false);
	e = pg_redis_store_lookup(key, NULL);
	if (e == NULL)
	{
		pg_redis_free_cstring_stack(keybuf, key);
		PG_RETURN_NULL();
	}
	if (e->type != PG_REDIS_TYPE_LIST)
	{
		PgRedisValueType actual = e->type;

		pg_redis_free_cstring_stack(keybuf, key);
		pg_redis_wrongtype(e->key, actual, PG_REDIS_TYPE_LIST);
	}
	popped = left
		? pg_redis_list_lpop(e->value.list_value, &plen)
		: pg_redis_list_rpop(e->value.list_value, &plen);
	if (popped == NULL)
	{
		pg_redis_free_cstring_stack(keybuf, key);
		PG_RETURN_NULL();
	}

	out = cstring_to_text_with_len(popped, plen);
	pfree(popped);

	e->version++;
	pg_redis_mark_dirty(e);

	pg_redis_free_cstring_stack(keybuf, key);
	PG_RETURN_TEXT_P(out);
}

PG_FUNCTION_INFO_V1(pg_redis_lpop);
Datum
pg_redis_lpop(PG_FUNCTION_ARGS)
{
	return list_pop(fcinfo, true);
}

PG_FUNCTION_INFO_V1(pg_redis_rpop);
Datum
pg_redis_rpop(PG_FUNCTION_ARGS)
{
	return list_pop(fcinfo, false);
}

PG_FUNCTION_INFO_V1(pg_redis_llen);
Datum
pg_redis_llen(PG_FUNCTION_ARGS)
{
	text	   *key_t = PG_GETARG_TEXT_PP(0);
	char		keybuf[PG_REDIS_KEY_STACK_BUFSZ];
	char	   *key;
	Size		key_len;
	PgRedisEntry *e;
	int64		len = 0;

	ensure_loaded();
	key = pg_redis_text_to_cstring_stack(key_t, keybuf, sizeof(keybuf),
										 &key_len, true, false);
	e = pg_redis_store_lookup(key, NULL);
	if (e == NULL)
	{
		pg_redis_free_cstring_stack(keybuf, key);
		PG_RETURN_INT64(0);
	}
	if (e->type != PG_REDIS_TYPE_LIST)
	{
		PgRedisValueType actual = e->type;

		pg_redis_free_cstring_stack(keybuf, key);
		pg_redis_wrongtype(e->key, actual, PG_REDIS_TYPE_LIST);
	}
	len = pg_redis_list_length(e->value.list_value);
	pg_redis_free_cstring_stack(keybuf, key);
	PG_RETURN_INT64(len);
}

/* -------------------------------------------------------------------------
 * Admin / debug
 * ------------------------------------------------------------------------- */

typedef struct KeysIterState
{
	HASH_SEQ_STATUS seq;
	MemoryContext mcxt;
} KeysIterState;

PG_FUNCTION_INFO_V1(pg_redis_keys);
Datum
pg_redis_keys(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	KeysIterState *st;
	PgRedisEntry *e;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcxt;

		ensure_loaded();
		funcctx = SRF_FIRSTCALL_INIT();
		oldcxt = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		st = (KeysIterState *) palloc0(sizeof(KeysIterState));
		st->mcxt = funcctx->multi_call_memory_ctx;
		pg_redis_store_seq_init(&st->seq);
		funcctx->user_fctx = st;
		MemoryContextSwitchTo(oldcxt);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (KeysIterState *) funcctx->user_fctx;

	while ((e = pg_redis_store_seq_next(&st->seq)) != NULL)
	{
		if (pg_redis_entry_is_expired(e))
			continue;
		SRF_RETURN_NEXT(funcctx, PointerGetDatum(cstring_to_text(e->key)));
	}
	SRF_RETURN_DONE(funcctx);
}

PG_FUNCTION_INFO_V1(pg_redis_flushall);
Datum
pg_redis_flushall(PG_FUNCTION_ARGS)
{
	pg_redis_store_reset();
	pg_redis_persistence_flushall();
	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(pg_redis_memory_usage);
Datum
pg_redis_memory_usage(PG_FUNCTION_ARGS)
{
	ensure_loaded();
	PG_RETURN_INT64((int64) pg_redis_store_memory_usage());
}

PG_FUNCTION_INFO_V1(pg_redis_info);
Datum
pg_redis_info(PG_FUNCTION_ARGS)
{
	StringInfoData s;
	PgRedisStoreStats stats;

	ensure_loaded();
	initStringInfo(&s);
	pg_redis_store_collect_stats(&stats);

	appendStringInfoString(&s, "# pg_redis\n");
	appendStringInfo(&s, "version:1.0\n");
	appendStringInfo(&s, "storage_mode:%s\n",
					 pg_redis_storage_mode ? pg_redis_storage_mode : "session");
	appendStringInfo(&s, "persistence_mode:%s\n",
					 pg_redis_persistence_mode ? pg_redis_persistence_mode : "sync_table");
	appendStringInfo(&s, "background_worker_enabled:%s\n",
					 pg_redis_enable_bgworker ? "yes" : "no");
	appendStringInfo(&s, "key_count:" INT64_FORMAT "\n", stats.key_count);
	appendStringInfo(&s, "memory_usage:" INT64_FORMAT "\n", stats.memory_usage);

	PG_RETURN_TEXT_P(cstring_to_text_with_len(s.data, s.len));
}

PG_FUNCTION_INFO_V1(pg_redis_stats);
Datum
pg_redis_stats(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	Datum		values[7];
	bool		nulls[7] = {false, false, false, false, false, false, false};
	PgRedisStoreStats stats;

	ensure_loaded();
	pg_redis_store_collect_stats(&stats);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	tupdesc = BlessTupleDesc(tupdesc);

	values[0] = Int64GetDatum(stats.key_count);
	values[1] = Int64GetDatum(stats.string_count);
	values[2] = Int64GetDatum(stats.int_count);
	values[3] = Int64GetDatum(stats.hash_count);
	values[4] = Int64GetDatum(stats.list_count);
	values[5] = Int64GetDatum(stats.expiring_count);
	values[6] = Int64GetDatum(stats.memory_usage);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/* -------------------------------------------------------------------------
 * Persistence commands
 * ------------------------------------------------------------------------- */

PG_FUNCTION_INFO_V1(pg_redis_save);
Datum
pg_redis_save(PG_FUNCTION_ARGS)
{
	ensure_loaded();
	(void) pg_redis_persistence_save_snapshot();
	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(pg_redis_bgsave);
Datum
pg_redis_bgsave(PG_FUNCTION_ARGS)
{
	const char *sql =
		"INSERT INTO pgredis.jobs(job_type, schedule_interval, enabled, next_run) "
		"VALUES ('snapshot_save', '1 hour'::interval, true, now())";

	ensure_loaded();
	if (SPI_connect() != SPI_OK_CONNECT)
		PG_RETURN_BOOL(false);
	(void) SPI_execute(sql, false, 0);
	SPI_finish();
	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(pg_redis_bgrewriteaof);
Datum
pg_redis_bgrewriteaof(PG_FUNCTION_ARGS)
{
	ereport(NOTICE,
			(errmsg("pg_redis: AOF rewrite not implemented in v0.1")));
	PG_RETURN_BOOL(false);
}

PG_FUNCTION_INFO_V1(pg_redis_run_job);
Datum
pg_redis_run_job(PG_FUNCTION_ARGS)
{
	int64		job_id = PG_GETARG_INT64(0);

	ensure_loaded();
	PG_RETURN_BOOL(pg_redis_jobs_run_one(job_id));
}
