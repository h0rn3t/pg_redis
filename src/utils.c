#include "postgres.h"
#include "varatt.h"
#include "executor/spi.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "fmgr.h"

#include <string.h>
#include <inttypes.h>
#include <errno.h>
#include <stdlib.h>

#include "types.h"
#include "utils.h"
#include "shmem.h"

static MemoryContext PgRedisMemoryContext = NULL;

/*
 * SPI_connect wrapper that asserts (debug-only) the no-SPI-under-LWLock
 * invariant: SPI internally waits on catalog/buffer LWLocks and takes
 * heavyweight relation locks, which must never happen while a pg_redis LWLock
 * is held. Routing pg_redis's SPI_connect calls through here catches a
 * regression at the connect, not later as a deadlock under load. Returns
 * exactly what SPI_connect returns.
 */
int
pg_redis_spi_connect(void)
{
	pg_redis_assert_no_pg_redis_lwlock_held();
	return SPI_connect();
}

MemoryContext
pg_redis_memcxt(void)
{
	if (PgRedisMemoryContext == NULL)
		PgRedisMemoryContext = AllocSetContextCreate(TopMemoryContext,
													 "PgRedisMemoryContext",
													 ALLOCSET_DEFAULT_SIZES);
	return PgRedisMemoryContext;
}

void
pg_redis_memcxt_reset(void)
{
	if (PgRedisMemoryContext != NULL)
		MemoryContextReset(PgRedisMemoryContext);
}

void
pg_redis_check_key_len(const char *key, Size len)
{
	int			max = pg_redis_max_key_size;

	if (max <= 0 || max > PG_REDIS_MAX_KEY_SIZE)
		max = PG_REDIS_MAX_KEY_SIZE;

	if (len == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_redis: key must not be empty")));
	if (len > (Size) max)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("pg_redis: key length %zu exceeds maximum %d",
						len, max)));
	(void) key;
}

void
pg_redis_check_field_len(const char *field, Size len)
{
	if (len == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_redis: hash field must not be empty")));
	if (len > PG_REDIS_MAX_FIELD_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("pg_redis: field length %zu exceeds maximum %d",
						len, PG_REDIS_MAX_FIELD_SIZE)));
	(void) field;
}

void
pg_redis_check_value_len(Size len)
{
	int			max = pg_redis_max_value_size;

	if (max <= 0)
		return;
	if (len > (Size) max)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("pg_redis: value length %zu exceeds maximum %d",
						len, max)));
}

char *
pg_redis_palloc_string(const char *src, Size len)
{
	MemoryContext old;
	char	   *dst;

	old = MemoryContextSwitchTo(pg_redis_memcxt());
	dst = (char *) palloc(len + 1);
	MemoryContextSwitchTo(old);

	if (len > 0 && src != NULL)
		memcpy(dst, src, len);
	dst[len] = '\0';
	return dst;
}

char *
pg_redis_text_to_pstring(text *t, Size *out_len)
{
	Size		len;
	char	   *p;

	if (t == NULL)
	{
		if (out_len)
			*out_len = 0;
		return NULL;
	}
	len = VARSIZE_ANY_EXHDR(t);
	p = pg_redis_palloc_string(VARDATA_ANY(t), len);
	if (out_len)
		*out_len = len;
	return p;
}

bool
pg_redis_parse_int64(const char *s, Size len, int64 *out)
{
	char	   *end;
	char		buf[32];
	int64		v;

	if (s == NULL || len == 0 || len >= sizeof(buf))
		return false;
	memcpy(buf, s, len);
	buf[len] = '\0';

	errno = 0;
	v = (int64) strtoll(buf, &end, 10);
	if (errno != 0 || end == buf || *end != '\0')
		return false;
	*out = v;
	return true;
}

char *
pg_redis_format_int64(int64 v, Size *out_len)
{
	char		buf[32];
	int			n;

	n = snprintf(buf, sizeof(buf), INT64_FORMAT, v);
	if (n < 0 || (Size) n >= sizeof(buf))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_redis: int64 format overflow")));

	if (out_len)
		*out_len = (Size) n;
	return pg_redis_palloc_string(buf, (Size) n);
}

PgRedisPersistenceMode
pg_redis_resolve_persistence_mode(void)
{
	const char *m = pg_redis_persistence_mode;

	if (m == NULL)
		return PG_REDIS_PERSIST_SYNC_TABLE;
	if (strcmp(m, "none") == 0)
		return PG_REDIS_PERSIST_NONE;
	if (strcmp(m, "sync_table") == 0)
		return PG_REDIS_PERSIST_SYNC_TABLE;
	if (strcmp(m, "async_table") == 0)
		return PG_REDIS_PERSIST_ASYNC_TABLE;
	if (strcmp(m, "snapshot") == 0)
		return PG_REDIS_PERSIST_SNAPSHOT;
	if (strcmp(m, "aof") == 0)
		return PG_REDIS_PERSIST_AOF;
	/* Unknown -> safest default. */
	return PG_REDIS_PERSIST_SYNC_TABLE;
}

PgRedisStorageMode
pg_redis_resolve_storage_mode(void)
{
	const char *m = pg_redis_storage_mode;

	if (m == NULL)
		return PG_REDIS_STORAGE_SESSION;
	if (strcmp(m, "shared") == 0)
		return PG_REDIS_STORAGE_SHARED;
	return PG_REDIS_STORAGE_SESSION;
}

bool
pg_redis_storage_is_shared(void)
{
	/* The GUC value alone is not sufficient — the shared HTAB exists only
	 * when the extension was loaded via shared_preload_libraries (so the
	 * shmem_request_hook actually ran). Probing the header is the cheapest
	 * way to detect "shared mode actually initialized". */
	if (pg_redis_resolve_storage_mode() != PG_REDIS_STORAGE_SHARED)
		return false;
	return pg_redis_shmem_header() != NULL;
}

PgRedisPersistenceMode
pg_redis_effective_persistence_mode(void)
{
	PgRedisPersistenceMode m = pg_redis_resolve_persistence_mode();

	/* async_table requires shared storage to be meaningful — otherwise each
	 * backend would silently lose visibility into others' acked writes. Fall
	 * back to sync_table semantics; the GUC check hook has emitted a WARNING
	 * at configuration time. */
	if (m == PG_REDIS_PERSIST_ASYNC_TABLE && !pg_redis_storage_is_shared())
		return PG_REDIS_PERSIST_SYNC_TABLE;
	return m;
}
