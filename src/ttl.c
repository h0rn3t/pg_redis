#include "postgres.h"
#include "datatype/timestamp.h"
#include "utils/timestamp.h"
#include "utils/hsearch.h"

#include "types.h"
#include "ttl.h"
#include "kv_store.h"

bool
pg_redis_entry_is_expired(const PgRedisEntry *e)
{
	if (e == NULL || !e->has_expire)
		return false;
	return GetCurrentTimestamp() >= e->expire_at;
}

bool
pg_redis_set_expire_seconds(const char *key, int32 seconds)
{
	PgRedisEntry *e;

	e = pg_redis_store_lookup(key, NULL);
	if (e == NULL)
		return false;

	if (seconds <= 0)
	{
		pg_redis_store_remove(key);
		return true;
	}

	e->has_expire = true;
	e->expire_at = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
											   ((int64) seconds) * 1000);
	e->dirty = true;
	e->version++;
	return true;
}

int32
pg_redis_ttl_seconds(const char *key)
{
	PgRedisEntry *e;
	TimestampTz now;
	int64		diff_us;

	e = pg_redis_store_lookup(key, NULL);
	if (e == NULL)
		return -2;
	if (!e->has_expire)
		return -1;

	now = GetCurrentTimestamp();
	if (now >= e->expire_at)
	{
		pg_redis_store_remove(key);
		return -2;
	}

	diff_us = (int64) (e->expire_at - now);
	/* Round up to next second so a fresh EXPIRE 60 returns 60, not 59. */
	return (int32) ((diff_us + 999999) / 1000000);
}

int64
pg_redis_ttl_sweep(void)
{
	HASH_SEQ_STATUS s;
	PgRedisEntry *e;
	int64		removed = 0;
	/* Collect victim keys first, remove afterwards: we must not call
	 * hash_search(HASH_REMOVE) inside a hash_seq_search() iteration unless
	 * we use hash_seq_term first. The cleaner approach is two-pass. */
	int			cap = 64;
	int			n = 0;
	char	  **victims;

	victims = (char **) palloc(sizeof(char *) * cap);
	pg_redis_store_seq_init(&s);
	while ((e = pg_redis_store_seq_next(&s)) != NULL)
	{
		if (pg_redis_entry_is_expired(e))
		{
			if (n == cap)
			{
				cap *= 2;
				victims = (char **) repalloc(victims, sizeof(char *) * cap);
			}
			victims[n++] = pstrdup(e->key);
		}
	}

	for (int i = 0; i < n; i++)
	{
		if (pg_redis_store_remove(victims[i]))
			removed++;
		pfree(victims[i]);
	}
	pfree(victims);
	return removed;
}
