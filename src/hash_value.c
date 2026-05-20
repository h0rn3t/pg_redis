#include "postgres.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#include <string.h>

#include "types.h"
#include "hash_value.h"
#include "utils.h"

PgRedisHash *
pg_redis_hash_create(void)
{
	MemoryContext old;
	PgRedisHash *h;
	HASHCTL		ctl;

	old = MemoryContextSwitchTo(pg_redis_memcxt());
	h = (PgRedisHash *) palloc0(sizeof(PgRedisHash));

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(((PgRedisHashField *) 0)->field);
	ctl.entrysize = sizeof(PgRedisHashField);
	ctl.hcxt = pg_redis_memcxt();

	h->fields = hash_create("pg_redis_hash_fields",
							16,
							&ctl,
							HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
	MemoryContextSwitchTo(old);
	return h;
}

void
pg_redis_hash_free(PgRedisHash *h)
{
	HASH_SEQ_STATUS s;
	PgRedisHashField *f;
	PgRedisHashTombstone *t;

	if (h == NULL)
		return;

	if (h->fields != NULL)
	{
		hash_seq_init(&s, h->fields);
		while ((f = (PgRedisHashField *) hash_seq_search(&s)) != NULL)
		{
			if (f->value)
				pfree(f->value);
		}
		hash_destroy(h->fields);
		h->fields = NULL;
	}

	t = h->tombstones;
	while (t != NULL)
	{
		PgRedisHashTombstone *next = t->next;

		if (t->field != NULL)
			pfree(t->field);
		pfree(t);
		t = next;
	}
	h->tombstones = NULL;

	pfree(h);
}

bool
pg_redis_hash_set(PgRedisHash *h, const char *field,
				  const char *value, Size value_len)
{
	bool		found;
	PgRedisHashField *f;
	char		fieldbuf[PG_REDIS_MAX_FIELD_SIZE + 1];
	Size		flen = strlen(field);

	if (flen > PG_REDIS_MAX_FIELD_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("pg_redis: hash field length %zu exceeds maximum %d",
						flen, PG_REDIS_MAX_FIELD_SIZE)));

	memset(fieldbuf, 0, sizeof(fieldbuf));
	memcpy(fieldbuf, field, flen);

	f = (PgRedisHashField *) hash_search(h->fields, fieldbuf, HASH_ENTER, &found);

	if (found)
	{
		if (f->value)
		{
			h->memory_usage -= f->value_len;
			pfree(f->value);
		}
	}
	else
	{
		h->field_count++;
		h->memory_usage += sizeof(PgRedisHashField);
	}

	f->value = pg_redis_palloc_string(value, value_len);
	f->value_len = value_len;
	f->dirty = true;
	h->memory_usage += value_len;
	return !found;
}

const char *
pg_redis_hash_get(PgRedisHash *h, const char *field, Size *out_len)
{
	bool		found;
	PgRedisHashField *f;
	char		fieldbuf[PG_REDIS_MAX_FIELD_SIZE + 1];
	Size		flen = strlen(field);

	if (flen > PG_REDIS_MAX_FIELD_SIZE)
		return NULL;
	memset(fieldbuf, 0, sizeof(fieldbuf));
	memcpy(fieldbuf, field, flen);

	f = (PgRedisHashField *) hash_search(h->fields, fieldbuf, HASH_FIND, &found);
	if (!found)
		return NULL;
	if (out_len)
		*out_len = f->value_len;
	return f->value;
}

bool
pg_redis_hash_exists(PgRedisHash *h, const char *field)
{
	Size		dummy;

	return pg_redis_hash_get(h, field, &dummy) != NULL;
}

bool
pg_redis_hash_del(PgRedisHash *h, const char *field)
{
	bool		found;
	PgRedisHashField *f;
	char		fieldbuf[PG_REDIS_MAX_FIELD_SIZE + 1];
	Size		flen = strlen(field);
	MemoryContext old;
	PgRedisHashTombstone *t;

	if (flen > PG_REDIS_MAX_FIELD_SIZE)
		return false;
	memset(fieldbuf, 0, sizeof(fieldbuf));
	memcpy(fieldbuf, field, flen);

	f = (PgRedisHashField *) hash_search(h->fields, fieldbuf, HASH_FIND, &found);
	if (!found)
		return false;

	if (f->value)
	{
		if (h->memory_usage >= f->value_len)
			h->memory_usage -= f->value_len;
		pfree(f->value);
		f->value = NULL;
	}
	hash_search(h->fields, fieldbuf, HASH_REMOVE, &found);
	if (h->memory_usage >= sizeof(PgRedisHashField))
		h->memory_usage -= sizeof(PgRedisHashField);
	if (h->field_count > 0)
		h->field_count--;

	/* Record a tombstone so the next flush deletes the field's durable row.
	 * Allocated in PgRedisMemoryContext so it survives the command's
	 * CurrentMemoryContext tear-down. */
	old = MemoryContextSwitchTo(pg_redis_memcxt());
	t = (PgRedisHashTombstone *) palloc(sizeof(PgRedisHashTombstone));
	t->field = (char *) palloc(flen + 1);
	memcpy(t->field, field, flen);
	t->field[flen] = '\0';
	t->next = h->tombstones;
	h->tombstones = t;
	MemoryContextSwitchTo(old);

	return true;
}

int64
pg_redis_hash_field_count(const PgRedisHash *h)
{
	return h == NULL ? 0 : h->field_count;
}

Size
pg_redis_hash_memory(const PgRedisHash *h)
{
	return h == NULL ? 0 : h->memory_usage;
}
