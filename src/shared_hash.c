#include "postgres.h"
#include "utils/dsa.h"
#include "utils/elog.h"

#include <string.h>

#include "types.h"
#include "shmem.h"
#include "hash_value.h"
#include "shared_hash.h"

/* Allocate a new field node in DSA and fill it with `field` and `value`.
 * Returns the dsa_pointer to the new node, or InvalidDsaPointer on OOM. */
static dsa_pointer
new_field_node(const char *field, Size fieldlen,
			   const unsigned char *value, Size value_len)
{
	dsa_pointer node_p;
	PgRedisSharedHashField *node;
	dsa_pointer field_p,
				value_p;

	node_p = pg_redis_shared_palloc(sizeof(PgRedisSharedHashField));
	if (node_p == InvalidDsaPointer)
		return InvalidDsaPointer;

	field_p = pg_redis_shared_palloc(fieldlen);
	if (field_p == InvalidDsaPointer)
	{
		pg_redis_shared_pfree(node_p);
		return InvalidDsaPointer;
	}
	memcpy(pg_redis_shared_addr(field_p), field, fieldlen);

	value_p = (value_len > 0) ? pg_redis_shared_palloc(value_len) : InvalidDsaPointer;
	if (value_len > 0 && value_p == InvalidDsaPointer)
	{
		pg_redis_shared_pfree(field_p);
		pg_redis_shared_pfree(node_p);
		return InvalidDsaPointer;
	}
	if (value_len > 0)
		memcpy(pg_redis_shared_addr(value_p), value, value_len);

	node = (PgRedisSharedHashField *) pg_redis_shared_addr(node_p);
	node->next = InvalidDsaPointer;
	node->field_len = (uint16) fieldlen;
	node->value_len = (uint16) value_len;
	node->field_dsa = field_p;
	node->value_dsa = value_p;
	return node_p;
}

static void
free_field_node(dsa_pointer node_p)
{
	PgRedisSharedHashField *node;

	if (node_p == InvalidDsaPointer)
		return;
	node = (PgRedisSharedHashField *) pg_redis_shared_addr(node_p);
	if (node == NULL)
		return;
	pg_redis_shared_pfree(node->field_dsa);
	pg_redis_shared_pfree(node->value_dsa);
	pg_redis_shared_pfree(node_p);
}

void
pg_redis_shared_hash_free(dsa_pointer head)
{
	dsa_pointer cur = head;

	while (cur != InvalidDsaPointer)
	{
		PgRedisSharedHashField *node =
			(PgRedisSharedHashField *) pg_redis_shared_addr(cur);
		dsa_pointer next = node ? node->next : InvalidDsaPointer;

		free_field_node(cur);
		cur = next;
	}
}

/* Find a field node by name. Returns the dsa_pointer to the node, or
 * InvalidDsaPointer if not found. Also returns the previous node's pointer
 * slot via *prev_slot — useful for unlinking. */
static dsa_pointer
find_field_node(dsa_pointer head,
				const char *field, Size fieldlen,
				dsa_pointer **prev_slot_out)
{
	dsa_pointer cur = head;
	dsa_pointer *prev_slot = NULL;

	while (cur != InvalidDsaPointer)
	{
		PgRedisSharedHashField *node =
			(PgRedisSharedHashField *) pg_redis_shared_addr(cur);
		const char *cur_field;

		if (node == NULL)
			break;
		cur_field = (const char *) pg_redis_shared_addr(node->field_dsa);
		if (node->field_len == fieldlen &&
			cur_field != NULL &&
			memcmp(cur_field, field, fieldlen) == 0)
		{
			if (prev_slot_out)
				*prev_slot_out = prev_slot;
			return cur;
		}
		prev_slot = &node->next;
		cur = node->next;
	}
	if (prev_slot_out)
		*prev_slot_out = NULL;
	return InvalidDsaPointer;
}

void
pg_redis_shared_hash_set(dsa_pointer *inout_head,
						 const char *field, Size fieldlen,
						 const unsigned char *value, Size value_len,
						 bool *was_new)
{
	dsa_pointer existing;
	PgRedisSharedHashField *node;
	dsa_pointer new_value_p;

	existing = find_field_node(*inout_head, field, fieldlen, NULL);
	if (existing != InvalidDsaPointer)
	{
		/* Overwrite in place: free old value, alloc new. */
		node = (PgRedisSharedHashField *) pg_redis_shared_addr(existing);
		new_value_p = (value_len > 0) ?
			pg_redis_shared_palloc(value_len) : InvalidDsaPointer;
		if (value_len > 0 && new_value_p == InvalidDsaPointer)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("pg_redis: out of shared memory in HSET overwrite"),
					 errhint("Raise pg_redis.shared_max_memory.")));
		if (value_len > 0)
			memcpy(pg_redis_shared_addr(new_value_p), value, value_len);
		pg_redis_shared_pfree(node->value_dsa);
		node->value_dsa = new_value_p;
		node->value_len = (uint16) value_len;
		if (was_new)
			*was_new = false;
		return;
	}

	/* Insert at head. */
	{
		dsa_pointer new_node = new_field_node(field, fieldlen, value, value_len);
		PgRedisSharedHashField *nn;

		if (new_node == InvalidDsaPointer)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("pg_redis: out of shared memory in HSET insert"),
					 errhint("Raise pg_redis.shared_max_memory.")));
		nn = (PgRedisSharedHashField *) pg_redis_shared_addr(new_node);
		nn->next = *inout_head;
		*inout_head = new_node;
		if (was_new)
			*was_new = true;
	}
}

bool
pg_redis_shared_hash_get(dsa_pointer head,
						 const char *field, Size fieldlen,
						 unsigned char **out_value, Size *out_value_len)
{
	dsa_pointer p = find_field_node(head, field, fieldlen, NULL);
	PgRedisSharedHashField *node;
	const unsigned char *src;
	unsigned char *buf;

	if (p == InvalidDsaPointer)
	{
		if (out_value)
			*out_value = NULL;
		if (out_value_len)
			*out_value_len = 0;
		return false;
	}
	node = (PgRedisSharedHashField *) pg_redis_shared_addr(p);
	src = (const unsigned char *) pg_redis_shared_addr(node->value_dsa);

	buf = (unsigned char *) palloc(node->value_len > 0 ? node->value_len : 1);
	if (node->value_len > 0 && src != NULL)
		memcpy(buf, src, node->value_len);

	*out_value = buf;
	*out_value_len = node->value_len;
	return true;
}

bool
pg_redis_shared_hash_exists(dsa_pointer head,
							const char *field, Size fieldlen)
{
	return find_field_node(head, field, fieldlen, NULL) != InvalidDsaPointer;
}

bool
pg_redis_shared_hash_del(dsa_pointer *inout_head,
						 const char *field, Size fieldlen)
{
	dsa_pointer *prev_slot = NULL;
	dsa_pointer victim = find_field_node(*inout_head, field, fieldlen, &prev_slot);
	PgRedisSharedHashField *victim_node;
	dsa_pointer victim_next;

	if (victim == InvalidDsaPointer)
		return false;

	victim_node = (PgRedisSharedHashField *) pg_redis_shared_addr(victim);
	victim_next = victim_node->next;

	if (prev_slot == NULL)
	{
		/* Victim was the head. */
		*inout_head = victim_next;
	}
	else
	{
		*prev_slot = victim_next;
	}

	free_field_node(victim);
	return true;
}

int64
pg_redis_shared_hash_count(dsa_pointer head)
{
	int64		n = 0;
	dsa_pointer cur = head;

	while (cur != InvalidDsaPointer)
	{
		PgRedisSharedHashField *node =
			(PgRedisSharedHashField *) pg_redis_shared_addr(cur);

		if (node == NULL)
			break;
		n++;
		cur = node->next;
	}
	return n;
}

PgRedisHash *
pg_redis_shared_hash_materialize(dsa_pointer head)
{
	PgRedisHash *h;
	dsa_pointer cur = head;

	if (head == InvalidDsaPointer)
		return NULL;

	/* Allocate the scratch hash in CurrentMemoryContext so it dies with the
	 * SQL statement. Allocating in PgRedisMemoryContext (the long-lived
	 * session-store context) would leak the hash on every shared-mode lookup,
	 * which compounds into hundreds of MB after a few thousand HSET/HDEL on a
	 * single key. */
	h = pg_redis_hash_create_in(CurrentMemoryContext);

	while (cur != InvalidDsaPointer)
	{
		PgRedisSharedHashField *node =
			(PgRedisSharedHashField *) pg_redis_shared_addr(cur);
		const char *field_bytes;
		const char *value_bytes;
		char		fieldbuf[PG_REDIS_MAX_FIELD_SIZE + 1];

		if (node == NULL)
			break;
		field_bytes = (const char *) pg_redis_shared_addr(node->field_dsa);
		value_bytes = (const char *) pg_redis_shared_addr(node->value_dsa);
		if (node->field_len > PG_REDIS_MAX_FIELD_SIZE)
			break;					/* corrupt */
		memcpy(fieldbuf, field_bytes, node->field_len);
		fieldbuf[node->field_len] = '\0';

		(void) pg_redis_hash_set(h, fieldbuf,
								 value_bytes ? value_bytes : "",
								 node->value_len);
		cur = node->next;
	}
	return h;
}
