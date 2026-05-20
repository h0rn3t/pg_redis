#include "postgres.h"
#include "utils/dsa.h"
#include "utils/elog.h"

#include <string.h>

#include "types.h"
#include "shmem.h"
#include "list.h"
#include "shared_list.h"

static dsa_pointer
new_node(const unsigned char *value, Size value_len, int64 ord)
{
	dsa_pointer node_p = pg_redis_shared_palloc(sizeof(PgRedisSharedListNode));
	PgRedisSharedListNode *node;
	dsa_pointer value_p = InvalidDsaPointer;

	if (node_p == InvalidDsaPointer)
		return InvalidDsaPointer;
	if (value_len > 0)
	{
		value_p = pg_redis_shared_palloc(value_len);
		if (value_p == InvalidDsaPointer)
		{
			pg_redis_shared_pfree(node_p);
			return InvalidDsaPointer;
		}
		memcpy(pg_redis_shared_addr(value_p), value, value_len);
	}

	node = (PgRedisSharedListNode *) pg_redis_shared_addr(node_p);
	node->prev = InvalidDsaPointer;
	node->next = InvalidDsaPointer;
	node->ord = ord;
	node->value_len = (uint16) value_len;
	node->value_dsa = value_p;
	return node_p;
}

static void
free_node(dsa_pointer node_p)
{
	PgRedisSharedListNode *node;

	if (node_p == InvalidDsaPointer)
		return;
	node = (PgRedisSharedListNode *) pg_redis_shared_addr(node_p);
	if (node == NULL)
		return;
	pg_redis_shared_pfree(node->value_dsa);
	pg_redis_shared_pfree(node_p);
}

void
pg_redis_shared_list_free(PgRedisSharedListMeta *meta)
{
	dsa_pointer cur = meta->head;

	while (cur != InvalidDsaPointer)
	{
		PgRedisSharedListNode *node =
			(PgRedisSharedListNode *) pg_redis_shared_addr(cur);
		dsa_pointer next = node ? node->next : InvalidDsaPointer;

		free_node(cur);
		cur = next;
	}
	meta->head = InvalidDsaPointer;
	meta->tail = InvalidDsaPointer;
	meta->length = 0;
	meta->min_ord = 0;
	meta->max_ord = -1;
	meta->ord_initialized = false;
}

int64
pg_redis_shared_list_lpush(PgRedisSharedListMeta *meta,
						   const unsigned char *value, Size value_len)
{
	int64		ord;
	dsa_pointer node_p;
	PgRedisSharedListNode *node;

	if (!meta->ord_initialized)
		ord = 0;
	else
		ord = meta->min_ord - 1;

	node_p = new_node(value, value_len, ord);
	if (node_p == InvalidDsaPointer)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("pg_redis: out of shared memory in LPUSH"),
				 errhint("Raise pg_redis.shared_max_memory.")));

	node = (PgRedisSharedListNode *) pg_redis_shared_addr(node_p);
	node->next = meta->head;
	if (meta->head != InvalidDsaPointer)
	{
		PgRedisSharedListNode *old_head =
			(PgRedisSharedListNode *) pg_redis_shared_addr(meta->head);

		old_head->prev = node_p;
	}
	else
	{
		meta->tail = node_p;
	}
	meta->head = node_p;
	meta->length++;
	if (!meta->ord_initialized)
	{
		meta->min_ord = ord;
		meta->max_ord = ord;
		meta->ord_initialized = true;
	}
	else
	{
		meta->min_ord = ord;
	}
	return meta->length;
}

int64
pg_redis_shared_list_rpush(PgRedisSharedListMeta *meta,
						   const unsigned char *value, Size value_len)
{
	int64		ord;
	dsa_pointer node_p;
	PgRedisSharedListNode *node;

	if (!meta->ord_initialized)
		ord = 0;
	else
		ord = meta->max_ord + 1;

	node_p = new_node(value, value_len, ord);
	if (node_p == InvalidDsaPointer)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("pg_redis: out of shared memory in RPUSH"),
				 errhint("Raise pg_redis.shared_max_memory.")));

	node = (PgRedisSharedListNode *) pg_redis_shared_addr(node_p);
	node->prev = meta->tail;
	if (meta->tail != InvalidDsaPointer)
	{
		PgRedisSharedListNode *old_tail =
			(PgRedisSharedListNode *) pg_redis_shared_addr(meta->tail);

		old_tail->next = node_p;
	}
	else
	{
		meta->head = node_p;
	}
	meta->tail = node_p;
	meta->length++;
	if (!meta->ord_initialized)
	{
		meta->min_ord = ord;
		meta->max_ord = ord;
		meta->ord_initialized = true;
	}
	else
	{
		meta->max_ord = ord;
	}
	return meta->length;
}

static unsigned char *
copy_value_out(PgRedisSharedListNode *node, Size *out_len)
{
	unsigned char *buf;
	const unsigned char *src;

	buf = (unsigned char *) palloc(node->value_len > 0 ? node->value_len : 1);
	src = (const unsigned char *) pg_redis_shared_addr(node->value_dsa);
	if (node->value_len > 0 && src != NULL)
		memcpy(buf, src, node->value_len);
	*out_len = node->value_len;
	return buf;
}

bool
pg_redis_shared_list_lpop(PgRedisSharedListMeta *meta,
						  unsigned char **out_value, Size *out_len,
						  int64 *out_ord)
{
	dsa_pointer head_p = meta->head;
	PgRedisSharedListNode *head;
	dsa_pointer next_p;

	if (head_p == InvalidDsaPointer)
		return false;
	head = (PgRedisSharedListNode *) pg_redis_shared_addr(head_p);
	if (head == NULL)
		return false;				/* defensive: bucket present but DSA resolve failed */

	*out_value = copy_value_out(head, out_len);
	*out_ord = head->ord;

	next_p = head->next;
	meta->head = next_p;
	if (next_p != InvalidDsaPointer)
	{
		PgRedisSharedListNode *next =
			(PgRedisSharedListNode *) pg_redis_shared_addr(next_p);

		next->prev = InvalidDsaPointer;
		meta->min_ord = next->ord;
	}
	else
	{
		meta->tail = InvalidDsaPointer;
		meta->min_ord = 0;
		meta->max_ord = -1;
		meta->ord_initialized = false;
	}
	meta->length--;
	free_node(head_p);
	return true;
}

bool
pg_redis_shared_list_rpop(PgRedisSharedListMeta *meta,
						  unsigned char **out_value, Size *out_len,
						  int64 *out_ord)
{
	dsa_pointer tail_p = meta->tail;
	PgRedisSharedListNode *tail;
	dsa_pointer prev_p;

	if (tail_p == InvalidDsaPointer)
		return false;
	tail = (PgRedisSharedListNode *) pg_redis_shared_addr(tail_p);
	if (tail == NULL)
		return false;				/* defensive: bucket present but DSA resolve failed */

	*out_value = copy_value_out(tail, out_len);
	*out_ord = tail->ord;

	prev_p = tail->prev;
	meta->tail = prev_p;
	if (prev_p != InvalidDsaPointer)
	{
		PgRedisSharedListNode *prev =
			(PgRedisSharedListNode *) pg_redis_shared_addr(prev_p);

		prev->next = InvalidDsaPointer;
		meta->max_ord = prev->ord;
	}
	else
	{
		meta->head = InvalidDsaPointer;
		meta->min_ord = 0;
		meta->max_ord = -1;
		meta->ord_initialized = false;
	}
	meta->length--;
	free_node(tail_p);
	return true;
}

PgRedisList *
pg_redis_shared_list_materialize(const PgRedisSharedListMeta *meta)
{
	PgRedisList *l;
	dsa_pointer cur;

	if (meta == NULL || meta->head == InvalidDsaPointer)
		return NULL;

	/* Scratch list dies with the statement — see the matching note in
	 * pg_redis_shared_hash_materialize. */
	l = pg_redis_list_create_in(CurrentMemoryContext);

	cur = meta->head;
	while (cur != InvalidDsaPointer)
	{
		PgRedisSharedListNode *node =
			(PgRedisSharedListNode *) pg_redis_shared_addr(cur);
		const char *src;

		if (node == NULL)
			break;
		src = (const char *) pg_redis_shared_addr(node->value_dsa);
		(void) pg_redis_list_rpush(l,
								   src ? src : "",
								   node->value_len);
		/* Override the assigned ord with the durable one and clear the
		 * pending_insert flag so this materialized copy doesn't re-publish
		 * inserts on writeback. */
		if (l->tail != NULL)
		{
			l->tail->ord = node->ord;
			l->tail->pending_insert = false;
		}
		cur = node->next;
	}
	l->min_ord = meta->min_ord;
	l->max_ord = meta->max_ord;
	l->ord_initialized = meta->ord_initialized;
	return l;
}
