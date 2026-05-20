#include "postgres.h"
#include "utils/memutils.h"

#include <string.h>

#include "types.h"
#include "list.h"
#include "utils.h"

PgRedisList *
pg_redis_list_create(void)
{
	MemoryContext old = MemoryContextSwitchTo(pg_redis_memcxt());
	PgRedisList *l = (PgRedisList *) palloc0(sizeof(PgRedisList));

	/* ord_initialized=false signals "first push picks ord=0". After at least
	 * one push has happened, min_ord/max_ord are authoritative. */
	l->min_ord = 0;
	l->max_ord = -1;
	l->ord_initialized = false;
	l->pending_delete_ords = NULL;
	MemoryContextSwitchTo(old);
	return l;
}

void
pg_redis_list_free(PgRedisList *list)
{
	PgRedisListNode *node;
	PgRedisOrdNode *o;

	if (list == NULL)
		return;

	node = list->head;
	while (node != NULL)
	{
		PgRedisListNode *next = node->next;

		if (node->value)
			pfree(node->value);
		pfree(node);
		node = next;
	}

	o = list->pending_delete_ords;
	while (o != NULL)
	{
		PgRedisOrdNode *next = o->next;

		pfree(o);
		o = next;
	}
	list->pending_delete_ords = NULL;

	pfree(list);
}

static PgRedisListNode *
new_node(const char *value, Size len)
{
	MemoryContext old = MemoryContextSwitchTo(pg_redis_memcxt());
	PgRedisListNode *n = (PgRedisListNode *) palloc0(sizeof(PgRedisListNode));

	n->value = (char *) palloc(len + 1);
	MemoryContextSwitchTo(old);

	if (len > 0)
		memcpy(n->value, value, len);
	n->value[len] = '\0';
	n->value_len = len;
	return n;
}

int64
pg_redis_list_lpush(PgRedisList *list, const char *value, Size len)
{
	PgRedisListNode *n = new_node(value, len);

	if (!list->ord_initialized)
	{
		n->ord = 0;
		list->min_ord = 0;
		list->max_ord = 0;
		list->ord_initialized = true;
	}
	else
	{
		n->ord = list->min_ord - 1;
		list->min_ord = n->ord;
	}
	n->pending_insert = true;

	n->next = list->head;
	if (list->head != NULL)
		list->head->prev = n;
	list->head = n;
	if (list->tail == NULL)
		list->tail = n;
	list->length++;
	list->memory_usage += sizeof(PgRedisListNode) + len;
	return list->length;
}

int64
pg_redis_list_rpush(PgRedisList *list, const char *value, Size len)
{
	PgRedisListNode *n = new_node(value, len);

	if (!list->ord_initialized)
	{
		n->ord = 0;
		list->min_ord = 0;
		list->max_ord = 0;
		list->ord_initialized = true;
	}
	else
	{
		n->ord = list->max_ord + 1;
		list->max_ord = n->ord;
	}
	n->pending_insert = true;

	n->prev = list->tail;
	if (list->tail != NULL)
		list->tail->next = n;
	list->tail = n;
	if (list->head == NULL)
		list->head = n;
	list->length++;
	list->memory_usage += sizeof(PgRedisListNode) + len;
	return list->length;
}

static char *
detach_value(PgRedisListNode *n, Size *out_len)
{
	char	   *out;

	if (out_len)
		*out_len = n->value_len;
	out = pg_redis_palloc_string(n->value, n->value_len);
	pfree(n->value);
	n->value = NULL;
	return out;
}

/* Record a delete-at-flush for the given ord, but only if the node was
 * already durable (not freshly inserted within this transaction). */
static void
record_pending_delete(PgRedisList *list, PgRedisListNode *n)
{
	MemoryContext old;
	PgRedisOrdNode *o;

	if (n->pending_insert)
		return;					/* never went durable — nothing to delete */

	old = MemoryContextSwitchTo(pg_redis_memcxt());
	o = (PgRedisOrdNode *) palloc(sizeof(PgRedisOrdNode));
	o->ord = n->ord;
	o->next = list->pending_delete_ords;
	list->pending_delete_ords = o;
	MemoryContextSwitchTo(old);
}

char *
pg_redis_list_lpop(PgRedisList *list, Size *out_len)
{
	PgRedisListNode *n;
	char	   *out;
	Size		node_bytes;

	if (list == NULL || list->head == NULL)
	{
		if (out_len)
			*out_len = 0;
		return NULL;
	}

	n = list->head;
	node_bytes = sizeof(PgRedisListNode) + n->value_len;
	out = detach_value(n, out_len);

	record_pending_delete(list, n);

	list->head = n->next;
	if (list->head != NULL)
	{
		list->head->prev = NULL;
		list->min_ord = list->head->ord;
	}
	else
	{
		list->tail = NULL;
		/* List drained; min/max are now meaningless until next push. */
		list->ord_initialized = false;
		list->min_ord = 0;
		list->max_ord = -1;
	}
	list->length--;
	if (list->memory_usage >= node_bytes)
		list->memory_usage -= node_bytes;
	pfree(n);
	return out;
}

char *
pg_redis_list_rpop(PgRedisList *list, Size *out_len)
{
	PgRedisListNode *n;
	char	   *out;
	Size		node_bytes;

	if (list == NULL || list->tail == NULL)
	{
		if (out_len)
			*out_len = 0;
		return NULL;
	}

	n = list->tail;
	node_bytes = sizeof(PgRedisListNode) + n->value_len;
	out = detach_value(n, out_len);

	record_pending_delete(list, n);

	list->tail = n->prev;
	if (list->tail != NULL)
	{
		list->tail->next = NULL;
		list->max_ord = list->tail->ord;
	}
	else
	{
		list->head = NULL;
		list->ord_initialized = false;
		list->min_ord = 0;
		list->max_ord = -1;
	}
	list->length--;
	if (list->memory_usage >= node_bytes)
		list->memory_usage -= node_bytes;
	pfree(n);
	return out;
}

int64
pg_redis_list_length(const PgRedisList *list)
{
	return list == NULL ? 0 : list->length;
}

Size
pg_redis_list_memory(const PgRedisList *list)
{
	return list == NULL ? 0 : list->memory_usage;
}
