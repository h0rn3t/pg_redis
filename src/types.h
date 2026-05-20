#ifndef PG_REDIS_TYPES_H
#define PG_REDIS_TYPES_H

#include "postgres.h"
#include "datatype/timestamp.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/dsa.h"

/* Hard limits — also enforced via GUC pg_redis.max_key_size / max_value_size. */
#define PG_REDIS_MAX_KEY_SIZE   1024
#define PG_REDIS_MAX_FIELD_SIZE 1024

/* Binary TLV type tags for pgredis.store.value bytea.
 * Layout: [u8 tag][u32 length_le][payload].
 * Used by the persistence module for string/int values; hash/list payloads
 * live in per-element tables and do not use this column. */
#define PG_REDIS_TLV_STRING 0x01
#define PG_REDIS_TLV_INT    0x02

/* Header size in bytes: 1 (tag) + 4 (length_le). */
#define PG_REDIS_TLV_HEADER_SIZE 5

typedef enum PgRedisValueType
{
	PG_REDIS_TYPE_STRING = 0,
	PG_REDIS_TYPE_INT,
	PG_REDIS_TYPE_HASH,
	PG_REDIS_TYPE_LIST
} PgRedisValueType;

typedef struct PgRedisListNode
{
	char	   *value;			/* palloc'd in PgRedisMemoryContext */
	Size		value_len;
	int64		ord;			/* stable ordinal, mirrored to pgredis.list_items */
	bool		pending_insert;	/* node needs an insert at next flush */
	struct PgRedisListNode *prev;
	struct PgRedisListNode *next;
} PgRedisListNode;

typedef struct PgRedisOrdNode
{
	int64		ord;
	struct PgRedisOrdNode *next;
} PgRedisOrdNode;

typedef struct PgRedisList
{
	PgRedisListNode *head;
	PgRedisListNode *tail;
	int64		length;
	Size		memory_usage;
	int64		min_ord;
	int64		max_ord;
	bool		ord_initialized;
	/* Owning context — all nodes and value buffers are palloc'd here. For the
	 * persistent session-mode store this is PgRedisMemoryContext; for a
	 * shared-mode scratch this is the calling SQL statement's context so the
	 * structure dies with the statement. */
	MemoryContext mcxt;
	/* Ords whose live nodes have already been freed but whose durable row
	 * still needs to be removed at flush. Allocated in `mcxt`. */
	PgRedisOrdNode *pending_delete_ords;
} PgRedisList;

typedef struct PgRedisHashTombstone
{
	char	   *field;			/* palloc'd in PgRedisMemoryContext */
	struct PgRedisHashTombstone *next;
} PgRedisHashTombstone;

typedef struct PgRedisHash
{
	HTAB	   *fields;			/* field name -> PgRedisHashField */
	int64		field_count;
	Size		memory_usage;
	/* Owning context — the HTAB, all field values, and tombstones are palloc'd
	 * here. PgRedisMemoryContext for the persistent session-mode store; the
	 * calling SQL statement's context for shared-mode scratches. */
	MemoryContext mcxt;
	/* Fields removed since last flush; their parent hash row remains. */
	PgRedisHashTombstone *tombstones;
} PgRedisHash;

typedef struct PgRedisHashField
{
	char		field[PG_REDIS_MAX_FIELD_SIZE + 1];	/* hash key — must be first */
	char	   *value;			/* palloc'd */
	Size		value_len;
	bool		dirty;			/* field has been mutated since last flush */
} PgRedisHashField;

typedef struct PgRedisEntry
{
	char		key[PG_REDIS_MAX_KEY_SIZE + 1];		/* hash key — must be first */
	PgRedisValueType type;
	TimestampTz expire_at;
	bool		has_expire;
	bool		dirty;
	bool		deleted;
	bool		in_dirty_set;	/* membership in the per-backend dirty list */
	struct PgRedisEntry *dirty_next;	/* singly-linked dirty list */
	uint64		version;
	Size		memory_usage;
	union
	{
		char	   *string_value;	/* palloc'd, NUL-terminated */
		int64		int_value;
		PgRedisHash *hash_value;
		PgRedisList *list_value;
	}			value;
	Size		string_len;		/* for string_value */
} PgRedisEntry;

/* Shared-memory hash field bucket — slot in a DSA-backed open-addressing
 * hash table. One bucket per slot in the table; OCCUPIED buckets correspond
 * to a (field, value) pair. Field and value bytes live in separate DSA
 * chunks pointed at by field_dsa / value_dsa. The bucket array itself sits
 * after a PgRedisSharedHashTable header in a single DSA allocation. */
typedef enum PgRedisSharedHashBucketState
{
	PG_REDIS_SHB_EMPTY = 0,
	PG_REDIS_SHB_OCCUPIED,
	PG_REDIS_SHB_TOMBSTONE
} PgRedisSharedHashBucketState;

typedef struct PgRedisSharedHashBucket
{
	uint16		state;			/* PgRedisSharedHashBucketState */
	uint16		field_len;
	uint16		value_len;
	uint16		_pad;
	dsa_pointer field_dsa;		/* field name bytes (when OCCUPIED) */
	dsa_pointer value_dsa;		/* value bytes (when OCCUPIED) */
} PgRedisSharedHashBucket;

/* Header for a per-key shared hash table. The bucket array of length
 * `bucket_count` is laid out contiguously immediately after the header in
 * the same DSA allocation, addressable via (PgRedisSharedHashBucket *)(header + 1). */
typedef struct PgRedisSharedHashTable
{
	int64		bucket_count;	/* power of two, >= initial size */
	int64		occupied;		/* live OCCUPIED buckets */
	int64		tombstones;		/* TOMBSTONE buckets — affect probe length */
} PgRedisSharedHashTable;

/* Shared-memory list node — node in a DSA-backed doubly-linked list. The
 * head/tail pointers sit on PgRedisSharedEntry.value.list. */
typedef struct PgRedisSharedListNode
{
	dsa_pointer prev;			/* dsa_pointer to previous node, or InvalidDsaPointer */
	dsa_pointer next;			/* dsa_pointer to next node, or InvalidDsaPointer */
	int64		ord;			/* stable ordinal mirrored to pgredis.list_items.ord */
	uint16		value_len;
	dsa_pointer value_dsa;		/* element value bytes */
} PgRedisSharedListNode;

/* Shared-memory entry — lives in the shared HTAB when storage_mode='shared'.
 * Variable-size payloads (TLV bytes for strings, field maps for hashes,
 * element lists for lists) live in the shared DSA segment and are addressed
 * via dsa_pointer. The struct is fixed-size so ShmemInitHash can pre-allocate
 * a flat array. */
typedef struct PgRedisSharedEntry
{
	char		key[PG_REDIS_MAX_KEY_SIZE + 1];		/* hash key — must be first */
	PgRedisValueType type;
	TimestampTz expire_at;
	bool		has_expire;
	uint64		version;
	union
	{
		struct
		{
			dsa_pointer dsa_value;	/* TLV bytes (no varlena header) */
			Size		value_len;	/* TLV body length */
			int64		int_value;	/* mirrored in-place for int type */
		}			scalar;
		struct
		{
			dsa_pointer table;		/* dsa_pointer to PgRedisSharedHashTable (header + bucket array) */
			int64		field_count;
		}			hash;
		struct
		{
			dsa_pointer head;		/* dsa_pointer to first PgRedisSharedListNode */
			dsa_pointer tail;
			int64		length;
			int64		min_ord;
			int64		max_ord;
			bool		ord_initialized;
		}			list;
	}			value;
} PgRedisSharedEntry;

/* GUCs */
extern char *pg_redis_persistence_mode;
extern char *pg_redis_storage_mode;
extern int	pg_redis_flush_interval_s;
extern int	pg_redis_flush_batch_size;
extern int	pg_redis_ttl_cleanup_interval_s;
extern int	pg_redis_max_key_size;
extern int	pg_redis_max_value_size;
extern bool pg_redis_enable_bgworker;
extern int	pg_redis_shared_max_memory_mb;
extern int	pg_redis_dirty_ring_size;
extern int	pg_redis_lock_partitions;
extern int	pg_redis_async_full_action;	/* parsed from enum GUC */

/* Persistence mode enumeration (parsed from GUC string). */
typedef enum PgRedisPersistenceMode
{
	PG_REDIS_PERSIST_NONE = 0,
	PG_REDIS_PERSIST_SYNC_TABLE,
	PG_REDIS_PERSIST_ASYNC_TABLE,
	PG_REDIS_PERSIST_SNAPSHOT,
	PG_REDIS_PERSIST_AOF
} PgRedisPersistenceMode;

extern PgRedisPersistenceMode pg_redis_resolve_persistence_mode(void);

/* Storage mode enumeration (parsed from GUC string). `shared` becomes
 * functional in v1.2 via the implement-async-table change; `session` remains
 * the default per-backend HTAB. */
typedef enum PgRedisStorageMode
{
	PG_REDIS_STORAGE_SESSION = 0,
	PG_REDIS_STORAGE_SHARED
} PgRedisStorageMode;

extern PgRedisStorageMode pg_redis_resolve_storage_mode(void);
extern bool pg_redis_storage_is_shared(void);

/* Async-full backpressure action — values for `pg_redis.async_full_action`. */
#define PG_REDIS_ASYNC_FULL_BLOCK      0
#define PG_REDIS_ASYNC_FULL_SYNC_FLUSH 1

/* Effective persistence mode, taking misconfiguration into account: when
 * `persistence_mode = 'async_table'` but `storage_mode != 'shared'`, this
 * returns `sync_table` (and the GUC check hook has already emitted a WARNING). */
extern PgRedisPersistenceMode pg_redis_effective_persistence_mode(void);

#endif							/* PG_REDIS_TYPES_H */
