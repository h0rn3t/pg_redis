/*
 * DSA-backed per-key hash storage for storage_mode='shared'.
 *
 * Each PgRedisSharedEntry of type HASH holds one dsa_pointer to a
 * PgRedisSharedHashTable header. The header is followed in the same DSA
 * allocation by a contiguous array of `bucket_count` PgRedisSharedHashBucket
 * slots, indexed by hash_bytes(field) & (bucket_count - 1) and resolved with
 * linear probing. Tombstones are required to preserve probe-chain integrity
 * after deletes. Grow at load factor 0.7; lazy shrink under 0.125 with a
 * lower bound of PG_REDIS_SHARED_HASH_INIT_BUCKETS.
 *
 * All accessors expect the caller to already hold the appropriate partition
 * LWLock — LW_EXCLUSIVE for set/del (which may rehash), LW_SHARED for get
 * /exists/count/materialize.
 */
#include "postgres.h"
#include "common/hashfn.h"
#include "utils/dsa.h"
#include "utils/elog.h"

#include <string.h>

#include "types.h"
#include "shmem.h"
#include "hash_value.h"
#include "shared_hash.h"

/* Load factor thresholds (numerator / denominator to stay in int math). */
#define GROW_NUM	7
#define GROW_DEN	10			/* grow when load > 0.7 */
#define SHRINK_NUM	1
#define SHRINK_DEN	8			/* shrink when occupied < 0.125 */

/* Returns a pointer to the bucket array sitting after the header. */
static inline PgRedisSharedHashBucket *
bucket_array(PgRedisSharedHashTable *t)
{
	return (PgRedisSharedHashBucket *) (t + 1);
}

static inline Size
table_alloc_size(int64 bucket_count)
{
	return sizeof(PgRedisSharedHashTable) +
		(Size) bucket_count * sizeof(PgRedisSharedHashBucket);
}

static inline int64
bucket_index(const char *field, Size fieldlen, int64 bucket_count)
{
	uint32		h = hash_bytes((const unsigned char *) field, (int) fieldlen);

	return (int64) (h & (uint32) (bucket_count - 1));
}

/* Compare a bucket's stored field bytes against `field` of length `fieldlen`. */
static bool
bucket_matches(PgRedisSharedHashBucket *b, const char *field, Size fieldlen)
{
	const char *bf;

	if (b->state != PG_REDIS_SHB_OCCUPIED)
		return false;
	if (b->field_len != fieldlen)
		return false;
	bf = (const char *) pg_redis_shared_addr(b->field_dsa);
	if (bf == NULL)
		return false;
	return memcmp(bf, field, fieldlen) == 0;
}

/*
 * Linear-probe lookup. On hit (OCCUPIED bucket with matching field), returns
 * the matching bucket index and sets *was_found=true. On miss, returns the
 * first encountered EMPTY or TOMBSTONE slot index suitable for insertion and
 * sets *was_found=false. Aborts probing on EMPTY.
 */
static int64
find_bucket(PgRedisSharedHashTable *t,
			const char *field, Size fieldlen,
			bool *was_found)
{
	PgRedisSharedHashBucket *buckets = bucket_array(t);
	int64		mask = t->bucket_count - 1;
	int64		idx = bucket_index(field, fieldlen, t->bucket_count);
	int64		first_tomb = -1;
	int64		probes;

	/* Bounded probe: with the grow-at-0.7 invariant there is always an EMPTY
	 * bucket within bucket_count steps. The bound is defensive — if a future
	 * regression breaks the invariant we'd otherwise loop forever. */
	for (probes = 0; probes < t->bucket_count; probes++)
	{
		PgRedisSharedHashBucket *b = &buckets[idx];

		if (b->state == PG_REDIS_SHB_EMPTY)
		{
			*was_found = false;
			return first_tomb >= 0 ? first_tomb : idx;
		}
		if (b->state == PG_REDIS_SHB_TOMBSTONE)
		{
			if (first_tomb < 0)
				first_tomb = idx;
		}
		else if (bucket_matches(b, field, fieldlen))
		{
			*was_found = true;
			return idx;
		}
		idx = (idx + 1) & mask;
	}

	/* Exhausted the table without finding an empty slot or match. If we saw a
	 * tombstone we can still insert there; otherwise the table is saturated
	 * and we have to error out. */
	if (first_tomb >= 0)
	{
		*was_found = false;
		return first_tomb;
	}
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("pg_redis: shared hash table saturated (no empty bucket in %ld probes)",
					(long) t->bucket_count)));
	return -1;					/* unreachable, silences compiler */
}

/*
 * Allocate a new table with `bucket_count` slots, zero-initialized. Returns
 * InvalidDsaPointer on OOM.
 */
static dsa_pointer
alloc_table(int64 bucket_count)
{
	dsa_pointer p = pg_redis_shared_palloc(table_alloc_size(bucket_count));
	PgRedisSharedHashTable *t;

	if (p == InvalidDsaPointer)
		return InvalidDsaPointer;

	t = (PgRedisSharedHashTable *) pg_redis_shared_addr(p);
	t->bucket_count = bucket_count;
	t->occupied = 0;
	t->tombstones = 0;
	memset(bucket_array(t), 0, (Size) bucket_count * sizeof(PgRedisSharedHashBucket));
	/* state == 0 == PG_REDIS_SHB_EMPTY for every slot — memset is correct. */
	return p;
}

/*
 * Free a table and every (field_dsa, value_dsa) chunk referenced from
 * OCCUPIED buckets. No-op on InvalidDsaPointer.
 */
void
pg_redis_shared_hash_free(dsa_pointer table_dsa)
{
	PgRedisSharedHashTable *t;
	PgRedisSharedHashBucket *buckets;
	int64		i;

	if (table_dsa == InvalidDsaPointer)
		return;
	t = (PgRedisSharedHashTable *) pg_redis_shared_addr(table_dsa);
	if (t == NULL)
		return;
	buckets = bucket_array(t);
	for (i = 0; i < t->bucket_count; i++)
	{
		if (buckets[i].state == PG_REDIS_SHB_OCCUPIED)
		{
			pg_redis_shared_pfree(buckets[i].field_dsa);
			pg_redis_shared_pfree(buckets[i].value_dsa);
		}
	}
	pg_redis_shared_pfree(table_dsa);
}

/*
 * Insert a (field_dsa, value_dsa, lengths) pair into a freshly-allocated
 * table that the caller owns exclusively. Used during rehash where the
 * caller carries ownership of the field/value DSA pointers across — no copy
 * of the bytes, just bucket-slot reassignment. Returns true on insert.
 */
static bool
insert_owned(PgRedisSharedHashTable *t,
			 dsa_pointer field_dsa, uint16 field_len,
			 dsa_pointer value_dsa, uint16 value_len)
{
	PgRedisSharedHashBucket *buckets = bucket_array(t);
	const char *field_bytes = (const char *) pg_redis_shared_addr(field_dsa);
	bool		was_found;
	int64		idx = find_bucket(t, field_bytes, field_len, &was_found);
	PgRedisSharedHashBucket *b = &buckets[idx];

	/* During rehash a destination bucket should always be EMPTY — we're
	 * filling a fresh allocation. Defensive: tolerate TOMBSTONE too. */
	if (b->state == PG_REDIS_SHB_TOMBSTONE)
		t->tombstones--;
	b->state = PG_REDIS_SHB_OCCUPIED;
	b->field_len = field_len;
	b->value_len = value_len;
	b->field_dsa = field_dsa;
	b->value_dsa = value_dsa;
	t->occupied++;
	return !was_found;
}

/*
 * Rehash `old_dsa` into a fresh table of `new_size` buckets. Reuses the
 * existing field_dsa/value_dsa DSA chunks (the bytes themselves don't move),
 * only reshuffles bucket slots. Frees the old table header on success.
 * Returns the new table's dsa_pointer, or InvalidDsaPointer on OOM (in which
 * case `old_dsa` is left unchanged).
 */
static dsa_pointer
rehash_table(dsa_pointer old_dsa, int64 new_size)
{
	dsa_pointer new_dsa;
	PgRedisSharedHashTable *old_t;
	PgRedisSharedHashTable *new_t;
	PgRedisSharedHashBucket *old_buckets;
	int64		i;

	new_dsa = alloc_table(new_size);
	if (new_dsa == InvalidDsaPointer)
		return InvalidDsaPointer;
	old_t = (PgRedisSharedHashTable *) pg_redis_shared_addr(old_dsa);
	new_t = (PgRedisSharedHashTable *) pg_redis_shared_addr(new_dsa);
	old_buckets = bucket_array(old_t);
	for (i = 0; i < old_t->bucket_count; i++)
	{
		PgRedisSharedHashBucket *b = &old_buckets[i];

		if (b->state != PG_REDIS_SHB_OCCUPIED)
			continue;
		(void) insert_owned(new_t,
							b->field_dsa, b->field_len,
							b->value_dsa, b->value_len);
	}
	pg_redis_shared_pfree(old_dsa);
	return new_dsa;
}

/* Whether a (occupied+tombstones)-after-insert state would exceed the grow
 * threshold of a `size`-bucket table. */
static inline bool
should_grow(int64 occupied, int64 tombstones, int64 size)
{
	return (occupied + tombstones + 1) * GROW_DEN > size * GROW_NUM;
}

/* Whether `occupied` (after a delete) is below the shrink threshold for
 * `size`. Lower bound on `size` is enforced by the caller. */
static inline bool
should_shrink(int64 occupied, int64 size)
{
	return size > PG_REDIS_SHARED_HASH_INIT_BUCKETS &&
		occupied * SHRINK_DEN < size * SHRINK_NUM;
}

void
pg_redis_shared_hash_set(dsa_pointer *inout_table_dsa,
						 const char *field, Size fieldlen,
						 const unsigned char *value, Size value_len,
						 bool *was_new)
{
	dsa_pointer table_dsa = *inout_table_dsa;
	PgRedisSharedHashTable *t;
	PgRedisSharedHashBucket *buckets;
	dsa_pointer field_p,
				value_p;
	bool		was_found;
	int64		idx;
	PgRedisSharedHashBucket *b;

	/* Bucket width caps the per-field value length at uint16. Reject oversize
	 * values up front so no DSA chunk is allocated for a truncated record. */
	if (value_len > UINT16_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("pg_redis: hash field value length %zu exceeds maximum %u",
						value_len, (unsigned) UINT16_MAX),
				 errhint("Shared-mode hash field values are bounded by the "
						 "PgRedisSharedHashBucket width.")));

	/* Lazy table allocation: first HSET on this key. */
	if (table_dsa == InvalidDsaPointer)
	{
		table_dsa = alloc_table(PG_REDIS_SHARED_HASH_INIT_BUCKETS);
		if (table_dsa == InvalidDsaPointer)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("pg_redis: out of shared memory allocating hash table"),
					 errhint("Raise pg_redis.shared_max_memory.")));
		*inout_table_dsa = table_dsa;
	}

	t = (PgRedisSharedHashTable *) pg_redis_shared_addr(table_dsa);

	/* Grow first if needed — keeps probe chains short. */
	if (should_grow(t->occupied, t->tombstones, t->bucket_count))
	{
		dsa_pointer new_dsa = rehash_table(table_dsa, t->bucket_count * 2);

		if (new_dsa == InvalidDsaPointer)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("pg_redis: out of shared memory growing hash table"),
					 errhint("Raise pg_redis.shared_max_memory.")));
		*inout_table_dsa = new_dsa;
		table_dsa = new_dsa;
		t = (PgRedisSharedHashTable *) pg_redis_shared_addr(table_dsa);
	}

	idx = find_bucket(t, field, fieldlen, &was_found);
	buckets = bucket_array(t);
	b = &buckets[idx];

	if (was_found)
	{
		/* Overwrite-in-place: free old value, alloc new. Field stays. */
		dsa_pointer new_value_p = (value_len > 0)
			? pg_redis_shared_palloc(value_len)
			: InvalidDsaPointer;

		if (value_len > 0 && new_value_p == InvalidDsaPointer)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("pg_redis: out of shared memory in HSET overwrite"),
					 errhint("Raise pg_redis.shared_max_memory.")));
		if (value_len > 0)
			memcpy(pg_redis_shared_addr(new_value_p), value, value_len);
		pg_redis_shared_pfree(b->value_dsa);
		b->value_dsa = new_value_p;
		b->value_len = (uint16) value_len;
		if (was_new)
			*was_new = false;
		return;
	}

	/* Insert into bucket `idx` (EMPTY or TOMBSTONE). */
	field_p = pg_redis_shared_palloc(fieldlen);
	if (field_p == InvalidDsaPointer)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("pg_redis: out of shared memory in HSET insert"),
				 errhint("Raise pg_redis.shared_max_memory.")));
	memcpy(pg_redis_shared_addr(field_p), field, fieldlen);

	value_p = (value_len > 0) ? pg_redis_shared_palloc(value_len) : InvalidDsaPointer;
	if (value_len > 0 && value_p == InvalidDsaPointer)
	{
		pg_redis_shared_pfree(field_p);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("pg_redis: out of shared memory in HSET insert"),
				 errhint("Raise pg_redis.shared_max_memory.")));
	}
	if (value_len > 0)
		memcpy(pg_redis_shared_addr(value_p), value, value_len);

	if (b->state == PG_REDIS_SHB_TOMBSTONE)
		t->tombstones--;
	b->state = PG_REDIS_SHB_OCCUPIED;
	b->field_len = (uint16) fieldlen;
	b->value_len = (uint16) value_len;
	b->field_dsa = field_p;
	b->value_dsa = value_p;
	t->occupied++;
	if (was_new)
		*was_new = true;
}

bool
pg_redis_shared_hash_get(dsa_pointer table_dsa,
						 const char *field, Size fieldlen,
						 unsigned char **out_value, Size *out_value_len)
{
	PgRedisSharedHashTable *t;
	PgRedisSharedHashBucket *b;
	const unsigned char *src;
	unsigned char *buf;
	bool		was_found;
	int64		idx;

	if (table_dsa == InvalidDsaPointer)
		goto miss;
	t = (PgRedisSharedHashTable *) pg_redis_shared_addr(table_dsa);
	if (t == NULL || t->occupied == 0)
		goto miss;
	idx = find_bucket(t, field, fieldlen, &was_found);
	if (!was_found)
		goto miss;
	b = &bucket_array(t)[idx];
	src = (const unsigned char *) pg_redis_shared_addr(b->value_dsa);
	buf = (unsigned char *) palloc(b->value_len > 0 ? b->value_len : 1);
	if (b->value_len > 0 && src != NULL)
		memcpy(buf, src, b->value_len);
	*out_value = buf;
	*out_value_len = b->value_len;
	return true;

miss:
	if (out_value)
		*out_value = NULL;
	if (out_value_len)
		*out_value_len = 0;
	return false;
}

bool
pg_redis_shared_hash_exists(dsa_pointer table_dsa,
							const char *field, Size fieldlen)
{
	PgRedisSharedHashTable *t;
	bool		was_found;

	if (table_dsa == InvalidDsaPointer)
		return false;
	t = (PgRedisSharedHashTable *) pg_redis_shared_addr(table_dsa);
	if (t == NULL || t->occupied == 0)
		return false;
	(void) find_bucket(t, field, fieldlen, &was_found);
	return was_found;
}

bool
pg_redis_shared_hash_del(dsa_pointer *inout_table_dsa,
						 const char *field, Size fieldlen)
{
	dsa_pointer table_dsa = *inout_table_dsa;
	PgRedisSharedHashTable *t;
	PgRedisSharedHashBucket *b;
	bool		was_found;
	int64		idx;

	if (table_dsa == InvalidDsaPointer)
		return false;
	t = (PgRedisSharedHashTable *) pg_redis_shared_addr(table_dsa);
	if (t == NULL || t->occupied == 0)
		return false;
	idx = find_bucket(t, field, fieldlen, &was_found);
	if (!was_found)
		return false;
	b = &bucket_array(t)[idx];
	pg_redis_shared_pfree(b->field_dsa);
	pg_redis_shared_pfree(b->value_dsa);
	b->state = PG_REDIS_SHB_TOMBSTONE;
	b->field_dsa = InvalidDsaPointer;
	b->value_dsa = InvalidDsaPointer;
	b->field_len = 0;
	b->value_len = 0;
	t->occupied--;
	t->tombstones++;

	/* Lazy shrink. Failure to grow leaves the table at the current size —
	 * correctness is unaffected, only memory footprint. */
	if (should_shrink(t->occupied, t->bucket_count))
	{
		dsa_pointer new_dsa = rehash_table(table_dsa, t->bucket_count / 2);

		if (new_dsa != InvalidDsaPointer)
			*inout_table_dsa = new_dsa;
	}
	return true;
}

int64
pg_redis_shared_hash_count(dsa_pointer table_dsa)
{
	PgRedisSharedHashTable *t;

	if (table_dsa == InvalidDsaPointer)
		return 0;
	t = (PgRedisSharedHashTable *) pg_redis_shared_addr(table_dsa);
	return t ? t->occupied : 0;
}

PgRedisHash *
pg_redis_shared_hash_materialize(dsa_pointer table_dsa)
{
	PgRedisHash *h;
	PgRedisSharedHashTable *t;
	PgRedisSharedHashBucket *buckets;
	int64		i;

	if (table_dsa == InvalidDsaPointer)
		return NULL;
	t = (PgRedisSharedHashTable *) pg_redis_shared_addr(table_dsa);
	if (t == NULL || t->occupied == 0)
		return NULL;

	/* Scratch lives in CurrentMemoryContext — see the same note in
	 * pg_redis_shared_list_materialize. */
	h = pg_redis_hash_create_in(CurrentMemoryContext);

	buckets = bucket_array(t);
	for (i = 0; i < t->bucket_count; i++)
	{
		PgRedisSharedHashBucket *b = &buckets[i];
		const char *field_bytes;
		const char *value_bytes;
		char		fieldbuf[PG_REDIS_MAX_FIELD_SIZE + 1];

		if (b->state != PG_REDIS_SHB_OCCUPIED)
			continue;
		if (b->field_len > PG_REDIS_MAX_FIELD_SIZE)
			continue;				/* defensive: skip corrupt bucket */
		field_bytes = (const char *) pg_redis_shared_addr(b->field_dsa);
		if (field_bytes == NULL)
			continue;				/* defensive: skip bucket with NULL-resolving field_dsa */
		value_bytes = (const char *) pg_redis_shared_addr(b->value_dsa);
		memcpy(fieldbuf, field_bytes, b->field_len);
		fieldbuf[b->field_len] = '\0';
		(void) pg_redis_hash_set(h, fieldbuf,
								 value_bytes ? value_bytes : "",
								 b->value_len);
	}
	return h;
}
