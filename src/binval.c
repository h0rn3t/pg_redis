#include "postgres.h"
#include "varatt.h"
#include "utils/palloc.h"

#include <string.h>

#include "types.h"
#include "binval.h"

/*
 * Little-endian helpers. We write/read u32 length and i64 payload as raw
 * little-endian bytes regardless of host byte order to keep the on-disk
 * layout portable across architectures.
 */

static inline void
put_u32_le(unsigned char *p, uint32 v)
{
	p[0] = (unsigned char) (v & 0xFF);
	p[1] = (unsigned char) ((v >> 8) & 0xFF);
	p[2] = (unsigned char) ((v >> 16) & 0xFF);
	p[3] = (unsigned char) ((v >> 24) & 0xFF);
}

static inline uint32
get_u32_le(const unsigned char *p)
{
	return ((uint32) p[0]) |
		((uint32) p[1] << 8) |
		((uint32) p[2] << 16) |
		((uint32) p[3] << 24);
}

static inline void
put_i64_le(unsigned char *p, int64 v)
{
	uint64		u = (uint64) v;

	p[0] = (unsigned char) (u & 0xFF);
	p[1] = (unsigned char) ((u >> 8) & 0xFF);
	p[2] = (unsigned char) ((u >> 16) & 0xFF);
	p[3] = (unsigned char) ((u >> 24) & 0xFF);
	p[4] = (unsigned char) ((u >> 32) & 0xFF);
	p[5] = (unsigned char) ((u >> 40) & 0xFF);
	p[6] = (unsigned char) ((u >> 48) & 0xFF);
	p[7] = (unsigned char) ((u >> 56) & 0xFF);
}

static inline int64
get_i64_le(const unsigned char *p)
{
	uint64		u;

	u = ((uint64) p[0]) |
		((uint64) p[1] << 8) |
		((uint64) p[2] << 16) |
		((uint64) p[3] << 24) |
		((uint64) p[4] << 32) |
		((uint64) p[5] << 40) |
		((uint64) p[6] << 48) |
		((uint64) p[7] << 56);
	return (int64) u;
}

bytea *
pg_redis_encode_value(PgRedisEntry *e)
{
	bytea	   *out;
	unsigned char *p;
	Size		payload_len;
	Size		total;

	if (e == NULL)
		return NULL;

	switch (e->type)
	{
		case PG_REDIS_TYPE_STRING:
			payload_len = e->string_len;
			total = VARHDRSZ + PG_REDIS_TLV_HEADER_SIZE + payload_len;
			out = (bytea *) palloc(total);
			SET_VARSIZE(out, total);
			p = (unsigned char *) VARDATA(out);
			p[0] = PG_REDIS_TLV_STRING;
			put_u32_le(p + 1, (uint32) payload_len);
			if (payload_len > 0 && e->value.string_value != NULL)
				memcpy(p + PG_REDIS_TLV_HEADER_SIZE,
					   e->value.string_value, payload_len);
			return out;

		case PG_REDIS_TYPE_INT:
			payload_len = 8;
			total = VARHDRSZ + PG_REDIS_TLV_HEADER_SIZE + payload_len;
			out = (bytea *) palloc(total);
			SET_VARSIZE(out, total);
			p = (unsigned char *) VARDATA(out);
			p[0] = PG_REDIS_TLV_INT;
			put_u32_le(p + 1, (uint32) payload_len);
			put_i64_le(p + PG_REDIS_TLV_HEADER_SIZE, e->value.int_value);
			return out;

		case PG_REDIS_TYPE_HASH:
		case PG_REDIS_TYPE_LIST:
			/* Hash/list payloads live in their per-element tables. */
			return NULL;
	}
	return NULL;				/* unreachable */
}

/*
 * Common header validation. Returns true and writes the payload pointer + len
 * on success; returns false if header is malformed or tag does not match
 * expected_tag.
 */
static bool
decode_header(const bytea *raw, unsigned char expected_tag,
			  const unsigned char **payload_out, uint32 *payload_len_out)
{
	Size		raw_total;
	Size		body_len;
	const unsigned char *p;
	uint32		declared_len;

	if (raw == NULL)
		return false;

	raw_total = VARSIZE_ANY(raw);
	if (raw_total < VARHDRSZ + PG_REDIS_TLV_HEADER_SIZE)
		return false;

	body_len = VARSIZE_ANY_EXHDR(raw);
	if (body_len < PG_REDIS_TLV_HEADER_SIZE)
		return false;

	p = (const unsigned char *) VARDATA_ANY(raw);
	if (p[0] != expected_tag)
		return false;

	declared_len = get_u32_le(p + 1);
	if ((Size) declared_len + PG_REDIS_TLV_HEADER_SIZE != body_len)
		return false;

	*payload_out = p + PG_REDIS_TLV_HEADER_SIZE;
	*payload_len_out = declared_len;
	return true;
}

bool
pg_redis_decode_string(const bytea *raw, char **out_str, Size *out_len)
{
	const unsigned char *payload;
	uint32		payload_len;
	char	   *buf;

	if (out_str == NULL)
		return false;

	if (!decode_header(raw, PG_REDIS_TLV_STRING, &payload, &payload_len))
		return false;

	buf = (char *) palloc(payload_len + 1);
	if (payload_len > 0)
		memcpy(buf, payload, payload_len);
	buf[payload_len] = '\0';

	*out_str = buf;
	if (out_len != NULL)
		*out_len = payload_len;
	return true;
}

bool
pg_redis_decode_int(const bytea *raw, int64 *out)
{
	const unsigned char *payload;
	uint32		payload_len;

	if (out == NULL)
		return false;

	if (!decode_header(raw, PG_REDIS_TLV_INT, &payload, &payload_len))
		return false;
	if (payload_len != 8)
		return false;

	*out = get_i64_le(payload);
	return true;
}
