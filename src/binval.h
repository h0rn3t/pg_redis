#ifndef PG_REDIS_BINVAL_H
#define PG_REDIS_BINVAL_H

#include "postgres.h"
#include "types.h"

/*
 * Encode a string/int PgRedisEntry value into the TLV bytea layout:
 *   [u8 type_tag][u32 length_le][payload]
 *
 * Returns a palloc'd bytea* in CurrentMemoryContext on success. Returns NULL
 * if the entry's type is not encodable in this column (hash/list — those
 * payloads live in their per-element tables).
 */
extern bytea *pg_redis_encode_value(PgRedisEntry *e);

/*
 * Decode a TLV bytea as a string. On success returns true and writes a
 * palloc'd NUL-terminated copy to *out_str (in CurrentMemoryContext) and the
 * length in bytes to *out_len. Returns false on tag mismatch or malformed
 * length.
 */
extern bool pg_redis_decode_string(const bytea *raw,
								   char **out_str, Size *out_len);

/*
 * Decode a TLV bytea as an int64. On success returns true and writes the
 * value to *out. Returns false on tag mismatch or malformed length.
 */
extern bool pg_redis_decode_int(const bytea *raw, int64 *out);

#endif							/* PG_REDIS_BINVAL_H */
