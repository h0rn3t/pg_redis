#ifndef PG_REDIS_UTILS_H
#define PG_REDIS_UTILS_H

#include "postgres.h"
#include "varatt.h"
#include "utils/palloc.h"
#include "types.h"

#include <string.h>

/* Stack-buffer sizes for short-lived key/field copies in command functions.
 * Keys/fields up to this length (inclusive of NUL terminator) avoid palloc;
 * longer inputs fall back to palloc in CurrentMemoryContext. */
#define PG_REDIS_KEY_STACK_BUFSZ   256
#define PG_REDIS_FIELD_STACK_BUFSZ 256

/* Get the long-lived MemoryContext used by the key-value store. */
extern MemoryContext pg_redis_memcxt(void);

/* Reset (destroy + recreate) the store memory context. Used by FLUSHALL. */
extern void pg_redis_memcxt_reset(void);

/* Validate / normalize key length. Raises ereport(ERROR) on overflow. */
extern void pg_redis_check_key_len(const char *key, Size len);
extern void pg_redis_check_value_len(Size len);
extern void pg_redis_check_field_len(const char *field, Size len);

/* Duplicate a sized byte buffer into PgRedisMemoryContext as a NUL-terminated
 * C string. Returned pointer must be freed via pfree (or via context reset). */
extern char *pg_redis_palloc_string(const char *src, Size len);

/* Convert text* argument into a copy in PgRedisMemoryContext. Length is
 * returned via *out_len (excluding terminator). */
extern char *pg_redis_text_to_pstring(text *t, Size *out_len);

/* Parse C string into int64. Returns true on success. */
extern bool pg_redis_parse_int64(const char *s, Size len, int64 *out);

/* Format an int64 into a freshly palloc'd C string (PG_REDIS context). */
extern char *pg_redis_format_int64(int64 v, Size *out_len);

/*
 * Materialize a text* argument into a NUL-terminated C string.
 *
 * Uses the caller-provided stack buffer when the payload (plus NUL) fits;
 * otherwise palloc's in CurrentMemoryContext. Performs length validation
 * via pg_redis_check_key_len() when check_key is true, or
 * pg_redis_check_field_len() when check_field is true (only one may be set).
 *
 * The returned pointer must be released with pg_redis_free_cstring_stack().
 * The returned buffer is short-lived: do NOT store it on a long-lived
 * structure — use pg_redis_palloc_string() for values that must live in
 * PgRedisMemoryContext.
 */
static inline char *
pg_redis_text_to_cstring_stack(text *t,
							   char *stackbuf, Size stackbuf_sz,
							   Size *out_len,
							   bool check_key, bool check_field)
{
	Size		len = VARSIZE_ANY_EXHDR(t);
	const char *src = VARDATA_ANY(t);
	char	   *dst;

	if (check_key)
		pg_redis_check_key_len(NULL, len);
	else if (check_field)
		pg_redis_check_field_len(NULL, len);

	if (len + 1 <= stackbuf_sz)
		dst = stackbuf;
	else
		dst = (char *) palloc(len + 1);

	if (len > 0)
		memcpy(dst, src, len);
	dst[len] = '\0';

	if (out_len != NULL)
		*out_len = len;
	return dst;
}

/* Release a buffer returned by pg_redis_text_to_cstring_stack(). */
static inline void
pg_redis_free_cstring_stack(char *stackbuf, char *p)
{
	if (p != stackbuf)
		pfree(p);
}

#endif							/* PG_REDIS_UTILS_H */
