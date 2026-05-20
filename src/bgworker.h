#ifndef PG_REDIS_BGWORKER_H
#define PG_REDIS_BGWORKER_H

#include "postgres.h"

/* Register the background worker if pg_redis.enable_background_worker is on.
 * Must be called from _PG_init while the postmaster is starting (i.e. when
 * pg_redis is loaded via shared_preload_libraries). */
extern void pg_redis_bgworker_register(void);

/* Entry point exported for PostgreSQL's bgworker machinery. */
pg_noreturn extern PGDLLEXPORT void pg_redis_bgworker_main(Datum main_arg);

#endif							/* PG_REDIS_BGWORKER_H */
