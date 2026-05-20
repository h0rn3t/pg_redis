#ifndef PG_REDIS_JOBS_H
#define PG_REDIS_JOBS_H

#include "postgres.h"

/* Execute a single job by id. Updates pg_redis.jobs (last_run, next_run,
 * last_error) and pg_redis.job_stats. Returns true on success. Assumes the
 * caller is already inside a transaction (and not connected via SPI). */
extern bool pg_redis_jobs_run_one(int64 job_id);

/* Run all jobs whose next_run <= now() and that are enabled. Returns the
 * number of jobs executed. Used by the background worker. */
extern int pg_redis_jobs_run_due(void);

#endif							/* PG_REDIS_JOBS_H */
