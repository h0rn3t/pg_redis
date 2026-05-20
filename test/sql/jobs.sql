-- Job-scheduling surface: ADD_*_POLICY / JOBS / RUN_JOB / JOB_STATS / DELETE_JOB.
CREATE EXTENSION pg_redis;

-- ADD_*_POLICY returns a fresh job_id (>0). We capture them to drive later tests.
SELECT pgredis."ADD_TTL_CLEANUP_POLICY"('1 minute'::interval) > 0 AS ttl_job_id_positive;
SELECT pgredis."ADD_FLUSH_POLICY"('30 seconds'::interval) > 0 AS flush_job_id_positive;
SELECT pgredis."ADD_SNAPSHOT_POLICY"('1 hour'::interval) > 0 AS snap_job_id_positive;

-- JOBS exposes all three rows, in order, with the expected types.
SELECT job_type, schedule_interval, enabled FROM pgredis."JOBS"();

-- RUN_JOB on the TTL cleanup job (job_id=1) succeeds and bumps stats.
SELECT pgredis."RUN_JOB"(1);
SELECT job_id, total_runs, successful_runs, failed_runs
  FROM pgredis."JOB_STATS"() WHERE job_id = 1;

-- RUN_JOB on a missing job returns false (no such row).
SELECT pgredis."RUN_JOB"(9999);

-- DELETE_JOB removes the row and returns true; second call returns false.
SELECT pgredis."DELETE_JOB"(2);
SELECT pgredis."DELETE_JOB"(2);
SELECT count(*) FROM pgredis."JOBS"();

DROP EXTENSION pg_redis CASCADE;
