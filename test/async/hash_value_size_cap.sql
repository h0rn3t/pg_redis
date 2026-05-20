-- Regression test for the 65535-byte hard cap on shared-mode hash field
-- values, enforced in pg_redis_shared_hash_set against PgRedisSharedHashBucket
-- value_len being uint16. Previously values >64KB were silently truncated.
--
-- Requires a cluster started with:
--   shared_preload_libraries = 'pg_redis'
--   pg_redis.storage_mode    = 'shared'

\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS pg_redis;

SELECT pgredis."FLUSHALL"();

-- 1. Value exactly at the limit (65535 bytes) must succeed and round-trip.
DO $$
DECLARE
    v text := repeat('x', 65535);
    got text;
BEGIN
    PERFORM pgredis."HSET"('h:cap', 'f_max', v);
    got := pgredis."HGET"('h:cap', 'f_max');
    IF length(got) <> 65535 THEN
        RAISE EXCEPTION 'expected 65535 bytes, got %', length(got);
    END IF;
    IF got <> v THEN
        RAISE EXCEPTION 'value did not round-trip exactly';
    END IF;
END$$;

-- 2. Value one byte over the limit (65536 bytes) must error out.
DO $$
BEGIN
    PERFORM pgredis."HSET"('h:cap', 'f_too_big', repeat('x', 65536));
    RAISE EXCEPTION 'expected ERRCODE_PROGRAM_LIMIT_EXCEEDED, none raised';
EXCEPTION
    WHEN SQLSTATE '54000' THEN
        RAISE NOTICE 'caught PROGRAM_LIMIT_EXCEEDED: %', SQLERRM;
END$$;

-- 3. The failed oversize HSET must NOT have inserted a partial bucket.
SELECT pgredis."HEXISTS"('h:cap', 'f_too_big');  -- f
SELECT pgredis."HEXISTS"('h:cap', 'f_max');      -- t (untouched by the failure)

-- 4. Previously-set fields on the same key must still be intact.
SELECT length(pgredis."HGET"('h:cap', 'f_max'));  -- 65535

SELECT pgredis."FLUSHALL"();
