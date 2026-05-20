-- basic SET/GET/DEL/EXISTS, overwrite, missing key, MEMORY_USAGE / FLUSHALL
CREATE EXTENSION pg_redis;

-- Missing key
SELECT pgredis."GET"('nope') IS NULL AS missing_is_null;
SELECT pgredis."EXISTS"('nope');

-- SET / GET / EXISTS
SELECT pgredis."SET"('k1', 'hello');
SELECT pgredis."GET"('k1');
SELECT pgredis."EXISTS"('k1');

-- Overwrite
SELECT pgredis."SET"('k1', 'world');
SELECT pgredis."GET"('k1');

-- DEL
SELECT pgredis."DEL"('k1');
SELECT pgredis."DEL"('k1');                 -- false: already gone
SELECT pgredis."EXISTS"('k1');

-- INCR / DECR
SELECT pgredis."INCR"('counter');           -- 1
SELECT pgredis."INCR"('counter');           -- 2
SELECT pgredis."DECR"('counter');           -- 1
SELECT pgredis."GET"('counter');            -- '1'

-- INCR on non-numeric
SELECT pgredis."SET"('bad', 'not-a-number');
DO $$
BEGIN
    PERFORM pgredis."INCR"('bad');
    RAISE EXCEPTION 'expected INCR on non-numeric to fail';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'INCR rejected non-numeric value: %', SQLERRM;
END$$;

-- MEMORY_USAGE > 0 while keys exist
SELECT pgredis."MEMORY_USAGE"() > 0 AS mem_positive;

-- FLUSHALL clears everything
SELECT pgredis."FLUSHALL"();
SELECT pgredis."MEMORY_USAGE"();
SELECT count(*) FROM pgredis."KEYS"();

DROP EXTENSION pg_redis CASCADE;
