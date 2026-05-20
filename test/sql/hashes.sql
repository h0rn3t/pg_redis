-- HSET / HGET / HDEL / HEXISTS + WRONGTYPE
CREATE EXTENSION pg_redis;

-- HSET on new key creates the hash
SELECT pgredis."HSET"('u', 'name', 'Alice');  -- true (new field)
SELECT pgredis."HSET"('u', 'name', 'Bob');    -- false (overwrote)
SELECT pgredis."HGET"('u', 'name');           -- 'Bob'
SELECT pgredis."HSET"('u', 'age', '30');
SELECT pgredis."HEXISTS"('u', 'age');
SELECT pgredis."HEXISTS"('u', 'no-such');

-- HGET on missing field
SELECT pgredis."HGET"('u', 'no-such') IS NULL AS missing_field_is_null;

-- HDEL
SELECT pgredis."HDEL"('u', 'age');            -- true
SELECT pgredis."HDEL"('u', 'age');            -- false (already gone)
SELECT pgredis."HEXISTS"('u', 'age');

-- WRONGTYPE: HSET on a string key
SELECT pgredis."SET"('s', 'hi');
DO $$
BEGIN
    PERFORM pgredis."HSET"('s', 'f', 'v');
    RAISE EXCEPTION 'expected WRONGTYPE';
EXCEPTION WHEN SQLSTATE '42804' THEN
    RAISE NOTICE 'WRONGTYPE: %', SQLERRM;
END$$;

-- WRONGTYPE: GET on a hash key
DO $$
BEGIN
    PERFORM pgredis."GET"('u');
    RAISE EXCEPTION 'expected WRONGTYPE';
EXCEPTION WHEN SQLSTATE '42804' THEN
    RAISE NOTICE 'WRONGTYPE: %', SQLERRM;
END$$;

DROP EXTENSION pg_redis CASCADE;
