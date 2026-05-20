-- LPUSH / RPUSH / LPOP / RPOP / LLEN, empty list NULL, WRONGTYPE
CREATE EXTENSION pg_redis;

-- LLEN on missing key returns 0
SELECT pgredis."LLEN"('q');

-- LPUSH adds to head, RPUSH to tail
SELECT pgredis."RPUSH"('q', 'a');             -- 1
SELECT pgredis."RPUSH"('q', 'b');             -- 2
SELECT pgredis."RPUSH"('q', 'c');             -- 3
SELECT pgredis."LPUSH"('q', 'X');             -- 4
SELECT pgredis."LLEN"('q');

-- LPOP returns head (X), RPOP returns tail (c)
SELECT pgredis."LPOP"('q');
SELECT pgredis."RPOP"('q');
SELECT pgredis."LLEN"('q');                   -- 2 (a, b remain)

-- Drain to empty
SELECT pgredis."LPOP"('q');
SELECT pgredis."LPOP"('q');
SELECT pgredis."LLEN"('q');                   -- 0
SELECT pgredis."LPOP"('q') IS NULL AS empty_pop_is_null;
SELECT pgredis."EXISTS"('q');                 -- true: empty list is retained

-- LIFO check: LPUSH a, b, c then LPOP three times -> c, b, a
SELECT pgredis."DEL"('lifo');
SELECT pgredis."LPUSH"('lifo', 'a');
SELECT pgredis."LPUSH"('lifo', 'b');
SELECT pgredis."LPUSH"('lifo', 'c');
SELECT pgredis."LPOP"('lifo');                -- c
SELECT pgredis."LPOP"('lifo');                -- b
SELECT pgredis."LPOP"('lifo');                -- a

-- WRONGTYPE: LPUSH on a hash key
SELECT pgredis."HSET"('h', 'f', 'v');
DO $$
BEGIN
    PERFORM pgredis."LPUSH"('h', 'v');
    RAISE EXCEPTION 'expected WRONGTYPE';
EXCEPTION WHEN SQLSTATE '42804' THEN
    RAISE NOTICE 'WRONGTYPE: %', SQLERRM;
END$$;

DROP EXTENSION pg_redis CASCADE;
