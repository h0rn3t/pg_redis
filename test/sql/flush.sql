-- Pre-commit batched flush semantics
CREATE EXTENSION pg_redis;

-- 11.1: BEGIN; SET k v1; SET k v2; COMMIT;
--     => coalesced to exactly one row in pgredis.store with the final value.
BEGIN;
SELECT pgredis."SET"('f:1', 'v1');
SELECT pgredis."SET"('f:1', 'v2');
COMMIT;
SELECT count(*) FROM pgredis.store WHERE key = 'f:1';      -- 1
-- TLV tag byte (0x01 = string), and last 2 bytes of payload spell "v2".
SELECT get_byte(value, 0) AS tag,
       convert_from(substring(value FROM 6), 'utf8') AS payload
  FROM pgredis.store WHERE key = 'f:1';

-- 11.2: BEGIN; SET k v; ROLLBACK; => no row.
BEGIN;
SELECT pgredis."SET"('f:rb', 'tmp');
ROLLBACK;
SELECT count(*) FROM pgredis.store WHERE key = 'f:rb';     -- 0

-- 11.3: HSET h f1 v1; HSET h f2 v2; HDEL h f1
--     => pgredis.hash_fields ends with only f2.
BEGIN;
SELECT pgredis."HSET"('f:h', 'f1', 'v1');
SELECT pgredis."HSET"('f:h', 'f2', 'v2');
SELECT pgredis."HDEL"('f:h', 'f1');
COMMIT;
SELECT field, convert_from(value, 'utf8') AS v
  FROM pgredis.hash_fields WHERE key = 'f:h' ORDER BY field;
SELECT count(*) FROM pgredis.hash_fields WHERE key = 'f:h'; -- 1

-- 11.4: LPUSH a; LPUSH b; RPUSH c; LPOP
--     => list_items contains the surviving rows in ord order.
--     With empty list, first LPUSH sits at ord=0. LPUSH again -> ord=-1.
--     RPUSH c -> ord=1. LPOP removes ord=-1.
BEGIN;
SELECT pgredis."LPUSH"('f:l', 'a');   -- ord=0
SELECT pgredis."LPUSH"('f:l', 'b');   -- ord=-1
SELECT pgredis."RPUSH"('f:l', 'c');   -- ord=1
SELECT pgredis."LPOP"('f:l');         -- removes ord=-1 (value 'b')
COMMIT;
SELECT ord, convert_from(value, 'utf8') AS v
  FROM pgredis.list_items WHERE key = 'f:l' ORDER BY ord;

-- 11.5: persistence_mode='none' => no DML against any of the three durable tables.
SELECT pgredis."FLUSHALL"();
SET pg_redis.persistence_mode = 'none';
SELECT pgredis."SET"('n:1', 'v');
SELECT pgredis."HSET"('n:h', 'f', 'v');
SELECT pgredis."RPUSH"('n:l', 'v');
-- Each of these counts MUST be zero — none mode skips the durable write.
SELECT count(*) AS store_rows FROM pgredis.store;
SELECT count(*) AS hash_rows FROM pgredis.hash_fields;
SELECT count(*) AS list_rows FROM pgredis.list_items;
-- And the in-memory view is still authoritative for reads.
SELECT pgredis."GET"('n:1');
SELECT pgredis."HGET"('n:h', 'f');
RESET pg_redis.persistence_mode;

DROP EXTENSION pg_redis CASCADE;
