-- sync_table batched pre-commit flush + SAVE/snapshots + rollback semantics
CREATE EXTENSION pg_redis;

SHOW pg_redis.persistence_mode;

-- SET writes a row in pgredis.store (TLV-encoded bytea: 0x01 || u32 len || payload)
SELECT pgredis."SET"('p:1', 'one');
SELECT key, type, length(value) AS vlen, get_byte(value, 0) AS tag
  FROM pgredis.store WHERE key = 'p:1';

-- HSET shape: parent row in pgredis.store with NULL value; one row per field
-- in pgredis.hash_fields.
SELECT pgredis."HSET"('p:h', 'name', 'Alice');
SELECT pgredis."HSET"('p:h', 'age', '30');
SELECT type, value IS NULL AS parent_null FROM pgredis.store WHERE key = 'p:h';
SELECT field, convert_from(value, 'utf8') AS v
  FROM pgredis.hash_fields WHERE key = 'p:h' ORDER BY field;

-- RPUSH shape: parent row + per-element rows in pgredis.list_items with
-- stable ordinals (RPUSH appends starting at 0, then max+1).
SELECT pgredis."RPUSH"('p:l', 'a');
SELECT pgredis."RPUSH"('p:l', 'b');
SELECT type, value IS NULL AS parent_null FROM pgredis.store WHERE key = 'p:l';
SELECT ord, convert_from(value, 'utf8') AS v
  FROM pgredis.list_items WHERE key = 'p:l' ORDER BY ord;

-- DEL removes the durable row (and via FK CASCADE, any child rows).
SELECT pgredis."DEL"('p:1');
SELECT count(*) FROM pgredis.store WHERE key = 'p:1';   -- 0

SELECT pgredis."DEL"('p:h');
SELECT count(*) FROM pgredis.store WHERE key = 'p:h';   -- 0
SELECT count(*) FROM pgredis.hash_fields WHERE key = 'p:h'; -- 0 (cascade)

-- ROLLBACK: durable row must not survive
BEGIN;
SELECT pgredis."SET"('p:rb', 'tmp');
ROLLBACK;
SELECT count(*) FROM pgredis.store WHERE key = 'p:rb'; -- 0

-- SAVE writes a row in pgredis.snapshots
SELECT pgredis."SAVE"();
SELECT count(*) FROM pgredis.snapshots WHERE key_count >= 0;

-- BGREWRITEAOF returns false with NOTICE
SELECT pgredis."BGREWRITEAOF"();

DROP EXTENSION pg_redis CASCADE;
