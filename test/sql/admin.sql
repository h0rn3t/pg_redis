-- STATS / INFO / SAVE / BGSAVE / BGREWRITEAOF / KEYS / PG18 virtual column.
CREATE EXTENSION pg_redis;

-- Empty store: every counter is zero.
SELECT key_count, string_count, int_count, hash_count, list_count, expiring_count
  FROM pgredis."STATS"();

-- Populate one of each type.
SELECT pgredis."SET"('s', 'hello');
SELECT pgredis."INCR"('c');                   -- creates an int
SELECT pgredis."HSET"('h', 'f', 'v');
SELECT pgredis."RPUSH"('l', 'v');

-- STATS reflects the population.
SELECT key_count, string_count, int_count, hash_count, list_count, expiring_count
  FROM pgredis."STATS"();

-- INFO returns non-empty text.
SELECT length(pgredis."INFO"()) > 0 AS info_nonempty;

-- KEYS returns every key (ordered for determinism).
SELECT key FROM pgredis."KEYS"() AS t(key) ORDER BY key;

-- PG18 virtual generated column on pgredis.store.
SELECT key, key_bytes FROM pgredis.store ORDER BY key;

-- SAVE writes a row with a UUIDv7 id (PG18 feature).
SELECT pgredis."SAVE"();
SELECT pg_typeof(snapshot_id)::text AS id_type, key_count
  FROM pgredis.snapshots ORDER BY created_at LIMIT 1;

-- BGSAVE schedules a snapshot_save job (returns true).
SELECT pgredis."BGSAVE"();
SELECT count(*) > 0 AS bgsave_scheduled
  FROM pgredis.jobs WHERE job_type = 'snapshot_save';

-- BGREWRITEAOF returns false in v0.1 and raises a NOTICE.
SELECT pgredis."BGREWRITEAOF"();

DROP EXTENSION pg_redis CASCADE;
