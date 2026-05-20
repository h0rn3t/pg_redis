-- EXPIRE / TTL semantics: -2 / -1 / seconds, immediate expiry on seconds<=0
CREATE EXTENSION pg_redis;

-- -2 on missing key
SELECT pgredis."TTL"('nope');

-- -1 on key without expire
SELECT pgredis."SET"('k', 'v');
SELECT pgredis."TTL"('k');

-- EXPIRE returns false for missing key
SELECT pgredis."EXPIRE"('nope', 60);

-- EXPIRE on existing returns true, TTL between 59 and 60
SELECT pgredis."EXPIRE"('k', 60);
SELECT pgredis."TTL"('k') BETWEEN 59 AND 60 AS ttl_ok;

-- EXPIRE with zero deletes the key immediately
SELECT pgredis."EXPIRE"('k', 0);
SELECT pgredis."EXISTS"('k');
SELECT pgredis."TTL"('k');                  -- -2

-- EXPIRE with negative also deletes
SELECT pgredis."SET"('k2', 'v2');
SELECT pgredis."EXPIRE"('k2', -5);
SELECT pgredis."EXISTS"('k2');

-- Lazy expiry via short TTL + sleep
SELECT pgredis."SET"('short', 's');
SELECT pgredis."EXPIRE"('short', 1);
SELECT pg_sleep(1.2);
SELECT pgredis."GET"('short') IS NULL AS expired_returns_null;
SELECT pgredis."EXISTS"('short');           -- false (also lazily removed)

DROP EXTENSION pg_redis CASCADE;
