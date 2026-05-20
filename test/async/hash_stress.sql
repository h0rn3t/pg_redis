-- Regression test for the DSA-backed open-addressing hash table used by
-- per-key field storage when pg_redis.storage_mode = 'shared'. Drives the
-- table through grow + tombstone-heavy + shrink patterns and verifies
-- that HGET / HEXISTS agree with an in-PL reference model.

\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS pg_redis;

SELECT pgredis."FLUSHALL"();

-- 1. Insert 1000 fields, forcing several grow rehashes
--    (16 -> 32 -> 64 -> ... -> 2048).
DO $$
BEGIN
    FOR i IN 1..1000 LOOP
        PERFORM pgredis."HSET"('h:stress', 'f' || i, 'v' || i);
    END LOOP;
END$$;

-- 2. Verify every field is readable.
DO $$
DECLARE
    bad int := 0;
BEGIN
    FOR i IN 1..1000 LOOP
        IF pgredis."HGET"('h:stress', 'f' || i) <> 'v' || i THEN
            bad := bad + 1;
        END IF;
    END LOOP;
    IF bad > 0 THEN
        RAISE EXCEPTION 'HGET mismatched on % fields after insert', bad;
    END IF;
END$$;

-- 3. Overwrite every field with a new value. Tests the in-place overwrite
--    path (was_found=true) — no growth, no tombstones.
DO $$
BEGIN
    FOR i IN 1..1000 LOOP
        PERFORM pgredis."HSET"('h:stress', 'f' || i, 'V' || i);
    END LOOP;
END$$;

DO $$
DECLARE
    bad int := 0;
BEGIN
    FOR i IN 1..1000 LOOP
        IF pgredis."HGET"('h:stress', 'f' || i) <> 'V' || i THEN
            bad := bad + 1;
        END IF;
    END LOOP;
    IF bad > 0 THEN
        RAISE EXCEPTION 'HGET mismatched on % fields after overwrite', bad;
    END IF;
END$$;

-- 4. Delete every odd-indexed field. Forces lots of tombstones, exercises
--    the lazy-shrink threshold at the lower end.
DO $$
BEGIN
    FOR i IN 1..999 BY 2 LOOP
        PERFORM pgredis."HDEL"('h:stress', 'f' || i);
    END LOOP;
END$$;

-- 5. Confirm even fields are still readable, odd fields return NULL.
DO $$
DECLARE
    bad_present int := 0;
    bad_absent  int := 0;
BEGIN
    FOR i IN 1..1000 LOOP
        IF i % 2 = 0 THEN
            IF pgredis."HGET"('h:stress', 'f' || i) <> 'V' || i THEN
                bad_present := bad_present + 1;
            END IF;
            IF NOT pgredis."HEXISTS"('h:stress', 'f' || i) THEN
                bad_present := bad_present + 1;
            END IF;
        ELSE
            IF pgredis."HGET"('h:stress', 'f' || i) IS NOT NULL THEN
                bad_absent := bad_absent + 1;
            END IF;
            IF pgredis."HEXISTS"('h:stress', 'f' || i) THEN
                bad_absent := bad_absent + 1;
            END IF;
        END IF;
    END LOOP;
    IF bad_present > 0 OR bad_absent > 0 THEN
        RAISE EXCEPTION 'mismatch: bad_present=% bad_absent=%', bad_present, bad_absent;
    END IF;
END$$;

-- 6. Delete the remaining fields too — drives the table to empty and lets
--    the lazy shrink converge to the initial size.
DO $$
BEGIN
    FOR i IN 2..1000 BY 2 LOOP
        PERFORM pgredis."HDEL"('h:stress', 'f' || i);
    END LOOP;
END$$;

-- 7. Insert into the same key after full delete — confirms the table can
--    transition from empty back to populated.
DO $$
BEGIN
    FOR i IN 1..50 LOOP
        PERFORM pgredis."HSET"('h:stress', 'f' || i, 'after' || i);
    END LOOP;
END$$;

DO $$
DECLARE
    bad int := 0;
BEGIN
    FOR i IN 1..50 LOOP
        IF pgredis."HGET"('h:stress', 'f' || i) <> 'after' || i THEN
            bad := bad + 1;
        END IF;
    END LOOP;
    IF bad > 0 THEN
        RAISE EXCEPTION 'HGET mismatched on % fields after reinsert', bad;
    END IF;
END$$;

SELECT pgredis."FLUSHALL"();
DROP EXTENSION pg_redis;
\echo PASS hash_stress
