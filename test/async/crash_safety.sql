-- Crash-safety regression test (the `fix-crash-safety` change). Runs under the
-- docker-test-async harness, which boots a cluster with:
--   shared_preload_libraries          = 'pg_redis'
--   pg_redis.storage_mode             = 'shared'
--   pg_redis.dirty_ring_size          = 1024   (minimum; small enough to
--                                                overflow inside one xact)
--   pg_redis.async_full_action        = 'sync_flush'
--   pg_redis.enable_background_worker = off    (producers are the sole drainer)
--
-- It exercises the v1.2 surface: the diagnostic functions, the two-phase
-- dirty-ring invariants, the commit-ordered (at-least-once) drain, and the
-- FLUSHALL-releases-locks-before-TRUNCATE path. The harness runs this with
-- ON_ERROR_STOP=on, so any RAISE EXCEPTION fails the test.

\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS pg_redis;

-- Confirm the harness wired the postmaster-level GUCs this test depends on.
DO $$
BEGIN
    IF current_setting('pg_redis.storage_mode') <> 'shared' THEN
        RAISE EXCEPTION 'pg_redis.storage_mode must be ''shared''';
    END IF;
    IF current_setting('pg_redis.dirty_ring_size')::int > 1024 THEN
        RAISE EXCEPTION 'pg_redis.dirty_ring_size must be 1024 (the min) for this test';
    END IF;
    IF current_setting('pg_redis.enable_background_worker')::bool THEN
        RAISE EXCEPTION 'pg_redis.enable_background_worker must be off';
    END IF;
    IF current_setting('pg_redis.async_full_action') <> 'sync_flush' THEN
        RAISE EXCEPTION 'pg_redis.async_full_action must be sync_flush';
    END IF;
END;
$$;

SET statement_timeout = '60s';
SET pg_redis.persistence_mode = 'async_table';

SELECT pgredis."FLUSHALL"();

-- ---------------------------------------------------------------------------
-- 1. Diagnostics exist and the cold-start state machine reaches "loaded".
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    s int;
BEGIN
    PERFORM pgredis."GET"('does-not-exist');   -- triggers the cold-start load
    s := pgredis."LOAD_STATE"();
    IF s IS DISTINCT FROM 2 THEN
        RAISE EXCEPTION 'LOAD_STATE expected 2 (loaded) in shared mode, got %', s;
    END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- 2. Two-phase dirty-ring invariants hold after many sync_flush drains.
--    3000 events with ring=1024 forces several producer-side drains.
-- ---------------------------------------------------------------------------
BEGIN;
SELECT count(*) FROM (
    SELECT pgredis."SET"('k' || g, 'v' || g)
    FROM generate_series(1, 3000) g
) s;
COMMIT;

DO $$
DECLARE
    w bigint; r bigint; pend bigint; stuck int;
BEGIN
    SELECT write_head, read_head, events_pending, stuck_writing_slots
      INTO w, r, pend, stuck
      FROM pgredis."RING_INSPECT"();
    IF stuck <> 0 THEN
        RAISE EXCEPTION 'stuck_writing_slots expected 0, got % (producer left a hole)', stuck;
    END IF;
    IF w < r THEN
        RAISE EXCEPTION 'write_head(%) < read_head(%) — read_head rewound (C2)', w, r;
    END IF;
    IF pend <> (w - r) THEN
        RAISE EXCEPTION 'events_pending(%) != write_head - read_head (%)', pend, w - r;
    END IF;
    RAISE NOTICE 'ring invariants ok: write_head=% read_head=% pending=% stuck=%',
        w, r, pend, stuck;
END;
$$;

-- Data integrity through the shared keyspace + sync_flush.
DO $$
BEGIN
    IF pgredis."GET"('k1') IS DISTINCT FROM 'v1' THEN
        RAISE EXCEPTION 'GET(k1) wrong';
    END IF;
    IF pgredis."GET"('k3000') IS DISTINCT FROM 'v3000' THEN
        RAISE EXCEPTION 'GET(k3000) wrong';
    END IF;
END;
$$;

-- No backend should be parked on a pg_redis LWLock at steady state.
DO $$
DECLARE
    n bigint;
BEGIN
    SELECT coalesce(sum(waiters), 0) INTO n FROM pgredis."LWLOCK_DIAG"();
    IF n <> 0 THEN
        RAISE EXCEPTION 'unexpected pg_redis LWLock waiters: %', n;
    END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- 3. At-least-once drain: a persist failure must NOT silently drop events
--    (Decision 11 / C3). Inject a durable-table failure, force a sync_flush
--    drain into it, and confirm nothing was lost.
-- ---------------------------------------------------------------------------
SELECT pgredis."FLUSHALL"();

CREATE OR REPLACE FUNCTION pgredis._fail_store_ins() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected store failure'; END $$;
CREATE TRIGGER _pgredis_fail BEFORE INSERT ON pgredis.store
    FOR EACH ROW EXECUTE FUNCTION pgredis._fail_store_ins();

DO $$
DECLARE
    got_error bool := false;
    pend bigint;
    n_durable bigint;
BEGIN
    -- Overflow the ring so a producer must sync-drain; the drain's INSERT hits
    -- the trigger and raises. The two-phase drain resets its slots to READY and
    -- leaves read_head unchanged, then re-throws — we catch it here.
    BEGIN
        PERFORM count(*) FROM (
            SELECT pgredis."SET"('f' || g, 'v' || g)
            FROM generate_series(1, 3000) g
        ) s;
    EXCEPTION WHEN others THEN
        got_error := true;
    END;

    IF NOT got_error THEN
        RAISE EXCEPTION 'expected the injected failure to surface via the sync_flush drain';
    END IF;

    -- The failed flush rolled back: no durable rows leaked in.
    SELECT count(*) INTO n_durable FROM pgredis.store;
    IF n_durable <> 0 THEN
        RAISE EXCEPTION 'durable store should be empty after a failed flush, got %', n_durable;
    END IF;

    -- The events are still in the ring (at-least-once), not silently dropped.
    SELECT events_pending INTO pend FROM pgredis."RING_INSPECT"();
    IF pend = 0 THEN
        RAISE EXCEPTION 'events were lost on persist failure (events_pending=0) — C3 regression';
    END IF;
    RAISE NOTICE 'at-least-once held: % events retained after persist failure', pend;
END;
$$;

-- Heal the durable path, then force successful drains. The retained events must
-- now persist (proving the ring kept them across the failure).
DROP TRIGGER _pgredis_fail ON pgredis.store;
DROP FUNCTION pgredis._fail_store_ins();

BEGIN;
SELECT count(*) FROM (
    SELECT pgredis."SET"('f' || g, 'v' || g)
    FROM generate_series(3001, 6000) g
) s;
COMMIT;

DO $$
DECLARE
    n_durable bigint;
BEGIN
    SELECT count(*) INTO n_durable FROM pgredis.store WHERE key LIKE 'f%';
    IF n_durable = 0 THEN
        RAISE EXCEPTION 'no f-keys persisted after healing — retained events never drained';
    END IF;
    RAISE NOTICE 'post-heal durable f-keys: %', n_durable;
END;
$$;

-- ---------------------------------------------------------------------------
-- 4. FLUSHALL wipes memory + durable + ring with no error (M1).
-- ---------------------------------------------------------------------------
SELECT pgredis."FLUSHALL"();

DO $$
DECLARE
    n bigint;
    pend bigint;
BEGIN
    SELECT count(*) INTO n FROM pgredis.store;
    IF n <> 0 THEN
        RAISE EXCEPTION 'durable store not empty after FLUSHALL: %', n;
    END IF;
    IF pgredis."GET"('k1') IS NOT NULL THEN
        RAISE EXCEPTION 'k1 survived FLUSHALL';
    END IF;
    SELECT events_pending INTO pend FROM pgredis."RING_INSPECT"();
    IF pend <> 0 THEN
        RAISE EXCEPTION 'ring not drained after FLUSHALL: %', pend;
    END IF;
END;
$$;

DROP EXTENSION pg_redis CASCADE;

\echo PASS crash_safety
