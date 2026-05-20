-- Regression test for the sync_flush producer fallback executing inside a
-- user transaction. Requires a cluster started with:
--   shared_preload_libraries           = 'pg_redis'
--   pg_redis.storage_mode              = 'shared'
--   pg_redis.dirty_ring_size           = 1024  (minimum allowed; small enough
--                                                to overflow inside one xact)
--   pg_redis.async_full_action         = 'sync_flush'  (PGC_SIGHUP — must be
--                                                       set at postmaster level
--                                                       or in postgresql.conf)
--   pg_redis.enable_background_worker  = off   (so producers must drain)
-- See scripts/docker-test-async.sh for the harness that boots this config.
--
-- The scenario reproduces the original bug ("StartTransactionCommand:
-- unexpected state STARTED"): a single user transaction issues enough SETs
-- to overflow the ring. With pg_redis.async_full_action = 'sync_flush' and
-- the BGW disabled, the producer is the only entity that can free slots,
-- so it MUST sync-drain. The fix exercises BeginInternalSubTransaction so
-- the drain succeeds without aborting the outer xact.
--
-- We use SET (one event per call, distinct keys) rather than HSET on the
-- same hash. HSET-on-same-key currently re-publishes every existing field
-- in the hash on each call (a separate pre-existing O(N^2) issue in
-- pg_redis_shared_hash_materialize), which would dominate the test and
-- obscure what we're actually exercising here.

\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS pg_redis;

-- Assert the test harness wired the required postmaster-level GUCs.
DO $$
DECLARE
    v_storage text := current_setting('pg_redis.storage_mode');
    v_ring    int  := current_setting('pg_redis.dirty_ring_size')::int;
    v_bgw     bool := current_setting('pg_redis.enable_background_worker')::bool;
BEGIN
    IF v_storage <> 'shared' THEN
        RAISE EXCEPTION 'pg_redis.storage_mode must be ''shared'' (got %)', v_storage;
    END IF;
    IF v_ring > 1024 THEN
        RAISE EXCEPTION 'pg_redis.dirty_ring_size must be at the minimum (1024) for this test (got %)', v_ring;
    END IF;
    IF v_bgw THEN
        RAISE EXCEPTION 'pg_redis.enable_background_worker must be off so the producer is the sole drainer';
    END IF;
END;
$$;

-- Fail fast instead of hanging if the producer/drain enters a bad state.
SET statement_timeout = '60s';

SET pg_redis.persistence_mode = 'async_table';

DO $$
BEGIN
    IF current_setting('pg_redis.async_full_action') <> 'sync_flush' THEN
        RAISE EXCEPTION 'pg_redis.async_full_action must be set to sync_flush at postmaster level (got %)',
            current_setting('pg_redis.async_full_action');
    END IF;
END;
$$;

-- Start clean so the assertions on durable rows are deterministic.
SELECT pgredis."FLUSHALL"();

-- Push N >> dirty_ring_size events inside a single user transaction. With
-- ring=1024 and BGW off, the producer hits the sync_flush fallback ~2x
-- during this loop. Before the fix this raised
-- "StartTransactionCommand: unexpected state STARTED" and aborted the xact.
-- Primary assertion: BEGIN ... COMMIT completes without error.
-- (psql runs with --set ON_ERROR_STOP=on; any error fails the harness.)
\timing on
BEGIN;
SELECT count(*) FROM (
    SELECT pgredis."SET"('k' || g, 'v' || g)
    FROM generate_series(1, 1500) g
) s;
COMMIT;
\timing off

-- Secondary assertion: the sync_flush path actually fired (not a false pass
-- where overflow never happened). With N=1500 and ring=1024, exactly one
-- overflow occurs, causing one sync_drain that persists ~1024 rows to
-- pgredis.store inside its own subtransaction. The trailing ~476 events
-- stay in the ring (no further BGW drain because BGW is off).
DO $$
DECLARE
    n_durable bigint;
BEGIN
    SELECT count(*) INTO n_durable
    FROM pgredis.store
    WHERE key LIKE 'k%';
    IF n_durable < 1000 THEN
        RAISE EXCEPTION
            'expected sync_flush to have persisted >=1000 rows, got % — sync_flush path may not have fired',
            n_durable;
    END IF;
    RAISE NOTICE 'sync_flush persisted % rows (>=1000 expected)', n_durable;
END;
$$;

-- In-memory state: all 1500 keys readable via GET (shared-keyspace path).
DO $$
DECLARE
    v text;
BEGIN
    v := pgredis."GET"('k1');
    IF v IS DISTINCT FROM 'v1' THEN
        RAISE EXCEPTION 'expected GET(k1) = v1, got %', v;
    END IF;
    v := pgredis."GET"('k1500');
    IF v IS DISTINCT FROM 'v1500' THEN
        RAISE EXCEPTION 'expected GET(k1500) = v1500, got %', v;
    END IF;
END;
$$;

SELECT pgredis."FLUSHALL"();
DROP EXTENSION pg_redis CASCADE;

\echo PASS sync_flush producer fallback inside user transaction
