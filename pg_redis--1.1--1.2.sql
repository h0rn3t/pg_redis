-- pg_redis 1.1 -> 1.2 migration
--
-- v1.2 is a crash-safety release. The on-disk schema (pgredis.store,
-- hash_fields, list_items, ...) and the binary TLV value format are UNCHANGED;
-- all 1.2 fixes live in the C extension (shared-memory sizing, lock ordering,
-- the two-phase dirty-ring, the commit-ordered drain, TOAST detoast on load,
-- a configurable bgworker database). The only SQL-surface change is the
-- crash-safety diagnostic functions below.
--
-- IMPORTANT: the in-shared-memory keyspace layout changed (HASH_PARTITION was
-- dropped from the shared HTAB). Shared memory is rebuilt from the durable
-- tables at postmaster start, so a clean restart of the cluster after upgrading
-- the binary is required; the durable tables are not affected.

\echo Use "ALTER EXTENSION pg_redis UPDATE TO '1.2'" to load this file. \quit

-- =========================================================================
-- Crash-safety diagnostics (v1.2)
-- =========================================================================

-- Snapshot of the async dirty-ring counters and slot state. Used to verify the
-- ring invariants (write_head/read_head monotonicity, no stuck WRITING slots)
-- in tests and during operations.
CREATE FUNCTION pgredis."RING_INSPECT"(
    OUT write_head          bigint,
    OUT read_head           bigint,
    OUT events_pending      bigint,
    OUT stuck_writing_slots int)
RETURNS record
AS 'MODULE_PATHNAME', 'pg_redis_ring_inspect' LANGUAGE C;

-- Current value of the cold-start coordination atomic:
--   0 = unloaded, 1 = loading, 2 = loaded; NULL in session mode.
CREATE FUNCTION pgredis."LOAD_STATE"() RETURNS int
AS 'MODULE_PATHNAME', 'pg_redis_load_state' LANGUAGE C;

-- Backends currently waiting on a pg_redis LWLock tranche, grouped by wait
-- event. An empty result means no contention on the pg_redis locks — the
-- invariant the no-SPI-under-LWLock tests assert.
CREATE FUNCTION pgredis."LWLOCK_DIAG"()
RETURNS TABLE (wait_event text, waiters bigint)
AS $$
    SELECT a.wait_event, count(*)::bigint
      FROM pg_stat_activity a
     WHERE a.wait_event_type = 'LWLock'
       AND a.wait_event LIKE 'pg_redis%'
     GROUP BY a.wait_event
     ORDER BY a.wait_event;
$$ LANGUAGE SQL STABLE;
