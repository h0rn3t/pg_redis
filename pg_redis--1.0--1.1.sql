-- pg_redis 1.0 -> 1.1 migration
--
-- v1.1 reshapes the durable layout:
--   * pgredis.store.value becomes bytea (TLV-encoded string/int).
--   * Hash payloads move into pgredis.hash_fields.
--   * List payloads move into pgredis.list_items.
--
-- Existing v1.0 data in pgredis.store CANNOT be migrated in place because the
-- jsonb -> bytea TLV translation requires the C-level encoder. Operators must
-- explicitly empty the keyspace before upgrading (see README "Migration v0.1
-- -> v0.2"). We enforce that here.

\echo Use "ALTER EXTENSION pg_redis UPDATE TO '1.1'" to load this file. \quit

DO $migrate$
DECLARE
    legacy_count bigint;
BEGIN
    SELECT count(*) INTO legacy_count FROM pgredis.store;
    IF legacy_count > 0 THEN
        RAISE NOTICE 'pg_redis 1.0 -> 1.1 requires an empty pgredis.store.';
        RAISE NOTICE 'Run: SELECT pgredis."FLUSHALL"();  -- discards in-memory + durable data';
        RAISE NOTICE '  or SELECT pgredis."SAVE"() before FLUSHALL to snapshot into pgredis.snapshots first.';
        RAISE EXCEPTION 'pg_redis 1.0 -> 1.1 migration aborted: pgredis.store contains % rows. See notices for required pre-upgrade steps.', legacy_count;
    END IF;
END;
$migrate$;

ALTER TABLE pgredis.store DROP COLUMN value;
ALTER TABLE pgredis.store ADD COLUMN value bytea;

CREATE TABLE IF NOT EXISTS pgredis.hash_fields (
    key   text  NOT NULL,
    field text  NOT NULL,
    value bytea,
    PRIMARY KEY (key, field),
    FOREIGN KEY (key) REFERENCES pgredis.store(key) ON DELETE CASCADE
);
SELECT pg_catalog.pg_extension_config_dump('pgredis.hash_fields', '');

CREATE TABLE IF NOT EXISTS pgredis.list_items (
    key   text   NOT NULL,
    ord   bigint NOT NULL,
    value bytea,
    PRIMARY KEY (key, ord),
    FOREIGN KEY (key) REFERENCES pgredis.store(key) ON DELETE CASCADE
);
SELECT pg_catalog.pg_extension_config_dump('pgredis.list_items', '');
