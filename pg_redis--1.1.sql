-- pg_redis 1.1
-- Redis-like in-memory key-value store embedded in PostgreSQL.
--
-- v1.1 changes vs v1.0:
--   * pgredis.store.value is bytea (was jsonb); holds a binary TLV
--     [u8 tag][u32 length_le][payload] for string (0x01) and int (0x02).
--   * hash payloads live in pgredis.hash_fields (key, field, value),
--     list payloads live in pgredis.list_items (key, ord, value);
--     pgredis.store.value is NULL for hash/list parents.
--   * persistence flushes batch at transaction pre-commit instead of
--     synchronously per command.

\echo Use "CREATE EXTENSION pg_redis" to load this file. \quit

-- =========================================================================
-- Internal tables
-- =========================================================================

CREATE TABLE IF NOT EXISTS pgredis.store (
    key        text PRIMARY KEY,
    type       text NOT NULL,
    value      bytea,
    expire_at  timestamptz,
    version    bigint NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT now(),
    -- PG18: virtual generated column, evaluated on read, never stored
    key_bytes  int GENERATED ALWAYS AS (octet_length(key)) VIRTUAL
);
SELECT pg_catalog.pg_extension_config_dump('pgredis.store', '');

CREATE INDEX IF NOT EXISTS store_expire_at_idx
    ON pgredis.store(expire_at)
    WHERE expire_at IS NOT NULL;

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

CREATE TABLE IF NOT EXISTS pgredis.meta (
    name       text PRIMARY KEY,
    value      jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
SELECT pg_catalog.pg_extension_config_dump('pgredis.meta', '');

CREATE TABLE IF NOT EXISTS pgredis.jobs (
    job_id            bigserial PRIMARY KEY,
    job_type          text NOT NULL,
    schedule_interval interval NOT NULL,
    config            jsonb NOT NULL DEFAULT '{}',
    enabled           boolean NOT NULL DEFAULT true,
    last_run          timestamptz,
    next_run          timestamptz,
    last_error        text,
    created_at        timestamptz NOT NULL DEFAULT now()
);
SELECT pg_catalog.pg_extension_config_dump('pgredis.jobs', '');
SELECT pg_catalog.pg_extension_config_dump('pgredis.jobs_job_id_seq', '');

CREATE TABLE IF NOT EXISTS pgredis.job_stats (
    job_id           bigint PRIMARY KEY REFERENCES pgredis.jobs(job_id) ON DELETE CASCADE,
    total_runs       bigint NOT NULL DEFAULT 0,
    successful_runs  bigint NOT NULL DEFAULT 0,
    failed_runs      bigint NOT NULL DEFAULT 0,
    last_duration_ms bigint,
    last_success     timestamptz,
    last_failure     timestamptz,
    last_error       text
);
SELECT pg_catalog.pg_extension_config_dump('pgredis.job_stats', '');

CREATE TABLE IF NOT EXISTS pgredis.snapshots (
    -- PG18: time-ordered UUIDv7 as the snapshot id; sortable, no sequence.
    snapshot_id uuid PRIMARY KEY DEFAULT uuidv7(),
    created_at  timestamptz NOT NULL DEFAULT now(),
    key_count   bigint NOT NULL,
    payload     jsonb NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('pgredis.snapshots', '');

CREATE TABLE IF NOT EXISTS pgredis.aof (
    id   bigserial PRIMARY KEY,
    ts   timestamptz NOT NULL DEFAULT now(),
    op   text NOT NULL,
    key  text,
    args jsonb NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('pgredis.aof', '');
SELECT pg_catalog.pg_extension_config_dump('pgredis.aof_id_seq', '');

-- =========================================================================
-- String / generic commands
-- =========================================================================

CREATE FUNCTION pgredis."SET"(key text, value text) RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_redis_set' LANGUAGE C STRICT;

CREATE FUNCTION pgredis."GET"(key text) RETURNS text
AS 'MODULE_PATHNAME', 'pg_redis_get' LANGUAGE C STRICT;

CREATE FUNCTION pgredis."DEL"(key text) RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_redis_del' LANGUAGE C STRICT;

CREATE FUNCTION pgredis."EXISTS"(key text) RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_redis_exists' LANGUAGE C STRICT;

CREATE FUNCTION pgredis."EXPIRE"(key text, seconds int) RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_redis_expire' LANGUAGE C STRICT;

CREATE FUNCTION pgredis."TTL"(key text) RETURNS int
AS 'MODULE_PATHNAME', 'pg_redis_ttl' LANGUAGE C STRICT;

CREATE FUNCTION pgredis."INCR"(key text) RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_redis_incr' LANGUAGE C STRICT;

CREATE FUNCTION pgredis."DECR"(key text) RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_redis_decr' LANGUAGE C STRICT;

-- =========================================================================
-- Hash commands
-- =========================================================================

CREATE FUNCTION pgredis."HSET"(key text, field text, value text) RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_redis_hset' LANGUAGE C STRICT;

CREATE FUNCTION pgredis."HGET"(key text, field text) RETURNS text
AS 'MODULE_PATHNAME', 'pg_redis_hget' LANGUAGE C STRICT;

CREATE FUNCTION pgredis."HDEL"(key text, field text) RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_redis_hdel' LANGUAGE C STRICT;

CREATE FUNCTION pgredis."HEXISTS"(key text, field text) RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_redis_hexists' LANGUAGE C STRICT;

-- =========================================================================
-- List commands
-- =========================================================================

CREATE FUNCTION pgredis."LPUSH"(key text, value text) RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_redis_lpush' LANGUAGE C STRICT;

CREATE FUNCTION pgredis."RPUSH"(key text, value text) RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_redis_rpush' LANGUAGE C STRICT;

CREATE FUNCTION pgredis."LPOP"(key text) RETURNS text
AS 'MODULE_PATHNAME', 'pg_redis_lpop' LANGUAGE C STRICT;

CREATE FUNCTION pgredis."RPOP"(key text) RETURNS text
AS 'MODULE_PATHNAME', 'pg_redis_rpop' LANGUAGE C STRICT;

CREATE FUNCTION pgredis."LLEN"(key text) RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_redis_llen' LANGUAGE C STRICT;

-- =========================================================================
-- Admin / debug commands
-- =========================================================================

CREATE FUNCTION pgredis."KEYS"() RETURNS SETOF text
AS 'MODULE_PATHNAME', 'pg_redis_keys' LANGUAGE C;

CREATE FUNCTION pgredis."FLUSHALL"() RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_redis_flushall' LANGUAGE C;

CREATE FUNCTION pgredis."MEMORY_USAGE"() RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_redis_memory_usage' LANGUAGE C;

CREATE FUNCTION pgredis."INFO"() RETURNS text
AS 'MODULE_PATHNAME', 'pg_redis_info' LANGUAGE C;

CREATE TYPE pgredis.stats_result AS (
    key_count       bigint,
    string_count    bigint,
    int_count       bigint,
    hash_count      bigint,
    list_count      bigint,
    expiring_count  bigint,
    memory_usage    bigint
);

CREATE FUNCTION pgredis."STATS"() RETURNS pgredis.stats_result
AS 'MODULE_PATHNAME', 'pg_redis_stats' LANGUAGE C;

-- =========================================================================
-- Persistence commands
-- =========================================================================

CREATE FUNCTION pgredis."SAVE"() RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_redis_save' LANGUAGE C;

CREATE FUNCTION pgredis."BGSAVE"() RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_redis_bgsave' LANGUAGE C;

CREATE FUNCTION pgredis."BGREWRITEAOF"() RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_redis_bgrewriteaof' LANGUAGE C;

-- =========================================================================
-- Background job management (SQL-driven scheduler)
-- =========================================================================

CREATE FUNCTION pgredis."ADD_FLUSH_POLICY"(schedule_interval interval) RETURNS bigint
AS $$
    INSERT INTO pgredis.jobs (job_type, schedule_interval, next_run)
    VALUES ('flush_dirty_keys', schedule_interval, now() + schedule_interval)
    RETURNING job_id;
$$ LANGUAGE SQL;

CREATE FUNCTION pgredis."ADD_TTL_CLEANUP_POLICY"(schedule_interval interval) RETURNS bigint
AS $$
    INSERT INTO pgredis.jobs (job_type, schedule_interval, next_run)
    VALUES ('ttl_cleanup', schedule_interval, now() + schedule_interval)
    RETURNING job_id;
$$ LANGUAGE SQL;

CREATE FUNCTION pgredis."ADD_SNAPSHOT_POLICY"(schedule_interval interval) RETURNS bigint
AS $$
    INSERT INTO pgredis.jobs (job_type, schedule_interval, next_run)
    VALUES ('snapshot_save', schedule_interval, now() + schedule_interval)
    RETURNING job_id;
$$ LANGUAGE SQL;

CREATE FUNCTION pgredis."DELETE_JOB"(p_job_id bigint) RETURNS boolean
AS $$
    WITH d AS (DELETE FROM pgredis.jobs WHERE job_id = p_job_id RETURNING 1)
    SELECT count(*) > 0 FROM d;
$$ LANGUAGE SQL;

CREATE FUNCTION pgredis."RUN_JOB"(p_job_id bigint) RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_redis_run_job' LANGUAGE C STRICT;

CREATE FUNCTION pgredis."JOBS"()
RETURNS TABLE (
    job_id            bigint,
    job_type          text,
    schedule_interval interval,
    enabled           boolean,
    last_run          timestamptz,
    next_run          timestamptz,
    last_error        text
)
AS $$
    SELECT job_id, job_type, schedule_interval, enabled, last_run, next_run, last_error
      FROM pgredis.jobs
     ORDER BY job_id;
$$ LANGUAGE SQL STABLE;

CREATE FUNCTION pgredis."JOB_STATS"()
RETURNS TABLE (
    job_id           bigint,
    total_runs       bigint,
    successful_runs  bigint,
    failed_runs      bigint,
    last_duration_ms bigint,
    last_success     timestamptz,
    last_failure     timestamptz,
    last_error       text
)
AS $$
    SELECT job_id, total_runs, successful_runs, failed_runs,
           last_duration_ms, last_success, last_failure, last_error
      FROM pgredis.job_stats
     ORDER BY job_id;
$$ LANGUAGE SQL STABLE;
