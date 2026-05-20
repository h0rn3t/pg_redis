# pg_redis

`pg_redis` is a PostgreSQL extension written in C that embeds a Redis-like
in-memory key-value store directly into a PostgreSQL backend, exposed through
quoted UPPERCASE SQL functions in schema `pgredis`. (The extension binary is
named `pg_redis`; PostgreSQL reserves the `pg_` schema prefix for system
catalogs, so the SQL-facing schema is the shortened `pgredis`.)

It is **not** a drop-in Redis replacement. There is no RESP protocol, no
pub/sub, no streams or sorted sets, and no clustering. What it does provide is
a small, well-tested Redis-shaped command surface — strings, integers, hashes,
lists, TTL, snapshots, and a background job scheduler — running inside the
same process that already serves your SQL queries, with durability riding on
PostgreSQL's WAL.

## Why you might want it

- Cut a Redis dependency for caches, counters, ephemeral session state, or
  lightweight job queues.
- Keep one connection pool, one backup, one ACL surface.
- Run inside an existing transaction so that a rollback also rolls back the
  durable copy of your cache write.

## Why you might not

- You need raw Redis throughput or sub-millisecond replicated clusters.
- You need pub/sub, streams, sorted sets, scripting, or Redis modules.
- You need keyspace visible across backends in real time without round-tripping
  through a table (v0.1 is session-local; see "Concurrency model" below).

## Features

- **Types**: `string`, `int`, `hash`, `list`.
- **Commands**: `SET`, `GET`, `DEL`, `EXISTS`, `EXPIRE`, `TTL`, `INCR`, `DECR`,
  `HSET`, `HGET`, `HDEL`, `HEXISTS`, `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`,
  `KEYS`, `FLUSHALL`, `MEMORY_USAGE`, `STATS`, `INFO`, `SAVE`, `BGSAVE`,
  `BGREWRITEAOF`.
- **Background jobs**: `ADD_FLUSH_POLICY`, `ADD_TTL_CLEANUP_POLICY`,
  `ADD_SNAPSHOT_POLICY`, `DELETE_JOB`, `RUN_JOB`, `JOBS`, `JOB_STATS`.
- **Persistence**: in-memory only (`none`), write-through to a logged table
  (`sync_table` — default), or table-backed snapshots (`SAVE`/`BGSAVE`).
- **Background worker** that scans `pgredis.jobs` for due jobs and runs them.

## Limitations (read before deploying)

- **Session-local keyspace.** Each PostgreSQL backend has its own copy of the
  in-memory keyspace. Setting a key in one connection does **not** make it
  visible in another connection — unless you're in the default `sync_table`
  mode, in which case the other backend reads the durable row on its first
  command and lazy-loads it into memory.
- **No fully shared keyspace yet.** The `pg_redis.storage_mode = 'shared'` GUC
  is reserved for a future release. v0.1 always behaves as session-local.
- **`async_table` falls back to `sync_table`** in v0.1. The infrastructure is
  in place but real async batched flushing requires the shared keyspace.
- **AOF is scaffolded, not implemented.** `BGREWRITEAOF` returns `false` and
  raises a `NOTICE`. The `pgredis.aof` table exists for forward compatibility.
- **Transactions.** Durable rows in `pgredis.store` participate in the calling
  transaction: a `ROLLBACK` after `SET` removes the row. The in-memory copy
  is **not** rolled back; instead, the next access in that backend lazy-reloads
  from the durable table via an `XactCallback`. Cross-session correctness
  depends on persisting through the table.

## Architecture

A short tour of what runs where, what lives in RAM, what lives on disk, and how
the two stay in sync.

### Process model

`pg_redis` is a shared library loaded into each PostgreSQL backend (the
process that handles your SQL connection). Every SQL function in `pgredis."…"`
is a C function exported from that library and runs **inside** the backend
process — no socket, no protocol layer. PostgreSQL is process-per-connection,
so:

- Each backend has its **own** copy of the in-memory keyspace.
- There is no shared keyspace across backends in v0.1; cross-backend visibility
  goes through the durable table (see "Persistence layer" below).
- One optional **background worker** (`pg_redis bgworker`) is registered when
  `shared_preload_libraries = 'pg_redis'`. It runs as a separate process,
  ticks every `pg_redis.flush_interval` seconds, and dispatches due jobs from
  `pgredis.jobs`.

### In-memory keyspace (per backend)

The hot path lives in a private chunk of the backend's address space and never
talks to disk:

```text
TopMemoryContext
└── PgRedisMemoryContext           ← long-lived, palloc'd values land here
    └── HTAB "pg_redis_store"      ← dynahash, key (text) → PgRedisEntry
        └── PgRedisEntry           ← per Redis key
            ├── key[1025]          ← inline NUL-terminated, fixed-size
            ├── type               ← STRING | INT | HASH | LIST
            ├── expire_at / has_expire  ← TTL (TimestampTz)
            ├── dirty / deleted / version / memory_usage
            └── value (union)
                ├── string_value   → char *  (palloc'd, NUL-terminated)
                ├── int_value      → int64   (inline, no alloc)
                ├── hash_value     → PgRedisHash { HTAB fields → PgRedisHashField, count, mem }
                └── list_value     → PgRedisList { doubly-linked PgRedisListNode head/tail, length, mem }
```

Key data structures (see [src/types.h](src/types.h)):

| Structure | Purpose | Where allocated |
| --- | --- | --- |
| `PgRedisEntry` | One Redis key | `dynahash` slot inside `PgRedisMemoryContext` |
| `PgRedisHash` | Hash container with its own `HTAB` of fields | `PgRedisMemoryContext` |
| `PgRedisHashField` | One hash field (`field[1025]` inline + `char *value`) | `PgRedisMemoryContext` |
| `PgRedisList` + `PgRedisListNode` | Doubly-linked list (Redis-style LPUSH/RPUSH at both ends) | `PgRedisMemoryContext` |

Notes on the in-memory design:

- The top-level store is PostgreSQL's `dynahash` HTAB (`utils/hsearch.h`),
  parented to `PgRedisMemoryContext` — see [src/kv_store.c](src/kv_store.c). All allocations for values
  (strings, list nodes, hash fields) palloc into the same context, so
  `FLUSHALL` is implemented as a single `MemoryContextReset` + HTAB rebuild.
- Hash values use a **nested** HTAB per Redis hash key — `O(1)` HGET/HSET
  regardless of field count.
- Lists are intrusive doubly-linked nodes with cached `head`, `tail`, and
  `length`, giving `O(1)` LPUSH/RPUSH/LPOP/RPOP/LLEN.
- TTL is stored as `TimestampTz` directly on the entry; expiration is
  **lazy** — checked on each access via `pg_redis_store_lookup()` — plus an
  optional periodic sweep via the `ttl_cleanup` job.
- `PgRedisEntry.dirty`, `.deleted`, `.version` exist for the (future)
  async-flush path; in sync mode the entry is persisted before the function
  returns and `dirty` is cleared immediately.

### Persistence layer (durable in PostgreSQL tables)

What's persistent and where it lives, all under schema `pgredis`
(see [pg_redis--1.1.sql](pg_redis--1.1.sql)):

| Table | Holds | Written by |
| --- | --- | --- |
| `pgredis.store` | One row per Redis key: `(key, type, value bytea, expire_at, version, updated_at)` plus the PG18 **virtual generated column** `key_bytes`. `value` holds the TLV-encoded string/int payload, or `NULL` for hash/list parents | the batched pre-commit flush in `sync_table` mode, via SPI |
| `pgredis.hash_fields` | One row per hash field: `(key, field, value bytea)`. FK `key → pgredis.store(key) ON DELETE CASCADE` | per-field upsert/delete at flush; never re-writes untouched fields |
| `pgredis.list_items` | One row per list element: `(key, ord bigint, value bytea)`. `ord` is a stable monotonic ordinal — LPUSH assigns `min(ord)-1`, RPUSH assigns `max(ord)+1`. FK `key → pgredis.store(key) ON DELETE CASCADE` | per-element insert/delete at flush; never re-writes untouched elements |
| `pgredis.meta` | Free-form metadata rows (jsonb), reserved | extension internals |
| `pgredis.jobs` | The job scheduler table | `ADD_*_POLICY`, `DELETE_JOB`, bgworker / `RUN_JOB` |
| `pgredis.job_stats` | Per-job counters | bgworker and `RUN_JOB` |
| `pgredis.snapshots` | Point-in-time dump of the keyspace as one `jsonb` payload, time-sortable `uuidv7()` PK | `SAVE` / `BGSAVE` |
| `pgredis.aof` | Scaffolded append-only log. Not populated in v0.1 | reserved |

How a value becomes a row:

- **Strings / ints** are stored in `pgredis.store.value` as a compact binary
  TLV: `[u8 tag][u32 length_le][payload]` (tag `0x01` for string, `0x02` for
  int — see `PG_REDIS_TLV_*` in [src/types.h](src/types.h),
  [src/binval.c](src/binval.c)). No JSON cast on the hot path.
- **Hashes** persist one row per field in `pgredis.hash_fields`; the parent
  row in `pgredis.store` carries `type='hash'` and `value IS NULL`. A single
  `HSET` produces a single row write, regardless of how many other fields the
  hash already has.
- **Lists** persist one row per element in `pgredis.list_items` with a stable
  `bigint` ordinal. LPUSH/RPUSH/LPOP/RPOP touch the boundary row only; the
  rest of the list is never re-written. On load, min/max ords are recomputed
  per list to anchor future pushes.
- **TTL** lives in `expire_at`. The partial index
  `store_expire_at_idx WHERE expire_at IS NOT NULL` keeps the sweep query
  fast even when most keys have no TTL.
- All `pgredis.*` tables are registered with `pg_extension_config_dump`, so
  `pg_dump` includes the rows.

Writes never go to disk synchronously per command. Mutators
(`SET`/`INCR`/`HSET`/`LPUSH`/…) mark the entry into a per-backend **dirty-set**
and queue tombstones for `DEL` / TTL eviction in a per-backend
`pending_deletes` list. At `XACT_EVENT_PRE_COMMIT` of the user's transaction,
the dirty-set drains in a **single SPI session** via six cached
`SPI_keepplan` plans, all parameterized with `unnest()` array inputs so an
entire batch — store rows, hash fields, list items — flushes in at most six
`SPI_execute_plan` calls. Repeated mutations of the same key inside one
transaction coalesce into a single durable write (see
[src/persistence.c](src/persistence.c) `run_flush`).

### Transaction integration

Because everything runs inside the calling backend, durable writes use the
**calling transaction**:

- `SET k v` → updates the in-memory entry, then upserts `pgredis.store` via
  SPI under the same `XID`. A `ROLLBACK` removes the row.
- An `XactCallback` (`pg_redis_xact_cb_persistence`) listens for
  `XACT_EVENT_ABORT` and flips a per-backend "needs reload" flag. The **next**
  access lazy-reloads the durable view; until then the in-memory copy still
  shows the aborted value — read once after rollback to re-sync.
- Lazy load (`pg_redis_persistence_load_if_needed`) also runs on the first
  command in a fresh backend: it scans `pgredis.store WHERE expire_at IS NULL
  OR expire_at > now()` and repopulates the in-memory HTAB so backend B sees
  what backend A committed.

### Background worker

Registered only when `shared_preload_libraries = 'pg_redis'` **and**
`pg_redis.enable_background_worker = on` — see [src/bgworker.c](src/bgworker.c). Lifecycle:

1. On postmaster start, `_PG_init` calls `pg_redis_bgworker_register()` to
   request a `BackgroundWorker` slot.
2. The worker connects to the `postgres` database and enters a loop:
   wait on its latch for `pg_redis.flush_interval` seconds, then call
   `pg_redis_jobs_tick()`.
3. `_tick` opens a transaction, scans `pgredis.jobs WHERE enabled AND
   next_run <= now()`, dispatches each `job_type` to its handler
   (`ttl_cleanup`, `snapshot_save`, `flush_dirty_keys`), updates `last_run`
   / `next_run` / `last_error`, and bumps `pgredis.job_stats`.
4. `SIGTERM` → clean shutdown. `SIGHUP` → reread GUCs.

Because the worker is its own backend, its in-memory keyspace is empty —
`flush_dirty_keys` is a no-op in v0.1 (it logs `DEBUG1`). The sweep that
matters today is `ttl_cleanup`, which runs `DELETE FROM pgredis.store WHERE
expire_at <= now()` and so cleans up durable rows across the whole cluster.
`snapshot_save` calls `pg_redis_persistence_save_snapshot()` to insert a fresh
row into `pgredis.snapshots`.

`RUN_JOB(<id>)` runs the same handler **inline** in a regular backend — useful
for tests and for one-off triggers without enabling the worker.

### Lifecycle of one `SET` (sync_table mode, default)

1. `pgredis."SET"(k, v)` enters `pg_redis_set` in
   [src/pg_redis.c](src/pg_redis.c).
2. Args are validated (`pg_redis_check_key_len`, `pg_redis_check_value_len`).
   The key is materialized into a 256-byte stack buffer when short enough to
   skip per-call `palloc`/`pfree` on the hot path.
3. `pg_redis_persistence_load_if_needed()` — first call in this backend reads
   `pgredis.store` + `pgredis.hash_fields` + `pgredis.list_items` and
   populates the HTAB.
4. `pg_redis_store_upsert(k)` — find or create the entry in the HTAB.
5. The old in-memory value (if any) is released; the new payload is
   palloc'd into `PgRedisMemoryContext` and attached to the entry; `version++`.
6. `pg_redis_mark_dirty(e)` — idempotent push into the per-backend dirty-set.
   No SPI yet.
7. The function returns `true` to SQL immediately. At
   `XACT_EVENT_PRE_COMMIT` the dirty-set drains via cached `SPI_keepplan`
   array-form plans; the durable row(s) land in the **calling** transaction.
   A `ROLLBACK` skips the flush and discards the dirty-set, and the
   `XactCallback` marks the backend's lazy-load flag dirty for re-sync on
   next access.

## Build

Requirements: **PostgreSQL 18+ (hard requirement)**, a C toolchain, `pg_config`
on your `PATH`, and the PostgreSQL server development headers (e.g.
`postgresql-server-dev-18` on Debian/Ubuntu, `postgresql@18` on Homebrew). The
build will refuse to compile against any earlier server version — pg_redis
uses PG18-only features (`uuidv7()`, virtual generated columns) in its SQL.

```bash
make
sudo make install
```

On macOS with Homebrew:

```bash
PATH="/opt/homebrew/opt/postgresql@18/bin:$PATH" make
sudo PATH="/opt/homebrew/opt/postgresql@18/bin:$PATH" make install
```

### PostgreSQL 18 features in use

- **`uuidv7()`** for `pgredis.snapshots.snapshot_id` — time-ordered identifiers
  without a sequence and without round-trip latency.
- **Virtual generated columns** — `pgredis.store.key_bytes` is computed at
  read time as `octet_length(key)`; it occupies no storage and never goes
  stale.
- **Compile-time gate** — `src/pg_redis.c` raises `#error` against any PG
  prior to 18.

## Docker

A multi-stage `Dockerfile` is provided that builds the extension against
`postgres:<PG_VERSION>-bookworm`, runs `make installcheck` inside the image,
and produces a runtime image with the extension installed.

```bash
# Build the runtime image (postgres + pg_redis preinstalled)
make docker-build                # default PG_VERSION=18
PG_VERSION=18 make docker-build  # explicit (PG18+ only)

# Run the regression suite inside Docker (builds the `test` stage)
make docker-test

# Spin up postgres with the extension preloaded
make docker-up
psql "postgres://postgres:postgres@localhost:5432/postgres" -c \
    "CREATE EXTENSION pg_redis; SELECT pgredis.\"INFO\"();"
make docker-down
```

`make docker-test` succeeds only if every regression file in `test/sql/`
diffs cleanly against `test/expected/`. Failures dump
`test/regression.diffs` and the postgres log to the build output.

The suite covers the full SQL surface:

| File | Covers |
| --- | --- |
| `basic.sql` | SET/GET/DEL/EXISTS, INCR/DECR (incl. non-numeric error), MEMORY_USAGE, FLUSHALL, KEYS |
| `ttl.sql` | EXPIRE/TTL semantics (`-1`/`-2`), lazy expiry, `EXPIRE 0` / negative as delete |
| `hashes.sql` | HSET/HGET/HDEL/HEXISTS + WRONGTYPE both directions |
| `lists.sql` | LPUSH/RPUSH/LPOP/RPOP/LLEN, empty-pop NULL, LIFO order, WRONGTYPE |
| `persistence.sql` | sync_table write-through, SAVE + snapshots, ROLLBACK semantics |
| `admin.sql` | STATS, INFO, BGSAVE, BGREWRITEAOF, PG18 virtual column on `pgredis.store`, UUIDv7 snapshot ids |
| `jobs.sql` | ADD_TTL_CLEANUP_POLICY / ADD_FLUSH_POLICY / ADD_SNAPSHOT_POLICY, JOBS, RUN_JOB (incl. missing), JOB_STATS, DELETE_JOB |

When you add or edit a test, regenerate the committed expected output with:

```bash
make docker-regen
```

This runs `installcheck` inside the builder image with `test/expected/`
bind-mounted, and copies the fresh `test/results/*.out` over the host's
expected files. Review the diff in `git status` before committing.

## Benchmark vs real Redis

A Python harness in `bench/` runs identical workloads against real Redis and
pg_redis, then prints a side-by-side report to stdout. Workload covers
strings (`SET`/`GET`/`EXISTS`/`DEL`), counters (`INCR`/`DECR`), hashes
(`HSET`/`HGET`/`HDEL`), and lists (`LPUSH`/`RPUSH`/`LPOP`/`RPOP`/`LLEN`).

```bash
# Default: 1000 iterations per op
make docker-bench

# Larger sample (slower)
BENCH_N=5000 make docker-bench
```

The target spins up `redis:7-alpine`, the `pg_redis:18` runtime image, and a
Python container with `redis-py` + `psycopg`. After the run it tears
everything down. Sample output (single-client, single-connection, BENCH_N=500
on Docker Desktop / Apple Silicon):

```text
Op       System        ops/sec        p50        p95        p99       winner
SET      redis         21.1k/s     44.2us     75.1us     95.3us    5.4x redis
         pg_redis       3.9k/s    247.5us    309.7us    354.6us
GET      redis         18.1k/s     52.2us     69.9us     89.2us       2.1x pg
         pg_redis      37.3k/s     25.3us     49.3us     50.8us
...
Aggregate throughput: redis = 18.7k/s, pg_redis = 4.3k/s  (redis 4.3x faster overall)
```

Reading the numbers: pg_redis loses on writes (each SET goes through the SQL
parser, planner, and a write-through INSERT into `pgredis.store`) but wins
on pure-read ops because the hashmap lookup is in-process while redis-py
has to round-trip TCP. Useful as a sanity check, not as a marketing claim.

## Install in a database

```sql
CREATE EXTENSION pg_redis;
```

The extension provisions schema `pgredis` plus eight internal tables
(`store`, `hash_fields`, `list_items`, `meta`, `jobs`, `job_stats`,
`snapshots`, `aof`). All tables are marked as extension config tables, so
`pg_dump` includes your user data.

To remove:

```sql
DROP EXTENSION pg_redis CASCADE;
```

### Migration v0.1 → v0.2 (pg_redis 1.0 → 1.1) — BREAKING

v1.1 changes the on-disk format:

- `pgredis.store.value` is now `bytea` (was `jsonb`), carrying a binary TLV
  `[u8 tag][u32 length_le][payload]` for string/int payloads only.
- Hash payloads live in `pgredis.hash_fields(key, field, value bytea)`.
- List payloads live in `pgredis.list_items(key, ord bigint, value bytea)`.

There is no in-place data migration: the v1.0 `jsonb` rows cannot be
translated to the new TLV format without C-level encoding context. The
upgrade script refuses to run against a populated keyspace.

**Required pre-upgrade workflow:**

```sql
-- Option A: disposable data
SELECT pgredis."FLUSHALL"();

-- Option B: snapshot first, then flush (snapshots survive the upgrade)
SELECT pgredis."SAVE"();
SELECT pgredis."FLUSHALL"();

ALTER EXTENSION pg_redis UPDATE TO '1.1';
```

If the keyspace is non-empty, the upgrade `RAISE`s with instructions to run
`FLUSHALL` first.

**Rollback:** downgrade requires
`DROP EXTENSION pg_redis; CREATE EXTENSION pg_redis VERSION '1.0';`.
Data does not round-trip back to the v1.0 `jsonb` schema.

### Resolved design questions (from the v1.0 → v1.1 change proposal)

- **Naming (OQ2)**: `pg_redis_persistence_save_entry` and
  `pg_redis_persistence_delete_key` are preserved as thin wrappers around
  `pg_redis_mark_dirty` / `pg_redis_mark_deleted`. Internal callers in
  `src/pg_redis.c` use the new names directly; the old symbols stay as a
  compatibility shim for any out-of-tree consumer.
- **List ordinals (OQ4)**: `min_ord` / `max_ord` are recomputed per list on
  load (`min(ord)`, `max(ord)` over `pgredis.list_items`) — no separate
  metadata row, no schema bloat.
- **Batch sizing (OQ3)**: the pre-commit flush always issues at most one
  `SPI_execute_plan` per category. `pg_redis.flush_batch_size` continues to
  govern only the bgworker dirty-keys job.

## Why all the quoted UPPERCASE?

PostgreSQL folds unquoted identifiers to lower case. Redis commands are
canonically UPPERCASE, so to make `pgredis."SET"` and `pgredis."GET"` map
1-to-1 with the documentation you'd expect, the extension declares them as
**quoted** identifiers. That means you must quote them at the call site too:

```sql
-- Works
SELECT pgredis."SET"('user:1:name', 'Alice');

-- Fails with: function pgredis.set(unknown, unknown) does not exist
SELECT pgredis.set('user:1:name', 'Alice');
```

There is no `pgredis.set(text, text)` function; only `pgredis."SET"(text, text)`.

## SQL examples

```sql
CREATE EXTENSION pg_redis;

-- Strings
SELECT pgredis."SET"('user:1:name', 'Alice');
SELECT pgredis."GET"('user:1:name');
SELECT pgredis."DEL"('user:1:name');

-- Counters
SELECT pgredis."INCR"('hits');
SELECT pgredis."INCR"('hits');
SELECT pgredis."GET"('hits');                 -- '2'

-- TTL (-2 = no key, -1 = no TTL, else seconds)
SELECT pgredis."SET"('session:abc', 'token');
SELECT pgredis."EXPIRE"('session:abc', 60);
SELECT pgredis."TTL"('session:abc');          -- 59..60

-- Hashes
SELECT pgredis."HSET"('user:1', 'name', 'Alice');
SELECT pgredis."HSET"('user:1', 'age', '30');
SELECT pgredis."HGET"('user:1', 'name');
SELECT pgredis."HEXISTS"('user:1', 'age');

-- Lists (LPUSH at head, RPUSH at tail)
SELECT pgredis."RPUSH"('queue', 'job1');
SELECT pgredis."RPUSH"('queue', 'job2');
SELECT pgredis."LPOP"('queue');               -- 'job1'

-- Admin
SELECT * FROM pgredis."KEYS"();
SELECT pgredis."MEMORY_USAGE"();
SELECT pgredis."INFO"();
SELECT * FROM pgredis."STATS"();

-- Snapshot to the snapshots table
SELECT pgredis."SAVE"();

-- Background-job scheduling
SELECT pgredis."ADD_TTL_CLEANUP_POLICY"('30 seconds');
SELECT pgredis."ADD_SNAPSHOT_POLICY"('1 hour');
SELECT * FROM pgredis."JOBS"();
SELECT pgredis."RUN_JOB"(1);                  -- runs the job inline
SELECT * FROM pgredis."JOB_STATS"();
```

## Configuration (GUCs)

| GUC | Type | Default | Effect |
| --- | --- | --- | --- |
| `pg_redis.persistence_mode` | string | `sync_table` | `none`, `sync_table`, `async_table`, `snapshot`, or `aof`. `async_table` and `aof` fall back to `sync_table` in v0.1. |
| `pg_redis.storage_mode` | string | `session` | `session` (per-backend in-memory) or `shared`. v0.1 implements `session` only. |
| `pg_redis.flush_interval` | int seconds | `5` | Background worker tick interval. |
| `pg_redis.flush_batch_size` | int | `1000` | Max dirty entries flushed per tick (reserved). |
| `pg_redis.ttl_cleanup_interval` | int seconds | `30` | Default TTL sweep cadence for the `ttl_cleanup` job. |
| `pg_redis.max_key_size` | int bytes | `1024` | Reject keys longer than this. |
| `pg_redis.max_value_size` | int bytes | `1048576` | Reject values longer than this. |
| `pg_redis.enable_background_worker` | bool | `off` | Enable the worker (requires `shared_preload_libraries`). |

Invalid persistence/storage values are rejected at `SET` time via a GUC
`check_hook`.

## Storage modes

### `session` (v0.1 default and only mode)

Each backend has its own `MemoryContext` and `dynahash` HTAB. Writes go to
your in-memory copy and — in `sync_table` mode — to `pgredis.store` in the
calling transaction. Other backends see your writes by reading the durable
row.

### `shared` (reserved)

A future release will host the keyspace in PostgreSQL shared memory behind an
LWLock so every backend sees the same state. Until then, treat `shared` as
a no-op; the GUC is accepted but the runtime stays in `session` mode and
emits a `WARNING` at startup if you set it.

## Persistence modes

| Mode | What happens on write | Durable on crash? | Visible to other backends? |
| --- | --- | --- | --- |
| `none` | memory only; dirty-set tracking still runs but pre-commit flush is a no-op | no | no (session-local) |
| `sync_table` (default) | memory + queued in per-backend dirty-set; flushed in one batched SPI session at `XACT_EVENT_PRE_COMMIT` | yes (rolls back with the user xact) | yes (after their first command lazy-loads) |
| `async_table` | falls back to `sync_table` in v0.1 | yes | yes |
| `snapshot` | use `SAVE`/`BGSAVE` to emit point-in-time rows in `pgredis.snapshots` | by snapshot | snapshots are global |
| `aof` | scaffolded only; `BGREWRITEAOF` returns false | no | n/a |

**Crash window in async mode (future):** in true `async_table`, mutating
commands are acked before the dirty queue is flushed. A postmaster crash or
hardware fault between the ack and the next flush will lose the most recent
dirty writes. Operators picking `async_table` for throughput must accept that
window. v0.1 sidesteps this by always doing the synchronous write.

## Background workers

When you set:

```conf
# postgresql.conf
shared_preload_libraries = 'pg_redis'
pg_redis.enable_background_worker = on
```

…and restart PostgreSQL, the extension registers a single background worker
("pg_redis bgworker") that connects to the `postgres` database, ticks every
`pg_redis.flush_interval` seconds, and runs any job in `pgredis.jobs` whose
`enabled` is true and `next_run <= now()`. The worker also handles `SIGTERM`
(clean shutdown) and `SIGHUP` (config reload).

You can always run jobs inline from a regular backend via
`SELECT pgredis."RUN_JOB"(<id>)` — useful for testing without changing
`shared_preload_libraries`.

**Important**: because the keyspace is session-local in v0.1, the
`flush_dirty_keys` job inside the worker can only see the worker's own
backend (which is empty). It logs a `DEBUG1` line saying so. Once shared
storage lands, the same job will flush across the cluster.

## Transaction limitations

- The durable row in `pgredis.store` participates in the calling transaction.
  Roll back, and the row goes with it.
- The in-memory copy is **not** rolled back. When `XACT_EVENT_ABORT` fires, the
  extension marks "needs reload" and the next access in that backend
  lazy-loads the (rolled-back) durable view from the table.
- Implication: between an aborted `SET` and the next `GET` in the same
  backend, the in-memory value remains the new one. Read after rollback to
  re-sync.

## Concurrency model

PostgreSQL is process-per-connection. v0.1 stores the keyspace in the
**calling backend's private memory**, so it is not shared. Cross-session
correctness goes through `pgredis.store`:

1. Backend A does `SET k v` → memory + table row written in A's transaction.
2. Backend B does `GET k` → first access in B lazy-loads from the table → B
   gets `v`.

If you set `pg_redis.persistence_mode = 'none'`, step 2 will return `NULL` —
because there is no durable row to lazy-load.

`shared_preload_libraries = 'pg_redis'` is required for the background worker
and (eventually) the shared keyspace. It is **not** required for the extension
itself or for any of the SQL commands.

## Roadmap

- `shared` storage mode with a DSA-backed keyspace and `dshash`.
- Real `async_table` with a shared dirty queue and batched flush.
- Append-only file replay (`BGREWRITEAOF`, AOF replay on first use).
- Transaction-aware in-memory rollback (cooperative subtransaction tracking).
- `SUBSCRIBE`/`PUBLISH`, sorted sets, streams — long term.
- Per-database / per-namespace separation.

## Test layout

Regression tests live under `test/sql/` (input scripts) and `test/expected/`
(expected output). The `Makefile` sets `REGRESS_OPTS = --inputdir=test
--outputdir=test`. Run them with:

```bash
make installcheck
```

Expected output is not committed in v0.1; regenerate with:

```bash
# After the first successful run, copy results/ to expected/:
cp test/results/*.out test/expected/
```

…and re-run `make installcheck` to confirm a clean diff.

## License

See `LICENSE` (Apache-2.0 by default).
