# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Communication

**Respond in Ukrainian** (відповідай українською). All user-facing prose —
explanations, summaries, questions, status updates — must be written in
Ukrainian. Keep code, identifiers, file paths, commands, and quoted output
unchanged (don't translate them).

## What this is

`pg_redis` is a PostgreSQL **extension written in C** that embeds a Redis-shaped
in-memory key-value store (strings, ints, hashes, lists, TTL, snapshots, a job
scheduler) directly inside each PostgreSQL backend. There is no RESP protocol or
socket — every command is a C function exported from the shared library and run
in-process. Durability rides on PostgreSQL tables and WAL.

- The **binary/extension** is named `pg_redis`; the **SQL-facing schema** is
  `pgredis` (Postgres reserves the `pg_` prefix for system catalogs).
- SQL functions are **quoted UPPERCASE** identifiers: call
  `pgredis."SET"('k','v')`, never `pgredis.set(...)`. There is no lowercase alias.

## Hard requirements (do not regress)

- **PostgreSQL 18+ only.** [src/pg_redis.c](src/pg_redis.c) has a compile-time
  `#error` gate (`PG_VERSION_NUM < 180000`). The SQL uses PG18-only features:
  `uuidv7()` (snapshot ids) and **virtual generated columns**
  (`pgredis.store.key_bytes`). Don't introduce features that break the PG18 floor
  without updating the gate and README.
- `pg_config` for PG18 must be on `PATH` to build locally. On macOS/Homebrew that
  means prefixing with `PATH="/opt/homebrew/opt/postgresql@18/bin:$PATH"`.

## Build, test, bench

```bash
make                 # build pg_redis.so via PGXS (needs PG18 pg_config on PATH)
sudo make install    # install .so + .control + versioned SQL into the cluster

# Regression tests — require a RUNNING PG18 instance with the extension installed.
make installcheck                   # full suite (REGRESS list in Makefile)
make installcheck REGRESS=basic     # single test file (test/sql/basic.sql)
```

Most work happens through Docker (no local PG18 needed):

```bash
make docker-test         # build + run full pg_regress suite inside the image (CI gate)
make docker-test-async   # shared-memory / async_table scenarios pg_regress can't cover
make docker-regen        # regenerate test/expected/*.out after editing test SQL
make docker-build        # runtime image: postgres + pg_redis preinstalled
make docker-up / down    # compose a live cluster with the extension preloaded
make docker-bench        # side-by-side latency vs real Redis (BENCH_N=1000 default)
```

`PG_VERSION` overrides the postgres base image (default `18`; PG18+ only).

## Test discipline

- `pg_regress` tests live in [test/sql/](test/sql/) (input) and
  [test/expected/](test/expected/) (committed golden output); `test/results/` is
  scratch. The active `REGRESS` list is in the [Makefile](Makefile) — adding a
  test file means adding it there.
- **Never hand-edit `test/expected/*.out`.** After changing test SQL or any
  output-affecting behavior, run `make docker-regen` and review the diff in
  `git status` before committing. A regression file passes only if it diffs
  cleanly; failures dump `test/regression.diffs`.
- `test/async/*.sql` is a separate suite run only via `make docker-test-async`
  (it needs `shared_preload_libraries=pg_redis` + `storage_mode=shared`).

## SQL versioning discipline

Installed SQL is version-scripted; `default_version = '1.1'` in
[pg_redis.control](pg_redis.control). Three files ship (listed in `DATA` in the
Makefile): `pg_redis--1.0.sql`, `pg_redis--1.1.sql` (full installs), and
`pg_redis--1.0--1.1.sql` (the migration). When you change the SQL surface or
on-disk schema you must add a new full-version script **and** a `--old--new`
migration, bump `default_version`, and add the file to `DATA`. The on-disk
format change (jsonb → binary TLV in v1.1) is deliberately non-reversible — see
the migration notes in [README.md](README.md).

## Architecture

PostgreSQL is process-per-connection, which drives the whole design.

**Two storage modes, two parallel implementations:**

- `storage_mode = 'session'` (default): each backend keeps its own keyspace in a
  private `MemoryContext` (`PgRedisMemoryContext`) holding a dynahash `HTAB`. All
  value allocations palloc into that context, so `FLUSHALL` is one
  `MemoryContextReset`. Cross-backend visibility goes **through the durable
  table** (lazy-load on first access). Code: [src/kv_store.c](src/kv_store.c),
  [src/list.c](src/list.c), [src/hash_value.c](src/hash_value.c).
- `storage_mode = 'shared'`: the keyspace lives in PostgreSQL shared memory
  (`ShmemInitHash` with `HASH_PARTITION`, guarded by partitioned LWLocks);
  variable-size payloads live in one DSA segment. Visible to every backend
  immediately. Requires `shared_preload_libraries='pg_redis'`. Code:
  [src/shared_store.c](src/shared_store.c) (scratch-copy CRUD contract),
  [src/shared_hash.c](src/shared_hash.c) (DSA open-addressing hash table),
  [src/shared_list.c](src/shared_list.c) (DSA doubly-linked list),
  [src/shmem.c](src/shmem.c) (segment + lock setup).

**Persistence** ([src/persistence.c](src/persistence.c), the largest module):
mutators mark entries into a per-backend **dirty-set** (and queue tombstones)
rather than writing per-command. At `XACT_EVENT_PRE_COMMIT` the dirty-set drains
in a single SPI session via cached `SPI_keepplan` array-form plans (at most one
`SPI_execute_plan` per category: store rows, hash fields, list items). Durable
rows land in the **calling transaction** — a `ROLLBACK` drops them; an
`XactCallback` then flips a "needs reload" flag so the next access lazy-reloads
the durable view (the in-memory copy is *not* rolled back).

**`async_table`** (functional only with `storage_mode=shared`): mutators publish
a `PgRedisDirtyEvent` into a shared multi-producer/single-consumer dirty-ring
([src/dirty_ring.c](src/dirty_ring.c)) and return before durability; the
background worker drains the ring into durable tables on its own schedule. Crash
window is bounded by `flush_interval`. Misconfiguring `async_table` +
`session` warns and downgrades to `sync_table` (`pg_redis_effective_persistence_mode`).

**Background worker** ([src/bgworker.c](src/bgworker.c), [src/jobs.c](src/jobs.c)):
registered in `_PG_init` only when `shared_preload_libraries='pg_redis'` and
`enable_background_worker=on`. Ticks every `flush_interval`s, runs due jobs from
`pgredis.jobs` (`ttl_cleanup`, `snapshot_save`, `flush_dirty_keys`) and drains
the async dirty-ring. Any job runs inline in a normal backend via
`pgredis."RUN_JOB"(id)` — use that in tests instead of enabling the worker.

**Binary value format** ([src/binval.c](src/binval.c)): string/int payloads are
encoded as TLV `[u8 tag][u32 length_le][payload]` into `pgredis.store.value`
(bytea). Hash fields and list items live in their own per-element tables, never
in this column.

### Module map

| File | Responsibility |
| --- | --- |
| [src/pg_redis.c](src/pg_redis.c) | `_PG_init`, GUC registration, all `PG_FUNCTION_INFO_V1` SQL entry points, command dispatch |
| [src/types.h](src/types.h) | All structs (`PgRedisEntry`, `PgRedisHash`/`Field`, `PgRedisList`/`Node`, shared-mem variants), TLV tags, GUC + enum externs |
| [src/kv_store.c](src/kv_store.c) | Session-mode top-level HTAB: init/reset/lookup/upsert, memory accounting |
| [src/ttl.c](src/ttl.c) | Lazy expiry, `EXPIRE`/`TTL` semantics (-1/-2) |
| [src/list.c](src/list.c), [src/hash_value.c](src/hash_value.c) | Session-mode list / hash value containers |
| [src/persistence.c](src/persistence.c) | Dirty-set, batched pre-commit flush, lazy-load, XactCallback, snapshots, async drain |
| [src/binval.c](src/binval.c) | TLV encode/decode for `store.value` |
| [src/shmem.c](src/shmem.c), [src/dirty_ring.c](src/dirty_ring.c) | Shared-memory segment, DSA, locks, async dirty-ring |
| [src/shared_store.c](src/shared_store.c), [src/shared_hash.c](src/shared_hash.c), [src/shared_list.c](src/shared_list.c) | Shared-mode keyspace + DSA-backed containers |
| [src/bgworker.c](src/bgworker.c), [src/jobs.c](src/jobs.c) | Background worker lifecycle + job scheduler/handlers |
| [src/utils.c](src/utils.c) | Arg validation (`pg_redis_check_key_len` / `_value_len`) and helpers |

## Code conventions

- Follows **PostgreSQL source style**: hard tabs, `lower_snake_case` functions
  prefixed `pg_redis_`, struct types in `PgRedisCamelCase`, brace-on-own-line.
  Match the surrounding file.
- Shared-memory work must respect the locking contract documented in the
  shared_* headers/comments (callers acquire partition LWLocks; helpers must not
  self-acquire — LWLocks are not reentrant). DSA allocations in multi-step
  builders must roll back on OOM via `PG_TRY`/`PG_CATCH` (see CHANGELOG entries
  for the failure modes already fixed).
- GUC values are validated at `SET` time via `check_hook`s; invalid
  persistence/storage strings are rejected there.

## Workflow (OpenSpec)

This project uses **OpenSpec** (spec-driven). Active and archived change
proposals live under [openspec/changes/](openspec/changes/); the current
specs are in [openspec/specs/](openspec/specs/). The `.claude/settings.local.json`
pre-allows the `opsx:*` / `openspec-*` and `superpowers:systematic-debugging`
skills. For a non-trivial feature or schema change, check for / create an
OpenSpec change rather than editing specs ad hoc.
