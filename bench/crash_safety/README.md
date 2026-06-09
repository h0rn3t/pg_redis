# Crash-safety stress harness

`run.sh` drives mixed read/write load against pg_redis in shared / async_table
mode with a small dirty-ring, while injecting faults, and asserts the
crash-safety invariants from the `fix-crash-safety` change.

Phases:

1. **Mixed load** — `WORKERS` concurrent psql workers run SET/GET/HSET/HDEL
   loops for `DURATION` seconds against a `DIRTY_RING_SIZE`-slot ring, so the
   producer `sync_flush` fallback and the BGW drain race continuously.
2. **kill -9 the bgworker** mid-load. Because the worker is registered
   `BGW_NEVER_RESTART`, it does not come back; the producer `sync_flush`
   recovery path must keep the ring draining (`stuck_writing_slots == 0`).
3. **Clean restart + `COLD_START_CONNS`-way concurrent cold start** — verifies
   the lock-free cold-start coordination (`LOAD_STATE` reaches `2`) returns
   correct results with no deadlock.

Pass criteria: no `PANIC`, no `cannot wait on LWLock while holding another
LWLock`, no segfault in the server log; the ring stays drained; every
cold-start connection succeeds.

## Run

```bash
docker build --target builder -t pg_redis-builder:18 .
docker run --rm --user postgres \
    -e DURATION=30 -e WORKERS=8 -e COLD_START_CONNS=32 \
    pg_redis-builder:18 bash /build/bench/crash_safety/run.sh
```

Tunables (env): `DURATION` (s, default 30), `WORKERS` (8), `DIRTY_RING_SIZE`
(1024), `COLD_START_CONNS` (32).
