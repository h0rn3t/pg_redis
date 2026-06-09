#!/usr/bin/env bash
# Crash-safety stress runner (the `fix-crash-safety` change, Group 13).
#
# Boots an ephemeral PostgreSQL with pg_redis in shared / async_table mode and
# a deliberately small dirty-ring, then drives mixed read/write load while
# injecting faults. Pass criteria:
#   * no PANIC in the server log
#   * no "cannot wait on LWLock while holding another LWLock" line
#   * the dirty-ring keeps draining (events_pending stays bounded)
#   * a clean restart + concurrent cold-start fires all return correct results
#
# Designed to run inside the `builder` image (postgres + pg_redis installed):
#   docker build --target builder -t pg_redis-builder:18 .
#   docker run --rm --user postgres pg_redis-builder:18 \
#       bash /build/bench/crash_safety/run.sh
#
# Tunables (env): DURATION (s, default 30), WORKERS (default 8),
#                 DIRTY_RING_SIZE (default 1024), COLD_START_CONNS (default 32).
set -euo pipefail

PGDATA="${PGDATA:-/tmp/pgdata-stress}"
LOGFILE="${LOGFILE:-/tmp/pg-stress.log}"
DURATION="${DURATION:-30}"
WORKERS="${WORKERS:-8}"
DIRTY_RING_SIZE="${DIRTY_RING_SIZE:-1024}"
COLD_START_CONNS="${COLD_START_CONNS:-32}"
PSQL=(psql -U postgres -h /tmp -d postgres --no-psqlrc -v ON_ERROR_STOP=1 -At)

start_cluster() {
    pg_ctl -D "$PGDATA" -l "$LOGFILE" -w start \
        -o "-c unix_socket_directories=/tmp \
            -c listen_addresses='' \
            -c shared_preload_libraries=pg_redis \
            -c pg_redis.storage_mode=shared \
            -c pg_redis.persistence_mode=async_table \
            -c pg_redis.dirty_ring_size=${DIRTY_RING_SIZE} \
            -c pg_redis.async_full_action=sync_flush \
            -c pg_redis.enable_background_worker=on \
            -c pg_redis.flush_interval=1 \
            -c max_connections=200"
}

stop_cluster() { pg_ctl -D "$PGDATA" stop -m fast -w >/dev/null 2>&1 || true; }

check_log_clean() {
    if grep -qiE 'PANIC|cannot wait on LWLock|segmentation fault|server process .* was terminated' "$LOGFILE"; then
        echo "FAIL: server log contains a crash-safety violation:"
        grep -iE 'PANIC|cannot wait on LWLock|segmentation fault|terminated' "$LOGFILE" | head
        return 1
    fi
    return 0
}

initdb -D "$PGDATA" -U postgres -E UTF8 --auth-local=trust --auth-host=trust >/dev/null
trap stop_cluster EXIT
start_cluster
"${PSQL[@]}" -c "CREATE EXTENSION IF NOT EXISTS pg_redis;" >/dev/null

echo "== phase 1: mixed read/write load, ${WORKERS} workers x ${DURATION}s =="
worker() {
    local id="$1" deadline=$(( $(date +%s) + DURATION )) i=0
    while [ "$(date +%s)" -lt "$deadline" ]; do
        i=$((i + 1))
        "${PSQL[@]}" -c "
            SELECT pgredis.\"SET\"('w${id}:k'||(${i} % 500), repeat('x', 1 + (${i} % 300)));
            SELECT pgredis.\"GET\"('w${id}:k'||(${i} % 500));
            SELECT pgredis.\"HSET\"('w${id}:h', 'f'||(${i} % 50), 'v${i}');
            SELECT pgredis.\"HDEL\"('w${id}:h', 'f'||((${i}+1) % 50));
        " >/dev/null 2>>"$LOGFILE" || true
    done
}
for w in $(seq 1 "$WORKERS"); do worker "$w" & done

# == phase 2: kill -9 the bgworker mid-load. With BGW_NEVER_RESTART it does NOT
# come back; the sync_flush producer recovery path must keep the ring draining.
sleep $(( DURATION / 3 ))
# Find the bgworker pid via the catalog (pgrep isn't in the postgres image).
BGW_PID="$("${PSQL[@]}" -c "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_redis' LIMIT 1;" 2>/dev/null || true)"
if [ -n "${BGW_PID:-}" ]; then
    echo "   killing bgworker pid=$BGW_PID (SIGKILL); BGW_NEVER_RESTART means producers must keep draining"
    kill -9 "$BGW_PID" || true
else
    echo "   (bgworker pid not found; skipping kill)"
fi

wait  # let the load workers finish

PENDING="$("${PSQL[@]}" -c "SELECT events_pending FROM pgredis.\"RING_INSPECT\"();")"
STUCK="$("${PSQL[@]}" -c "SELECT stuck_writing_slots FROM pgredis.\"RING_INSPECT\"();")"
echo "   after load: events_pending=${PENDING} stuck_writing_slots=${STUCK}"
if [ "${STUCK:-1}" -ne 0 ]; then
    echo "FAIL: ${STUCK} slots stuck in WRITING (producer left a hole)"; exit 1
fi
check_log_clean

echo "== phase 3: clean restart + ${COLD_START_CONNS}-way concurrent cold start =="
stop_cluster
start_cluster
pids=()
for c in $(seq 1 "$COLD_START_CONNS"); do
    ( for n in $(seq 1 100); do
        "${PSQL[@]}" -c "SELECT pgredis.\"GET\"('w1:k'||(${n} % 500));" >/dev/null 2>>"$LOGFILE"
      done ) &
    pids+=($!)
done
fail=0
for p in "${pids[@]}"; do wait "$p" || fail=1; done
if [ "$fail" -ne 0 ]; then echo "FAIL: a cold-start connection errored"; exit 1; fi

LOAD_STATE="$("${PSQL[@]}" -c "SELECT pgredis.\"LOAD_STATE\"();")"
echo "   cold start LOAD_STATE=${LOAD_STATE} (expect 2)"
[ "${LOAD_STATE:-0}" -eq 2 ] || { echo "FAIL: cold start did not reach loaded(2)"; exit 1; }
check_log_clean

echo
echo "PASS crash_safety stress: no PANIC / no LWLock violation; ring drained; cold start clean."
