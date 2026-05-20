#!/usr/bin/env bash
# Run the async-mode regression tests against an ephemeral PostgreSQL with
# shared_preload_libraries=pg_redis and a small dirty-ring, so producer-side
# sync_flush fallback paths actually execute. This is the harness for
# test/async/*.sql files — they CANNOT run under the normal pg_regress because
# pg_regress doesn't load shared_preload_libraries.
set -euo pipefail

PGDATA="${PGDATA:-/tmp/pgdata-async}"
LOGFILE="${LOGFILE:-/tmp/pg-async.log}"

# Cluster config that matches what test/async/*.sql files assume.
# 1024 is the minimum allowed by pg_redis.dirty_ring_size (a hardcoded GUC
# floor in src/pg_redis.c); we use it for testability without weakening the
# production min.
DIRTY_RING_SIZE="${DIRTY_RING_SIZE:-1024}"

initdb -D "$PGDATA" -U postgres -E UTF8 --auth-local=trust --auth-host=trust >/dev/null

pg_ctl -D "$PGDATA" -l "$LOGFILE" -w start \
    -o "-c unix_socket_directories=/tmp \
        -c listen_addresses='' \
        -c shared_preload_libraries=pg_redis \
        -c pg_redis.storage_mode=shared \
        -c pg_redis.dirty_ring_size=${DIRTY_RING_SIZE} \
        -c pg_redis.async_full_action=sync_flush \
        -c pg_redis.enable_background_worker=off"

cleanup() {
    pg_ctl -D "$PGDATA" stop -m fast >/dev/null 2>&1 || true
}
trap cleanup EXIT

cd /build

rc=0
shopt -s nullglob
tests=(test/async/*.sql)
if [ ${#tests[@]} -eq 0 ]; then
    echo "no test/async/*.sql files found"
    exit 1
fi

for t in "${tests[@]}"; do
    echo
    echo "=== running $t ==="
    if ! psql -U postgres -h /tmp -d postgres \
              --no-psqlrc \
              --set ON_ERROR_STOP=on \
              -f "$t"; then
        rc=1
        echo "FAIL $t"
    fi
done

if [ "$rc" -ne 0 ]; then
    echo
    echo "=== postgres log ==="
    cat "$LOGFILE" || true
    exit "$rc"
fi

echo
echo "all async tests passed"
