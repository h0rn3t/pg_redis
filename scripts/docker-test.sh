#!/usr/bin/env bash
# Run pg_regress against an ephemeral PostgreSQL.
#
# Modes:
#   default                       — run installcheck, exit non-zero on diffs.
#   PGREDIS_REGEN_DIR=/path/to/d  — run installcheck, copy results/*.out to
#                                   that directory, exit 0 regardless of diffs.
#                                   Use this to (re)generate expected outputs.
set -euo pipefail

PGDATA="${PGDATA:-/tmp/pgdata}"
LOGFILE="${LOGFILE:-/tmp/pg.log}"
REGEN_DIR="${PGREDIS_REGEN_DIR:-}"

initdb -D "$PGDATA" -U postgres -E UTF8 --auth-local=trust --auth-host=trust >/dev/null

pg_ctl -D "$PGDATA" -l "$LOGFILE" -w start \
    -o "-c unix_socket_directories=/tmp -c listen_addresses=''"

cleanup() {
    pg_ctl -D "$PGDATA" stop -m fast >/dev/null 2>&1 || true
}
trap cleanup EXIT

cd /build

set +e
make installcheck PGUSER=postgres PGHOST=/tmp
rc=$?
set -e

if [ -n "$REGEN_DIR" ]; then
    echo
    echo "Regenerating expected outputs in $REGEN_DIR"
    mkdir -p "$REGEN_DIR"
    cp -v test/results/*.out "$REGEN_DIR/"
    exit 0
fi

if [ "$rc" -ne 0 ]; then
    echo
    echo "=== test/regression.diffs ==="
    [ -f test/regression.diffs ] && cat test/regression.diffs || echo "(missing)"
    echo
    echo "=== test/regression.out ==="
    [ -f test/regression.out ] && cat test/regression.out || echo "(missing)"
    echo
    echo "=== postgres log ==="
    cat "$LOGFILE" || true
    exit "$rc"
fi

echo "installcheck passed"
