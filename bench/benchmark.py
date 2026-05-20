"""Compare pg_redis vs real Redis across the SET/GET/INCR/HASH/LIST surface.

Runs the same workload against both systems, measures per-operation latency,
and prints a side-by-side ASCII report to stdout.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import Callable, Iterable

import psycopg
import redis


@dataclass
class OpResult:
    name: str
    system: str
    ops: int
    total_s: float
    latencies_us: list[float]

    @property
    def ops_per_sec(self) -> float:
        return self.ops / self.total_s if self.total_s > 0 else float("inf")

    def percentile(self, p: float) -> float:
        if not self.latencies_us:
            return 0.0
        data = sorted(self.latencies_us)
        k = max(0, min(len(data) - 1, int(round(p * (len(data) - 1)))))
        return data[k]


def run_op(name: str, system: str, n: int, op: Callable[[int], None]) -> OpResult:
    # warmup
    warmup = min(50, max(1, n // 20))
    for i in range(warmup):
        op(i)

    latencies: list[float] = []
    start_total = time.perf_counter_ns()
    for i in range(n):
        t0 = time.perf_counter_ns()
        op(i)
        latencies.append((time.perf_counter_ns() - t0) / 1000.0)
    total_s = (time.perf_counter_ns() - start_total) / 1e9
    return OpResult(name=name, system=system, ops=n, total_s=total_s, latencies_us=latencies)


# ---------- Redis adapters ----------

class RedisAdapter:
    name = "redis"

    def __init__(self, url: str) -> None:
        self.r = redis.Redis.from_url(url, decode_responses=True)
        self.r.ping()

    def setup(self) -> None:
        self.r.flushdb()

    def teardown(self) -> None:
        self.r.flushdb()

    def set(self, i: int) -> None: self.r.set(f"bench:s:{i}", "value")
    def get(self, i: int) -> None: self.r.get(f"bench:s:{i}")
    def exists(self, i: int) -> None: self.r.exists(f"bench:s:{i}")
    def delete(self, i: int) -> None: self.r.delete(f"bench:s:{i}")
    def incr(self, i: int) -> None: self.r.incr("bench:c")
    def decr(self, i: int) -> None: self.r.decr("bench:c")
    def hset(self, i: int) -> None: self.r.hset("bench:h", f"f{i}", "v")
    def hget(self, i: int) -> None: self.r.hget("bench:h", f"f{i}")
    def hdel(self, i: int) -> None: self.r.hdel("bench:h", f"f{i}")
    def lpush(self, i: int) -> None: self.r.lpush("bench:lpush", f"v{i}")
    def rpush(self, i: int) -> None: self.r.rpush("bench:rpush", f"v{i}")
    def lpop(self, i: int) -> None: self.r.lpop("bench:lpush")
    def rpop(self, i: int) -> None: self.r.rpop("bench:rpush")
    def llen(self, i: int) -> None: self.r.llen("bench:rpush")


# ---------- pg_redis adapters ----------

class PgRedisAdapter:
    name = "pg_redis"

    def __init__(self, dsn: str) -> None:
        self.conn = psycopg.connect(dsn, autocommit=True)
        self.cur = self.conn.cursor()
        self._exec("CREATE EXTENSION IF NOT EXISTS pg_redis", fetch=False)

    def _exec(self, sql: str, params: Iterable | None = None, fetch: bool = True):
        self.cur.execute(sql, params or ())
        if fetch:
            try:
                return self.cur.fetchone()
            except psycopg.ProgrammingError:
                return None
        return None

    def setup(self) -> None:
        self._exec("SELECT pgredis.\"FLUSHALL\"()")

    def teardown(self) -> None:
        self._exec("SELECT pgredis.\"FLUSHALL\"()")

    def set(self, i: int) -> None: self._exec('SELECT pgredis."SET"(%s, %s)', (f"bench:s:{i}", "value"))
    def get(self, i: int) -> None: self._exec('SELECT pgredis."GET"(%s)', (f"bench:s:{i}",))
    def exists(self, i: int) -> None: self._exec('SELECT pgredis."EXISTS"(%s)', (f"bench:s:{i}",))
    def delete(self, i: int) -> None: self._exec('SELECT pgredis."DEL"(%s)', (f"bench:s:{i}",))
    def incr(self, i: int) -> None: self._exec('SELECT pgredis."INCR"(%s)', ("bench:c",))
    def decr(self, i: int) -> None: self._exec('SELECT pgredis."DECR"(%s)', ("bench:c",))
    def hset(self, i: int) -> None: self._exec('SELECT pgredis."HSET"(%s, %s, %s)', ("bench:h", f"f{i}", "v"))
    def hget(self, i: int) -> None: self._exec('SELECT pgredis."HGET"(%s, %s)', ("bench:h", f"f{i}"))
    def hdel(self, i: int) -> None: self._exec('SELECT pgredis."HDEL"(%s, %s)', ("bench:h", f"f{i}"))
    def lpush(self, i: int) -> None: self._exec('SELECT pgredis."LPUSH"(%s, %s)', ("bench:lpush", f"v{i}"))
    def rpush(self, i: int) -> None: self._exec('SELECT pgredis."RPUSH"(%s, %s)', ("bench:rpush", f"v{i}"))
    def lpop(self, i: int) -> None: self._exec('SELECT pgredis."LPOP"(%s)', ("bench:lpush",))
    def rpop(self, i: int) -> None: self._exec('SELECT pgredis."RPOP"(%s)', ("bench:rpush",))
    def llen(self, i: int) -> None: self._exec('SELECT pgredis."LLEN"(%s)', ("bench:rpush",))


# ---------- workload definition ----------

OP_SEQUENCE: list[tuple[str, str, str | None]] = [
    # (op_name, method_name, prep_method_name_or_None)
    ("SET",    "set",    None),
    ("GET",    "get",    "set"),
    ("EXISTS", "exists", "set"),
    ("DEL",    "delete", "set"),
    ("INCR",   "incr",   None),
    ("DECR",   "decr",   None),
    ("HSET",   "hset",   None),
    ("HGET",   "hget",   "hset"),
    ("HDEL",   "hdel",   "hset"),
    ("LPUSH",  "lpush",  None),
    ("LPOP",   "lpop",   "lpush"),
    ("RPUSH",  "rpush",  None),
    ("RPOP",   "rpop",   "rpush"),
    ("LLEN",   "llen",   "rpush"),
]


def run_system(adapter, n: int) -> list[OpResult]:
    results: list[OpResult] = []
    adapter.setup()
    for op_name, method, prep in OP_SEQUENCE:
        if prep is not None:
            prep_op = getattr(adapter, prep)
            for i in range(n):
                prep_op(i)
        fn = getattr(adapter, method)
        res = run_op(op_name, adapter.name, n, fn)
        results.append(res)
    adapter.teardown()
    return results


# ---------- report ----------

def format_us(us: float) -> str:
    if us < 1000:
        return f"{us:6.1f}us"
    if us < 1_000_000:
        return f"{us/1000:6.2f}ms"
    return f"{us/1e6:6.2f}s "


def format_ops(ops: float) -> str:
    if ops >= 1_000_000:
        return f"{ops/1e6:.2f}M/s"
    if ops >= 1_000:
        return f"{ops/1e3:.1f}k/s"
    return f"{ops:.0f}/s"


def print_report(redis_results: list[OpResult], pg_results: list[OpResult], n: int) -> None:
    by_op = {(r.name, r.system): r for r in redis_results + pg_results}

    print()
    print("=" * 90)
    print(f"pg_redis vs Redis benchmark  ({n} ops per operation, single client, single connection)")
    print("=" * 90)
    header = f"{'Op':<8} {'System':<10} {'ops/sec':>10} {'p50':>10} {'p95':>10} {'p99':>10} {'winner':>12}"
    print(header)
    print("-" * 92)
    for op_name, _, _ in OP_SEQUENCE:
        r = by_op.get((op_name, "redis"))
        p = by_op.get((op_name, "pg_redis"))
        if r is None or p is None:
            continue
        if r.ops_per_sec >= p.ops_per_sec:
            ratio = r.ops_per_sec / p.ops_per_sec if p.ops_per_sec > 0 else float("inf")
            winner = f"{ratio:5.1f}x redis"
        else:
            ratio = p.ops_per_sec / r.ops_per_sec
            winner = f"{ratio:5.1f}x pg"
        print(f"{op_name:<8} {'redis':<10} "
              f"{format_ops(r.ops_per_sec):>10} "
              f"{format_us(r.percentile(0.50)):>10} "
              f"{format_us(r.percentile(0.95)):>10} "
              f"{format_us(r.percentile(0.99)):>10} "
              f"{winner:>12}")
        print(f"{'':<8} {'pg_redis':<10} "
              f"{format_ops(p.ops_per_sec):>10} "
              f"{format_us(p.percentile(0.50)):>10} "
              f"{format_us(p.percentile(0.95)):>10} "
              f"{format_us(p.percentile(0.99)):>10}")
        print("-" * 92)

    # aggregate
    r_total_ops = sum(r.ops for r in redis_results)
    r_total_s = sum(r.total_s for r in redis_results)
    p_total_ops = sum(r.ops for r in pg_results)
    p_total_s = sum(r.total_s for r in pg_results)
    r_agg = r_total_ops / r_total_s
    p_agg = p_total_ops / p_total_s
    print()
    if r_agg >= p_agg:
        print(f"Aggregate throughput: redis = {format_ops(r_agg)}, pg_redis = {format_ops(p_agg)}  "
              f"(redis {r_agg/p_agg:.1f}x faster overall)")
    else:
        print(f"Aggregate throughput: redis = {format_ops(r_agg)}, pg_redis = {format_ops(p_agg)}  "
              f"(pg_redis {p_agg/r_agg:.1f}x faster overall)")
    print()


# ---------- entrypoint ----------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--pg-dsn", default=os.environ.get("PG_DSN", "postgresql://postgres:postgres@localhost:5432/postgres"))
    ap.add_argument("-n", "--iterations", type=int, default=int(os.environ.get("BENCH_N", "1000")))
    args = ap.parse_args()

    print(f"Connecting to redis: {args.redis_url}")
    redis_adapter = RedisAdapter(args.redis_url)

    print(f"Connecting to pg_redis: {args.pg_dsn}")
    pg_adapter = PgRedisAdapter(args.pg_dsn)

    print(f"Running {args.iterations} iterations per operation...")
    print()

    pg_results = run_system(pg_adapter, args.iterations)
    redis_results = run_system(redis_adapter, args.iterations)

    print_report(redis_results, pg_results, args.iterations)
    return 0


if __name__ == "__main__":
    sys.exit(main())
