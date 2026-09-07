"""Run the query suite through PostgreSQL and print one JSON record per query.

    python -m taxibench.postgres <dsn> [options]

The records are the shape `python/taxibench/__main__.py` emits, so
`scripts/compare.py` diffs them against the PyIceberg answers with no changes:
same keys, same measurement discipline — p50 of N with p90 beside it, never
best-of-N, a discarded warm-up, and one query per process because neighbouring
queries warm the buffer cache for each other.

Three fields need saying out loud, because PostgreSQL is not reading the
Iceberg table and two of them would otherwise be quietly wrong.

**`files`** is the number of table partitions the planner left in the plan. The
PostgreSQL table is range-partitioned into the same 24 months the Iceberg table
is partitioned into, and each month is a single ~550 MB relation — below the
1 GB segment size, so a partition really is one file. It is read out of
`EXPLAIN`, not asserted: the count is whatever partition pruning actually did,
which is how it comes out equal to Iceberg's file count on every query rather
than by being told to.

**`plan_ms`** is the server's own `Planning Time` from `EXPLAIN (SUMMARY ON)`,
which is the same thing PyIceberg's `plan_ms` is: the cost of deciding what to
read, reported beside the total rather than subtracted from it, because
planning happens again inside the execute.

**`peak_rss_mb`** is the *client's* peak RSS and is therefore not comparable
with the other two implementations, where the client is the engine. It is left
in the record for shape rather than for meaning; what bounds the server is
`shared_buffers` and `work_mem`, which are reported alongside it.
"""

from __future__ import annotations

import argparse
import json
import resource
import sys
import time
from collections import Counter

import psycopg

from . import postgres_queries as queries


def percentile(sorted_samples: list[float], q: float) -> float:
    """Nearest-rank percentile; `repeat` is small, so nothing fancier is honest."""
    if not sorted_samples:
        return 0.0
    index = min(len(sorted_samples) - 1, int(round(q * (len(sorted_samples) - 1))))
    return round(sorted_samples[index], 2)


def peak_rss_mb() -> float:
    """Peak resident set size. Linux reports KiB, macOS reports bytes."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024


def summarise_plan(plan: dict) -> tuple[int, str]:
    """(partitions the plan will read, how it will read them).

    Walking the plan tree for `Relation Name` counts the leaf partitions that
    survived pruning, which is the `files` number.

    The scan summary is a histogram rather than one word because the planner
    decides *per partition* and does not always decide the same way: with the
    `payment_type` index in place, q3 comes out as sixteen index scans and
    eight bitmap heap scans over the twenty-four months in the serial leg, and
    as fourteen, eight and two sequential scans in the parallel one. Collapsing
    that to "indexed" would hide the most interesting thing the indexed leg has
    to say. `Bitmap Index Scan` nodes are left out because each one is the
    lower half of a `Bitmap Heap Scan` that is already counted.
    """
    relations: set[str] = set()
    kinds: Counter[str] = Counter()

    def walk(node: dict) -> None:
        name = node.get("Relation Name")
        node_type = node.get("Node Type", "")
        if name:
            relations.add(name)
        if node_type.endswith("Scan") and node_type != "Bitmap Index Scan":
            kinds[node_type] += 1
        for child in node.get("Plans", []):
            walk(child)

    walk(plan["Plan"])
    summary = ", ".join(
        f"{kind} x{count}" if count > 1 else kind
        for kind, count in sorted(kinds.items(), key=lambda kv: (-kv[1], kv[0]))
    )
    return len(relations), summary or "none"


def run_one(conn: psycopg.Connection, query: queries.PgQuery) -> dict:
    with conn.cursor() as cur:
        cur.execute("EXPLAIN (FORMAT JSON, SUMMARY ON) " + query.sql)
        explained = cur.fetchone()[0][0]
        plan_ms = float(explained.get("Planning Time", 0.0))
        files, scan = summarise_plan(explained)

        t0 = time.monotonic()
        cur.execute(query.sql)
        rows = cur.fetchall()
        total_ms = (time.monotonic() - t0) * 1000

    result, rows_scanned = query.finish(rows)
    return {
        "query": query.name,
        "title": query.title,
        "engine": "postgres",
        "files": files,
        "rows_scanned": rows_scanned,
        "plan_ms": round(plan_ms, 2),
        "total_ms": round(total_ms, 2),
        "result": result,
        "scan": scan,
    }


def setting(conn: psycopg.Connection, name: str) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT current_setting(%s)", (name,))
        return cur.fetchone()[0]


def main() -> int:
    ap = argparse.ArgumentParser(prog="taxibench.postgres")
    ap.add_argument("dsn", help="libpq connection string")
    ap.add_argument("--query", action="append", help="run only these (repeatable)")
    ap.add_argument("--repeat", type=int, default=1, help="timed runs per query")
    ap.add_argument("--warmup", action="store_true", help="discard a first run")
    ap.add_argument(
        "--workers",
        type=int,
        default=0,
        help="parallel workers per gather; 0 is a serial plan",
    )
    ap.add_argument("--out", default="-", help="write JSON lines here")
    args = ap.parse_args()

    selected = queries.ALL
    if args.query:
        missing = [n for n in args.query if n not in queries.BY_NAME]
        if missing:
            raise SystemExit(f"error: unknown queries: {', '.join(missing)}")
        selected = [queries.BY_NAME[n] for n in args.query]

    conn = psycopg.connect(args.dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        # Naming the worker count is the difference between a comparison and an
        # anecdote, and it is the same thing the other two legs do with their
        # thread counts. 0 forces a serial plan; N asks for N workers beside
        # the leader, which the leaf partitions' `parallel_workers` setting
        # makes the planner actually grant.
        # SET takes no parameters, so the value is interpolated; it is an int
        # straight off argparse.
        cur.execute(f"SET max_parallel_workers_per_gather = {int(args.workers)}")
    threads = args.workers + 1

    indexes = 0
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid"
            " JOIN pg_inherits h ON h.inhrelid = c.oid"
            " WHERE h.inhparent = 'trips'::regclass"
        )
        indexes = int(cur.fetchone()[0])
        cur.execute(
            "SELECT sum(pg_total_relation_size(c.oid))::bigint"
            " FROM pg_class c JOIN pg_inherits i ON i.inhrelid = c.oid"
            " WHERE i.inhparent = 'trips'::regclass"
        )
        table_bytes = int(cur.fetchone()[0])

    sink = sys.stdout if args.out == "-" else open(args.out, "w")
    try:
        for query in selected:
            if args.warmup:
                run_one(conn, query)
            runs = [run_one(conn, query) for _ in range(args.repeat)]
            samples = sorted(r["total_ms"] for r in runs)

            record = runs[0]
            record["total_ms"] = percentile(samples, 0.50)
            record["p90_ms"] = percentile(samples, 0.90)
            record["min_ms"] = samples[0]
            record["plan_ms"] = percentile(sorted(r["plan_ms"] for r in runs), 0.50)
            record["repeat"] = args.repeat
            record["threads"] = threads
            record["workers"] = threads
            record["parallel_workers"] = args.workers
            record["indexed"] = indexes > 0
            record["table_bytes"] = table_bytes
            record["shared_buffers"] = setting(conn, "shared_buffers")
            record["work_mem"] = setting(conn, "work_mem")
            record["peak_rss_mb"] = round(peak_rss_mb(), 1)
            # Every timed run of a float sum is its own accumulation order once
            # workers are involved, so the spread across the repeats is the
            # honest way to say how reproducible the answer is. It is reported
            # rather than hidden behind taking runs[0].
            sums = [
                r["result"].get("sum")
                for r in runs
                if isinstance(r["result"].get("sum"), float)
            ]
            if sums and max(sums) != min(sums):
                scale = max(abs(max(sums)), abs(min(sums)), 1.0)
                record["sum_spread_rel"] = (max(sums) - min(sums)) / scale
            print(json.dumps(record), file=sink, flush=True)
    finally:
        if sink is not sys.stdout:
            sink.close()
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
