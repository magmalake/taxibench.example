"""Run the query suite through PyIceberg and print one JSON record per query.

    python -m taxibench <table-dir-or-metadata.json> [options]

The output is the same shape the Mojo binary emits, so the two can be diffed
directly — which is how the answers are checked against each other rather than
taken on trust.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import sys
import time

import pyarrow as pa
from pyiceberg.table import StaticTable

from . import queries


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


def find_metadata(target: str) -> str:
    """Accept a metadata.json, a table directory, or a warehouse root."""
    if target.endswith(".json"):
        return target
    if os.path.isfile(os.path.join(target, "metadata_location.txt")):
        with open(os.path.join(target, "metadata_location.txt")) as fh:
            return fh.read().strip()
    metadata_dir = os.path.join(target, "metadata")
    if os.path.isdir(metadata_dir):
        # The highest-numbered metadata.json is the current one.
        candidates = sorted(
            f for f in os.listdir(metadata_dir) if f.endswith(".metadata.json")
        )
        if candidates:
            return os.path.join(metadata_dir, candidates[-1])
    raise SystemExit(f"error: no table metadata under {target}")


def run_one(table, query: queries.Query, snapshot_id: int | None) -> dict:
    scan = table.scan(
        row_filter=query.row_filter,
        selected_fields=query.columns or ("*",),
        snapshot_id=snapshot_id,
    )

    t0 = time.monotonic()
    tasks = list(scan.plan_files())
    plan_ms = (time.monotonic() - t0) * 1000

    # Planning runs again inside the read; `read_ms` is the whole scan, which
    # is what a caller actually waits for, and `plan_ms` is reported beside it
    # rather than subtracted out.
    t1 = time.monotonic()
    state = query.init()
    rows = 0
    reader = scan.to_arrow_batch_reader()
    for batch in reader:
        rows += batch.num_rows
        state = query.fold(state, batch)
    total_ms = (time.monotonic() - t1) * 1000

    return {
        "query": query.name,
        "title": query.title,
        "engine": "pyiceberg",
        "files": len(tasks),
        "rows_scanned": rows,
        "plan_ms": round(plan_ms, 2),
        "total_ms": round(total_ms, 2),
        "result": query.finish(state),
    }


def main() -> int:
    ap = argparse.ArgumentParser(prog="taxibench")
    ap.add_argument("table", help="metadata.json, table dir, or warehouse root")
    ap.add_argument("--query", action="append", help="run only these (repeatable)")
    ap.add_argument("--repeat", type=int, default=1, help="timed runs per query")
    ap.add_argument("--warmup", action="store_true", help="discard a first run")
    ap.add_argument("--snapshot", type=int, default=None, help="read this snapshot")
    ap.add_argument("--out", default="-", help="write JSON lines here")
    ap.add_argument(
        "--threads",
        type=int,
        default=0,
        help="pyarrow CPU threads; 0 leaves pyarrow's own default",
    )
    args = ap.parse_args()

    # Naming the thread count on both sides is the difference between a
    # comparison and an anecdote. pyarrow reads Parquet multi-threaded by
    # default and never says so; `set_cpu_count(1)` is what makes the
    # single-threaded leg actually single-threaded, and it matters even when
    # `use_threads=False` is passed further down.
    if args.threads > 0:
        pa.set_cpu_count(args.threads)
        pa.set_io_thread_count(args.threads)

    metadata = find_metadata(args.table)
    table = StaticTable.from_metadata(metadata)

    selected = queries.ALL
    if args.query:
        missing = [n for n in args.query if n not in queries.BY_NAME]
        if missing:
            raise SystemExit(f"error: unknown queries: {', '.join(missing)}")
        selected = [queries.BY_NAME[n] for n in args.query]

    sink = sys.stdout if args.out == "-" else open(args.out, "w")
    try:
        for query in selected:
            if args.warmup:
                run_one(table, query, args.snapshot)
            runs = [run_one(table, query, args.snapshot) for _ in range(args.repeat)]
            samples = sorted(r["total_ms"] for r in runs)

            record = runs[0]
            # p50 is the headline and p90 sits beside it; a mean over an
            # unbounded tail measures how the machine felt rather than how the
            # code performs, and min is only honest as an explicit floor.
            record["total_ms"] = percentile(samples, 0.50)
            record["p90_ms"] = percentile(samples, 0.90)
            record["min_ms"] = samples[0]
            record["plan_ms"] = percentile(sorted(r["plan_ms"] for r in runs), 0.50)
            record["repeat"] = args.repeat
            record["threads"] = args.threads
            record["peak_rss_mb"] = round(peak_rss_mb(), 1)
            print(json.dumps(record), file=sink, flush=True)
    finally:
        if sink is not sys.stdout:
            sink.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
