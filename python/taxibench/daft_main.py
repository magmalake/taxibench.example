"""Run the query suite through Daft, over one of two scan sources.

    python -m taxibench.daft_main <table-dir> --source native|mojo [options]

`native` is `daft.read_iceberg` — Daft's own Parquet reader, planned through
PyIceberg. `mojo` is `IcebergLocalSource` from `pyarrow-flight.example`, which
calls `iceberg.mojo`'s scan library in this process and hands Daft the Arrow
buffers where they already are.

Everything above the source is identical: the same DataFrame expressions, the
same engine, the same machine. The output is the JSON-lines shape the other
legs emit, so `scripts/compare.py` diffs these two against each other exactly
as it diffs iceberg.mojo against PyIceberg.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

from . import daft_queries
from .__main__ import find_metadata, peak_rss_mb, percentile

DEFAULT_DAFT_FLIGHT = "../pyarrow-flight.example"
"""Where `daft_flight` lives: a sibling checkout, the way the tins are.

The connector is not a published package — it needs a Mojo-built shared
library beside it to be worth anything — so this leg imports it from the
repository that owns it rather than pretending it can be installed.
"""


def open_source(args) -> tuple[object, int]:
    """The DataFrame to query, and how many scan tasks are behind it.

    The task count is knowable for the Mojo source, which plans through a
    function call we make ourselves, and is not for the native one: Daft's
    scan tasks are behind its own planner and `num_partitions()` on an
    unexecuted DataFrame is `None`. It is reported where it is known and left
    at 0 where it is not, rather than guessed at.
    """
    if args.source == "native":
        import daft

        return daft.read_iceberg(find_metadata(args.table), snapshot_id=args.snapshot), 0

    sys.path.insert(0, os.path.abspath(args.daft_flight))
    from daft_flight import IcebergLocalSource

    source = IcebergLocalSource(
        args.lib, args.table, split_size=args.split_size, shim_prefix=args.shim_prefix
    )
    # What the plan looks like with no predicate: the number a pushed-down
    # filter is measured against.
    return source.read(), len(source._plan(b""))


def run_one(df, query: daft_queries.DaftQuery) -> dict:
    t0 = time.monotonic()
    result = query.run(df)
    total_ms = (time.monotonic() - t0) * 1000
    return {
        "query": query.name,
        "title": query.title,
        "rows_scanned": result["count"],
        "plan_ms": 0.0,
        "total_ms": round(total_ms, 2),
        "result": result,
    }


def main() -> int:
    ap = argparse.ArgumentParser(prog="taxibench.daft")
    ap.add_argument("table", help="table directory (or metadata.json for --source native)")
    ap.add_argument("--source", choices=("native", "mojo"), required=True)
    ap.add_argument("--query", action="append", help="run only these (repeatable)")
    ap.add_argument("--repeat", type=int, default=1, help="timed runs per query")
    ap.add_argument("--warmup", action="store_true", help="discard a first run")
    ap.add_argument("--snapshot", type=int, default=None, help="read this snapshot")
    ap.add_argument("--out", default="-", help="write JSON lines here")
    ap.add_argument("--lib", default=os.environ.get("TAXIBENCH_SCAN_LIB", ""),
                    help="libibscan for --source mojo")
    ap.add_argument("--daft-flight", default=os.environ.get("DAFT_FLIGHT_DIR", DEFAULT_DAFT_FLIGHT),
                    help=f"checkout holding daft_flight/ (default {DEFAULT_DAFT_FLIGHT})")
    ap.add_argument("--shim-prefix", default=os.environ.get("MAGMALAKE_SHIM_PREFIX"),
                    help="prefix holding the Mojo codec shims")
    ap.add_argument("--split-size", type=int, default=128 * 1024 * 1024)
    args = ap.parse_args()

    if args.source == "mojo" and not args.lib:
        raise SystemExit("error: --source mojo needs --lib (iceberg.mojo's libibscan)")

    selected = daft_queries.ALL
    if args.query:
        missing = [n for n in args.query if n not in daft_queries.BY_NAME]
        if missing:
            raise SystemExit(f"error: unknown queries: {', '.join(missing)}")
        selected = [daft_queries.BY_NAME[n] for n in args.query]

    df, splits = open_source(args)
    engine = "daft-mojo" if args.source == "mojo" else "daft-native"

    sink = sys.stdout if args.out == "-" else open(args.out, "w")
    try:
        for query in selected:
            if args.warmup:
                run_one(df, query)
            runs = [run_one(df, query) for _ in range(args.repeat)]
            samples = sorted(r["total_ms"] for r in runs)

            record = runs[0]
            record["engine"] = engine
            record["files"] = 0
            record["splits"] = splits
            record["total_ms"] = percentile(samples, 0.50)
            record["p90_ms"] = percentile(samples, 0.90)
            record["min_ms"] = samples[0]
            record["repeat"] = args.repeat
            record["peak_rss_mb"] = round(peak_rss_mb(), 1)
            print(json.dumps(record), file=sink, flush=True)
    finally:
        if sink is not sys.stdout:
            sink.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
