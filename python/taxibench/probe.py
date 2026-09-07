"""The PyIceberg half of the probe. Same scan, same shape of output.

    python -m taxibench.probe <warehouse> [--select a,b] [--filter EXPR]
                              [--threads N] [--repeat N] [--warmup] [--plan-only]

See src/probe.mojo for why this exists: to hold the file set still and vary only
the projection, so the fixed per-scan cost separates from the per-column cost.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time

import pyarrow as pa
from pyiceberg.table import StaticTable

from .__main__ import find_metadata


def main() -> int:
    ap = argparse.ArgumentParser(prog="taxibench.probe")
    ap.add_argument("table")
    ap.add_argument("--select", default="")
    ap.add_argument("--filter", default="")
    ap.add_argument("--threads", type=int, default=1)
    ap.add_argument("--repeat", type=int, default=5)
    ap.add_argument("--warmup", action="store_true")
    ap.add_argument("--plan-only", action="store_true")
    args = ap.parse_args()

    if args.threads > 0:
        pa.set_cpu_count(args.threads)
        pa.set_io_thread_count(args.threads)

    table = StaticTable.from_metadata(find_metadata(args.table))
    columns = tuple(c for c in args.select.split(",") if c) or ("*",)

    samples, rows, files = [], 0, 0
    for run in range(args.repeat + (1 if args.warmup else 0)):
        scan = table.scan(
            row_filter=args.filter or "true", selected_fields=columns
        )
        start = time.monotonic()
        if args.plan_only:
            files = len(list(scan.plan_files()))
            rows = 0
        else:
            rows = 0
            for batch in scan.to_arrow_batch_reader():
                rows += batch.num_rows
        elapsed = (time.monotonic() - start) * 1000
        if run > 0 or not args.warmup:
            samples.append(elapsed)

    samples.sort()
    last = len(samples) - 1
    print(
        json.dumps(
            {
                "engine": "pyiceberg",
                "columns": 0 if columns == ("*",) else len(columns),
                "rows": rows,
                "files": files,
                "threads": args.threads,
                "plan_only": args.plan_only,
                "p50_ms": round(samples[(50 * last + 50) // 100], 3),
                "p90_ms": round(samples[(90 * last + 50) // 100], 3),
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
