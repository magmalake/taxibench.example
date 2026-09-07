"""Run the query suite over the Lance dataset and print one JSON record per query.

    python -m taxibench_lance <lance-dir> [options]

The output is the shape `python/taxibench/__main__.py` emits, field for field,
so `scripts/compare.py` diffs a Lance result file against either of the other
two without knowing anything about Lance. `engine` says `lance`, and `files`
counts fragments rather than Parquet data files — the same 24 units, because
the converter writes one fragment per month.

The measurement discipline is the suite's: p50 of N with p90 beside it, never a
best-of-N minimum, a discarded warm-up, and one query per process.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import sys
import time

import pyarrow as pa

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


# Lance reads in batches of 8,192 rows by default, which a selective filter
# thins to about 2,000 by the time the fold sees them; PyIceberg hands the same
# fold batches of 17,000 to 129,000. Since the fold is the same code on both
# legs, that difference is measured as Lance being slow when what is actually
# slow is a Python loop running eight times as often: q5 folds 9,712 batches at
# the default and 617 at this one, for the same 77,929,134 rows. Reading in
# batches of 131,072 puts Lance's granularity where PyIceberg's already is, and
# is where the curve flattens — 262,144 buys nothing on any query but q5. The
# batch stays bounded either way, which is the property that matters.
DEFAULT_BATCH_SIZE = 131_072


def run_one(
    lance, root: str, uri: str, query, scan: queries.Scan, batch_size: int
) -> dict:
    # Planning here is what planning is on the other two legs: open the table's
    # current metadata and work out which units of storage the query can touch.
    # For Lance that is the manifest plus the sidecar the converter wrote, both
    # read from disk on every run rather than cached across them, because
    # PyIceberg re-reads its manifests on every `plan_files` too.
    t0 = time.monotonic()
    dataset = lance.dataset(uri)
    entries = queries.load_sidecar(root)["fragments"]
    groups = queries.plan(scan, entries)
    by_id = {f.fragment_id: f for f in dataset.get_fragments()}
    plans = [([by_id[i] for i in ids], filter_) for ids, filter_ in groups]
    plan_ms = (time.monotonic() - t0) * 1000
    files = sum(len(fragments) for fragments, _ in plans)

    # The whole scan is timed — decode, filter and fold — which is what a
    # caller waits for, and batches are streamed rather than materialised, since
    # q4's three columns are about 1.9 GB of Arrow if taken at once. One
    # difference from the other two legs: they re-plan inside the read, so
    # their `plan_ms` is work that is also inside `total_ms`, while here the
    # scanner is handed the fragments the plan already chose and does not plan
    # again. It is a fraction of a millisecond either way, and as everywhere
    # else in this suite `plan_ms` is reported beside the total rather than
    # added to or subtracted from it.
    t1 = time.monotonic()
    state = query.init()
    rows = 0
    for fragments, filter_ in plans:
        scanner = dataset.scanner(
            columns=list(scan.columns) or None,
            filter=filter_,
            fragments=fragments,
            batch_size=batch_size,
        )
        for batch in scanner.to_batches():
            rows += batch.num_rows
            state = query.fold(state, batch)
    total_ms = (time.monotonic() - t1) * 1000

    return {
        "query": query.name,
        "title": query.title,
        "engine": "lance",
        "files": files,
        "rows_scanned": rows,
        "plan_ms": round(plan_ms, 2),
        "total_ms": round(total_ms, 2),
        "result": query.finish(state),
    }


def main() -> int:
    ap = argparse.ArgumentParser(prog="taxibench_lance")
    ap.add_argument("dataset", help="the directory convert_lance.py wrote")
    ap.add_argument("--query", action="append", help="run only these (repeatable)")
    ap.add_argument("--repeat", type=int, default=1, help="timed runs per query")
    ap.add_argument("--warmup", action="store_true", help="discard a first run")
    ap.add_argument("--out", default="-", help="write JSON lines here")
    ap.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"rows per read batch (default {DEFAULT_BATCH_SIZE})",
    )
    ap.add_argument(
        "--threads",
        type=int,
        default=0,
        help="Lance CPU and I/O threads; 0 leaves Lance's own default",
    )
    args = ap.parse_args()

    # Lance sizes its thread pools from the environment when its runtime starts,
    # which happens on the first call into the extension module — so the count
    # has to be set before `lance` is imported, and the import is deliberately
    # late for that reason. Naming the thread count is what makes the single
    # thread leg meaningful: Lance, like pyarrow, reads in parallel by default
    # and does not announce it.
    if args.threads > 0:
        os.environ["LANCE_CPU_THREADS"] = str(args.threads)
        os.environ["LANCE_IO_THREADS"] = str(args.threads)
        # The fold runs through pyarrow.compute, exactly as the PyIceberg leg's
        # does, so pyarrow gets the same ceiling on both.
        pa.set_cpu_count(args.threads)
        pa.set_io_thread_count(args.threads)

    import lance

    root = os.path.abspath(args.dataset)
    sidecar = queries.load_sidecar(root)
    uri = os.path.join(root, sidecar["dataset"])

    selected = queries.ALL
    if args.query:
        missing = [n for n in args.query if n not in queries.BY_NAME]
        if missing:
            raise SystemExit(f"error: unknown queries: {', '.join(missing)}")
        selected = [queries.BY_NAME[n] for n in args.query]

    sink = sys.stdout if args.out == "-" else open(args.out, "w")
    try:
        for query in selected:
            scan = queries.SCANS[query.name]
            if args.warmup:
                run_one(lance, root, uri, query, scan, args.batch_size)
            runs = [
                run_one(lance, root, uri, query, scan, args.batch_size)
                for _ in range(args.repeat)
            ]
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
            record["batch_size"] = args.batch_size
            record["peak_rss_mb"] = round(peak_rss_mb(), 1)
            print(json.dumps(record), file=sink, flush=True)
    finally:
        if sink is not sys.stdout:
            sink.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
