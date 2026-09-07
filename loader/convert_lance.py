#!/usr/bin/env python3
"""Transcode the Iceberg table into a Lance dataset — a second copy of the data.

This is the honest thing to be explicit about: Lance does not read the Iceberg
table. It reads its own files, in its own format, so the benchmark's third leg
needs its own copy of the 79,478,796 rows. This script makes that copy, and the
README says what it costs in bytes and in minutes.

**The source is the Iceberg table's own data files, not the raw TLC Parquet.**
`loader/load_table.py` does real work on the way in — it casts two different
TLC schemas onto one, and drops the ~1,150 rows whose pickup timestamp falls
outside the month their file is named for. Reproducing that here would be a
second implementation of it, and a second implementation that drifted by one
row would make every answer differ for a reason that has nothing to do with
Lance. Reading the table's 24 Parquet data files instead inherits the
normalisation rather than repeating it: the rows cannot differ, because they
are the same rows.

**One Lance fragment per month, mirroring the 24 Iceberg data files.** Lance
has no partitioning: a dataset is a list of fragments and nothing in the format
records what is in one. Writing month by month gives the two formats the same
physical grouping, so a query that touches three months touches three units on
both sides, and neither gets a layout the other does not have. Nothing is
sorted or clustered on the way through — the row order inside each month is the
Iceberg file's row order, which is the TLC file's row order.

Because Lance stores no partition values, the fragment-to-month mapping is
written out beside the dataset as `fragments.json`, with each fragment's row
count and pickup-timestamp bounds. That sidecar is what the Lance runner prunes
with, and it is the one place where this leg is handed something Iceberg
maintains for itself. See `python/taxibench_lance/queries.py`.

Usage: convert_lance.py <warehouse-dir> <lance-dir> [--compress zstd]
"""

import argparse
import glob
import json
import os
import re
import shutil
import sys
import time

import lance
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

PARTITION_RE = re.compile(r"pickup_month=(\d{4}-\d{2})")

# One fragment per month. The largest TLC month here is about 3.7M rows, so any
# ceiling above that leaves the month whole; Lance's default of 1,048,576 would
# cut each month into four and give the pruning a different shape than the
# Iceberg table's.
MAX_ROWS_PER_FILE = 16_000_000


def data_files(warehouse: str) -> list[tuple[str, str]]:
    """The table's live data files, as (month, path), oldest month first.

    Globbing rather than reading the manifests is safe here because of how the
    table is built: `load_table.py` deletes the warehouse and writes it fresh
    with one append per month and no deletes or rewrites, so every Parquet file
    under `data/` is live and belongs to exactly one partition. A table that
    had been compacted or had rows deleted would need the manifests.
    """
    pattern = os.path.join(warehouse, "warehouse", "*", "*", "data", "*", "*.parquet")
    found: list[tuple[str, str]] = []
    for path in sorted(glob.glob(pattern)):
        match = PARTITION_RE.search(path)
        if not match:
            raise SystemExit(f"error: unpartitioned data file: {path}")
        found.append((match.group(1), path))
    months = [month for month, _ in found]
    if len(set(months)) != len(months):
        raise SystemExit("error: more than one data file per month; not a fresh load")
    return found


def restate_schema(table: pa.Table, compression: str | None) -> pa.Table:
    """Drop the Parquet field-id metadata, and optionally ask Lance for zstd.

    Lance assigns its own field ids, so the `PARQUET:field_id` metadata the
    Iceberg writer stamped on means nothing here and only clutters the manifest.

    Compression is off by default because that is what a Lance user gets by
    default: the stable format applies structural encodings — bit-packing,
    dictionaries, run-length — but no general-purpose block compressor unless
    the field asks for one. It is worth knowing what the difference costs, so
    `--compress zstd` asks for one and the README reports both sizes.
    """
    metadata = {"lance-encoding:compression": compression} if compression else None
    schema = pa.schema(
        [pa.field(f.name, f.type, f.nullable, metadata=metadata) for f in table.schema]
    )
    return pa.Table.from_arrays(list(table.columns), schema=schema)


def bounds(column: pa.ChunkedArray) -> tuple[str, str, int]:
    """Min, max and null count of a timestamp column, as the sidecar records them."""
    extremes = pc.min_max(column).as_py()
    nulls = column.null_count
    return extremes["min"].isoformat(), extremes["max"].isoformat(), nulls


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("warehouse", help="the Iceberg warehouse to transcode")
    ap.add_argument("lance", help="directory to write the Lance dataset into")
    ap.add_argument(
        "--compress",
        default=None,
        help="ask Lance for a block compressor on every column, e.g. zstd",
    )
    args = ap.parse_args()

    warehouse = os.path.abspath(args.warehouse)
    root = os.path.abspath(args.lance)
    uri = os.path.join(root, "trips.lance")
    if os.path.isdir(root):
        shutil.rmtree(root)
    os.makedirs(root)

    files = data_files(warehouse)
    if not files:
        print(f"error: no Iceberg data files under {warehouse}", file=sys.stderr)
        return 1

    entries: list[dict] = []
    total, started, mode = 0, time.monotonic(), "create"
    for month, path in files:
        t0 = time.monotonic()
        batch = restate_schema(pq.read_table(path), args.compress)
        low, high, nulls = bounds(batch.column("tpep_pickup_datetime"))
        dataset = lance.write_dataset(
            batch,
            uri,
            mode=mode,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            data_storage_version="stable",
        )
        mode = "append"
        # The fragment this append created is the last one in the manifest.
        fragment = dataset.get_fragments()[-1]
        if fragment.count_rows() != batch.num_rows:
            raise SystemExit(f"error: {month} split into more than one fragment")
        entries.append(
            {
                "fragment": fragment.fragment_id,
                "month": month,
                "rows": batch.num_rows,
                "pickup_min": low,
                "pickup_max": high,
                "pickup_nulls": nulls,
            }
        )
        total += batch.num_rows
        print(
            f"{month}  {batch.num_rows:>9,} rows"
            f"  fragment {fragment.fragment_id:>2}  {time.monotonic() - t0:5.1f}s",
            flush=True,
        )

    elapsed = time.monotonic() - started
    dataset = lance.dataset(uri)
    counted = dataset.count_rows()
    if counted != total:
        raise SystemExit(f"error: dataset has {counted:,} rows, wrote {total:,}")

    size = sum(
        os.path.getsize(os.path.join(dirpath, name))
        for dirpath, _, names in os.walk(uri)
        for name in names
    )
    sidecar = {
        "dataset": os.path.basename(uri),
        "source": warehouse,
        "rows": total,
        "fragments": entries,
        "convert_seconds": round(elapsed, 1),
        "bytes": size,
        "compression": args.compress,
        "lance_version": lance.__version__,
        "data_storage_version": dataset.data_storage_version,
    }
    with open(os.path.join(root, "fragments.json"), "w") as fh:
        json.dump(sidecar, fh, indent=2)

    print(f"\n{total:,} rows in {len(files)} fragments, {elapsed:.1f}s")
    print(f"{size / 1e9:.2f} GB on disk, Lance {lance.__version__}")
    print(f"dataset: {uri}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
