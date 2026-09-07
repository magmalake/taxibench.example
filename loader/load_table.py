#!/usr/bin/env python3
"""Build the benchmark table: NYC yellow taxi trips, 2023-2024, as Iceberg v2.

PyIceberg writes it, deliberately. Neither implementation under test creates
the table, so neither gets to choose a layout that happens to suit it, and the
table is one a PyIceberg user would actually have.

The TLC files are not self-consistent, which is the first thing this script has
to deal with. The 2023 months type `VendorID`, `PULocationID`, `DOLocationID`,
`RatecodeID` and `passenger_count` as int64 or double and spell the airport fee
`airport_fee`; the 2024 months narrow those to int32/int64 and spell it
`Airport_fee`. One Iceberg schema has to cover both, so every file is cast to
the schema below before it is appended.

Each month is its own append, so the table ends with 24 snapshots and a real
history to time-travel through. Rows whose pickup timestamp falls outside the
month the file is named for are dropped: every TLC file carries a handful of
them, dated anywhere from 2001 to 2098, and keeping them would scatter dozens
of one-row partitions through the table for no benefit to what is being
measured. The count that was dropped is reported.

Usage: load_table.py <warehouse-dir> <tlc-dir> [--months N]
"""

import argparse
import datetime as dt
import glob
import os
import re
import shutil
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
)

# Field ids are explicit and stable: the Mojo reader resolves projection by
# field id, not by name, so these numbers are part of the contract between the
# two implementations.
SCHEMA = Schema(
    NestedField(1, "VendorID", IntegerType(), required=False),
    NestedField(2, "tpep_pickup_datetime", TimestampType(), required=False),
    NestedField(3, "tpep_dropoff_datetime", TimestampType(), required=False),
    NestedField(4, "passenger_count", IntegerType(), required=False),
    NestedField(5, "trip_distance", DoubleType(), required=False),
    NestedField(6, "RatecodeID", IntegerType(), required=False),
    NestedField(7, "store_and_fwd_flag", StringType(), required=False),
    NestedField(8, "PULocationID", IntegerType(), required=False),
    NestedField(9, "DOLocationID", IntegerType(), required=False),
    NestedField(10, "payment_type", IntegerType(), required=False),
    NestedField(11, "fare_amount", DoubleType(), required=False),
    NestedField(12, "extra", DoubleType(), required=False),
    NestedField(13, "mta_tax", DoubleType(), required=False),
    NestedField(14, "tip_amount", DoubleType(), required=False),
    NestedField(15, "tolls_amount", DoubleType(), required=False),
    NestedField(16, "improvement_surcharge", DoubleType(), required=False),
    NestedField(17, "total_amount", DoubleType(), required=False),
    NestedField(18, "congestion_surcharge", DoubleType(), required=False),
    NestedField(19, "airport_fee", DoubleType(), required=False),
)

# The Arrow schema the table's Parquet files are written with. It mirrors
# SCHEMA field for field; `field_id` metadata is what PyIceberg stamps on the
# way out, so building it here keeps the cast in one place.
ARROW_SCHEMA = pa.schema(
    [
        pa.field("VendorID", pa.int32()),
        pa.field("tpep_pickup_datetime", pa.timestamp("us")),
        pa.field("tpep_dropoff_datetime", pa.timestamp("us")),
        pa.field("passenger_count", pa.int32()),
        pa.field("trip_distance", pa.float64()),
        pa.field("RatecodeID", pa.int32()),
        pa.field("store_and_fwd_flag", pa.string()),
        pa.field("PULocationID", pa.int32()),
        pa.field("DOLocationID", pa.int32()),
        pa.field("payment_type", pa.int32()),
        pa.field("fare_amount", pa.float64()),
        pa.field("extra", pa.float64()),
        pa.field("mta_tax", pa.float64()),
        pa.field("tip_amount", pa.float64()),
        pa.field("tolls_amount", pa.float64()),
        pa.field("improvement_surcharge", pa.float64()),
        pa.field("total_amount", pa.float64()),
        pa.field("congestion_surcharge", pa.float64()),
        pa.field("airport_fee", pa.float64()),
    ]
)

MONTH_RE = re.compile(r"yellow_tripdata_(\d{4})-(\d{2})\.parquet$")


def normalise(table: pa.Table) -> pa.Table:
    """Bring one TLC month onto ARROW_SCHEMA, whichever spelling it arrived in."""
    names = {n.lower(): n for n in table.column_names}
    columns = []
    for field in ARROW_SCHEMA:
        source = names.get(field.name.lower())
        if source is None:
            columns.append(pa.nulls(table.num_rows, type=field.type))
            continue
        column = table.column(source)
        if column.type != field.type:
            # 2023 carries whole numbers in float64 columns; a safe cast
            # rejects those, and there is nothing to lose by truncating.
            column = column.cast(field.type, safe=False)
        columns.append(column)
    return pa.Table.from_arrays(columns, schema=ARROW_SCHEMA)


def in_month(table: pa.Table, year: int, month: int) -> tuple[pa.Table, int]:
    """Drop rows whose pickup falls outside the month the file is named for."""
    lo = dt.datetime(year, month, 1)
    hi = dt.datetime(year + (month == 12), month % 12 + 1, 1)
    pickup = table.column("tpep_pickup_datetime")
    keep = pc.and_(
        pc.greater_equal(pickup, pa.scalar(lo, type=pa.timestamp("us"))),
        pc.less(pickup, pa.scalar(hi, type=pa.timestamp("us"))),
    )
    # A null pickup passes neither test and would be dropped by the filter; be
    # explicit that it is intentional rather than an artefact of null logic.
    keep = pc.fill_null(keep, False)
    filtered = table.filter(keep)
    return filtered, table.num_rows - filtered.num_rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("warehouse")
    ap.add_argument("tlc")
    ap.add_argument("--months", type=int, default=0, help="load only the first N")
    args = ap.parse_args()

    root = os.path.abspath(args.warehouse)
    if os.path.isdir(root):
        shutil.rmtree(root)
    os.makedirs(os.path.join(root, "warehouse"))

    catalog = SqlCatalog(
        "taxi",
        uri=f"sqlite:///{root}/catalog.db",
        warehouse=f"file://{root}/warehouse",
    )
    catalog.create_namespace("taxi")
    spec = PartitionSpec(
        PartitionField(
            source_id=2, field_id=1000, transform=MonthTransform(), name="pickup_month"
        )
    )
    table = catalog.create_table(
        "taxi.trips",
        schema=SCHEMA,
        partition_spec=spec,
        properties={"format-version": "2"},
    )

    files = sorted(glob.glob(os.path.join(args.tlc, "yellow_tripdata_*.parquet")))
    if args.months:
        files = files[: args.months]
    if not files:
        print(f"error: no TLC files in {args.tlc}", file=sys.stderr)
        return 1

    total, dropped_total, started = 0, 0, time.monotonic()
    for path in files:
        match = MONTH_RE.search(path)
        if not match:
            print(f"skipping unrecognised name: {path}", file=sys.stderr)
            continue
        year, month = int(match.group(1)), int(match.group(2))
        t0 = time.monotonic()
        batch = normalise(pq.read_table(path))
        batch, dropped = in_month(batch, year, month)
        table.append(batch)
        total += batch.num_rows
        dropped_total += dropped
        print(
            f"{year}-{month:02d}  {batch.num_rows:>9,} rows"
            f"  (-{dropped:>3} out of month)  {time.monotonic() - t0:5.1f}s",
            flush=True,
        )

    elapsed = time.monotonic() - started
    print(f"\n{total:,} rows in {len(files)} appends, {elapsed:.1f}s")
    print(f"{dropped_total:,} rows dropped as out of month")

    # The Mojo CLI takes a metadata.json path; leave it where the bench script
    # can find it without going through a catalog.
    location = table.metadata_location
    with open(os.path.join(root, "metadata_location.txt"), "w") as fh:
        fh.write(location + "\n")
    print(f"metadata: {location}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
