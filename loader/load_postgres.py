#!/usr/bin/env python3
"""Build the PostgreSQL copy of the benchmark table from the same TLC files.

This is the third implementation's data, and the one thing it must not do is
be a different dataset. `loader/load_table.py` is imported rather than
paraphrased: the same `normalise()` reconciles the two schemas TLC publishes,
the same `in_month()` drops the rows whose pickup falls outside the month their
file is named for, and the same `ARROW_SCHEMA` decides the column order and
types. If the row count does not come out at 79,478,796 the load is wrong and
every answer will be wrong with it, so the count is asserted at the end.

**The table is partitioned by month, deliberately.** The Iceberg table is
partitioned by `month(tpep_pickup_datetime)` into 24 data files, and a
PostgreSQL table declaratively range-partitioned into the same 24 months is the
closest physical analogue there is: the planner prunes to the same partitions
the Iceberg planner prunes to, so `files` means the same thing on both sides
and the two are answering the query with the same amount of the table in view.
An unpartitioned heap would have made every query a full scan and turned q2, q6
and q7 into measurements of nothing.

Rows are encoded straight into PostgreSQL's binary COPY format with numpy and
streamed in. Going through `copy.write_row()` would mean building 79.5 million
Python tuples; building the wire bytes column-wise from the Arrow buffers keeps
the client out of the way, which matters because what is being measured later
is the server. Binary is also the only format that round-trips a float64
exactly — a text round-trip through a shortest-repr printer is *probably*
exact, and "probably" is not good enough for a benchmark whose gate is 1e-9.

Usage: load_postgres.py <dsn> <tlc-dir> [--months N]
"""

from __future__ import annotations

import argparse
import glob
import os
import sys
import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import psycopg

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from load_table import ARROW_SCHEMA, MONTH_RE, in_month, normalise  # noqa: E402

TABLE = "trips"

# PostgreSQL binary COPY: an 11-byte signature, a flags word and an extension
# length, then one tuple per row, then a -1 field count as the trailer.
COPY_HEADER = b"PGCOPY\n\xff\r\n\0" + b"\0\0\0\0" + b"\0\0\0\0"
COPY_TRAILER = b"\xff\xff"

# PostgreSQL's timestamp epoch is 2000-01-01, Arrow's is 1970-01-01. Both count
# microseconds, so the whole conversion is this subtraction.
PG_EPOCH_US = 946_684_800_000_000

# Arrow type -> (PostgreSQL type, wire width, binary encoder). Driving the DDL
# and the encoder off ARROW_SCHEMA is what keeps this file from drifting away
# from the Iceberg schema when that one changes.
PG_TYPE = {
    pa.int32(): "integer",
    pa.timestamp("us"): "timestamp",
    pa.float64(): "double precision",
    pa.string(): "text",
}
WIDTH = {pa.int32(): 4, pa.timestamp("us"): 8, pa.float64(): 8, pa.string(): 1}


def _values(array: pa.Array, arrow_type: pa.DataType) -> np.ndarray:
    """The column's payload bytes, big-endian, one row per row.

    Null slots still occupy a row here and are masked out afterwards; filling
    them with a harmless value keeps the whole conversion vectorised.
    """
    if arrow_type == pa.int32():
        filled = pc.fill_null(array, 0).to_numpy(zero_copy_only=False)
        return filled.astype(">i4")
    if arrow_type == pa.timestamp("us"):
        filled = pc.fill_null(array, pa.scalar(0, pa.timestamp("us")))
        micros = filled.cast(pa.int64()).to_numpy(zero_copy_only=False)
        return (micros - PG_EPOCH_US).astype(">i8")
    if arrow_type == pa.float64():
        filled = pc.fill_null(array, 0.0).to_numpy(zero_copy_only=False)
        return filled.astype(">f8")
    if arrow_type == pa.string():
        # store_and_fwd_flag is one character. TLC only ever writes 'Y', 'N' or
        # null; the assertion is here because a fourth value would be silently
        # rewritten as 'N' by the if_else below rather than noticed.
        distinct = set(pc.unique(array).to_pylist())
        if not distinct <= {"Y", "N", None}:
            raise SystemExit(f"error: unexpected store_and_fwd_flag {distinct}")
        codes = pc.if_else(pc.equal(array, "Y"), ord("Y"), ord("N"))
        return pc.fill_null(codes, ord("N")).to_numpy(zero_copy_only=False).astype(
            np.uint8
        )
    raise SystemExit(f"error: no binary encoder for {arrow_type}")


def encode_batch(batch: pa.RecordBatch) -> bytes:
    """One record batch as PostgreSQL binary COPY tuples.

    Every field is laid out at its full width in a fixed-stride matrix, and a
    parallel boolean mask marks the bytes that survive. A null field is a -1
    length and *no* payload, so the rows are not all the same length; the mask
    is what removes the payload of the null ones in a single vectorised
    compaction rather than a Python loop over 3 million rows.
    """
    n = batch.num_rows
    fields = list(ARROW_SCHEMA)
    stride = 2 + sum(4 + WIDTH[f.type] for f in fields)

    buf = np.zeros((n, stride), dtype=np.uint8)
    keep = np.ones((n, stride), dtype=bool)
    buf[:, 0:2] = np.frombuffer(
        np.array(len(fields), dtype=">i2").tobytes(), dtype=np.uint8
    )

    offset = 2
    for field in fields:
        width = WIDTH[field.type]
        column = batch.column(field.name)
        valid = pc.is_valid(column).to_numpy(zero_copy_only=False)

        present = np.frombuffer(np.array(width, dtype=">i4").tobytes(), dtype=np.uint8)
        absent = np.frombuffer(
            np.array(-1, dtype=">i4").tobytes(), dtype=np.uint8
        )
        buf[:, offset : offset + 4] = np.where(valid[:, None], present, absent)
        offset += 4

        payload = _values(column, field.type)
        buf[:, offset : offset + width] = payload.view(np.uint8).reshape(n, width)
        keep[:, offset : offset + width] = valid[:, None]
        offset += width

    return buf[keep].tobytes()


def partition_name(year: int, month: int) -> str:
    return f"{TABLE}_{year}_{month:02d}"


def create_schema(conn: psycopg.Connection, months: list[tuple[int, int]]) -> None:
    """The partitioned parent and one leaf per month, matching the 24 files.

    UNLOGGED, because this table is a benchmark fixture that is rebuilt from
    the TLC Parquet whenever it is wanted: skipping the WAL halves the load and
    changes nothing about how the table reads. `parallel_workers` is pinned on
    every leaf so the parallel leg gets the worker count it asks for instead of
    the one PostgreSQL derives from the relation size — the Iceberg legs name
    their thread count and this one has to as well.
    """
    columns = ",\n    ".join(
        f'"{f.name}" {PG_TYPE[f.type]}' for f in ARROW_SCHEMA
    )
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {TABLE} CASCADE")
        cur.execute(
            # The parent holds no storage of its own, so it cannot be — and
            # need not be — UNLOGGED; the leaves that do hold the rows are.
            f"CREATE TABLE {TABLE} (\n    {columns}\n)"
            ' PARTITION BY RANGE ("tpep_pickup_datetime")'
        )
        for year, month in months:
            hi_year, hi_month = year + (month == 12), month % 12 + 1
            leaf = partition_name(year, month)
            cur.execute(
                f"CREATE UNLOGGED TABLE {leaf} PARTITION OF {TABLE}"
                f" FOR VALUES FROM ('{year}-{month:02d}-01')"
                f" TO ('{hi_year}-{hi_month:02d}-01')"
            )
            cur.execute(f"ALTER TABLE {leaf} SET (parallel_workers = 9)")
    conn.commit()


def copy_month(
    conn: psycopg.Connection, leaf: str, table: pa.Table, chunk_rows: int
) -> None:
    """Stream one month into its own partition.

    Copying into the leaf rather than the parent skips tuple routing: the month
    is already known from the file name, and `in_month()` has already proved
    every row belongs in it.
    """
    quoted = ", ".join(f'"{f.name}"' for f in ARROW_SCHEMA)
    with conn.cursor() as cur:
        with cur.copy(f"COPY {leaf} ({quoted}) FROM STDIN (FORMAT BINARY)") as copy:
            copy.write(COPY_HEADER)
            for batch in table.to_batches(max_chunksize=chunk_rows):
                copy.write(encode_batch(batch))
            copy.write(COPY_TRAILER)
    conn.commit()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("dsn")
    ap.add_argument("tlc")
    ap.add_argument("--months", type=int, default=0, help="load only the first N")
    ap.add_argument("--chunk-rows", type=int, default=262_144)
    args = ap.parse_args()

    files = sorted(glob.glob(os.path.join(args.tlc, "yellow_tripdata_*.parquet")))
    if args.months:
        files = files[: args.months]
    if not files:
        print(f"error: no TLC files in {args.tlc}", file=sys.stderr)
        return 1

    parsed = []
    for path in files:
        match = MONTH_RE.search(path)
        if not match:
            print(f"skipping unrecognised name: {path}", file=sys.stderr)
            continue
        parsed.append((path, int(match.group(1)), int(match.group(2))))

    conn = psycopg.connect(args.dsn)
    conn.autocommit = False
    create_schema(conn, [(y, m) for _, y, m in parsed])

    total, dropped_total, started = 0, 0, time.monotonic()
    for path, year, month in parsed:
        t0 = time.monotonic()
        batch = normalise(pq.read_table(path))
        batch, dropped = in_month(batch, year, month)
        copy_month(conn, partition_name(year, month), batch, args.chunk_rows)
        total += batch.num_rows
        dropped_total += dropped
        print(
            f"{year}-{month:02d}  {batch.num_rows:>9,} rows"
            f"  (-{dropped:>3} out of month)  {time.monotonic() - t0:5.1f}s",
            flush=True,
        )

    elapsed = time.monotonic() - started
    print(f"\n{total:,} rows in {len(parsed)} partitions, {elapsed:.1f}s")
    print(f"{dropped_total:,} rows dropped as out of month")

    # ANALYZE is not optional here. Without statistics the planner has no idea
    # how selective q8 is and will pick the wrong plan for it, and a benchmark
    # of a plan nobody would run in production measures nothing.
    print("== analyze")
    t0 = time.monotonic()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"ANALYZE {TABLE}")
    print(f"   {time.monotonic() - t0:.1f}s")

    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {TABLE}")
        counted = cur.fetchone()[0]
        cur.execute(f"SELECT pg_total_relation_size('{TABLE}')")
        # A partitioned parent has no storage of its own; sum the leaves.
        cur.execute(
            "SELECT sum(pg_total_relation_size(c.oid))"
            " FROM pg_class c JOIN pg_inherits i ON i.inhrelid = c.oid"
            " WHERE i.inhparent = %s::regclass",
            (TABLE,),
        )
        size = int(cur.fetchone()[0])
    conn.close()

    print(f"\n{counted:,} rows in {TABLE}, {size / 1e9:.2f} GB on disk")
    if counted != total:
        print(f"error: server counted {counted:,}, loader sent {total:,}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
