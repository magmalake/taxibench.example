"""The same eight queries, as Daft DataFrames.

`queries.py` is a scan plus a fold over Arrow batches, which is what a reader
with no engine behind it has to do. These are the same eight questions handed
to an engine instead: Daft does the filtering, the grouping and the summing,
and the only thing that differs between the two runs is where its rows come
from.

That is the whole point of this module. `queries.py` measures two
implementations of Iceberg; these measure **one engine over two scan sources**
— `daft.read_iceberg`, which is Daft's own Rust reader planned through
PyIceberg, and `IcebergLocalSource`, which is the Mojo library handing Daft
Arrow buffers over the C Data Interface. Same plan, same aggregation, same
machine; a different reader underneath.

The answers must come out identical to the other legs', which is why each of
these returns the dict shape `queries.py` returns and `scripts/compare.py`
diffs them the same way.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Callable

from daft import DataFrame, col, lit

# The zone ids the TLC dictionary defines, as in `queries.py`.
MAX_ZONE = 266

# A column that is never null, so `count` counts rows rather than non-nulls.
# Every other leg counts rows, and `count()` on a real column would quietly
# disagree with them wherever that column has nulls.
ONE = "_one"


@dataclass
class DaftQuery:
    """One benchmark query as a Daft plan and the shape of its answer."""

    name: str
    title: str
    run: Callable[[DataFrame], dict]
    notes: str = ""


def _ts(year: int, month: int) -> datetime.datetime:
    """A partition boundary. The literal is a datetime, not a string.

    A string would need a cast to be compared against a timestamp column, and
    a cast is the one thing `daft_flight/predicate.py` will not push down — so
    spelling it this way is what lets the predicate reach Iceberg's planner on
    the source that has one.
    """
    return datetime.datetime(year, month, 1)


def _count(df: DataFrame, predicate) -> dict:
    return {"count": df.where(predicate).count_rows()}


def _sum(df: DataFrame, predicate, column: str) -> dict:
    out = (
        df.where(predicate)
        .with_column(ONE, lit(1))
        .agg(col(ONE).count().alias("n"), col(column).sum().alias("s"))
        .to_pydict()
    )
    return {"count": out["n"][0], "sum": out["s"][0] or 0.0}


def _top_zones(df: DataFrame, predicate, value) -> dict:
    """The dense group-by, folded down to the same ten rows the others report.

    Daft returns every group; `queries.py` accumulates into a dense array and
    takes the ten largest. Counting the rows *here* rather than with a second
    query is what keeps this one execution — the group counts add up to the
    rows that passed the filter, nulls included.
    """
    out = (
        df.where(predicate)
        .with_column(ONE, lit(1))
        .groupby("PULocationID")
        .agg(col(ONE).count().alias("n"), value.alias("v"))
        .to_pydict()
    )
    rows = 0
    zones = []
    for zone, n, total in zip(out["PULocationID"], out["n"], out["v"]):
        rows += n
        if zone is not None and total and 0 <= zone < MAX_ZONE:
            zones.append((float(total), zone))
    # Largest first, and the lower zone id first among equals — the order
    # `heapq.nlargest` over a dense array produces on the other legs.
    zones.sort(key=lambda pair: (-pair[0], pair[1]))
    return {"count": rows, "top": [[zone, total] for total, zone in zones[:10]]}


ALL: list[DaftQuery] = [
    DaftQuery(
        name="q1_scan_count",
        title="rows with a positive trip distance",
        run=lambda df: _count(df, col("trip_distance") > 0),
        notes="full scan, one column; no partition pruning is possible",
    ),
    DaftQuery(
        name="q2_month_range",
        title="fares over one quarter",
        run=lambda df: _sum(
            df,
            (col("tpep_pickup_datetime") >= lit(_ts(2024, 3)))
            & (col("tpep_pickup_datetime") < lit(_ts(2024, 6))),
            "total_amount",
        ),
        notes="partition pruning: 3 of 24 partitions survive",
    ),
    DaftQuery(
        name="q3_payment_sum",
        title="cash fares across the whole table",
        run=lambda df: _sum(df, col("payment_type") == 2, "total_amount"),
        notes="full scan, two columns; the predicate cuts about a third",
    ),
    DaftQuery(
        name="q4_tip_ratio",
        title="tip ratio on long trips",
        run=lambda df: _ratio(df),
        notes="full scan, three columns, two predicates",
    ),
    DaftQuery(
        name="q5_top_zones",
        title="busiest pickup zones",
        run=lambda df: _top_zones(
            df, col("trip_distance") > 0, col(ONE).count().cast("float64")
        ),
        notes="full scan with a dense group-by over 266 zones",
    ),
    DaftQuery(
        name="q6_zone_revenue",
        title="revenue by pickup zone, 2024",
        run=lambda df: _top_zones(
            df,
            (col("tpep_pickup_datetime") >= lit(_ts(2024, 1)))
            & (col("tpep_pickup_datetime") < lit(_ts(2025, 1))),
            col("total_amount").sum(),
        ),
        notes="pruning to 12 partitions, then a grouped sum",
    ),
    DaftQuery(
        name="q7_wide",
        title="every column of one month",
        run=lambda df: _wide(df),
        notes="one partition, all 19 columns: wide decode rather than filtering",
    ),
    DaftQuery(
        name="q8_selective",
        title="long cash trips from one zone",
        run=lambda df: _sum(
            df,
            (col("PULocationID") == 132)
            & (col("payment_type") == 1)
            & (col("trip_distance") > 20),
            "total_amount",
        ),
        notes="highly selective; row-group and page statistics should do the work",
    ),
]


def _ratio(df: DataFrame) -> dict:
    out = (
        df.where((col("trip_distance") > 10) & (col("fare_amount") > 0))
        .with_column(ONE, lit(1))
        .agg(
            col(ONE).count().alias("n"),
            col("tip_amount").sum().alias("tip"),
            col("fare_amount").sum().alias("fare"),
        )
        .to_pydict()
    )
    tip = out["tip"][0] or 0.0
    fare = out["fare"][0] or 0.0
    return {
        "count": out["n"][0],
        "tip": tip,
        "fare": fare,
        "tip_ratio": tip / fare if fare else 0.0,
    }


def _wide(df: DataFrame) -> dict:
    """One month, every column — and every column actually materialised.

    `count_rows()` would be answered from the row count without decoding a
    thing, which is the opposite of what this query is for. The other legs read
    all 19 columns and count; `collect()` is that, in Daft.
    """
    rows = df.where(
        (col("tpep_pickup_datetime") >= lit(_ts(2024, 6)))
        & (col("tpep_pickup_datetime") < lit(_ts(2024, 7)))
    ).collect()
    return {"count": len(rows)}


BY_NAME = {q.name: q for q in ALL}
