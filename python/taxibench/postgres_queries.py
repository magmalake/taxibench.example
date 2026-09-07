"""The same eight queries, as SQL against the PostgreSQL copy of the table.

Every query here has to return the answer `python/taxibench/queries.py` returns
for the same name, key for key, because `scripts/compare.py` diffs the two
structurally and exits non-zero if they differ. That constraint is what decides
most of the shapes below: q5 and q6 do not `LIMIT 10` in SQL, because the
PyIceberg fold counts *every* row it scanned and then takes the top ten out of
a dense 266-slot accumulator, so the SQL returns one row per zone and the top
ten is picked client-side by the same rule. The group-by collapses 79 million
rows to 267, so shipping them costs nothing and it removes any chance of the
two sides disagreeing about tie-breaking or about which zones are in range.

`count(*)` rides along in every aggregate because `rows_scanned` on the other
two implementations is the number of rows the fold saw, which is the number of
rows that passed the filter. Asking for it in the same pass is what makes the
two comparable without a second scan.

The sums are plain `double precision`. PostgreSQL accumulates them in table
order and, in the parallel leg, per worker and then combined, so the last bits
land differently from pyarrow's SIMD-blocked sum. That is what the harness's
1e-9 relative tolerance is for. `sum(x::numeric)` would be exact and would have
made the question go away, but it would also have been a different and much
slower computation than the one the other two implementations perform, so the
comparison would have stopped being a comparison.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Sequence

# The zone ids the TLC dictionary defines, matching queries.MAX_ZONE. Zones
# outside the range are counted in `rows_scanned` but never enter the top ten,
# which is exactly what the dense accumulator on the PyIceberg side does.
MAX_ZONE = 266

Rows = Sequence[Sequence[Any]]


@dataclass
class PgQuery:
    """One benchmark query: the SQL, and how to shape its rows into an answer."""

    name: str
    title: str
    sql: str
    # rows -> (result, rows_scanned)
    finish: Callable[[Rows], tuple[dict, int]]
    notes: str = ""


def _count_finish(rows: Rows) -> tuple[dict, int]:
    n = int(rows[0][0])
    return {"count": n}, n


def _sum_finish(rows: Rows) -> tuple[dict, int]:
    n = int(rows[0][0])
    # An empty aggregate is NULL in SQL and 0.0 in the other two folds.
    total = float(rows[0][1]) if rows[0][1] is not None else 0.0
    return {"count": n, "sum": total}, n


def _ratio_finish(rows: Rows) -> tuple[dict, int]:
    n = int(rows[0][0])
    tip = float(rows[0][1]) if rows[0][1] is not None else 0.0
    fare = float(rows[0][2]) if rows[0][2] is not None else 0.0
    return (
        {
            "count": n,
            "tip": tip,
            "fare": fare,
            "tip_ratio": tip / fare if fare else 0.0,
        },
        n,
    )


def _top_finish(rows: Rows, value_index: int) -> tuple[dict, int]:
    """Total rows scanned, and the ten highest zones, by the PyIceberg rule.

    The accumulator on the other side is a dense list of floats indexed by
    zone, so a zone outside [0, MAX_ZONE) never appears, a zone whose value is
    not positive never appears, and ties break towards the lower zone id.
    """
    scanned = sum(int(row[1]) for row in rows)
    ranked = [
        (int(row[0]), float(row[value_index]))
        for row in rows
        if row[0] is not None
        and row[value_index] is not None
        and 0 <= int(row[0]) < MAX_ZONE
        and float(row[value_index]) > 0
    ]
    ranked.sort(key=lambda pair: (-pair[1], pair[0]))
    return {"count": scanned, "top": [[z, v] for z, v in ranked[:10]]}, scanned


ALL: list[PgQuery] = [
    PgQuery(
        name="q1_scan_count",
        title="rows with a positive trip distance",
        sql="SELECT count(*) FROM trips WHERE trip_distance > 0",
        finish=_count_finish,
        notes="every partition, one column, nothing to prune",
    ),
    PgQuery(
        name="q2_month_range",
        title="fares over one quarter",
        sql=(
            "SELECT count(*), sum(total_amount) FROM trips"
            " WHERE tpep_pickup_datetime >= TIMESTAMP '2024-03-01 00:00:00'"
            " AND tpep_pickup_datetime < TIMESTAMP '2024-06-01 00:00:00'"
        ),
        finish=_sum_finish,
        notes="partition pruning: 3 of 24 partitions survive",
    ),
    PgQuery(
        name="q3_payment_sum",
        title="cash fares across the whole table",
        sql=(
            "SELECT count(*), sum(total_amount) FROM trips WHERE payment_type = 2"
        ),
        finish=_sum_finish,
        notes="every partition; the predicate cuts about a third",
    ),
    PgQuery(
        name="q4_tip_ratio",
        title="tip ratio on long trips",
        sql=(
            "SELECT count(*), sum(tip_amount), sum(fare_amount) FROM trips"
            " WHERE trip_distance > 10 AND fare_amount > 0"
        ),
        finish=_ratio_finish,
        notes="every partition, two predicates",
    ),
    PgQuery(
        name="q5_top_zones",
        title="busiest pickup zones",
        sql=(
            'SELECT "PULocationID", count(*) FROM trips'
            " WHERE trip_distance > 0 GROUP BY 1"
        ),
        finish=lambda rows: _top_finish(rows, 1),
        notes="every partition, grouped into 266 zones",
    ),
    PgQuery(
        name="q6_zone_revenue",
        title="revenue by pickup zone, 2024",
        sql=(
            'SELECT "PULocationID", count(*), sum(total_amount) FROM trips'
            " WHERE tpep_pickup_datetime >= TIMESTAMP '2024-01-01 00:00:00'"
            " AND tpep_pickup_datetime < TIMESTAMP '2025-01-01 00:00:00'"
            " GROUP BY 1"
        ),
        finish=lambda rows: _top_finish(rows, 2),
        notes="pruning to 12 partitions, then a grouped sum",
    ),
    PgQuery(
        name="q7_wide",
        title="every column of one month",
        sql=(
            "SELECT count(*) FROM trips"
            " WHERE tpep_pickup_datetime >= TIMESTAMP '2024-06-01 00:00:00'"
            " AND tpep_pickup_datetime < TIMESTAMP '2024-07-01 00:00:00'"
        ),
        finish=_count_finish,
        # A row store has no wide-decode leg to this query. The tuple is read
        # whole whether one column is asked for or nineteen, and count(*) does
        # not deform it at all, so PostgreSQL does strictly less work here than
        # the two Iceberg readers, which decode all nineteen columns. The
        # README says so rather than quoting the ratio as if it meant
        # something.
        notes="one partition; in a row store this is not a wide decode",
    ),
    PgQuery(
        name="q8_selective",
        title="long cash trips from one zone",
        sql=(
            "SELECT count(*), sum(total_amount) FROM trips"
            ' WHERE "PULocationID" = 132 AND payment_type = 1'
            " AND trip_distance > 20"
        ),
        finish=_sum_finish,
        notes="highly selective; the one query an index can change",
    ),
]

BY_NAME = {q.name: q for q in ALL}
