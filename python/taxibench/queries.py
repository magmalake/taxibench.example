"""The query suite, as PyIceberg scans plus aggregation over Arrow batches.

Eight queries over the trips table. Every one of them has to read data: none
can be answered from manifest counts alone, because a benchmark where one side
returns a metadata lookup and the other reads 79 million rows measures nothing.
That rules out a bare `count(*)`, which PyIceberg answers from the manifests,
and is why Q1 counts rows passing a predicate instead.

Each query is scan, then aggregate over a stream of Arrow record batches. The
stream matters: materialising all of Q4's three columns at once is about 1.9 GB
of Arrow, so both implementations aggregate batch by batch and stay bounded.

The Mojo side runs the same eight, spelled in its own filter DSL. The harness
compares the answers, so the pairing is checked rather than asserted.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Callable

import pyarrow as pa
import pyarrow.compute as pc

# The zone ids the TLC dictionary defines; the group-by accumulators are dense
# arrays of this length, which is what makes them cheap on both sides.
MAX_ZONE = 266


@dataclass
class Query:
    """One benchmark query: what to scan, and how to fold the batches."""

    name: str
    title: str
    row_filter: str
    columns: tuple[str, ...]
    # (state, batch) -> state. State starts at `init()`.
    fold: Callable[[dict, pa.RecordBatch], dict]
    init: Callable[[], dict]
    finish: Callable[[dict], dict]
    notes: str = ""


def _count_init() -> dict:
    return {"n": 0}


def _count_fold(state: dict, batch: pa.RecordBatch) -> dict:
    state["n"] += batch.num_rows
    return state


def _count_finish(state: dict) -> dict:
    return {"count": state["n"]}


def _sum_init() -> dict:
    return {"n": 0, "sum": 0.0}


def _sum_fold_on(column: str):
    def fold(state: dict, batch: pa.RecordBatch) -> dict:
        state["n"] += batch.num_rows
        total = pc.sum(batch.column(column))
        if total.is_valid:
            state["sum"] += total.as_py()
        return state

    return fold


def _sum_finish(state: dict) -> dict:
    # Full precision, deliberately. The two implementations sum the same
    # doubles in a different order, so the last bits differ; the harness
    # compares with a relative tolerance rather than pretending they match
    # exactly, and rounding here would hide the size of the disagreement.
    return {"count": state["n"], "sum": state["sum"]}


def _ratio_init() -> dict:
    return {"n": 0, "tip": 0.0, "fare": 0.0}


def _ratio_fold(state: dict, batch: pa.RecordBatch) -> dict:
    state["n"] += batch.num_rows
    tip = pc.sum(batch.column("tip_amount"))
    fare = pc.sum(batch.column("fare_amount"))
    if tip.is_valid:
        state["tip"] += tip.as_py()
    if fare.is_valid:
        state["fare"] += fare.as_py()
    return state


def _ratio_finish(state: dict) -> dict:
    ratio = state["tip"] / state["fare"] if state["fare"] else 0.0
    return {
        "count": state["n"],
        "tip": state["tip"],
        "fare": state["fare"],
        "tip_ratio": ratio,
    }


def _zone_init() -> dict:
    return {"n": 0, "acc": [0.0] * MAX_ZONE}


def _zone_count_fold(state: dict, batch: pa.RecordBatch) -> dict:
    state["n"] += batch.num_rows
    # value_counts returns each distinct zone once per batch, which is far
    # less work than touching all 3M rows in Python.
    counts = pc.value_counts(batch.column("PULocationID"))
    acc = state["acc"]
    for pair in counts:
        zone = pair["values"].as_py()
        if zone is not None and 0 <= zone < MAX_ZONE:
            acc[zone] += pair["counts"].as_py()
    return state


def _zone_sum_fold(state: dict, batch: pa.RecordBatch) -> dict:
    state["n"] += batch.num_rows
    table = pa.Table.from_batches([batch])
    grouped = table.group_by("PULocationID").aggregate([("total_amount", "sum")])
    zones = grouped.column("PULocationID")
    sums = grouped.column("total_amount_sum")
    acc = state["acc"]
    for zone, total in zip(zones.to_pylist(), sums.to_pylist()):
        if zone is not None and total is not None and 0 <= zone < MAX_ZONE:
            acc[zone] += total
    return state


def _zone_finish(state: dict) -> dict:
    acc = state["acc"]
    top = heapq.nlargest(10, range(MAX_ZONE), key=lambda z: acc[z])
    return {
        "count": state["n"],
        "top": [[z, acc[z]] for z in top if acc[z] > 0],
    }


ALL: list[Query] = [
    Query(
        name="q1_scan_count",
        title="rows with a positive trip distance",
        row_filter="trip_distance > 0",
        columns=("trip_distance",),
        init=_count_init,
        fold=_count_fold,
        finish=_count_finish,
        notes="full scan, one column; no partition pruning is possible",
    ),
    Query(
        name="q2_month_range",
        title="fares over one quarter",
        row_filter=(
            "tpep_pickup_datetime >= '2024-03-01T00:00:00' "
            "and tpep_pickup_datetime < '2024-06-01T00:00:00'"
        ),
        columns=("total_amount",),
        init=_sum_init,
        fold=_sum_fold_on("total_amount"),
        finish=_sum_finish,
        notes="partition pruning: 3 of 24 partitions survive",
    ),
    Query(
        name="q3_payment_sum",
        title="cash fares across the whole table",
        row_filter="payment_type = 2",
        columns=("payment_type", "total_amount"),
        init=_sum_init,
        fold=_sum_fold_on("total_amount"),
        finish=_sum_finish,
        notes="full scan, two columns; the predicate cuts about a third",
    ),
    Query(
        name="q4_tip_ratio",
        title="tip ratio on long trips",
        row_filter="trip_distance > 10 and fare_amount > 0",
        columns=("trip_distance", "fare_amount", "tip_amount"),
        init=_ratio_init,
        fold=_ratio_fold,
        finish=_ratio_finish,
        notes="full scan, three columns, two predicates",
    ),
    Query(
        name="q5_top_zones",
        title="busiest pickup zones",
        row_filter="trip_distance > 0",
        columns=("PULocationID", "trip_distance"),
        init=_zone_init,
        fold=_zone_count_fold,
        finish=_zone_finish,
        notes="full scan with a dense group-by over 266 zones",
    ),
    Query(
        name="q6_zone_revenue",
        title="revenue by pickup zone, 2024",
        row_filter=(
            "tpep_pickup_datetime >= '2024-01-01T00:00:00' "
            "and tpep_pickup_datetime < '2025-01-01T00:00:00'"
        ),
        columns=("PULocationID", "total_amount"),
        init=_zone_init,
        fold=_zone_sum_fold,
        finish=_zone_finish,
        notes="pruning to 12 partitions, then a grouped sum",
    ),
    Query(
        name="q7_wide",
        title="every column of one month",
        row_filter=(
            "tpep_pickup_datetime >= '2024-06-01T00:00:00' "
            "and tpep_pickup_datetime < '2024-07-01T00:00:00'"
        ),
        columns=(),  # all 19
        init=_count_init,
        fold=_count_fold,
        finish=_count_finish,
        notes="one partition, all 19 columns: wide decode rather than filtering",
    ),
    Query(
        name="q8_selective",
        title="long cash trips from one zone",
        row_filter="PULocationID = 132 and payment_type = 1 and trip_distance > 20",
        columns=("PULocationID", "payment_type", "trip_distance", "total_amount"),
        init=_sum_init,
        fold=_sum_fold_on("total_amount"),
        finish=_sum_finish,
        notes="highly selective; row-group and page statistics should do the work",
    ),
]

BY_NAME = {q.name: q for q in ALL}
