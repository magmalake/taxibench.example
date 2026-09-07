"""The same eight queries, as Lance scans over the transcoded dataset.

**The aggregation is not reimplemented here.** The folds come from
`taxibench.queries` — the PyIceberg leg's own code, which needs nothing but
pyarrow to import — so the only thing that differs between the two Python legs
is how the Arrow batches are produced. A third fold, written slightly
differently, would put its own arithmetic into every number and make the
comparison about the aggregation instead of about the reader.

What this module supplies is the other half: the scan. Two things about Lance
make that more than a translation of the PyIceberg filter strings.

**Lance has no partitioning.** A dataset is a flat list of fragments and the
format records nothing about what is in one; there is no partition value to
compare a predicate against and no manifest summary to prune with. The
equivalent knowledge is written out at conversion time — `fragments.json`
records each fragment's month and its pickup-timestamp bounds — and `plan`
below does the pruning against it. Iceberg maintains that for itself, from the
partition spec; this leg is handed it. That is the one place where the Lance
side is given something it did not have to build, and it is why q2, q6 and q7
report 3, 12 and 1 files rather than 24.

**A covered fragment needs no predicate.** When a fragment's pickup bounds sit
wholly inside the query's range, every row in it qualifies, so the filter is
dropped and the timestamp column is never read — the same reduction Iceberg's
residual evaluator performs when a predicate is implied by the partition value.
All three month-range queries here are month-aligned, so every surviving
fragment is covered and no residual survives; the partial case is implemented
anyway, because a range that split a month would otherwise silently return the
wrong rows.

Filters are DataFusion SQL. Identifiers resolve case-insensitively, so
`PULocationID` needs no quoting, but it is backquoted for the reader's sake:
double quotes are string literals in this dialect, not identifiers.
"""

from __future__ import annotations

import datetime as dt
import json
import os
from dataclasses import dataclass

from taxibench.queries import BY_NAME as ICEBERG_BY_NAME
from taxibench.queries import Query


@dataclass(frozen=True)
class Scan:
    """How the Lance leg reads one query: a projection, and what to prune with.

    `predicate` is applied to every fragment that is not fully covered by
    `pickup`; `pickup` is a half-open `[low, high)` range on
    `tpep_pickup_datetime`, expressed the way the query means it rather than as
    a string to be re-parsed per fragment.
    """

    columns: tuple[str, ...]
    predicate: str | None = None
    pickup: tuple[str, str] | None = None

    def timestamp_filter(self) -> str | None:
        """The pickup range as SQL, for a fragment the range only partly covers."""
        if self.pickup is None:
            return None
        low, high = self.pickup
        return (
            f"tpep_pickup_datetime >= timestamp '{low}' "
            f"and tpep_pickup_datetime < timestamp '{high}'"
        )


# Keyed by the query names the suite already uses. The projections match the
# PyIceberg leg's `selected_fields` column for column, including the columns
# that are only there to be filtered on: reading four columns to sum one is
# part of what q8 measures.
SCANS: dict[str, Scan] = {
    "q1_scan_count": Scan(
        columns=("trip_distance",),
        predicate="trip_distance > 0",
    ),
    "q2_month_range": Scan(
        columns=("total_amount",),
        pickup=("2024-03-01T00:00:00", "2024-06-01T00:00:00"),
    ),
    "q3_payment_sum": Scan(
        columns=("payment_type", "total_amount"),
        predicate="payment_type = 2",
    ),
    "q4_tip_ratio": Scan(
        columns=("trip_distance", "fare_amount", "tip_amount"),
        predicate="trip_distance > 10 and fare_amount > 0",
    ),
    "q5_top_zones": Scan(
        columns=("PULocationID", "trip_distance"),
        predicate="trip_distance > 0",
    ),
    "q6_zone_revenue": Scan(
        columns=("PULocationID", "total_amount"),
        pickup=("2024-01-01T00:00:00", "2025-01-01T00:00:00"),
    ),
    "q7_wide": Scan(
        columns=(),  # all 19
        pickup=("2024-06-01T00:00:00", "2024-07-01T00:00:00"),
    ),
    "q8_selective": Scan(
        columns=("PULocationID", "payment_type", "trip_distance", "total_amount"),
        predicate="`PULocationID` = 132 and payment_type = 1 and trip_distance > 20",
    ),
}

ALL: list[Query] = [ICEBERG_BY_NAME[name] for name in SCANS]
BY_NAME: dict[str, Query] = {q.name: q for q in ALL}


def load_sidecar(root: str) -> dict:
    """Read `fragments.json`, the fragment-to-month map the converter wrote."""
    path = os.path.join(root, "fragments.json")
    if not os.path.isfile(path):
        raise SystemExit(
            f"error: no fragments.json under {root} — "
            "run scripts/convert-lance.sh to build the dataset"
        )
    with open(path) as fh:
        return json.load(fh)


def _parse(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value)


def plan(scan: Scan, entries: list[dict]) -> list[tuple[list[int], str | None]]:
    """Prune to the fragments a query can touch, and reduce its filter on each.

    Returns groups of `(fragment ids, filter)`. A query with no pickup range
    reads every fragment under its own predicate; a month-range query reads
    only the fragments whose bounds overlap the range, and a fragment the range
    covers entirely is read with no filter at all. The groups are separate
    scans because Lance takes one filter per scanner, so a range that cut
    through a month would produce two.
    """
    if scan.pickup is None:
        return [([entry["fragment"] for entry in entries], scan.predicate)]

    low, high = _parse(scan.pickup[0]), _parse(scan.pickup[1])
    covered: list[int] = []
    partial: list[int] = []
    for entry in entries:
        lowest, highest = _parse(entry["pickup_min"]), _parse(entry["pickup_max"])
        if highest < low or lowest >= high:
            continue
        # A null pickup satisfies no comparison, so it would be dropped by the
        # filter but kept by a covered fragment read without one. The converter
        # records the count; the loader leaves none behind, and this is what
        # makes depending on that safe rather than lucky.
        if lowest >= low and highest < high and not entry["pickup_nulls"]:
            covered.append(entry["fragment"])
        else:
            partial.append(entry["fragment"])

    groups = []
    if covered:
        groups.append((covered, scan.predicate))
    if partial:
        residual = scan.timestamp_filter()
        if scan.predicate:
            residual = f"({scan.predicate}) and ({residual})"
        groups.append((partial, residual))
    return groups
