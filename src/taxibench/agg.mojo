"""Folds over the Arrow arrays a scan hands back.

iceberg.mojo's kernels cast, fill and filter, and then stop: there is no sum,
no count, no group-by anywhere in the library, because none of that is Iceberg's
job. A benchmark still needs an answer per query, so the eight aggregates the
suite asks for live here.

None of this is a query engine, and it is not trying to be one. Every fold
below reads one or two `ArrayData` buffers straight — a `Float64` load per row,
a validity bit when the column has nulls — which is all the suite's aggregates
need. The point of the study is the scan underneath; this is the small amount of
arithmetic that turns a scan into a number worth printing.

Zones are accumulated into a dense 266-wide array rather than a hash map. The
TLC dictionary numbers its zones 1-265, so the "hash" is the value itself and
the group-by costs one indexed add per row.
"""

from parquet.arrow import (
    AT_FLOAT64,
    AT_INT32,
    ArrayData,
    bit_get,
    load_f64,
    load_i32,
)

comptime MAX_ZONE = 266
"""One past the highest TLC pickup-zone id, so `acc[zone]` is always in range."""


struct Totals(Copyable, Defaultable, Movable):
    """Everything the eight queries accumulate, in one flat value.

    A tagged union per fold kind would be tidier and would cost a branch per
    batch to unpack. There are five kinds and eight queries; a struct with a
    field per accumulator is the cheaper shape and reads no worse.
    """

    var rows: Int
    var sum_a: Float64
    var sum_b: Float64
    var zones: List[Float64]

    def __init__(out self):
        self.rows = 0
        self.sum_a = 0.0
        self.sum_b = 0.0
        self.zones = List[Float64]()

    def __init__(out self, *, copy: Self):
        self.rows = copy.rows
        self.sum_a = copy.sum_a
        self.sum_b = copy.sum_b
        self.zones = copy.zones.copy()

    def __init__(out self, *, deinit move: Self):
        self.rows = move.rows
        self.sum_a = move.sum_a
        self.sum_b = move.sum_b
        self.zones = move.zones^

    def ensure_zones(mut self):
        """Allocate the dense zone accumulator on first use."""
        if len(self.zones) == 0:
            for _ in range(MAX_ZONE):
                self.zones.append(0.0)


def is_double(array: ArrayData) -> Bool:
    """Iceberg's `double` is Arrow's float64."""
    return array.type.id == AT_FLOAT64


def sum_f64(array: ArrayData) raises -> Float64:
    """Sum the non-null values of a double column.

    The null-free case is the common one here — the money columns in the TLC
    data are dense — so it gets a loop with no per-row bit test.
    """
    if not is_double(array):
        raise Error(
            "taxibench: expected a double column, got Arrow type "
            + String(array.type.id)
            + " for '"
            + array.name
            + "'"
        )
    var values = Span(array.values)
    var total = 0.0
    if array.null_count == 0:
        for i in range(array.length):
            total += load_f64(values, i)
        return total
    var validity = Span(array.validity)
    for i in range(array.length):
        if bit_get(validity, i):
            total += load_f64(values, i)
    return total


def zone_count(array: ArrayData, mut totals: Totals) raises:
    """Count rows per pickup zone into the dense accumulator."""
    if array.type.id != AT_INT32:
        raise Error(
            "taxibench: expected an int32 zone column, got Arrow type "
            + String(array.type.id)
        )
    totals.ensure_zones()
    var values = Span(array.values)
    var validity = Span(array.validity)
    var dense = array.null_count == 0
    for i in range(array.length):
        if not dense and not bit_get(validity, i):
            continue
        var zone = Int(load_i32(values, i))
        if zone >= 0 and zone < MAX_ZONE:
            totals.zones[zone] += 1.0


def zone_sum(zones: ArrayData, amounts: ArrayData, mut totals: Totals) raises:
    """Sum a double column per pickup zone into the dense accumulator."""
    if zones.type.id != AT_INT32:
        raise Error(
            "taxibench: expected an int32 zone column, got Arrow type "
            + String(zones.type.id)
        )
    if not is_double(amounts):
        raise Error(
            "taxibench: expected a double amount column, got Arrow type "
            + String(amounts.type.id)
        )
    if zones.length != amounts.length:
        raise Error("taxibench: zone and amount columns differ in length")
    totals.ensure_zones()
    var zone_values = Span(zones.values)
    var zone_validity = Span(zones.validity)
    var amount_values = Span(amounts.values)
    var amount_validity = Span(amounts.validity)
    var zones_dense = zones.null_count == 0
    var amounts_dense = amounts.null_count == 0
    for i in range(zones.length):
        if not zones_dense and not bit_get(zone_validity, i):
            continue
        if not amounts_dense and not bit_get(amount_validity, i):
            continue
        var zone = Int(load_i32(zone_values, i))
        if zone >= 0 and zone < MAX_ZONE:
            totals.zones[zone] += load_f64(amount_values, i)


def top_zones(totals: Totals, k: Int) -> List[Tuple[Int, Float64]]:
    """The k zones with the largest accumulated value, descending.

    A selection sort over 266 slots: k is 10, so this is 2660 comparisons once
    per query, which is not worth a heap.
    """
    var out = List[Tuple[Int, Float64]]()
    if len(totals.zones) == 0:
        return out^
    var taken = List[Bool]()
    for _ in range(MAX_ZONE):
        taken.append(False)
    for _ in range(k):
        var best = -1
        var best_value = 0.0
        for zone in range(MAX_ZONE):
            if taken[zone] or totals.zones[zone] <= 0.0:
                continue
            if best < 0 or totals.zones[zone] > best_value:
                best = zone
                best_value = totals.zones[zone]
        if best < 0:
            break
        taken[best] = True
        out.append((best, best_value))
    return out^
