"""`taxibench` — run the query suite through iceberg.mojo.

    taxibench <table-dir-or-metadata.json> [options]

    --query NAME     run only this query (repeatable)
    --repeat N       timed runs per query; p50 is reported, p90 and min beside
    --warmup         discard one run before timing
    --workers N      data files read in parallel (0 = one per core, default 1)
    --snapshot ID    read this snapshot instead of the current one
    --out PATH       write JSON lines here instead of stdout

One JSON object per query, the same shape `python -m taxibench` emits, so the
two can be diffed field for field. That diff is the correctness gate: the
numbers below are only worth reading if both implementations agree on what the
answers are.
"""

from std.sys import argv
from std.time import perf_counter_ns

from iceberg.catalog.filesystem import Table, find_latest_metadata
from iceberg.io import FileIO
from iceberg.metadata import TableMetadata
from iceberg.read import ScanOptions
from iceberg.scan import TableScan
from parquet.reader import RecordBatch

from taxibench.agg import Totals, sum_f64, top_zones, zone_count, zone_sum
from taxibench.queries import (
    K_COUNT,
    K_RATIO,
    K_SUM,
    K_ZONE_COUNT,
    K_ZONE_SUM,
    Query,
    all_queries,
)


comptime USAGE = String(
    "taxibench — the NYC taxi query suite over iceberg.mojo\n"
    "\n"
    "usage:\n"
    "  taxibench <table> [--query NAME] [--repeat N] [--warmup]\n"
    "            [--workers N] [--snapshot ID] [--out PATH]\n"
    "\n"
    "<table> is a metadata.json, a table directory, or a warehouse root\n"
    "containing metadata_location.txt.\n"
)


def read_text(path: String) raises -> String:
    with open(path, "r") as handle:
        return handle.read()


def strip_scheme(location: String) -> String:
    """`file:///x` -> `/x`; anything else is returned unchanged."""
    if location.startswith("file://"):
        return String(location[byte=7 : location.byte_length()])
    return location.copy()


def resolve_metadata(io: FileIO, target: String) raises -> String:
    """Accept a metadata.json, a table directory, or a warehouse root."""
    if target.endswith(".json"):
        return target.copy()
    var pointer = target + "/metadata_location.txt"
    try:
        var location = read_text(pointer).strip()
        return strip_scheme(String(location))
    except:
        pass
    return find_latest_metadata(io, target)


def json_escape(text: String) -> String:
    var out = String()
    for slice in text.codepoint_slices():
        if slice == '"':
            out += '\\"'
        elif slice == "\\":
            out += "\\\\"
        elif slice == "\n":
            out += "\\n"
        else:
            out += String(slice)
    return out^


def json_float(value: Float64) -> String:
    """Enough digits that a reader gets the same double back."""
    return String(value)


def ms_from_ns(ns: Int) -> Float64:
    return Float64(ns) / 1.0e6


def sorted_ascending(var samples: List[Int]) -> List[Int]:
    """Insertion sort. `repeat` is a handful of runs, so nothing else is warranted.
    """
    for i in range(1, len(samples)):
        var value = samples[i]
        var j = i - 1
        while j >= 0 and samples[j] > value:
            samples[j + 1] = samples[j]
            j -= 1
        samples[j + 1] = value
    return samples^


def percentile_ns(sorted_samples: List[Int], numerator: Int, denominator: Int) -> Int:
    """Nearest-rank percentile over an already-sorted list.

    p50 is the headline and p90 sits beside it: a mean over an unbounded tail
    measures how the machine felt rather than how the code performs, and the
    minimum is only honest when labelled as a floor.
    """
    if len(sorted_samples) == 0:
        return 0
    var last = len(sorted_samples) - 1
    var index = (numerator * last + denominator // 2) // denominator
    if index > last:
        index = last
    return sorted_samples[index]


def column_named(batch: RecordBatch, name: String) raises -> Int:
    """Index into `batch.roots` of the column with this name."""
    for i in range(len(batch.roots)):
        if batch.arena.nodes[batch.roots[i]].name == name:
            return i
    raise Error("taxibench: the scan returned no column named '" + name + "'")


def fold_batch(query: Query, batch: RecordBatch, mut totals: Totals) raises:
    """Accumulate one record batch into `totals`, per the query's fold kind."""
    totals.rows += batch.num_rows
    if query.kind == K_COUNT:
        return
    if query.kind == K_SUM:
        ref column = batch.arena.nodes[
            batch.roots[column_named(batch, query.sum_column)]
        ]
        totals.sum_a += sum_f64(column)
        return
    if query.kind == K_RATIO:
        ref tip = batch.arena.nodes[
            batch.roots[column_named(batch, String("tip_amount"))]
        ]
        ref fare = batch.arena.nodes[
            batch.roots[column_named(batch, String("fare_amount"))]
        ]
        totals.sum_a += sum_f64(tip)
        totals.sum_b += sum_f64(fare)
        return
    if query.kind == K_ZONE_COUNT:
        ref zones = batch.arena.nodes[
            batch.roots[column_named(batch, String("PULocationID"))]
        ]
        zone_count(zones, totals)
        return
    if query.kind == K_ZONE_SUM:
        ref zones = batch.arena.nodes[
            batch.roots[column_named(batch, String("PULocationID"))]
        ]
        ref amounts = batch.arena.nodes[
            batch.roots[column_named(batch, String("total_amount"))]
        ]
        zone_sum(zones, amounts, totals)
        return
    raise Error("taxibench: unknown fold kind " + String(query.kind))


def result_json(query: Query, totals: Totals) raises -> String:
    """The `result` object, shaped exactly as the Python side emits it."""
    if query.kind == K_COUNT:
        return String('{"count": ', totals.rows, "}")
    if query.kind == K_SUM:
        return String(
            '{"count": ', totals.rows, ', "sum": ', json_float(totals.sum_a), "}"
        )
    if query.kind == K_RATIO:
        var ratio = 0.0
        if totals.sum_b != 0.0:
            ratio = totals.sum_a / totals.sum_b
        return String(
            '{"count": ',
            totals.rows,
            ', "tip": ',
            json_float(totals.sum_a),
            ', "fare": ',
            json_float(totals.sum_b),
            ', "tip_ratio": ',
            json_float(ratio),
            "}",
        )
    if query.kind == K_ZONE_COUNT or query.kind == K_ZONE_SUM:
        var top = top_zones(totals, 10)
        var out = String('{"count": ', totals.rows, ', "top": [')
        for i in range(len(top)):
            if i > 0:
                out += ", "
            out += String("[", top[i][0], ", ", json_float(top[i][1]), "]")
        out += "]}"
        return out^
    raise Error("taxibench: unknown fold kind " + String(query.kind))


struct Run(Copyable, Movable):
    """What one timed execution of a query produced."""

    var files: Int
    var plan_ns: Int
    var total_ns: Int
    var totals: Totals

    def __init__(out self, files: Int, plan_ns: Int, total_ns: Int, var totals: Totals):
        self.files = files
        self.plan_ns = plan_ns
        self.total_ns = total_ns
        self.totals = totals^

    def __init__(out self, *, copy: Self):
        self.files = copy.files
        self.plan_ns = copy.plan_ns
        self.total_ns = copy.total_ns
        self.totals = copy.totals.copy()

    def __init__(out self, *, deinit move: Self):
        self.files = move.files
        self.plan_ns = move.plan_ns
        self.total_ns = move.total_ns
        self.totals = move.totals^


def build_scan(
    metadata: TableMetadata, io: FileIO, query: Query, snapshot: Int64
) raises -> TableScan:
    var scan = TableScan(metadata.copy(), io.copy()).filter(query.filter)
    if len(query.columns) > 0:
        scan = scan.select(query.columns.copy())
    if snapshot >= 0:
        scan = scan.use_snapshot(snapshot)
    return scan^


def run_once(
    metadata: TableMetadata,
    io: FileIO,
    query: Query,
    options: ScanOptions,
    snapshot: Int64,
) raises -> Run:
    var planning = build_scan(metadata, io, query, snapshot)
    var t0 = perf_counter_ns()
    var tasks = planning.plan_files()
    var plan_ns = perf_counter_ns() - t0

    # Planning happens again inside the read. `plan_ns` is reported beside the
    # total rather than subtracted from it, which is what the Python side does.
    var scan = build_scan(metadata, io, query, snapshot)
    var t1 = perf_counter_ns()
    var batches = scan.to_batches(options)
    var totals = Totals()
    for i in range(len(batches)):
        fold_batch(query, batches[i], totals)
    var total_ns = perf_counter_ns() - t1

    return Run(len(tasks), plan_ns, total_ns, totals^)


def main() raises:
    var args = argv()
    if len(args) < 2:
        print(USAGE)
        return

    var target = String(args[1])
    var wanted = List[String]()
    var repeat = 1
    var warmup = False
    var workers = 1
    var snapshot = Int64(-1)
    var out_path = String("-")

    var i = 2
    while i < len(args):
        var flag = String(args[i])
        if flag == "--query" and i + 1 < len(args):
            wanted.append(String(args[i + 1]))
            i += 2
        elif flag == "--repeat" and i + 1 < len(args):
            repeat = Int(String(args[i + 1]))
            i += 2
        elif flag == "--workers" and i + 1 < len(args):
            workers = Int(String(args[i + 1]))
            i += 2
        elif flag == "--snapshot" and i + 1 < len(args):
            snapshot = Int64(Int(String(args[i + 1])))
            i += 2
        elif flag == "--out" and i + 1 < len(args):
            out_path = String(args[i + 1])
            i += 2
        elif flag == "--warmup":
            warmup = True
            i += 1
        else:
            raise Error("taxibench: unexpected argument '" + flag + "'")

    var io = FileIO.local()
    var metadata_path = resolve_metadata(io, target)
    var metadata = TableMetadata.parse(read_text(metadata_path))

    var options = ScanOptions()
    options.num_workers = workers

    var suite = all_queries()
    var selected = List[Query]()
    if len(wanted) == 0:
        selected = suite.copy()
    else:
        for name in wanted:
            var found = False
            for query in suite:
                if query.name == name:
                    selected.append(query.copy())
                    found = True
            if not found:
                raise Error("taxibench: unknown query '" + name + "'")

    var lines = String()
    for query in selected:
        if warmup:
            _ = run_once(metadata, io, query, options, snapshot)
        var best = run_once(metadata, io, query, options, snapshot)
        var totals = List[Int]()
        var plans = List[Int]()
        totals.append(best.total_ns)
        plans.append(best.plan_ns)
        for _ in range(repeat - 1):
            var candidate = run_once(metadata, io, query, options, snapshot)
            totals.append(candidate.total_ns)
            plans.append(candidate.plan_ns)
        var sorted_totals = sorted_ascending(totals^)
        var sorted_plans = sorted_ascending(plans^)
        var record = String(
            '{"query": "',
            json_escape(query.name),
            '", "title": "',
            json_escape(query.title),
            '", "engine": "iceberg.mojo", "files": ',
            best.files,
            ', "rows_scanned": ',
            best.totals.rows,
            ', "plan_ms": ',
            json_float(ms_from_ns(percentile_ns(sorted_plans, 50, 100))),
            ', "total_ms": ',
            json_float(ms_from_ns(percentile_ns(sorted_totals, 50, 100))),
            ', "p90_ms": ',
            json_float(ms_from_ns(percentile_ns(sorted_totals, 90, 100))),
            ', "min_ms": ',
            json_float(ms_from_ns(sorted_totals[0])),
            ', "result": ',
            result_json(query, best.totals),
            ', "repeat": ',
            repeat,
            ', "workers": ',
            workers,
            "}",
        )
        if out_path == "-":
            print(record)
        else:
            lines += record + "\n"

    if out_path != "-":
        with open(out_path, "w") as handle:
            handle.write(lines)
