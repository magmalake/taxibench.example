"""`taxibench-probe` — one scan, any projection, timed. A diagnostic, not a benchmark.

    taxibench-probe <warehouse> [--select a,b,c] [--filter DSL]
                    [--workers N] [--repeat N] [--warmup] [--plan-only]

The suite in `main.mojo` runs eight fixed queries. This runs whatever scan you
describe, which is what isolating a cost needs: hold the file set still, vary
only the column count, and the intercept of the line is the per-scan overhead
that a narrow projection cannot amortise.

`--plan-only` stops after `plan_files()` and skips the read, separating what
planning costs from what decoding costs.

Rows are counted and thrown away. Nothing is folded, so the number is the scan
alone — no aggregation, unlike the suite.
"""

from std.sys import argv
from std.time import perf_counter_ns

from iceberg.catalog.filesystem import find_latest_metadata
from iceberg.io import FileIO
from iceberg.metadata import TableMetadata
from iceberg.read import ScanOptions
from iceberg.scan import TableScan


def read_text(path: String) raises -> String:
    with open(path, "r") as handle:
        return handle.read()


def strip_scheme(location: String) -> String:
    if location.startswith("file://"):
        return String(location[byte=7 : location.byte_length()])
    return location.copy()


def resolve_metadata(io: FileIO, target: String) raises -> String:
    if target.endswith(".json"):
        return target.copy()
    try:
        return strip_scheme(String(read_text(target + "/metadata_location.txt").strip()))
    except:
        pass
    return find_latest_metadata(io, target)


def split_commas(text: String) -> List[String]:
    var out = List[String]()
    var current = String()
    for slice in text.codepoint_slices():
        if slice == ",":
            if current.byte_length() > 0:
                out.append(current.copy())
            current = String()
        else:
            current += String(slice)
    if current.byte_length() > 0:
        out.append(current^)
    return out^


def sorted_ascending(var samples: List[Int]) -> List[Int]:
    for i in range(1, len(samples)):
        var value = samples[i]
        var j = i - 1
        while j >= 0 and samples[j] > value:
            samples[j + 1] = samples[j]
            j -= 1
        samples[j + 1] = value
    return samples^


def main() raises:
    var args = argv()
    if len(args) < 2:
        print("usage: taxibench-probe <warehouse> [--select a,b] [--filter DSL]")
        return

    var target = String(args[1])
    var columns = List[String]()
    var filter_dsl = String()
    var workers = 1
    var repeat = 5
    var warmup = False
    var plan_only = False
    var lazy = False

    var i = 2
    while i < len(args):
        var flag = String(args[i])
        if flag == "--select" and i + 1 < len(args):
            columns = split_commas(String(args[i + 1]))
            i += 2
        elif flag == "--filter" and i + 1 < len(args):
            filter_dsl = String(args[i + 1])
            i += 2
        elif flag == "--workers" and i + 1 < len(args):
            workers = Int(String(args[i + 1]))
            i += 2
        elif flag == "--repeat" and i + 1 < len(args):
            repeat = Int(String(args[i + 1]))
            i += 2
        elif flag == "--warmup":
            warmup = True
            i += 1
        elif flag == "--plan-only":
            plan_only = True
            i += 1
        elif flag == "--lazy":
            lazy = True
            i += 1
        else:
            raise Error("taxibench-probe: unexpected argument '" + flag + "'")

    var io = FileIO.local()
    var metadata = TableMetadata.parse(read_text(resolve_metadata(io, target)))
    var options = ScanOptions()
    options.num_workers = workers
    # `lazy` fetches the footer and only the surviving row groups instead of
    # reading the whole data file. Its docstring calls that pointless on a
    # local disk; this flag exists to check whether that is true, since a
    # narrow projection still pays to pull every byte of a 50 MB file.
    options.lazy = lazy

    var samples = List[Int]()
    var rows = 0
    var files = 0
    var runs = repeat + 1 if warmup else repeat
    for run in range(runs):
        var scan = TableScan(metadata.copy(), io.copy())
        if filter_dsl != "":
            scan = scan.filter(filter_dsl)
        if len(columns) > 0:
            scan = scan.select(columns.copy())

        var start = perf_counter_ns()
        if plan_only:
            var tasks = scan.plan_files()
            files = len(tasks)
            rows = 0
        else:
            var batches = scan.to_batches(options)
            rows = 0
            for k in range(len(batches)):
                rows += batches[k].num_rows
        var elapsed = perf_counter_ns() - start
        if run > 0 or not warmup:
            samples.append(elapsed)

    var ordered = sorted_ascending(samples^)
    var last = len(ordered) - 1
    var p50 = ordered[(50 * last + 50) // 100]
    var p90 = ordered[(90 * last + 50) // 100]

    print(
        String(
            '{"engine": "iceberg.mojo", "columns": ',
            len(columns),
            ', "rows": ',
            rows,
            ', "files": ',
            files,
            ', "workers": ',
            workers,
            ', "plan_only": ',
            "true" if plan_only else "false",
            ', "lazy": ',
            "true" if lazy else "false",
            ', "p50_ms": ',
            Float64(p50) / 1.0e6,
            ', "p90_ms": ',
            Float64(p90) / 1.0e6,
            "}",
        )
    )
