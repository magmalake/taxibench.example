"""`taxibench-maintain` — the delete half of the study, through iceberg.mojo.

    taxibench-maintain <warehouse-root> --op OP [options]

    --op delete-cow     delete the filter's rows, copy-on-write
    --op delete-mor     delete them merge-on-read (v2: position delete files)
    --op count          count the rows a filter matches, deletes applied
    --op snapshots      list the snapshot ids, oldest first
    --filter DSL        the row filter, as a JSON S-expression
    --snapshot ID       read this snapshot instead of the current one
    --workers N         data files read in parallel (0 = one per core)

Everything goes through the **SQL catalog**, not the filesystem layout, for one
reason: iceberg.mojo's `SqlCatalog` writes the same two tables PyIceberg's does,
so after this binary deletes rows PyIceberg can open the same catalog and say
what it sees. A delete benchmark nobody can check is not worth timing.

The warehouse root is the directory the loader built: `catalog.db` beside a
`warehouse/` tree.
"""

from std.sys import argv
from std.time import perf_counter_ns

from iceberg.catalog.sql import SqlCatalog
from iceberg.io import FileIO
from iceberg.read import ScanOptions
from iceberg.scan import TableScan


comptime NAMESPACE = String("taxi")
comptime TABLE = String("trips")

comptime USAGE = String(
    "taxibench-maintain — the delete study, through iceberg.mojo\n"
    "\n"
    "usage:\n"
    "  taxibench-maintain <warehouse-root> --op OP [--filter DSL]\n"
    "                     [--snapshot ID] [--workers N]\n"
    "\n"
    "ops: delete-cow, delete-mor, count, snapshots\n"
)


def json_float(value: Float64) -> String:
    return String(value)


def ms_from_ns(ns: Int) -> Float64:
    return Float64(ns) / 1.0e6


def open_catalog(root: String) raises -> SqlCatalog:
    """The catalog the loader wrote: `catalog.db` beside `warehouse/`."""
    return SqlCatalog.local(
        String("taxi"),
        String("sqlite:///", root, "/catalog.db"),
        String("file://", root, "/warehouse"),
    )


def current_snapshot(root: String) raises -> Int64:
    var catalog = open_catalog(root)
    var table = catalog.load_table(NAMESPACE, TABLE)
    if not table.scan().has_any_snapshot():
        return -1
    return table.scan().snapshot().snapshot_id


def count_rows(
    root: String, filter_dsl: String, snapshot: Int64, workers: Int
) raises -> Tuple[Int, Int, Int]:
    """Rows matching the filter, plus the files and delete files planned."""
    var catalog = open_catalog(root)
    var table = catalog.load_table(NAMESPACE, TABLE)
    var scan = table.scan()
    if filter_dsl != "":
        scan = scan.filter(filter_dsl)
    if snapshot >= 0:
        scan = scan.use_snapshot(snapshot)

    var tasks = scan.plan_files()
    var deletes = 0
    for i in range(len(tasks)):
        deletes += len(tasks[i].delete_files)

    var options = ScanOptions()
    options.num_workers = workers
    var batches = scan.to_batches(options)
    var rows = 0
    for i in range(len(batches)):
        rows += batches[i].num_rows
    return (rows, len(tasks), deletes)


def main() raises:
    var args = argv()
    if len(args) < 2:
        print(USAGE)
        return

    var root = String(args[1])
    var op = String()
    var filter_dsl = String()
    var snapshot = Int64(-1)
    var workers = 0

    var i = 2
    while i < len(args):
        var flag = String(args[i])
        if flag == "--op" and i + 1 < len(args):
            op = String(args[i + 1])
            i += 2
        elif flag == "--filter" and i + 1 < len(args):
            filter_dsl = String(args[i + 1])
            i += 2
        elif flag == "--snapshot" and i + 1 < len(args):
            snapshot = Int64(Int(String(args[i + 1])))
            i += 2
        elif flag == "--workers" and i + 1 < len(args):
            workers = Int(String(args[i + 1]))
            i += 2
        else:
            raise Error("taxibench-maintain: unexpected argument '" + flag + "'")

    if op == "snapshots":
        var catalog = open_catalog(root)
        var table = catalog.load_table(NAMESPACE, TABLE)
        var out = String("[")
        for k in range(len(table.metadata.snapshots)):
            if k > 0:
                out += ", "
            out += String(table.metadata.snapshots[k].snapshot_id)
        out += "]"
        print(out)
        return

    if op == "count":
        var before = perf_counter_ns()
        var result = count_rows(root, filter_dsl, snapshot, workers)
        var elapsed = perf_counter_ns() - before
        print(
            String(
                '{"op": "count", "engine": "iceberg.mojo", "rows": ',
                result[0],
                ', "files": ',
                result[1],
                ', "delete_files": ',
                result[2],
                ', "ms": ',
                json_float(ms_from_ns(elapsed)),
                "}",
            )
        )
        return

    if op == "delete-cow" or op == "delete-mor":
        if filter_dsl == "":
            raise Error("taxibench-maintain: --filter is required to delete")
        var mode = String("copy-on-write")
        if op == "delete-mor":
            mode = String("merge-on-read")

        var snapshot_before = current_snapshot(root)
        var catalog = open_catalog(root)

        var before = perf_counter_ns()
        _ = catalog.delete_where(NAMESPACE, TABLE, filter_dsl, mode)
        var elapsed = perf_counter_ns() - before

        # The row count the delete reports is the rows it removed; read the
        # table back so the number quoted is what a later reader will see,
        # which is the only number that means anything after a merge-on-read
        # delete leaves the data files in place.
        var remaining = count_rows(root, String(), Int64(-1), workers)
        print(
            String(
                '{"op": "',
                op,
                '", "engine": "iceberg.mojo", "mode": "',
                mode,
                '", "ms": ',
                json_float(ms_from_ns(elapsed)),
                ', "rows_after": ',
                remaining[0],
                ', "files_after": ',
                remaining[1],
                ', "delete_files_after": ',
                remaining[2],
                ', "snapshot_before": ',
                snapshot_before,
                ', "snapshot_after": ',
                current_snapshot(root),
                "}",
            )
        )
        return

    raise Error("taxibench-maintain: unknown --op '" + op + "'")
