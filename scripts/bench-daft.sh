#!/usr/bin/env bash
#
# Run the suite through Daft twice — once over its own Iceberg reader, once
# over iceberg.mojo — and diff the answers.
#
#   scripts/bench-daft.sh
#   TAXIBENCH_REPEAT=9 scripts/bench-daft.sh
#
# This asks a different question from scripts/bench.sh. That one compares two
# implementations of Iceberg, each with its own aggregation: it measures the
# readers *and* everything above them. This one holds the engine fixed —
# Daft's scheduler, Daft's predicates, Daft's group-bys — and changes only
# where its rows come from. What is left in the difference is the scan.
#
# It needs two things scripts/bench.sh does not: iceberg.mojo's scan library
# (`pixi run carrow-scan-lib` in that repo), and the `daft_flight` connector
# from the pyarrow-flight.example checkout beside it. Both are named below and
# both can be pointed elsewhere.
#
# Each query still runs in its own process, for the reason bench.sh explains:
# neighbouring queries warm caches and allocators for each other.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WAREHOUSE="${TAXIBENCH_WAREHOUSE:-$ROOT/build/warehouse}"
# The table directory, not the warehouse root: the Mojo source reads a table
# where the other legs take a warehouse and find the one table in it. load.sh
# puts the catalog's own warehouse directory inside $WAREHOUSE.
TABLE="${TAXIBENCH_TABLE:-$WAREHOUSE/warehouse/taxi/trips}"
VENV="${TAXIBENCH_DAFT_VENV:-$ROOT/build/venv-daft}"
REPEAT="${TAXIBENCH_REPEAT:-5}"
DAFT_FLIGHT="${DAFT_FLIGHT_DIR:-$ROOT/../pyarrow-flight.example}"
SCAN_LIB="${TAXIBENCH_SCAN_LIB:-$ROOT/../iceberg.mojo/build/libibscan.dylib}"
SHIM_PREFIX="${MAGMALAKE_SHIM_PREFIX:-$ROOT/../iceberg.mojo/.pixi/envs/default}"

QUERIES=(
    q1_scan_count q2_month_range q3_payment_sum q4_tip_ratio
    q5_top_zones q6_zone_revenue q7_wide q8_selective
)

[ -d "$TABLE" ] || { echo "error: no table at $TABLE — run scripts/load.sh first" >&2; exit 1; }
[ -f "$SCAN_LIB" ] || {
    echo "error: no scan library at $SCAN_LIB" >&2
    echo "  build it with 'pixi run carrow-scan-lib' in iceberg.mojo" >&2
    exit 1; }
[ -d "$DAFT_FLIGHT/daft_flight" ] || {
    echo "error: no daft_flight/ under $DAFT_FLIGHT" >&2
    echo "  set DAFT_FLIGHT_DIR to the pyarrow-flight.example checkout" >&2
    exit 1; }
mkdir -p build

# Daft is PyPI-only (the conda-forge package of that name is an unrelated
# library for drawing probabilistic graphical models), and the native leg
# plans through PyIceberg, so both are in one venv of their own.
if [ ! -x "$VENV/bin/python" ] \
    || ! "$VENV/bin/python" -c "import daft, pyiceberg" 2>/dev/null; then
    command -v uv >/dev/null 2>&1 || {
        echo "error: uv is needed to build the Daft venv" >&2; exit 1; }
    echo "== building $VENV"
    uv venv --python 3.12 "$VENV" >/dev/null
    VIRTUAL_ENV="$VENV" uv pip install --quiet \
        "daft>=0.7,<0.8" "pyiceberg[pyarrow]==0.11.1"
fi

run_leg() {  # <source> <query> <out>
    PYTHONPATH="$ROOT/python" \
    DAFT_FLIGHT_DIR="$DAFT_FLIGHT" \
    MAGMALAKE_SHIM_PREFIX="$SHIM_PREFIX" \
        "$VENV/bin/python" -m taxibench.daft_main "$TABLE" \
        --source "$1" --lib "$SCAN_LIB" --query "$2" \
        --repeat "$REPEAT" --warmup >> "$3" 2>/dev/null
}

mojo_out="build/results-daft-mojo.jsonl"
native_out="build/results-daft-native.jsonl"
: > "$mojo_out"
: > "$native_out"

echo "== one engine, two scan sources (repeat $REPEAT, warmup discarded)"
for query in "${QUERIES[@]}"; do
    printf '   %-18s' "$query"
    run_leg mojo "$query" "$mojo_out"
    run_leg native "$query" "$native_out"
    echo "done"
done

echo
python3 scripts/compare.py "$mojo_out" "$native_out" \
    --json "build/results-daft.json" --label "Daft over both readers"
