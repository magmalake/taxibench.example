#!/usr/bin/env bash
#
# Run the suite through both implementations and diff the answers.
#
#   scripts/bench.sh                 # native binaries on this machine
#   TAXIBENCH_DOCKER=1 scripts/bench.sh   # the two container images instead
#   TAXIBENCH_LEG=single scripts/bench.sh # only the one-thread leg
#
# Two things here are deliberate and are what make the numbers defensible.
#
# **Each query runs in its own process.** Neighbouring queries warm caches and
# allocators for each other, so a single pass over the whole suite measures the
# order as much as the code.
#
# **Both legs name their thread count on both sides.** pyarrow reads Parquet
# multi-threaded by default and never announces it; comparing that against a
# single-worker Mojo scan is not a comparison. So the suite runs twice: one
# thread each, then one worker per core each.
#
# The warehouse is mounted into the containers at the same absolute path it has
# on the host. An Iceberg table's metadata names its data files by absolute
# location, so a table generated at /x/build/warehouse is only readable at
# /x/build/warehouse unless the reader rewrites paths on the way through.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WAREHOUSE="${TAXIBENCH_WAREHOUSE:-$ROOT/build/warehouse}"
VENV="${TAXIBENCH_VENV:-$ROOT/build/venv}"
REPEAT="${TAXIBENCH_REPEAT:-5}"
USE_DOCKER="${TAXIBENCH_DOCKER:-0}"
LEGS="${TAXIBENCH_LEG:-single threaded}"

QUERIES=(
    q1_scan_count q2_month_range q3_payment_sum q4_tip_ratio
    q5_top_zones q6_zone_revenue q7_wide q8_selective
)

if [ ! -d "$WAREHOUSE" ]; then
    echo "error: no warehouse at $WAREHOUSE — run scripts/load.sh first" >&2
    exit 1
fi
mkdir -p build

CORES="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 8)"

run_mojo() {   # <query> <workers> <out>
    if [ "$USE_DOCKER" = "1" ]; then
        docker run --rm -v "$WAREHOUSE:$WAREHOUSE:ro" taxibench-mojo:latest \
            "$WAREHOUSE" --query "$1" --workers "$2" \
            --repeat "$REPEAT" --warmup >> "$3"
    else
        ./build/taxibench "$WAREHOUSE" --query "$1" --workers "$2" \
            --repeat "$REPEAT" --warmup >> "$3"
    fi
}

run_python() { # <query> <threads> <out>
    if [ "$USE_DOCKER" = "1" ]; then
        docker run --rm -v "$WAREHOUSE:$WAREHOUSE:ro" taxibench-python:latest \
            "$WAREHOUSE" --query "$1" --threads "$2" \
            --repeat "$REPEAT" --warmup >> "$3"
    else
        PYTHONPATH="$ROOT/python" "$VENV/bin/python" -m taxibench \
            "$WAREHOUSE" --query "$1" --threads "$2" \
            --repeat "$REPEAT" --warmup >> "$3"
    fi
}

if [ "$USE_DOCKER" != "1" ] && [ ! -x build/taxibench ]; then
    echo "error: no build/taxibench — run 'pixi run build' first" >&2
    exit 1
fi

for leg in $LEGS; do
    case "$leg" in
        single)   workers=1; threads=1; label="one thread each" ;;
        threaded) workers=0; threads="$CORES"; label="$CORES threads each" ;;
        *) echo "error: unknown leg '$leg'" >&2; exit 1 ;;
    esac

    mojo_out="build/results-mojo-$leg.jsonl"
    python_out="build/results-python-$leg.jsonl"
    : > "$mojo_out"
    : > "$python_out"

    echo "== leg: $leg ($label)"
    for query in "${QUERIES[@]}"; do
        printf '   %-18s' "$query"
        run_mojo "$query" "$workers" "$mojo_out"
        run_python "$query" "$threads" "$python_out"
        echo "done"
    done

    echo
    python3 scripts/compare.py "$mojo_out" "$python_out" \
        --json "build/results-$leg.json" --label "$label"
    echo
done
