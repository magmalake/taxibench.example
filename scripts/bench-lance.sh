#!/usr/bin/env bash
#
# Run the suite over the Lance dataset, and over the Iceberg table beside it.
#
#   scripts/bench-lance.sh                  # both legs, then the answer diff
#   TAXIBENCH_LEG=single scripts/bench-lance.sh
#
# The third engine is measured separately from `scripts/bench.sh` on purpose.
# That script compares two implementations of Iceberg reading one physical
# table; this one compares two file formats, each read by its own reader, over
# two copies of the same 79,478,796 rows. Those are different questions and
# putting them in one table would suggest they are the same one.
#
# The PyIceberg leg is re-run here rather than reusing whatever is already in
# build/. A Lance number taken today against a PyIceberg number taken last week
# is not a comparison — the two have to see the same machine, in the same
# state, in the same session, which is also why each query still runs in its
# own process and why both sides are told how many threads to use.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WAREHOUSE="${TAXIBENCH_WAREHOUSE:-$ROOT/build/warehouse}"
LANCE="${TAXIBENCH_LANCE:-$ROOT/build/lance}"
VENV="${TAXIBENCH_VENV:-$ROOT/build/venv}"
LANCE_VENV="${TAXIBENCH_LANCE_VENV:-$ROOT/build/venv-lance}"
REPEAT="${TAXIBENCH_REPEAT:-5}"
LEGS="${TAXIBENCH_LEG:-single threaded}"

QUERIES=(
    q1_scan_count q2_month_range q3_payment_sum q4_tip_ratio
    q5_top_zones q6_zone_revenue q7_wide q8_selective
)

if [ ! -d "$LANCE" ]; then
    echo "error: no Lance dataset at $LANCE — run scripts/convert-lance.sh" >&2
    exit 1
fi
if [ ! -d "$WAREHOUSE" ]; then
    echo "error: no warehouse at $WAREHOUSE — run scripts/load.sh first" >&2
    exit 1
fi
mkdir -p build

CORES="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 8)"

for leg in $LEGS; do
    case "$leg" in
        single)   threads=1; label="one thread each" ;;
        threaded) threads="$CORES"; label="$CORES threads each" ;;
        *) echo "error: unknown leg '$leg'" >&2; exit 1 ;;
    esac

    lance_out="build/results-lance-$leg.jsonl"
    python_out="build/results-python-$leg.jsonl"
    : > "$lance_out"
    : > "$python_out"

    echo "== leg: $leg ($label)"
    for query in "${QUERIES[@]}"; do
        printf '   %-18s' "$query"
        PYTHONPATH="$ROOT/python" "$LANCE_VENV/bin/python" -m taxibench_lance \
            "$LANCE" --query "$query" --threads "$threads" \
            --repeat "$REPEAT" --warmup >> "$lance_out"
        PYTHONPATH="$ROOT/python" "$VENV/bin/python" -m taxibench \
            "$WAREHOUSE" --query "$query" --threads "$threads" \
            --repeat "$REPEAT" --warmup >> "$python_out"
        echo "done"
    done

    echo
    python3 scripts/compare.py "$lance_out" "$python_out" \
        --json "build/results-lance-$leg.json" --label "$label"
    echo
done
