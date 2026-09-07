#!/usr/bin/env bash
#
# Run the suite through PostgreSQL and diff its answers against PyIceberg's.
#
#   scripts/bench-postgres.sh                    # both legs, then the diff
#   TAXIBENCH_LEG=single scripts/bench-postgres.sh
#
# The two legs are the same two the Iceberg suite runs. `single` is a serial
# plan — `max_parallel_workers_per_gather = 0` — against PyIceberg's
# `set_cpu_count(1)`. `threaded` is nine parallel workers beside the leader,
# ten processes on ten cores, against PyIceberg's ten threads. A comparison
# where one side quietly uses every core is not a comparison, and PostgreSQL
# parallelises analytical aggregates by default, so the worker count is named
# here exactly the way the thread count is named there.
#
# Each query runs in its own process, as in scripts/bench.sh: neighbouring
# queries warm the buffer cache and the allocator for each other, so a single
# pass over the suite measures the order as much as the engine. What the
# process boundary cannot reset is the server's own buffer cache — the backend
# is a separate long-lived process — which is why every query still discards a
# warm-up run of its own.
#
# This does not start or load the cluster. Run scripts/load-postgres.sh first.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

VENV="${TAXIBENCH_VENV:-$ROOT/build/venv}"
REPEAT="${TAXIBENCH_REPEAT:-5}"
LEGS="${TAXIBENCH_LEG:-single threaded}"
# Which PyIceberg results to diff against; the suite writes these.
SUFFIX="${TAXIBENCH_PG_SUFFIX:-}"

QUERIES=(
    q1_scan_count q2_month_range q3_payment_sum q4_tip_ratio
    q5_top_zones q6_zone_revenue q7_wide q8_selective
)

DSN="$(bash scripts/pg-server.sh dsn)"
mkdir -p build

for leg in $LEGS; do
    case "$leg" in
        single)   workers=0; label="serial plan vs one thread" ;;
        threaded) workers=9; label="9 workers + leader vs 10 threads" ;;
        *) echo "error: unknown leg '$leg'" >&2; exit 1 ;;
    esac

    out="build/results-postgres-$leg$SUFFIX.jsonl"
    reference="build/results-python-$leg.jsonl"
    : > "$out"

    echo "== leg: $leg ($label)"
    for query in "${QUERIES[@]}"; do
        printf '   %-18s' "$query"
        PYTHONPATH="$ROOT/python" "$VENV/bin/python" -m taxibench.postgres \
            "$DSN" --query "$query" --workers "$workers" \
            --repeat "$REPEAT" --warmup >> "$out"
        echo "done"
    done

    echo
    if [ -f "$reference" ]; then
        # compare.py unmodified: counts exact, sums within 1e-9 relative, and a
        # non-zero exit if any answer disagrees. Its first column is labelled
        # for whichever engine's records are in the first file.
        python3 scripts/compare.py "$out" "$reference" \
            --json "build/results-postgres-$leg$SUFFIX.json" --label "$label"
    else
        echo "no $reference to diff against; run scripts/bench.sh first" >&2
    fi
    echo
done
