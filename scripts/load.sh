#!/usr/bin/env bash
#
# Download the TLC months and have PyIceberg build the table from them.
#
# Neither implementation under test writes this table. PyIceberg does, so that
# the layout is one a PyIceberg user would really have and neither side gets a
# warehouse arranged to suit it.
#
# The Parquet downloads are about 1.2 GB and the table about 1.3 GB; both live
# under build/ and neither is checked in. Set TAXIBENCH_MONTHS to load fewer
# months when iterating.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TLC="${TAXIBENCH_TLC:-$ROOT/build/tlc}"
WAREHOUSE="${TAXIBENCH_WAREHOUSE:-$ROOT/build/warehouse}"
VENV="${TAXIBENCH_VENV:-$ROOT/build/venv}"
MONTHS="${TAXIBENCH_MONTHS:-0}"
YEARS=("${TAXIBENCH_YEARS:-2023 2024}")

command -v uv >/dev/null 2>&1 || {
    echo "error: uv is needed to build the loader environment" >&2
    echo "       see https://docs.astral.sh/uv/" >&2
    exit 1
}

if [ ! -x "$VENV/bin/python" ] || ! "$VENV/bin/python" -c "import pyiceberg, pyarrow" 2>/dev/null; then
    echo "== building the loader environment"
    uv venv --python 3.12 "$VENV" >/dev/null
    VIRTUAL_ENV="$VENV" uv pip install --quiet \
        "pyiceberg[sql-sqlite,pyarrow]==0.11.1" >/dev/null
fi

mkdir -p "$TLC"
echo "== downloading TLC months into $TLC"
for year in ${YEARS[*]}; do
    for month in 01 02 03 04 05 06 07 08 09 10 11 12; do
        file="yellow_tripdata_${year}-${month}.parquet"
        if [ -f "$TLC/$file" ]; then
            continue
        fi
        echo "   $file"
        curl -sfo "$TLC/$file" \
            "https://d37ci6vzurychx.cloudfront.net/trip-data/$file" || {
            echo "error: could not download $file" >&2
            rm -f "$TLC/$file"
            exit 1
        }
    done
done

echo "== building the Iceberg table in $WAREHOUSE"
if [ "$MONTHS" != "0" ]; then
    "$VENV/bin/python" loader/load_table.py "$WAREHOUSE" "$TLC" --months "$MONTHS"
else
    "$VENV/bin/python" loader/load_table.py "$WAREHOUSE" "$TLC"
fi
