#!/usr/bin/env bash
#
# Start a throwaway PostgreSQL cluster and load the same 79,478,796 trips into
# it, from the same TLC Parquet the Iceberg table was built from.
#
#   scripts/load-postgres.sh          # cluster + schema + load + ANALYZE
#   scripts/load-postgres.sh --drop   # tear the cluster and its data down
#
# Reproducible from an empty checkout: this needs the TLC files, which
# scripts/load.sh downloads, and nothing that is installed on the machine. The
# server comes from the `postgres` pixi environment, the cluster lives under
# build/ on a free port, and `--drop` removes every trace of it.
#
# The load is not the benchmark, but its cost is part of the honest picture, so
# it is timed and the resulting on-disk size is printed: PostgreSQL holds a
# second, private copy of the data in its own heap format, and the README
# reports what that copy costs against the 1.3 GB of Parquet it duplicates.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TLC="${TAXIBENCH_TLC:-$ROOT/build/tlc}"
VENV="${TAXIBENCH_VENV:-$ROOT/build/venv}"
MONTHS="${TAXIBENCH_MONTHS:-0}"

if [ "${1:-}" = "--drop" ]; then
    bash scripts/pg-server.sh destroy
    echo "cluster removed"
    exit 0
fi

if [ ! -d "$TLC" ]; then
    echo "error: no TLC Parquet at $TLC — run scripts/load.sh first" >&2
    exit 1
fi

command -v uv >/dev/null 2>&1 || {
    echo "error: uv is needed to build the loader environment" >&2
    exit 1
}

# psycopg is the driver and numpy builds the binary COPY payload; both go into
# the same venv scripts/load.sh creates for PyIceberg, so there is one Python
# environment in the repo rather than two.
if ! "$VENV/bin/python" -c "import psycopg, numpy" 2>/dev/null; then
    echo "== adding psycopg and numpy to the loader environment"
    VIRTUAL_ENV="$VENV" uv pip install --quiet "psycopg[binary]>=3.2" numpy >/dev/null
fi

echo "== starting the cluster"
DSN="$(bash scripts/pg-server.sh start)"
echo "   $DSN"

echo "== loading"
if [ "$MONTHS" != "0" ]; then
    "$VENV/bin/python" loader/load_postgres.py "$DSN" "$TLC" --months "$MONTHS"
else
    "$VENV/bin/python" loader/load_postgres.py "$DSN" "$TLC"
fi

echo
echo "== on disk"
bash scripts/pg-indexes.sh size
du -sh "${TAXIBENCH_PGROOT:-$ROOT/build/pg}"
