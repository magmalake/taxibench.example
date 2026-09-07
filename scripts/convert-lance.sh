#!/usr/bin/env bash
#
# Transcode the Iceberg table into a Lance dataset — a second copy of the data.
#
#   scripts/convert-lance.sh                     # build/warehouse -> build/lance
#   TAXIBENCH_COMPRESS=zstd scripts/convert-lance.sh   # block-compressed columns
#
# Lance goes in its own virtual environment. The PyIceberg environment is what
# one of the two engines under test is measured through, and its pyarrow is
# 25.0.1; letting a resolver move that while installing something unrelated
# would silently re-time the benchmark. Keeping the two apart means the Lance
# leg can be added, removed or upgraded without touching what the other leg
# runs on. Both end up on the same pyarrow version, which is what matters for
# the comparison: the fold is pyarrow on both sides.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WAREHOUSE="${TAXIBENCH_WAREHOUSE:-$ROOT/build/warehouse}"
LANCE="${TAXIBENCH_LANCE:-$ROOT/build/lance}"
LANCE_VENV="${TAXIBENCH_LANCE_VENV:-$ROOT/build/venv-lance}"
COMPRESS="${TAXIBENCH_COMPRESS:-}"

command -v uv >/dev/null 2>&1 || {
    echo "error: uv is needed to build the Lance environment" >&2
    echo "       see https://docs.astral.sh/uv/" >&2
    exit 1
}

if [ ! -d "$WAREHOUSE" ]; then
    echo "error: no warehouse at $WAREHOUSE — run scripts/load.sh first" >&2
    exit 1
fi

if [ ! -x "$LANCE_VENV/bin/python" ] || ! "$LANCE_VENV/bin/python" -c "import lance" 2>/dev/null; then
    echo "== building the Lance environment"
    uv venv --python 3.12 "$LANCE_VENV" >/dev/null
    VIRTUAL_ENV="$LANCE_VENV" uv pip install --quiet "pylance==11.0.0" >/dev/null
fi

echo "== transcoding the Iceberg table into $LANCE"
if [ -n "$COMPRESS" ]; then
    "$LANCE_VENV/bin/python" loader/convert_lance.py \
        "$WAREHOUSE" "$LANCE" --compress "$COMPRESS"
else
    "$LANCE_VENV/bin/python" loader/convert_lance.py "$WAREHOUSE" "$LANCE"
fi
