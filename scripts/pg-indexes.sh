#!/usr/bin/env bash
#
# Add or remove the indexes for the indexed leg of the PostgreSQL suite.
#
#   scripts/pg-indexes.sh create   # build them, reporting time and size
#   scripts/pg-indexes.sh drop     # back to the unindexed leg
#   scripts/pg-indexes.sh size     # what the table and its indexes cost
#
# **The rule is one index per equality predicate in the suite, and nothing
# else.** That is `PULocationID`, which q5 and q6 group by and q8 filters on,
# and `payment_type`, which q3 and q8 filter on. No composite, no covering
# index, no `INCLUDE (total_amount)` — any of those would be an index built
# for one query in the suite after seeing the suite, which is how a benchmark
# stops measuring a database and starts measuring the person tuning it.
#
# The indexes are created on the partitioned parent, so PostgreSQL builds one
# per partition and attaches them; the planner prunes partitions first and then
# chooses per partition.
#
# What they change: q1, q4, q5 and q7 have no equality predicate at all and
# cannot be touched by either index. q8 is the query an index is *for* —
# 470,349 rows out of 79.5 million — and it becomes a bitmap heap scan through
# `PULocationID` on all 24 partitions. q3 is the surprise: `payment_type = 2`
# matches about a seventh of the table, well past where a sequential scan
# normally wins, and the planner still takes the index on most partitions and
# not on others — sixteen index scans and eight bitmap heap scans in the serial
# leg, fourteen, eight and two sequential in the parallel one. Whether that is
# faster is a timing question and the README answers it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DSN="$(bash "$ROOT/scripts/pg-server.sh" dsn)"

if ! command -v psql >/dev/null 2>&1; then
    PATH="$ROOT/.pixi/envs/postgres/bin:$PATH"
    export PATH
fi

size() {
    psql "$DSN" -c "
        SELECT
            pg_size_pretty(sum(pg_table_size(c.oid))) AS heap,
            pg_size_pretty(sum(pg_indexes_size(c.oid))) AS indexes,
            pg_size_pretty(sum(pg_total_relation_size(c.oid))) AS total
        FROM pg_class c
        JOIN pg_inherits i ON i.inhrelid = c.oid
        WHERE i.inhparent = 'trips'::regclass"
}

case "${1:-}" in
    create)
        echo "== building indexes"
        time psql "$DSN" -v ON_ERROR_STOP=1 \
            -c 'CREATE INDEX IF NOT EXISTS trips_pu_idx ON trips ("PULocationID")' \
            -c 'CREATE INDEX IF NOT EXISTS trips_payment_idx ON trips (payment_type)'
        # Fresh indexes shift the planner's cost estimates; re-analyzing keeps
        # the indexed leg from being measured against stale statistics.
        psql "$DSN" -c "ANALYZE trips"
        size
        ;;
    drop)
        psql "$DSN" -v ON_ERROR_STOP=1 \
            -c "DROP INDEX IF EXISTS trips_pu_idx" \
            -c "DROP INDEX IF EXISTS trips_payment_idx"
        psql "$DSN" -c "ANALYZE trips"
        size
        ;;
    size) size ;;
    *)
        echo "usage: pg-indexes.sh {create|drop|size}" >&2
        exit 1
        ;;
esac
