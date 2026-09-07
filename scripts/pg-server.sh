#!/usr/bin/env bash
#
# A throwaway PostgreSQL cluster for the benchmark, on a free port, under
# build/. Adapted from postgres.mojo's scripts/pg-server.sh, which is how that
# tin's tests get a server without touching anything installed.
#
#   scripts/pg-server.sh start     # initdb + start; writes build/pg/dsn
#   scripts/pg-server.sh stop      # idempotent
#   scripts/pg-server.sh destroy   # stop, then delete the cluster and its data
#   scripts/pg-server.sh dsn       # print the DSN of the running cluster
#   scripts/pg-server.sh psql ...  # psql against it
#
# The cluster is persistent between `start` and `destroy` because the table it
# holds takes minutes to build and tens of gigabytes to store; `stop` leaves it
# on disk. The server binaries come from the `postgres` pixi environment, which
# is separate from the default one so the Mojo toolchain and PostgreSQL never
# have to solve against each other. Run this under `pixi run -e postgres`, or
# let it find the environment itself, which is what the code below does.
#
# macOS caps Unix socket paths at 104 bytes and a data directory inside a
# checkout on an external volume can bust that once "/.s.PGSQL.NNNNN" is
# appended, so the socket directory lives under $TMPDIR and its path is
# recorded alongside the cluster.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PGROOT="${TAXIBENCH_PGROOT:-$ROOT/build/pg}"
PGDATA="$PGROOT/data"
PGLOG="$PGROOT/server.log"

# Put the pixi postgres environment on PATH unless a server is already there.
if ! command -v initdb >/dev/null 2>&1; then
    if [ -x "$ROOT/.pixi/envs/postgres/bin/initdb" ]; then
        PATH="$ROOT/.pixi/envs/postgres/bin:$PATH"
        export PATH
    else
        echo "error: no initdb on PATH and no postgres pixi environment" >&2
        echo "       run 'pixi install -e postgres' first" >&2
        exit 1
    fi
fi

# The configuration, in one place, because "we used the defaults" and "we tuned
# it until it won" are both wrong answers and the only defensible thing is to
# say exactly what was set.
#
# shared_buffers 4GB: the heap is about 12 GB on a 24 GB machine, so no setting
#   holds all of it. 4 GB is the conventional quarter-of-RAM and leaves the OS
#   page cache room to hold the rest, which is the arrangement that makes the
#   warm-cache leg actually warm. Going higher would double-buffer the same
#   pages and take the memory from the cache that is doing the work.
# work_mem 256MB: enough that nothing in the suite spills. It barely matters —
#   the widest aggregate here groups into 266 zones — but a spill would be
#   measuring the disk rather than the engine.
# maintenance_work_mem 2GB: index builds, which are reported separately.
# effective_cache_size 16GB: a planner hint, not an allocation. It tells the
#   planner the data is cached, which is true after the warm-up run.
# random_page_cost 1.1: an NVMe SSD. Left at the rotating-disk default of 4.0
#   the planner would refuse the index plan on q8 and the indexed leg would
#   measure nothing.
# max_parallel_workers 10 on a 10-core machine, with
#   max_parallel_workers_per_gather set per session by the runner: 0 for the
#   one-thread leg and 9 for the ten-thread leg, so the two legs line up with
#   the Mojo and PyIceberg ones.
# fsync/synchronous_commit/full_page_writes off and max_wal_size 8GB: load-time
#   settings for a cluster that is rebuilt rather than recovered. They make the
#   reported load time faster than a durable load would be, which is said
#   plainly in the README; they have no effect on read timings.
# jit left at its default (on): PostgreSQL compiles the aggregate expressions
#   for scans this size, and turning that off would be tuning against it.
write_config() {
    cat >> "$PGDATA/postgresql.conf" <<'CONF'

# --- taxibench ---------------------------------------------------------
shared_buffers = 4GB
work_mem = 256MB
maintenance_work_mem = 2GB
effective_cache_size = 16GB
random_page_cost = 1.1
effective_io_concurrency = 200
max_worker_processes = 16
max_parallel_workers = 10
max_parallel_workers_per_gather = 0
max_parallel_maintenance_workers = 4
fsync = off
synchronous_commit = off
full_page_writes = off
max_wal_size = 8GB
checkpoint_timeout = 30min
CONF
}

start() {
    if [ -f "$PGROOT/dsn" ] && pg_isready -d "$(cat "$PGROOT/dsn")" >/dev/null 2>&1; then
        cat "$PGROOT/dsn"
        return 0
    fi

    mkdir -p "$PGROOT"

    if [ ! -f "$PGDATA/PG_VERSION" ]; then
        rm -rf "$PGDATA"
        if ! initdb -D "$PGDATA" --auth=trust --username=postgres -E UTF8 \
                --locale=C >"$PGROOT/initdb.log" 2>&1; then
            echo "== initdb failed; tail of $PGROOT/initdb.log:" >&2
            tail -n 40 "$PGROOT/initdb.log" >&2 || true
            exit 1
        fi
        write_config
    fi

    socketdir="$(mktemp -d "${TMPDIR:-/tmp}/pgsock.XXXXXX")"
    echo "$socketdir" > "$PGROOT/socketdir"

    port="$(python3 -c '
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
')"
    echo "$port" > "$PGROOT/port"

    if ! pg_ctl -D "$PGDATA" -w -o \
            "-p $port -k $socketdir -c listen_addresses=127.0.0.1" \
            -l "$PGLOG" start >/dev/null; then
        echo "== pg_ctl start failed; tail of $PGLOG:" >&2
        tail -n 40 "$PGLOG" >&2 || true
        exit 1
    fi

    i=0
    until pg_isready -h 127.0.0.1 -p "$port" >/dev/null 2>&1; do
        i=$((i + 1))
        if [ "$i" -ge 150 ]; then
            echo "== postgres never became ready; tail of $PGLOG:" >&2
            tail -n 40 "$PGLOG" >&2 || true
            pg_ctl -D "$PGDATA" -m immediate stop >/dev/null 2>&1 || true
            exit 1
        fi
        sleep 0.2
    done

    if ! psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -tAc \
            "SELECT 1 FROM pg_database WHERE datname = 'taxi'" | grep -q 1; then
        createdb -h 127.0.0.1 -p "$port" -U postgres taxi
    fi

    dsn="postgresql://postgres@127.0.0.1:$port/taxi"
    echo "$dsn" > "$PGROOT/dsn"
    echo "$dsn"
}

stop() {
    if [ -d "$PGDATA" ]; then
        pg_ctl -D "$PGDATA" -m fast stop >/dev/null 2>&1 || true
    fi
    if [ -f "$PGROOT/socketdir" ]; then
        rm -rf "$(cat "$PGROOT/socketdir")" 2>/dev/null || true
        rm -f "$PGROOT/socketdir"
    fi
    rm -f "$PGROOT/dsn"
}

case "${1:-}" in
    start) start ;;
    stop) stop ;;
    destroy)
        stop
        rm -rf "$PGROOT"
        ;;
    dsn)
        if [ ! -f "$PGROOT/dsn" ]; then
            echo "error: no cluster running; scripts/pg-server.sh start" >&2
            exit 1
        fi
        cat "$PGROOT/dsn"
        ;;
    psql)
        shift
        exec psql "$(cat "$PGROOT/dsn")" "$@"
        ;;
    *)
        echo "usage: pg-server.sh {start|stop|destroy|dsn|psql ...}" >&2
        exit 1
        ;;
esac
