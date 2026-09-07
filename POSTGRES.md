# PostgreSQL

> A leg of [taxibench.example](README.md) — the same eight analytical queries,
> answered from a relational database instead of a table format.

It is a common and reasonable belief that a relational database will do well
enough on analytical work: the data is already there, the query is already SQL,
and PostgreSQL is very good at a great many things. This is an attempt to find
out what that actually costs, on a workload that is squarely analytical —
79,478,796 rows, eight queries that scan most of them, and two columns of
nineteen touched at a time.

The answer is not a surprise in direction, only in size, and the size is the
point. Nothing here is a criticism of PostgreSQL; it is a measurement of what a
column-oriented table format buys, expressed as the gap to a row store that was
given every reasonable advantage — matching partitioning, tuned configuration,
and both index states.

Everything below is reproducible from the repository: a throwaway cluster under
`build/` on a free port, loaded, measured and torn down by scripts.

`python/taxibench/postgres.py` answers the same eight questions in SQL against
PostgreSQL 18.4, over its own copy of the same rows, and
`scripts/compare.py` diffs those answers against PyIceberg's with no changes
of any kind: **all eight agree, every count exactly and every sum to within
1.8e-11 relative** against a gate of 1e-9.

## What this is not

The two implementations in [the main README](README.md) are two readers of one
physical table. This one
is not.

- **It is a different storage engine.** PostgreSQL is a row store reading its
  own heap: 19 columns interleaved in every tuple, behind a 23-byte header, in
  8 KB pages, uncompressed and unencoded. The other two read column chunks of
  zstd-compressed Parquet. On a query that touches one column of 79 million
  rows this is not close, and it is not supposed to be.
- **It is a second copy of the data.** The Iceberg table is not the input to
  the PostgreSQL leg at run time, only at load time. Two copies of 79,478,796
  trips exist on the disk while this runs, and the second one is nine and a
  half times the size of the first.
- **It is a database, not a library.** The work happens in a server process
  over a socket, planned by a cost-based optimiser that may choose a different
  plan per partition. The other two are a scan loop in the caller's process.

So the total at the bottom of the table is not a verdict on PostgreSQL, which
is not built for this, and not a victory for Iceberg, which is being asked to
do the one thing it is built for. What it is good for is sizing the gap: how
much a columnar file format and a metadata layer are worth on an analytical
scan, in a stack where nothing else is held constant either.

## The table

The PostgreSQL table is **range-partitioned by month**, into the same 24
months the Iceberg table is partitioned into by
`month(tpep_pickup_datetime)`. Each month is a single relation of about
550 MB, which is under PostgreSQL's 1 GB segment size, so a partition really
is one file — and `files` in the JSON records means on this side what it means
on the other two: the number of physical units the planner decided it had to
open. It is read out of `EXPLAIN` rather than asserted, and it comes out at
24, 3, 12 and 1 on the same queries, for the same reason.

A single unpartitioned heap was the other option and it was rejected for being
a straw man. It would have turned q2, q6 and q7 into full 12 GiB scans and
their ratios into a measurement of the fact that nobody had partitioned the
table, which is a property of the setup rather than of PostgreSQL.

`loader/load_postgres.py` **imports `loader/load_table.py`** rather than
restating it: the same `normalise()` reconciles the two schemas TLC publishes,
the same `in_month()` drops the 1,150 rows whose pickup falls outside the month
their file is named for, and the same `ARROW_SCHEMA` fixes the column order and
types. The row count is asserted against the server at the end of the load,
because a table that is off by a thousand rows produces eight wrong answers
and no error.

## What it costs to load, and to hold

Rows are encoded straight into PostgreSQL's binary COPY format from the Arrow
buffers with numpy, and streamed in one COPY per month directly to that
month's partition. Binary rather than text because it is the only format that
round-trips a float64 exactly, and column-wise rather than
`copy.write_row()` because 79.5 million Python tuples would have made the
client the bottleneck of its own load.

| | |
|---|---:|
| COPY, 24 months, one stream per partition | **77–84 s** |
| `ANALYZE trips` | **10–21 s** |
| building both indexes | **28.5 s** |

Those are ranges over two runs on a machine that was not gated quiet, which is
enough precision for a fixture build and not enough to quote as a benchmark.

| on disk | | vs Parquet |
|---|---:|---:|
| Iceberg table — 24 Parquet files, zstd | **1.28 GiB** | — |
| PostgreSQL heap — 24 partitions | **12.26 GiB** | 9.6× |
| ...plus the two indexes | 13.28 GiB | 10.4× |
| the whole cluster directory | 13.51 GiB | 10.6× |

**166 bytes per row against Parquet's 17.3.** Nothing exotic is happening: a
heap tuple carries a 23-byte header and a null bitmap and pads its fields to
alignment, and none of the 19 columns is compressed, dictionary-encoded or
run-length encoded. Parquet spends CPU on decode to buy that factor of nine,
and the queries below are where it gets charged for it.

**The load time is flattered and the reason is stated here rather than
buried.** The cluster runs with `fsync = off`, `synchronous_commit = off` and
`full_page_writes = off`, and the partitions are `UNLOGGED`, so the load writes
no WAL and never waits for the disk. That is defensible for a fixture that is
rebuilt from the TLC Parquet whenever it is wanted, and it is not defensible as
a load-time benchmark: a durable load of the same rows would be materially
slower. None of it affects a read.

## Indexes

Both are measured, because the choice changes q8 enormously and q1 not at all,
and reporting one of them would be reporting whichever suited the conclusion.
`scripts/pg-indexes.sh create` and `drop` move between the two states in half a
minute, so there was no reason to pick.

**The rule is one index per equality predicate in the suite, and nothing
else**: a btree on `PULocationID`, which q5 and q6 group by and q8 filters on,
and one on `payment_type`, which q3 and q8 filter on. No composite index, no
`INCLUDE (total_amount)`, nothing chosen after seeing which query was slow —
that is the line between benchmarking a database and benchmarking the person
tuning it.

The two together are **1.03 GiB**, which is far less than the 2.4 GiB two
btrees over 79.5 million rows would normally cost, because both columns are
low-cardinality — 265 zones and 5 payment types — and PostgreSQL's btree
deduplication collapses each key into a posting list of tuple ids.

What they do, from the plans:

| | with indexes | why |
|---|---|---|
| q1, q4, q5, q7 | unchanged, sequential | no equality predicate to index |
| q2, q6 | unchanged, sequential | the predicate is the partition key; pruning already did the work |
| q3 | **a different plan per partition, and per leg** | `payment_type = 2` is a seventh of the table — 16 index scans and 8 bitmap heap scans in the serial leg, 14 index scans, 8 bitmap heap scans and 2 sequential scans in the parallel one |
| q8 | **24 bitmap heap scans** | 470,349 rows out of 79.5 million; this is what an index is for |

q3 is the one worth looking at. The planner's choice is not uniform across the
24 partitions — some months get an index scan, some a bitmap heap scan, some
neither — and it is not the same split in the serial leg as in the parallel
one, because a parallel sequential scan is cheaper than a serial one and moves
the crossover. That is a thing the other two implementations cannot do and
cannot be hurt by, and it is why the `scan` field in the PostgreSQL records is
a histogram rather than one word.

## Configuration

A stock `postgresql.conf` is tuned for a small machine and is not a fair
configuration for analytical work; a configuration tuned against this suite
would not be fair either. What follows is the whole of it, in
`scripts/pg-server.sh`, with the reasoning attached rather than implied.

| setting | value | why |
|---|---|---|
| `shared_buffers` | 4 GB | The conventional quarter of a 24 GB machine. No setting holds the 12 GiB heap, so the OS page cache has to hold it and taking memory from the cache to double-buffer the same pages would make the warm leg colder, not warmer. |
| `work_mem` | 256 MB | Enough that nothing spills. It barely matters — the widest aggregate here groups into 266 zones — but a spill would measure the disk. |
| `maintenance_work_mem` | 2 GB | Index builds, reported above and not in the query numbers. |
| `effective_cache_size` | 16 GB | A planner hint, not an allocation, and true after the warm-up run. |
| `random_page_cost` | 1.1 | An NVMe SSD. Left at the rotating-disk default of 4.0 the planner refuses the index plan on q8 and the indexed leg measures nothing. |
| `max_parallel_workers` | 10 | Ten cores. `max_parallel_workers_per_gather` is set per session by the runner, not here. |
| `parallel_workers` on each partition | 9 | Pinned, so the parallel leg gets the worker count it asks for instead of the one PostgreSQL derives from the relation size. |
| `jit` | default (on) | PostgreSQL compiles aggregate expressions for scans this size. Turning it off would be tuning against the engine. |

**The suite runs the same two legs the Iceberg suite runs**, and for the same
reason: PostgreSQL parallelises analytical aggregates by default and does not
announce it, so comparing that against a single-worker Mojo scan is not a
comparison. The `single` leg sets `max_parallel_workers_per_gather = 0`, a
strictly serial plan, against PyIceberg's `set_cpu_count(1)`. The `threaded`
leg asks for 9 workers beside the leader — ten processes on ten cores —
against PyIceberg's ten threads.

## What is not comparable, query by query

- **q7 is not a wide decode here, and PostgreSQL should not be credited for
  winning it.** The query is "every column of one month": the Iceberg readers
  decode all 19 columns and stream them. A row store reads the whole tuple
  whether one column is asked for or nineteen, and `count(*)` does not deform
  the tuple at all, so PostgreSQL does strictly *less* work than the other two
  and any ratio above 1.0 on this row is an artefact of the question not
  making sense in a row store. Projection is free in a heap, and also useless.
- **q2 and q6 give PostgreSQL less than Iceberg gets.** Iceberg's residual
  evaluator proves a fully-contained partition already satisfies the filter and
  drops it, so no filter column is read at all. PostgreSQL prunes to the same
  partitions and then still evaluates `tpep_pickup_datetime >= … AND < …` per
  row, because the planner does not use the partition constraint to eliminate a
  redundant qual here. Same files, more work.
- **The fold is inside the engine on this side.** q5 and q6 aggregate in
  PostgreSQL's own hash aggregate; on the other two the group-by is application
  code over Arrow batches. The README already flags that the two folds are not
  equally optimised, and this is a third one.
- **q1 is the cleanest comparison in the suite.** One predicate on one column
  over the whole table, no pruning available to anyone, no index that could
  help, the aggregate trivial on all three sides. If one number in the
  PostgreSQL column is worth reading as a storage-engine comparison, it is that
  one.
- **`peak_rss_mb` in the PostgreSQL records is the client's**, not the
  server's, and is therefore meaningless next to the other two, where the
  client *is* the engine. It is in the record for shape. What bounds the server
  is `shared_buffers` and `work_mem`, and both are in the record too.

## The float sums

The gate allows counts to differ not at all and sums to differ by 1e-9
relative. PostgreSQL sums `double precision` in heap order, and in the parallel
leg each worker sums its own share and the leader combines them, so the last
bits land somewhere else than pyarrow's SIMD-blocked sum does. Two numbers,
both measured rather than assumed:

- **The largest disagreement with PyIceberg anywhere in the suite is 1.76e-11
  relative** (q4's `sum(tip_amount)` over 6.5 million rows), about fifty times
  inside the gate. The rest are between 3e-13 and 8e-12.
- **The parallel leg is not deterministic run to run.** Workers finish in
  whatever order they finish and the combine order follows, so the same query
  returns sums that differ across repeats by around 2e-14 relative. The serial
  leg is bit-identical every time.

The tolerance was not touched. `sum(x::numeric)` would have made the question
disappear by computing an exact decimal sum, and it would also have been a
different and much slower computation than the one the other two
implementations perform — the comparison would have stopped being a comparison
in order to pass its own gate.

## Results

Both index states, because the choice is part of the result.

| | unindexed | indexed | PyIceberg | vs PyIceberg |
|---|---:|---:|---:|---:|
| one thread | 40085.4 ms | **39523.8 ms** | 4247.5 ms | 0.11× |
| 9 workers + leader | 21406.8 ms | **20724.7 ms** | 1514.1 ms | 0.07× |

All eight answers agree in every combination.

**The indexes bought nothing: 1.4% on one thread and 3.2% on ten, both inside
the run-to-run spread, for 1053 MB on top of a 12.26 GiB heap.** That is not a
failure of the index; it is what this suite asks for. Seven of the eight
queries are full scans, where no index can help, and q8 — the one selective
query — returns 470,349 rows of 79,478,796. At 0.6% of a table this size a
bitmap heap scan is not meaningfully better than reading the heap, and on one
run it came out worse. An index earns its keep at a selectivity this workload
never asks for.

**PostgreSQL is 9–14× behind the columnar engines**, which is the honest result
for a row store asked to scan 79.5 million rows and touch two columns of
nineteen. It is here to show what a table format buys, not to lose a race it
was never entered in.

**q7 is the exception and should be discounted**, for the reason given above:
`count(*)` over one month never deforms a tuple, so PostgreSQL does strictly
less work than an engine decoding nineteen columns. It is the most misleading
number in this file.

## Running it

```sh
scripts/load.sh                  # the TLC Parquet, if it is not there yet
scripts/load-postgres.sh         # cluster on a free port, schema, COPY, ANALYZE
scripts/bench-postgres.sh        # both legs, then the answer diff
scripts/pg-indexes.sh create     # the indexed leg; `drop` goes back
scripts/load-postgres.sh --drop  # remove the cluster and its 13.5 GiB
```

Everything lives under `build/`, which is gitignored, and nothing installed on
the machine is touched: `scripts/pg-server.sh` runs `initdb` into `build/pg` on
a free port, and the server binaries come from a separate `postgres` pixi
environment so that the Mojo toolchain and PostgreSQL never have to solve
against each other. `pixi run -e postgres` is not required — the scripts find
the environment themselves.

`scripts/bench-postgres.sh` diffs against `build/results-python-*.jsonl`, so
run `scripts/bench.sh` first or the diff has nothing to compare with.
`compare.py` labels its first column `mojo` whatever is in it; the comparison
itself does not care which engines it is given.
