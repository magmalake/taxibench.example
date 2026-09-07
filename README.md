# taxibench.example

> Part of [**magmalake**](https://magmalake.org) — data lake building blocks in Mojo.

This repo exists for 3 reasons:

- further validate the [iceberg-mojo](https://mojoshelf.org/tins/iceberg-mojo)o implementation
- compare the performance with the python implementation, PyIceberg 0.11.1
- further evaluate against much more different approaches like Postgres and LanceDB

The same eight queries are run over the data using representations
native to the various implementations. This is a measurement, not a demo. Every implementation must agree on every
answer before any timing is worth reading, so `scripts/compare.py` diffs the
results and exits non-zero if they disagree.

We start with Apache Iceberg table for the two Iceberg implementations.

A third implementation answers the same eight questions from **PostgreSQL**.
It is not a third reader of the Iceberg table: it holds its own copy of the
same 79,478,796 rows in its own row-oriented heap, and on full-table analytical
scans a row store is expected to lose. It is here because the size of that loss
is the closest thing this repository has to a measurement of what a table
format is worth. Read [PostgreSQL](#postgresql) before reading its numbers as
a race.


The LanceDB approach reads a Lance dataset: a second
copy of the same 79,478,796 rows, in a different file format, 3.5× the size,
produced by a conversion step neither of the other two pays for. Two
implementations of one table format reading one physical table is an
apples-to-apples comparison; adding Lance turns this into the same analytical
workload across storage engines, which is a looser and more interesting claim.
A Lance number and an Iceberg number here are not measuring the same thing end
to end. [The Lance leg](#the-lance-leg) is where that is spelled out, and the
answers are still held to the same gate.

## What is measured

79,478,796 NYC yellow-taxi trips — every month of 2023 and 2024 from the
[TLC trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
— as an Iceberg v2 table partitioned by `month(tpep_pickup_datetime)`: 24 data
files, 24 snapshots, 1.3 GB of Parquet.

**PyIceberg writes the table.** Neither implementation under test creates it, so
neither gets a layout arranged to suit it, and what both read is a table a
PyIceberg user would actually have.

Eight queries, each of which has to read data. A bare `count(*)` is deliberately
absent: both implementations answer it from the manifests without opening a
file, so it measures nothing here. Q1 counts rows passing a predicate instead.

| | query | shape |
|---|---|---|
| Q1 | `count(*) where trip_distance > 0` | full scan, 1 column, no pruning possible |
| Q2 | `sum(total_amount)` over one quarter | partition pruning to 3 of 24 |
| Q3 | `sum(total_amount) where payment_type = 2` | full scan, 2 columns |
| Q4 | tip ratio on trips over 10 miles | full scan, 3 columns, 2 predicates |
| Q5 | busiest pickup zones | full scan, dense group-by over 266 zones |
| Q6 | revenue by pickup zone, 2024 | pruning to 12, then a grouped sum |
| Q7 | every column of one month | 1 file, all 19 columns — wide decode |
| Q8 | long cash trips from one zone | highly selective; statistics should prune |

Iceberg gives neither side an aggregation kernel, because aggregation is not a
table format's job. Each implementation brings its own: `src/taxibench/agg.mojo`
folds Arrow buffers directly, and `python/taxibench/queries.py` uses
`pyarrow.compute`. Both fold batch by batch, in their own idiom rather than a
transliteration of the other's.

## The Lance leg

[Lance](https://lancedb.github.io/lance/) is a columnar file format with its own
reader, not another way of reading Parquet, so the third engine needs its own
copy of the data. `loader/convert_lance.py` transcodes the Iceberg table's 24
Parquet data files into a Lance dataset of 24 fragments, one per month —
79,478,796 rows, the same count, and by construction rather than by luck.

**The source is the table's data files, not the raw TLC Parquet.** The loader
does real work on the way in: it casts the two schemas TLC publishes onto one,
and drops the ~1,150 rows whose pickup timestamp falls outside the month their
file is named for. Reproducing that would be a second implementation of it, and
a second implementation off by one row would make every answer differ for a
reason that has nothing to do with Lance. Reading what PyIceberg already wrote
inherits the normalisation instead of repeating it.

`python/taxibench_lance/` runs the suite. It imports the folds from
`python/taxibench/queries.py` — the PyIceberg leg's own aggregation code, which
needs nothing but pyarrow — so between the two Python legs the only thing that
differs is how the Arrow batches are produced. Both are on pyarrow 25.0.1.

### What a Lance number does not measure

**It is a separate copy, and it is much bigger.** Nothing here is free:

| | on disk | vs Iceberg | to build |
|---|---:|---:|---:|
| Iceberg table, zstd Parquet | 1.37 GB | — | (the load) |
| Lance dataset, as written by default | **4.79 GB** | 3.5× | 5.9 s |
| Lance dataset, `--compress zstd` | 2.55 GB | 1.9× | 8.6 s |

The conversion is 5.9 s of wall clock on ten cores — 6.8 s including the flush
to disk — with both source and destination on local NVMe and the source already
in page cache. It is not a slow step. It is a step the other two engines do not
have, and the queries are timed after it, so it appears in no table below.

**Lance is uncompressed by default, and this benchmark runs warm.** The stable
format applies structural encodings — bit-packing, dictionaries, run-length —
but no general-purpose block compressor unless a field asks for one, which is
why the same rows take 3.5× the space. On a warm page cache, 3.5× the bytes is
close to free and skipping zstd is not: the benchmark hands Lance the copy of
the data that needs no decompression and then charges it nothing for the size.
That is the single biggest way this comparison flatters Lance. Cold cache,
network storage, or a machine whose RAM will not hold 4.79 GB would all read
differently, and `TAXIBENCH_COMPRESS=zstd scripts/convert-lance.sh` builds the
1.9× dataset if you want to price it.

**The pruning is handed to Lance; Iceberg does it for itself.** A Lance dataset
is a flat list of fragments and the format records nothing about what is in
one — there is no partition spec, no partition value on a data file, and no
manifest summary to test a predicate against. So the converter writes
`fragments.json` beside the dataset, recording each fragment's month and its
pickup-timestamp bounds, and the runner prunes against that. It is what gives
q2, q6 and q7 their 3, 12 and 1 fragments, and it is also how a fragment the
range covers entirely is read with no filter at all, which is exactly the
reduction Iceberg's residual evaluator performs. Iceberg maintains that
metadata as a property of the table; here it is a sidecar written by hand to
make the two sides comparable. The idiomatic Lance answer is a scalar index on
`tpep_pickup_datetime`, which is a different mechanism with its own build cost
and which prunes row ranges rather than files — worth measuring, but it would
end the file-count comparison rather than fit into it.

**The read batch had to be set.** Lance reads 8,192 rows at a time by default,
which a selective filter thins to about 2,000 by the time the fold sees them,
where PyIceberg hands the same fold batches of 17,000 to 129,000. With the same
aggregation code on both legs, that difference is measured as Lance being slow
when what is slow is a Python loop running eight times as often — on q5, 9,712
fold calls against 617. The Lance leg therefore reads in batches of 131,072,
which puts its granularity where PyIceberg's already is and is where the curve
flattens. `--batch-size` exposes it, and the batch stays bounded either way.

**Nothing is sorted or clustered on the way through.** This is the leg-up a
conversion step usually gets, and it is not taken: the row order inside each
fragment is the row order of the Iceberg data file, which is the row order of
the TLC file. q8's predicate is on `PULocationID`, which is unsorted on both
sides, so neither format's statistics can skip much of it. Sorting by pickup
zone would make q8 collapse on the Lance side and would be worth reporting as
its own result — but not in a table next to a table that was not sorted.

### Where Lance is the better tool, and where it is not

Lance is built for random access: fetching rows by id, reading a single column
without touching the rest of the file, adding a column without rewriting the
data, and vector search over embeddings. Those are the things Parquet is bad
at, and none of them is in this suite. What is in this suite — full scans,
predicates over a fixed set of columns, and grouped aggregation — is what
Parquet was designed for and what every part of the Iceberg stack is tuned
around. Whatever the numbers say, they are numbers from Parquet's home ground,
and a reader deciding between the two formats should weigh a scan benchmark
accordingly.

There is a `lancedb-mojo` tin on [mojoshelf](https://mojoshelf.org), an FFI
binding to a Rust cdylib, but it binds LanceDB's vector-store surface — open a
table, add and delete rows, build an index, count, search by vector. There is
no projected scan returning Arrow batches, so it cannot express these eight
queries; the analytical path would need Lance's dataset scanner rather than the
LanceDB table API. This leg is Python for that reason.

There is no third container image either. The image-size table below is about
how tightly each Iceberg stack packages; a Lance image would be measuring a
different question.

### Lance results

Same machine and same discipline as the tables below.
`scripts/bench-lance.sh` runs the Lance leg and a PyIceberg leg in one session,
so the pair is internally consistent.

| | Lance | PyIceberg | | vs iceberg.mojo |
|---|---:|---:|---:|---|
| one thread | **1537.3 ms** | 4322.3 ms | 2.81× | Lance **2.06×** faster |
| ten threads | **1504.5 ms** | 1534.8 ms | 1.02× | iceberg.mojo **1.67×** faster |

All eight answers agree. Two things there deserve more than a glance.

**Lance is the fastest single-threaded engine here by a wide margin**, and on
the queries that prune it is not close: q2 is 9.1 ms against PyIceberg's 125.5
and iceberg.mojo's 42.0.

**But it barely scales — 1537 ms to 1504 ms, a 1.02× speedup from ten
threads.** That is reproducible across both passes rather than one noisy cell,
and it is why the same engine leads one leg and trails the other. We have not
chased it down. Either the reader is not spending the thread budget it is
given, or the shared Python fold — the same code the PyIceberg leg runs —
becomes the ceiling once the reader stops being the bottleneck, which the
batch-size finding above makes plausible. Read it as an open question about
this leg rather than a verdict on Lance's threading.

One measurement note. In the session that produced the threaded numbers, the
PyIceberg leg's q5 recorded a p50 of 404.5 ms against a p90 of 1207.5 ms. Its
p50 agrees with the other two sessions, so the leg is sound, but that p90
records interference — so the table above uses the clean PyIceberg figures from
the Iceberg session.

## Results: the two Iceberg readers

Lance and PostgreSQL each read a separate copy of the data in their own format,
so their numbers belong under their own sections rather than in the tables
below. These two read the same files.

Apple M4, 10 cores (4 performance), macOS 15, against
[iceberg-mojo 0.7.2](https://mojoshelf.org/tins/iceberg-mojo) on
[parquet-mojo 0.8.0](https://mojoshelf.org/tins/parquet-mojo). Warm cache,
**p50 of 5 runs after a discarded warm-up, each query in its own process**,
machine gated quiet. Times are the whole scan — planning, Parquet decode,
filtering and the fold — which is what a caller actually waits for.

The suite runs **twice, with the thread count named on both sides**. pyarrow
reads Parquet multi-threaded by default and does not announce it, so a
single-worker Mojo scan compared against pyarrow's default is not a comparison.

### One thread each

`--workers 1` against `pa.set_cpu_count(1)`.

| query | files | rows out | iceberg.mojo | PyIceberg | ratio |
|---|---:|---:|---:|---:|---:|
| q1_scan_count | 24 | 77,929,134 | 289.3 ms | 296.4 ms | 1.02× |
| q2_month_range | 3 | 10,820,685 | 42.0 ms | 123.2 ms | 2.94× |
| q3_payment_sum | 24 | 11,944,903 | 440.4 ms | 606.6 ms | 1.38× |
| q4_tip_ratio | 24 | 6,532,607 | 684.9 ms | 766.9 ms | 1.12× |
| q5_top_zones | 24 | 77,929,134 | 527.4 ms | 699.4 ms | 1.33× |
| q6_zone_revenue | 12 | 41,169,300 | 245.2 ms | 728.8 ms | 2.97× |
| q7_wide | 1 | 3,539,142 | 166.8 ms | 246.6 ms | 1.48× |
| q8_selective | 24 | 470,349 | 768.8 ms | 779.5 ms | 1.01× |
| **total** | | | **3164.8 ms** | **4247.5 ms** | **1.34×** |

### Ten threads each

`--workers 0` against `pa.set_cpu_count(10)`.

| query | files | rows out | iceberg.mojo | PyIceberg | ratio |
|---|---:|---:|---:|---:|---:|
| q1_scan_count | 24 | 77,929,134 | 75.6 ms | 122.9 ms | 1.63× |
| q2_month_range | 3 | 10,820,685 | 21.3 ms | 36.5 ms | 1.72× |
| q3_payment_sum | 24 | 11,944,903 | 101.3 ms | 153.1 ms | 1.51× |
| q4_tip_ratio | 24 | 6,532,607 | 173.7 ms | 201.3 ms | 1.16× |
| q5_top_zones | 24 | 77,929,134 | 208.3 ms | 401.9 ms | 1.93× |
| q6_zone_revenue | 12 | 41,169,300 | 95.4 ms | 309.6 ms | 3.24× |
| q7_wide | 1 | 3,539,142 | 50.9 ms | 86.8 ms | 1.70× |
| q8_selective | 24 | 470,349 | 175.4 ms | 202.1 ms | 1.15× |
| **total** | | | **901.9 ms** | **1514.1 ms** | **1.68×** |

All eight answers agree in both legs, to exact equality on every count and to
within 1e-9 relative on every sum. Every cell came in at a p90/p50 spread of
1.06× or tighter.

**iceberg.mojo is ahead on every query in both legs — 1.34× on one thread and
1.68× on ten.** Three changes across iceberg-mojo 0.7.0–0.7.2 got it there, and
they are worth separating because they act on different things:

- **A predicate the partition already guarantees now costs nothing.** The
  residual evaluator reduces a boundary-aligned range to `true`, so the filter
  column is neither read nor tested per row — q2 and q6, at 2.9× and 3.0×.
- **The residual that remains is evaluated a vector at a time** rather than a
  row at a time, which is most of q3, q4 and q8.
- **A scan fetches only the column chunks its projection reaches.** Four
  columns of nineteen is 13 MiB of a 60 MiB file, and reading the other 47 MiB
  cost more than decompressing the 13.

**The threaded leg gains more than the single one, and that is the projection
change.** Removing I/O parallelises better than removing CPU did: iceberg.mojo
scales 3.5× from one thread to ten where pyarrow scales 2.8×. Both fall well
short of 10 and both bend at the four performance cores, but the gap widens
with workers rather than closing.

## PostgreSQL

`python/taxibench/postgres.py` answers the same eight questions in SQL against
PostgreSQL 18.4, over its own copy of the same rows, and
`scripts/compare.py` diffs those answers against PyIceberg's with no changes
of any kind: **all eight agree, every count exactly and every sum to within
1.8e-11 relative** against a gate of 1e-9.

### What this is not

The two implementations above are two readers of one physical table. This one
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

### The table

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

### What it costs to load, and to hold

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

### Indexes

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

### Configuration

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

### What is not comparable, query by query

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

### The float sums

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

### Results

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

### Running it

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

## Image size

Both images are built as carefully as each other: the Python one installs
`pyiceberg[pyarrow]` rather than dragging in SQLAlchemy and the catalog drivers
the benchmark never imports, and strips bundled test suites and headers. Both
are `linux/arm64`, and all three answer the suite identically.

| image | base | total | vs Python |
|---|---|---:|---:|
| `taxibench-python` | `python:3.12-slim-bookworm` (214 MB) | **521 MB** | — |
| `taxibench-mojo` | `distroless/cc-debian12` (47.6 MB) | **126 MB** | 4.1× smaller |
| `taxibench-mojo:local` | `distroless/cc-debian12` (47.6 MB) | **57 MB** | 9.1× smaller |

The compiled binary is **2.1 MB**. Everything else in the Mojo images is the
Mojo runtime and the C libraries the tins open at runtime.

**The two Mojo images differ only in whether object storage is included.**
`objectstore-mojo` wraps libcurl, and on conda-forge libcurl pulls OpenSSL,
Kerberos, libssh2, nghttp2 and libpsl — and libpsl links ICU, whose character
database alone is 33 MB. Verified with `ldd`: libpsl is the only consumer of ICU
in the closure, and it wants it for the public-suffix list that matches cookie
domains. A scan of a table on a local filesystem cannot reach any of it.
Building with `--build-arg TAXIBENCH_OBJECTSTORE=0` removes 69 MB and costs
`s3://`, `gs://` and `az://`.

Two things the packaging has to get right:

- **Nothing in the closure appears in `ldd` output.** The binary links only the
  Mojo runtime and libc; every C library is reached through a shim the tin
  `dlopen`s by name, so the dependency closure has to be seeded by hand.
  `docker/collect-libs.sh` does that, and the image sets `CONDA_PREFIX` because
  outside a pixi environment the shim lookup falls back to a path relative to
  the working directory and finds nothing.
- **Do not ship a C++ runtime the base already has.** Copying conda's
  `libstdc++`/`libgcc_s` alongside distroless/cc's own costs 33 MB for nothing.

## Running it

```sh
scripts/load.sh                  # download ~1.2 GB of TLC Parquet, build the table
pixi run build                   # the Mojo binary
scripts/bench.sh                 # both legs, both implementations, then the answer diff
TAXIBENCH_DOCKER=1 scripts/bench.sh   # the same, through the two images

scripts/convert-lance.sh         # transcode the table into build/lance (4.79 GB)
scripts/bench-lance.sh           # Lance against PyIceberg, then the answer diff
scripts/load-postgres.sh         # the PostgreSQL copy; see the section above
scripts/bench-postgres.sh        # and its two legs, diffed against PyIceberg's answers
```

Lance installs into its own virtual environment, `build/venv-lance`. The
PyIceberg environment is what one of the two engines under test is measured
through, and its pyarrow is pinned at 25.0.1; letting a resolver move that while
installing something unrelated would silently re-time the benchmark. The two end
up on the same pyarrow either way, which is what the comparison needs.

`TAXIBENCH_MONTHS=2 scripts/load.sh` builds a much smaller table for iterating.
`pixi run build-probe` builds `src/probe.mojo`, which runs one scan with any
projection and filter — the tool for isolating a cost rather than reporting a
benchmark.

The warehouse is mounted into the containers at the same absolute path it has on
the host. An Iceberg table's metadata names its data files by absolute location,
so a table generated at `/x/build/warehouse` is only readable at
`/x/build/warehouse` unless the reader rewrites paths on the way through.

## Caveats

- **The Lance leg reads a different table.** Not a different reader over the
  same files: a converted copy, in another format, 3.5× the size, with the
  pruning metadata supplied by hand. [The Lance leg](#the-lance-leg) is the
  section, not this bullet, because it is not a footnote to the numbers.
- **One machine, one shape of data.** Twenty-four files of about 3 million rows
  each, all local, all zstd. Object storage, many small files, or a table with
  row-level deletes would each move these numbers.
- **The fold is part of the measurement.** Every side pays for aggregation, and
  the implementations of it are not equally optimised — that is most of where
  Q5 and Q6 come from. On the PostgreSQL side the fold is not even in the same
  process: it is the server's own hash aggregate.
- **PostgreSQL is not reading the Iceberg table.** It is a row store over its
  own 12.26 GiB copy of the same rows, so the comparison is between two
  storage designs and not between two readers of one. What is and is not
  comparable is set out query by query under
  [PostgreSQL](#what-is-not-comparable-query-by-query).
- **The timings are native, not from the containers.** On macOS a container runs
  inside a VM over virtiofs, which distorts both CPU and I/O; the images are
  measured for size and verified to produce identical answers, but the numbers
  above come from the host binaries.
- **`plan_ms` is reported beside the total, not subtracted from it.** Planning
  runs again inside the read on every side; the number is there to show it is
  small (0.3–3 ms) rather than to be added or removed. On the PostgreSQL side
  it is the server's own `Planning Time` from `EXPLAIN (SUMMARY ON)`, which is
  0.04–3.5 ms and covers pruning 24 partitions and costing them.
- The TLC files are not schema-consistent. The 2023 months type the id columns
  as int64/double and spell the airport fee `airport_fee`; 2024 narrows them and
  spells it `Airport_fee`. `loader/load_table.py` casts both onto one schema.
  Rows whose pickup falls outside the month their file is named for — about
  1,150 of them, dated as far off as 2001 and 2098 — are dropped.
