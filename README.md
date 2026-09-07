# taxibench.example

> Part of [**magmalake**](https://magmalake.org) — data lake building blocks in Mojo.

The same eight queries over the same Apache Iceberg table, run twice: once
through [iceberg-mojo](https://mojoshelf.org/tins/iceberg-mojo) and once through
PyIceberg 0.11.1, and packaged as two container images so the runtime cost of
each stack is a number rather than an impression.

This is a measurement, not a demo. Both implementations must agree on every
answer before any timing is worth reading, so `scripts/compare.py` diffs the
results and exits non-zero if they disagree.

There is now a third engine, and it changes what this repository is comparing.
**Lance does not read the Iceberg table.** It reads a Lance dataset: a second
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

**Not yet measured.** The implementation is in place and it passes the answer
gate — all eight answers agree with PyIceberg, exactly on every count and
within 1e-9 relative on every sum, and the fragment counts line up with the
file counts at 24, 3, 24, 24, 24, 12, 1, 24. The timings wait on a quiet
machine, which is the same rule the two tables below were taken under.
`scripts/bench-lance.sh` produces them: the Lance leg and the PyIceberg leg in
one session, both told the same thread count, each query in its own process.

## Results

Apple M4, 10 cores (4 performance), macOS 15, against
[iceberg-mojo 0.7.1](https://mojoshelf.org/tins/iceberg-mojo). Warm cache,
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
| q1_scan_count | 24 | 77,929,134 | 483.8 ms | 306.5 ms | 0.63× |
| **q2_month_range** | 3 | 10,820,685 | **61.8 ms** | 126.5 ms | **2.05×** |
| **q3_payment_sum** | 24 | 11,944,903 | **599.9 ms** | 614.4 ms | **1.02×** |
| q4_tip_ratio | 24 | 6,532,607 | 838.1 ms | 783.2 ms | 0.93× |
| **q5_top_zones** | 24 | 77,929,134 | **686.8 ms** | 694.2 ms | **1.01×** |
| **q6_zone_revenue** | 12 | 41,169,300 | **325.1 ms** | 737.2 ms | **2.27×** |
| **q7_wide** | 1 | 3,539,142 | **170.9 ms** | 253.0 ms | **1.48×** |
| q8_selective | 24 | 470,349 | 914.0 ms | 796.3 ms | 0.87× |
| **total** | | | **4080.4 ms** | **4311.3 ms** | **1.06×** |

### Ten threads each

`--workers 0` against `pa.set_cpu_count(10)`.

| query | files | rows out | iceberg.mojo | PyIceberg | ratio |
|---|---:|---:|---:|---:|---:|
| q1_scan_count | 24 | 77,929,134 | 183.1 ms | 121.6 ms | 0.66× |
| q2_month_range | 3 | 10,820,685 | 38.2 ms | 37.3 ms | 0.98× |
| q3_payment_sum | 24 | 11,944,903 | 207.4 ms | 153.6 ms | 0.74× |
| q4_tip_ratio | 24 | 6,532,607 | 274.9 ms | 205.9 ms | 0.75× |
| **q5_top_zones** | 24 | 77,929,134 | **318.8 ms** | 392.1 ms | **1.23×** |
| **q6_zone_revenue** | 12 | 41,169,300 | **149.7 ms** | 310.9 ms | **2.08×** |
| **q7_wide** | 1 | 3,539,142 | **51.7 ms** | 88.9 ms | **1.72×** |
| q8_selective | 24 | 470,349 | 280.0 ms | 203.6 ms | 0.73× |
| **total** | | | **1503.8 ms** | **1513.9 ms** | **1.01×** |

All eight answers agree in both legs, to exact equality on every count and to
within 1e-9 relative on every sum. Every cell came in at a p90/p50 spread of
1.12× or tighter.

**The two stacks are level: 1.06× on one thread, 1.01× on ten.** That is close
enough that the totals should be read as a tie rather than a win — a few percent
is within what a different machine or a different month of data would move.

What is not a tie is the spread underneath. iceberg.mojo is **2.0×–2.3× faster
on the two partition-filtered queries** (q2, q6) and **1.5×–1.7× on the
single-file wide scan** (q7), and **0.63×–0.75× on q1 and q8**. Those are
different queries with different bottlenecks, and averaging them into one number
hides more than it shows.

**Where it wins:** a predicate on the partition column reduces to nothing.
Iceberg's residual evaluator proves the partition value already satisfies the
filter, so no filter column is read and no per-row check runs — q2 scans three
files in 61.8 ms where PyIceberg takes 126.5 ms. q7 wins for an unrelated
reason: it touches one file, and the scan spends its whole worker budget on the
row groups inside it rather than leaving nine cores idle.

**Where it loses, the cost is Parquet decode, not Iceberg.** Profiling the scan
by stage puts 155 ms of q1's 446 ms and 548 ms of q8's 929 ms in the decoder;
the Iceberg layer above it — cast, residual, filter, Arrow assembly — is 51 to
119 ms per query. q8 is the sharpest case: it decodes four columns over 79.5M
rows to return 470,349, and its predicate is on an unsorted column, so page
statistics cannot skip the work. That is
[parquet.mojo](https://github.com/magmalake/parquet.mojo)'s half of the stack
and it is where the remaining difference lives.

Whole-suite scaling from one thread to ten is 2.7× for iceberg.mojo and 2.8× for
pyarrow, both well short of 10 and consistent with the bend sitting at the four
performance cores rather than the ten logical ones.

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
- **The fold is part of the measurement.** Both sides pay for aggregation, and
  the two implementations of it are not equally optimised — that is most of
  where Q5 and Q6 come from.
- **The timings are native, not from the containers.** On macOS a container runs
  inside a VM over virtiofs, which distorts both CPU and I/O; the images are
  measured for size and verified to produce identical answers, but the numbers
  above come from the host binaries.
- **`plan_ms` is reported beside the total, not subtracted from it.** Planning
  runs again inside the read on both Iceberg sides; the number is there to show
  it is small (0.3–3 ms) rather than to be added or removed. The Lance leg is
  the exception: its scanner is handed the fragments the plan chose and does
  not plan again, so its `plan_ms` is work that happens once.
- The TLC files are not schema-consistent. The 2023 months type the id columns
  as int64/double and spell the airport fee `airport_fee`; 2024 narrows them and
  spells it `Airport_fee`. `loader/load_table.py` casts both onto one schema.
  Rows whose pickup falls outside the month their file is named for — about
  1,150 of them, dated as far off as 2001 and 2098 — are dropped.
