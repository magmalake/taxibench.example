# taxibench.example

> Part of [**magmalake**](https://magmalake.org) — data lake building blocks in Mojo.

This repo exists for three reasons:

- further validate the [iceberg-mojo](https://mojoshelf.org/tins/iceberg-mojo)
  implementation
- compare its performance with the Python implementation, PyIceberg 0.11.1
- evaluate it against a materially different approach, a relational database

The same eight queries are run over the data in the representation native to
each implementation. This is a measurement, not a demo. Every implementation
must agree on every answer before any timing is worth reading, so
`scripts/compare.py` diffs the results and exits non-zero if they disagree.

The two Iceberg readers below read **one physical table**, so that comparison is
apples to apples and is what this repository certifies.

**[PostgreSQL](POSTGRES.md) answers the same eight questions from a relational
database**, over its own copy of the rows. It is a common belief that a
relational database does well enough on analytical work; that document is an
attempt to measure what it actually costs. It is a separate file because the
answer needs its own context — matching partitioning, configuration, and both
index states — and because mixing it into the tables below would invite reading
it as a race.

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

## Results

These two read the same files. [PostgreSQL](POSTGRES.md) reads its own copy and
keeps its numbers in its own document.

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

```

[POSTGRES.md](POSTGRES.md) has the commands for the relational leg; it raises
its own cluster under `build/` and touches nothing installed on the machine.

`TAXIBENCH_MONTHS=2 scripts/load.sh` builds a much smaller table for iterating.
`pixi run build-probe` builds `src/probe.mojo`, which runs one scan with any
projection and filter — the tool for isolating a cost rather than reporting a
benchmark.

The warehouse is mounted into the containers at the same absolute path it has on
the host. An Iceberg table's metadata names its data files by absolute location,
so a table generated at `/x/build/warehouse` is only readable at
`/x/build/warehouse` unless the reader rewrites paths on the way through.

## Caveats

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
