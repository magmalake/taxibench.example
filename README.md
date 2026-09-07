# taxibench.example

> Part of [**magmalake**](https://magmalake.org) — data lake building blocks in Mojo.

The same eight queries over the same Apache Iceberg table, run twice: once
through [iceberg-mojo](https://mojoshelf.org/tins/iceberg-mojo) and once through
PyIceberg 0.11.1, and packaged as two container images so the runtime cost of
each stack is a number rather than an impression.

This is a measurement, not a demo. Both implementations must agree on every
answer before any timing is worth reading, so `scripts/compare.py` diffs the
results and exits non-zero if they disagree.

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

Apple M4, 10 cores (4 performance), macOS 15, against
[iceberg-mojo 0.7.0](https://mojoshelf.org/tins/iceberg-mojo). Warm
cache, **p50 of 5 runs after a discarded warm-up, each query in its own
process**, machine gated quiet. Times are the whole scan — planning, Parquet
decode, filtering and the fold — which is what a caller actually waits for.

The suite runs **twice, with the thread count named on both sides**. pyarrow
reads Parquet multi-threaded by default and does not announce it, so a
single-worker Mojo scan compared against pyarrow's default is not a comparison.

### One thread each

`--workers 1` against `pa.set_cpu_count(1)`.

| query | files | rows out | iceberg.mojo | PyIceberg | ratio |
|---|---:|---:|---:|---:|---:|
| q1_scan_count | 24 | 77,929,134 | 599.8 ms | 299.8 ms | 0.50× |
| **q2_month_range** | 3 | 10,820,685 | **68.3 ms** | 125.9 ms | **1.84×** |
| q3_payment_sum | 24 | 11,944,903 | 1014.2 ms | 610.8 ms | 0.60× |
| q4_tip_ratio | 24 | 6,532,607 | 1316.2 ms | 778.3 ms | 0.59× |
| q5_top_zones | 24 | 77,929,134 | 876.3 ms | 699.0 ms | 0.80× |
| **q6_zone_revenue** | 12 | 41,169,300 | **345.0 ms** | 732.3 ms | **2.12×** |
| **q7_wide** | 1 | 3,539,142 | **169.7 ms** | 249.7 ms | **1.47×** |
| q8_selective | 24 | 470,349 | 1688.2 ms | 785.8 ms | 0.47× |
| **total** | | | **6077.8 ms** | **4281.5 ms** | **0.70×** |

### Ten threads each

`--workers 0` against `pa.set_cpu_count(10)`.

| query | files | rows out | iceberg.mojo | PyIceberg | ratio |
|---|---:|---:|---:|---:|---:|
| q1_scan_count | 24 | 77,929,134 | 198.6 ms | 122.7 ms | 0.62× |
| q2_month_range | 3 | 10,820,685 | 38.5 ms | 36.8 ms | 0.96× |
| q3_payment_sum | 24 | 11,944,903 | 268.8 ms | 152.9 ms | 0.57× |
| q4_tip_ratio | 24 | 6,532,607 | 329.2 ms | 205.8 ms | 0.63× |
| **q5_top_zones** | 24 | 77,929,134 | **347.8 ms** | 403.0 ms | **1.16×** |
| **q6_zone_revenue** | 12 | 41,169,300 | **167.0 ms** | 311.5 ms | **1.87×** |
| **q7_wide** | 1 | 3,539,142 | **54.1 ms** | 90.3 ms | **1.67×** |
| q8_selective | 24 | 470,349 | 381.8 ms | 208.2 ms | 0.55× |
| **total** | | | **1785.7 ms** | **1531.3 ms** | **0.86×** |

All eight answers agree in both legs, to exact equality on every count and to
within 1e-9 relative on every sum. Every cell came in at a p90/p50 spread of
1.15× or tighter.

**PyIceberg is faster overall — 1.42× on one thread, 1.17× on ten.** The split
between the queries it wins and the ones it loses is not arbitrary, and one
measurement explains all of it.

**A predicate the partition already guarantees costs nothing; any other
predicate costs 15 ns/row.** Q2, Q6 and Q7 filter on the partition column, so
their residual reduces to `true`, no filter column is read and no per-row check
runs — and those are exactly the three queries iceberg.mojo wins. Q1, Q3, Q4, Q5
and Q8 filter on data columns, so a residual is evaluated for every row, and
those are exactly the five it loses. Measured directly: adding one always-true
`trip_distance > -1` to a partition-aligned query costs **+52 ms** in
iceberg.mojo and **+11 ms** in pyarrow, over 3.5M rows. There is no third group,
and it is tracked as
[iceberg.mojo#14](https://github.com/magmalake/iceberg.mojo/issues/14).

With the residual out of the way the decode itself is ahead on both terms:
holding the file still and widening the projection from 1 to 19 columns fits
**~8.7 ms fixed + ~8.1 ms per column** against pyarrow's ~27.0 ms + ~11.3 ms.

Whole-suite scaling from one thread to ten is 3.4× for iceberg.mojo and 2.8× for
pyarrow, both short of 10 and consistent with the bend sitting at the four
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
```

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
- **The fold is part of the measurement.** Both sides pay for aggregation, and
  the two implementations of it are not equally optimised — that is most of
  where Q5 and Q6 come from.
- **The timings are native, not from the containers.** On macOS a container runs
  inside a VM over virtiofs, which distorts both CPU and I/O; the images are
  measured for size and verified to produce identical answers, but the numbers
  above come from the host binaries.
- **`plan_ms` is reported beside the total, not subtracted from it.** Planning
  runs again inside the read on both sides; the number is there to show it is
  small (0.3–3 ms) rather than to be added or removed.
- The TLC files are not schema-consistent. The 2023 months type the id columns
  as int64/double and spell the airport fee `airport_fee`; 2024 narrows them and
  spells it `Airport_fee`. `loader/load_table.py` casts both onto one schema.
  Rows whose pickup falls outside the month their file is named for — about
  1,150 of them, dated as far off as 2001 and 2098 — are dropped.
