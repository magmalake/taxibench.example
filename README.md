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
absent: PyIceberg answers it from the manifests without opening a file, and a
benchmark where one side does a metadata lookup and the other reads 79 million
rows measures nothing. Q1 counts rows passing a predicate instead.

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
`pyarrow.compute`. Both fold batch by batch, and both use their own idiom rather
than a transliteration of the other's.

## Results

Apple M4, 10 cores (4 performance), macOS 15. Warm cache, **p50 of 5 runs after
a discarded warm-up, each query in its own process**, on a machine gated quiet
before the run. Times are the whole scan — planning, Parquet decode, filtering
and the fold — which is what a caller actually waits for.

The suite runs **twice, with the thread count named on both sides**. That is not
ceremony: pyarrow reads Parquet multi-threaded by default and does not announce
it, so a single-worker Mojo scan compared against pyarrow's default is not a
comparison at all. The first draft of this file made exactly that mistake.

### One thread each

`--workers 1` against `pa.set_cpu_count(1)`.

| query | files | rows out | iceberg.mojo | PyIceberg | ratio |
|---|---:|---:|---:|---:|---:|
| q1_scan_count | 24 | 77,929,134 | 614.6 ms | 304.0 ms | 0.49× |
| q2_month_range | 3 | 10,820,685 | 148.0 ms | 127.4 ms | 0.86× |
| q3_payment_sum | 24 | 11,944,903 | 1031.1 ms | 617.7 ms | 0.60× |
| q4_tip_ratio | 24 | 6,532,607 | 1304.6 ms | 789.4 ms | 0.61× |
| q5_top_zones | 24 | 77,929,134 | 917.1 ms | 691.4 ms | 0.75× |
| **q6_zone_revenue** | 12 | 41,169,300 | **425.4 ms** | 815.5 ms | **1.92×** |
| **q7_wide** | 1 | 3,539,142 | **187.0 ms** | 255.1 ms | **1.36×** |
| q8_selective | 24 | 470,349 | 1730.1 ms | 819.2 ms | 0.47× |
| **total** | | | **6358.0 ms** | **4419.8 ms** | **0.70×** |

### Ten threads each

`--workers 0` against `pa.set_cpu_count(10)`.

| query | files | rows out | iceberg.mojo | PyIceberg | ratio |
|---|---:|---:|---:|---:|---:|
| q1_scan_count | 24 | 77,929,134 | 196.4 ms | 124.0 ms | 0.63× |
| q2_month_range | 3 | 10,820,685 | 78.5 ms | 37.5 ms | 0.48× |
| q3_payment_sum | 24 | 11,944,903 | 278.1 ms | 153.8 ms | 0.55× |
| q4_tip_ratio | 24 | 6,532,607 | 334.1 ms | 207.6 ms | 0.62× |
| **q5_top_zones** | 24 | 77,929,134 | **337.4 ms** | 405.9 ms | **1.20×** |
| **q6_zone_revenue** | 12 | 41,169,300 | **186.8 ms** | 316.1 ms | **1.69×** |
| q7_wide | 1 | 3,539,142 | 190.6 ms | 88.6 ms | 0.46× |
| q8_selective | 24 | 470,349 | 383.7 ms | 204.8 ms | 0.53× |
| **total** | | | **1985.6 ms** | **1538.2 ms** | **0.77×** |

All eight answers agree in both legs, to exact equality on every count and to
within 1e-9 relative on every sum.

**PyIceberg is faster overall in both legs** — 1.44× on one thread, 1.29× on
ten. That is the honest headline. What follows is why, not an excuse.

**Where Mojo wins, it wins on the fold.** Q6 is a grouped sum and Q5 a grouped
count, and the dense 266-slot accumulator in `agg.mojo` — one indexed add per
row, no hashing — beats a pyarrow hash aggregation followed by a Python-level
loop over the groups. Q6 reads 41 million rows and stays ahead in both legs.

**The single-file query is the clearest structural gap.** Q7 is
**187.0 ms on one thread and 190.6 ms on ten — it does not move**, because
`ScanOptions.num_workers` parallelizes across *data files*, one worker per
`FileScanTask`, and Q7 touches one file. pyarrow goes 255.1 → 88.6 ms on the
same query by parallelizing row groups *within* the file. Note the direction on
one thread: Mojo is **1.36× faster** there. The decode is not what is behind;
the parallel decomposition is.

**Q8 is a real weakness and not a threading artefact.** It returns 470k rows of
79M and is Mojo's worst ratio in both legs, barely improving with workers.
Whatever the selective path is doing, adding cores does not fix it.

Whole-suite scaling is 3.2× for Mojo and 2.9× for pyarrow going from one thread
to ten — both well short of 10, and consistent with the bend sitting at the
four performance cores rather than the ten logical ones.

**Two cells were not steady and should be read as soft**: `q1` at ten threads
(p90/p50 = 1.83×) and PyIceberg's `q6` on one thread (1.56×). Every other cell
in both legs came in at 1.09× or tighter. An earlier contended run reported
q5 single-thread at 2866 ms; quiet, it is 917 ms. That 3.1× is the measurement
error you get from not checking, and it is why the spread is reported at all.

### What the single-file gap was, and what closing it is worth

The tables above are iceberg-mojo **0.6.7**, which is what `pixi shelf add`
installs today. The Q7 diagnosis turned out to be half right, and the half that
was wrong is the more useful half.

parquet.mojo **0.7.0 already parallelises inside a file** — it flattens
*(row group, leaf)* pairs into one work list, which is the right shape. The
missing piece was one level up: `iceberg.mojo` spent its whole worker budget on
the file axis and never told the reader it could use threads, so a one-file scan
left every core but one idle. Splitting the budget — `min(w, n)` files at once,
`w // min(w, n)` threads inside each, so only the slack the file axis cannot use
goes inward — gives this, measured the same way on the same machine:

| query | files | 0.6.7 | with the split | |
|---|---:|---:|---:|---|
| **q7_wide** | 1 | 177.3 ms | **64.6 ms** | **2.74×** |
| q2_month_range | 3 | 76.5 ms | 68.5 ms | 1.12× |
| q1_scan_count | 24 | 200.4 ms | 200.3 ms | flat |

Q7 goes from **0.46× to 1.36×** against PyIceberg, and the suite total from
1985.6 ms to 1839.4 ms — all answers still agreeing. The 24-file queries are
unchanged, which is the point: at `n >= w` the split is byte-for-byte the old
path, so this is not the even halving between axes that costs more than it wins.

The q7 worker ladder is **181.7 / 113.4 / 90.3 / 78.4 / 71.7 / 67.8 / 64.4 ms**
at 1/2/3/4/6/8/10 workers. The bend is at four — the performance-core count, not
the ten logical ones — and a fitted serial fraction of 25–28% caps this at about
3.5–4×. That quarter is iceberg's own per-batch casting, residual evaluation,
delete application and Arrow assembly, all still on the calling thread. More
workers will not move it; that stage is where the next factor is.

This is measured from an unmerged branch and is not in any published version.

## Image size

Both images are built as carefully as each other: the Python one installs
`pyiceberg[pyarrow]` rather than dragging in SQLAlchemy and the catalog drivers
the benchmark never imports, and strips bundled test suites and headers. Both
are `linux/arm64`, built and run on the same machine, and all three answer the
suite identically.

| image | base | total | vs Python |
|---|---|---:|---:|
| `taxibench-python` | `python:3.12-slim-bookworm` (214 MB) | **521 MB** | — |
| `taxibench-mojo` | `distroless/cc-debian12` (47.6 MB) | **126 MB** | 4.1× smaller |
| `taxibench-mojo:local` | `distroless/cc-debian12` (47.6 MB) | **57 MB** | 9.1× smaller |

The compiled binary is **2.1 MB**. Everything else in the Mojo images is the
Mojo runtime and the C libraries the tins open at runtime, and the interesting
part is which ones.

**The two Mojo images differ only in whether object storage is included.**
`objectstore-mojo` wraps libcurl, and on conda-forge libcurl pulls OpenSSL,
Kerberos, libssh2, nghttp2 and libpsl — and libpsl links ICU, whose character
database alone is 33 MB. Verified with `ldd`: libpsl is the only consumer of
ICU in the closure, and it wants it for the public-suffix list that matches
cookie domains. A scan of a table on a local filesystem cannot reach any of it.
Excluding the object-store shim removes 69 MB and costs `s3://`, `gs://` and
`az://`.

Two smaller findings, both worth having:

- **Nothing in the closure appears in `ldd` output.** The binary links only the
  Mojo runtime and libc; every C library is reached through a shim the tin
  `dlopen`s by name, so the dependency closure has to be seeded by hand. That is
  what `docker/collect-libs.sh` does, and it is why the image sets
  `CONDA_PREFIX` — outside a pixi environment the shim lookup falls back to a
  path relative to the working directory and finds nothing.
- **Do not ship a C++ runtime the base already has.** Copying conda's
  `libstdc++`/`libgcc_s` alongside distroless/cc's own cost 33 MB for nothing;
  the base's versions load the shims fine, which the end-to-end run in each
  image confirms.

## Running it

```sh
scripts/load.sh                  # download ~1.2 GB of TLC Parquet, build the table
pixi run build                   # the Mojo binary
scripts/bench.sh                 # both implementations, then the answer diff
TAXIBENCH_DOCKER=1 scripts/bench.sh   # the same, through the two images
```

`TAXIBENCH_MONTHS=2 scripts/load.sh` builds a much smaller table for iterating.

The warehouse is mounted into the containers at the same absolute path it has on
the host. An Iceberg table's metadata names its data files by absolute location,
so a table generated at `/x/build/warehouse` is only readable at
`/x/build/warehouse` unless the reader rewrites paths on the way through;
mirroring the path keeps the container and native runs reading byte-identical
metadata.

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
- **Memory is not compared here.** `to_batches()` materialises every batch before
  returning, where PyIceberg's `to_arrow_batch_reader()` streams. Peak RSS came
  out close on this suite because the predicates cut the output down first, but
  a query returning most of a large table would separate them, and the
  process-wide peak RSS this harness records is too blunt to say more.
- **`plan_ms` is reported beside the total, not subtracted from it.** Planning
  runs again inside the read on both sides; the number is there to show it is
  small (0.4–3.2 ms) rather than to be added or removed.
- The TLC files are not schema-consistent. The 2023 months type the id columns
  as int64/double and spell the airport fee `airport_fee`; 2024 narrows them and
  spells it `Airport_fee`. `loader/load_table.py` casts both onto one schema.
  Rows whose pickup falls outside the month their file is named for — about
  1,150 of them, dated as far off as 2001 and 2098 — are dropped.
