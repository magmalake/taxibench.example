# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

This repository is a benchmark rather than a library: it has no public API and
nothing depends on it, so it is not versioned. What matters here is which
revision of iceberg-mojo a number was taken against, and that is recorded with
the numbers.

## [Unreleased]

### Added
- **A third engine: Lance 11.0.0.** `loader/convert_lance.py` transcodes the
  Iceberg table's own 24 Parquet data files into a Lance dataset of 24
  fragments, one per month — 79,478,796 rows, the same count by construction —
  and `python/taxibench_lance/` answers the same eight queries over it, in the
  JSON shape `scripts/compare.py` already reads. All eight answers agree with
  PyIceberg, exactly on every count and within 1e-9 relative on every sum.
  Timings are not taken yet: `scripts/bench-lance.sh` runs the Lance leg and
  the PyIceberg leg in one session, both told the same thread count and each
  query in its own process, and it wants a quiet machine.
- **The README says what the third engine is not.** Lance does not read the
  Iceberg table; it reads a converted copy, 4.79 GB against 1.37 GB of zstd
  Parquet, built in 5.9 s. The benchmark runs warm, so that comparison hands
  Lance the copy of the data that needs no decompression and then charges it
  nothing for the size. Lance also has no partitioning, so the fragment-to-month
  map the pruning needs is a sidecar the converter writes, where Iceberg
  maintains the equivalent itself. Nothing is sorted or clustered on the way
  through, and `TAXIBENCH_COMPRESS=zstd` builds the 2.55 GB variant for anyone
  who wants to price the first of those.

### Changed
- **`scripts/compare.py` takes its column headings from the records.** Either
  side can now be any of three engines, and a heading reading `mojo p50` over
  Lance numbers would be wrong. Nothing about the gate itself moves.
- **Re-measured against iceberg-mojo 0.7.1.** The two stacks are now level —
  1.06x on one thread and 1.01x on ten, from 0.70x and 0.86x at 0.6.7 — so the
  README no longer organises itself around a single explanation for the split.
  What remains is per-query: partition-filtered queries win 2.0x–2.3x, the
  single-file wide scan 1.5x–1.7x, and q1 and q8 lose at 0.63x–0.75x, where the
  cost is Parquet decode rather than anything in the Iceberg layer.

### Added
- **The query suite.** Eight queries over 79,478,796 NYC yellow-taxi trips
  (TLC 2023–2024) as an Iceberg v2 table partitioned by
  `month(tpep_pickup_datetime)` — 24 data files, 24 snapshots, 1.3 GB of
  Parquet. Run through iceberg-mojo (`src/main.mojo`) and PyIceberg 0.11.1
  (`python/taxibench/`). PyIceberg writes the table, so neither implementation
  under test chose the layout.
- **The answer diff, `scripts/compare.py`.** Counts must match exactly and sums
  within 1e-9 relative; it exits non-zero on disagreement, so a timing is only
  quoted once both sides agree on what the answer is.
- **Two thread legs.** `scripts/bench.sh` runs the suite twice — one thread each
  and one worker per core each — with the thread count set explicitly on both
  sides, and each query in its own process.
- **Two container images.** `docker/Dockerfile.mojo` (with a
  `TAXIBENCH_OBJECTSTORE=0` build arg for a local-filesystem-only variant) and
  `docker/Dockerfile.python`. `docker/collect-libs.sh` stages the Mojo binary's
  runtime closure, which has to be seeded by hand because every C library is
  reached through a shim that is `dlopen`ed by name and so appears in no `ldd`
  output.
- **`src/probe.mojo` and `python/taxibench/probe.py`.** One scan, any
  projection, timed — the diagnostic that isolates a cost by holding the file
  set still while the column count varies. Not part of the suite.
- **`loader/load_table.py`.** Downloads the TLC months and builds the table.
  Normalises the two schemas TLC publishes: 2023 types the id columns as
  int64/double and spells the airport fee `airport_fee`, 2024 narrows them and
  spells it `Airport_fee`.

### Found
Four gaps in the Mojo stack, filed against iceberg.mojo and all measured here.
Three are fixed: [#12](https://github.com/magmalake/iceberg.mojo/issues/12)
streaming, [#13](https://github.com/magmalake/iceberg.mojo/issues/13) residual
reduction for partition-aligned predicates, and
[#15](https://github.com/magmalake/iceberg.mojo/issues/15) a metadata-only
`count()`, along with intra-file parallelism.
[#14](https://github.com/magmalake/iceberg.mojo/issues/14), a fixed per-file
scan cost, is still open.
