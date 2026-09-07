# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

This repository is a benchmark rather than a library: it has no public API and
nothing depends on it, so it is not versioned. What matters here is which
revision of iceberg-mojo a number was taken against, and that is recorded with
the numbers.

## [Unreleased]

### Changed
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
