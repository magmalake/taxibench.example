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
- **A PostgreSQL implementation of the suite.** The same eight questions in SQL
  against PostgreSQL 18.4, over its own copy of the same 79,478,796 rows —
  `loader/load_postgres.py`, `python/taxibench/postgres.py`,
  `python/taxibench/postgres_queries.py`, and `scripts/pg-server.sh`,
  `scripts/load-postgres.sh`, `scripts/bench-postgres.sh` and
  `scripts/pg-indexes.sh` to raise a throwaway cluster under `build/` on a free
  port, load it, run it and tear it down. `scripts/compare.py` diffs its
  answers against PyIceberg's unmodified and all eight agree: every count
  exactly, every sum to within 1.8e-11 relative against a gate of 1e-9. The
  timings are not in yet; they need a quiet machine and the README says so
  rather than quoting numbers taken on a busy one.
- **The framing that has to go with it.** PostgreSQL is a row store reading its
  own heap and is not reading the Iceberg table at all, so the README sets out
  what is not comparable rather than quoting a total: q7 is not a wide decode
  in a row store and `count(*)` does not even deform the tuple, so PostgreSQL
  does less work there than either Iceberg reader; q2 and q6 prune to the same
  partitions but still evaluate the redundant predicate per row where Iceberg's
  residual evaluator drops it; the fold happens inside the server. The second
  copy costs **12.26 GiB of heap against 1.28 GiB of Parquet — 166 bytes per
  row against 17.3** — and about 80 s to load with the WAL turned off, which is
  stated as flattering the load rather than left implicit.
- **Both index states, because the choice changes q8 and does nothing for q1.**
  One btree per equality predicate in the suite and nothing else, 1.03 GiB for
  the pair thanks to btree deduplication over 265 zones and 5 payment types.
  q8 becomes 24 bitmap heap scans; q3 becomes 16 index scans and 8 bitmap heap
  scans, because the planner decides per partition and does not decide the same
  way twice; q1, q4, q5 and q7 have no equality predicate and cannot be touched.
- **The PostgreSQL table is range-partitioned into the same 24 months** the
  Iceberg table is partitioned into, so `files` counts the same thing on both
  sides — read out of `EXPLAIN`, not asserted — and q2, q6 and q7 measure
  pruning rather than measuring the fact that nobody partitioned the table.
- **A `postgres` pixi environment**, separate from the default one so the Mojo
  toolchain and PostgreSQL never have to solve against each other. It carries
  the server binaries and nothing else, and the scripts find it themselves.

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
