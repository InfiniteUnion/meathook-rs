# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- *(hf-bucket)* `HfBucketSink`: terminal sink writing parquet/json/csv
  windows to a Hugging Face storage bucket. Uploads go through Xet
  (`hf-xet` crate) with chunk deduplication, then register via the
  sans-IO `BatchAction` (`POST /api/buckets/{bucket}/batch`). Also adds
  `CreateBucketAction`. Opt-in feature `hf-bucket`; requires Rust 1.89+
  (transitive `redb` MSRV).
- *(hf-bucket)* `nea_weather_bucket` example: the reference NEA pipelines
  writing to a storage bucket; collectors, records, and config plumbing
  are now shared with the dataset example via `examples/common/mod.rs`.

## [0.4.0](https://github.com/InfiniteUnion/meathook-rs/compare/v0.3.0...v0.4.0) - 2026-07-13

### Added

- *(parquet)* zstd compression

### Added

- Opt-in, type-level zstd parquet compression through
  `ParquetEncoder<Zstd<LEVEL>>`, with `LEVEL` restricted at compile time to
  `1..=22`. `Zstd` defaults to level 1 and `HfSink` remains uncompressed by
  default.

### Changed

- **Breaking:** `ParquetEncoder` is now generic over a sealed
  `ParquetCompression` policy and is no longer a unit value. Migrate
  `ParquetEncoder.encode(&records)` to
  `ParquetEncoder::default().encode(&records)` for the previous uncompressed
  behavior.
- Changing compression changes the encoded-byte fingerprint in Hugging Face
  object paths. Drain pending JSONL spool segments before changing compression
  if replaying the same logical rows to a second path would be unacceptable.

## [0.3.0](https://github.com/InfiniteUnion/meathook-rs/compare/v0.2.0...v0.3.0) - 2026-07-12

### Added

- better ergonomics for stackign tiered memories

### Added

- `SinkStack`: compose buffering tiers in incoming record order, then finish
  the concrete stack with `.terminal(sink)`.

### Changed

- Sink composition APIs and examples now read in incoming record order.
- The NEA reference consumer now demonstrates
  `MemStore → JsonlStore → HfSink`.

### Removed

- **Breaking:** `SinkExt::tier`. Migrate
  `terminal.tier(jsonl, jsonl_policy).tier(mem, mem_policy)` to
  `SinkStack::new().tier(mem, mem_policy).tier(jsonl, jsonl_policy).terminal(terminal)`;
  both forms produce the same concrete nested `Tier` topology.

## [0.2.0](https://github.com/InfiniteUnion/meathook-rs/compare/v0.1.2...v0.2.0) - 2026-07-10

### Added

- generic Encoder trait — split format out of HfSink
- make intermediate spool a generic memory tier

### Fixed

- review findings — content-keyed object paths, doc link, client timeout
- shutdown-flush HF commit contention + sub-hourly path clobbering

### Other

- run test job across the feature matrix

### Added

- `Store` / `Segment` traits (`meathook::store`): pluggable window-keyed
  storage backends for buffering tiers, with GAT segment handles that are
  committed (removed from the store) only after the downstream sink accepted
  the records.
- `MemStore`: in-memory backend (the old `Buffered` internals).
- `JsonlStore`: durable write-ahead JSONL segment backend (the old
  `DiskSpool` internals — same on-disk layout, fsync write-ahead, crash
  recovery, and torn-line tolerance).
- `Tier<R, St, S>` + `TierError`: one generic buffering layer owning window
  alignment, `FlushPolicy` firing, replay-on-startup, and
  retain-on-downstream-failure; compose with `SinkExt::tier(store, policy)`.
  Tiers nest arbitrarily, and zero tiers (a terminal sink alone) is fine.
- `CommitGate`: client-side commit serialization for `HfSink`
  (`HfSink::gate`). HuggingFace serializes commits per repo in a
  server-side concurrency queue that 429s requests queued too long, so
  pipelines committing to the same repo (e.g. the shutdown flush racing
  every pipeline's final window) share one gate and queue client-side.
  Give gated clients a request timeout (`reqwest::ClientBuilder::timeout`):
  the permit is held for one send attempt, so a stalled upload otherwise
  blocks every sink sharing the gate.
- `HfSink` retries transient commit failures (transport errors, 429, 5xx)
  up to 3 times with 2s/4s/8s backoff before the error propagates —
  previously the one-shot shutdown flush had no retry at all. `Tier`
  retention/replay remains the durable fallback.

### Removed

- **Breaking:** `Buffered`, `DiskSpool`, `SpoolError`, `SinkExt::buffered`,
  and `SinkExt::spooled`. Migrate `sink.buffered(policy)` to
  `sink.tier(MemStore::new(), policy)` and `sink.spooled(dir, policy)` to
  `sink.tier(JsonlStore::new(dir), policy)`.

### Changed

- In-memory tier windows are now wall-clock aligned like the disk spool's
  (a first record landing at :59 of an hourly window flushes at the
  boundary, ~1 minute later — not a full hour later).
- Drained `WindowMeta` is reconstructed from the window key for all tiers
  (the old `Buffered` used first-tick start / wall-clock end).
- A drain pass attempts every closed window oldest-first and returns the
  first error; a failed window is retained for the next firing without
  blocking newer windows (the old `DiskSpool` behavior, now also for
  in-memory tiers).
- Live batches through a `JsonlStore` tier carry the first-seen
  `meta.pipeline` instead of always the directory-derived name (replay
  before any live batch still uses the store's hint).
- **Breaking:** `HfSink` object paths are keyed by the full window start
  and a fingerprint of the parquet bytes
  (`data/{pipeline}/{YYYY-MM-DD}/{HH}-{MM}-{SS}-{fnv1a64}.parquet`, was
  `{HH}.parquet`). Two silent-overwrite modes existed — after the spool
  segment was already deleted, so the records were unrecoverable:
  sub-hourly `FlushPolicy::every` mapped every window in an hour to the
  same file, and a window drained more than once (the `max_records` valve
  firing mid-window, or a failed drain retried after the window grew)
  overwrote its earlier chunk. Replays still re-encode to identical bytes
  and overwrite their own file (idempotent); the fingerprint is stable for
  identical bytes, though a parquet dependency upgrade between crash and
  replay may re-ship a window to a new path, duplicating rather than
  losing it. Migration: files under the old naming are left in place —
  they remain valid data but read as duplicates to consumers globbing
  `data/**/*.parquet` once their windows are ever replayed; delete them
  if that matters.

### Fixed

- Draining a tier under a time-unbounded policy (e.g.
  `FlushPolicy::new(Duration::MAX, n)` for records-only flushing) no longer
  panics on `time` range overflow computing the drained window's `end` — it
  saturates to the max representable datetime. Inherited from `DiskSpool`,
  which had the same edge.

## [0.1.2](https://github.com/InfiniteUnion/meathook-rs/compare/v0.1.1...v0.1.2) - 2026-06-25

### Other

- adhere to a more strict set of clippy lints

## [0.1.1](https://github.com/InfiniteUnion/meathook-rs/compare/v0.1.0...v0.1.1) - 2026-06-24

### Added

- update satay

### Other

- update cargo.toml to include docs
- release v0.1.0

## [0.1.0](https://github.com/InfiniteUnion/meathook-rs/releases/tag/v0.1.0) - 2026-06-24

### Added

- split satay dep into feature
- *(website)* consistency in animations and lines
- *(website)* add favicon and final cta
- add memory and CPU usage graphs
- new content on website
- add website
- initial commit

### Fixed

- *(website)* ensure that there are spacing between mono and normal text
- *(website)* animations on hero and flow
- *(website)* anchor gitignore to /data/ and commit footprint.json

### Other

- update cargo.toml
- update readme
- update readme
- add github actions for rust and website
- wrong link for nea-rs
- update to astro 7
- move docs to docs folder
- update README
- make sure dylint lints dont fail
