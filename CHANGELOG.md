# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/InfiniteUnion/meathook-rs/compare/v0.1.2...v0.2.0) - 2026-07-10

### Added

- make intermediate spool a generic memory tier

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
