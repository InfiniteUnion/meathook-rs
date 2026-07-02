# Fix shutdown-flush HF commit contention + sub-hourly path clobbering

## Context

On Ctrl+C, all three example pipelines run their final flush concurrently, each draining
its spooled window into a HuggingFace commit — three simultaneous commits to the same
repo. HF serializes commits per repo in a server-side concurrency queue, so shutdown
hangs ~100s and then fails with `429 "maximum time in concurrency queue reached"` /
`500`. The data is safe (Tier removes a spool segment only after downstream success —
`spool-test/{air_temperature,rainfall}/1782998400.jsonl` are still on disk awaiting
replay), but the shutdown flush is one-shot: `HfSink` deliberately has no retry
(`src/sink/huggingface.rs:156`) and after shutdown there is no "next firing", so
near-every multi-pipeline Ctrl+C ends in spurious-looking ERRORs and a slow exit.

Adjacent bug found while tracing: `object_path` (`src/sink/huggingface.rs:190`) keys
files by hour only (`data/{pipeline}/{date}/{HH}.parquet`), but the example config uses
`every = "10m"`. On a long run the 13:00 window commits `13.parquet` (spool then
deleted), and the 13:10 window **overwrites** it — the 13:00 records are gone from both
HF and the spool. Silent data loss whenever `every` < 1h.

Two fixes, both in the sink layer (user picked defaults were not confirmed — recommended
options chosen: gate + retry, window-keyed path):

## Changes

### 1. `CommitGate`: client-side per-repo commit serialization

`src/sink/huggingface.rs`:
- New public type `CommitGate(Arc<tokio::sync::Semaphore>)` with `Clone`, `Default`,
  and `CommitGate::new()` (1 permit). Doc: share one gate across every `HfSink`
  targeting the same repo so commits queue client-side instead of in HF's per-repo
  concurrency queue.
- `HfSink` gains `gate: Option<CommitGate>` + builder method `.gate(CommitGate)`
  (matches existing `.branch()` builder style).
- In `ingest`, acquire the permit around **each send attempt** (released during backoff
  sleeps, so another pipeline can commit while this one backs off). Tokio semaphores are
  FIFO, so ordering is fair.
- Re-export at `src/lib.rs:54`: `pub use sink::huggingface::{CommitGate, HfSink, HfSinkError};`

### 2. Bounded in-sink retry for transient failures

`src/sink/huggingface.rs`, inside `ingest`:
- Retry on `Transport` errors and `Rejected` with 429 or any 5xx; never on other 4xx
  (auth/bad request). 4 attempts total, backoff 2s/4s/8s between attempts. `warn!` per
  retry with status + delay.
- Factor the decision into a pure helper `fn transient(&HfSinkError) -> bool` so it is
  unit-testable without HTTP.
- Update the `HfSink` doc comment (currently "Retry/backoff is *not* handled here"):
  transient statuses now retry briefly in-sink; `Tier` remains the durable retry for
  everything else. Worst case this adds ~14s per still-failing commit to shutdown —
  the ERROR + replay-on-next-start path stays as the final fallback, unchanged.

### 3. Window-keyed object path (fixes sub-hourly clobbering)

`src/sink/huggingface.rs`:
- `object_path` → `data/{pipeline}/{YYYY-MM-DD}/{HH}-{MM}.parquet` (minute from
  `meta.start`, i.e. the window start reconstructed by `Tier::drain`). No clobbering at
  any window size; replays stay idempotent (path still depends only on window start).
- Update the module/`HfSink` doc examples (`08.parquet` → `08-00.parquet`) and the
  layout string in `README.md:120`.

### 4. Example wiring

`examples/nea_weather.rs`:
- `Ctx` gains `gate: CommitGate`, created once in `ctx_from_config`.
- `Ctx::tiered_hf` adds `.gate(self.gate.clone())` to the sink construction.

### 5. Bookkeeping

- `CHANGELOG.md`: entry under unreleased — commit gate, transient retry, **breaking**
  dataset layout change (`{HH}.parquet` → `{HH}-{MM}.parquet`).

## Tests

In `src/sink/huggingface.rs` tests (sans-IO style, no HTTP server needed):
- `transient()` classification: 429 → true, 500/503 → true, 401/400 → false.
- Update `object_path_is_hive_partitioned` for `08-00.parquet`; add a sub-hourly case
  (13:20 window → `13-20.parquet`) proving distinct windows get distinct paths.
- Update the `path_in_repo` fixture in `commit_request_shape` if desired (cosmetic).

## Verification (end-to-end)

1. `cargo test` and `cargo clippy --all-targets --all-features` (repo uses strict lints).
2. `HF_TOKEN=... cargo run --example nea_weather -- examples/meathook.toml`:
   - Startup replay should commit the two leftover spool segments
     (`spool-test/{air_temperature,rainfall}/1782998400.jsonl`) under the new naming
     (`13-20.parquet`) and delete the jsonl files.
   - Let it run past a 10m window boundary, then Ctrl+C: expect three sequential
     `committed window` INFO lines (gate serializes them), **no** `final flush failed`
     ERRORs, and empty `spool-test/*/` dirs.
   - Spot-check the HF repo: per-window files, no overwritten windows.
