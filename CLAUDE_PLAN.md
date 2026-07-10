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

### 3. Content-keyed object path (fixes sub-hourly + same-window clobbering)

`src/sink/huggingface.rs`:
- `object_path` → `data/{pipeline}/{YYYY-MM-DD}/{HH}-{MM}-{SS}-{fnv1a64(content)}.parquet`
  (window start from `meta.start` as reconstructed by `Tier::drain`, plus an FNV-1a
  64-bit fingerprint of the parquet bytes). Review of the `{HH}-{MM}` draft caught two
  residual overwrite modes: sub-*minute* windows within one minute, and — at any window
  size — repeated drains of one window key (`max_records` valve firing mid-window, or a
  failed drain retried after the window grew) committing different chunks to the same
  path. Seconds key the window; the fingerprint keys the chunk. Replays stay idempotent:
  identical records re-encode to identical bytes, so a replay overwrites its own file.
  FNV-1a is hand-rolled (6 lines) — no new dependency, and the algorithm must stay
  frozen since the fingerprint is load-bearing for replay idempotency.
- Update the module/`HfSink` doc examples and the layout string in `README.md`; note the
  multi-drain contract on `Tier`'s docs (terminal sinks keyed by window start alone
  overwrite).

### 4. Example wiring

`examples/nea_weather.rs`:
- `Ctx` gains `gate: CommitGate`, created once in `ctx_from_config`.
- `Ctx::tiered_hf` adds `.gate(self.gate.clone())` to the sink construction.
- The shared `reqwest::Client` gets `.timeout(60s)`: a stalled upload holds the gate's
  permit, so without a timeout one dead connection blocks every pipeline's commits.

### 5. Bookkeeping

- `CHANGELOG.md`: entry under unreleased — commit gate (+ timeout guidance), transient
  retry, **breaking** dataset layout change (`{HH}.parquet` →
  `{HH}-{MM}-{SS}-{fnv1a64}.parquet`) with migration note for old-named files.

## Tests

In `src/sink/huggingface.rs` tests (sans-IO style, no HTTP server needed):
- `transient()` classification: 429 → true, 500/503 → true, 401/400 → false.
- `object_path_is_hive_partitioned` pins the exact path for known bytes (the
  fingerprint is a stability contract — an algorithm change must fail this test).
- `object_path_keeps_windows_distinct_down_to_seconds`: two 30s windows in one minute
  with identical payloads get distinct paths.
- `object_path_separates_chunks_of_one_window`: same window, different content →
  different paths; same content → same path (replay idempotency).
- `path_in_repo` fixture in `commit_request_shape` updated to the new naming.

## Verification (end-to-end)

1. `cargo test` and `cargo clippy --all-targets --all-features` (repo uses strict lints).
2. `HF_TOKEN=... cargo run --example nea_weather -- examples/meathook.toml`:
   - Startup replay should commit the two leftover spool segments
     (`spool-test/{air_temperature,rainfall}/1782998400.jsonl`) under the new naming
     (`13-20-00-{hash}.parquet`) and delete the jsonl files.
   - Let it run past a 10m window boundary, then Ctrl+C: expect three sequential
     `committed window` INFO lines (gate serializes them), **no** `final flush failed`
     ERRORs, and empty `spool-test/*/` dirs.
   - Spot-check the HF repo: per-window files, no overwritten windows.
