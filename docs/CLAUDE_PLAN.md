# meathook-rs — periodic API data collector with pluggable sinks

## Context

Greenfield project (empty `main.rs`, no deps). Goal per `IDEA.md`: a long-running collector that polls APIs on intervals, buffers samples into time windows, and ships them to a long-term store (HuggingFace datasets first). It is a **library-first base** (tower-style): users wire collectors/sinks in code by implementing traits — no dynamic YAML-driven plugin system.

Key insight from planning: the collector abstraction should target **satay-rs generated clients** generically, not NEA specifically. nea-rs is just the first satay-generated client. The universal contract is `satay_runtime::Action`:

```rust
// satay-runtime 0.1.2 (crates.io)
pub trait Action {
    type Response;
    fn request(self) -> Result<http::Request<Vec<u8>>, Error>;
    fn decode<B: AsRef<[u8]>>(response: ResponseParts<B>) -> Result<Self::Response, Error>;
}
```

`satay-reqwest` 0.1.2 provides a blanket `ReqwestActionExt::send_with(&reqwest::Client)`. nea-rs 0.1.1 usage: `Api::new().x_api_key(k).air_temperature().send_with(&client).await` (actions borrow `&Api`, so they're built per tick inside the task — no `'static` issue).

Decisions made with user:
- **Durability & flushing**: buffering is a *sink combinator*, not a separate pipeline stage — flush tiers stack like tower layers (`Buffered(mem) → DiskSpool → HfSink`), each tier with its own flush policy. `DiskSpool` (write-ahead spool with replay-on-restart) is **in v1 scope** — it is the crash-recovery mechanism (see "Crash recovery" below).
- **Format**: Parquet on HF.
- **Scope**: generic Sink + generic satay-Action collector adapter; NEA is the example wiring.
- **Config**: TOML for intervals/repo/window; secrets (`X_API_KEY`, `HF_TOKEN`) from env.
- **Layout**: pure library crate (no binary). NEA wiring lives in `examples/nea_weather.rs` — same structure nea-rs uses for its own examples. HF sink and parquet encoding behind feature flags; `nea-rs` is a dev-dependency only.
- **HF upload**: hand-written sans-IO `CommitAction` implementing `satay_runtime::Action` (NDJSON commit payload, base64-inlined file), sent through `satay_reqwest::send_with` like every collector. A satay-*generated* HF client isn't possible yet: satay-codegen rejects non-JSON request bodies, and NDJSON only gets first-class OpenAPI treatment in 3.2 (`itemSchema`/sequential media types). Extending satay-codegen to OpenAPI 3.2 is a separate satay-rs roadmap item; once a generated huggingface crate exists, swap it in — the Action contract keeps the sink interface unchanged.
- **Errors**: `thiserror` enums everywhere; no `BoxError`/type erasure. Traits carry an associated `type Error: std::error::Error + Send + Sync + 'static`.

## Architecture

```
tick(interval) ──► Collector::collect() ──► Vec<Record> ──► Sink::ingest(...)
                                                                 │
                              the "sink" is a composed stack of layers, e.g.:
                                                                 │
                          Buffered(mem, flush: 5m | 10k records) │  tier 1
                                       DiskSpool(flush: 1h)      │  tier 2 (later)
                                  HfSink(parquet encode+commit)  ▼  terminal
```

Each layer owns its records until *its* flush policy fires (interval elapsed / max records / explicit `flush()`), then pushes downstream. Multiple flush cadences fall out naturally: memory drains often, the remote sink sees hourly windows. Fan-out is just another combinator (`Tee<A, B>` ingests into both).

One tokio task per pipeline, owned by a supervisor that respawns on panic with exponential backoff. Graceful shutdown (SIGTERM/ctrl-c via `tokio_util::sync::CancellationToken`) drains all buffers through the sink before exit.

### Core traits (`src/` modules)

`src/collector.rs`
```rust
pub trait Collector: Send {
    type Record: Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;
    fn name(&self) -> &str;
    fn collect(&mut self) -> impl Future<Output = Result<Vec<Self::Record>, Self::Error>> + Send;
}
```
Plus the satay adapter — the reusable piece that makes any satay client a collector:
```rust
// satay.rs: MakeAction builds a fresh action each tick (actions are consumed by request())
pub struct SatayCollector<M, T> { name: String, client: reqwest::Client, make: M, transform: T }
// M: FnMut(&reqwest::Client) -> Future<Output = Result<Resp, _>>  (or: make-action + send_with internally)
// T: FnMut(Resp) -> Vec<Record>   — flattens API response into row-shaped records
```
Implementation detail: simplest viable shape is `M: FnMut() -> A, A: Action + Send`, and `SatayCollector` calls `send_with` itself via `satay_reqwest::ReqwestActionExt`.

`src/sink.rs`
```rust
pub struct WindowMeta { pub pipeline: String, pub start: OffsetDateTime, pub end: OffsetDateTime }
pub trait Sink<R>: Send {
    type Error: std::error::Error + Send + Sync + 'static;
    /// Hand records to this layer. A buffering layer may hold them; a terminal sink ships them.
    fn ingest(&mut self, meta: &WindowMeta, records: Vec<R>) -> impl Future<Output = Result<(), Self::Error>> + Send;
    /// Force-drain this layer and everything downstream (shutdown, final flush).
    fn flush(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
```

`src/layer.rs` — sink combinators (the tower-layer part):
```rust
pub struct FlushPolicy { pub every: Duration, pub max_records: usize }

/// In-memory tier: holds records, pushes downstream when its policy fires.
/// Checks elapsed-time/size on each ingest (collector ticks are frequent vs flush
/// windows, so no extra timer task); flush() force-drains. On downstream failure
/// it KEEPS the records and retries on the next firing (transient HF 5xx ≠ data loss).
pub struct Buffered<R, S: Sink<R>> { buf: Vec<R>, window_start: OffsetDateTime, policy: FlushPolicy, inner: S }
impl<R: Send + 'static, S: Sink<R>> Sink<R> for Buffered<R, S> { type Error = S::Error; ... }

/// Fan-out: ingest into both. Errors reported per-branch (thiserror enum TeeError<A, B>).
pub struct Tee<A, B>(A, B);

/// Durable tier (requires R: Serialize + DeserializeOwned). The DISK is the buffer:
/// ingest = append JSONL lines to the active segment file (records are never only
/// in memory past an ingest call). Flush = read segments back, push downstream,
/// delete segment on success / leave in place on failure.
pub struct DiskSpool<R, S: Sink<R>> { dir: PathBuf, policy: FlushPolicy, inner: S, ... }
```
Builder ergonomics mirror tower's ServiceBuilder: `hf_sink.spooled(dir, FlushPolicy::hourly())` (extension trait `SinkExt` with `.buffered(policy)`, `.spooled(dir, policy)`, `.tee(other)`).

### Crash recovery (`DiskSpool` protocol)

On-disk layout, one directory per pipeline (config `spool_dir`, a PVC mount on k8s):
```
{spool_dir}/{pipeline}/{window_start_unix}.jsonl   # active + any unflushed segments
```
- **ingest**: serialize each record as one JSON line, append to the segment named by the current window start, fsync the file (and the directory on segment creation). A new window ⇒ new segment file. Write happens before ingest returns — write-ahead semantics; after a tick's ingest returns, those records survive SIGKILL.
- **flush** (policy fires, or `flush()` on shutdown): for each closed segment (oldest first): read + deserialize lines, reconstruct `WindowMeta` from the filename, `inner.ingest(meta, records)`; on success delete the segment, on failure leave it and retry at the next firing.
- **startup recovery** (`DiskSpool::open`): scan the pipeline's spool dir; any leftover `*.jsonl` segments are unflushed windows from a previous run — flush them downstream immediately, before the first tick. A torn final line (crash mid-append) is detected by failed JSON parse of the last line only and skipped with a warning; that single record is the maximum loss window.
- **idempotent replay**: the HF path is deterministic per window (`data/{pipeline}/{date}/{hour}.parquet`), so the crash-after-upload-before-delete race just overwrites the same file with the same content — replays are safe, no duplicate rows.

Loss/recovery matrix:

| Failure | What happens | Data lost |
|---|---|---|
| SIGKILL / OOM-kill | leftover segments replayed by `DiskSpool::open` on next start | ≤ 1 torn record |
| Task panic | supervisor respawns the pipeline from its factory; spool dir untouched | none |
| Sink outage (HF 5xx) | segments accumulate on disk, retried each firing | none |
| Graceful SIGTERM | runtime calls `flush()` through the stack before exit | none |
| Disk lost (no PVC) | spool gone | everything unflushed — hence PVC in k8s |

Respawn detail: panics make the task's `JoinSet` entry resolve with `is_panic()`; since the pipeline was consumed by the spawned task, the runtime registers each pipeline as a **factory closure** (`impl Fn() -> Pipeline + Send`), so a respawn rebuilds the stack — and `DiskSpool::open` re-runs recovery on whatever the dead task left behind.

Error design (`src/error.rs` + per-module): every concrete error is a `thiserror` enum — `SatayCollectError` (wraps `satay_reqwest::Error`), `EncodeError`, `HfSinkError` (http status / decode / auth variants), `TeeError`, `ConfigError`. Generic code is generic over `C::Error`/`S::Error` and only needs `Display + Error` — no boxing.

`src/pipeline.rs` + `src/runtime.rs`
- `Pipeline<C, S>`: collector + composed sink stack + `PipelineConfig { poll_interval }`. The pipeline loop is now trivial: tick → collect → dedupe → `sink.ingest(...)`; all flush cadence lives in the sink layers.
- Dedup: consecutive polls of "latest reading" APIs return repeats — dedupe by a user-supplied key fn (e.g. `(station_id, timestamp)`) before ingesting, via an optional `key_fn` on the pipeline builder.
- `Meathook` runtime: builder collects type-erased pipeline **factories** (`Box<dyn Fn() -> BoxFuture + Send>`), runs them in a `JoinSet`, supervises panics (respawn from factory w/ backoff, give up after N consecutive failures within a window), handles signals, and calls `sink.flush()` through the whole stack on shutdown.

```rust
Meathook::builder()
    .pipeline(air_temp_pipeline)
    .pipeline(pm25_pipeline)
    .run().await?;
```

### Parquet encoding (`src/encode.rs`, feature `parquet`)

Use **`serde_arrow`** + `arrow` + `parquet` crates: records are plain `#[derive(Serialize)]` structs; schema via `serde_arrow::schema::SchemaLike::from_type::<R>()`; build a `RecordBatch`, write with `parquet::arrow::ArrowWriter` into a `Vec<u8>`. This keeps the user contract at "derive Serialize on your record struct" — no manual arrow builders.

### HuggingFace sink (`src/sink/huggingface.rs`, feature `huggingface`)

Sans-IO, satay-style: a hand-written `CommitAction` implements `satay_runtime::Action` (reusing `satay_runtime::{insert_header, into_request, ResponseParts}`):
- `request()`: builds `POST https://huggingface.co/api/datasets/{repo}/commit/{branch}`, `Authorization: Bearer <token>`, `Content-Type: application/x-ndjson`, NDJSON body:
  - line 1: `{"key":"header","value":{"summary":"meathook: <pipeline> <window>"}}`
  - line 2: `{"key":"file","value":{"path":"data/<pipeline>/<YYYY-MM-DD>/<HH>.parquet","content":"<base64>","encoding":"base64"}}`
- `decode()`: 200 → typed commit response (oid/url), else `HfSinkError::UnexpectedStatus(status, body)`.

`HfSink { client: reqwest::Client, repo, branch, token }` (token from `HF_TOKEN`) builds a `CommitAction` per flush and sends it with `satay_reqwest::ReqwestActionExt::send_with` — same transport path as the collectors. Future: replace `CommitAction` with a satay-generated huggingface crate once satay-codegen learns OpenAPI 3.2 sequential media types (`itemSchema`); the `Action` boundary makes that a drop-in swap.
- Hive-style path partitioning (`data/{pipeline}/{date}/{hour}.parquet`) keeps the HF dataset viewer happy and analytics-friendly.
- Retry/backoff on 5xx/429 handled by the pipeline's retained-records retry (above); treat 4xx (bad token/repo) as fatal-log-and-retain.

### Config (example-side)

```toml
# meathook.toml
spool_dir = "/var/lib/meathook/spool"   # PVC mount on k8s

[flush]                 # default FlushPolicy for the DiskSpool layer
every = "1h"
max_records = 50_000

[sink.huggingface]
repo = "zeon256/sg-weather"
branch = "main"

[collectors.air_temperature]
interval = "1m"
[collectors.pm25]
interval = "1h"
```
Config parsing lives in the **example**, not the library core — the library API takes typed values (`Duration`, `PathBuf`, `FlushPolicy`) via builders. The example parses `meathook.toml` with `toml` + `serde` + `humantime-serde` and looks up each collector's interval by name with a default. Secrets only from env.

### Example (`examples/nea_weather.rs`)

The reference consumer: wires 2–3 NEA collectors (air_temperature, pm25, rainfall), each into `HfSink::new(repo).spooled(spool_dir.join(name), policy_from_config)`, loads `examples/meathook.toml`, installs `tracing_subscriber`, runs `Meathook`. This is the deployable artifact for the user's own k8s collection job (`cargo build --example nea_weather` or a thin downstream bin crate later).

## Dependencies (Cargo.toml)

- core: `tokio` (rt-multi-thread, signal, time, sync), `tokio-util`, `reqwest` (rustls-tls), `serde`, `serde_json`, `thiserror`, `tracing`, `time` (serde, formatting), `satay-runtime = 0.1.2`, `satay-reqwest = 0.1.2`
- feature `parquet`: `arrow`, `parquet`, `serde_arrow`, `base64` (check serde_arrow's supported arrow major and pin both to match)
- feature `huggingface` (implies `parquet`): no extra deps beyond core
- dev-dependencies (examples + tests): `nea-rs = 0.1.1`, `tracing-subscriber` (env-filter), `anyhow`, `toml`, `humantime-serde`, `tokio` (macros), `tempfile`
- default features = `["parquet", "huggingface"]`

## Implementation order

1. `Cargo.toml` deps + module skeleton (`lib.rs` re-exports).
2. Core traits: `collector.rs`, `sink.rs`, `layer.rs` (`Buffered`, `Tee`, `SinkExt`), thiserror error enums (`error.rs`).
3. `layer/disk.rs` `DiskSpool`: segment append/fsync, flush-and-delete, startup recovery (unit tests with tempdir: replay leftover segments, tolerate torn final line, retain segments across failing downstream).
4. `pipeline.rs` (tick/dedup loop) + `runtime.rs` (factory-based supervisor, signals, final flush) — test with a fake collector + `Vec`-backed test sink using `tokio::time::pause()` for deterministic interval/flush-policy tests (including: Buffered retains records across a failing downstream, Tee fan-out, flush() drains the whole stack, panic → respawn → spool recovery).
5. `satay.rs` adapter (`SatayCollector`), compile-checked against nea-rs actions.
6. `encode.rs` parquet encoding (unit test: encode sample records, read back with `parquet` reader, assert round-trip).
7. `sink/huggingface.rs`: sans-IO `CommitAction` (`Action` impl) + `HfSink` (unit test `request()` output — method/uri/headers/NDJSON body — and `decode()` against canned responses, no network needed thanks to sans-IO; integration behind `HF_TOKEN`-gated `#[ignore]` test against a scratch dataset repo).
8. `examples/nea_weather.rs` (NEA wiring + TOML config loading) + sample `examples/meathook.toml`.

## Verification

- `cargo test` — pipeline timing tests (paused clock), parquet round-trip, HF payload shape.
- `cargo run --example nea_weather` with `X_API_KEY` unset (NEA works keyless for these endpoints per examples) and a short `every = "2m"`, `interval = "30s"` test config + real `HF_TOKEN` against a scratch dataset repo (e.g. `zeon256/meathook-test`): observe two flush cycles land parquet files at `data/air_temperature/<date>/<hour>.parquet`, check the HF dataset viewer renders them.
- Graceful kill: `kill -TERM` mid-window → stack flushes before exit (records since last flush appear on HF).
- Hard kill: `kill -9` mid-window → restart the binary → leftover spool segments are replayed on startup and the window lands on HF with the correct (original) path; spool dir empties.
- Panic test: temporary collector that panics every 3rd tick → supervisor respawns from factory, spool recovery runs, pipeline keeps collecting.
