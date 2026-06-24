<h1 align="center">meathook</h1>

<p align="center">
  <img src="logo.png" alt="meathook" width="500">
</p>

<p align="center">
  <a href="https://crates.io/crates/meathook-rs"><img src="https://img.shields.io/crates/v/meathook-rs" alt="Crates.io"></a>
  <a href="https://docs.rs/meathook-rs"><img src="https://img.shields.io/docsrs/meathook-rs" alt="Docs.rs"></a>
  <a href="#license"><img src="https://img.shields.io/badge/license-Apache--2.0%2FMIT-blue" alt="License"></a>
  <a href="https://doc.rust-lang.org/edition-guide/rust-2024/"><img src="https://img.shields.io/badge/Rust-2024-blue" alt="Rust Edition"></a>
</p>

<p align="center">
  A polling runtime with composable, durable sinks: poll a source on an
  interval, buffer samples into time windows, and ship them to long-term
  storage — durably.
</p>

<p align="center">
  Core traits, sink combinators, and the supervised runtime are IO-stack
  free. Optional adapters sit on top: <code>SatayCollector</code> (feature
  <code>satay</code>) adapts any
  <a href="https://github.com/zeon256/satay-rs">satay-rs</a>-generated client,
  and a sans-IO HuggingFace parquet sink ships out of the box
  (feature <code>huggingface</code>).
</p>

## How it works

There is no YAML plugin system: you wire collectors and sinks in code by
implementing two traits, and sinks compose like tower layers.

```text
tick(interval) ──► Collector::collect() ──► Vec<Record> ──► Sink::ingest(...)
                                                                 │
                              the "sink" is a composed stack of layers, e.g.:
                                                                 │
                          Buffered(mem, flush: 5m | 10k records) │  tier 1
                                       DiskSpool(flush: 1h)      │  tier 2
                                  HfSink(parquet encode+commit)  ▼  terminal
```

- **[`Collector`]** produces a batch of row-shaped records per tick.
  [`SatayCollector`] adapts any satay-generated client (e.g.
  [nea-rs](https://github.com/InfiniteUnion/nea-rs)) into a collector.
- **[`Sink`]** receives records. Each layer owns its records until *its*
  [`FlushPolicy`] fires (interval elapsed / max records / explicit `flush()`),
  then pushes downstream. Fan-out is just another combinator (`Tee`).
- **[`Meathook`]** runs one tokio task per pipeline, respawns panicked
  pipelines from their factory with exponential backoff, and drains every
  sink stack on SIGTERM/ctrl-c before exiting.

Errors are concrete `thiserror` enums end to end — traits carry an associated
`Error` type, nothing is boxed.

## Quick start

```rust,no_run
use std::time::Duration;

use meathook::{FlushPolicy, HfSink, Meathook, Pipeline, SatayCollector, SinkExt as _};
use satay_reqwest::ReqwestActionExt as _;

#[tokio::main]
async fn main() -> Result<(), meathook::runtime::RuntimeError> {
    let client = reqwest::Client::new();
    let token = std::env::var("HF_TOKEN").expect("HF_TOKEN must be set");

    Meathook::builder()
        .pipeline(move || {
            // The factory is re-invoked after a panic, rebuilding the whole
            // stack (and re-running spool crash recovery).
            let api = nea_rs::Api::new();
            let collector = SatayCollector::new(
                "air_temperature",
                client.clone(),
                move |client| {
                    let api = api.clone();
                    async move { api.air_temperature().send_with(&client).await }
                },
                |response| flatten(response), // typed API response -> Vec<MyRecord>
            );

            // Durable stack: every tick is fsynced to disk before ingest
            // returns; hourly windows land on HF as parquet.
            let sink = HfSink::new(client.clone(), "you/your-dataset", token.clone())
                .spooled("/var/lib/meathook/spool/air_temperature", FlushPolicy::hourly());

            Pipeline::new(collector, sink, Duration::from_secs(60))
                // "latest reading" APIs repeat across polls; dedupe by key
                .with_key_fn(|r: &MyRecord| (r.station_id.clone(), r.timestamp.clone()))
        })
        .run()
        .await
}
```

Records are plain structs — `#[derive(Serialize, Deserialize)]` is all the
parquet encoder needs (the arrow schema is derived from the type via
[serde_arrow](https://crates.io/crates/serde_arrow)).

See [`examples/nea_weather.rs`](examples/nea_weather.rs) for the full
reference consumer: three NEA pipelines, TOML config, graceful shutdown.

## Durability

`DiskSpool` is a write-ahead spool: `ingest` appends records as JSON lines to
an fsynced segment file *before returning*, and segments are deleted only
after the downstream sink accepted them. Storage paths are deterministic per
window (`data/{pipeline}/{YYYY-MM-DD}/{HH}.parquet`), so replays are
idempotent.

| Failure | What happens | Data lost |
|---|---|---|
| SIGKILL / OOM-kill | leftover segments replayed on next start | ≤ 1 torn record |
| Task panic | supervisor respawns the pipeline from its factory | none |
| Sink outage (HF 5xx) | segments accumulate on disk, retried each firing | none |
| Graceful SIGTERM | runtime drains every sink stack before exit | none |
| Disk lost | spool gone | everything unflushed — use a PVC on k8s |

## HuggingFace sink

`HfSink` commits one parquet file per window via a hand-written, sans-IO
`CommitAction` implementing `satay_runtime::Action` (NDJSON commit payload,
base64-inlined file), sent through `satay_reqwest` — the same transport path
as the collectors. The Hive-style partitioning keeps the HF dataset viewer
happy.

Retry/backoff is deliberately *not* in the sink: the upstream `DiskSpool` or
`Buffered` tier retains records when the sink errors and retries at its next
firing.

## Feature flags

| Feature | Default | Implies | What it enables |
|---|---|---|---|
| `parquet` | ✓ | — | `encode::to_parquet` (arrow + parquet + serde_arrow) |
| `satay` | — | — | `SatayCollector`: any satay-generated API client as a `Collector` |
| `huggingface` | ✓ | `parquet`, `satay` | `HfSink` + sans-IO `CommitAction` |

With `--no-default-features` you get the core traits, sink combinators,
write-ahead spool, and supervised runtime — no HTTP/IO stack pulled in
(transitively satay-free) — bring your own collector and terminal sink.

## Configuration

The library API takes typed values (`Duration`, `PathBuf`, `FlushPolicy`)
via builders; config parsing lives in your binary. The example parses a
small TOML file ([`examples/meathook.toml`](examples/meathook.toml)):

```toml
spool_dir = "/var/lib/meathook/spool"   # PVC mount on k8s

[flush]                  # FlushPolicy for each pipeline's DiskSpool tier
every = "1h"
max_records = 50_000

[sink.huggingface]
repo = "zeon256/sg-weather"
branch = "main"

[collectors.air_temperature]
interval = "1m"
```

Secrets come from the environment only: `HF_TOKEN` (required by the HF
sink), `X_API_KEY` (optional for NEA).

## Running the example

```bash
HF_TOKEN=hf_... cargo run --example nea_weather -- examples/meathook.toml
```

## Testing

```bash
cargo test                       # unit tests: paused-clock timing, spool recovery,
                                 # parquet round-trip, HF payload shape (sans-IO)

HF_TOKEN=hf_... MEATHOOK_TEST_REPO=you/meathook-test \
    cargo test --test hf_integration -- --ignored   # real commit to a scratch repo
```

## License

Licensed under either of

- Apache License, Version 2.0
- MIT license

at your option.

[`Collector`]: https://docs.rs/meathook-rs/latest/meathook/collector/trait.Collector.html
[`Sink`]: https://docs.rs/meathook-rs/latest/meathook/sink/trait.Sink.html
[`SatayCollector`]: https://docs.rs/meathook-rs/latest/meathook/satay/struct.SatayCollector.html
[`FlushPolicy`]: https://docs.rs/meathook-rs/latest/meathook/layer/struct.FlushPolicy.html
[`Meathook`]: https://docs.rs/meathook-rs/latest/meathook/runtime/struct.Meathook.html


## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
