# Streaming sources — design sketch

> Status: design note, not implemented. See the tracking issue linked in the
> repo description for current state. Scope locked against a concrete source
> is still open; this sketch is the architectural basis.

## TL;DR

Streaming needs **~no sink changes**. `DiskSpool` already windows on its own
wall clock (`src/layer/disk.rs`, `DiskSpool::append` — `self.align(now_unix)`)
and ignores the collector-passed `WindowMeta::start` for segment assignment;
`Buffered` builds its own `meta` in `drain` (`src/layer.rs`). The entire sink
stack works unchanged for a push source. The only wrong-for-streaming code is
`Pipeline::run`'s `sleep(interval) → collect()` loop. Adding streaming is a
**pipeline-loop + builder-seam change**, not a sink change. Fully backward
compatible — existing `Pipeline` (pull/interval/dedupe) stays untouched.

## Scope (when eventually implemented)

| What | Status | Change needed |
|---|---|---|
| `Sink`, `Buffered`, `DiskSpool`, `HfSink` | already streaming-ready | none |
| `Collector` trait (pull) | unchanged | none |
| `Pipeline` (pull/interval/dedupe) | unchanged | none |
| `MeathookBuilder`, supervisor, factory respawn | extends | add `.source(factory)` seam |
| New `Source` trait + `StreamPipeline` | new | ~100 lines |
| Tests + example | new | ~150 lines |

Estimate: ~300 lines, no breakage, no new deps.

## The `Source` trait

Sits beside `Collector`, symmetric, *not* overloading it (the semantic
distinction — pull-cadence vs push-readiness — is worth keeping separate):

```rust
// src/source.rs
pub trait Source: Send {
    type Record: Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;
    fn name(&self) -> &str;
    /// Yield the next batch of records. May await until data is ready
    /// (push semantics); an empty `Vec` is a valid idle signal (the
    /// pipeline applies no backpressure tick — the sink stack's
    /// `ingest().await` is the backpressure).
    fn next(&mut self) -> impl Future<Output = Result<Vec<Self::Record>, Self::Error>> + Send;
}

// Blanket impl: any Stream<Item = Result<Vec<R>, E>> is a Source.
// Lets users pass tokio_stream::wrappers::ReceiverStream, async iterators, etc.
impl<S, R, E> Source for S
where
    S: Stream<Item = Result<Vec<R>, E>> + Send + Unpin,
    E: std::error::Error + Send + Sync + 'static,
{
    type Record = R;
    type Error = E;
    fn name(&self) -> &str { "stream" }   // overridden via NamedStream wrapper
    fn next(&mut self) -> impl Future<Output = Result<Vec<R>, E>> + Send {
        async move { self.next().await.unwrap_or(Ok(vec![])) }
    }
}
```

The blanket impl's `name` is generic — a `NamedStream { name: String, inner:
S }` newtype or a small adapter provides per-pipeline names for tracing parity
with `Collector`.

## `StreamPipeline` loop

Mirrors `Pipeline::run` minus interval and dedupe. Same startup-recovery
flush, same final-drain-on-cancel:

```rust
// src/stream_pipeline.rs
pub struct StreamPipeline<So, S> { source: So, sink: S }

impl<So: Source, S: Sink<So::Record>> StreamPipeline<So, S> {
    pub async fn run(mut self, cancel: CancellationToken) {
        let name = self.source.name().to_owned();
        self.sink.flush().await;              // startup recovery (same as Pipeline)
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                batch = self.source.next() => match batch {
                    Ok(records) if !records.is_empty() => {
                        let now = OffsetDateTime::now_utc();
                        let meta = WindowMeta { pipeline: name.clone(), start: now, end: now };
                        let _ = self.sink.ingest(&meta, records).await; // log err
                    }
                    Ok(_) => {},              // idle: no records
                    Err(e) => { /* log; non-fatal, keep streaming */ }
                }
            }
        }
        self.sink.flush().await;              // final drain (same as Pipeline)
    }
}
```

**Backpressure** is inherent: `sink.ingest().await` blocks the loop; a source
backed by a bounded `mpsc::Receiver` gets backpressure to its producer for
free. meathook adds no extra buffering — the sink stack IS the buffer. User
picks channel capacity upstream.

## Builder seam

```rust
impl MeathookBuilder {
    /// Register a streaming pipeline. Factory semantics identical to
    /// `.pipeline()`: re-invoked on panic-respawn, so the source (and any
    /// producer task feeding it) is rebuilt fresh.
    pub fn source<So, S, Make>(mut self, factory: Make) -> Self
    where
        Make: Fn() -> StreamPipeline<So, S> + Send + Sync + 'static,
        So: Source + Send + 'static,
        S: Sink<So::Record> + Send + 'static,
    { ... }
}
```

The supervisor's respawn logic is source-agnostic — it just spawns
`factory(cancel)` futures. Pull and streaming pipelines are the same shape
from the supervisor's perspective; no change to the panic/backoff/give-up
machinery.

## Decisions locked

- **Dedupe**: dropped for streaming. Cross-batch dedupe is a windowed/TTL
  problem that belongs in a sink layer (`Dedupe` combinator, future work),
  not the pipeline loop. Ship streaming without it.
- **Windowing**: arrival-time. `meta.start = OffsetDateTime::now_utc()` at
  ingest. `DiskSpool` already aligns to wall clock, so this is consistent.
  Event-time windowing (extracting `timestamp` from the record) is deferred
  — would require `DiskSpool`/`Buffered` to honor `meta.start` (currently
  ignored) and a `time_fn` extractor. Note this as a known limitation for
  out-of-order/replay sources.
- **Backpressure**: deferred to the user's channel upstream. Document the
  pattern (`mpsc::channel(capacity)` → `ReceiverStream` → `Source` via
  blanket impl).

## What stays the same (explicitly)

- `Collector`, `Pipeline`, `with_key_fn`, dedupe — untouched.
- `Sink`, `Buffered`, `DiskSpool`, `SpoolError`, `FlushPolicy`, `SinkExt` —
  untouched.
- `HfSink`, `CommitAction`, encode, satay adapter — untouched.
- `MeathookBuilder::pipeline`, supervisor, signal handling, factory respawn
  — untouched (`.source` is additive).
- Feature flags / satay decoupling — untouched.
- Public API — no breakage.

## Example (when implemented)

`examples/stream_demo.rs` — an `mpsc`-backed source demonstrating backpressure
through a `Buffered` + `DiskSpool` stack:

```rust
let (tx, rx) = mpsc::channel::<Result<Vec<TempReading>, Never>>(64);
tokio::spawn(async move {
    // producer: sensor readings every 200ms, backpressure via tx.send().await
    loop { tx.send(Ok(vec![read_sensor().await])).await.unwrap(); }
});

Meathook::builder()
    .source(move || {
        let sink = HfSink::new(client.clone(), "you/sensor", token.clone())
            .buffered(FlushPolicy::every(Duration::from_secs(60)))
            .spooled(spool_dir.join("sensor"), FlushPolicy::hourly());
        StreamPipeline::new(
            NamedStream::new("sensor", ReceiverStream::new(rx)),
            sink,
        )
    })
    .run().await
```

## Tests to add (when implemented)

- `StreamPipeline` drains on cancel (records since last flush reach the sink).
- Source error is non-fatal (stream keeps going after a failed `next()`).
- Bounded channel applies backpressure: a slow `DiskSpool` flush stalls the
  consumer, the producer's `send().await` blocks (paused-clock timing test).
- `NamedStream` wrapper carries the name into tracing.
- Factory respawn rebuilds the source on panic (mirror
  `respawns_panicked_pipeline_from_factory`).

## Open questions left for implementation time

- Whether `NamedStream` is worth a first-class type or just a free adapter fn
  `name_stream("sensor", stream)`.
- Whether the blanket `Stream → Source` impl's empty-batch-on-`None` (stream
  end) should instead terminate the pipeline. Current sketch treats `None`
  as idle — but an *ended* stream should probably stop the pipeline. Worth
  deciding against a concrete source.