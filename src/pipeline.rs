//! [`Pipeline`]: one collector wired to one composed sink stack.
//!
//! The pipeline loop is deliberately trivial: tick → collect → dedupe →
//! `sink.ingest(...)`. All flush cadence lives in the sink layers.

use std::collections::HashSet;
use std::hash::Hash;
use std::time::Duration;

use ::time::OffsetDateTime;
use tokio::time;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::collector::Collector;
use crate::sink::{Sink, WindowMeta};

/// Marker key type for pipelines without deduplication.
pub type NoKey = ();

/// A collector polled on `poll_interval`, feeding a sink stack.
///
/// Consecutive polls of "latest reading" APIs return repeats, so an optional
/// key function dedupes records across consecutive ticks (a record is
/// dropped if its key was seen this tick or the previous one).
pub struct Pipeline<C, S, F = fn(&<C as Collector>::Record) -> NoKey, K = NoKey>
where
    C: Collector,
{
    collector: C,
    sink: S,
    poll_interval: Duration,
    key_fn: Option<F>,
    seen_prev: HashSet<K>,
}

impl<C, S> Pipeline<C, S>
where
    C: Collector,
{
    #[must_use]
    pub fn new(collector: C, sink: S, poll_interval: Duration) -> Self {
        Self {
            collector,
            sink,
            poll_interval,
            key_fn: None,
            seen_prev: HashSet::new(),
        }
    }
}

impl<C, S, F, K> Pipeline<C, S, F, K>
where
    C: Collector,
{
    /// Dedupe records across consecutive polls by the given key.
    ///
    /// Typical key for station readings: `(station_id, timestamp)`.
    #[must_use]
    pub fn with_key_fn<F2, K2>(self, key_fn: F2) -> Pipeline<C, S, F2, K2>
    where
        F2: FnMut(&C::Record) -> K2,
        K2: Eq + Hash,
    {
        Pipeline {
            collector: self.collector,
            sink: self.sink,
            poll_interval: self.poll_interval,
            key_fn: Some(key_fn),
            seen_prev: HashSet::new(),
        }
    }
}

impl<C, S, F, K> Pipeline<C, S, F, K>
where
    C: Collector,
    S: Sink<C::Record>,
    F: FnMut(&C::Record) -> K + Send,
    K: Eq + Hash + Send,
{
    /// Run until `cancel` fires, then drain the sink stack and return.
    ///
    /// Collector and sink errors are logged, never fatal: the loop keeps
    /// ticking and durable layers retry on their own cadence.
    pub async fn run(mut self, cancel: CancellationToken) {
        let name = self.collector.name().to_owned();
        info!(pipeline = %name, interval = ?self.poll_interval, "pipeline starting");

        // Trigger startup recovery in durable layers (a no-op elsewhere)
        // before the first tick.
        if let Err(error) = self.sink.flush().await {
            warn!(pipeline = %name, %error, "startup flush failed");
        }

        let mut interval = time::interval(self.poll_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                _instant = interval.tick() => self.tick(&name).await,
            }
        }

        info!(pipeline = %name, "pipeline shutting down; draining sink stack");
        if let Err(error) = self.sink.flush().await {
            error!(pipeline = %name, %error, "final flush failed; spooled data will replay on next start");
        }
    }

    async fn tick(&mut self, name: &str) {
        let start = OffsetDateTime::now_utc();
        let records = match self.collector.collect().await {
            Ok(records) => records,
            Err(error) => {
                warn!(pipeline = %name, %error, "collect failed; will retry next tick");
                return;
            }
        };

        let fetched = records.len();
        let records = self.dedupe(records);
        debug!(pipeline = %name, fetched, fresh = records.len(), "tick");
        if records.is_empty() {
            return;
        }

        let meta = WindowMeta {
            pipeline: name.to_owned(),
            start,
            end: OffsetDateTime::now_utc(),
        };
        if let Err(error) = self.sink.ingest(&meta, records).await {
            warn!(pipeline = %name, %error, "sink ingest failed");
        }
    }

    fn dedupe(&mut self, records: Vec<C::Record>) -> Vec<C::Record> {
        let Some(key_fn) = &mut self.key_fn else {
            return records;
        };
        let mut seen_curr = HashSet::new();
        let mut fresh = Vec::with_capacity(records.len());
        for record in records {
            let key = key_fn(&record);
            let in_prev = self.seen_prev.contains(&key);
            // Insert regardless: a key the API keeps returning must stay
            // remembered, otherwise it would re-emerge after two ticks.
            let in_curr = !seen_curr.insert(key);
            if !in_prev && !in_curr {
                fresh.push(record);
            }
        }
        self.seen_prev = seen_curr;
        fresh
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::test_util::SharedSink;

    /// Emits `(tick, i)` pairs, overlapping the previous tick's batch to
    /// exercise dedup: tick n emits keys n and n+1.
    struct FakeCollector {
        ticks: Arc<AtomicUsize>,
    }

    impl Collector for FakeCollector {
        type Record = (usize, usize);
        type Error = Infallible;

        fn name(&self) -> &'static str {
            "fake"
        }

        async fn collect(&mut self) -> Result<Vec<Self::Record>, Infallible> {
            let tick = self.ticks.fetch_add(1, Ordering::SeqCst);
            Ok(vec![(tick, 0), (tick + 1, 0)])
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ticks_dedupes_and_flushes_on_cancel() {
        let sink = SharedSink::<(usize, usize)>::new();
        let pipeline = Pipeline::new(
            FakeCollector {
                ticks: Arc::new(AtomicUsize::new(0)),
            },
            sink.clone(),
            Duration::from_secs(60),
        )
        .with_key_fn(|r: &(usize, usize)| *r);

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(pipeline.run(cancel.clone()));

        // Paused clock auto-advances: sleep past three ticks.
        time::sleep(Duration::from_secs(150)).await;
        cancel.cancel();
        handle.await.unwrap();

        // Ticks emit {0,1}, {1,2}, {2,3}: dedup keeps each key once.
        let records = sink.records();
        assert_eq!(records, vec![(0, 0), (1, 0), (2, 0), (3, 0)]);
        assert!(sink.flushed());
    }

    #[tokio::test(start_paused = true)]
    async fn collector_errors_are_not_fatal() {
        struct Flaky {
            calls: Arc<AtomicUsize>,
        }

        #[derive(Debug, thiserror::Error)]
        #[error("flaky")]
        struct FlakyError;

        impl Collector for Flaky {
            type Record = usize;
            type Error = FlakyError;

            fn name(&self) -> &'static str {
                "flaky"
            }

            async fn collect(&mut self) -> Result<Vec<usize>, FlakyError> {
                let call = self.calls.fetch_add(1, Ordering::SeqCst);
                if call.is_multiple_of(2) {
                    Err(FlakyError)
                } else {
                    Ok(vec![call])
                }
            }
        }

        let sink = SharedSink::<usize>::new();
        let pipeline = Pipeline::new(
            Flaky {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            sink.clone(),
            Duration::from_secs(60),
        );

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(pipeline.run(cancel.clone()));
        time::sleep(Duration::from_secs(250)).await;
        cancel.cancel();
        handle.await.unwrap();

        assert_eq!(sink.records(), vec![1, 3]);
    }
}
