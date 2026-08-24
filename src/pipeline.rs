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

/// What a pipeline does with its active partial window on graceful shutdown.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ShutdownPolicy {
    /// Force-deliver every buffered record, including the active partial
    /// wall-clock window. This preserves the existing lifecycle behavior.
    #[default]
    FlushAll,
    /// Deliver only closed windows and leave the active window in its store
    /// for a later process to continue.
    ///
    /// This is only durable when the active records are held by a durable
    /// store such as [`JsonlStore`](crate::JsonlStore). An active
    /// [`MemStore`](crate::MemStore) window is lost when the process exits.
    PreserveActiveWindow,
}

trait WallClock {
    fn now_utc(&self) -> OffsetDateTime;
}

#[derive(Debug, Clone, Copy)]
struct SystemClock;

impl WallClock for SystemClock {
    fn now_utc(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

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
    shutdown_policy: ShutdownPolicy,
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
            shutdown_policy: ShutdownPolicy::default(),
            key_fn: None,
            seen_prev: HashSet::new(),
        }
    }
}

impl<C, S, F, K> Pipeline<C, S, F, K>
where
    C: Collector,
{
    /// Choose whether graceful shutdown force-flushes the active partial
    /// window or preserves it for a later process. The default is
    /// [`ShutdownPolicy::FlushAll`].
    #[must_use]
    pub fn with_shutdown_policy(mut self, shutdown_policy: ShutdownPolicy) -> Self {
        self.shutdown_policy = shutdown_policy;
        self
    }

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
            shutdown_policy: self.shutdown_policy,
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
    /// Run until `cancel` fires, then apply the configured shutdown policy
    /// and return.
    ///
    /// Collector and sink errors are logged, never fatal: the loop keeps
    /// ticking and durable layers retry on their own cadence.
    pub async fn run(self, cancel: CancellationToken) {
        self.run_with_clock(cancel, SystemClock).await;
    }

    async fn run_with_clock<W>(mut self, cancel: CancellationToken, clock: W)
    where
        W: WallClock,
    {
        let name = self.collector.name().to_owned();
        info!(pipeline = %name, interval = ?self.poll_interval, "pipeline starting");

        // Recover closed durable windows without splitting the wall-clock
        // window which is still active at startup.
        if let Err(error) = self.sink.advance(clock.now_utc()).await {
            warn!(pipeline = %name, %error, "startup window advancement failed");
        }

        let mut interval = time::interval(self.poll_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                _instant = interval.tick() => self.tick(&name, &clock).await,
            }
        }

        let result = match self.shutdown_policy {
            ShutdownPolicy::FlushAll => {
                info!(pipeline = %name, "pipeline shutting down; force-draining sink stack");
                self.sink.flush().await
            }
            ShutdownPolicy::PreserveActiveWindow => {
                info!(pipeline = %name, "pipeline shutting down; preserving active wall-clock window");
                self.sink.advance(clock.now_utc()).await
            }
        };
        if let Err(error) = result {
            error!(pipeline = %name, %error, "final sink lifecycle operation failed; durably stored data will replay on next start");
        }
    }

    async fn tick(&mut self, name: &str, clock: &impl WallClock) {
        let start = clock.now_utc();
        // This heartbeat is independent of collection success or batch
        // size, so an idle collector still closes elapsed windows.
        if let Err(error) = self.sink.advance(start).await {
            warn!(pipeline = %name, %error, "window advancement failed");
        }
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
            end: clock.now_utc(),
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
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ::time::macros::datetime;
    use parking_lot::Mutex;
    use serde::{Deserialize, Serialize};
    use tokio::sync::Notify;

    use super::*;
    use crate::test_util::{FakeObjectSink, SharedSink};
    use crate::{FlushPolicy, JsonlStore, Tier};

    #[derive(Clone)]
    struct ManualClock(Arc<Mutex<OffsetDateTime>>);

    impl ManualClock {
        fn at(now: OffsetDateTime) -> Self {
            Self(Arc::new(Mutex::new(now)))
        }

        fn set(&self, now: OffsetDateTime) {
            *self.0.lock() = now;
        }
    }

    impl WallClock for ManualClock {
        fn now_utc(&self) -> OffsetDateTime {
            *self.0.lock()
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Pm25Reading {
        region: String,
        timestamp: String,
        value: f64,
    }

    enum Pm25Poll {
        Records(Vec<Pm25Reading>),
        Empty,
        Error,
    }

    #[derive(Clone, Default)]
    struct PollProgress {
        completed: Arc<AtomicUsize>,
        changed: Arc<Notify>,
    }

    impl PollProgress {
        async fn wait_for(&self, target: usize) {
            loop {
                if self.completed.load(Ordering::SeqCst) >= target {
                    return;
                }
                let changed = self.changed.notified();
                if self.completed.load(Ordering::SeqCst) >= target {
                    return;
                }
                changed.await;
            }
        }
    }

    struct ScriptedPm25 {
        polls: VecDeque<Pm25Poll>,
        progress: PollProgress,
    }

    impl ScriptedPm25 {
        fn new(polls: impl IntoIterator<Item = Pm25Poll>) -> Self {
            Self {
                polls: polls.into_iter().collect(),
                progress: PollProgress::default(),
            }
        }

        fn progress(&self) -> PollProgress {
            self.progress.clone()
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("scripted PM2.5 provider failure")]
    struct ScriptedPm25Error;

    impl Collector for ScriptedPm25 {
        type Record = Pm25Reading;
        type Error = ScriptedPm25Error;

        fn name(&self) -> &'static str {
            "pm25"
        }

        fn collect(
            &mut self,
        ) -> impl Future<Output = Result<Vec<Pm25Reading>, ScriptedPm25Error>> + Send {
            let result = match self.polls.pop_front().unwrap_or(Pm25Poll::Empty) {
                Pm25Poll::Records(records) => Ok(records),
                Pm25Poll::Empty => Ok(vec![]),
                Pm25Poll::Error => Err(ScriptedPm25Error),
            };
            self.progress.completed.fetch_add(1, Ordering::SeqCst);
            self.progress.changed.notify_one();
            std::future::ready(result)
        }
    }

    fn pm25_rows(timestamp: &str, base: f64) -> Vec<Pm25Reading> {
        ["east", "west", "north", "south", "central"]
            .into_iter()
            .enumerate()
            .map(|(offset, region)| Pm25Reading {
                region: region.to_owned(),
                timestamp: timestamp.to_owned(),
                value: base + offset as f64,
            })
            .collect()
    }
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

        fn collect(
            &mut self,
        ) -> impl Future<Output = Result<Vec<Self::Record>, Infallible>> + Send {
            let tick = self.ticks.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(vec![(tick, 0), (tick + 1, 0)]))
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

            fn collect(&mut self) -> impl Future<Output = Result<Vec<usize>, FlakyError>> + Send {
                let call = self.calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(if call.is_multiple_of(2) {
                    Err(FlakyError)
                } else {
                    Ok(vec![call])
                })
            }
        }

        let sink = SharedSink::<usize>::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let pipeline = Pipeline::new(
            Flaky {
                calls: Arc::clone(&calls),
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
        assert_eq!(sink.advances(), calls.load(Ordering::SeqCst) + 1);
    }

    #[tokio::test(start_paused = true)]
    async fn empty_polls_advance_and_preserve_shutdown_does_not_flush() {
        struct Empty {
            calls: Arc<AtomicUsize>,
        }

        impl Collector for Empty {
            type Record = usize;
            type Error = Infallible;

            fn name(&self) -> &'static str {
                "empty"
            }

            fn collect(&mut self) -> impl Future<Output = Result<Vec<usize>, Infallible>> + Send {
                self.calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(vec![]))
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let sink = SharedSink::<usize>::new();
        let pipeline = Pipeline::new(
            Empty {
                calls: Arc::clone(&calls),
            },
            sink.clone(),
            Duration::from_secs(60),
        )
        .with_shutdown_policy(ShutdownPolicy::PreserveActiveWindow);

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(pipeline.run(cancel.clone()));
        time::sleep(Duration::from_secs(150)).await;
        cancel.cancel();
        handle.await.unwrap();

        assert!(sink.records().is_empty());
        assert_eq!(sink.advances(), calls.load(Ordering::SeqCst) + 2);
        assert!(!sink.flushed());
    }
    #[tokio::test(start_paused = true)]
    async fn configured_pm25_poll_uploads_previous_fifteen_minute_window() {
        let spool = tempfile::tempdir().unwrap();
        let spool_dir = spool.path().join("pm25");
        let remote = FakeObjectSink::default();
        let first = pm25_rows("2026-08-25T12:00:00Z", 10.0);
        let second = pm25_rows("2026-08-25T12:15:00Z", 20.0);
        let collector = ScriptedPm25::new([
            Pm25Poll::Records(first.clone()),
            Pm25Poll::Records(second.clone()),
        ]);
        let progress = collector.progress();
        let stack = Tier::new(
            JsonlStore::new(&spool_dir),
            FlushPolicy::every(Duration::from_secs(900)),
            remote.clone(),
        );
        let pipeline = Pipeline::new(collector, stack, Duration::from_secs(900))
            .with_shutdown_policy(ShutdownPolicy::PreserveActiveWindow)
            .with_key_fn(|row: &Pm25Reading| (row.region.clone(), row.timestamp.clone()));
        let clock = ManualClock::at(datetime!(2026-08-25 12:03 UTC));
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(pipeline.run_with_clock(cancel.clone(), clock.clone()));

        progress.wait_for(1).await;
        assert!(remote.objects().is_empty());

        clock.set(datetime!(2026-08-25 12:18 UTC));
        time::advance(Duration::from_secs(900)).await;
        progress.wait_for(2).await;

        cancel.cancel();
        handle.await.unwrap();

        let objects = remote.objects();
        assert_eq!(objects.len(), 1);
        let (path, content) = objects.iter().next().unwrap();
        assert!(path.starts_with("data/pm25/2026-08-25/12-00-00-"));
        assert_eq!(
            serde_json::from_slice::<Vec<Pm25Reading>>(content).unwrap(),
            first
        );

        let segments = std::fs::read_dir(spool_dir)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(segments.len(), 1);
        let active = std::fs::read_to_string(segments[0].path()).unwrap();
        let active = active
            .lines()
            .map(|line| serde_json::from_str::<Pm25Reading>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(active, second);
    }

    #[tokio::test(start_paused = true)]
    async fn provider_error_still_advances_closed_pm25_window() {
        let spool = tempfile::tempdir().unwrap();
        let spool_dir = spool.path().join("pm25");
        let remote = FakeObjectSink::default();
        let collector = ScriptedPm25::new([
            Pm25Poll::Records(pm25_rows("2026-08-25T12:00:00Z", 10.0)),
            Pm25Poll::Error,
        ]);
        let progress = collector.progress();
        let stack = Tier::new(
            JsonlStore::new(&spool_dir),
            FlushPolicy::every(Duration::from_secs(900)),
            remote.clone(),
        );
        let pipeline = Pipeline::new(collector, stack, Duration::from_secs(900))
            .with_shutdown_policy(ShutdownPolicy::PreserveActiveWindow)
            .with_key_fn(|row: &Pm25Reading| (row.region.clone(), row.timestamp.clone()));
        let clock = ManualClock::at(datetime!(2026-08-25 12:03 UTC));
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(pipeline.run_with_clock(cancel.clone(), clock.clone()));

        progress.wait_for(1).await;
        clock.set(datetime!(2026-08-25 12:18 UTC));
        time::advance(Duration::from_secs(900)).await;
        progress.wait_for(2).await;

        cancel.cancel();
        handle.await.unwrap();

        let objects = remote.objects();
        assert_eq!(objects.len(), 1);
        assert!(
            objects
                .keys()
                .next()
                .unwrap()
                .starts_with("data/pm25/2026-08-25/12-00-00-")
        );
        assert_eq!(std::fs::read_dir(spool_dir).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn repeated_pm25_snapshot_is_deduplicated_until_timestamp_changes() {
        let first = pm25_rows("2026-08-25T12:00:00Z", 10.0);
        let second = pm25_rows("2026-08-25T12:15:00Z", 20.0);
        let collector = ScriptedPm25::new([
            Pm25Poll::Records(first.clone()),
            Pm25Poll::Records(first.clone()),
            Pm25Poll::Records(second.clone()),
        ]);
        let sink = SharedSink::new();
        let mut pipeline = Pipeline::new(collector, sink.clone(), Duration::from_secs(900))
            .with_key_fn(|row: &Pm25Reading| (row.region.clone(), row.timestamp.clone()));
        let clock = ManualClock::at(datetime!(2026-08-25 12:03 UTC));

        pipeline.tick("pm25", &clock).await;
        clock.set(datetime!(2026-08-25 12:04 UTC));
        pipeline.tick("pm25", &clock).await;
        clock.set(datetime!(2026-08-25 12:18 UTC));
        pipeline.tick("pm25", &clock).await;

        let mut expected = first;
        expected.extend(second);
        assert_eq!(sink.records(), expected);
    }

    #[tokio::test]
    async fn restart_resets_pm25_dedupe_and_can_repeat_source_rows() {
        let spool = tempfile::tempdir().unwrap();
        let spool_dir = spool.path().join("pm25");
        let remote = FakeObjectSink::default();
        let repeated = pm25_rows("2026-08-25T12:00:00Z", 10.0);
        let clock = ManualClock::at(datetime!(2026-08-25 12:03 UTC));

        {
            let stack = Tier::new(
                JsonlStore::new(&spool_dir),
                FlushPolicy::every(Duration::from_secs(900)),
                remote.clone(),
            );
            let collector = ScriptedPm25::new([Pm25Poll::Records(repeated.clone())]);
            let mut pipeline = Pipeline::new(collector, stack, Duration::from_secs(900))
                .with_key_fn(|row: &Pm25Reading| (row.region.clone(), row.timestamp.clone()));
            pipeline.tick("pm25", &clock).await;
        }

        let stack = Tier::new(
            JsonlStore::new(&spool_dir),
            FlushPolicy::every(Duration::from_secs(900)),
            remote.clone(),
        );
        let collector = ScriptedPm25::new([Pm25Poll::Records(repeated), Pm25Poll::Empty]);
        let mut restarted = Pipeline::new(collector, stack, Duration::from_secs(900))
            .with_key_fn(|row: &Pm25Reading| (row.region.clone(), row.timestamp.clone()));

        clock.set(datetime!(2026-08-25 12:04 UTC));
        restarted.tick("pm25", &clock).await;
        clock.set(datetime!(2026-08-25 12:18 UTC));
        restarted.tick("pm25", &clock).await;

        let objects = remote.objects();
        assert_eq!(objects.len(), 1);
        let rows =
            serde_json::from_slice::<Vec<Pm25Reading>>(objects.values().next().unwrap()).unwrap();
        assert_eq!(rows.len(), 10);
        assert_eq!(&rows[..5], &rows[5..]);
    }
}
