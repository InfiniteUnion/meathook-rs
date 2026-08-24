//! [`Tier`]: one buffering layer, generic over its [`Store`] backend.
//!
//! A tier owns all the pipeline-side buffering logic exactly once —
//! wall-clock window alignment, [`FlushPolicy`] firing, replay-on-startup,
//! and retain-on-downstream-failure — and delegates the actual holding of
//! records to a [`Store`]. `Tier` backed by [`MemStore`](crate::MemStore)
//! is a volatile in-memory buffer; backed by
//! [`JsonlStore`](crate::JsonlStore) it is a durable write-ahead spool.

use std::error;
use std::marker::PhantomData;

use time::OffsetDateTime;
use tracing::{debug, warn};

use super::FlushPolicy;
use crate::sink::{Sink, WindowMeta};
use crate::store::{Segment as _, Store};

/// Error from a [`Tier`] layer.
#[derive(Debug, thiserror::Error)]
pub enum TierError<St, S>
where
    St: error::Error + Send + Sync + 'static,
    S: error::Error + Send + Sync + 'static,
{
    /// The tier's store failed to append, read, or remove a window.
    #[error("store error: {0}")]
    Store(#[source] St),
    /// The wrapped sink rejected a drained window.
    #[error("downstream sink error: {0}")]
    Downstream(#[source] S),
}

/// Buffering tier over a pluggable [`Store`].
///
/// Records ingest into the wall-clock-aligned window
/// `align(meta.end)` (aligned to the policy's `every`); when the policy
/// fires, stored windows drain downstream oldest-first, each removed from
/// the store only after the downstream sink accepted it — a transient
/// outage of the terminal sink does not lose data held in this tier.
///
/// One window key can drain downstream **more than once**: the
/// `max_records` valve fires mid-window and later records re-open the same
/// key, and a failed drain is retried after the window has grown. Both
/// deliveries carry the same [`WindowMeta`], so a terminal sink that keys
/// storage by window start alone would overwrite the earlier chunk — key
/// by content as well, as `HfSink` (feature `huggingface`) does.
///
/// A window whose delivery keeps failing (for example a record the
/// terminal sink deterministically rejects) does not stall the pipeline:
/// each drain pass attempts every closed window oldest-first, retains the
/// failed ones for the next pass, and surfaces the first error. Such a
/// poisoned window is retried on every pass and retained in the store
/// indefinitely — remove its segment by hand (for
/// [`JsonlStore`](crate::JsonlStore), delete the window's `.jsonl` file)
/// if it must be discarded.
///
/// [`advance`](Sink::advance) closes elapsed wall-clock windows even when a
/// collector produces no records, while [`flush`](Sink::flush) force-drains
/// the active partial window as well. On first use, persisted windows older
/// than the current wall window replay downstream and the current segment's
/// record count is restored so `max_records` still applies across restarts.
/// Replayed [`WindowMeta`] is reconstructed from the window key alone, so
/// replayed windows land at the same storage path (idempotent).
pub struct Tier<R, St, S> {
    /// Backend that persists buffered records by aligned window key.
    store: St,
    /// Policy that defines the window duration and record-count flush threshold.
    policy: FlushPolicy,
    /// Downstream sink that receives drained windows.
    inner: S,
    /// Pipeline name attached to drained window metadata.
    pipeline: Option<String>,
    /// Whether the one-time startup advancement has been attempted.
    initialized: bool,
    /// Aligned Unix timestamp identifying the window currently accepting records.
    active_window: Option<i64>,
    /// Number of records persisted in the active window.
    active_count: usize,
    /// Associates the tier with its record type without storing a record.
    _record: PhantomData<fn() -> R>,
}

impl<R, St, S> Tier<R, St, S> {
    /// Create a tier holding records in `store` until `policy` fires.
    #[must_use]
    pub fn new(store: St, policy: FlushPolicy, inner: S) -> Self {
        Self {
            store,
            policy,
            inner,
            pipeline: None,
            initialized: false,
            active_window: None,
            active_count: 0,
            _record: PhantomData,
        }
    }

    /// Override the pipeline name used in drained [`WindowMeta`] (default:
    /// the first live meta seen, then the store's
    /// [`pipeline_hint`](Store::pipeline_hint)).
    #[must_use]
    pub fn with_pipeline_name(mut self, name: impl Into<String>) -> Self {
        self.pipeline = Some(name.into());
        self
    }

    /// Access the wrapped sink.
    #[must_use]
    pub fn inner(&self) -> &S {
        &self.inner
    }

    fn window_secs(&self) -> i64 {
        i64::try_from(self.policy.every.as_secs())
            .unwrap_or(i64::MAX)
            .max(1)
    }

    fn align(&self, unix: i64) -> i64 {
        unix - unix.rem_euclid(self.window_secs())
    }
}

impl<R, St, S> Tier<R, St, S>
where
    R: Send + 'static,
    St: Store<R>,
    S: Sink<R>,
{
    fn pipeline_name(&self) -> String {
        self.pipeline
            .as_deref()
            .or_else(|| self.store.pipeline_hint())
            .unwrap_or("unknown")
            .to_owned()
    }

    /// First-use crash recovery at the supplied wall time. Failures are
    /// logged, not propagated: retained windows retry on this ingest's drain
    /// pass or the next lifecycle heartbeat, and new records may still be
    /// appended when the store remains writable.
    async fn ensure_init(&mut self, now: OffsetDateTime) {
        if self.initialized {
            return;
        }
        if let Err(error) = self.advance(now).await {
            warn!(
                pipeline = %self.pipeline_name(),
                %error,
                "startup window advancement failed; stored windows retained for retry"
            );
        }
    }

    /// Count records already persisted under `target` without committing
    /// the segment. This restores the `max_records` safety valve after a
    /// process restart without expanding the public [`Store`] contract.
    async fn stored_count(&mut self, target: i64) -> Result<usize, TierError<St::Error, S::Error>> {
        let mut cursor = None;
        loop {
            let Some(mut seg) = self.store.oldest(cursor).await.map_err(TierError::Store)? else {
                return Ok(0);
            };
            let window = seg.window();
            if window < target {
                cursor = Some(window);
                continue;
            }
            if window > target {
                return Ok(0);
            }
            return seg
                .records()
                .await
                .map(|records| records.len())
                .map_err(TierError::Store);
        }
    }

    async fn activate(&mut self, window: i64) -> Result<(), TierError<St::Error, S::Error>> {
        let count = self.stored_count(window).await?;
        self.active_window = Some(window);
        self.active_count = count;
        Ok(())
    }

    /// Drain stored windows downstream, oldest first, removing each from
    /// the store only after downstream success. A failed window —
    /// unreadable, rejected downstream, or failing removal — is retained
    /// for the next pass and does not block newer windows: the pass skips
    /// past it, attempts the rest, and returns the first error at the end.
    /// With `include_active == false`, the active window and any future
    /// windows are retained; only windows strictly older than the current
    /// aligned wall window are eligible.
    async fn drain(&mut self, include_active: bool) -> Result<(), TierError<St::Error, S::Error>> {
        // Hoisted so no `&self` method call overlaps the live segment
        // borrow below.
        let pipeline = self.pipeline_name();
        let window_secs = self.window_secs();
        // Windows at or below the cursor were skipped this pass (failed or
        // still active); skipped segments drop uncommitted, so their
        // records stay in the store.
        let mut cursor = None;
        let mut first_error = None;
        loop {
            let Some(mut seg) = self.store.oldest(cursor).await.map_err(TierError::Store)? else {
                break;
            };
            let window = seg.window();
            if !include_active && self.active_window.is_some_and(|active| window >= active) {
                // Segments are ordered, so this window and everything after
                // it is current or future custody and must remain stored.
                break;
            }
            let records = match seg.records().await {
                Ok(records) => records,
                Err(error) => {
                    warn!(
                        pipeline = %pipeline,
                        window,
                        %error,
                        "failed to read stored window; retained for retry"
                    );
                    first_error.get_or_insert(TierError::Store(error));
                    cursor = Some(window);
                    continue;
                }
            };
            if !records.is_empty() {
                let start = OffsetDateTime::from_unix_timestamp(window)
                    .unwrap_or(OffsetDateTime::UNIX_EPOCH);
                let meta = WindowMeta {
                    pipeline: pipeline.clone(),
                    start,
                    // `saturating_add`: a time-unbounded policy (e.g.
                    // `every == Duration::MAX`) makes `window_secs` equal
                    // `i64::MAX`; plain `+` panics on `time` range overflow.
                    end: start.saturating_add(time::Duration::seconds(window_secs)),
                };
                debug!(
                    pipeline = %pipeline,
                    window,
                    records = records.len(),
                    "tier draining window downstream"
                );
                if let Err(error) = self.inner.ingest(&meta, records).await {
                    warn!(
                        pipeline = %pipeline,
                        window,
                        %error,
                        "downstream rejected window; retained for retry"
                    );
                    first_error.get_or_insert(TierError::Downstream(error));
                    cursor = Some(window);
                    continue;
                }
            }
            if let Err(error) = seg.commit().await {
                warn!(
                    pipeline = %pipeline,
                    window,
                    %error,
                    "failed to remove drained window from store"
                );
                first_error.get_or_insert(TierError::Store(error));
                cursor = Some(window);
                continue;
            }
            if Some(window) == self.active_window {
                self.active_window = None;
                self.active_count = 0;
            }
        }
        match first_error {
            None => Ok(()),
            Some(error) => Err(error),
        }
    }
}

impl<R, St, S> Sink<R> for Tier<R, St, S>
where
    R: Send + 'static,
    St: Store<R>,
    S: Sink<R>,
{
    type Error = TierError<St::Error, S::Error>;

    async fn ingest(&mut self, meta: &WindowMeta, records: Vec<R>) -> Result<(), Self::Error> {
        if self.pipeline.is_none() {
            self.pipeline = Some(meta.pipeline.clone());
        }
        self.ensure_init(meta.end).await;

        if !records.is_empty() {
            let window = self.align(meta.end.unix_timestamp());
            if self.active_window != Some(window) {
                self.activate(window).await?;
            }
            let count = records.len();
            self.store
                .append(window, records)
                .await
                .map_err(TierError::Store)?;
            self.active_count += count;
        }

        if self.active_count >= self.policy.max_records {
            self.drain(true).await
        } else {
            self.drain(false).await
        }
    }

    async fn advance(&mut self, now: OffsetDateTime) -> Result<(), Self::Error> {
        self.initialized = true;
        let current = self.align(now.unix_timestamp());
        if self.active_window != Some(current) {
            self.activate(current).await?;
        }

        if self.active_count >= self.policy.max_records {
            self.drain(true).await?;
        } else {
            self.drain(false).await?;
        }
        self.inner.advance(now).await.map_err(TierError::Downstream)
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        // flush IS the recovery drain, so skip ensure_init's error-swallowing
        // variant: errors here must propagate (final flush on shutdown).
        self.initialized = true;
        self.drain(true).await?;
        self.inner.flush().await.map_err(TierError::Downstream)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::store::MemStore;
    use crate::test_util::{SharedSink, TestSinkFailure, meta_at};

    /// Rejects any batch containing `13` while armed; accepted batches
    /// land in the wrapped [`SharedSink`].
    struct PoisonSink {
        inner: SharedSink<i32>,
        armed: Arc<AtomicBool>,
    }

    impl Sink<i32> for PoisonSink {
        type Error = TestSinkFailure;

        async fn ingest(
            &mut self,
            meta: &WindowMeta,
            records: Vec<i32>,
        ) -> Result<(), TestSinkFailure> {
            if self.armed.load(Ordering::SeqCst) && records.contains(&13) {
                return Err(TestSinkFailure);
            }
            self.inner.ingest(meta, records).await
        }

        async fn flush(&mut self) -> Result<(), TestSinkFailure> {
            self.inner.flush().await
        }
    }

    #[tokio::test]
    async fn mem_tier_holds_until_max_records() {
        let inner = SharedSink::new();
        let mut sink = Tier::new(
            MemStore::new(),
            FlushPolicy::new(Duration::from_secs(3600), 3),
            inner.clone(),
        );

        sink.ingest(&meta_at("p", 10), vec![1, 2]).await.unwrap();
        assert!(inner.batches().is_empty());

        sink.ingest(&meta_at("p", 20), vec![3]).await.unwrap();
        let batches = inner.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].1, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn time_unbounded_policy_drains_on_record_count() {
        let inner = SharedSink::new();
        let mut sink = Tier::new(
            MemStore::new(),
            FlushPolicy::new(Duration::MAX, 2),
            inner.clone(),
        );

        // All records land in the single epoch-anchored window; only the
        // record cap can fire. Draining must not panic computing the
        // window's `end` (epoch + i64::MAX seconds overflows `time`).
        sink.ingest(&meta_at("p", 10), vec![1]).await.unwrap();
        assert!(inner.batches().is_empty());

        sink.ingest(&meta_at("p", 20), vec![2]).await.unwrap();
        let batches = inner.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].0.start.unix_timestamp(), 0);
        // `end` saturates to `time`'s max representable datetime.
        assert_eq!(
            batches[0].0.end,
            batches[0]
                .0
                .start
                .saturating_add(time::Duration::seconds(i64::MAX))
        );
        assert_eq!(batches[0].1, vec![1, 2]);
    }

    #[tokio::test]
    async fn mem_tier_drains_closed_window_on_next_ingest() {
        let inner = SharedSink::new();
        let mut sink = Tier::new(
            MemStore::new(),
            FlushPolicy::every(Duration::from_secs(300)),
            inner.clone(),
        );

        sink.ingest(&meta_at("p", 10), vec![1]).await.unwrap();
        assert!(inner.batches().is_empty());

        // The next tick lands in the following aligned window: the previous
        // window is now closed and ships; the new record stays held.
        sink.ingest(&meta_at("p", 310), vec![2]).await.unwrap();
        let batches = inner.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].0.pipeline, "p");
        assert_eq!(batches[0].0.start.unix_timestamp(), 0);
        assert_eq!(batches[0].0.end.unix_timestamp(), 300);
        assert_eq!(batches[0].1, vec![1]);
    }

    #[tokio::test]
    async fn advance_drains_a_closed_window_without_new_records() {
        let inner = SharedSink::new();
        let mut sink = Tier::new(
            MemStore::new(),
            FlushPolicy::every(Duration::from_secs(300)),
            inner.clone(),
        );

        sink.ingest(&meta_at("p", 10), vec![1]).await.unwrap();
        sink.advance(OffsetDateTime::from_unix_timestamp(310).unwrap())
            .await
            .unwrap();

        let batches = inner.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].0.start.unix_timestamp(), 0);
        assert_eq!(batches[0].1, vec![1]);
    }

    #[tokio::test]
    async fn mem_tier_retains_records_across_failing_downstream() {
        let inner = SharedSink::new();
        let mut sink = Tier::new(
            MemStore::new(),
            FlushPolicy::new(Duration::from_secs(3600), 2),
            inner.clone(),
        );

        inner.set_fail(true);
        assert!(sink.ingest(&meta_at("p", 10), vec![1, 2]).await.is_err());
        assert!(inner.batches().is_empty());

        inner.set_fail(false);
        sink.ingest(&meta_at("p", 20), vec![3]).await.unwrap();
        let batches = inner.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].1, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn flush_drains_the_whole_stack() {
        let bottom = SharedSink::new();
        let mut sink = Tier::new(
            MemStore::new(),
            FlushPolicy::hourly(),
            Tier::new(MemStore::new(), FlushPolicy::hourly(), bottom.clone()),
        );

        sink.ingest(&meta_at("p", 10), vec![1, 2, 3]).await.unwrap();
        assert!(bottom.batches().is_empty());

        sink.flush().await.unwrap();
        assert_eq!(bottom.batches()[0].1, vec![1, 2, 3]);
        assert!(bottom.flushed());
    }

    #[tokio::test]
    async fn poison_window_does_not_block_newer_windows() {
        let inner = SharedSink::new();
        let armed = Arc::new(AtomicBool::new(true));
        let mut sink = Tier::new(
            MemStore::new(),
            FlushPolicy::every(Duration::from_secs(300)),
            PoisonSink {
                inner: inner.clone(),
                armed: Arc::clone(&armed),
            },
        );

        // Window 0 gets the poison record; it closes on the next tick.
        sink.ingest(&meta_at("p", 10), vec![13]).await.unwrap();
        assert!(sink.ingest(&meta_at("p", 310), vec![2]).await.is_err());
        assert!(inner.batches().is_empty());

        // Next tick: window 0 still fails, but the now-closed window 300
        // drains past it. The first error still surfaces.
        assert!(sink.ingest(&meta_at("p", 610), vec![3]).await.is_err());
        let batches = inner.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].0.start.unix_timestamp(), 300);
        assert_eq!(batches[0].1, vec![2]);

        // The poison window was retained, not dropped: once the sink
        // accepts it, it ships before newer held windows.
        armed.store(false, Ordering::SeqCst);
        sink.flush().await.unwrap();
        let batches = inner.batches();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[1].0.start.unix_timestamp(), 0);
        assert_eq!(batches[1].1, vec![13]);
        assert_eq!(batches[2].0.start.unix_timestamp(), 600);
        assert_eq!(batches[2].1, vec![3]);
    }

    #[tokio::test]
    async fn outage_retains_all_windows_and_recovers_in_order() {
        let inner = SharedSink::new();
        let mut sink = Tier::new(
            MemStore::new(),
            FlushPolicy::every(Duration::from_secs(300)),
            inner.clone(),
        );

        sink.ingest(&meta_at("p", 10), vec![1]).await.unwrap();
        inner.set_fail(true);
        assert!(sink.ingest(&meta_at("p", 310), vec![2]).await.is_err());
        assert!(sink.ingest(&meta_at("p", 610), vec![3]).await.is_err());
        assert!(inner.batches().is_empty());

        inner.set_fail(false);
        sink.flush().await.unwrap();
        let starts = inner
            .batches()
            .iter()
            .map(|(meta, records)| (meta.start.unix_timestamp(), records.clone()))
            .collect::<Vec<_>>();
        assert_eq!(starts, vec![(0, vec![1]), (300, vec![2]), (600, vec![3])]);
    }
}
