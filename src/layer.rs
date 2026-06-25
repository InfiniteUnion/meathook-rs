//! Sink combinators: the tower-layer part of meathook.
//!
//! Flush tiers stack: `Buffered(mem) → DiskSpool → HfSink`, each tier with
//! its own [`FlushPolicy`]. Each layer owns its records until *its* policy
//! fires, then pushes downstream.

use std::error;
use std::path;
use std::time::Duration;

use ::time::OffsetDateTime;
use tokio::time::Instant;
use tracing::debug;

use crate::sink::{Sink, WindowMeta};

mod disk;

pub use disk::{DiskSpool, SpoolError};

/// When a buffering layer pushes its held records downstream.
///
/// A layer fires when its window has been open for `every`, or when it holds
/// at least `max_records`, whichever comes first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushPolicy {
    /// Maximum age of a window before it is flushed downstream.
    pub every: Duration,
    /// Maximum records held before flushing early (a safety valve; size it
    /// well above one window's worth of records).
    pub max_records: usize,
}

impl FlushPolicy {
    #[must_use]
    pub const fn new(every: Duration, max_records: usize) -> Self {
        Self { every, max_records }
    }

    /// Flush on age only, never on record count.
    #[must_use]
    pub const fn every(every: Duration) -> Self {
        Self::new(every, usize::MAX)
    }

    /// One-hour windows, no record cap.
    #[must_use]
    pub const fn hourly() -> Self {
        Self::every(Duration::from_secs(3600))
    }
}

/// Builder-style composition for sinks, mirroring tower's `ServiceBuilder`.
///
/// ```ignore
/// let sink = hf_sink.spooled("/var/lib/meathook/spool/pm25", FlushPolicy::hourly());
/// ```
pub trait SinkExt<R>: Sink<R> + Sized {
    /// Wrap `self` in an in-memory buffering tier.
    #[must_use]
    fn buffered(self, policy: FlushPolicy) -> Buffered<R, Self> {
        Buffered::new(policy, self)
    }

    /// Wrap `self` in a durable write-ahead spool rooted at `dir`.
    ///
    /// The pipeline name recorded in replayed [`WindowMeta`] is derived from
    /// the last component of `dir`, so point each pipeline at
    /// `spool_root.join(pipeline_name)`.
    #[must_use]
    fn spooled(self, dir: impl Into<path::PathBuf>, policy: FlushPolicy) -> DiskSpool<R, Self> {
        DiskSpool::new(dir, policy, self)
    }

    /// Fan out: every batch is ingested into both `self` and `other`.
    #[must_use]
    fn tee<B: Sink<R>>(self, other: B) -> Tee<Self, B> {
        Tee(self, other)
    }
}

impl<R, S: Sink<R>> SinkExt<R> for S {}

/// In-memory buffering tier: holds records and pushes them downstream when
/// its [`FlushPolicy`] fires.
///
/// The policy is checked on each `ingest` (collector ticks are frequent
/// compared to flush windows, so no extra timer task is needed);
/// [`flush`](Sink::flush) force-drains. On downstream failure the records
/// are **kept** and retried at the next firing — a transient outage of the
/// terminal sink does not lose data held in this tier.
///
/// Requires `R: Clone` so retained records survive a failed downstream
/// ingest.
pub struct Buffered<R, S> {
    buf: Vec<R>,
    window: Option<Window>,
    policy: FlushPolicy,
    inner: S,
}

#[derive(Debug, Clone)]
struct Window {
    pipeline: String,
    start: OffsetDateTime,
    opened_at: Instant,
}

impl<R, S> Buffered<R, S> {
    #[must_use]
    pub fn new(policy: FlushPolicy, inner: S) -> Self {
        Self {
            buf: vec![],
            window: None,
            policy,
            inner,
        }
    }

    /// Access the wrapped sink.
    #[must_use]
    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<R, S> Buffered<R, S>
where
    R: Clone + Send + 'static,
    S: Sink<R>,
{
    async fn drain(&mut self) -> Result<(), S::Error> {
        let Some(window) = &self.window else {
            return Ok(());
        };
        let meta = WindowMeta {
            pipeline: window.pipeline.clone(),
            start: window.start,
            end: OffsetDateTime::now_utc(),
        };
        debug!(
            pipeline = %meta.pipeline,
            records = self.buf.len(),
            "buffered tier draining downstream"
        );
        self.inner.ingest(&meta, self.buf.clone()).await?;
        self.buf.clear();
        self.window = None;
        Ok(())
    }

    fn should_fire(&self) -> bool {
        let Some(window) = &self.window else {
            return false;
        };
        self.buf.len() >= self.policy.max_records || window.opened_at.elapsed() >= self.policy.every
    }
}

impl<R, S> Sink<R> for Buffered<R, S>
where
    R: Clone + Send + 'static,
    S: Sink<R>,
{
    type Error = S::Error;

    async fn ingest(&mut self, meta: &WindowMeta, records: Vec<R>) -> Result<(), Self::Error> {
        if self.window.is_none() {
            self.window = Some(Window {
                pipeline: meta.pipeline.clone(),
                start: meta.start,
                opened_at: Instant::now(),
            });
        }
        self.buf.extend(records);
        if self.should_fire() {
            self.drain().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        self.drain().await?;
        self.inner.flush().await
    }
}

/// Fan-out combinator: ingests every batch into both sinks.
///
/// Both branches are always attempted; errors are reported per branch via
/// [`TeeError`].
pub struct Tee<A, B>(pub A, pub B);

/// Error from a [`Tee`]: one or both branches failed.
#[derive(Debug, thiserror::Error)]
pub enum TeeError<A, B>
where
    A: error::Error + Send + Sync + 'static,
    B: error::Error + Send + Sync + 'static,
{
    #[error("tee: first sink failed: {0}")]
    First(#[source] A),
    #[error("tee: second sink failed: {0}")]
    Second(#[source] B),
    #[error("tee: both sinks failed: first: {first}; second: {second}")]
    Both { first: A, second: B },
}

impl<R, A, B> Sink<R> for Tee<A, B>
where
    R: Clone + Send + 'static,
    A: Sink<R>,
    B: Sink<R>,
{
    type Error = TeeError<A::Error, B::Error>;

    async fn ingest(&mut self, meta: &WindowMeta, records: Vec<R>) -> Result<(), Self::Error> {
        let first = self.0.ingest(meta, records.clone()).await;
        let second = self.1.ingest(meta, records).await;
        match (first, second) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(a), Ok(())) => Err(TeeError::First(a)),
            (Ok(()), Err(b)) => Err(TeeError::Second(b)),
            (Err(a), Err(b)) => Err(TeeError::Both {
                first: a,
                second: b,
            }),
        }
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        let first = self.0.flush().await;
        let second = self.1.flush().await;
        match (first, second) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(a), Ok(())) => Err(TeeError::First(a)),
            (Ok(()), Err(b)) => Err(TeeError::Second(b)),
            (Err(a), Err(b)) => Err(TeeError::Both {
                first: a,
                second: b,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{SharedSink, meta};
    use tokio::time;

    #[tokio::test]
    async fn buffered_holds_until_max_records() {
        let inner = SharedSink::new();
        let mut sink = inner
            .clone()
            .buffered(FlushPolicy::new(Duration::from_secs(3600), 3));

        sink.ingest(&meta("p"), vec![1, 2]).await.unwrap();
        assert!(inner.batches().is_empty());

        sink.ingest(&meta("p"), vec![3]).await.unwrap();
        let batches = inner.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].1, vec![1, 2, 3]);
    }

    #[tokio::test(start_paused = true)]
    async fn buffered_fires_on_elapsed_window() {
        let inner = SharedSink::new();
        let mut sink = inner
            .clone()
            .buffered(FlushPolicy::every(Duration::from_secs(300)));

        sink.ingest(&meta("p"), vec![1]).await.unwrap();
        assert!(inner.batches().is_empty());

        time::advance(Duration::from_secs(301)).await;
        sink.ingest(&meta("p"), vec![2]).await.unwrap();

        let batches = inner.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].1, vec![1, 2]);
    }

    #[tokio::test]
    async fn buffered_retains_records_across_failing_downstream() {
        let inner = SharedSink::new();
        let mut sink = inner
            .clone()
            .buffered(FlushPolicy::new(Duration::from_secs(3600), 2));

        inner.set_fail(true);
        assert!(sink.ingest(&meta("p"), vec![1, 2]).await.is_err());
        assert!(inner.batches().is_empty());

        inner.set_fail(false);
        sink.ingest(&meta("p"), vec![3]).await.unwrap();
        let batches = inner.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].1, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn tee_fans_out_to_both_branches() {
        let a = SharedSink::new();
        let b = SharedSink::new();
        let mut sink = a.clone().tee(b.clone());

        sink.ingest(&meta("p"), vec![1, 2]).await.unwrap();
        assert_eq!(a.batches()[0].1, vec![1, 2]);
        assert_eq!(b.batches()[0].1, vec![1, 2]);
    }

    #[tokio::test]
    async fn tee_reports_failing_branch_but_feeds_the_other() {
        let a = SharedSink::new();
        let b = SharedSink::new();
        a.set_fail(true);
        let mut sink = a.clone().tee(b.clone());

        let err = sink.ingest(&meta("p"), vec![1]).await.unwrap_err();
        assert!(matches!(err, TeeError::First(_)));
        assert_eq!(b.batches().len(), 1);
    }

    #[tokio::test]
    async fn flush_drains_the_whole_stack() {
        let bottom = SharedSink::new();
        let mut sink = bottom
            .clone()
            .buffered(FlushPolicy::hourly())
            .buffered(FlushPolicy::hourly());

        sink.ingest(&meta("p"), vec![1, 2, 3]).await.unwrap();
        assert!(bottom.batches().is_empty());

        sink.flush().await.unwrap();
        assert_eq!(bottom.batches()[0].1, vec![1, 2, 3]);
        assert!(bottom.flushed());
    }
}
