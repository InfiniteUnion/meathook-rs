//! Sink combinators: the tower-layer part of meathook.
//!
//! Build buffering tiers in record-entry order with [`SinkStack`]:
//! `MemStore → JsonlStore → terminal`. Each tier owns its records until its
//! [`FlushPolicy`] fires, then pushes them to the next declared tier.

use std::error;
use std::time::Duration;

use crate::sink::{Sink, WindowMeta};

mod builder;
mod tier;

#[doc(hidden)]
pub use builder::BuildSinkStack;
pub use builder::SinkStack;
pub use tier::{Tier, TierError};

/// When a buffering layer pushes its held records downstream.
///
/// A layer fires when its window has been open for `every`, or when it holds
/// at least `max_records`, whichever comes first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushPolicy {
    /// Maximum age of a window before it is flushed downstream.
    ///
    /// An `every` beyond `time`'s representable range (e.g.
    /// [`Duration::MAX`]) never fires on age: records land in a single
    /// epoch-anchored window, only `max_records` (or an explicit flush)
    /// drains it, and the drained window's `end` saturates rather than
    /// overflowing.
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

/// Extension combinators for completed sinks.
///
/// Build buffering layers with [`SinkStack`]; this trait supplies fan-out
/// after a sink or stack has been completed.
pub trait SinkExt<R>: Sink<R> + Sized {
    /// Fan out: every batch is ingested into both `self` and `other`.
    #[must_use]
    fn tee<B: Sink<R>>(self, other: B) -> Tee<Self, B> {
        Tee(self, other)
    }
}

impl<R, S: Sink<R>> SinkExt<R> for S {}

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

    async fn advance(&mut self, now: time::OffsetDateTime) -> Result<(), Self::Error> {
        let first = self.0.advance(now).await;
        let second = self.1.advance(now).await;
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
    async fn tee_advances_both_branches() {
        let a = SharedSink::<i32>::new();
        let b = SharedSink::<i32>::new();
        let mut sink = a.clone().tee(b.clone());

        sink.advance(time::OffsetDateTime::UNIX_EPOCH)
            .await
            .unwrap();

        assert_eq!(a.advances(), 1);
        assert_eq!(b.advances(), 1);
    }
}
