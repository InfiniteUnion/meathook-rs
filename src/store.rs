//! The [`Store`] trait: pluggable window-keyed storage backing a
//! [`Tier`](crate::Tier) layer.
//!
//! A store holds batches of records grouped by window start (unix seconds,
//! aligned by the tier that owns the store). The tier appends on ingest and
//! drains oldest-first, committing each segment only after the downstream
//! sink accepted it — dropping a segment uncommitted must leave its records
//! in the store.
//!
//! Two backends ship with the crate: [`MemStore`] (in-memory, nothing
//! survives a crash) and [`JsonlStore`] (durable write-ahead segment files).
//! Implement [`Store`] to bring your own backend (`SQLite`, object storage,
//! ...); the tier supplies all windowing, flush-policy, and replay logic.

use std::error;
use std::future::Future;

pub mod jsonl;
pub mod mem;

pub use jsonl::{JsonlStore, JsonlStoreError};
pub use mem::MemStore;

/// Window-keyed record storage backing a [`Tier`](crate::Tier).
pub trait Store<R>: Send {
    /// Concrete error type (a `thiserror` enum, not a boxed error).
    type Error: error::Error + Send + Sync + 'static;

    /// Handle to the oldest stored window. May borrow the store; must be
    /// `Send` because the tier holds it across downstream ingest awaits.
    type Segment<'a>: Segment<R, Error = Self::Error> + Send
    where
        Self: 'a;

    /// Add records to `window` (aligned unix seconds). Durable stores must
    /// be write-ahead: don't return until the records would survive a
    /// crash.
    fn append(
        &mut self,
        window: i64,
        records: Vec<R>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Open the oldest stored window strictly newer than `after` (`None`
    /// opens the oldest overall), or `None` when no such window exists.
    ///
    /// The cursor is how a drain pass moves past a window whose delivery
    /// failed without losing its records: the tier drops the segment
    /// uncommitted and resumes iteration after it.
    fn oldest(
        &mut self,
        after: Option<i64>,
    ) -> impl Future<Output = Result<Option<Self::Segment<'_>>, Self::Error>> + Send;

    /// Pipeline name to use for windows replayed before the tier has seen a
    /// live [`WindowMeta`](crate::WindowMeta) (startup crash recovery).
    fn pipeline_hint(&self) -> Option<&str> {
        None
    }
}

/// One window's worth of records checked out of a [`Store`].
pub trait Segment<R> {
    /// Matches the owning store's error type.
    type Error;

    /// Window start (unix seconds).
    fn window(&self) -> i64;

    /// The records in this window. The store must retain the data until
    /// [`commit`](Segment::commit) — a failed downstream ingest followed by
    /// dropping the segment loses nothing.
    fn records(&mut self) -> impl Future<Output = Result<Vec<R>, Self::Error>> + Send;

    /// Permanently remove this segment's records from the store. Call
    /// only after the downstream sink accepted the records.
    ///
    /// Once `commit` returns `Ok(())`, this segment's records must no
    /// longer be observable: [`Store::oldest`] must not yield them again
    /// — the tier's drain loop relies on this to terminate. A backend
    /// that defers physical deletion (lazy or asynchronous cleanup) must
    /// still exclude them from `oldest` immediately. The window key
    /// itself may legitimately reappear when records are appended to it
    /// after the commit (a `max_records` flush mid-window); such a
    /// segment holds only the newly appended records. A failed commit
    /// carries no such obligation: the drain pass retains the window and
    /// skips past it.
    fn commit(self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
