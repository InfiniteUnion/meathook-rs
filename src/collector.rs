//! The [`Collector`] trait: a source that produces a batch of records per tick.

use std::future::Future;

/// A source polled by a [`Pipeline`](crate::Pipeline) on an interval.
///
/// Each tick the pipeline calls [`collect`](Collector::collect) once and
/// hands the resulting records to the sink stack. Errors are logged and the
/// pipeline keeps ticking — a failed poll is not fatal.
pub trait Collector: Send {
    /// The row-shaped record this collector produces.
    type Record: Send + 'static;
    /// Concrete error type (a `thiserror` enum, not a boxed error).
    type Error: std::error::Error + Send + Sync + 'static;

    /// Name of this collector; used as the pipeline name in
    /// [`WindowMeta`](crate::WindowMeta) and for tracing.
    fn name(&self) -> &str;

    /// Fetch one batch of records.
    fn collect(&mut self) -> impl Future<Output = Result<Vec<Self::Record>, Self::Error>> + Send;
}
