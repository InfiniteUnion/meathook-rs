//! The [`Sink`] trait and window metadata passed alongside records.

use std::error;
use std::future::Future;

use time::OffsetDateTime;

#[cfg(feature = "huggingface")]
pub mod huggingface;

/// Metadata describing the time window a batch of records belongs to.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowMeta {
    /// Pipeline (collector) name; used for partitioning storage paths.
    pub pipeline: String,
    /// Start of the window the records were collected in.
    pub start: OffsetDateTime,
    /// End of the window (the time the batch was handed over).
    pub end: OffsetDateTime,
}

/// A destination for records.
///
/// Build buffering and durable layers in record-entry order with
/// [`SinkStack`](crate::SinkStack), then finish with a terminal sink. Fan out
/// completed sinks with [`SinkExt::tee`](crate::SinkExt::tee).
pub trait Sink<R>: Send {
    /// Concrete error type (a `thiserror` enum, not a boxed error).
    type Error: error::Error + Send + Sync + 'static;

    /// Hand records to this layer. A buffering layer may hold them; a
    /// terminal sink ships them immediately.
    fn ingest(
        &mut self,
        meta: &WindowMeta,
        records: Vec<R>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Force-drain this layer and everything downstream (shutdown, final
    /// flush, startup recovery).
    fn flush(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
