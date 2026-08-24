//! The [`Sink`] trait and window metadata passed alongside records.

use std::error;
use std::future::Future;

use time::OffsetDateTime;

#[cfg(feature = "hf-bucket")]
pub mod hf_bucket;

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

    /// Advance this sink to `now`, delivering windows which have closed by
    /// that wall-clock time while retaining the current partial window.
    ///
    /// Terminal sinks and layers without time-based state may use this
    /// default no-op implementation. Buffering combinators must propagate
    /// advancement to their children.
    fn advance(
        &mut self,
        _now: OffsetDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    /// Force-drain this layer and everything downstream, including the
    /// active partial wall-clock window.
    fn flush(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// FNV-1a 64-bit — stable across releases (the fingerprint is load-bearing
/// for replay idempotency: same bytes must map to the same path forever).
pub(crate) fn fingerprint(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0100_0000_01b3;
    bytes.iter().fold(OFFSET_BASIS, |hash, &byte| {
        (hash ^ u64::from(byte)).wrapping_mul(PRIME)
    })
}

/// Object path keyed by the full window start plus a fingerprint of the
/// encoded bytes. The start (to the second) identifies the window — distinct
/// windows get distinct paths no matter their content; the fingerprint
/// separates repeated drains of one window — paths only ever collide when
/// window *and* content match, and then overwriting is a no-op (see
/// [`HfSink`] docs; bucket sinks share the same contract).
///
/// [`HfSink`]: crate::sink::huggingface::HfSink
pub(crate) fn object_path(meta: &WindowMeta, content: &[u8], ext: &str) -> String {
    let date = meta.start.date();
    format!(
        "data/{}/{:04}-{:02}-{:02}/{:02}-{:02}-{:02}-{:016x}.{}",
        meta.pipeline,
        date.year(),
        u8::from(date.month()),
        date.day(),
        meta.start.hour(),
        meta.start.minute(),
        meta.start.second(),
        fingerprint(content),
        ext,
    )
}
