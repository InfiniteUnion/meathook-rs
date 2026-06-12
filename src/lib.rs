//! # meathook
//!
//! A library-first, tower-style base for long-running data collection jobs:
//! poll APIs on an interval, buffer samples into time windows, and ship them
//! to long-term storage.
//!
//! The two core abstractions are:
//!
//! - [`Collector`]: produces a batch of records per tick. The
//!   [`satay::SatayCollector`] adapter turns any [satay](https://docs.rs/satay-runtime)
//!   generated API client into a collector.
//! - [`Sink`]: receives records. Sinks compose like tower layers — a
//!   buffering tier ([`Buffered`]), a durable write-ahead spool
//!   ([`DiskSpool`]), fan-out ([`Tee`]), and terminal sinks such as
//!   [`HfSink`](sink::huggingface::HfSink) stack via [`SinkExt`].
//!
//! Pipelines (collector + sink stack) are supervised by the [`Meathook`]
//! runtime: one tokio task each, respawn-on-panic with backoff, and a final
//! `flush()` through the whole stack on graceful shutdown.

pub mod collector;
#[cfg(feature = "parquet")]
pub mod encode;
pub mod layer;
pub mod pipeline;
pub mod runtime;
pub mod satay;
pub mod sink;

#[cfg(test)]
pub(crate) mod test_util;

pub use collector::Collector;
pub use layer::{Buffered, DiskSpool, FlushPolicy, SinkExt, SpoolError, Tee, TeeError};
pub use pipeline::Pipeline;
pub use runtime::{Meathook, MeathookBuilder, RuntimeError};
pub use satay::SatayCollector;
pub use sink::{Sink, WindowMeta};

#[cfg(feature = "parquet")]
pub use encode::EncodeError;
#[cfg(feature = "huggingface")]
pub use sink::huggingface::{HfSink, HfSinkError};
