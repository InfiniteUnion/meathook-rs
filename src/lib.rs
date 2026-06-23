//! # meathook
//!
//! A polling runtime with composable, durable sinks: poll a source on an
//! interval, buffer samples into time windows, and ship them to long-term
//! storage — durably.
//!
//! The core (traits, sink combinators, write-ahead spool, supervised
//! runtime) is free of any HTTP/IO stack dependency: with
//! `--no-default-features` you get just that and bring your own collector
//! and terminal sink.
//!
//! The two core abstractions are:
//!
//! - [`Collector`]: produces a batch of records per tick. With the `satay`
//!   feature, [`satay::SatayCollector`] adapts any
//!   [satay](https://docs.rs/satay-runtime)-generated API client into a
//!   collector.
//! - [`Sink`]: receives records. Sinks compose like tower layers — a
//!   buffering tier ([`Buffered`]), a durable write-ahead spool
//!   ([`DiskSpool`]), fan-out ([`Tee`]), and terminal sinks such as
//!   [`HfSink`](sink::huggingface::HfSink) (feature `huggingface`) stack via
//!   [`SinkExt`].
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
#[cfg(feature = "satay")]
pub mod satay;
pub mod sink;

#[cfg(test)]
pub(crate) mod test_util;

pub use collector::Collector;
pub use layer::{Buffered, DiskSpool, FlushPolicy, SinkExt, SpoolError, Tee, TeeError};
pub use pipeline::Pipeline;
pub use runtime::{Meathook, MeathookBuilder, RuntimeError};
#[cfg(feature = "satay")]
pub use satay::SatayCollector;
pub use sink::{Sink, WindowMeta};

#[cfg(feature = "parquet")]
pub use encode::EncodeError;
#[cfg(feature = "huggingface")]
pub use sink::huggingface::{HfSink, HfSinkError};
