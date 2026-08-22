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
//!   feature, `satay::SatayCollector` adapts any
//!   [satay](https://docs.rs/satay-runtime)-generated API client into a
//!   collector.
//! - [`Sink`]: receives records. [`SinkStack`] composes any number of
//!   buffering tiers ([`Tier`]), each backed by a pluggable [`Store`]:
//!   in-memory [`MemStore`], durable write-ahead [`JsonlStore`], or your own.
//!   Finish the stack with a terminal sink such as `HfSink` (feature
//!   `huggingface`) or `HfBucketSink` for Hugging Face storage buckets
//!   (feature `hf-bucket`); fan out completed sinks with
//!   [`SinkExt::tee`].
//!
//! Pipelines (collector + sink stack) are supervised by the [`Meathook`]
//! runtime: one tokio task each, respawn-on-panic with backoff, and a final
//! `flush()` through the whole stack on graceful shutdown.

pub mod collector;
pub mod encode;
pub mod layer;
pub mod pipeline;
pub mod runtime;
#[cfg(feature = "satay")]
pub mod satay;
pub mod sink;
pub mod store;

#[cfg(test)]
pub(crate) mod test_util;

pub use collector::Collector;
pub use layer::{FlushPolicy, SinkExt, SinkStack, Tee, TeeError, Tier, TierError};
pub use pipeline::Pipeline;
pub use runtime::{Meathook, MeathookBuilder, RuntimeError};
#[cfg(feature = "satay")]
pub use satay::SatayCollector;
pub use sink::{Sink, WindowMeta};
pub use store::{JsonlStore, JsonlStoreError, MemStore, Segment, Store};

#[cfg(feature = "csv")]
pub use encode::{CsvEncoder, CsvError};
pub use encode::{Encoder, JsonEncoder};
#[cfg(feature = "parquet")]
pub use encode::{ParquetCompression, ParquetEncodeError, ParquetEncoder, Uncompressed, Zstd};
#[cfg(feature = "hf-bucket")]
pub use sink::hf_bucket::{
    BatchAction, BatchOutcome, CreateBucketAction, CreateBucketOutcome, HfBucketSink,
    HfBucketSinkError,
};
#[cfg(feature = "huggingface")]
pub use sink::huggingface::{CommitGate, HfSink, HfSinkError};
