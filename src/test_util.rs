//! Shared fakes for unit tests: a `Vec`-backed sink with a failure toggle.

use parking_lot::Mutex;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};

use time::OffsetDateTime;

use crate::encode::{Encoder, JsonEncoder};
use crate::sink::{Sink, WindowMeta, object_path};

#[derive(Debug, thiserror::Error)]
#[error("test sink failure")]
pub struct TestSinkFailure;

#[derive(Debug, thiserror::Error)]
pub enum FakeObjectError {
    #[error("failed to encode test object: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("test object was written but its acknowledgement was lost")]
    AcknowledgementLost,
}

/// Deterministic object-storage sink for tests.
///
/// Objects are keyed by the same window-and-content path as remote sinks.
/// `fail_after_put_once` models a remote write whose acknowledgement is lost:
/// the object remains visible while the upstream tier retains its segment.
#[derive(Clone, Default)]
pub struct FakeObjectSink {
    objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    attempts: Arc<Mutex<Vec<String>>>,
    fail_after_put: Arc<AtomicBool>,
}

impl FakeObjectSink {
    pub fn fail_after_put_once(&self) {
        self.fail_after_put.store(true, Ordering::SeqCst);
    }

    #[must_use]
    pub fn objects(&self) -> BTreeMap<String, Vec<u8>> {
        self.objects.lock().clone()
    }

    #[must_use]
    pub fn attempts(&self) -> Vec<String> {
        self.attempts.lock().clone()
    }
}

impl<R> Sink<R> for FakeObjectSink
where
    R: Serialize + DeserializeOwned + Send + 'static,
{
    type Error = FakeObjectError;

    fn ingest(
        &mut self,
        meta: &WindowMeta,
        records: Vec<R>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let result = (|| {
            if records.is_empty() {
                return Ok(());
            }

            let content = JsonEncoder.encode(&records)?;
            let path = object_path(meta, &content, JsonEncoder::EXT);
            self.attempts.lock().push(path.clone());
            self.objects.lock().insert(path, content);

            if self.fail_after_put.swap(false, Ordering::SeqCst) {
                return Err(FakeObjectError::AcknowledgementLost);
            }
            Ok(())
        })();
        std::future::ready(result)
    }

    fn flush(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }
}

type Batches<R> = Arc<Mutex<Vec<(WindowMeta, Vec<R>)>>>;

/// Clonable terminal sink recording every batch it accepts.
pub struct SharedSink<R = i32> {
    batches: Batches<R>,
    fail: Arc<AtomicBool>,
    flushed: Arc<AtomicBool>,
    advances: Arc<AtomicUsize>,
}

impl<R> Clone for SharedSink<R> {
    fn clone(&self) -> Self {
        Self {
            batches: Arc::clone(&self.batches),
            fail: Arc::clone(&self.fail),
            flushed: Arc::clone(&self.flushed),
            advances: Arc::clone(&self.advances),
        }
    }
}

impl<R> SharedSink<R> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            batches: Arc::default(),
            fail: Arc::default(),
            flushed: Arc::default(),
            advances: Arc::default(),
        }
    }

    pub fn set_fail(&self, fail: bool) {
        self.fail.store(fail, Ordering::SeqCst);
    }

    #[must_use]
    pub fn flushed(&self) -> bool {
        self.flushed.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn advances(&self) -> usize {
        self.advances.load(Ordering::SeqCst)
    }
}

impl<R: Clone> SharedSink<R> {
    #[must_use]
    pub fn batches(&self) -> Vec<(WindowMeta, Vec<R>)> {
        self.batches.lock().clone()
    }

    #[must_use]
    pub fn records(&self) -> Vec<R> {
        self.batches()
            .into_iter()
            .flat_map(|(_, records)| records)
            .collect()
    }
}

impl<R: Send + 'static> Sink<R> for SharedSink<R> {
    type Error = TestSinkFailure;

    fn ingest(
        &mut self,
        meta: &WindowMeta,
        records: Vec<R>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let result = if self.fail.load(Ordering::SeqCst) {
            Err(TestSinkFailure)
        } else {
            self.batches.lock().push((meta.clone(), records));
            Ok(())
        };
        std::future::ready(result)
    }

    fn flush(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let result = if self.fail.load(Ordering::SeqCst) {
            Err(TestSinkFailure)
        } else {
            self.flushed.store(true, Ordering::SeqCst);
            Ok(())
        };
        std::future::ready(result)
    }

    fn advance(
        &mut self,
        _now: OffsetDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let result = if self.fail.load(Ordering::SeqCst) {
            Err(TestSinkFailure)
        } else {
            self.advances.fetch_add(1, Ordering::SeqCst);
            Ok(())
        };
        std::future::ready(result)
    }
}

/// A `WindowMeta` for tests.
#[must_use]
pub fn meta(pipeline: &str) -> WindowMeta {
    let now = OffsetDateTime::now_utc();
    WindowMeta {
        pipeline: pipeline.to_owned(),
        start: now,
        end: now,
    }
}

/// A `WindowMeta` whose start/end are the given unix timestamp — lets tests
/// steer a tier's window alignment deterministically.
#[must_use]
pub fn meta_at(pipeline: &str, unix: i64) -> WindowMeta {
    let t = OffsetDateTime::from_unix_timestamp(unix).unwrap();
    WindowMeta {
        pipeline: pipeline.to_owned(),
        start: t,
        end: t,
    }
}
