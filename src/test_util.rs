//! Shared fakes for unit tests: a `Vec`-backed sink with a failure toggle.

use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};

use time::OffsetDateTime;

use crate::sink::{Sink, WindowMeta};

#[derive(Debug, thiserror::Error)]
#[error("test sink failure")]
pub struct TestSinkFailure;

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
