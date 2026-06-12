//! Shared fakes for unit tests: a `Vec`-backed sink with a failure toggle.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

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
}

impl<R> Clone for SharedSink<R> {
    fn clone(&self) -> Self {
        Self {
            batches: Arc::clone(&self.batches),
            fail: Arc::clone(&self.fail),
            flushed: Arc::clone(&self.flushed),
        }
    }
}

impl<R> SharedSink<R> {
    pub fn new() -> Self {
        Self {
            batches: Arc::default(),
            fail: Arc::default(),
            flushed: Arc::default(),
        }
    }

    pub fn set_fail(&self, fail: bool) {
        self.fail.store(fail, Ordering::SeqCst);
    }

    pub fn flushed(&self) -> bool {
        self.flushed.load(Ordering::SeqCst)
    }
}

impl<R: Clone> SharedSink<R> {
    pub fn batches(&self) -> Vec<(WindowMeta, Vec<R>)> {
        self.batches.lock().unwrap().clone()
    }

    pub fn records(&self) -> Vec<R> {
        self.batches()
            .into_iter()
            .flat_map(|(_, records)| records)
            .collect()
    }
}

impl<R: Send + 'static> Sink<R> for SharedSink<R> {
    type Error = TestSinkFailure;

    async fn ingest(&mut self, meta: &WindowMeta, records: Vec<R>) -> Result<(), Self::Error> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(TestSinkFailure);
        }
        self.batches.lock().unwrap().push((meta.clone(), records));
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(TestSinkFailure);
        }
        self.flushed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

/// A `WindowMeta` for tests.
pub fn meta(pipeline: &str) -> WindowMeta {
    let now = OffsetDateTime::now_utc();
    WindowMeta {
        pipeline: pipeline.to_owned(),
        start: now,
        end: now,
    }
}
