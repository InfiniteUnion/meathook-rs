//! [`DiskSpool`]: durable write-ahead tier with replay-on-restart.
//!
//! The **disk is the buffer**: `ingest` appends records as JSON lines to the
//! active segment file and fsyncs before returning, so once a tick's ingest
//! returns those records survive `SIGKILL`. `flush` reads segments back,
//! pushes them downstream, and deletes each segment only after the
//! downstream sink accepted it — a failed downstream leaves the segment in
//! place to be retried at the next firing.
//!
//! On-disk layout (one directory per pipeline):
//!
//! ```text
//! {spool_dir}/{pipeline}/{window_start_unix}.jsonl
//! ```
//!
//! Segment files are named by the start of their flush window (unix seconds,
//! aligned to the policy's `every`), so [`WindowMeta`] is reconstructed from
//! the filename alone — leftover segments from a crashed run replay with
//! their original window and land at the same storage path (idempotent).

use std::error;
use std::fs;
use std::io;
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde::de::DeserializeOwned;
use time::OffsetDateTime;
use tracing::{debug, info, warn};

use super::FlushPolicy;
use crate::sink::{Sink, WindowMeta};

/// Error from a [`DiskSpool`] layer.
#[derive(Debug, thiserror::Error)]
pub enum SpoolError<E>
where
    E: error::Error + Send + Sync + 'static,
{
    #[error("spool I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to serialize record for spooling: {0}")]
    Serialize(#[source] serde_json::Error),
    #[error("downstream sink error: {0}")]
    Downstream(#[source] E),
}

/// Durable write-ahead spool tier. See the [module docs](self) for the
/// on-disk protocol and crash-recovery semantics.
///
/// Construction is infallible and does no I/O; the spool directory is
/// created and leftover segments from a previous run are replayed
/// downstream on the first `ingest` or `flush` call.
///
/// File I/O uses synchronous `std::fs` calls: appends are a few kilobytes
/// plus an fsync, which is acceptable to block the runtime for at the
/// collection rates this crate targets.
pub struct DiskSpool<R, S> {
    pipeline: String,
    dir: PathBuf,
    policy: FlushPolicy,
    inner: S,
    initialized: bool,
    active_window: Option<i64>,
    active_count: usize,
    _record: PhantomData<fn() -> R>,
}

impl<R, S> DiskSpool<R, S> {
    /// Create a spool rooted at `dir`. The pipeline name used when
    /// reconstructing [`WindowMeta`] for replayed segments is the last
    /// component of `dir`.
    pub fn new(dir: impl Into<PathBuf>, policy: FlushPolicy, inner: S) -> Self {
        let dir = dir.into();
        let pipeline = dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "unknown".to_owned());
        Self {
            pipeline,
            dir,
            policy,
            inner,
            initialized: false,
            active_window: None,
            active_count: 0,
            _record: PhantomData,
        }
    }

    /// Override the pipeline name derived from the directory.
    pub fn with_pipeline_name(mut self, name: impl Into<String>) -> Self {
        self.pipeline = name.into();
        self
    }

    fn window_secs(&self) -> i64 {
        i64::try_from(self.policy.every.as_secs())
            .unwrap_or(i64::MAX)
            .max(1)
    }

    fn align(&self, unix: i64) -> i64 {
        unix - unix.rem_euclid(self.window_secs())
    }

    fn segment_path(&self, window_start: i64) -> PathBuf {
        self.dir.join(format!("{window_start}.jsonl"))
    }
}

impl<R, S> DiskSpool<R, S>
where
    R: Serialize + DeserializeOwned + Send + 'static,
    S: Sink<R>,
{
    fn io_err(path: &Path, source: io::Error) -> SpoolError<S::Error> {
        SpoolError::Io {
            path: path.to_owned(),
            source,
        }
    }

    /// Create the spool dir and replay anything left behind by a previous
    /// run. Replay failures are logged, not propagated: the segments stay
    /// on disk and are retried at the next firing.
    async fn ensure_init(&mut self) -> Result<(), SpoolError<S::Error>> {
        if self.initialized {
            return Ok(());
        }
        fs::create_dir_all(&self.dir).map_err(|e| Self::io_err(&self.dir, e))?;
        self.initialized = true;
        let leftovers = self.list_segments()?;
        if !leftovers.is_empty() {
            info!(
                pipeline = %self.pipeline,
                segments = leftovers.len(),
                "replaying spool segments left by a previous run"
            );
            if let Err(error) = self.drain(true).await {
                warn!(
                    pipeline = %self.pipeline,
                    %error,
                    "spool recovery failed; segments retained for retry"
                );
            }
        }
        Ok(())
    }

    /// All segment files in the spool dir, oldest first.
    fn list_segments(&self) -> Result<Vec<(i64, PathBuf)>, SpoolError<S::Error>> {
        let mut segments = vec![];
        let entries = fs::read_dir(&self.dir).map_err(|e| Self::io_err(&self.dir, e))?;
        for entry in entries {
            let entry = entry.map_err(|e| Self::io_err(&self.dir, e))?;
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "jsonl") {
                continue;
            }
            let Some(start) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<i64>().ok())
            else {
                warn!(path = %path.display(), "ignoring unrecognized file in spool dir");
                continue;
            };
            segments.push((start, path));
        }
        segments.sort_unstable_by_key(|(start, _)| *start);
        Ok(segments)
    }

    /// Append records to the active segment, fsyncing the file (and the
    /// directory when the segment is new) before returning.
    fn append(&mut self, records: &[R]) -> Result<(), SpoolError<S::Error>> {
        let window = self.align(OffsetDateTime::now_utc().unix_timestamp());
        let path = self.segment_path(window);

        let mut lines = vec![];
        for record in records {
            serde_json::to_writer(&mut lines, record).map_err(SpoolError::Serialize)?;
            lines.push(b'\n');
        }

        let is_new = !path.exists();
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| Self::io_err(&path, e))?;
        file.write_all(&lines).map_err(|e| Self::io_err(&path, e))?;
        file.sync_all().map_err(|e| Self::io_err(&path, e))?;
        if is_new {
            fs::File::open(&self.dir)
                .and_then(|d| d.sync_all())
                .map_err(|e| Self::io_err(&self.dir, e))?;
        }

        if self.active_window != Some(window) {
            self.active_window = Some(window);
            self.active_count = 0;
        }
        self.active_count += records.len();
        Ok(())
    }

    /// Read a segment, push it downstream, and delete it on success.
    async fn flush_segment(
        &mut self,
        window_start: i64,
        path: &Path,
    ) -> Result<(), SpoolError<S::Error>> {
        let contents = fs::read_to_string(path).map_err(|e| Self::io_err(path, e))?;
        let lines = contents
            .lines()
            .filter(|l| !l.is_empty())
            .collect::<Vec<&str>>();
        let mut records = Vec::with_capacity(lines.len());
        let last = lines.len().saturating_sub(1);
        for (i, line) in lines.iter().enumerate() {
            match serde_json::from_str::<R>(line) {
                Ok(record) => records.push(record),
                Err(error) if i == last => {
                    warn!(
                        path = %path.display(),
                        %error,
                        "skipping torn final line in spool segment (crash mid-append)"
                    );
                }
                Err(error) => {
                    warn!(
                        path = %path.display(),
                        line = i,
                        %error,
                        "skipping corrupt line in spool segment"
                    );
                }
            }
        }

        if !records.is_empty() {
            let start = OffsetDateTime::from_unix_timestamp(window_start)
                .unwrap_or(OffsetDateTime::UNIX_EPOCH);
            let meta = WindowMeta {
                pipeline: self.pipeline.clone(),
                start,
                end: start + time::Duration::seconds(self.window_secs()),
            };
            debug!(
                pipeline = %self.pipeline,
                window_start,
                records = records.len(),
                "flushing spool segment downstream"
            );
            self.inner
                .ingest(&meta, records)
                .await
                .map_err(SpoolError::Downstream)?;
        }

        fs::remove_file(path).map_err(|e| Self::io_err(path, e))?;
        if Some(window_start) == self.active_window {
            self.active_window = None;
            self.active_count = 0;
        }
        Ok(())
    }

    /// Flush segments downstream, oldest first. Failed segments are left in
    /// place and the remaining segments are still attempted; the first error
    /// is returned.
    async fn drain(&mut self, include_active: bool) -> Result<(), SpoolError<S::Error>> {
        let mut first_error = None;
        for (window_start, path) in self.list_segments()? {
            if !include_active && Some(window_start) == self.active_window {
                continue;
            }
            if let Err(error) = self.flush_segment(window_start, &path).await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        match first_error {
            None => Ok(()),
            Some(error) => Err(error),
        }
    }

    fn has_closed_segments(&self) -> Result<bool, SpoolError<S::Error>> {
        Ok(self
            .list_segments()?
            .iter()
            .any(|(start, _)| Some(*start) != self.active_window))
    }
}

impl<R, S> Sink<R> for DiskSpool<R, S>
where
    R: Serialize + DeserializeOwned + Send + 'static,
    S: Sink<R>,
{
    type Error = SpoolError<S::Error>;

    async fn ingest(&mut self, _meta: &WindowMeta, records: Vec<R>) -> Result<(), Self::Error> {
        self.ensure_init().await?;
        if !records.is_empty() {
            self.append(&records)?;
        }
        if self.active_count >= self.policy.max_records {
            self.drain(true).await
        } else if self.has_closed_segments()? {
            self.drain(false).await
        } else {
            Ok(())
        }
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        self.ensure_init().await?;
        self.drain(true).await?;
        self.inner.flush().await.map_err(SpoolError::Downstream)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::layer::SinkExt;
    use crate::test_util::{SharedSink, meta};

    fn policy() -> FlushPolicy {
        FlushPolicy::new(Duration::from_secs(3600), usize::MAX)
    }

    #[tokio::test]
    async fn ingest_is_write_ahead() {
        let dir = tempfile::tempdir().unwrap();
        let spool_dir = dir.path().join("p");
        let inner = SharedSink::new();
        let mut spool = inner.clone().spooled(&spool_dir, policy());

        spool.ingest(&meta("p"), vec![1, 2]).await.unwrap();
        spool.ingest(&meta("p"), vec![3]).await.unwrap();

        // Records are on disk before any flush fires.
        let files = fs::read_dir(&spool_dir).unwrap().collect::<Vec<_>>();
        assert_eq!(files.len(), 1);
        let contents = fs::read_to_string(files[0].as_ref().unwrap().path()).unwrap();
        assert_eq!(contents.lines().count(), 3);
        assert!(inner.batches().is_empty());
    }

    #[tokio::test]
    async fn replays_leftover_segments_on_first_use() {
        let dir = tempfile::tempdir().unwrap();
        let spool_dir = dir.path().join("weather");
        fs::create_dir_all(&spool_dir).unwrap();
        // Two leftover segments from a "previous run", an hour apart.
        fs::write(spool_dir.join("3600.jsonl"), "1\n2\n").unwrap();
        fs::write(spool_dir.join("7200.jsonl"), "3\n").unwrap();

        let inner = SharedSink::new();
        let mut spool: DiskSpool<i32, _> = inner.clone().spooled(&spool_dir, policy());
        spool.flush().await.unwrap();

        let batches = inner.batches();
        assert_eq!(batches.len(), 2);
        // Oldest first, meta reconstructed from the filename.
        assert_eq!(batches[0].0.start.unix_timestamp(), 3600);
        assert_eq!(batches[0].0.pipeline, "weather");
        assert_eq!(batches[0].1, vec![1, 2]);
        assert_eq!(batches[1].0.start.unix_timestamp(), 7200);
        assert_eq!(batches[1].1, vec![3]);
        // Segments are gone after a successful replay.
        assert_eq!(fs::read_dir(&spool_dir).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn tolerates_torn_final_line() {
        let dir = tempfile::tempdir().unwrap();
        let spool_dir = dir.path().join("p");
        fs::create_dir_all(&spool_dir).unwrap();
        fs::write(spool_dir.join("3600.jsonl"), "1\n2\n{\"trunc").unwrap();

        let inner = SharedSink::new();
        let mut spool: DiskSpool<i32, _> = inner.clone().spooled(&spool_dir, policy());
        spool.flush().await.unwrap();

        assert_eq!(inner.batches()[0].1, vec![1, 2]);
    }

    #[tokio::test]
    async fn retains_segments_across_failing_downstream() {
        let dir = tempfile::tempdir().unwrap();
        let spool_dir = dir.path().join("p");
        let inner = SharedSink::new();
        let mut spool = inner.clone().spooled(&spool_dir, policy());

        spool.ingest(&meta("p"), vec![1, 2]).await.unwrap();

        inner.set_fail(true);
        assert!(spool.flush().await.is_err());
        assert_eq!(fs::read_dir(&spool_dir).unwrap().count(), 1);
        assert!(inner.batches().is_empty());

        inner.set_fail(false);
        spool.flush().await.unwrap();
        assert_eq!(inner.batches()[0].1, vec![1, 2]);
        assert_eq!(fs::read_dir(&spool_dir).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn max_records_drains_active_segment() {
        let dir = tempfile::tempdir().unwrap();
        let spool_dir = dir.path().join("p");
        let inner = SharedSink::new();
        let mut spool = inner
            .clone()
            .spooled(&spool_dir, FlushPolicy::new(Duration::from_secs(3600), 3));

        spool.ingest(&meta("p"), vec![1, 2]).await.unwrap();
        assert!(inner.batches().is_empty());
        spool.ingest(&meta("p"), vec![3]).await.unwrap();

        assert_eq!(inner.batches()[0].1, vec![1, 2, 3]);
        assert_eq!(fs::read_dir(&spool_dir).unwrap().count(), 0);
    }
}
