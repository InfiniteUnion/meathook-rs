//! [`JsonlStore`]: durable write-ahead segment files (JSON lines).
//!
//! The **disk is the buffer**: `append` writes records as JSON lines to the
//! window's segment file and fsyncs before returning. Once an ingest reaches
//! the JSONL-backed [`Tier`](crate::Tier) and returns, those records survive
//! `SIGKILL`; records still held by an outer tier have not reached disk.
//! `commit` deletes the segment only after the downstream sink accepts it, so
//! a failed downstream leaves the segment in place for the next firing. If a
//! volatile downstream tier accepts the records, durable custody ends before
//! terminal delivery.
//!
//! On-disk layout (one directory per pipeline):
//!
//! ```text
//! {dir}/{window_start_unix}.jsonl
//! ```
//!
//! Segment files are named by the start of their flush window (unix
//! seconds, aligned by the tier), so windows are reconstructed from the
//! filename alone — leftover segments from a crashed run replay with their
//! original window and land at the same storage path (idempotent). Torn
//! final lines (crash mid-append) and corrupt lines are skipped with a
//! warning on read.

use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::warn;

use super::{Segment, Store};

/// Error from a [`JsonlStore`].
#[derive(Debug, thiserror::Error)]
pub enum JsonlStoreError {
    /// Reading or writing a segment file (or the store directory) failed.
    #[error("store I/O error at {path}: {source}")]
    Io {
        /// The file or directory the operation failed on.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },
    /// A record could not be serialized to a JSON line.
    #[error("failed to serialize record for spooling: {0}")]
    Serialize(#[source] serde_json::Error),
}

fn io_err(path: &Path, source: io::Error) -> JsonlStoreError {
    JsonlStoreError::Io {
        path: path.to_owned(),
        source,
    }
}

/// Durable write-ahead store rooted at one directory (one per pipeline).
/// See the [module docs](self) for the on-disk protocol.
///
/// Construction is infallible and does no I/O; the directory is created on
/// first use. The [`Store::pipeline_hint`] is the last component of `dir`
/// (override with [`with_pipeline_name`](Self::with_pipeline_name)), so
/// point each pipeline at `spool_root.join(pipeline_name)`.
///
/// File I/O uses synchronous `std::fs` calls: appends are a few kilobytes
/// plus an fsync, which is acceptable to block the runtime for at the
/// collection rates this crate targets.
pub struct JsonlStore<R> {
    /// Directory holding this store's segment files, one per window.
    dir: PathBuf,
    /// Hint reported via [`Store::pipeline_hint`]: the last component of
    /// `dir` unless overridden with
    /// [`with_pipeline_name`](Self::with_pipeline_name).
    pipeline: String,
    /// Whether `dir` has been created; construction does no I/O, so this
    /// happens lazily on first use.
    initialized: bool,
    /// Ties the store to one record type without owning any (`fn() -> R`
    /// keeps `Send`/`Sync` independent of `R`).
    _record: PhantomData<fn() -> R>,
}

impl<R> JsonlStore<R> {
    /// Create a store rooted at `dir`. The pipeline hint is derived from
    /// the last component of `dir`.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        let pipeline = dir.file_name().map_or_else(
            || "unknown".to_owned(),
            |n| n.to_string_lossy().into_owned(),
        );
        Self {
            dir,
            pipeline,
            initialized: false,
            _record: PhantomData,
        }
    }

    /// Override the pipeline hint derived from the directory name.
    #[must_use]
    pub fn with_pipeline_name(mut self, name: impl Into<String>) -> Self {
        self.pipeline = name.into();
        self
    }

    fn ensure_dir(&mut self) -> Result<(), JsonlStoreError> {
        if !self.initialized {
            fs::create_dir_all(&self.dir).map_err(|e| io_err(&self.dir, e))?;
            self.initialized = true;
        }
        Ok(())
    }

    fn segment_path(&self, window: i64) -> PathBuf {
        self.dir.join(format!("{window}.jsonl"))
    }

    /// All segment files in the store directory, oldest first.
    fn list_segments(&self) -> Result<Vec<(i64, PathBuf)>, JsonlStoreError> {
        let mut segments = vec![];
        let entries = fs::read_dir(&self.dir).map_err(|e| io_err(&self.dir, e))?;
        for entry in entries {
            let entry = entry.map_err(|e| io_err(&self.dir, e))?;
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "jsonl") {
                continue;
            }
            let Some(start) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<i64>().ok())
            else {
                warn!(path = %path.display(), "ignoring unrecognized file in store dir");
                continue;
            };
            segments.push((start, path));
        }
        segments.sort_unstable_by_key(|(start, _)| *start);
        Ok(segments)
    }
}

impl<R> Store<R> for JsonlStore<R>
where
    R: Serialize + DeserializeOwned + Send + 'static,
{
    type Error = JsonlStoreError;
    type Segment<'a>
        = JsonlSegment<R>
    where
        Self: 'a;

    /// Append records to the window's segment file, fsyncing the file (and
    /// the directory when the segment is new) before returning.
    fn append(
        &mut self,
        window: i64,
        records: Vec<R>,
    ) -> impl Future<Output = Result<(), JsonlStoreError>> + Send {
        let result = (|| {
            self.ensure_dir()?;
            let path = self.segment_path(window);

            let mut lines = vec![];
            for record in &records {
                serde_json::to_writer(&mut lines, record).map_err(JsonlStoreError::Serialize)?;
                lines.push(b'\n');
            }

            let is_new = !path.exists();
            let mut file = fs::OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&path)
                .map_err(|e| io_err(&path, e))?;
            // A crash may leave a partial final JSON value without its
            // newline. Separate it from newly appended records so the torn
            // value is skipped on replay without swallowing the first new
            // record into the same invalid line.
            if file.metadata().map_err(|e| io_err(&path, e))?.len() > 0 {
                file.seek(SeekFrom::End(-1)).map_err(|e| io_err(&path, e))?;
                let mut last = [0];
                file.read_exact(&mut last).map_err(|e| io_err(&path, e))?;
                if last[0] != b'\n' {
                    file.write_all(b"\n").map_err(|e| io_err(&path, e))?;
                }
            }
            file.write_all(&lines).map_err(|e| io_err(&path, e))?;
            file.sync_all().map_err(|e| io_err(&path, e))?;
            if is_new {
                fs::File::open(&self.dir)
                    .and_then(|d| d.sync_all())
                    .map_err(|e| io_err(&self.dir, e))?;
            }
            Ok(())
        })();
        std::future::ready(result)
    }

    fn oldest(
        &mut self,
        after: Option<i64>,
    ) -> impl Future<Output = Result<Option<JsonlSegment<R>>, JsonlStoreError>> + Send {
        let result = (|| {
            self.ensure_dir()?;
            Ok(self
                .list_segments()?
                .into_iter()
                .find(|(window, _)| after.is_none_or(|a| *window > a))
                .map(|(window, path)| JsonlSegment {
                    window,
                    path,
                    _record: PhantomData,
                }))
        })();
        std::future::ready(result)
    }

    fn pipeline_hint(&self) -> Option<&str> {
        Some(&self.pipeline)
    }
}

/// Oldest segment file checked out of a [`JsonlStore`]. Holds only the
/// path; the file itself is the retained copy until [`Segment::commit`]
/// deletes it.
pub struct JsonlSegment<R> {
    /// Window start (unix seconds), parsed from the segment filename.
    window: i64,
    /// The segment file: reads re-open it, [`Segment::commit`] deletes it.
    path: PathBuf,
    /// Ties the segment to its store's record type.
    _record: PhantomData<fn() -> R>,
}

impl<R> Segment<R> for JsonlSegment<R>
where
    R: DeserializeOwned + Send + 'static,
{
    type Error = JsonlStoreError;

    fn window(&self) -> i64 {
        self.window
    }

    fn records(&mut self) -> impl Future<Output = Result<Vec<R>, JsonlStoreError>> + Send {
        let result = (|| {
            let contents = fs::read_to_string(&self.path).map_err(|e| io_err(&self.path, e))?;
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
                            path = %self.path.display(),
                            %error,
                            "skipping torn final line in store segment (crash mid-append)"
                        );
                    }
                    Err(error) => {
                        warn!(
                            path = %self.path.display(),
                            line = i,
                            %error,
                            "skipping corrupt line in store segment"
                        );
                    }
                }
            }
            Ok(records)
        })();
        std::future::ready(result)
    }

    fn commit(self) -> impl Future<Output = Result<(), JsonlStoreError>> + Send {
        std::future::ready(fs::remove_file(&self.path).map_err(|e| io_err(&self.path, e)))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::layer::{FlushPolicy, Tier};
    use crate::sink::Sink;
    use crate::test_util::{SharedSink, meta_at};
    use time::OffsetDateTime;

    fn policy() -> FlushPolicy {
        FlushPolicy::new(Duration::from_secs(3600), usize::MAX)
    }

    #[tokio::test]
    async fn tier_ingest_is_write_ahead() {
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("p");
        let inner = SharedSink::new();
        let mut tier = Tier::new(JsonlStore::new(&store_dir), policy(), inner.clone());

        tier.ingest(&meta_at("p", 10), vec![1, 2]).await.unwrap();
        tier.ingest(&meta_at("p", 20), vec![3]).await.unwrap();

        // Records are on disk before any flush fires.
        let files = fs::read_dir(&store_dir).unwrap().collect::<Vec<_>>();
        assert_eq!(files.len(), 1);
        let contents = fs::read_to_string(files[0].as_ref().unwrap().path()).unwrap();
        assert_eq!(contents.lines().count(), 3);
        assert!(inner.batches().is_empty());
    }

    #[tokio::test]
    async fn replays_leftover_segments_on_first_use() {
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("weather");
        fs::create_dir_all(&store_dir).unwrap();
        // Two leftover segments from a "previous run", an hour apart.
        fs::write(store_dir.join("3600.jsonl"), "1\n2\n").unwrap();
        fs::write(store_dir.join("7200.jsonl"), "3\n").unwrap();

        let inner = SharedSink::new();
        let mut tier = Tier::new(JsonlStore::<i32>::new(&store_dir), policy(), inner.clone());
        tier.flush().await.unwrap();

        let batches = inner.batches();
        assert_eq!(batches.len(), 2);
        // Oldest first, meta reconstructed from the filename; the pipeline
        // name comes from the store's hint (no live meta seen yet).
        assert_eq!(batches[0].0.start.unix_timestamp(), 3600);
        assert_eq!(batches[0].0.pipeline, "weather");
        assert_eq!(batches[0].1, vec![1, 2]);
        assert_eq!(batches[1].0.start.unix_timestamp(), 7200);
        assert_eq!(batches[1].1, vec![3]);
        // Segments are gone after a successful replay.
        assert_eq!(fs::read_dir(&store_dir).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn restart_within_window_continues_the_same_segment() {
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("weather");
        let inner = SharedSink::new();

        {
            let mut first = Tier::new(JsonlStore::new(&store_dir), policy(), inner.clone());
            first
                .ingest(&meta_at("weather", 44_400), vec![1, 2])
                .await
                .unwrap();
            first
                .advance(OffsetDateTime::from_unix_timestamp(45_300).unwrap())
                .await
                .unwrap();
        }

        assert_eq!(
            fs::read_to_string(store_dir.join("43200.jsonl")).unwrap(),
            "1\n2\n"
        );
        assert!(inner.batches().is_empty());

        let mut restarted = Tier::new(JsonlStore::new(&store_dir), policy(), inner.clone());
        restarted
            .advance(OffsetDateTime::from_unix_timestamp(45_600).unwrap())
            .await
            .unwrap();
        restarted
            .ingest(&meta_at("weather", 45_600), vec![3])
            .await
            .unwrap();

        assert_eq!(
            fs::read_to_string(store_dir.join("43200.jsonl")).unwrap(),
            "1\n2\n3\n"
        );
        assert!(inner.batches().is_empty());

        restarted
            .advance(OffsetDateTime::from_unix_timestamp(46_800).unwrap())
            .await
            .unwrap();
        let batches = inner.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].0.start.unix_timestamp(), 43_200);
        assert_eq!(batches[0].1, vec![1, 2, 3]);
        assert!(!store_dir.join("43200.jsonl").exists());
    }

    #[tokio::test]
    async fn restart_continuation_separates_a_torn_final_line() {
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("weather");
        fs::create_dir_all(&store_dir).unwrap();
        fs::write(store_dir.join("43200.jsonl"), "1\n{\"torn").unwrap();

        let inner = SharedSink::new();
        let mut restarted = Tier::new(JsonlStore::new(&store_dir), policy(), inner.clone());
        restarted
            .advance(OffsetDateTime::from_unix_timestamp(45_600).unwrap())
            .await
            .unwrap();
        restarted
            .ingest(&meta_at("weather", 45_600), vec![2])
            .await
            .unwrap();
        restarted
            .advance(OffsetDateTime::from_unix_timestamp(46_800).unwrap())
            .await
            .unwrap();

        assert_eq!(inner.batches()[0].1, vec![1, 2]);
    }

    #[tokio::test]
    async fn restart_after_boundary_replays_only_closed_segments() {
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("weather");
        fs::create_dir_all(&store_dir).unwrap();
        fs::write(store_dir.join("43200.jsonl"), "1\n").unwrap();
        fs::write(store_dir.join("46800.jsonl"), "2\n").unwrap();
        fs::write(store_dir.join("50400.jsonl"), "3\n").unwrap();

        let inner = SharedSink::new();
        let mut restarted = Tier::new(JsonlStore::<i32>::new(&store_dir), policy(), inner.clone());
        restarted
            .advance(OffsetDateTime::from_unix_timestamp(46_800).unwrap())
            .await
            .unwrap();

        let batches = inner.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].0.start.unix_timestamp(), 43_200);
        assert_eq!(batches[0].1, vec![1]);
        assert!(!store_dir.join("43200.jsonl").exists());
        assert!(store_dir.join("46800.jsonl").exists());
        assert!(store_dir.join("50400.jsonl").exists());
    }

    #[tokio::test]
    async fn restart_restores_active_count_for_max_records() {
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("p");
        let inner = SharedSink::new();
        let capped = FlushPolicy::new(Duration::from_secs(3600), 3);

        {
            let mut first = Tier::new(JsonlStore::new(&store_dir), capped, inner.clone());
            first
                .ingest(&meta_at("p", 44_400), vec![1, 2])
                .await
                .unwrap();
        }

        let mut restarted = Tier::new(JsonlStore::new(&store_dir), capped, inner.clone());
        restarted
            .advance(OffsetDateTime::from_unix_timestamp(45_600).unwrap())
            .await
            .unwrap();
        restarted
            .ingest(&meta_at("p", 45_600), vec![3])
            .await
            .unwrap();

        assert_eq!(inner.batches()[0].1, vec![1, 2, 3]);
        assert_eq!(fs::read_dir(&store_dir).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn failed_wall_advance_retains_segment_for_replay() {
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("p");
        fs::create_dir_all(&store_dir).unwrap();
        fs::write(store_dir.join("43200.jsonl"), "1\n2\n").unwrap();

        let inner = SharedSink::new();
        inner.set_fail(true);
        let mut tier = Tier::new(JsonlStore::<i32>::new(&store_dir), policy(), inner.clone());
        assert!(
            tier.advance(OffsetDateTime::from_unix_timestamp(46_800).unwrap())
                .await
                .is_err()
        );
        assert!(store_dir.join("43200.jsonl").exists());

        inner.set_fail(false);
        tier.advance(OffsetDateTime::from_unix_timestamp(46_900).unwrap())
            .await
            .unwrap();
        assert_eq!(inner.batches()[0].1, vec![1, 2]);
        assert!(!store_dir.join("43200.jsonl").exists());
    }

    #[tokio::test]
    async fn tier_flush_tolerates_torn_final_line() {
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("p");
        fs::create_dir_all(&store_dir).unwrap();
        fs::write(store_dir.join("3600.jsonl"), "1\n2\n{\"trunc").unwrap();

        let inner = SharedSink::new();
        let mut tier = Tier::new(JsonlStore::<i32>::new(&store_dir), policy(), inner.clone());
        tier.flush().await.unwrap();

        assert_eq!(inner.batches()[0].1, vec![1, 2]);
        assert_eq!(fs::read_dir(&store_dir).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn tier_retains_segments_across_failing_downstream() {
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("p");
        let inner = SharedSink::new();
        let mut tier = Tier::new(JsonlStore::new(&store_dir), policy(), inner.clone());

        tier.ingest(&meta_at("p", 10), vec![1, 2]).await.unwrap();

        inner.set_fail(true);
        assert!(tier.flush().await.is_err());
        assert_eq!(fs::read_dir(&store_dir).unwrap().count(), 1);
        assert!(inner.batches().is_empty());

        inner.set_fail(false);
        tier.flush().await.unwrap();
        assert_eq!(inner.batches()[0].1, vec![1, 2]);
        assert_eq!(fs::read_dir(&store_dir).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn tier_max_records_drains_active_segment() {
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("p");
        let inner = SharedSink::new();
        let mut tier = Tier::new(
            JsonlStore::new(&store_dir),
            FlushPolicy::new(Duration::from_secs(3600), 3),
            inner.clone(),
        );

        tier.ingest(&meta_at("p", 10), vec![1, 2]).await.unwrap();
        assert!(inner.batches().is_empty());
        tier.ingest(&meta_at("p", 20), vec![3]).await.unwrap();

        assert_eq!(inner.batches()[0].1, vec![1, 2, 3]);
        assert_eq!(fs::read_dir(&store_dir).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn append_is_write_ahead_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("p");
        let mut store = JsonlStore::new(&store_dir);

        store.append(0, vec![1, 2]).await.unwrap();
        store.append(0, vec![3]).await.unwrap();

        let contents = fs::read_to_string(store_dir.join("0.jsonl")).unwrap();
        assert_eq!(contents, "1\n2\n3\n");
    }

    #[tokio::test]
    async fn oldest_is_oldest_window_and_commit_deletes() {
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("p");
        let mut store = JsonlStore::new(&store_dir);
        store.append(7200, vec![3]).await.unwrap();
        store.append(3600, vec![1, 2]).await.unwrap();

        let mut seg = store.oldest(None).await.unwrap().unwrap();
        assert_eq!(seg.window(), 3600);
        assert_eq!(seg.records().await.unwrap(), vec![1, 2]);
        seg.commit().await.unwrap();
        assert!(!store_dir.join("3600.jsonl").exists());

        let seg = store.oldest(None).await.unwrap().unwrap();
        assert_eq!(seg.window(), 7200);
    }

    #[tokio::test]
    async fn oldest_after_skips_windows_at_or_below_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("p");
        let mut store = JsonlStore::new(&store_dir);
        store.append(3600, vec![1]).await.unwrap();
        store.append(7200, vec![2]).await.unwrap();

        let seg = store.oldest(Some(3600)).await.unwrap().unwrap();
        assert_eq!(seg.window(), 7200);
        assert!(store.oldest(Some(7200)).await.unwrap().is_none());

        // Skipped segment files are untouched.
        assert!(store_dir.join("3600.jsonl").exists());
    }

    #[tokio::test]
    async fn ignores_non_jsonl_files() {
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("p");
        fs::create_dir_all(&store_dir).unwrap();
        fs::write(store_dir.join("notes.txt"), "hi").unwrap();
        fs::write(store_dir.join("weird.jsonl"), "1\n").unwrap();
        fs::write(store_dir.join("100.jsonl"), "1\n").unwrap();

        let mut store: JsonlStore<i32> = JsonlStore::new(&store_dir);
        let seg = store.oldest(None).await.unwrap().unwrap();
        assert_eq!(seg.window(), 100);
    }

    #[tokio::test]
    async fn segment_tolerates_torn_final_line() {
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("p");
        fs::create_dir_all(&store_dir).unwrap();
        fs::write(store_dir.join("3600.jsonl"), "1\n2\n{\"trunc").unwrap();

        let mut store: JsonlStore<i32> = JsonlStore::new(&store_dir);
        let mut seg = store.oldest(None).await.unwrap().unwrap();
        assert_eq!(seg.records().await.unwrap(), vec![1, 2]);
    }

    #[test]
    fn pipeline_hint_is_dir_derived_and_overridable() {
        let store: JsonlStore<i32> = JsonlStore::new("/var/spool/weather");
        assert_eq!(Store::<i32>::pipeline_hint(&store), Some("weather"));

        let store = store.with_pipeline_name("rain");
        assert_eq!(Store::<i32>::pipeline_hint(&store), Some("rain"));
    }
}
