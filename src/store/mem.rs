//! [`MemStore`]: in-memory window storage, the volatile tier backend.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::ops::Bound;

use super::{Segment, Store};

/// In-memory store. Nothing survives a crash. If its [`Tier`](crate::Tier)
/// wraps a durable tier, records remain volatile until this tier's policy
/// forwards them downstream.
///
/// When every successful top-level ingest must remain crash-durable until
/// terminal delivery, use a [`JsonlStore`](super::JsonlStore) tier directly
/// around the durable or terminal sink, with no `MemStore` before or after it.
///
/// Requires `R: Clone`: [`Sink::ingest`](crate::Sink::ingest) consumes its
/// `Vec<R>`, so the segment hands downstream a clone and the original stays
/// in the map until [`Segment::commit`] — a failed downstream ingest loses
/// nothing. (Memory has no backing copy to re-read, unlike a durable
/// store.) Dropping the `Clone` bound would require `Sink::ingest` to
/// borrow its records instead.
#[derive(Debug)]
pub struct MemStore<R> {
    /// Records keyed by window start (unix seconds), ordered oldest-first.
    windows: BTreeMap<i64, Vec<R>>,
}

impl<R> MemStore<R> {
    /// Create an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            windows: BTreeMap::new(),
        }
    }
}

impl<R> Default for MemStore<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R> Store<R> for MemStore<R>
where
    R: Clone + Send + 'static,
{
    type Error = Infallible;
    type Segment<'a>
        = MemSegment<'a, R>
    where
        Self: 'a;

    async fn append(&mut self, window: i64, records: Vec<R>) -> Result<(), Infallible> {
        self.windows.entry(window).or_default().extend(records);
        Ok(())
    }

    async fn oldest(
        &mut self,
        after: Option<i64>,
    ) -> Result<Option<MemSegment<'_, R>>, Infallible> {
        let lower = after.map_or(Bound::Unbounded, Bound::Excluded);
        let Some(window) = self
            .windows
            .range((lower, Bound::Unbounded))
            .next()
            .map(|(window, _)| *window)
        else {
            return Ok(None);
        };
        Ok(Some(MemSegment {
            store: self,
            window,
        }))
    }
}

/// Oldest window checked out of a [`MemStore`].
pub struct MemSegment<'a, R> {
    /// The borrowed store: the window's records stay in its map until
    /// [`Segment::commit`] removes them.
    store: &'a mut MemStore<R>,
    /// Window start (unix seconds) this segment was checked out for.
    window: i64,
}

impl<R> Segment<R> for MemSegment<'_, R>
where
    R: Clone + Send + 'static,
{
    type Error = Infallible;

    fn window(&self) -> i64 {
        self.window
    }

    async fn records(&mut self) -> Result<Vec<R>, Infallible> {
        Ok(self
            .store
            .windows
            .get(&self.window)
            .cloned()
            .unwrap_or_default())
    }

    async fn commit(self) -> Result<(), Infallible> {
        self.store.windows.remove(&self.window);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn oldest_returns_windows_in_order() {
        let mut store = MemStore::new();
        store.append(200, vec![3]).await.unwrap();
        store.append(100, vec![1, 2]).await.unwrap();

        let seg = store.oldest(None).await.unwrap().unwrap();
        assert_eq!(seg.window(), 100);
        seg.commit().await.unwrap();

        let seg = store.oldest(None).await.unwrap().unwrap();
        assert_eq!(seg.window(), 200);
    }

    #[tokio::test]
    async fn records_leave_store_intact_until_commit() {
        let mut store = MemStore::new();
        store.append(100, vec![1, 2]).await.unwrap();

        // Reading and dropping the segment uncommitted retains the data.
        {
            let mut seg = store.oldest(None).await.unwrap().unwrap();
            assert_eq!(seg.records().await.unwrap(), vec![1, 2]);
        }

        let mut seg = store.oldest(None).await.unwrap().unwrap();
        assert_eq!(seg.records().await.unwrap(), vec![1, 2]);
        seg.commit().await.unwrap();

        assert!(store.oldest(None).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn append_extends_an_existing_window() {
        let mut store = MemStore::new();
        store.append(100, vec![1]).await.unwrap();
        store.append(100, vec![2]).await.unwrap();

        let mut seg = store.oldest(None).await.unwrap().unwrap();
        assert_eq!(seg.records().await.unwrap(), vec![1, 2]);
    }

    #[tokio::test]
    async fn oldest_after_skips_windows_at_or_below_cursor() {
        let mut store = MemStore::new();
        store.append(100, vec![1]).await.unwrap();
        store.append(200, vec![2]).await.unwrap();

        let seg = store.oldest(Some(100)).await.unwrap().unwrap();
        assert_eq!(seg.window(), 200);
        assert!(store.oldest(Some(200)).await.unwrap().is_none());

        // Skipped windows are untouched: the plain oldest is still 100.
        let seg = store.oldest(None).await.unwrap().unwrap();
        assert_eq!(seg.window(), 100);
    }
}
