//! Top-down construction of concrete sink stacks.

use super::{FlushPolicy, Tier};
use crate::sink::Sink;
use crate::store::Store;

/// Builds a sink stack in record-entry order.
///
/// The first [`tier`](Self::tier) call becomes the outermost tier and receives
/// records first. Each later tier receives records only when the tier before
/// it forwards them; [`terminal`](Self::terminal) finishes the stack with its
/// final sink.
///
/// The builder introduces no boxing, dynamic dispatch, runtime collection, or
/// store calls. Finishing it produces the existing nested [`Tier`] types.
///
/// ```
/// use std::convert::Infallible;
/// use std::time::Duration;
///
/// use meathook::{
///     FlushPolicy, JsonlStore, MemStore, Sink, SinkStack, WindowMeta,
/// };
///
/// struct Terminal;
///
/// impl Sink<i32> for Terminal {
///     type Error = Infallible;
///
///     async fn ingest(
///         &mut self,
///         _: &WindowMeta,
///         _: Vec<i32>,
///     ) -> Result<(), Self::Error> {
///         Ok(())
///     }
///
///     async fn flush(&mut self) -> Result<(), Self::Error> {
///         Ok(())
///     }
/// }
///
/// let sink = SinkStack::new()
///     .tier(
///         MemStore::new(),
///         FlushPolicy::new(Duration::from_secs(300), 10_000),
///     )
///     .tier(JsonlStore::new("spool"), FlushPolicy::hourly())
///     .terminal(Terminal);
///
/// // Concrete type, inferred without boxing or `dyn Sink`.
/// let _: meathook::Tier<
///     i32,
///     MemStore<i32>,
///     meathook::Tier<i32, JsonlStore<i32>, Terminal>,
/// > = sink;
/// ```
#[must_use = "a SinkStack does nothing until terminal() finishes it"]
pub struct SinkStack<L = ()> {
    layers: L,
}

impl SinkStack<()> {
    /// Start an empty sink stack.
    pub const fn new() -> Self {
        Self { layers: () }
    }
}

impl Default for SinkStack<()> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L> SinkStack<L> {
    /// Add the next tier in record-entry order.
    pub fn tier<St>(self, store: St, policy: FlushPolicy) -> SinkStack<(St, FlushPolicy, L)> {
        SinkStack {
            layers: (store, policy, self.layers),
        }
    }

    /// Finish the stack with its terminal sink.
    ///
    /// The record type is normally inferred from the stores, terminal, or
    /// surrounding pipeline. For a terminal that implements [`Sink`] for
    /// multiple record types, specify it explicitly with
    /// `terminal::<MyRecord, _>(sink)`.
    #[must_use = "the completed sink stack must be run by a Pipeline or used as a Sink"]
    pub fn terminal<R, S>(self, sink: S) -> <L as BuildSinkStack<R, S>>::Output
    where
        S: Sink<R>,
        L: BuildSinkStack<R, S>,
    {
        sealed::Build::build(self.layers, sink)
    }
}

mod sealed {
    use super::{FlushPolicy, Sink, Store, Tier};

    pub trait Build<R, S>
    where
        S: Sink<R>,
    {
        type Output: Sink<R>;

        fn build(self, sink: S) -> Self::Output;
    }

    impl<R, S> Build<R, S> for ()
    where
        S: Sink<R>,
    {
        type Output = S;

        fn build(self, sink: S) -> Self::Output {
            sink
        }
    }

    impl<R, S, St, Rest> Build<R, S> for (St, FlushPolicy, Rest)
    where
        R: Send + 'static,
        S: Sink<R>,
        St: Store<R>,
        Rest: Build<R, Tier<R, St, S>>,
    {
        type Output = Rest::Output;

        fn build(self, sink: S) -> Self::Output {
            let (store, policy, rest) = self;
            rest.build(Tier::new(store, policy, sink))
        }
    }
}

/// Type-level completion of a [`SinkStack`].
///
/// This trait is public only so [`SinkStack::terminal`] can expose its exact
/// concrete output type. Its private supertrait seals the supported layer-list
/// representation; use [`SinkStack`] rather than naming this trait directly.
#[doc(hidden)]
pub trait BuildSinkStack<R, S>:
    sealed::Build<R, S, Output = <Self as BuildSinkStack<R, S>>::Output>
where
    S: Sink<R>,
{
    type Output: Sink<R>;
}

impl<R, S, L> BuildSinkStack<R, S> for L
where
    S: Sink<R>,
    L: sealed::Build<R, S>,
{
    type Output = <L as sealed::Build<R, S>>::Output;
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::SinkStack;
    use crate::layer::{FlushPolicy, Tier};
    use crate::sink::Sink;
    use crate::store::{JsonlStore, MemStore};
    use crate::test_util::{SharedSink, meta_at};

    #[tokio::test]
    async fn zero_tiers_return_the_terminal_unchanged() {
        let terminal = SharedSink::<i32>::new();
        let mut sink: SharedSink<i32> = SinkStack::new().terminal(terminal.clone());

        sink.ingest(&meta_at("p", 10), vec![1, 2]).await.unwrap();

        assert_eq!(terminal.records(), vec![1, 2]);
    }

    type TwoTier = Tier<i32, MemStore<i32>, Tier<i32, JsonlStore<i32>, SharedSink<i32>>>;

    fn assert_two_tier_type(_: &TwoTier) {}

    #[tokio::test]
    async fn tiers_build_in_source_order() {
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("p");
        let terminal = SharedSink::new();
        let mut sink = SinkStack::new()
            .tier(
                MemStore::new(),
                FlushPolicy::new(Duration::from_secs(300), 2),
            )
            .tier(JsonlStore::new(&store_dir), FlushPolicy::hourly())
            .terminal(terminal.clone());
        assert_two_tier_type(&sink);

        sink.ingest(&meta_at("p", 10), vec![1]).await.unwrap();
        let has_segments =
            store_dir.exists() && std::fs::read_dir(&store_dir).unwrap().next().is_some();
        assert!(!has_segments);
        assert!(terminal.batches().is_empty());

        sink.ingest(&meta_at("p", 20), vec![2]).await.unwrap();
        assert_eq!(std::fs::read_dir(&store_dir).unwrap().count(), 1);
        assert!(terminal.batches().is_empty());

        sink.flush().await.unwrap();
        assert_eq!(terminal.records(), vec![1, 2]);
        assert_eq!(std::fs::read_dir(&store_dir).unwrap().count(), 0);
    }

    type ThreeTier = Tier<
        i32,
        MemStore<i32>,
        Tier<i32, MemStore<i32>, Tier<i32, MemStore<i32>, SharedSink<i32>>>,
    >;

    fn assert_three_tier_type(_: &ThreeTier) {}

    #[test]
    fn three_tiers_preserve_recursive_type() {
        let sink = SinkStack::new()
            .tier(MemStore::<i32>::new(), FlushPolicy::hourly())
            .tier(MemStore::<i32>::new(), FlushPolicy::hourly())
            .tier(MemStore::<i32>::new(), FlushPolicy::hourly())
            .terminal(SharedSink::<i32>::new());

        assert_three_tier_type(&sink);
    }
}
