//! [`Meathook`]: the supervisor runtime.
//!
//! Each pipeline is registered as a **factory closure** and runs in its own
//! tokio task. Panicked pipelines are rebuilt from their factory and
//! respawned with exponential backoff (durable layers re-run crash recovery
//! on whatever the dead task left behind). On SIGTERM/ctrl-c every pipeline
//! drains its sink stack before the runtime returns.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::io;
use std::pin::Pin;
use std::time::Duration;

use tokio::signal;
use tokio::signal::unix;
use tokio::task;
use tokio::task::JoinSet;
use tokio::time;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::collector::Collector;
use crate::pipeline::Pipeline;
use crate::sink::Sink;

type PipelineFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type PipelineFactory = Box<dyn Fn(CancellationToken) -> PipelineFuture + Send + Sync>;

/// Error from [`Meathook::run`].
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("failed to install signal handler: {0}")]
    Signal(#[source] io::Error),
}

/// Builder for the [`Meathook`] runtime.
pub struct MeathookBuilder {
    factories: Vec<PipelineFactory>,
    max_consecutive_failures: u32,
    base_backoff: Duration,
    /// A pipeline alive at least this long resets its failure streak.
    failure_window: Duration,
}

impl Default for MeathookBuilder {
    fn default() -> Self {
        Self {
            factories: vec![],
            max_consecutive_failures: 5,
            base_backoff: Duration::from_secs(1),
            failure_window: Duration::from_secs(300),
        }
    }
}

impl MeathookBuilder {
    /// Register a pipeline via its factory. The factory is invoked for the
    /// initial spawn and again after every panic, so the whole stack
    /// (collector, sink layers, spool recovery) is rebuilt fresh.
    pub fn pipeline<C, S, F, K, Make>(mut self, factory: Make) -> Self
    where
        Make: Fn() -> Pipeline<C, S, F, K> + Send + Sync + 'static,
        C: Collector + Send + 'static,
        S: Sink<C::Record> + Send + 'static,
        F: FnMut(&C::Record) -> K + Send + 'static,
        K: Eq + Hash + Send + 'static,
    {
        self.factories
            .push(Box::new(move |cancel| Box::pin(factory().run(cancel))));
        self
    }

    /// Give up respawning a pipeline after this many consecutive panics
    /// (default 5).
    pub fn max_consecutive_failures(mut self, max: u32) -> Self {
        self.max_consecutive_failures = max;
        self
    }

    /// Base delay for the exponential respawn backoff (default 1s).
    pub fn base_backoff(mut self, base: Duration) -> Self {
        self.base_backoff = base;
        self
    }

    pub fn build(self) -> Meathook {
        Meathook { builder: self }
    }

    /// Shorthand for `.build().run()`.
    pub async fn run(self) -> Result<(), RuntimeError> {
        self.build().run().await
    }
}

/// Supervisor runtime owning every registered pipeline.
pub struct Meathook {
    builder: MeathookBuilder,
}

struct PipelineState {
    failures: u32,
    spawned_at: Instant,
    gave_up: bool,
}

impl Meathook {
    pub fn builder() -> MeathookBuilder {
        MeathookBuilder::default()
    }

    /// Run until SIGTERM/ctrl-c, then drain every pipeline's sink stack
    /// before returning.
    pub async fn run(self) -> Result<(), RuntimeError> {
        let cancel = CancellationToken::new();
        let signal_cancel = cancel.clone();

        #[cfg(unix)]
        let mut sigterm = unix::signal(unix::SignalKind::terminate())
            .map_err(RuntimeError::Signal)?;
        tokio::spawn(async move {
            #[cfg(unix)]
            tokio::select! {
                _ = signal::ctrl_c() => info!("received ctrl-c; shutting down"),
                _ = sigterm.recv() => info!("received SIGTERM; shutting down"),
            }
            #[cfg(not(unix))]
            if signal::ctrl_c().await.is_ok() {
                info!("received ctrl-c; shutting down");
            }
            signal_cancel.cancel();
        });

        self.run_with_shutdown(cancel).await;
        Ok(())
    }

    /// Run until `cancel` fires. Exposed for embedding and tests; signal
    /// handling is layered on top by [`run`](Meathook::run).
    pub async fn run_with_shutdown(self, cancel: CancellationToken) {
        let MeathookBuilder {
            factories,
            max_consecutive_failures,
            base_backoff,
            failure_window,
        } = self.builder;

        let mut join_set = JoinSet::new();
        let mut states: Vec<PipelineState> = Vec::with_capacity(factories.len());
        let mut task_pipeline: HashMap<task::Id, usize> = HashMap::new();

        for (index, factory) in factories.iter().enumerate() {
            let handle = join_set.spawn(factory(cancel.child_token()));
            task_pipeline.insert(handle.id(), index);
            states.push(PipelineState {
                failures: 0,
                spawned_at: Instant::now(),
                gave_up: false,
            });
        }

        info!(pipelines = factories.len(), "meathook runtime started");

        while let Some(joined) = join_set.join_next_with_id().await {
            let (task_id, panicked) = match joined {
                Ok((id, ())) => (id, false),
                Err(join_error) => {
                    if join_error.is_cancelled() {
                        continue;
                    }
                    (join_error.id(), true)
                }
            };
            let Some(&index) = task_pipeline.get(&task_id) else {
                continue;
            };

            if !panicked || cancel.is_cancelled() {
                // Clean exit (cancellation drained the stack), or we are
                // shutting down anyway: don't respawn.
                continue;
            }

            let state = &mut states[index];
            if state.spawned_at.elapsed() >= failure_window {
                state.failures = 0;
            }
            state.failures += 1;

            if state.failures > max_consecutive_failures {
                if !state.gave_up {
                    state.gave_up = true;
                    error!(
                        pipeline = index,
                        failures = state.failures,
                        "pipeline keeps panicking; giving up on it"
                    );
                }
                continue;
            }

            let backoff = base_backoff.saturating_mul(1 << (state.failures - 1).min(16));
            warn!(
                pipeline = index,
                failures = state.failures,
                ?backoff,
                "pipeline panicked; respawning from factory"
            );

            tokio::select! {
                _ = cancel.cancelled() => continue,
                _ = time::sleep(backoff) => {}
            }

            state.spawned_at = Instant::now();
            let handle = join_set.spawn(factories[index](cancel.child_token()));
            task_pipeline.insert(handle.id(), index);
        }

        info!("meathook runtime stopped");
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::test_util::SharedSink;

    /// Panics on its very first collect of the process, then behaves: lets
    /// the test observe respawn-from-factory.
    struct PanicsOnce {
        global_calls: Arc<AtomicUsize>,
    }

    impl Collector for PanicsOnce {
        type Record = usize;
        type Error = Infallible;

        fn name(&self) -> &str {
            "panics-once"
        }

        async fn collect(&mut self) -> Result<Vec<usize>, Infallible> {
            let call = self.global_calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                panic!("boom");
            }
            Ok(vec![call])
        }
    }

    #[tokio::test(start_paused = true)]
    async fn respawns_panicked_pipeline_from_factory() {
        let sink = SharedSink::<usize>::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let built = Arc::new(AtomicUsize::new(0));

        let runtime = {
            let sink = sink.clone();
            let calls = Arc::clone(&calls);
            let built = Arc::clone(&built);
            Meathook::builder()
                .pipeline(move || {
                    built.fetch_add(1, Ordering::SeqCst);
                    Pipeline::new(
                        PanicsOnce {
                            global_calls: Arc::clone(&calls),
                        },
                        sink.clone(),
                        Duration::from_secs(60),
                    )
                })
                .build()
        };

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(runtime.run_with_shutdown(cancel.clone()));

        time::sleep(Duration::from_secs(200)).await;
        cancel.cancel();
        handle.await.unwrap();

        assert!(built.load(Ordering::SeqCst) >= 2, "factory not re-invoked");
        assert!(!sink.records().is_empty(), "no records after respawn");
        assert!(sink.flushed(), "stack not drained on shutdown");
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_after_max_consecutive_failures() {
        struct AlwaysPanics;
        impl Collector for AlwaysPanics {
            type Record = usize;
            type Error = Infallible;
            fn name(&self) -> &str {
                "always-panics"
            }
            async fn collect(&mut self) -> Result<Vec<usize>, Infallible> {
                panic!("boom");
            }
        }

        let built = Arc::new(AtomicUsize::new(0));
        let runtime = {
            let built = Arc::clone(&built);
            Meathook::builder()
                .max_consecutive_failures(2)
                .pipeline(move || {
                    built.fetch_add(1, Ordering::SeqCst);
                    Pipeline::new(
                        AlwaysPanics,
                        SharedSink::<usize>::new(),
                        Duration::from_secs(1),
                    )
                })
                .build()
        };

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(runtime.run_with_shutdown(cancel.clone()));
        time::sleep(Duration::from_secs(3600)).await;
        cancel.cancel();
        handle.await.unwrap();

        // Initial spawn + 2 respawns, then the supervisor gives up.
        assert_eq!(built.load(Ordering::SeqCst), 3);
    }
}
