// Copyright 2026 The Drasi Authors.
// Licensed under the Apache License, Version 2.0.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use tokio::{
    sync::{mpsc, RwLock},
    task::JoinHandle,
};

use super::{ComponentLogLayer, ComponentLogRegistry, LogMessage};
use crate::context::workers::{
    join_owned_worker_gracefully, spawn_owned_worker, WorkerCleanupError, WorkerCompletion,
};

const LOG_CHANNEL_CAPACITY: usize = 10_000;

/// Unsampled local diagnostics, independent of subscribers and metric exporters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComponentLogLoss {
    pub full: u64,
    pub closed: u64,
}

pub(super) struct LogSubmission {
    sender: Mutex<Option<mpsc::Sender<LogMessage>>>,
    full: AtomicU64,
    closed: AtomicU64,
}

impl LogSubmission {
    pub(super) fn submit(&self, message: LogMessage) {
        let sender = self.sender.lock().expect("log submission lock poisoned");
        let result = match sender.as_ref() {
            Some(sender) => sender.try_send(message),
            None => Err(mpsc::error::TrySendError::Closed(message)),
        };
        let counter = match result {
            Ok(()) => return,
            Err(mpsc::error::TrySendError::Full(_)) => &self.full,
            Err(mpsc::error::TrySendError::Closed(_)) => &self.closed,
        };
        let discarded = counter.fetch_add(1, Ordering::Relaxed) + 1;
        // Never report loss through the same tracing layer.
        if discarded.is_power_of_two() {
            eprintln!(
                "drasi-lib: component log records discarded; inspect ComponentLogRuntime::loss()"
            );
        }
    }

    fn close(&self) {
        self.sender
            .lock()
            .expect("log submission lock poisoned")
            .take();
    }
}

/// Host-owned bounded component-log collection.
///
/// Install [`Self::layer`] in the application's subscriber before constructing
/// components. Stop all log producers before calling [`Self::shutdown`].
/// A timed-out or cancelled shutdown retains the worker for another attempt.
pub struct ComponentLogRuntime {
    registry: Arc<ComponentLogRegistry>,
    submission: Arc<LogSubmission>,
    worker: RwLock<Option<JoinHandle<()>>>,
    failure: Mutex<Option<crate::error::RetainedError>>,
}

impl ComponentLogRuntime {
    /// Create a collector on the caller's Tokio runtime, without installing globals.
    pub async fn new(
        registry: Arc<ComponentLogRegistry>,
        capacity: std::num::NonZeroUsize,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            capacity.get() <= tokio::sync::Semaphore::MAX_PERMITS,
            "component log capacity exceeds the runtime maximum"
        );
        tokio::runtime::Handle::try_current()?;
        let (sender, mut receiver) = mpsc::channel(capacity.get());
        let runtime = Self {
            registry: registry.clone(),
            submission: Arc::new(LogSubmission {
                sender: Mutex::new(Some(sender)),
                full: AtomicU64::new(0),
                closed: AtomicU64::new(0),
            }),
            worker: RwLock::new(None),
            failure: Mutex::new(None),
        };
        spawn_owned_worker(&runtime.worker, async move {
            while let Some(message) = receiver.recv().await {
                registry.log(message).await;
            }
        })
        .await?;
        Ok(runtime)
    }

    pub fn registry(&self) -> Arc<ComponentLogRegistry> {
        self.registry.clone()
    }

    /// Each layer submits to this runtime's bounded queue, not a global sender.
    pub fn layer(&self) -> ComponentLogLayer {
        ComponentLogLayer::new(self.submission.clone())
    }

    pub fn loss(&self) -> ComponentLogLoss {
        ComponentLogLoss {
            full: self.submission.full.load(Ordering::Relaxed),
            closed: self.submission.closed.load(Ordering::Relaxed),
        }
    }

    /// Close admission and drain accepted records. Retains ownership on timeout.
    pub async fn shutdown(&self, timeout: Duration) -> anyhow::Result<()> {
        self.submission.close();
        let mut worker = self.worker.write().await;
        if let Some(error) = self.failure.lock().expect("log cleanup ownership").clone() {
            return Err(error.into());
        }
        let error = match join_owned_worker_gracefully(&mut worker, timeout).await {
            Ok(WorkerCompletion::Absent | WorkerCompletion::Completed(())) => return Ok(()),
            Err(error @ WorkerCleanupError::TimedOut { .. }) => return Err(error.into()),
            Err(error) => anyhow::Error::from(error),
            Ok(WorkerCompletion::Cancelled) => {
                anyhow::anyhow!("component log worker was cancelled before its drain completed")
            }
        };
        let error = crate::error::RetainedError::from(error);
        *self.failure.lock().expect("log cleanup ownership") = Some(error.clone());
        Err(error.into())
    }
}

impl Drop for ComponentLogRuntime {
    fn drop(&mut self) {
        self.submission.close();
        if let Some(worker) = self.worker.get_mut() {
            worker.abort();
            eprintln!("drasi-lib: component log runtime dropped without awaited shutdown; queued logs may be lost");
        }
    }
}

/// Construct the default component-log collector without installing a subscriber.
///
/// The shared registry is also used by default `DrasiLib` construction. Hosts
/// wanting an isolated registry can use [`ComponentLogRuntime::new`] and
/// `DrasiLibBuilder::with_log_registry`.
pub async fn init_component_logging() -> anyhow::Result<ComponentLogRuntime> {
    ComponentLogRuntime::new(
        super::get_or_init_global_registry(),
        std::num::NonZeroUsize::new(LOG_CHANNEL_CAPACITY).expect("positive log capacity"),
    )
    .await
}

/// Explicit convenience initialization, including the `log` bridge.
///
/// Existing-subscriber/logger conflicts and invalid `RUST_LOG` filters are
/// errors. The caller retains the returned runtime through producer shutdown.
pub async fn init_default_subscriber() -> anyhow::Result<ComponentLogRuntime> {
    use tracing_subscriber::{layer::SubscriberExt, Layer};

    let filter = match std::env::var("RUST_LOG") {
        Ok(value) => tracing_subscriber::EnvFilter::try_new(value)?,
        Err(std::env::VarError::NotPresent) => tracing_subscriber::EnvFilter::new("info"),
        Err(error) => return Err(error.into()),
    };
    let runtime = init_component_logging().await?;
    let subscriber = tracing_subscriber::registry()
        .with(runtime.layer().with_filter(filter.clone()))
        .with(tracing_subscriber::fmt::layer().with_filter(filter));
    let installed = tracing_log::LogTracer::init()
        .map_err(anyhow::Error::from)
        .and_then(|()| tracing::subscriber::set_global_default(subscriber).map_err(Into::into));
    if let Err(error) = installed {
        let mut failures = vec![error];
        if let Err(cleanup) = runtime.shutdown(Duration::from_secs(5)).await {
            failures.push(cleanup);
        }
        return Err(crate::error::OperationFailures::new(
            "default logging initialization failed",
            failures,
        )
        .into());
    }
    Ok(runtime)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::{ComponentLogKey, ComponentLogType, LogLevel};
    use tracing_subscriber::layer::SubscriberExt;

    #[tokio::test]
    async fn layer_drains_its_own_registry_and_reports_closed_admission() {
        let registry = Arc::new(ComponentLogRegistry::new());
        let runtime =
            ComponentLogRuntime::new(registry.clone(), std::num::NonZeroUsize::new(2).unwrap())
                .await
                .unwrap();
        let subscriber = tracing_subscriber::registry().with(runtime.layer());
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(
                "test",
                instance_id = "instance",
                component_id = "source",
                component_type = "source"
            );
            span.in_scope(|| tracing::info!("captured"));
        });
        runtime.shutdown(Duration::from_secs(1)).await.unwrap();
        let key = ComponentLogKey::new("instance", ComponentLogType::Source, "source");
        let history = registry.get_history_by_key(&key).await;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].message, "captured");
        runtime.submission.submit(LogMessage::with_instance(
            LogLevel::Info,
            "closed",
            "instance",
            "source",
            ComponentLogType::Source,
        ));
        assert_eq!(runtime.loss(), ComponentLogLoss { full: 0, closed: 1 });
    }

    #[tokio::test]
    async fn overflow_is_bounded_and_accepted_records_drain() {
        let registry = Arc::new(ComponentLogRegistry::new());
        let runtime = ComponentLogRuntime::new(registry, std::num::NonZeroUsize::new(1).unwrap())
            .await
            .unwrap();
        let message = || {
            LogMessage::with_instance(
                LogLevel::Info,
                "bounded",
                "instance",
                "source",
                ComponentLogType::Source,
            )
        };
        // No suspension: the worker cannot consume the first record yet.
        runtime.submission.submit(message());
        runtime.submission.submit(message());
        assert_eq!(runtime.loss().full, 1);
        runtime.shutdown(Duration::from_secs(1)).await.unwrap();
        assert!(runtime.worker.read().await.is_none());
    }

    #[tokio::test]
    async fn timed_out_shutdown_retains_the_worker_until_joined() {
        let (sender, receiver) = mpsc::channel(1);
        let release = Arc::new(tokio::sync::Notify::new());
        let worker = RwLock::new(None);
        let wait = release.clone();
        spawn_owned_worker(&worker, async move {
            wait.notified().await;
            drop(receiver);
        })
        .await
        .unwrap();
        let runtime = ComponentLogRuntime {
            registry: Arc::new(ComponentLogRegistry::new()),
            submission: Arc::new(LogSubmission {
                sender: Mutex::new(Some(sender)),
                full: AtomicU64::new(0),
                closed: AtomicU64::new(0),
            }),
            worker,
            failure: Mutex::new(None),
        };
        let error = runtime
            .shutdown(Duration::from_millis(1))
            .await
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<WorkerCleanupError>(),
            Some(WorkerCleanupError::TimedOut { .. })
        ));
        assert!(runtime.worker.read().await.is_some());
        release.notify_one();
        runtime.shutdown(Duration::from_secs(1)).await.unwrap();
        assert!(runtime.worker.read().await.is_none());
    }

    #[tokio::test]
    async fn repeated_shutdown_preserves_terminal_worker_failure() {
        let runtime = init_component_logging().await.unwrap();
        runtime.worker.read().await.as_ref().unwrap().abort();
        assert!(runtime.shutdown(Duration::from_secs(1)).await.is_err());
        assert!(runtime.worker.read().await.is_none());
        assert!(runtime.shutdown(Duration::from_secs(1)).await.is_err());
    }
}
