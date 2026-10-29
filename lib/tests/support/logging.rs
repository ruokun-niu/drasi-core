// Copyright 2026 The Drasi Authors.
// Licensed under the Apache License, Version 2.0.

use tracing::instrument::WithSubscriber;
use tracing_subscriber::layer::SubscriberExt;

pub async fn with_component_logs(work: impl std::future::Future<Output = ()>) {
    static BRIDGE: std::sync::Once = std::sync::Once::new();
    BRIDGE.call_once(|| tracing_log::LogTracer::init().expect("install test-host log bridge"));
    let runtime = drasi_lib::observability::init_component_logging()
        .await
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(runtime.layer());
    work.with_subscriber(subscriber).await;
    runtime
        .shutdown(std::time::Duration::from_secs(5))
        .await
        .unwrap();
}
