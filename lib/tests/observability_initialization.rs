// Copyright 2026 The Drasi Authors.
// Licensed under the Apache License, Version 2.0.

use drasi_lib::{
    observability::{init_default_subscriber, TelemetryProfile},
    DrasiLib,
};

#[tokio::test]
async fn builders_install_no_globals_and_explicit_initialization_reports_conflicts() {
    let core = DrasiLib::builder()
        .with_id("no-global-side-effects")
        .build()
        .await
        .unwrap();
    core.shutdown().await.unwrap();
    let _graph = drasi_lib::computation::v1::ComputationGraph::empty("no-global-graph").unwrap();
    tracing::subscriber::set_global_default(tracing_subscriber::registry()).unwrap();
    metrics_util::debugging::DebuggingRecorder::new()
        .install()
        .unwrap();
    assert!(init_default_subscriber().await.is_err());
    assert!(DrasiLib::builder()
        .with_telemetry(TelemetryProfile::Basic.resolve())
        .build()
        .await
        .is_err());
}
