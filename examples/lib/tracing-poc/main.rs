// Copyright 2026 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");

//! POC validating the tracing/metrics integration added to drasi-lib.
//!
//! Verifies the smallest interesting slice:
//!
//! 1. `metrics` crate counters/histograms are recorded at:
//!     - `SourceBase::dispatch_source_change` (`drasi.source.events_dispatched`,
//!       `drasi.source.dispatch_duration_ns`)
//!     - Query forwarder (`drasi.query.events_received`)
//!     - Query processor (`drasi.query.events_processed`,
//!       `drasi.query.engine_duration_ns`)
//!
//! 2. The PriorityQueue carries `tracing::Span` handles across the
//!    forwarder→processor task boundary so that the `query.process` span
//!    can `follows_from` the `query.receive` span — producing a single
//!    connected trace tree across an async channel hop.
//!
//! Run with: `cargo run -p tracing-poc`

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use drasi_lib::{DrasiLib, Query};
use drasi_reaction_log::{LogReaction, QueryConfig, TemplateSpec};
use drasi_source_mock::{DataType, MockSource};
use metrics_util::debugging::DebuggingRecorder;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Tracing subscriber — show span open/close so we can see the tree.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,drasi_lib=info,tracing_poc=info")),
        )
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .with_target(true)
        .init();

    // 2. Metrics recorder we can snapshot at the end.
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install metrics recorder");

    println!("=========================================================");
    println!(" drasi-lib tracing + metrics POC");
    println!("=========================================================\n");

    // 3. Build pipeline. Use the mock source's built-in auto-generation
    //    (Counter type emits one Insert per interval) — that drives the
    //    full source -> query -> reaction path through the new spans
    //    and metrics without us needing to call inject_event after the
    //    source has been moved into the DrasiLib builder.
    let mock_source = MockSource::builder("poc-source")
        .with_data_type(DataType::Counter)
        .with_interval_ms(200)
        .with_auto_start(true)
        .build()?;

    let query = Query::cypher("poc-query")
        .query(
            r#"
            MATCH (n:Counter)
            RETURN n.counter_id AS id, n.value AS value
            "#,
        )
        .from_source("poc-source")
        .auto_start(true)
        .enable_bootstrap(false)
        .build();

    let log_reaction = LogReaction::builder("poc-logger")
        .from_query("poc-query")
        .with_default_template(QueryConfig {
            added: Some(TemplateSpec::new("[+] id={{after.id}} value={{after.value}}")),
            updated: Some(TemplateSpec::new(
                "[~] id={{after.id}} {{before.value}} -> {{after.value}}",
            )),
            deleted: Some(TemplateSpec::new("[-] id={{before.id}} removed")),
        })
        .build()?;

    // Second reaction subscribed to the same query — exercises the
    // query.dispatch -> reaction.receive fan-out.
    let log_reaction_2 = LogReaction::builder("poc-logger-2")
        .from_query("poc-query")
        .with_default_template(QueryConfig {
            added: Some(TemplateSpec::new("[#2 +] {{after.id}}")),
            updated: Some(TemplateSpec::new("[#2 ~] {{after.id}}")),
            deleted: Some(TemplateSpec::new("[#2 -] {{before.id}}")),
        })
        .build()?;

    let core = Arc::new(
        DrasiLib::builder()
            .with_id("tracing-poc")
            .with_source(mock_source)
            .with_query(query)
            .with_reaction(log_reaction)
            .with_reaction(log_reaction_2)
            .build()
            .await?,
    );

    println!("--- Starting pipeline ---");
    core.start().await?;

    // Let several events flow through.
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("\n--- Stopping pipeline ---");
    core.stop().await?;

    // 4. Snapshot metrics.
    println!("\n--- Recorded metrics ---");
    let snap = snapshotter.snapshot().into_vec();
    if snap.is_empty() {
        println!("(no metrics recorded — something is wrong)");
    } else {
        for (key, _unit, _desc, value) in &snap {
            println!("  {:<70} {:?}", format!("{}", key.key()), value);
        }
    }

    println!("\n--- Asserting expected metrics ---");
    let names: Vec<String> = snap
        .iter()
        .map(|(k, _, _, _)| k.key().name().to_string())
        .collect();
    let expected = [
        "drasi.source.events_dispatched",
        "drasi.source.dispatch_duration_ns",
        "drasi.query.events_received",
        "drasi.query.events_processed",
        "drasi.query.engine_duration_ns",
        "drasi.query.dispatch_duration_ns",
        "drasi.reaction.events_enqueued",
        "drasi.reaction.enqueue_duration_ns",
    ];
    let mut missing = vec![];
    for e in expected {
        if !names.iter().any(|n| n == e) {
            missing.push(e);
        }
    }
    if missing.is_empty() {
        println!("  OK — all expected metric names recorded.");
    } else {
        println!("  MISSING metrics: {:?}", missing);
        std::process::exit(1);
    }

    // Verify the source spans/metrics now cover the mock source path
    // (i.e., source.dispatch is recorded with source_id=poc-source, not
    // only the internal __component_graph__ source).
    let saw_poc_source = snap.iter().any(|(k, _, _, _)| {
        k.key().name() == "drasi.source.events_dispatched"
            && k.key()
                .labels()
                .any(|l| l.key() == "source_id" && l.value() == "poc-source")
    });
    if saw_poc_source {
        println!("  OK — drasi.source.events_dispatched recorded for poc-source.");
    } else {
        println!("  FAIL — no drasi.source.events_dispatched for poc-source");
        std::process::exit(1);
    }

    // Verify reaction fan-out: both reactions should have enqueued events.
    let reaction_ids: std::collections::HashSet<String> = snap
        .iter()
        .filter(|(k, _, _, _)| k.key().name() == "drasi.reaction.events_enqueued")
        .flat_map(|(k, _, _, _)| {
            k.key()
                .labels()
                .filter(|l| l.key() == "reaction_id")
                .map(|l| l.value().to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    println!("  Reactions that enqueued events: {:?}", reaction_ids);
    if reaction_ids.contains("poc-logger") && reaction_ids.contains("poc-logger-2") {
        println!("  OK — fan-out to both reactions recorded.");
    } else {
        println!("  FAIL — expected both reactions to enqueue events");
        std::process::exit(1);
    }

    println!("\ndone.");
    Ok(())
}
