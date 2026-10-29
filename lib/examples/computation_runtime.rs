// Copyright 2026 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Native application components in the DrasiLib-owned ComputationGraph.
//! Run: cargo run -p drasi-lib --example computation_runtime
//! Experiment: cargo run -p drasi-lib --example computation_runtime -- --observability debug --slow-sink-ms 25
//! Jaeger demo: bash lib/examples/observability-demo/demo.sh run

#[path = "support/telemetry.rs"]
mod telemetry_host;
#[path = "support/export.rs"]
mod trace_export;

use drasi_core::computation::InMemoryComputationProvider;
use drasi_lib::observability::{TelemetryProfile, TelemetrySettings};
use drasi_lib::{computation::v1::*, DrasiLib};
use drasi_reaction_application::NativeApplicationReaction;
use drasi_source_application::{NativeApplicationSource, PropertyMapBuilder};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};

const SERVICE_NAME: &str = "drasi-observability-playground";
const USAGE: &str = "usage: computation_runtime [--observability off|basic|debug \
    [--slow-sink-ms 0..1000] [--otlp-endpoint http://127.0.0.1:4318/v1/traces] \
    [--run-name NAME]]";

#[derive(Debug)]
struct ExperimentOptions {
    profile: TelemetryProfile,
    delay: Duration,
    otlp_endpoint: Option<String>,
    run_name: String,
}

impl ExperimentOptions {
    fn parse(arguments: Vec<String>) -> anyhow::Result<Option<Self>> {
        if arguments.is_empty() {
            return Ok(None);
        }
        anyhow::ensure!(arguments.len() % 2 == 0, "{USAGE}");
        let mut profile = None;
        let mut delay = None;
        let mut otlp_endpoint = None;
        let mut run_name = None;
        let mut seen = std::collections::BTreeSet::new();
        for pair in arguments.chunks_exact(2) {
            let flag = pair[0].as_str();
            let value = pair[1].as_str();
            anyhow::ensure!(seen.insert(flag), "duplicate option: {flag}");
            match flag {
                "--observability" => {
                    profile = Some(match value {
                        "off" => TelemetryProfile::Off,
                        "basic" => TelemetryProfile::Basic,
                        "debug" => TelemetryProfile::Debug,
                        other => anyhow::bail!("unknown telemetry profile: {other}"),
                    });
                }
                "--slow-sink-ms" => {
                    let millis: u64 = value.parse()?;
                    anyhow::ensure!(millis <= 1_000, "slow sink delay must be at most 1000 ms");
                    delay = Some(Duration::from_millis(millis));
                }
                "--otlp-endpoint" => {
                    anyhow::ensure!(
                        value.starts_with("http://") && value.ends_with("/v1/traces"),
                        "this local demo requires an HTTP trace endpoint ending in /v1/traces"
                    );
                    otlp_endpoint = Some(value.to_owned());
                }
                "--run-name" => {
                    anyhow::ensure!(
                        (1..=64).contains(&value.len())
                            && value
                                .bytes()
                                .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c)),
                        "run name must be 1..=64 ASCII letters, digits, '.', '_' or '-'"
                    );
                    run_name = Some(value.to_owned());
                }
                other => anyhow::bail!("unknown option: {other}; {USAGE}"),
            }
        }
        let profile = profile.ok_or_else(|| anyhow::anyhow!("{USAGE}"))?;
        anyhow::ensure!(
            profile != TelemetryProfile::Off || otlp_endpoint.is_none(),
            "OTLP export requires basic or debug observability"
        );
        Ok(Some(Self {
            profile,
            delay: delay.unwrap_or(Duration::ZERO),
            otlp_endpoint,
            run_name: run_name.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        }))
    }
}

#[allow(clippy::print_stdout)]
async fn run(options: Option<Duration>, telemetry: TelemetrySettings) -> anyhow::Result<()> {
    const INSTANCE: &str = "native-runtime-example";
    let drasi = DrasiLib::builder()
        .with_id(INSTANCE)
        .with_telemetry(telemetry)
        .build()
        .await?;
    let result = async {
        drasi.start().await?;
        let control = drasi.computation_control()?;
        let graph_id = control.desired_snapshot().id.to_string();
        let capacity = NonZeroUsize::new(16).expect("nonzero capacity");
        let source_stream = StreamId::try_new("orders/out")?;
        let query_stream = StreamId::try_new("orders-query/out")?;
        let (source, input) = NativeApplicationSource::channel(
            INSTANCE,
            graph_id.clone(),
            "orders",
            source_stream.clone(),
            capacity,
        )?;
        let source_output = source.output();
        let query_id = ComponentId::try_new("orders-query")?;
        let query_text = if options.is_some() {
            "MATCH (o:Order) WHERE o.active = true RETURN o.name AS name"
        } else {
            "MATCH (o:Order) RETURN o.name AS name"
        };
        let query = ContinuousQueryTransformer::new(
            ContinuousQueryDefinition {
                graph_id,
                id: query_id.clone(),
                query: query_text.into(),
                language: ComputationQueryLanguage::Cypher,
                output_stream: query_stream.clone(),
                outbox_capacity: capacity,
            },
            Arc::new(InMemoryComputationProvider),
        )
        .await?
        .with_recovery_scope(INSTANCE)?;
        let (reaction, mut receiver) = if let Some(delay) = options {
            let (sender, receiver) = tokio::sync::mpsc::channel(16);
            let reaction = NativeApplicationReaction::callback(
                "output",
                QueryChangeCodec::schema().descriptor().clone(),
                move |input| {
                    let sender = sender.clone();
                    async move {
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
                        anyhow::ensure!(
                            !input.envelope.changes().operations().is_empty(),
                            "unexpected empty result"
                        );
                        sender.send(input).await?;
                        Ok(())
                    }
                },
            )?;
            (reaction, receiver)
        } else {
            NativeApplicationReaction::channel(
                "output",
                QueryChangeCodec::schema().descriptor().clone(),
                capacity,
            )?
        };
        let reaction_input = reaction.input();
        println!(
            "Source: {} (NativeApplicationSource; in-memory application input)",
            source_output.component
        );
        println!("Query: {query_id} (ContinuousQueryTransformer; Cypher)");
        println!("Cypher: {query_text}");
        match options {
            Some(delay) => println!(
                "Reaction: {} (NativeApplicationReaction; callback -> in-memory verification channel; sink delay={} ms; completion=Handled)",
                reaction_input.component,
                delay.as_millis()
            ),
            None => println!(
                "Reaction: {} (NativeApplicationReaction; in-memory result channel; completion=Accepted)",
                reaction_input.component
            ),
        }
        let query_input = Endpoint::new(query_id.clone(), PortId::try_new("in")?);
        let query_output = Endpoint::new(query_id, PortId::try_new("out")?);
        let mut members = Vec::new();
        for addition in [
            ComponentAddition::new(ConstructedComponent::source(Box::new(source)))
                .bind_stream(PortId::try_new("out")?, source_stream),
            ComponentAddition::new(ConstructedComponent::query(Box::new(query)))
                .bind_stream(PortId::try_new("out")?, query_stream),
            ComponentAddition::new(ConstructedComponent::sink(Box::new(reaction))),
        ] {
            members.push(addition.definition.descriptor.id().clone());
            control.add_component(addition.defer_activation()).await?;
        }
        for (from, to) in [(source_output, query_input), (query_output, reaction_input)] {
            let connected = control
                .connect(
                    EdgeDefinition::new(from, to),
                    Box::new(BoundedPipeConfig {
                        capacity: if options.is_some() { 1 } else { 16 },
                    }),
                    RelationshipPolicy::default(),
                )
                .await?;
            anyhow::ensure!(connected.committed, "{connected:?}");
        }
        let started = control
            .start_components(
                control.desired_snapshot().revision,
                GraphSelection::Exact(members),
            )
            .await?;
        anyhow::ensure!(
            started.summary == OperationSummary::Completed,
            "{started:?}"
        );
        if options.is_some() {
            let properties = |name, active| {
                PropertyMapBuilder::new()
                    .with_string("name", name)
                    .with_bool("active", active)
                    .build()
            };
            input
                .send_node_insert("one", vec!["Order"], properties("First order", true))
                .await?;
            input
                .send_node_update("one", vec!["Order"], properties("Updated order", true))
                .await?;
            input.send_delete("one", vec!["Order"]).await?;
            input
                .send_node_insert("filtered", vec!["Order"], properties("Not selected", false))
                .await?;
            input
                .send_node_insert("last", vec!["Order"], properties("Last order", true))
                .await?;
        } else {
            input
                .send_node_insert(
                    "one",
                    vec!["Order"],
                    PropertyMapBuilder::new()
                        .with_string("name", "First order")
                        .build(),
                )
                .await?;
        }
        for _ in 0..if options.is_some() { 4 } else { 1 } {
            let result = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
                .await?
                .ok_or_else(|| anyhow::anyhow!("output closed"))?;
            println!(
                "ComputationGraph emitted {} result changes",
                result.envelope.changes().operations().len()
            );
        }
        if options.is_some() {
            anyhow::ensure!(
                drasi.get_query_results("orders-query").await?
                    == vec![serde_json::json!({"name": "Last order"})],
                "experiment produced unexpected final rows"
            );
        }
        println!(
            "Current rows: {:?}",
            drasi.get_query_results("orders-query").await?
        );
        println!(
            "Query metrics: {:?}",
            drasi.get_query_output_metrics("orders-query").await?
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let shutdown = drasi.shutdown().await;
    let failures: Vec<anyhow::Error> = result
        .err()
        .into_iter()
        .chain(shutdown.err().map(Into::into))
        .collect();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(drasi_lib::error::OperationFailures::new("example failed", failures).into())
    }
}

#[tokio::main(flavor = "current_thread")]
#[allow(clippy::print_stdout)]
async fn main() -> anyhow::Result<()> {
    use opentelemetry::trace::TracerProvider;
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::{layer::SubscriberExt, Layer};

    let Some(options) = ExperimentOptions::parse(std::env::args().skip(1).collect())? else {
        return run(None, TelemetrySettings::default()).await;
    };
    let diagnostics = trace_export::ExportDiagnostics::default();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_sampler(opentelemetry_sdk::trace::Sampler::ParentBased(Box::new(
            opentelemetry_sdk::trace::Sampler::AlwaysOn,
        )))
        .with_resource(
            opentelemetry_sdk::Resource::builder_empty()
                .with_service_name(SERVICE_NAME)
                .with_attributes([
                    opentelemetry::KeyValue::new("drasi.demo.run", options.run_name.clone()),
                    opentelemetry::KeyValue::new(
                        "drasi.demo.sink_delay_ms",
                        options.delay.as_millis() as i64,
                    ),
                ])
                .build(),
        );
    let provider = if let Some(endpoint) = &options.otlp_endpoint {
        use opentelemetry_otlp::WithExportConfig;
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
            .with_endpoint(endpoint)
            .with_timeout(Duration::from_secs(5))
            .build()?;
        provider.with_batch_exporter(diagnostics.observe(exporter))
    } else {
        provider
            .with_batch_exporter(diagnostics.observe(opentelemetry_stdout::SpanExporter::default()))
    }
    .build();
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install()?;
    tracing_log::LogTracer::init()?;
    let logs = drasi_lib::observability::init_component_logging().await?;
    let export_traces = options.profile != TelemetryProfile::Off;
    let graph_only = options.otlp_endpoint.is_some();
    let subscriber = tracing_subscriber::registry().with(logs.layer()).with(
        tracing_opentelemetry::layer()
            .with_tracer(provider.tracer(SERVICE_NAME))
            .with_filter(tracing_subscriber::filter::filter_fn(move |metadata| {
                export_traces
                    && metadata.name() != "computation_component"
                    && (!graph_only || metadata.target() == "drasi::telemetry")
            })),
    );
    println!(
        "Trace run: {} (service={SERVICE_NAME}, sink_delay={} ms, exporter={})",
        options.run_name,
        options.delay.as_millis(),
        options.otlp_endpoint.as_deref().unwrap_or("stdout"),
    );
    let result = run(
        Some(options.delay),
        options
            .profile
            .resolve()
            .with_trace_adapter(Arc::new(telemetry_host::OtelContextAdapter)),
    )
    .with_subscriber(subscriber)
    .await;
    let logging_shutdown = logs.shutdown(Duration::from_secs(5)).await;
    let export_shutdown = tokio::task::spawn_blocking(move || provider.shutdown()).await;
    for (key, unit, _, value) in snapshotter.snapshot().into_vec() {
        println!("metric {:?} unit={unit:?} value={value:?}", key.key());
    }
    let mut failures: Vec<anyhow::Error> = result
        .err()
        .into_iter()
        .chain(logging_shutdown.err())
        .collect();
    match export_shutdown {
        Ok(Ok(())) => {}
        Ok(Err(error)) => failures.push(error.into()),
        Err(error) => failures.push(error.into()),
    }
    match diagnostics.check() {
        Ok(count) => println!("Trace export completed: {count} spans"),
        Err(error) => failures.push(error),
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(drasi_lib::error::OperationFailures::new("telemetry example failed", failures).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(arguments: &[&str]) -> anyhow::Result<Option<ExperimentOptions>> {
        ExperimentOptions::parse(arguments.iter().map(|value| (*value).to_owned()).collect())
    }

    #[test]
    fn existing_invocations_keep_their_defaults() {
        assert!(options(&[]).unwrap().is_none());
        let stdout = options(&["--observability", "debug", "--slow-sink-ms", "25"])
            .unwrap()
            .unwrap();
        assert_eq!(stdout.profile, TelemetryProfile::Debug);
        assert_eq!(stdout.delay, Duration::from_millis(25));
        assert!(stdout.otlp_endpoint.is_none());
        assert_eq!(
            options(&["--observability", "off"])
                .unwrap()
                .unwrap()
                .profile,
            TelemetryProfile::Off
        );
    }

    #[test]
    fn otlp_and_run_identity_are_explicit_and_options_can_be_reordered() {
        let options = options(&[
            "--run-name",
            "slow-demo",
            "--otlp-endpoint",
            "http://127.0.0.1:4318/v1/traces",
            "--observability",
            "debug",
            "--slow-sink-ms",
            "250",
        ])
        .unwrap()
        .unwrap();
        assert_eq!(options.run_name, "slow-demo");
        assert_eq!(options.delay, Duration::from_millis(250));
        assert_eq!(
            options.otlp_endpoint.as_deref(),
            Some("http://127.0.0.1:4318/v1/traces")
        );
    }

    #[test]
    fn invalid_or_misleading_options_fail() {
        for arguments in [
            vec!["--observability"],
            vec!["--observability", "other"],
            vec!["--observability", "debug", "--slow-sink-ms", "1001"],
            vec!["--observability", "debug", "--slow-sink-ms", "-1"],
            vec!["--observability", "debug", "--slow-sink-ms", "not-a-number"],
            vec!["--observability", "debug", "--run-name", ""],
            vec!["--observability", "debug", "--run-name", "spaces not allowed"],
            vec!["--observability", "debug", "--unknown", "value"],
            vec!["--observability", "debug", "--observability", "basic"],
            vec!["--observability", "debug", "--otlp-endpoint", "http://127.0.0.1:4318"],
            vec!["--observability", "off", "--otlp-endpoint", "http://127.0.0.1:4318/v1/traces"],
            vec!["--run-name", "missing-profile"],
        ] {
            assert!(options(&arguments).is_err(), "{arguments:?}");
        }
    }
}
