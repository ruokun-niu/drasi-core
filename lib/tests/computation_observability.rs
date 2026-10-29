// Copyright 2026 The Drasi Authors.
// Licensed under the Apache License, Version 2.0.

#[path = "../examples/support/telemetry.rs"]
mod telemetry_host;

use std::{
    collections::{BTreeMap, VecDeque},
    num::NonZeroUsize,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use async_trait::async_trait;
use drasi_core::{
    computation::InMemoryComputationProvider,
    models::{Element, ElementMetadata, ElementPropertyMap, ElementReference, SourceChange},
};
use drasi_lib::{
    computation::v1::*,
    observability::{ExecutionContext, TelemetryProfile, TelemetrySettings, TraceContextV1},
};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use opentelemetry::trace::{TraceContextExt, TracerProvider};
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider, SpanData, SpanExporter};
use serial_test::serial;
use tracing::instrument::WithSubscriber;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{layer::SubscriberExt, Layer};

#[derive(Debug, Clone, Default)]
struct Capture(Arc<Mutex<Vec<SpanData>>>);

impl SpanExporter for Capture {
    async fn export(&self, spans: Vec<SpanData>) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.lock().expect("captured spans").extend(spans);
        Ok(())
    }
}

fn recorder() -> &'static Snapshotter {
    static RECORDER: OnceLock<Snapshotter> = OnceLock::new();
    RECORDER.get_or_init(|| {
        let recorder = DebuggingRecorder::new();
        let snapshot = recorder.snapshotter();
        recorder.install().expect("install test recorder once");
        snapshot
    })
}

struct Metric {
    name: String,
    labels: BTreeMap<String, String>,
    value: DebugValue,
}

fn measurements(scope: &str) -> Vec<Metric> {
    recorder()
        .snapshot()
        .into_vec()
        .into_iter()
        .filter_map(|(key, _, _, value)| {
            let labels: BTreeMap<_, _> = key
                .key()
                .labels()
                .map(|label| (label.key().to_owned(), label.value().to_owned()))
                .collect();
            (labels.get("instance_id").is_some_and(|id| id == scope)).then(|| Metric {
                name: key.key().name().to_owned(),
                labels,
                value,
            })
        })
        .collect()
}

fn count(metrics: &[Metric], name: &str, labels: &[(&str, &str)]) -> u64 {
    metrics
        .iter()
        .filter(|metric| {
            metric.name == name
                && labels
                    .iter()
                    .all(|(key, value)| metric.labels.get(*key).is_some_and(|v| v == value))
        })
        .map(|metric| match metric.value {
            DebugValue::Counter(value) => value,
            _ => panic!("not a counter"),
        })
        .sum()
}

fn id(value: &str) -> ComponentId {
    ComponentId::try_new(value).expect("component ID")
}
fn port(value: &str) -> PortId {
    PortId::try_new(value).expect("port ID")
}
fn stream(value: &str) -> StreamId {
    StreamId::try_new(value).expect("stream ID")
}
fn endpoint(node: &str, name: &str) -> Endpoint {
    Endpoint::new(id(node), port(name))
}

fn descriptor(
    name: &str,
    direction: PortDirection,
    schema: SchemaDescriptor,
) -> ComponentDescriptor {
    ComponentDescriptor::try_new(
        id(name),
        vec![PortDescriptor::new(
            port(if direction == PortDirection::Output {
                "out"
            } else {
                "in"
            }),
            direction,
            schema,
            PipeRequirements::default(),
        )],
    )
    .expect("test descriptor")
}

fn event(sequence: u64, element_id: &str, active: bool, update: bool) -> ChangeEnvelope {
    let element = Element::Node {
        metadata: ElementMetadata {
            reference: ElementReference::new("source", element_id),
            labels: Arc::from([Arc::from("Item")]),
            effective_from: sequence,
        },
        properties: ElementPropertyMap::from(
            serde_json::json!({"active": active, "value": element_id}),
        ),
    };
    GraphChangeCodec::encode_change(
        if update {
            SourceChange::update(element)
        } else {
            SourceChange::Insert { element }
        },
        stream("source/out"),
        sequence,
        None,
    )
    .expect("test graph change")
}

struct Source {
    descriptor: ComponentDescriptor,
    events: VecDeque<ChangeEnvelope>,
}

#[async_trait]
impl ComputationComponent for Source {
    fn descriptor(&self) -> &ComponentDescriptor {
        &self.descriptor
    }
    async fn start(&mut self) -> anyhow::Result<()> {
        tracing::info!("source started");
        Ok(())
    }
    async fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
#[async_trait]
impl EnvelopeSource for Source {
    async fn next(&mut self) -> anyhow::Result<Option<OutputEnvelope>> {
        Ok(self.events.pop_front().map(|envelope| OutputEnvelope {
            port: port("out"),
            envelope,
        }))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("injected sink failure")]
struct InjectedFailure;

#[derive(Default)]
struct Received {
    envelopes: Vec<ChangeEnvelope>,
    contexts: Vec<opentelemetry::trace::SpanContext>,
}

struct Sink {
    descriptor: ComponentDescriptor,
    received: Arc<Mutex<Received>>,
    fail: bool,
    delay: Duration,
}

#[async_trait]
impl ComputationComponent for Sink {
    fn descriptor(&self) -> &ComponentDescriptor {
        &self.descriptor
    }
    async fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
#[async_trait]
impl EnvelopeSink for Sink {
    fn completion(&self) -> SinkCompletion {
        SinkCompletion::Handled
    }
    async fn handle(&mut self, input: InputEnvelope) -> anyhow::Result<()> {
        if self.fail {
            return Err(InjectedFailure.into());
        }
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        let context = tracing::Span::current().context();
        let mut received = self.received.lock().expect("received results");
        received
            .contexts
            .push(context.span().span_context().clone());
        received.envelopes.push(input.envelope);
        Ok(())
    }
}

async fn pipeline(
    scope: &str,
    settings: TelemetrySettings,
    fail: bool,
    delay: Duration,
) -> anyhow::Result<(Option<Arc<GraphError>>, Arc<Mutex<Received>>)> {
    let received = Arc::new(Mutex::new(Received::default()));
    let source = Source {
        descriptor: descriptor(
            "source",
            PortDirection::Output,
            GraphChangeCodec::schema().descriptor().clone(),
        ),
        events: VecDeque::from([
            event(1, "one", true, false),
            event(2, "one", false, true),
            event(3, "filtered", false, false),
            event(4, "last", true, false),
        ]),
    };
    let query = ContinuousQueryTransformer::new(
        ContinuousQueryDefinition {
            graph_id: scope.into(),
            id: id("query"),
            query: "MATCH (n:Item) WHERE n.active = true RETURN n.value AS value".into(),
            language: ComputationQueryLanguage::Cypher,
            output_stream: stream("query/out"),
            outbox_capacity: NonZeroUsize::new(4).expect("nonzero capacity"),
        },
        Arc::new(InMemoryComputationProvider),
    )
    .await?;
    let sink = Sink {
        descriptor: descriptor(
            "sink",
            PortDirection::Input,
            QueryChangeCodec::schema().descriptor().clone(),
        ),
        received: received.clone(),
        fail,
        delay,
    };
    let mut graph = ComputationGraph::builder(scope.to_owned())
        .with_telemetry(settings)
        .source(Box::new(source))
        .query(Box::new(query))
        .sink(Box::new(sink))
        .bind_stream(endpoint("source", "out"), stream("source/out"))
        .bind_stream(endpoint("query", "out"), stream("query/out"))
        .connect(
            EdgeDefinition::new(endpoint("source", "out"), endpoint("query", "in")),
            Box::new(BoundedPipeConfig { capacity: 1 }),
        )
        .connect(
            EdgeDefinition::new(endpoint("query", "out"), endpoint("sink", "in")),
            Box::new(BoundedPipeConfig { capacity: 1 }),
        )
        .build()?;
    let run = graph.start()?;
    let control = run.control();
    let mut observed = control.subscribe_observed();
    tokio::pin!(run);
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            result = &mut run => Ok::<_, anyhow::Error>(result.err().map(Arc::new)),
            failed = observed.wait_for(|state| state.components[&id("sink")].failure.is_some()), if fail => {
                let cause = failed?.components[&id("sink")].failure.as_ref().expect("observed failure").cause.clone();
                control.cancel();
                if let Err(error) = run.await {
                    if !matches!(error, GraphError::Cancelled) {
                        return Err(error.into());
                    }
                }
                Ok(Some(cause))
            }
        }
    }).await??;
    Ok((result, received))
}

async fn qualify(scope: &str, sampled: bool, profile: TelemetryProfile) {
    recorder();
    let capture = Capture::default();
    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::ParentBased(Box::new(if sampled {
            Sampler::AlwaysOn
        } else {
            Sampler::AlwaysOff
        })))
        .with_simple_exporter(capture.clone())
        .build();
    let logs = drasi_lib::observability::init_component_logging()
        .await
        .expect("initialize log runtime");
    let subscriber = tracing_subscriber::registry().with(logs.layer()).with(
        tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("test"))
            .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                metadata.name() != "computation_component"
            })),
    );
    let (error, received) = pipeline(
        scope,
        profile
            .resolve()
            .with_trace_adapter(Arc::new(telemetry_host::OtelContextAdapter)),
        false,
        Duration::from_millis(10),
    )
    .with_subscriber(subscriber)
    .await
    .expect("complete test pipeline");
    assert!(error.is_none(), "{error:?}");
    logs.shutdown(Duration::from_secs(1))
        .await
        .expect("drain logs");
    provider.force_flush().expect("flush spans");
    let contexts = {
        let received = received.lock().expect("received results");
        assert_eq!(received.envelopes.len(), 3);
        received.contexts.clone()
    };
    let metrics = measurements(scope);
    if profile == TelemetryProfile::Off {
        assert!(metrics.is_empty());
        assert!(capture
            .0
            .lock()
            .expect("captured spans")
            .iter()
            .all(|span| !matches!(
                span.name.as_ref(),
                "source.publish" | "component.process" | "sink.handle"
            )));
    } else {
        assert_eq!(count(&metrics, "drasi.source.events_dispatched", &[]), 4);
        assert_eq!(
            count(
                &metrics,
                "drasi.component.calls",
                &[("component_id", "query"), ("operation", "transform")]
            ),
            4
        );
        assert_eq!(
            count(
                &metrics,
                "drasi.component.inputs_processed",
                &[("component_id", "query")]
            ),
            4
        );
        assert_eq!(
            count(
                &metrics,
                "drasi.component.inputs_processed",
                &[("component_id", "sink"), ("completion", "handled")]
            ),
            3
        );
        assert_eq!(
            count(
                &metrics,
                "drasi.component.outputs_emitted",
                &[("component_id", "query")]
            ),
            3
        );
        assert_eq!(count(&metrics, "drasi.errors", &[]), 0);
        assert!(count(&metrics, "drasi.queue.blocked_enqueues", &[]) > 0);
        for metric in &metrics {
            assert!(!metric.labels.contains_key("generation"));
            assert!(!metric.labels.contains_key("trace_id"));
            if metric.name == "drasi.queue.depth" || metric.name == "drasi.component.up" {
                assert!(
                    matches!(metric.value, DebugValue::Gauge(value) if value.into_inner() == 0.0)
                );
            }
        }
        for context in &contexts {
            assert!(context.is_valid(), "unsampled context must still propagate");
            assert_eq!(context.is_sampled(), sampled);
        }
        let spans = capture.0.lock().expect("captured spans");
        if sampled {
            let publications: Vec<_> = spans
                .iter()
                .filter(|span| span.name == "source.publish")
                .collect();
            let queries: Vec<_> = spans
                .iter()
                .filter(|span| span.name == "component.process")
                .collect();
            let sinks: Vec<_> = spans
                .iter()
                .filter(|span| span.name == "sink.handle")
                .collect();
            assert_eq!((publications.len(), queries.len(), sinks.len()), (4, 4, 3));
            let roots: std::collections::HashSet<_> = publications
                .iter()
                .map(|span| span.span_context.trace_id())
                .collect();
            assert_eq!(roots.len(), 4);
            for query in &queries {
                assert!(publications
                    .iter()
                    .any(
                        |source| source.span_context.span_id() == query.parent_span_id
                            && source.span_context.trace_id() == query.span_context.trace_id()
                    ));
            }
            for sink in &sinks {
                assert!(queries
                    .iter()
                    .any(|query| query.span_context.span_id() == sink.parent_span_id
                        && query.span_context.trace_id() == sink.span_context.trace_id()));
                assert!(sink
                    .attributes
                    .iter()
                    .any(|field| field.key.as_str() == "completion"
                        && field.value.as_str() == "handled"));
            }
            for forwarded in spans.iter().filter(|span| span.name == "pipe.send") {
                if let Some(query) = queries
                    .iter()
                    .find(|query| query.span_context.span_id() == forwarded.parent_span_id)
                {
                    assert!(
                        query.end_time <= forwarded.start_time,
                        "processing includes subsequent forwarding"
                    );
                }
            }
            assert!(
                spans.iter().any(|span| span.name == "pipe.send")
                    == (profile == TelemetryProfile::Debug)
            );
        } else {
            assert!(spans.is_empty());
        }
    }
    let key = drasi_lib::observability::ComponentLogKey::new(
        scope,
        drasi_lib::observability::ComponentLogType::Source,
        "source",
    );
    assert!(logs
        .registry()
        .get_history_by_key(&key)
        .await
        .iter()
        .any(|log| log.message == "source started"));
    provider.shutdown().expect("shutdown tracer");
}

#[tokio::test(flavor = "current_thread")]
#[serial]
async fn connected_trace_and_exact_metrics_on_current_thread() {
    qualify("trace-current", true, TelemetryProfile::Debug).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn connected_trace_and_exact_metrics_on_multiple_threads() {
    qualify("trace-multi", true, TelemetryProfile::Basic).await;
}

#[tokio::test]
#[serial]
async fn unsampled_traces_preserve_context_and_metric_counts() {
    qualify("trace-unsampled", false, TelemetryProfile::Basic).await;
}

#[tokio::test]
#[serial]
async fn disabled_collection_preserves_results_and_component_logs() {
    qualify("trace-disabled", true, TelemetryProfile::Off).await;
}

#[tokio::test]
#[serial]
async fn sink_failure_is_observed_once_and_preserves_its_cause() {
    recorder();
    let (error, _) = pipeline(
        "trace-error",
        TelemetryProfile::Basic.resolve().without_traces(),
        true,
        Duration::ZERO,
    )
    .await
    .unwrap();
    let error = error.expect("sink must fail");
    let GraphError::Component { source, .. } = error.underlying() else {
        panic!("unexpected failure: {error:#}");
    };
    assert!(source.downcast_ref::<InjectedFailure>().is_some());
    let metrics = measurements("trace-error");
    assert_eq!(
        count(&metrics, "drasi.errors", &[("component_id", "sink")]),
        1
    );
    assert_eq!(
        count(
            &metrics,
            "drasi.component.inputs_processed",
            &[("component_id", "sink")]
        ),
        0
    );
}

#[tokio::test]
async fn transient_pipe_context_does_not_change_encoded_envelopes() {
    let envelope = event(1, "one", true, false);
    let mut codec = BinaryEnvelopeCodec::new(NonZeroUsize::new(64 * 1024).unwrap());
    codec.register_schema(GraphChangeCodec::schema()).unwrap();
    let before = codec.encode(&envelope).unwrap();
    let context = ExecutionContext {
        trace: Some(TraceContextV1 {
            trace_id: [1; 16],
            span_id: [2; 8],
            trace_flags: 0,
            trace_state: None,
        }),
    };
    let mut pipe = BoundedPipe::new(1).unwrap();
    let mut receiver = pipe.take_receiver().unwrap();
    pipe.sender()
        .send_with_context(envelope.clone(), context.clone())
        .await
        .unwrap();
    let received = receiver.receive().await.unwrap().unwrap();
    assert_eq!(received.execution_context(), &context);
    assert_eq!(codec.encode(received.envelope()).unwrap(), before);
    assert_eq!(codec.encode(&envelope).unwrap(), before);
}

#[tokio::test]
#[serial]
async fn recreated_components_accumulate_counters_under_stable_identity() {
    recorder();
    for _ in 0..2 {
        let (error, received) = pipeline(
            "trace-recreated",
            TelemetryProfile::Basic.resolve().without_traces(),
            false,
            Duration::ZERO,
        )
        .await
        .unwrap();
        assert!(error.is_none());
        assert_eq!(received.lock().unwrap().envelopes.len(), 3);
    }
    let metrics = measurements("trace-recreated");
    assert_eq!(count(&metrics, "drasi.source.events_dispatched", &[]), 8);
    assert_eq!(
        count(
            &metrics,
            "drasi.component.inputs_processed",
            &[("component_id", "query")]
        ),
        8
    );
    assert_eq!(
        count(
            &metrics,
            "drasi.component.inputs_processed",
            &[("component_id", "sink")]
        ),
        6
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn identical_component_ids_in_two_graphs_have_separate_metrics() {
    recorder();
    let settings = TelemetryProfile::Basic.resolve().without_traces();
    let (a, b) = tokio::join!(
        pipeline("trace-isolation-a", settings.clone(), false, Duration::ZERO),
        pipeline("trace-isolation-b", settings, false, Duration::ZERO),
    );
    assert!(a.unwrap().0.is_none());
    assert!(b.unwrap().0.is_none());
    let snapshot = recorder().snapshot().into_vec();
    for scope in ["trace-isolation-a", "trace-isolation-b"] {
        let publication = snapshot
            .iter()
            .find(|(key, _, _, _)| {
                key.key().name() == "drasi.source.events_dispatched"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "instance_id" && label.value() == scope)
            })
            .expect("scoped publication counter");
        assert_eq!(publication.3, DebugValue::Counter(4));
    }
}

struct PendingSink {
    descriptor: ComponentDescriptor,
    entered: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl ComputationComponent for PendingSink {
    fn descriptor(&self) -> &ComponentDescriptor {
        &self.descriptor
    }
    async fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl EnvelopeSink for PendingSink {
    fn completion(&self) -> SinkCompletion {
        SinkCompletion::Handled
    }
    async fn handle(&mut self, _: InputEnvelope) -> anyhow::Result<()> {
        self.entered.notify_one();
        std::future::pending().await
    }
}

#[tokio::test]
#[serial]
async fn cancelling_a_handler_is_not_successful_handling() {
    recorder();
    let entered = Arc::new(tokio::sync::Notify::new());
    let schema = GraphChangeCodec::schema().descriptor().clone();
    let mut graph = ComputationGraph::builder("trace-cancelled")
        .with_telemetry(TelemetryProfile::Basic.resolve().without_traces())
        .source(Box::new(Source {
            descriptor: descriptor("source", PortDirection::Output, schema.clone()),
            events: VecDeque::from([event(1, "one", true, false)]),
        }))
        .sink(Box::new(PendingSink {
            descriptor: descriptor("sink", PortDirection::Input, schema),
            entered: entered.clone(),
        }))
        .bind_stream(endpoint("source", "out"), stream("source/out"))
        .connect(
            EdgeDefinition::new(endpoint("source", "out"), endpoint("sink", "in")),
            Box::new(BoundedPipeConfig { capacity: 1 }),
        )
        .build()
        .unwrap();
    let run = graph.start().unwrap();
    let control = run.control();
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(run, async {
            entered.notified().await;
            control.cancel();
        })
    })
    .await
    .unwrap();
    assert!(matches!(result, Err(GraphError::Cancelled)));
    let metrics = measurements("trace-cancelled");
    assert_eq!(
        count(
            &metrics,
            "drasi.component.calls",
            &[("component_id", "sink"), ("operation", "handle")]
        ),
        1
    );
    assert_eq!(
        count(
            &metrics,
            "drasi.component.inputs_processed",
            &[("component_id", "sink")]
        ),
        0
    );
    assert!(metrics.iter().any(|metric| {
        metric.name == "drasi.component.operation.duration"
            && metric
                .labels
                .get("component_id")
                .is_some_and(|value| value == "sink")
            && metric
                .labels
                .get("operation")
                .is_some_and(|value| value == "handle")
            && metric
                .labels
                .get("outcome")
                .is_some_and(|value| value == "cancelled")
            && matches!(&metric.value, DebugValue::Histogram(values) if values.len() == 1)
    }));
}

struct UnavailableTraceAdapter;

impl drasi_lib::observability::TraceContextAdapter for UnavailableTraceAdapter {
    fn context(
        &self,
        _: &tracing::Span,
        _: Option<&TraceContextV1>,
        _: &[TraceContextV1],
    ) -> anyhow::Result<TraceContextV1> {
        anyhow::bail!("injected adapter failure")
    }
}

#[tokio::test]
#[serial]
async fn adapter_failure_is_locally_diagnosed_without_failing_business_processing() {
    let settings = TelemetryProfile::Basic
        .resolve()
        .without_metrics()
        .with_trace_adapter(Arc::new(UnavailableTraceAdapter));
    let diagnostics = settings.clone();
    let (error, received) = pipeline("trace-adapter-failure", settings, false, Duration::ZERO)
        .with_subscriber(tracing_subscriber::registry())
        .await
        .unwrap();
    assert!(error.is_none());
    assert_eq!(received.lock().unwrap().envelopes.len(), 3);
    assert!(diagnostics.context_failures() > 0);
}
