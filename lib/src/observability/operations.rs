// Copyright 2026 The Drasi Authors.
// Licensed under the Apache License, Version 2.0.

use std::{
    future::Future,
    sync::{Arc, Mutex, MutexGuard},
    time::Instant,
};

use metrics_facade::{Counter, Gauge, Histogram, Label, Unit};
use tracing::{field, Instrument, Span};

use super::{ComponentLogType, ExecutionContext, TelemetrySettings, TraceContextV1};
use crate::computation::v1::{ComponentRole, ComponentSemanticKind};

#[derive(Clone, Copy)]
pub(crate) enum ObservedOperation {
    Publish,
    Transform,
    Continue,
    Replay,
    Handle,
    Start,
    Stop,
    Reconfigure,
    Next,
}

impl ObservedOperation {
    const ALL: [Self; 9] = [
        Self::Publish,
        Self::Transform,
        Self::Continue,
        Self::Replay,
        Self::Handle,
        Self::Start,
        Self::Stop,
        Self::Reconfigure,
        Self::Next,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Publish => "publish",
            Self::Transform => "transform",
            Self::Continue => "continue_transform",
            Self::Replay => "output_replay",
            Self::Handle => "handle",
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Reconfigure => "reconfigure",
            Self::Next => "next",
        }
    }
}

#[derive(Clone)]
struct OperationMetrics {
    calls: Counter,
    durations: [Histogram; 3],
    errors: Counter,
}

fn counter(settings: &TelemetrySettings, name: &'static str, labels: &[Label]) -> Counter {
    if settings.metric_enabled(name) {
        metrics_facade::describe_counter!(name, Unit::Count, name);
        metrics_facade::counter!(name, labels.to_vec())
    } else {
        Counter::noop()
    }
}

fn gauge(settings: &TelemetrySettings, name: &'static str, labels: &[Label]) -> Gauge {
    if settings.metric_enabled(name) {
        metrics_facade::describe_gauge!(name, Unit::Count, name);
        metrics_facade::gauge!(name, labels.to_vec())
    } else {
        Gauge::noop()
    }
}

fn histogram(settings: &TelemetrySettings, labels: &[Label]) -> Histogram {
    const NAME: &str = "drasi.component.operation.duration";
    if settings.metric_enabled(NAME) {
        metrics_facade::describe_histogram!(NAME, Unit::Seconds, "Elapsed local operation time");
        metrics_facade::histogram!(NAME, labels.to_vec())
    } else {
        Histogram::noop()
    }
}

pub(crate) struct ComponentTelemetry {
    settings: TelemetrySettings,
    instance: Arc<str>,
    graph: Arc<str>,
    component: Arc<str>,
    kind: &'static str,
    log_kind: &'static str,
    role: &'static str,
    generation: u64,
    operations: [OperationMetrics; 9],
    inputs: [Counter; 3],
    outputs: Counter,
    dispatched: Counter,
    up: Gauge,
    forwarding_errors: Counter,
}

impl ComponentTelemetry {
    pub(crate) fn new(
        settings: &TelemetrySettings,
        instance: Arc<str>,
        graph: Arc<str>,
        component: Arc<str>,
        role: ComponentRole,
        semantic: Option<ComponentSemanticKind>,
        generation: u64,
    ) -> Self {
        let log_kind = ComponentLogType::for_component(role, semantic).as_str();
        let role_name = ComponentLogType::from(role).as_str();
        let kind = match semantic {
            Some(ComponentSemanticKind::Source) => "source",
            Some(ComponentSemanticKind::Transformer) => "transformer",
            Some(ComponentSemanticKind::Query) => "query",
            Some(ComponentSemanticKind::Sink) => "sink",
            Some(ComponentSemanticKind::Reaction) => "reaction",
            Some(ComponentSemanticKind::Service) => "service",
            None => role_name,
        };
        let labels = vec![
            Label::new("instance_id", instance.to_string()),
            Label::new("graph_id", graph.to_string()),
            Label::new("component_id", component.to_string()),
            Label::new("component_kind", kind),
        ];
        let operations = ObservedOperation::ALL.map(|operation| {
            let mut labels = labels.clone();
            labels.push(Label::new("operation", operation.name()));
            let calls = counter(settings, "drasi.component.calls", &labels);
            let durations = ["success", "error", "cancelled"].map(|outcome| {
                let mut labels = labels.clone();
                labels.push(Label::new("outcome", outcome));
                histogram(settings, &labels)
            });
            labels.push(Label::new("error_kind", "component_operation"));
            OperationMetrics {
                calls,
                durations,
                errors: counter(settings, "drasi.errors", &labels),
            }
        });
        let inputs = ["accepted", "handled", "transformed"].map(|completion| {
            let mut labels = labels.clone();
            labels.push(Label::new("completion", completion));
            counter(settings, "drasi.component.inputs_processed", &labels)
        });
        let outputs = counter(settings, "drasi.component.outputs_emitted", &labels);
        let up = gauge(settings, "drasi.component.up", &labels);
        let mut forward_labels = labels.clone();
        forward_labels.push(Label::new("error_kind", "forwarding"));
        let forwarding_errors = counter(settings, "drasi.errors", &forward_labels);
        let dispatched = if role == ComponentRole::Source {
            let mut labels = labels;
            labels.push(Label::new("source_id", component.to_string()));
            labels.push(Label::new("phase", "steady"));
            counter(settings, "drasi.source.events_dispatched", &labels)
        } else {
            Counter::noop()
        };
        Self {
            settings: settings.clone(),
            instance,
            graph,
            component,
            kind,
            log_kind,
            role: role_name,
            generation,
            operations,
            inputs,
            outputs,
            dispatched,
            up,
            forwarding_errors,
        }
    }

    pub(crate) fn set_up(&self, up: bool) {
        self.up.set(if up { 1.0 } else { 0.0 });
    }

    pub(crate) fn input_processed(
        &self,
        completion: Option<crate::computation::v1::SinkCompletion>,
    ) {
        use crate::computation::v1::SinkCompletion;
        self.inputs[match completion {
            Some(SinkCompletion::Accepted) => 0,
            Some(SinkCompletion::Handled) => 1,
            None => 2,
        }]
        .increment(1);
    }

    pub(crate) fn outputs(&self, count: usize) {
        self.outputs.increment(count as u64);
    }

    pub(crate) fn dispatched(&self, count: usize) {
        self.dispatched.increment(count as u64);
    }

    pub(crate) fn forwarding_failed(&self) {
        self.forwarding_errors.increment(1);
    }

    pub(crate) fn traces_enabled(&self) -> bool {
        self.settings.traces
    }

    fn context(&self, span: &Span, parent: &ExecutionContext) -> ExecutionContext {
        if !self.settings.traces || span.is_disabled() {
            return parent.clone();
        }
        let parent = match parent.trace.as_ref() {
            Some(context) if context.validate().is_ok() => Some(context),
            Some(_) => {
                self.trace_failure("invalid input trace context");
                None
            }
            None => None,
        };
        let adapter = self
            .settings
            .adapter
            .as_ref()
            .expect("validated trace adapter");
        match adapter.context(span, parent, &[]).and_then(|context| {
            context.validate()?;
            Ok(context)
        }) {
            Ok(trace) => ExecutionContext { trace: Some(trace) },
            Err(error) => {
                self.trace_failure(&error.to_string());
                ExecutionContext {
                    trace: parent.cloned(),
                }
            }
        }
    }

    fn trace_failure(&self, reason: &str) {
        let count = self.settings.record_context_failure();
        if count.is_power_of_two() {
            log::warn!(
                "Trace context unavailable for component {} ({} failures): {}",
                self.component,
                count,
                reason
            );
        }
    }

    pub(crate) fn begin(
        &self,
        operation: ObservedOperation,
        parent: &ExecutionContext,
    ) -> OperationObservation {
        macro_rules! operation_span {
            ($name:literal) => {
                tracing::info_span!(
                    target: "drasi::telemetry", parent: None, $name,
                    instance_id = %self.instance, graph_id = %self.graph,
                    component_id = %self.component, component_kind = self.kind,
                    component_type = self.log_kind, role = self.role, generation = self.generation,
                    operation = operation.name(), outcome = field::Empty, error_kind = field::Empty,
                    input_port = field::Empty, output_port = field::Empty,
                    input_count = field::Empty, output_count = field::Empty,
                    change_count = field::Empty, completion = field::Empty,
                    publication_mode = field::Empty,
                )
            };
        }
        let span = if self.settings.traces {
            match operation {
                ObservedOperation::Publish => operation_span!("source.publish"),
                ObservedOperation::Transform | ObservedOperation::Continue => {
                    operation_span!("component.process")
                }
                ObservedOperation::Replay => operation_span!("component.scheduled"),
                ObservedOperation::Handle => operation_span!("sink.handle"),
                ObservedOperation::Start => operation_span!("component.start"),
                ObservedOperation::Stop => operation_span!("component.stop"),
                ObservedOperation::Reconfigure => operation_span!("component.reconfigure"),
                ObservedOperation::Next => Span::none(),
            }
        } else {
            Span::none()
        };
        let context = self.context(&span, parent);
        let metrics = self.operations[operation as usize].clone();
        metrics.calls.increment(1);
        OperationObservation {
            span,
            context,
            metrics,
            outcome: 2,
            started: self.settings.metrics.then(Instant::now),
        }
    }

    async fn observe<T, E>(
        &self,
        operation: ObservedOperation,
        parent: &ExecutionContext,
        input: Option<(&str, usize, Option<crate::computation::v1::SinkCompletion>)>,
        future: impl Future<Output = Result<T, E>>,
    ) -> (Result<T, E>, ExecutionContext) {
        let mut observation = self.begin(operation, parent);
        if let Some((port, changes, completion)) = input {
            observation.span.record("input_port", port);
            observation.span.record("input_count", 1u64);
            observation.span.record("change_count", changes as u64);
            if let Some(completion) = completion {
                observation.span.record(
                    "completion",
                    match completion {
                        crate::computation::v1::SinkCompletion::Accepted => "accepted",
                        crate::computation::v1::SinkCompletion::Handled => "handled",
                    },
                );
            }
        }
        let result = future.instrument(observation.span.clone()).await;
        observation.finish(result.is_ok());
        if let Some((_, _, completion)) = input {
            if result.is_ok() {
                self.input_processed(completion);
            }
        }
        (result, observation.context.clone())
    }

    pub(crate) async fn send(
        &self,
        queue: &str,
        output_port: &str,
        parent: &ExecutionContext,
        future: impl Future<
            Output = Result<
                crate::computation::v1::EnqueueReceipt,
                crate::computation::v1::SendFailure,
            >,
        >,
    ) -> Result<crate::computation::v1::EnqueueReceipt, crate::computation::v1::SendFailure> {
        if !self.settings.debug_spans() {
            return future.await;
        }
        let span = tracing::debug_span!(
            target: "drasi::telemetry", parent: None, "pipe.send",
            instance_id = %self.instance, graph_id = %self.graph,
            component_id = %self.component, component_type = self.log_kind,
            component_kind = self.kind, generation = self.generation,
            queue_id = queue, output_port, outcome = field::Empty,
            acceptance = field::Empty,
        );
        self.context(&span, parent);
        let mut outcome = SpanOutcome(span.clone());
        let result = future.instrument(span.clone()).await;
        span.record("outcome", if result.is_ok() { "success" } else { "error" });
        span.record(
            "acceptance",
            match &result {
                Ok(_) => "accepted",
                Err(error) => match error.acceptance() {
                    crate::computation::v1::AcceptanceState::NotAccepted => "not_accepted",
                    crate::computation::v1::AcceptanceState::Unknown => "unknown",
                },
            },
        );
        outcome.0 = Span::none();
        result
    }
}

pub(crate) async fn observe_component<T, E>(
    telemetry: Option<&ComponentTelemetry>,
    operation: ObservedOperation,
    parent: &ExecutionContext,
    input: Option<(&str, usize, Option<crate::computation::v1::SinkCompletion>)>,
    future: impl Future<Output = Result<T, E>>,
) -> (Result<T, E>, ExecutionContext) {
    match telemetry {
        Some(telemetry) => Box::pin(telemetry.observe(operation, parent, input, future)).await,
        None => (future.await, parent.clone()),
    }
}

struct SpanOutcome(Span);
impl Drop for SpanOutcome {
    fn drop(&mut self) {
        self.0.record("outcome", "cancelled");
        self.0.record("acceptance", "unknown");
    }
}

pub(crate) struct OperationObservation {
    pub(crate) span: Span,
    pub(crate) context: ExecutionContext,
    metrics: OperationMetrics,
    outcome: usize,
    started: Option<Instant>,
}

impl OperationObservation {
    pub(crate) fn finish(&mut self, success: bool) {
        self.outcome = usize::from(!success);
    }
}

impl Drop for OperationObservation {
    fn drop(&mut self) {
        self.span
            .record("outcome", ["success", "error", "cancelled"][self.outcome]);
        if self.outcome == 1 {
            self.span.record("error_kind", "component_operation");
            self.metrics.errors.increment(1);
        }
        if let Some(started) = self.started {
            self.metrics.durations[self.outcome].record(started.elapsed().as_secs_f64());
        }
    }
}

/// A provider's optional, binding-scoped queue instruments.
///
/// The runtime attaches this only to a provider exposing authoritative updates.
/// Providers must serialize updates with their actual queue counters.
pub struct QueueTelemetry {
    depth: Gauge,
    maximum: Gauge,
    blocked: Counter,
    drops: Counter,
    updates: Mutex<()>,
}

impl QueueTelemetry {
    pub(crate) fn new(
        settings: &TelemetrySettings,
        instance: &str,
        graph: &str,
        queue: &str,
    ) -> Self {
        let labels = [
            Label::new("instance_id", instance.to_owned()),
            Label::new("graph_id", graph.to_owned()),
            Label::new("queue_id", queue.to_owned()),
        ];
        let telemetry = Self {
            depth: gauge(settings, "drasi.queue.depth", &labels),
            maximum: gauge(settings, "drasi.queue.depth_max", &labels),
            blocked: counter(settings, "drasi.queue.blocked_enqueues", &labels),
            drops: counter(settings, "drasi.queue.drops", &labels),
            updates: Mutex::new(()),
        };
        telemetry.occupancy(0, 0);
        telemetry
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, ()> {
        self.updates
            .lock()
            .expect("queue telemetry update poisoned")
    }
    pub(crate) fn occupancy(&self, depth: usize, maximum: usize) {
        self.depth.set(depth as f64);
        self.maximum.set(maximum as f64);
    }
    pub(crate) fn blocked(&self) {
        self.blocked.increment(1);
    }
    pub(crate) fn discarded(&self, count: usize) {
        self.drops.increment(count as u64);
    }
}
