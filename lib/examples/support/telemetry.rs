// Copyright 2026 The Drasi Authors.
// Licensed under the Apache License, Version 2.0.

use drasi_lib::observability::{TraceContextAdapter, TraceContextV1};
use opentelemetry::{
    trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState},
    Context,
};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Example host integration, not an OpenTelemetry dependency of the runtime.
pub struct OtelContextAdapter;

fn span_context(context: &TraceContextV1) -> anyhow::Result<SpanContext> {
    context.validate()?;
    Ok(SpanContext::new(
        TraceId::from_bytes(context.trace_id),
        SpanId::from_bytes(context.span_id),
        TraceFlags::new(context.trace_flags),
        true,
        context
            .trace_state
            .as_deref()
            .map(str::parse)
            .transpose()?
            .unwrap_or_else(TraceState::default),
    ))
}

impl TraceContextAdapter for OtelContextAdapter {
    fn context(
        &self,
        span: &tracing::Span,
        parent: Option<&TraceContextV1>,
        links: &[TraceContextV1],
    ) -> anyhow::Result<TraceContextV1> {
        let parent = match parent {
            Some(parent) => Context::new().with_remote_span_context(span_context(parent)?),
            None => Context::new(),
        };
        span.set_parent(parent)?;
        for link in links {
            span.add_link(span_context(link)?);
        }
        let context = span.context();
        let current = context.span();
        let context = current.span_context();
        anyhow::ensure!(
            context.is_valid(),
            "host tracing layer did not provide a valid context"
        );
        let trace_state = context.trace_state().header();
        Ok(TraceContextV1 {
            trace_id: context.trace_id().to_bytes(),
            span_id: context.span_id().to_bytes(),
            trace_flags: context.trace_flags().to_u8(),
            trace_state: (!trace_state.is_empty()).then_some(trace_state),
        })
    }
}
