// Copyright 2026 The Drasi Authors.
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

pub use drasi_computation_contracts::{ExecutionContext, TraceContextError, TraceContextV1};

/// Host integration between ordinary tracing spans and portable trace identity.
///
/// Attach the explicit parent (or root) and bounded links before making the
/// sampling decision, and return context even for an unsampled span. Implementors
/// must not perform exporter I/O here. An OpenTelemetry host can implement this
/// using `tracing-opentelemetry`; the runtime has no SDK dependency.
pub trait TraceContextAdapter: Send + Sync {
    fn context(
        &self,
        span: &tracing::Span,
        parent: Option<&TraceContextV1>,
        links: &[TraceContextV1],
    ) -> anyhow::Result<TraceContextV1>;
}

/// Collection policy, independent of host export and trace sampling.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TelemetryProfile {
    #[default]
    Off,
    Basic,
    Debug,
    Persistence,
}

impl TelemetryProfile {
    /// Resolve collection flags without creating subscribers, recorders or exporters.
    pub fn resolve(self) -> TelemetrySettings {
        TelemetrySettings {
            profile: self,
            traces: self != Self::Off,
            metrics: self != Self::Off,
            metric_exclusions: Vec::new(),
            adapter: None,
            context_failures: Arc::new(AtomicU64::new(0)),
        }
    }
}

/// Opt-in telemetry for a graph. Install host collection before building it.
#[derive(Clone)]
pub struct TelemetrySettings {
    pub(crate) profile: TelemetryProfile,
    pub(crate) traces: bool,
    pub(crate) metrics: bool,
    pub(crate) metric_exclusions: Vec<String>,
    pub(crate) adapter: Option<Arc<dyn TraceContextAdapter>>,
    context_failures: Arc<AtomicU64>,
}

impl Default for TelemetrySettings {
    fn default() -> Self {
        TelemetryProfile::Off.resolve()
    }
}

impl std::fmt::Debug for TelemetrySettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TelemetrySettings")
            .field("profile", &self.profile)
            .field("traces", &self.traces)
            .field("metrics", &self.metrics)
            .field("metric_exclusions", &self.metric_exclusions)
            .field("has_trace_adapter", &self.adapter.is_some())
            .finish()
    }
}

impl TelemetrySettings {
    /// Local context-validation/adapter failures, shared by clones of these settings.
    /// Readable without metric export; intentional filtering is not counted as loss.
    pub fn context_failures(&self) -> u64 {
        self.context_failures.load(Ordering::Relaxed)
    }

    pub(crate) fn record_context_failure(&self) -> u64 {
        self.context_failures.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn with_trace_adapter(mut self, adapter: Arc<dyn TraceContextAdapter>) -> Self {
        self.adapter = Some(adapter);
        self
    }

    /// Disable new spans without disabling metrics, ordinary logs or inspection.
    pub fn without_traces(mut self) -> Self {
        self.traces = false;
        self
    }

    pub fn without_metrics(mut self) -> Self {
        self.metrics = false;
        self
    }

    /// Exclusions are metric-name substrings, not glob patterns.
    pub fn excluding_metrics(mut self, exclusions: impl IntoIterator<Item = String>) -> Self {
        self.metric_exclusions = exclusions.into_iter().collect();
        self
    }

    /// Validate before constructing components or caching recorder handles.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.traces || self.adapter.is_some(),
            "connected tracing requires a host TraceContextAdapter"
        );
        anyhow::ensure!(
            self.profile != TelemetryProfile::Persistence,
            "the persistence telemetry profile is not implemented in this experiment"
        );
        anyhow::ensure!(
            !self.metric_exclusions.iter().any(String::is_empty),
            "metric exclusion substrings must not be empty"
        );
        Ok(())
    }

    pub(crate) fn metric_enabled(&self, name: &str) -> bool {
        self.metrics
            && !self
                .metric_exclusions
                .iter()
                .any(|excluded| name.contains(excluded))
    }

    pub(crate) fn debug_spans(&self) -> bool {
        self.traces && self.profile == TelemetryProfile::Debug
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_is_off_by_default_and_metrics_do_not_require_tracing() {
        assert!(TelemetrySettings::default().validate().is_ok());
        assert!(TelemetryProfile::Basic.resolve().validate().is_err());
        let metrics = TelemetryProfile::Basic.resolve().without_traces();
        assert!(metrics.validate().is_ok());
        assert!(metrics.metric_enabled("drasi.component.calls"));
        assert!(!metrics.debug_spans());
        let metrics = metrics.excluding_metrics(["component.".to_owned()]);
        assert!(!metrics.metric_enabled("drasi.component.calls"));
        assert!(metrics.metric_enabled("drasi.queue.depth"));
    }
}
