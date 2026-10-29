// Copyright 2026 The Drasi Authors.
// Licensed under the Apache License, Version 2.0.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use opentelemetry_sdk::{
    error::OTelSdkResult,
    trace::{SpanData, SpanExporter},
    Resource,
};

/// Keep background export failures visible even if a later flush succeeds.
#[derive(Clone, Debug, Default)]
pub struct ExportDiagnostics {
    failed_batches: Arc<AtomicU64>,
    exported_spans: Arc<AtomicU64>,
}

impl ExportDiagnostics {
    pub fn check(&self) -> anyhow::Result<u64> {
        let failures = self.failed_batches.load(Ordering::Relaxed);
        anyhow::ensure!(
            failures == 0,
            "{failures} trace export batch(es) failed; see the exporter errors above"
        );
        Ok(self.exported_spans.load(Ordering::Relaxed))
    }

    pub fn observe<E: SpanExporter>(&self, exporter: E) -> ObservedExporter<E> {
        ObservedExporter {
            exporter,
            diagnostics: self.clone(),
        }
    }
}

#[derive(Debug)]
pub struct ObservedExporter<E> {
    exporter: E,
    diagnostics: ExportDiagnostics,
}

impl<E: SpanExporter> SpanExporter for ObservedExporter<E> {
    async fn export(&self, spans: Vec<SpanData>) -> OTelSdkResult {
        let count = spans.len() as u64;
        let result = self.exporter.export(spans).await;
        match &result {
            Ok(()) => {
                self.diagnostics
                    .exported_spans
                    .fetch_add(count, Ordering::Relaxed);
            }
            Err(error) => {
                self.diagnostics
                    .failed_batches
                    .fetch_add(1, Ordering::Relaxed);
                // This executes on the exporter worker, never the graph data path.
                eprintln!("Trace export failed: {error}");
            }
        }
        result
    }

    fn shutdown_with_timeout(&mut self, timeout: Duration) -> OTelSdkResult {
        self.exporter.shutdown_with_timeout(timeout)
    }

    fn force_flush(&mut self) -> OTelSdkResult {
        self.exporter.force_flush()
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.exporter.set_resource(resource);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_sdk::error::OTelSdkError;

    #[derive(Debug)]
    struct FailOnce(AtomicU64);

    impl SpanExporter for FailOnce {
        async fn export(&self, _: Vec<SpanData>) -> OTelSdkResult {
            if self.0.fetch_add(1, Ordering::Relaxed) == 0 {
                Err(OTelSdkError::InternalFailure("test export failure".into()))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn a_successful_later_export_does_not_hide_a_failure() {
        let diagnostics = ExportDiagnostics::default();
        let exporter = diagnostics.observe(FailOnce(AtomicU64::new(0)));
        assert!(exporter.export(Vec::new()).await.is_err());
        assert!(exporter.export(Vec::new()).await.is_ok());
        assert!(diagnostics.check().is_err());
    }
}
