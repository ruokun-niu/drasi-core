// Copyright 2026 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");

//! POC verifying plugin span + metric propagation across a (simulated) FFI
//! boundary.
//!
//! This validates the §7 design claim that:
//! - The host can pass its current `trace_id` + `parent_span_id` to a plugin via
//!   an `#[repr(C)]` struct.
//! - The plugin (with its own independent OpenTelemetry tracer / span processor)
//!   can create spans whose `trace_id` matches the host's, and whose
//!   `parent_span_id` is the host span the plugin was invoked from.
//! - When a plugin span ends, its data can cross the FFI boundary as a flat
//!   `#[repr(C)]` struct (`FfiCompletedSpan`) and the host can ingest it into
//!   its own collection / export pipeline.
//! - The plugin can emit metrics (counters / histograms) that cross the FFI
//!   boundary as a flat `#[repr(C)]` struct (`FfiMetricEntry`) and land in
//!   the host's `metrics::Recorder`, exactly as if they had been emitted
//!   in-process.
//!
//! The "FFI" here is simulated in-process: the plugin and host each have their
//! own `opentelemetry_sdk::trace::TracerProvider`, communicating only through
//! `extern "C"` function pointers and `#[repr(C)]` structs — exactly what a
//! real cdylib boundary would look like. No `tracing` integration is used; the
//! interesting verification is at the OTel data layer.
//!
//! Run: `cargo run -p ffi-tracing-poc`

use std::ffi::{c_char, c_void, CString};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use opentelemetry::trace::{
    Span as _, SpanContext, SpanId, SpanKind, TraceContextExt, TraceFlags, TraceId, TraceState,
    Tracer, TracerProvider as _,
};
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::export::trace::{ExportResult, SpanData, SpanExporter};
use opentelemetry_sdk::trace as sdktrace;

// =====================================================================
//  FFI types — these would live in `ffi-primitives` in the real design.
// =====================================================================

/// Trace context the host injects into plugin calls (matches §7 design).
#[repr(C)]
#[derive(Clone, Copy)]
struct FfiTraceContext {
    trace_id: [u8; 16],
    parent_span_id: [u8; 8],
    /// Bit 0 = sampled
    trace_flags: u8,
}

/// Completed span shipped from plugin → host via callback (matches §7 design).
#[repr(C)]
struct FfiCompletedSpan {
    name: *const c_char,
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_span_id: [u8; 8],
    start_time_ns: u64,
    end_time_ns: u64,
}

/// Host-provided callback. Invoked by the plugin's span exporter when a span
/// closes. `ctx` is opaque host context (here: a `*mut HostState`).
type SpanCallbackFn = extern "C" fn(ctx: *mut c_void, span: *const FfiCompletedSpan);

/// Kind of metric being emitted across the FFI boundary.
/// Mirrors the relevant subset of `metrics::Recorder` operations.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FfiMetricKind {
    CounterIncrement = 0,
    HistogramRecord = 1,
    GaugeSet = 2,
}

/// Flat metric event shipped from plugin → host (matches §7 design).
///
/// Labels are intentionally omitted in this POC; in the real design they would
/// be a flat `*const FfiKv` + `len` pair owned by the plugin for the call's
/// duration. The mechanism for `name` here demonstrates the same ownership
/// rule: pointer is valid only for the duration of the callback.
#[repr(C)]
struct FfiMetricEntry {
    name: *const c_char,
    kind: FfiMetricKind,
    /// For CounterIncrement: the integer increment, cast to f64.
    /// For HistogramRecord / GaugeSet: the observation value.
    value: f64,
}

/// Host-provided callback. Invoked by the plugin every time it emits a
/// metric. `ctx` is opaque host context (here: a `*mut HostState`).
type MetricsCallbackFn = extern "C" fn(ctx: *mut c_void, entry: *const FfiMetricEntry);

// =====================================================================
//  HOST side
// =====================================================================

/// Everything the host accumulates: its own OTel spans plus plugin spans
/// that arrived over the FFI callback. In a real design this would feed an
/// OTLP exporter; here we just collect for assertion.
struct HostState {
    /// Spans exported by the host's own TracerProvider.
    own_spans: Mutex<Vec<SpanData>>,
    /// Plugin spans received via FFI callback, captured as plain bytes so
    /// we can assert IDs without depending on host-side OTel API shape.
    plugin_spans: Mutex<Vec<CollectedPluginSpan>>,
    /// Plugin metric events received via FFI callback (raw, before being
    /// re-emitted into the host's metrics recorder). Kept for assertion.
    plugin_metrics: Mutex<Vec<CollectedPluginMetric>>,
}

#[derive(Debug, Clone)]
struct CollectedPluginSpan {
    name: String,
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_span_id: [u8; 8],
}

#[derive(Debug, Clone)]
struct CollectedPluginMetric {
    name: String,
    kind: FfiMetricKind,
    value: f64,
}

/// Custom in-memory exporter: the host's TracerProvider pushes ended spans here.
#[derive(Debug)]
struct HostInMemoryExporter {
    sink: Arc<Mutex<Vec<SpanData>>>,
}

impl SpanExporter for HostInMemoryExporter {
    fn export(
        &mut self,
        batch: Vec<SpanData>,
    ) -> futures::future::BoxFuture<'static, ExportResult> {
        let sink = self.sink.clone();
        Box::pin(async move {
            sink.lock().unwrap().extend(batch);
            Ok(())
        })
    }
}

/// The host-provided callback for plugin spans.
extern "C" fn host_span_callback(ctx: *mut c_void, span: *const FfiCompletedSpan) {
    // SAFETY: `ctx` is the `*mut HostState` we registered when calling into the
    // plugin; `span` is valid for the duration of this call (the plugin must
    // not free its memory until we return).
    assert!(!ctx.is_null());
    assert!(!span.is_null());
    let host_state = unsafe { &*(ctx as *const HostState) };
    let s = unsafe { &*span };
    let name = unsafe {
        std::ffi::CStr::from_ptr(s.name)
            .to_string_lossy()
            .into_owned()
    };
    host_state.plugin_spans.lock().unwrap().push(CollectedPluginSpan {
        name,
        trace_id: s.trace_id,
        span_id: s.span_id,
        parent_span_id: s.parent_span_id,
    });
}

/// The host-provided callback for plugin metric emissions.
///
/// This is the host-side bridge: a plugin metric event arrives as a flat FFI
/// struct, we copy out the name, record it for assertion, and then re-emit it
/// via the `metrics` facade so it lands in *whatever* recorder the host
/// installed (here: `metrics_util::debugging::DebuggingRecorder`). This is
/// exactly the §7 design contract — plugin metrics show up in host metrics
/// pipelines indistinguishably from in-process metrics.
extern "C" fn host_metric_callback(ctx: *mut c_void, entry: *const FfiMetricEntry) {
    assert!(!ctx.is_null());
    assert!(!entry.is_null());
    let host_state = unsafe { &*(ctx as *const HostState) };
    let e = unsafe { &*entry };
    let name = unsafe {
        std::ffi::CStr::from_ptr(e.name)
            .to_string_lossy()
            .into_owned()
    };

    host_state.plugin_metrics.lock().unwrap().push(CollectedPluginMetric {
        name: name.clone(),
        kind: e.kind,
        value: e.value,
    });

    // Re-emit on the host's `metrics` recorder. We must use 'static names with
    // the macros, so we leak the name string — acceptable for this POC; in the
    // real design the host would call `Recorder::register_*` + `Key::from_*`
    // directly to avoid leaks. Labels are omitted here.
    let static_name: &'static str = Box::leak(name.into_boxed_str());
    match e.kind {
        FfiMetricKind::CounterIncrement => {
            metrics::counter!(static_name).increment(e.value as u64);
        }
        FfiMetricKind::HistogramRecord => {
            metrics::histogram!(static_name).record(e.value);
        }
        FfiMetricKind::GaugeSet => {
            metrics::gauge!(static_name).set(e.value);
        }
    }
}

// =====================================================================
//  PLUGIN side — in a real design this is in a separate cdylib.
// =====================================================================

/// Send-safe holder for the host's opaque pointer. The pointer itself is
/// `*mut c_void` (not Send) but we know the value behind it (`HostState`) is
/// `Sync` and lives long enough — that's the FFI contract.
#[derive(Clone, Copy)]
struct HostCtxPtr(*mut c_void);
unsafe impl Send for HostCtxPtr {}
unsafe impl Sync for HostCtxPtr {}

/// SpanExporter installed in the plugin's TracerProvider. On each `export`
/// call it flattens the OTel `SpanData` to `FfiCompletedSpan` and invokes the
/// host's callback. This is the "bridge" the §7 design relies on.
#[derive(Debug)]
struct PluginFfiExporter {
    host_ctx: HostCtxPtr,
    callback: SpanCallbackFn,
}

impl std::fmt::Debug for HostCtxPtr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HostCtxPtr({:p})", self.0)
    }
}

impl SpanExporter for PluginFfiExporter {
    fn export(
        &mut self,
        batch: Vec<SpanData>,
    ) -> futures::future::BoxFuture<'static, ExportResult> {
        // Carry the host pointer through the async state machine as a usize
        // so the future is Send (raw pointers are not Send).
        let host_ctx_addr: usize = self.host_ctx.0 as usize;
        let callback = self.callback;
        Box::pin(async move {
            let raw_ctx: *mut c_void = host_ctx_addr as *mut c_void;
            for span in batch {
                let name_c = match CString::new(span.name.into_owned()) {
                    Ok(c) => c,
                    Err(_) => continue,
                };
                let start_ns = system_time_to_ns(span.start_time);
                let end_ns = system_time_to_ns(span.end_time);
                let ffi_span = FfiCompletedSpan {
                    name: name_c.as_ptr(),
                    trace_id: span.span_context.trace_id().to_bytes(),
                    span_id: span.span_context.span_id().to_bytes(),
                    parent_span_id: span.parent_span_id.to_bytes(),
                    start_time_ns: start_ns,
                    end_time_ns: end_ns,
                };
                callback(raw_ctx, &ffi_span);
                // name_c dropped here AFTER callback returns — matches the
                // ownership contract in §7 ("pointers valid for the duration
                // of the callback; host must copy").
                drop(name_c);
            }
            Ok(())
        })
    }
}

/// Plugin-side metrics emitter. Plugin code calls these helpers; each call
/// builds an `FfiMetricEntry` and invokes the host callback synchronously.
/// In a real cdylib plugin this would be wired up as a `metrics::Recorder`
/// implementation (`PluginFfiRecorder`) so plugin code could keep using the
/// `metrics::counter!`/`histogram!` macros unmodified.
struct PluginMetrics {
    host_ctx: HostCtxPtr,
    callback: MetricsCallbackFn,
}

impl PluginMetrics {
    fn emit(&self, name: &str, kind: FfiMetricKind, value: f64) {
        let name_c = match CString::new(name) {
            Ok(c) => c,
            Err(_) => return,
        };
        let entry = FfiMetricEntry {
            name: name_c.as_ptr(),
            kind,
            value,
        };
        (self.callback)(self.host_ctx.0, &entry);
        drop(name_c);
    }

    fn counter_increment(&self, name: &str, by: u64) {
        self.emit(name, FfiMetricKind::CounterIncrement, by as f64);
    }

    fn histogram_record(&self, name: &str, value: f64) {
        self.emit(name, FfiMetricKind::HistogramRecord, value);
    }
}

/// Plugin module: holds its own TracerProvider (a separate world from the
/// host) and a metrics handle pointing at the host's metrics callback.
struct PluginRuntime {
    tracer_provider: sdktrace::TracerProvider,
    metrics: PluginMetrics,
}

impl PluginRuntime {
    fn new(
        host_ctx: *mut c_void,
        span_callback: SpanCallbackFn,
        metrics_callback: MetricsCallbackFn,
    ) -> Self {
        let exporter = PluginFfiExporter {
            host_ctx: HostCtxPtr(host_ctx),
            callback: span_callback,
        };
        // SimpleSpanProcessor flushes synchronously on span end — best for a
        // deterministic POC.
        let tracer_provider = sdktrace::TracerProvider::builder()
            .with_simple_exporter(exporter)
            .build();
        let metrics = PluginMetrics {
            host_ctx: HostCtxPtr(host_ctx),
            callback: metrics_callback,
        };
        Self {
            tracer_provider,
            metrics,
        }
    }

    /// "Plugin work" entry point. Mimics the per-call FFI shape from §7:
    /// the host hands the plugin its current trace context, the plugin opens
    /// a span as a child, does work, emits a couple of metrics, span closes,
    /// exporter ships it back.
    fn do_work(&self, parent_ctx: &FfiTraceContext) {
        let tracer = self.tracer_provider.tracer("ffi-poc-plugin");

        // Reconstruct an OTel parent context from the FFI struct.
        let parent_sc = SpanContext::new(
            TraceId::from_bytes(parent_ctx.trace_id),
            SpanId::from_bytes(parent_ctx.parent_span_id),
            TraceFlags::new(parent_ctx.trace_flags),
            /* is_remote = */ true,
            TraceState::default(),
        );
        let otel_parent = Context::new().with_remote_span_context(parent_sc);

        // This is the span that should appear under the host's trace tree.
        let _span = tracer
            .span_builder("plugin.work")
            .with_kind(SpanKind::Internal)
            .with_attributes(vec![KeyValue::new("plugin.kind", "ffi-poc")])
            .start_with_context(&tracer, &otel_parent);

        // Emit two metrics across the FFI boundary while the plugin span is
        // active. These would, in a real plugin, come from `metrics::counter!`
        // / `metrics::histogram!` invocations routed through a plugin-side
        // `metrics::Recorder` proxy.
        self.metrics
            .counter_increment("drasi.plugin.events_processed", 1);
        self.metrics
            .histogram_record("drasi.plugin.work_duration_ns", 12_345.0);

        // span dropped here -> ends -> processor exports -> FFI callback fires.
    }

    fn shutdown(self) {
        // Force flush so all spans are exported before we assert; the
        // SimpleSpanProcessor flushes synchronously, but we still call
        // force_flush for symmetry with what a real plugin would do.
        let _ = self.tracer_provider.force_flush();
        // Drop the provider; SimpleSpanProcessor releases its exporter.
        drop(self.tracer_provider);
    }
}

// =====================================================================
//  Helpers
// =====================================================================

fn system_time_to_ns(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

// =====================================================================
//  Main: wires host + plugin and asserts trace continuity.
// =====================================================================

fn main() -> anyhow::Result<()> {
    println!("=========================================================");
    println!(" drasi-lib FFI tracing + metrics bridge POC");
    println!("=========================================================\n");

    // ---- Host metrics recorder (its own world) ---------------------
    // `DebuggingRecorder` captures every metric op so we can assert on them.
    // This is the same recorder type we use in the in-process slices.
    let metrics_recorder = metrics_util::debugging::DebuggingRecorder::new();
    let metrics_snapshotter = metrics_recorder.snapshotter();
    metrics_recorder
        .install()
        .expect("install metrics recorder");

    // ---- Host TracerProvider (its own world) ------------------------
    let host_sink: Arc<Mutex<Vec<SpanData>>> = Arc::new(Mutex::new(Vec::new()));
    let host_provider = sdktrace::TracerProvider::builder()
        .with_simple_exporter(HostInMemoryExporter {
            sink: host_sink.clone(),
        })
        .build();
    let host_tracer = host_provider.tracer("ffi-poc-host");

    let host_state = Arc::new(HostState {
        own_spans: Mutex::new(Vec::new()),
        plugin_spans: Mutex::new(Vec::new()),
        plugin_metrics: Mutex::new(Vec::new()),
    });

    // ---- Spin up plugin "world" with our callbacks wired in --------
    let plugin = PluginRuntime::new(
        Arc::as_ptr(&host_state) as *mut c_void,
        host_span_callback,
        host_metric_callback,
    );

    // ---- Open a host span; capture its IDs into FfiTraceContext -----
    let host_span = host_tracer
        .span_builder("host.dispatch")
        .with_kind(SpanKind::Internal)
        .start(&host_tracer);
    let host_sc = host_span.span_context().clone();
    println!(
        "host span:    trace_id={}, span_id={}",
        hex(&host_sc.trace_id().to_bytes()),
        hex(&host_sc.span_id().to_bytes())
    );

    let ffi_ctx = FfiTraceContext {
        trace_id: host_sc.trace_id().to_bytes(),
        parent_span_id: host_sc.span_id().to_bytes(),
        trace_flags: host_sc.trace_flags().to_u8(),
    };

    // ---- Cross the FFI boundary: host -> plugin ---------------------
    plugin.do_work(&ffi_ctx);

    // ---- End host span and shut everything down ---------------------
    drop(host_span);
    plugin.shutdown();
    let _ = host_provider.force_flush();
    drop(host_provider);

    // ---- Verification ----------------------------------------------
    let plugin_spans = host_state.plugin_spans.lock().unwrap().clone();
    let host_spans = host_sink.lock().unwrap().clone();

    println!("\nhost spans collected:");
    for s in &host_spans {
        println!(
            "  - name={:?}  trace_id={}  span_id={}  parent={}",
            s.name,
            hex(&s.span_context.trace_id().to_bytes()),
            hex(&s.span_context.span_id().to_bytes()),
            hex(&s.parent_span_id.to_bytes())
        );
    }
    println!("plugin spans received via FFI:");
    for s in &plugin_spans {
        println!(
            "  - name={}  trace_id={}  span_id={}  parent={}",
            s.name,
            hex(&s.trace_id),
            hex(&s.span_id),
            hex(&s.parent_span_id)
        );
    }

    // --- Assertions ---
    println!("\nAssertions:");

    // 1. Plugin emitted exactly one span via the FFI bridge.
    assert_eq!(plugin_spans.len(), 1, "expected 1 plugin span via FFI");
    println!("  OK — plugin emitted 1 span via FFI callback.");

    // 2. Plugin span's trace_id matches host span's trace_id.
    let p = &plugin_spans[0];
    assert_eq!(
        p.trace_id,
        host_sc.trace_id().to_bytes(),
        "plugin trace_id should match host trace_id"
    );
    println!("  OK — plugin span's trace_id matches host span's trace_id.");

    // 3. Plugin span's parent_span_id == host span's span_id.
    assert_eq!(
        p.parent_span_id,
        host_sc.span_id().to_bytes(),
        "plugin parent_span_id should be host span_id"
    );
    println!("  OK — plugin span's parent_span_id == host span's span_id.");

    // 4. Plugin span's span_id is non-zero and distinct from the host's.
    assert_ne!(p.span_id, [0u8; 8], "plugin span_id should be non-zero");
    assert_ne!(
        p.span_id,
        host_sc.span_id().to_bytes(),
        "plugin span_id should differ from host span_id"
    );
    println!("  OK — plugin span has its own non-zero span_id.");

    // 5. Host's own exporter saw the host span.
    let host_match = host_spans.iter().any(|s| {
        s.span_context.trace_id().to_bytes() == host_sc.trace_id().to_bytes()
            && s.span_context.span_id().to_bytes() == host_sc.span_id().to_bytes()
    });
    assert!(host_match, "host span not found in host exporter");
    println!("  OK — host span exported through host's own pipeline.");

    // ---- Metrics verification --------------------------------------
    let plugin_metrics = host_state.plugin_metrics.lock().unwrap().clone();
    println!("\nplugin metrics received via FFI:");
    for m in &plugin_metrics {
        println!("  - name={}  kind={:?}  value={}", m.name, m.kind, m.value);
    }

    // 6. Plugin emitted exactly the two metrics we expected via FFI.
    assert_eq!(plugin_metrics.len(), 2, "expected 2 plugin metric events");
    assert!(plugin_metrics.iter().any(|m| m.name
        == "drasi.plugin.events_processed"
        && m.kind == FfiMetricKind::CounterIncrement
        && (m.value - 1.0).abs() < f64::EPSILON));
    assert!(plugin_metrics.iter().any(|m| m.name
        == "drasi.plugin.work_duration_ns"
        && m.kind == FfiMetricKind::HistogramRecord
        && (m.value - 12_345.0).abs() < f64::EPSILON));
    println!("  OK — plugin emitted counter + histogram via FFI callback.");

    // 7. Re-emitted metrics show up in the host's `DebuggingRecorder` snapshot.
    let snapshot = metrics_snapshotter.snapshot().into_vec();
    let mut counter_seen = false;
    let mut histogram_seen = false;
    println!("\nhost metrics snapshot (post-bridge):");
    for (key, _unit, _desc, value) in &snapshot {
        let name = key.key().name();
        println!("  - {} => {:?}", name, value);
        if name == "drasi.plugin.events_processed" {
            if let metrics_util::debugging::DebugValue::Counter(v) = value {
                assert_eq!(*v, 1, "counter should have been incremented by 1");
                counter_seen = true;
            }
        }
        if name == "drasi.plugin.work_duration_ns" {
            if let metrics_util::debugging::DebugValue::Histogram(samples) = value {
                assert!(
                    samples.iter().any(|s| (s.into_inner() - 12_345.0).abs() < f64::EPSILON),
                    "histogram should contain the recorded sample"
                );
                histogram_seen = true;
            }
        }
    }
    assert!(counter_seen, "plugin counter not seen in host snapshot");
    assert!(histogram_seen, "plugin histogram not seen in host snapshot");
    println!("  OK — plugin metrics landed in host's metrics recorder.");

    println!("\nAll assertions passed. The FFI trace + metrics bridge in §7");
    println!("works at the OTel / metrics data layer: plugin spans inherit");
    println!("the host's trace_id/parent_span_id, and plugin metric calls");
    println!("land in the host's metrics::Recorder, all via flat #[repr(C)]");
    println!("structs and extern \"C\" callbacks.");

    Ok(())
}
