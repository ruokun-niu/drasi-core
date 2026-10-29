# Five-minute Drasi trace demo with Jaeger

This demo uses the existing [computation runtime example](../computation_runtime.rs):

```text
Application source -> bounded pipe -> query -> bounded pipe -> callback sink
                                      |
                         OpenTelemetry spans from the graph
                                      |
                         host-owned OTLP/HTTP batch exporter
                                      |
                          Jaeger collector + trace UI
```

Jaeger runs locally with in-memory trace storage. No Prometheus, Grafana,
external database, OpenTelemetry Collector, or cloud account is required.
Metrics still print to the example's console log; Jaeger's trace UI does not
display the `metrics` facade's counters or histograms.

## Start and verify

Prerequisites: the repository's Rust toolchain, Python 3, curl, and a running
Podman engine. On macOS, start the existing VM with `podman machine start` if
necessary. Docker can be selected with `CONTAINER_ENGINE=docker`.

From the repository root:

```bash
bash lib/examples/observability-demo/demo.sh run
```

The script:

1. Starts only the dedicated `drasi-observability-jaeger` container, using the
   pinned official `cr.jaegertracing.io/jaegertracing/jaeger:2.22.0` image.
2. Binds Jaeger UI port **16686** and OTLP/HTTP port **4318** to **127.0.0.1 only**.
   Existing unrelated containers are not modified. A port conflict fails explicitly.
3. Runs the workload twice: no artificial sink delay, then a **250 ms** delay.
   Prints the source, query, reaction and actual Cypher text for each scenario.
4. Queries Jaeger's API to verify exact span counts, parentage, the filtered
   branch, sink duration and backpressure. Export failures cause a nonzero exit.
5. Prints direct links to handled, filtered, and backpressured traces.

For example, the slow scenario prints:

```text
Source: orders (NativeApplicationSource; in-memory application input)
Query: orders-query (ContinuousQueryTransformer; Cypher)
Cypher: MATCH (o:Order) WHERE o.active = true RETURN o.name AS name
Reaction: output (NativeApplicationReaction; callback -> in-memory verification channel; sink delay=250 ms; completion=Handled)
```

The summary comes from the Rust example, using the same query text passed to
the query definition. Results stay in the example's in-memory verification
channel; they are not sent to Jaeger or an external destination. The query text
is printed to the console, not added to trace attributes.

## Presenter walkthrough

The quoted text below is a short speaking script. The accompanying tables and
notes provide detail for questions; they do not all need to be presented.

### 0:00 - Show the graph and explain the two signals

Open <http://127.0.0.1:16686>.

Select service **drasi-observability-playground** and span name
**source.publish**, then click **Find Traces**. Use the script's search links
to select a particular run automatically.

Explain: "These are real OpenTelemetry spans from one Drasi computation graph.
The application owns the exporter; the query engine is backend-neutral."

**What is running:** One application source called `orders`, one query called
`orders-query`, and one callback sink called `output`. The two graph pipes each
hold one queued envelope. The application sends synthetic changes; no external
database is involved.

**What Jaeger shows:** Traces connect related operations for an individual piece
of work. Metrics describe counts, rates, occupancy and duration distributions
across work. This presentation uses Jaeger for traces; the example still prints
metrics in its run logs, but there is no Prometheus dashboard in this demo.

### 1:00 - Follow one change

Open the **Handled** link from the fast scenario. Expand the spans and click
individual rows to inspect their attributes:

```text
source.publish
  pipe.send              orders -> orders-query
  component.process      orders-query
    pipe.send            orders-query -> output
    sink.handle          output
```

| Span | What its duration measures | What it does not prove |
|------|----------------------------|------------------------|
| `source.publish` | The publication attempt after an input is available, including forwarding to its bound destination | That the query or sink finished; idle waiting for the next source input is excluded |
| First `pipe.send` | Acceptance into the source-to-query pipe, including any capacity wait | Completed query processing |
| `component.process` | The query's processing call, including work it awaits internally | Pure evaluator CPU time, or subsequent forwarding/handling time |
| Second `pipe.send` | Acceptance into the query-to-sink pipe, including any capacity wait | Completed sink handling |
| `sink.handle` | The callback's handling call, including the artificial delay in the slow scenario | An externally committed or exactly-once effect |

Point out these attributes:

- `component_id`: `orders`, `orders-query`, or `output`.
- `instance_id` and `graph_id`: the owning runtime and graph.
- `operation` and `outcome`: what ran and how that local call ended.
- `completion=handled`: the sink callback completed, rather than merely
  accepting work into another queue.
- `queue_id` on each `pipe.send`: which actual queue is being observed.

The shared trace ID and parent span IDs connect work across asynchronous queues.
All components use the same host service name, so **Services: 1** is expected:
it does not mean only one graph component ran.

Parentage expresses causality, not a synchronous call stack. A downstream span
can outlive its parent; publication/processing spans deliberately do not stay
open while downstream work completes.

### 2:00 - Explain the missing result

Open the **Filtered** link. It has a source publication and query processing,
including the source-to-query pipe send, but no query-output send or sink span.
The input did not match the query's `active = true` condition. "No output" is not
a processing failure.

The [example query and input workload](../computation_runtime.rs) are deliberate:

```cypher
MATCH (o:Order)
WHERE o.active = true
RETURN o.name AS name
```

| Input change | Input data | Change to the query result |
|--------------|------------|----------------------------|
| Insert order `one` | `active=true`, name `First order` | Add a result |
| Update order `one` | `active=true`, name `Updated order` | Update the result |
| Delete order `one` | Remove the previously matching order | Remove the result |
| Insert order `filtered` | **`active=false`**, name `Not selected` | **No result change** |
| Insert order `last` | `active=true`, name `Last order` | Add a result |

The fourth input is constructed using `properties("Not selected", false)`.
The property is not missing: it is explicitly false. `active` is an application
data property, not a switch for telemetry or source health.

The source still publishes that event, and the query still processes it. Since
the result set does not change, there is nothing to send to the sink. Look for
successful outcomes on the source/query operations, rather than interpreting a
shorter trace as lost data. The filtered trace has **three spans** in this demo:
`source.publish`, its `pipe.send`, and `component.process`.

The delete event does produce a result change even though the delete message
does not carry `active=true`: the query removes the result associated with the
previously matching order.

Each run has **5 source changes**, **5 query calls**, **4 handled outputs** and
**1 filtered input**. Result rows are not interchangeable with input counts.
Only `Last order` remains in the final query result.

Say: "We intentionally send one inactive order. The trace proves Drasi received
and processed it, but correctly produced no result because the query selects
only active orders."

### 3:00 - Find a bottleneck

Open the slow scenario's **Handled** and **Backpressure** links.

The workload and query are unchanged. The only injected slowdown is an awaited
**250 ms delay inside each sink callback**. The slow scenario still processes
the same five source changes and four outputs.

Start with `sink.handle`, then inspect the two `pipe.send` spans:

- `sink.handle` takes at least 250 ms because the delay is part of handling.
- A send is quick while its destination queue has capacity.
- A send waits when that queue is full. This wait is backpressure, not extra
  query evaluation.
- `component.process` ends before the query's subsequent forwarding attempt.
  Its elapsed time can include internal query/storage work, so it is still not
  an evaluator CPU-time metric.

#### How pressure moves upstream

```text
Source -> [one queued input] -> Query -> [one queued result] -> Sink
                                                               waits 250 ms
```

1. The sink starts handling a result and waits inside its callback.
2. The query can enqueue another result in the available pipe slot.
3. A further query result cannot be enqueued while that slot is occupied.
   The query's `pipe.send` now waits for the sink to consume the queued result.
4. While forwarding is blocked, the query cannot consume its next source input.
5. The source-to-query queue can fill too. A later source publication must wait
   for that queue to have space, so backpressure becomes visible at the source.

"Capacity one" means one queued envelope per graph pipe, not one event in the
whole pipeline. A sink can be handling one result while another is queued.


### 4:00 - Show the boundary of this POC

**What this demonstrates:**

- One source and a statically linked, in-process computation graph, using actual
  asynchronous graph execution rather than a mock trace.
- Connected trace identity across the graph's supported in-process pipes.
- Successful handling versus valid no-result processing.
- Publication, query processing, forwarding and handling measured at distinct
  boundaries, making downstream backpressure visible.
- Host-owned export to a real OpenTelemetry backend. Jaeger is not embedded in
  the query engine.

**What not to claim:**

- Dynamic-plugin/FFI tracing, durable replay tracing and broader graph topologies
  are not qualified by this demo.
- `Handled` means this callback completed, not an external transaction committed
  or exactly-once effects.
- The displayed trace duration is not a newly implemented, unsampled end-to-end
  latency metric. It is a timeline assembled from this trace's spans.
- This single host's `Services: 1` is not a distributed multi-service demo.
- Framework attributes do not include event payloads or query text; the workload
  table above explains the inputs, but Jaeger is not an event-payload browser.

Close with: "We can follow a change, explain why it produced no result, and
locate a downstream bottleneck without changing the query or its result
semantics. The same host-owned approach can be extended to the remaining
component and plugin boundaries."

## Key code to highlight in computation_runtime.rs

The flow is:

```text
Drasi creates spans -> OpenTelemetry collects them -> exporter sends them -> Jaeger displays them
```

These are simplified excerpts from
[computation_runtime.rs](../computation_runtime.rs), showing only the key wiring.

### 1. Tell OpenTelemetry where to send traces

```rust
let exporter = opentelemetry_otlp::SpanExporter::builder()
    .with_http()
    .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
    .with_endpoint("http://127.0.0.1:4318/v1/traces")
    .build()?;

let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
    .with_batch_exporter(exporter)
    .build();
```

Say: "Send collected spans to Jaeger in background batches."

Port **4318** receives trace data; **16686** is the browser UI.

### 2. Connect Rust tracing to OpenTelemetry

```rust
let subscriber = tracing_subscriber::registry().with(
    tracing_opentelemetry::layer()
        .with_tracer(provider.tracer(SERVICE_NAME)),
);
```

Say: "Collect the spans emitted by our Rust code and pass them to OpenTelemetry."

### 3. Enable observability in Drasi

In `main`, select the profile, supply the context adapter, and run the workload
under the subscriber:

```rust
let telemetry = TelemetryProfile::Debug
    .resolve()
    .with_trace_adapter(Arc::new(telemetry_host::OtelContextAdapter));

run(Some(options.delay), telemetry)
    .with_subscriber(subscriber)
    .await?;
```

Inside `run`, pass those settings into the Drasi builder:

```rust
let drasi = DrasiLib::builder()
    .with_telemetry(telemetry)
    .build()
    .await?;
```

Say: "Enable graph telemetry, keep related operations connected, and collect
their spans using our configured subscriber."

**Key distinction:** Drasi does not receive Jaeger's URL. Drasi emits telemetry;
the example's OpenTelemetry setup decides where it goes.

## Manual runs

```bash
bash lib/examples/observability-demo/demo.sh up

cargo run --quiet -p drasi-lib --example computation_runtime -- \
  --observability debug \
  --otlp-endpoint http://127.0.0.1:4318/v1/traces \
  --run-name manual-fast

cargo run --quiet -p drasi-lib --example computation_runtime -- \
  --observability debug --slow-sink-ms 250 \
  --otlp-endpoint http://127.0.0.1:4318/v1/traces \
  --run-name manual-slow
```

Use fresh run names when repeating manual runs. The endpoint is the complete
HTTP trace URL, including `/v1/traces`, not the Jaeger UI or gRPC address.
In Jaeger's **Attributes** search field, use `drasi.demo.run=manual-fast` or
`drasi.demo.run=manual-slow` to distinguish these two invocations. The script's
generated search links apply the corresponding unique run label automatically.
The demo explicitly sets `service.name`, uses parent-based always-on sampling,
and bounds the HTTP export timeout to five seconds. Exports run on the SDK's
bounded batch worker; shutdown is awaited off the current-thread runtime.
For a readable timeline, this example's OTLP exporter layer selects the graph's
`drasi::telemetry` target. This host filter does not disable component-log capture.

Without `--otlp-endpoint`, the existing stdout exporter remains the default.
The no-argument example and `--observability off` behavior remain available.

## Status and cleanup

```bash
bash lib/examples/observability-demo/demo.sh status
bash lib/examples/observability-demo/demo.sh down
```

`down` stops and removes only the labeled demo container. It does not stop the
Podman VM or unrelated containers. **In-memory traces are lost on stop.**

If Jaeger shows no traces, check the selected service, run tag, and lookback
window. Inspect the printed run logs and `podman logs drasi-observability-jaeger`.
If more than five results appear, select `source.publish` and exactly one run;
otherwise lifecycle traces or multiple invocations may be included. If an old
direct link no longer works after a container restart, rerun the demo and use
the new links.
Do not treat a successful computation as proof of successful telemetry export:
the script checks both exporter errors and received trace contents.

Reference: [Jaeger 2.22 getting started](https://www.jaegertracing.io/docs/2.22/getting-started/).
