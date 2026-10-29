#!/usr/bin/env python3
# Copyright 2026 The Drasi Authors.
# Licensed under the Apache License, Version 2.0.
"""Verify the finite fast/slow workload through Jaeger's query API."""

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request

SERVICE = "drasi-observability-playground"


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def attributes(span):
    return {entry["key"]: entry["value"] for entry in span.get("tags", [])}


def parent(span):
    parents = [ref for ref in span.get("references", []) if ref["refType"] == "CHILD_OF"]
    require(len(parents) <= 1, "span has multiple parents")
    return parents[0]["spanID"] if parents else None


def decode_traces(body):
    """Jaeger v3 streams OTLP JSON messages, not the legacy UI JSON model."""
    traces = {}
    for line in body.splitlines():
        document = json.loads(line)
        require("error" not in document, f"Jaeger query error: {document.get('error')}")
        for resource in document["result"].get("resourceSpans", []):
            def tags(entries):
                return [{"key": entry["key"], "value": next(iter(entry["value"].values()))}
                        for entry in entries]

            resource_tags = tags(resource.get("resource", {}).get("attributes", []))
            service = next((tag["value"] for tag in resource_tags if tag["key"] == "service.name"), None)
            for scope in resource.get("scopeSpans", []):
                for span in scope.get("spans", []):
                    trace_id = span["traceId"]
                    trace = traces.setdefault(trace_id, {"traceID": trace_id, "spans": [], "processes": {}})
                    process_id = str(len(trace["processes"]))
                    trace["processes"][process_id] = {"serviceName": service, "tags": resource_tags}
                    parent_id = span.get("parentSpanId")
                    start = int(span["startTimeUnixNano"])
                    end = int(span["endTimeUnixNano"])
                    require(end >= start, "span ends before it starts")
                    trace["spans"].append({
                        "traceID": trace_id, "spanID": span["spanId"], "operationName": span["name"],
                        "startTime": start, "duration": end - start,
                        "tags": tags(span.get("attributes", [])),
                        "references": [{"refType": "CHILD_OF", "spanID": parent_id}] if parent_id else [],
                    })
    return list(traces.values())


def fetch_traces(base_url, run_name):
    now = datetime.now(timezone.utc)
    query = urllib.parse.urlencode({
        "query.service_name": SERVICE,
        "query.attributes": json.dumps({"drasi.demo.run": run_name}),
        "query.start_time_min": (now - timedelta(hours=1)).isoformat(),
        "query.start_time_max": (now + timedelta(seconds=30)).isoformat(),
        "query.search_depth": 100,
        "query.raw_traces": "true",
    })
    url = f"{base_url}/api/v3/traces?{query}"
    deadline = time.monotonic() + 20
    last_error = "no complete source traces received"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as response:
                traces = decode_traces(response.read().decode("utf-8"))
            counts = Counter(span["operationName"] for trace in traces for span in trace["spans"])
            if (counts["source.publish"], counts["component.process"], counts["sink.handle"], counts["pipe.send"]) == (5, 5, 4, 9):
                return traces
            last_error = f"incomplete or unexpected span counts: {dict(counts)}"
        except (urllib.error.URLError, TimeoutError) as error:
            last_error = str(error)
        time.sleep(0.25)
    raise RuntimeError(f"{run_name}: {last_error}; inspect {url}")


def verify(base_url, run_name, delay_ms):
    traces = fetch_traces(base_url, run_name)
    source_traces = [trace for trace in traces if any(
        span["operationName"] == "source.publish" for span in trace["spans"]
    )]
    require(len(source_traces) == 5, f"{run_name}: expected five separate source-change traces")
    durations = []
    forwarding = []
    filtered = []
    handled = []
    for trace in source_traces:
        spans = trace["spans"]
        require(all(span["traceID"] == trace["traceID"] for span in spans), "trace ID changed across a delivery")
        for process in trace["processes"].values():
            tags = {entry["key"]: entry["value"] for entry in process.get("tags", [])}
            require(process["serviceName"] == SERVICE, "wrong host service identity")
            require(tags.get("drasi.demo.run") == run_name, "mixed demo runs")
        sources = [span for span in spans if span["operationName"] == "source.publish"]
        queries = [span for span in spans if span["operationName"] == "component.process"]
        sinks = [span for span in spans if span["operationName"] == "sink.handle"]
        sends = [span for span in spans if span["operationName"] == "pipe.send"]
        require(len(sources) == 1 and len(queries) == 1 and len(sinks) <= 1, "unexpected pipeline shape")
        source, query = sources[0], queries[0]
        require(parent(source) is None, "source publication must be a root")
        require(parent(query) == source["spanID"], "query lost its source parent")
        require(attributes(source).get("component_id") == "orders", "wrong source identity")
        require(attributes(query).get("component_id") == "orders-query", "wrong query identity")
        for span in [source, query, *sinks, *sends]:
            require(attributes(span).get("outcome") == "success", f"unsuccessful {span['operationName']}")
        require(len(sends) == 1 + len(sinks), "unexpected physical pipe-send count")
        for send in sends:
            require(parent(send) in {source["spanID"], query["spanID"]}, "pipe send has wrong parent")
            if parent(send) == query["spanID"]:
                require(query["startTime"] + query["duration"] <= send["startTime"],
                        "query processing includes subsequent forwarding")
            forwarding.append((send["duration"], trace["traceID"]))
        if sinks:
            sink = sinks[0]
            require(parent(sink) == query["spanID"], "sink lost its query parent")
            require(attributes(sink).get("completion") == "handled", "sink only accepted work")
            require(attributes(sink).get("component_id") == "output", "wrong sink identity")
            require(sink["duration"] >= delay_ms * 1_000_000, "sink duration excludes injected delay")
            durations.append(sink["duration"])
            handled.append(trace["traceID"])
        else:
            filtered.append(trace["traceID"])
    require(len(durations) == 4 and len(filtered) == 1, "filtering must end exactly one trace without a sink")
    longest_send, blocked_trace = max(forwarding)
    if delay_ms:
        require(longest_send >= delay_ms * 500_000, "slow sink did not produce visible pipe backpressure")
    median_ms = statistics.median(durations) / 1_000_000
    print(f"\n{run_name}: PASS -- 5 publications, 5 query calls, 4 handled results, 1 filtered input")
    print(f"  sink median: {median_ms:.3f} ms; longest pipe.send: {longest_send / 1_000_000:.3f} ms")
    search = urllib.parse.urlencode({
        "service": SERVICE, "operation": "source.publish", "lookback": "1h",
        "tags": json.dumps({"drasi.demo.run": run_name}),
    })
    print(f"  Search:       {base_url}/search?{search}")
    print(f"  Handled:      {base_url}/trace/{handled[0]}")
    print(f"  Filtered:     {base_url}/trace/{filtered[0]}")
    if delay_ms:
        print(f"  Backpressure: {base_url}/trace/{blocked_trace}")
    return median_ms


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("jaeger_url")
    parser.add_argument("fast_run")
    parser.add_argument("slow_run")
    args = parser.parse_args()
    url = args.jaeger_url.rstrip("/")
    fast = verify(url, args.fast_run, 0)
    slow = verify(url, args.slow_run, 250)
    require(slow > fast, "slow sink must be slower than baseline; rerun if the host is overloaded")
    print("\nConnected parentage, filtering, injected sink latency and pipe backpressure verified.")


if __name__ == "__main__":
    main()
