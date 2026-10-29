#!/usr/bin/env bash
# Copyright 2026 The Drasi Authors.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$directory/../../.." && pwd)"
engine="${CONTAINER_ENGINE:-podman}"
name="drasi-observability-jaeger"
image="cr.jaegertracing.io/jaegertracing/jaeger:2.22.0"
label="io.drasi.observability-demo"
ui="http://127.0.0.1:16686"
endpoint="http://127.0.0.1:4318/v1/traces"

case "$engine" in
    podman|docker) ;;
    *) echo "CONTAINER_ENGINE must be podman or docker" >&2; exit 1 ;;
esac
command -v "$engine" >/dev/null
command -v curl >/dev/null
if ! "$engine" info >/dev/null; then
    echo "Start your container engine first (on macOS: podman machine start)." >&2
    exit 1
fi

exists() {
    "$engine" container inspect "$name" >/dev/null 2>&1
}

check_owner() {
    local owner
    owner="$("$engine" inspect --format "{{ index .Config.Labels \"$label\" }}" "$name")"
    if [[ "$owner" != "jaeger" ]]; then
        echo "Container $name exists but is not owned by this demo; leaving it untouched." >&2
        exit 1
    fi
}

up() {
    if exists; then
        check_owner
        if [[ "$("$engine" inspect --format '{{.State.Running}}' "$name")" != "true" ]]; then
            "$engine" start "$name"
        fi
    else
        "$engine" run --detach --name "$name" \
            --label "$label=jaeger" \
            --publish 127.0.0.1:16686:16686 \
            --publish 127.0.0.1:4318:4318 \
            "$image"
    fi
    for ((attempt = 0; attempt < 60; attempt++)); do
        if curl --fail --silent --max-time 2 "$ui/api/v3/services" >/dev/null; then
            echo "Jaeger is ready: $ui"
            return
        fi
        sleep 1
    done
    "$engine" logs --tail 30 "$name" >&2
    echo "Jaeger did not become ready; inspect with: $engine logs $name" >&2
    exit 1
}

case "${1:-run}" in
    up)
        up
        ;;
    run)
        command -v cargo >/dev/null
        command -v python3 >/dev/null
        up
        cd "$root"
        run_id="$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
        log_dir="$root/target/observability-demo/$run_id"
        mkdir -p "$log_dir"
        for scenario in fast slow; do
            delay=0
            if [[ "$scenario" == "slow" ]]; then delay=250; fi
            echo "Running $scenario scenario (sink delay ${delay}ms)..."
            if ! cargo run --quiet -p drasi-lib --example computation_runtime -- \
                --observability debug --slow-sink-ms "$delay" \
                --otlp-endpoint "$endpoint" --run-name "$scenario-$run_id" \
                >"$log_dir/$scenario.log" 2>&1; then
                tail -n 40 "$log_dir/$scenario.log" >&2
                echo "Scenario failed. Full log: $log_dir/$scenario.log" >&2
                exit 1
            fi
            if ! grep -E '^(Source|Query|Cypher|Reaction): ' "$log_dir/$scenario.log"; then
                echo "Missing graph summary in $log_dir/$scenario.log" >&2
                exit 1
            fi
        done
        python3 "$directory/verify.py" "$ui" "fast-$run_id" "slow-$run_id"
        echo "Console metrics and run logs: $log_dir"
        echo "Jaeger remains available. Stop only this demo with: bash lib/examples/observability-demo/demo.sh down"
        ;;
    down)
        if exists; then
            check_owner
            "$engine" stop "$name"
            "$engine" rm "$name"
        else
            echo "No demo container to remove."
        fi
        ;;
    status)
        if exists; then
            check_owner
            "$engine" inspect --format '{{.Name}} running={{.State.Running}}' "$name"
            curl --fail --silent --show-error --max-time 5 "$ui/api/v3/services"
            echo
        else
            echo "Demo container is not running." >&2
            exit 1
        fi
        ;;
    *)
        echo "usage: bash lib/examples/observability-demo/demo.sh [up|run|status|down]" >&2
        exit 1
        ;;
esac
