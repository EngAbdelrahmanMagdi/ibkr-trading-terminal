#!/usr/bin/env bash
# Repeat read-only measurements against an explicitly selected disposable stack.
set -euo pipefail
project="${BENCHMARK_PROJECT:?Set the disposable Compose project}"
case "$project" in marketpulse-hardening|marketpulse-resilience-*) ;; *) echo 'Refusing non-disposable project' >&2; exit 2 ;; esac
url="${BENCHMARK_URL:?Set an internal HTTP read endpoint}"
duration="${BENCHMARK_DURATION:-60s}"
clients="${BENCHMARK_CLIENTS:-10}"
container="$(docker ps --filter "label=com.docker.compose.project=$project" --filter label=com.docker.compose.service=realtime-gateway --format '{{.ID}}')"
[[ -n "$container" ]] || { echo 'Gateway is not running' >&2; exit 1; }
echo "Warmup: $clients clients for 10s" >&2
docker exec "$container" api-load -url "$url" -clients "$clients" -duration 10s >/dev/null
for run in 1 2 3; do
  echo "Run $run: $duration, $clients clients" >&2
  docker exec "$container" api-load -url "$url" -clients "$clients" -duration "$duration"
  docker stats --no-stream --format '{{.Name}} CPU={{.CPUPerc}} memory={{.MemUsage}}' "$container" >&2
done
