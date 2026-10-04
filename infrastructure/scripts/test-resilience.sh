#!/usr/bin/env bash
# Bounded disruption checks against an explicitly named disposable MOCK stack only.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export MSYS_NO_PATHCONV=1
env_file="${RESILIENCE_ENV_FILE:?Set RESILIENCE_ENV_FILE to a disposable environment file}"
override="${RESILIENCE_COMPOSE_OVERRIDE:?Set RESILIENCE_COMPOSE_OVERRIDE to its image/secret override}"
while IFS='=' read -r key value; do
  [[ "$key" =~ ^[A-Z][A-Z0-9_]*$ ]] && printf -v "$key" '%s' "$value"
done < <(grep -E '^[A-Z][A-Z0-9_]*=' "$env_file")
project="${COMPOSE_PROJECT_NAME:?Missing disposable project name}"
case "$project" in marketpulse-hardening|marketpulse-resilience-*) ;; *) echo 'Refusing non-disposable project' >&2; exit 2 ;; esac
compose() { docker compose -p "$project" --env-file "$env_file" -f docker-compose.yml -f "$override" -f infrastructure/testing/compose.resilience.yml --profile gateway --profile core --profile ai --profile web "$@"; }
cid() { compose ps -q "$1"; }
core="http://127.0.0.1:${TRADING_CORE_HOST_PORT:?}"
management="http://127.0.0.1:${TRADING_CORE_MANAGEMENT_HOST_PORT:?}"
gateway="http://127.0.0.1:${GATEWAY_HOST_PORT:?}"
status() { curl -s --max-time 5 -o /dev/null -w '%{http_code}' "$1" || true; }
wait_status() { for ((attempt=0; attempt<45; attempt++)); do [[ "$(status "$1")" == "$2" ]] && return 0; sleep 2; done; echo "Recovery deadline: $1" >&2; return 1; }
restore() { compose up -d --no-deps postgres redis kafka realtime-gateway trading-core ai-insights >/dev/null; }
trap restore EXIT
compose config --format json | docker run --rm -i python:3.14.7-slim python -c 'import json,sys; s=json.load(sys.stdin)["services"]; assert s["trading-core"]["environment"]["APP_RUNTIME_MODE"] == "MOCK"; assert s["ai-insights"]["environment"]["AI_PROVIDER"] == "FIXTURE"'
wait_status "$management/actuator/health/readiness" 200
echo 'PostgreSQL outage: readiness and false-success protection'
compose stop postgres >/dev/null
wait_status "$management/actuator/health/readiness" 503
[[ "$(status "$core/api/v1/portfolio")" == 503 ]]
compose start postgres >/dev/null
wait_status "$management/actuator/health/readiness" 200
echo 'Redis outage: bars remain available through disposable-cache fallback'
compose stop redis >/dev/null
[[ "$(status "$gateway/api/v1/market/bars?symbol=NVDA&interval=1m&range=1d")" == 200 ]]
compose start redis >/dev/null
echo 'Kafka outage: Core remains ready, worker loses readiness and recovers'
compose stop kafka >/dev/null
[[ "$(status "$management/actuator/health/readiness")" == 200 ]]
sleep 12
docker exec "$(cid ai-insights)" python -c 'import urllib.request,urllib.error; r=None
try: urllib.request.urlopen("http://127.0.0.1:8092/readiness", timeout=3)
except urllib.error.HTTPError as e: r=e.code
except urllib.error.URLError: r=503
assert r == 503'
compose start kafka >/dev/null
for ((attempt=0; attempt<60; attempt++)); do
  if docker exec "$(cid ai-insights)" python -c 'import urllib.request; assert urllib.request.urlopen("http://127.0.0.1:8092/readiness",timeout=3).status == 200' 2>/dev/null; then break; fi
  sleep 2
done
[[ "$attempt" -lt 60 ]]
echo 'Gateway termination: unavailable API and bounded restart'
compose kill realtime-gateway >/dev/null
compose start realtime-gateway >/dev/null
wait_status "$gateway/api/v1/market/bars?symbol=NVDA&interval=1m&range=1d" 200
echo 'Core termination: durable state remains available after restart'
before="$(curl -fsS --max-time 5 "$core/api/v1/executions")"
compose kill trading-core >/dev/null
compose start trading-core >/dev/null
wait_status "$management/actuator/health/readiness" 200
after="$(curl -fsS --max-time 5 "$core/api/v1/executions")"
[[ "$before" == "$after" ]]
echo 'Worker network isolation: trading routes and direct Internet denied'
docker exec "$(cid ai-insights)" python -c 'import socket
for host,port in [("trading-core",8080),("realtime-gateway",8090),("postgres",5432),("redis",6379),("1.1.1.1",443),("2606:4700:4700::1111",443)]:
    try: c=socket.create_connection((host,port),timeout=2)
    except OSError: continue
    c.close(); raise AssertionError("Unexpected route")'
echo 'Disposable stack recovery checks passed. Broker-call/duplicate/transaction failures use deterministic integration suites.'
