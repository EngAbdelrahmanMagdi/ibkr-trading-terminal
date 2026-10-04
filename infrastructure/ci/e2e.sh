#!/usr/bin/env bash
# An ephemeral runner never shares owner data or credentials.
set -euo pipefail
[[ "${GITHUB_ACTIONS:-}" == true ]] || { echo 'CI-only disposable runner' >&2; exit 2; }
[[ "${GITHUB_RUN_ID:-}" =~ ^[0-9]+$ && "${GITHUB_RUN_ATTEMPT:-}" =~ ^[0-9]+$ ]] || exit 2
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export COMPOSE_PROJECT_NAME="marketpulse-ci-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
compose() { docker compose --profile gateway --profile core --profile ai --profile web "$@"; }
cleanup() {
  result=$?
  if [ -n "${probe_pid:-}" ]; then kill "$probe_pid" 2>/dev/null || true; fi
  if [ "$result" -ne 0 ]; then
    mkdir -p .artifacts/e2e
    compose ps --all --format json >.artifacts/e2e/compose-state.json 2>/dev/null || true
    for service in realtime-gateway trading-core postgres redis kafka ai-insights web; do
      container="$(compose ps --all -q "$service")"
      [ -n "$container" ] || continue
      docker inspect --format '{{json .State}}' "$container" >".artifacts/e2e/$service-state.json" 2>/dev/null || true
      compose logs --no-color --tail 150 "$service" >".artifacts/e2e/$service.log" 2>&1 || true
    done
    python3 infrastructure/ci/redact-diagnostics.py .artifacts/e2e secrets
  fi
  compose down --volumes --remove-orphans
  exit "$result"
}
trap cleanup EXIT
bash infrastructure/scripts/bootstrap.sh
# Host directories remain private; mounted files must be readable by each nonroot service.
chmod 444 secrets/*_password
compose up -d --wait --wait-timeout 300 postgres redis kafka
compose run --rm kafka-init
compose up --no-deps --no-build -d --wait --wait-timeout 300 realtime-gateway trading-core ai-insights web
# A fresh stack has no quote cache until a real subscription starts. Establish
# the market-data prerequisite before the browser performs its first trade.
compose exec -T realtime-gateway /usr/local/bin/stream-probe --symbols AAPL,NVDA,META --quotes 2000 --timeout 5m >.artifacts/market-feed.log 2>&1 &
probe_pid=$!
ready=false
for _attempt in $(seq 1 30); do
  # Expand the password and user inside the Redis container, never in host logs.
  # shellcheck disable=SC2016
  if compose exec -T redis sh -c 'REDISCLI_AUTH="$(cat /run/secrets/redis_app_password)" redis-cli --no-auth-warning --user "$REDIS_APP_USER" --raw get quote:MOCK:AAPL' \
    | python3 -c 'import json,sys,time; q=json.load(sys.stdin); from datetime import datetime; assert q["dataMode"] == "REALTIME" and not q["stale"] and q["ask"] and time.time()-datetime.fromisoformat(q["timestamp"].replace("Z","+00:00")).timestamp() < 10' 2>/dev/null; then
    ready=true
    break
  fi
  sleep 1
done
[ "$ready" = true ] || { echo 'Fresh authoritative MOCK quote cache was not established' >&2; exit 1; }
make test-e2e
