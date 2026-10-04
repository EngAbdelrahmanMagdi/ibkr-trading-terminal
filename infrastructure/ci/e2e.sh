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
make test-e2e
