#!/usr/bin/env bash
# An ephemeral runner never shares owner data or credentials.
set -euo pipefail
[[ "${GITHUB_ACTIONS:-}" == true ]] || { echo 'CI-only disposable runner' >&2; exit 2; }
[[ "${GITHUB_RUN_ID:-}" =~ ^[0-9]+$ && "${GITHUB_RUN_ATTEMPT:-}" =~ ^[0-9]+$ ]] || exit 2
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export COMPOSE_PROJECT_NAME="marketpulse-ci-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
compose() { docker compose --profile gateway --profile core --profile ai --profile web "$@"; }
cleanup() { compose down --volumes --remove-orphans; }
trap cleanup EXIT
bash infrastructure/scripts/bootstrap.sh
compose up -d --wait --wait-timeout 300 postgres redis kafka
compose run --rm kafka-init
compose up --no-deps --no-build -d --wait --wait-timeout 300 realtime-gateway trading-core ai-insights web
make test-e2e
