#!/usr/bin/env bash
# Controlled outages are manual and restricted to a new ephemeral GitHub runner project.
set -euo pipefail
[[ "${GITHUB_ACTIONS:-}" == true ]] || { echo 'CI-only maintenance runner' >&2; exit 2; }
[[ "${GITHUB_RUN_ID:-}" =~ ^[0-9]+$ && "${GITHUB_RUN_ATTEMPT:-}" =~ ^[0-9]+$ ]] || exit 2
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
project="marketpulse-resilience-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
export COMPOSE_PROJECT_NAME="$project"
compose() { docker compose -p "$project" --profile gateway --profile core --profile web --profile ai --profile observability "$@"; }
cleanup() { compose down --volumes --remove-orphans; }
trap cleanup EXIT
bash infrastructure/ci/build-images.sh
sed -i "s/^COMPOSE_PROJECT_NAME=.*/COMPOSE_PROJECT_NAME=$project/" .env
mkdir -p .artifacts/maintenance
printf 'services: {}\n' >.artifacts/maintenance/compose.yml
compose up -d --wait --wait-timeout 300 postgres redis kafka
compose run --rm kafka-init
compose up --no-deps --no-build -d --wait --wait-timeout 300 realtime-gateway trading-core ai-insights web prometheus grafana tempo otel-collector
VERIFY_RESTARTS=1 make verify
KAFKA_TEST_PROJECT="$project" KAFKA_TEST_SECRET_DIR=secrets/kafka make test-kafka-access
RESILIENCE_ENV_FILE=.env RESILIENCE_COMPOSE_OVERRIDE=.artifacts/maintenance/compose.yml make test-resilience
