#!/usr/bin/env bash
# Prepares a fresh clone for `docker compose`:
#   1. checks prerequisites (Docker, Docker Compose >= MIN_COMPOSE_VERSION)
#   2. creates .env from .env.example (never overwrites an existing .env)
#   3. generates missing local values (KAFKA_CLUSTER_ID) and secret files under ./secrets/
#      (never overwrites existing secrets)
#   4. warns about host ports that are already in use
# Safe to run repeatedly.
set -euo pipefail

# `docker compose up --wait-timeout` was introduced in Docker Compose v2.17.0.
readonly MIN_COMPOSE_VERSION="2.17.0"
readonly SECRET_NAMES=(
  postgres_admin_password
  trading_owner_password
  trading_app_password
  redis_app_password
  grafana_admin_password
)

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

info() { printf '  %s\n' "$*"; }
fail() { printf 'bootstrap: ERROR: %s\n' "$*" >&2; exit 1; }

version_ge() { # version_ge <have> <need>
  [[ "$(printf '%s\n%s\n' "$2" "$1" | sort -V | head -n1)" == "$2" ]]
}

random_hex() { # 32 random bytes as 64 hex characters (no trailing newline or carriage return)
  if command -v openssl > /dev/null 2>&1; then
    openssl rand -hex 32 | tr -d '\r\n'
  else
    od -An -tx1 -N32 /dev/urandom | tr -d ' \r\n'
  fi
}

random_cluster_id() { # 16 random bytes, base64url without padding (22 characters)
  if command -v openssl > /dev/null 2>&1; then
    openssl rand -base64 16 | tr '+/' '-_' | tr -d '=\r\n'
  else
    head -c 16 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\r\n'
  fi
}

echo "bootstrap: checking prerequisites"
command -v docker > /dev/null 2>&1 || fail "docker is not installed or not on PATH"
docker info > /dev/null 2>&1 || fail "the Docker daemon is not reachable (is Docker running?)"
compose_version="$(docker compose version --short 2> /dev/null || true)"
compose_version="${compose_version#v}"
[[ -n "$compose_version" ]] || fail "'docker compose' (Compose v2 plugin) is not available"
version_ge "$compose_version" "$MIN_COMPOSE_VERSION" \
  || fail "Docker Compose $compose_version found; $MIN_COMPOSE_VERSION or newer is required (for 'up --wait-timeout')"
info "Docker Compose $compose_version (minimum $MIN_COMPOSE_VERSION)"
command -v curl > /dev/null 2>&1 || info "note: curl not found - it is required by 'make verify'"

echo "bootstrap: environment file"
if [[ -f .env ]]; then
  info ".env exists - existing values left unchanged"
  # Append settings introduced in .env.example since .env was created (never overwrites existing keys).
  while IFS= read -r line; do
    key="${line%%=*}"
    if ! grep -qE "^${key}=" .env; then
      printf '%s\n' "$line" >> .env
      info "added new setting ${key}"
    fi
  done < <(grep -E '^[A-Z][A-Z0-9_]*=' .env.example)
else
  cp .env.example .env
  info "created .env from .env.example"
fi
if grep -qE '^KAFKA_CLUSTER_ID=$' .env; then
  cluster_id="$(random_cluster_id)"
  sed -i.bak "s/^KAFKA_CLUSTER_ID=$/KAFKA_CLUSTER_ID=${cluster_id}/" .env && rm -f .env.bak
  info "generated KAFKA_CLUSTER_ID"
fi

echo "bootstrap: local secrets (./secrets, gitignored)"
mkdir -p secrets
chmod 700 secrets 2> /dev/null || true
for name in "${SECRET_NAMES[@]}"; do
  file="secrets/${name}"
  if [[ -s "$file" ]]; then
    info "${name}: exists - left unchanged"
  else
    (umask 077 && random_hex > "$file")
    info "${name}: generated"
  fi
done

echo "bootstrap: host port check"
running="$(docker compose ps -q 2> /dev/null || true)"
if [[ -n "$running" ]]; then
  info "environment already running - skipped"
else
  while IFS='=' read -r key value; do
    [[ "$key" =~ _HOST_PORT$ ]] || continue
    if (exec 3<> "/dev/tcp/127.0.0.1/${value}") 2> /dev/null; then
      info "WARNING: 127.0.0.1:${value} (${key}) is already in use - change it in .env"
    fi
  done < <(grep -E '^[A-Z_]+_HOST_PORT=[0-9]+$' .env)
  info "done"
fi

echo "bootstrap: ready"
