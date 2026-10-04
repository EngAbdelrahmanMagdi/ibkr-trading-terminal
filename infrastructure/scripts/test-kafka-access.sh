#!/usr/bin/env bash
# Real broker trust and authorization checks, restricted to a disposable project.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export MSYS_NO_PATHCONV=1
project="${KAFKA_TEST_PROJECT:?Set the disposable Kafka project}"
case "$project" in marketpulse-hardening|marketpulse-resilience-*) ;; *) echo 'Refusing non-disposable project' >&2; exit 2 ;; esac
secrets="${KAFKA_TEST_SECRET_DIR:?Set its Kafka identity directory}"
secret_root="$(cygpath -am "$secrets" 2>/dev/null || realpath "$secrets")"
temporary="$(mktemp -d)"
trap 'rm -f "$temporary"/*.pem "$temporary"/*.key "$temporary"/*.csr; rmdir "$temporary"' EXIT
host_temporary="$(cygpath -am "$temporary" 2>/dev/null || realpath "$temporary")"
docker run --rm --entrypoint sh -v "$secret_root:/trust:ro" -v "$host_temporary:/test" maven:3.9.16-eclipse-temurin-25 -c '
  set -eu
  openssl req -new -newkey rsa:2048 -nodes -subj /CN=kafka-unmatched -keyout /test/unknown.key -out /test/unknown.csr 2>/dev/null
  openssl x509 -req -in /test/unknown.csr -CA /trust/ca.pem -CAkey /trust/ca.key -set_serial 987654321 -days 1 -out /test/unknown.pem 2>/dev/null
  cat /test/unknown.key /test/unknown.pem > /test/unknown.keystore.pem
  openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=trading-core -keyout /test/rogue.key -out /test/rogue.pem 2>/dev/null
  cat /test/rogue.key /test/rogue.pem > /test/rogue.keystore.pem
  chmod 444 /test/*.pem'
probe() {
  local identity="$1" key="$2" mode="$3"
  docker run --rm --network "${project}_data" -e KAFKA_BOOTSTRAP=kafka:29092 \
    -v "$secret_root/ca.pem:/run/secrets/kafka_ca:ro" \
    -v "$key:/run/secrets/probe_$identity:ro" -v "${project}_kafka-data:/audit:ro" \
    --entrypoint java trading-terminal/kafka-provisioner:local -Xmx64m \
    --class-path '/opt/kafka/libs/*:/opt/principal-probe' PrincipalProbe "$identity" "$mode"
}
for identity in trading-core realtime-gateway ai-insights; do probe "$identity" "$secret_root/$identity.keystore" access; done
probe unknown "$host_temporary/unknown.keystore.pem" unknown
probe rogue "$host_temporary/rogue.keystore.pem" tls-failure
broker_ip="$(docker inspect -f "{{(index .NetworkSettings.Networks \"${project}_data\").IPAddress}}" "${project}-kafka-1")"
docker run --rm --network "${project}_data" --add-host "wrong-kafka-name:$broker_ip" \
  -e KAFKA_BOOTSTRAP=wrong-kafka-name:29092 \
  -v "$secret_root/ca.pem:/run/secrets/kafka_ca:ro" \
  -v "$secret_root/trading-core.keystore:/run/secrets/probe_trading-core:ro" \
  --entrypoint java trading-terminal/kafka-provisioner:local -Xmx64m \
  --class-path '/opt/kafka/libs/*:/opt/principal-probe' PrincipalProbe trading-core tls-failure
echo 'Kafka observed-principal, least-privilege and certificate trust checks passed'
