#!/usr/bin/env bash
# Scan local runtime images and retain machine-readable evidence. Advisory downloads are cached.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export MSYS_NO_PATHCONV=1
scanner=ghcr.io/aquasecurity/trivy:0.75.0@sha256:af6acf9a6b85dfe389a1941505c0ce9efef52a4719635e1a962f022a3d855daa
output="${SECURITY_REPORT_DIR:-docs/security/images}"
mkdir -p "$output"
host_output="$(cygpath -am "$output" 2>/dev/null || realpath "$output")"
status=0
for image in ${SECURITY_IMAGES:-trading-terminal/trading-core:local trading-terminal/realtime-gateway:local trading-terminal/ai-insights:local trading-terminal/web:local trading-terminal/kafka:local trading-terminal/postgres:local trading-terminal/redis:local trading-terminal/kafka-provisioner:local prom/prometheus:v3.13.4 otel/opentelemetry-collector:0.161.0 grafana/tempo:3.1.0 grafana/grafana:13.2.3}; do
  docker image inspect --format '{{.Id}} {{json .RepoDigests}}' "$image"
  name="${image//\//_}"
  name="${name//:/_}"
  docker run --rm -v /var/run/docker.sock:/var/run/docker.sock \
    -v "${TRIVY_CACHE:-trading-terminal-trivy}:/root/.cache/trivy" -v "$host_output:/reports" "$scanner" \
    image --timeout 15m --db-repository ghcr.io/aquasecurity/trivy-db:2 --java-db-repository ghcr.io/aquasecurity/trivy-java-db:1 \
    --scanners vuln --severity HIGH,CRITICAL --list-all-pkgs --format json \
    --output "/reports/$name.json" --exit-code 1 "$image" || {
      result=$?
      if [ "$result" -eq 1 ] && [ -s "$output/$name.json" ]; then
        [ "$status" -ne 0 ] || status=1
      else
        status=2
      fi
    }
done
echo "Image audit includes detected OS and packaged application components; unidentified native libraries require separate review."
exit "$status"
