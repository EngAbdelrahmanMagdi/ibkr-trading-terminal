#!/usr/bin/env bash
# Vulnerability findings and exception-policy decisions remain separate evidence.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export MSYS_NO_PATHCONV=1
mkdir -p .artifacts/security
output="$(mktemp -d .artifacts/security/run-XXXXXX)"
root="$(cygpath -am . 2>/dev/null || pwd)"
# Observability images also use reviewed immutable bases, never mutable tag drift.
while IFS=' ' read -r tag reference; do
  docker pull "$reference" >>"$output/image-inputs.log" 2>&1
  docker tag "$reference" "$tag"
done < <(python3 - <<'PY'
import json
from pathlib import Path
bases = json.loads(Path('security/image-inputs.json').read_text())['bases']
for tag in ['prom/prometheus:v3.13.4', 'otel/opentelemetry-collector:0.161.0', 'grafana/tempo:3.1.0', 'grafana/grafana:13.2.3']:
    print(tag, bases[tag])
PY
)
set +e
make -k security-source >"$output/source.log" 2>&1
source_status=$?
make security-ai >"$output/python.log" 2>&1
python_status=$?
SECURITY_REPORT_DIR="$output/images" make security-images >"$output/images.log" 2>&1
image_status=$?
docker run --rm -v "$root/apps/web:/src:ro" -w /src node:24.21.0 npm audit --json --audit-level=high >"$output/npm.json" 2>"$output/npm.log"
npm_status=$?
set -e
# Report validation distinguishes expected vulnerability exits from broken scans.
printf '{"source":%s,"python":%s,"images":%s,"npm":%s}\n' "$source_status" "$python_status" "$image_status" "$npm_status" >"$output/scanner-status.json"
python3 infrastructure/ci/collect_evidence.py --directory "$output"
baseline="${SECURITY_BASELINE:-security/accepted-findings.json}"
python3 "${SECURITY_POLICY:-infrastructure/ci/security_policy.py}" --baseline "$baseline" --evidence "$output/evidence.json" --output "$output/policy.json"
printf '%s\n' "$output" >.artifacts/security/latest-path.txt
