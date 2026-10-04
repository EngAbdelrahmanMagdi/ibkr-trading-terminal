#!/usr/bin/env bash
# Build and record only a newly created, runner-owned simulated environment.
set -euo pipefail
# Long-running builds must not read a script that may be edited during review.
if [[ "${DEMO_SCRIPT_SNAPSHOT:-}" != true ]]; then
  source_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  mkdir -p "$source_root/.artifacts"
  snapshot="$(mktemp "$source_root/.artifacts/record-script-XXXXXX")"
  cp "${BASH_SOURCE[0]}" "$snapshot"
  exec env DEMO_SCRIPT_SNAPSHOT=true DEMO_SOURCE_ROOT="$source_root" bash "$snapshot"
fi
cd "${DEMO_SOURCE_ROOT:?Missing recording source root}"
export MSYS_NO_PATHCONV=1
project="marketpulse-record-$(date -u +%Y%m%d%H%M%S)-${RANDOM}"
[[ "$project" =~ ^marketpulse-record-[0-9]+-[0-9]+$ ]] || exit 2
output=".artifacts/demo/$project"
mkdir -p "$output/workspace"
root="$(pwd)"
encoder="${DEMO_FFMPEG:-$root/.artifacts/tools/ffmpeg-9.0.2-essentials_build/bin/ffmpeg.exe}"
[[ -f "$encoder" ]] || { echo 'Configure the verified FFmpeg 9.0.2 encoder before recording' >&2; exit 2; }
# A separate source copy means bootstrap cannot touch an owner's .env or secrets.
git ls-files -z --cached --others --exclude-standard \
  | tar --null -T - -cf - | tar -C "$output/workspace" -xf -
cd "$output/workspace"
cp .env.example .env
python3 - "$project" <<'PY'
from pathlib import Path
import socket
import sys
ports = {'POSTGRES_HOST_PORT': 46532, 'REDIS_HOST_PORT': 46679,
         'KAFKA_HOST_PORT': 46992, 'GATEWAY_HOST_PORT': 46090,
         'GATEWAY_HEALTH_HOST_PORT': 46091, 'WEB_HOST_PORT': 46000,
         'TRADING_CORE_HOST_PORT': 46080, 'TRADING_CORE_MANAGEMENT_HOST_PORT': 46081}
for port in ports.values():
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', port))
values = {key: str(value) for key, value in ports.items()}
values.update(COMPOSE_PROJECT_NAME=sys.argv[1],
              GATEWAY_ALLOWED_ORIGINS='http://127.0.0.1:46000',
              TRADING_CORE_ALLOWED_ORIGINS='http://127.0.0.1:46000',
              WEB_CORE_ORIGIN='http://127.0.0.1:46080',
              WEB_GATEWAY_ORIGIN='http://127.0.0.1:46090',
              WEB_GATEWAY_WS_URL='ws://127.0.0.1:46090/ws')
text = Path('.env').read_text()
for key, value in values.items():
    lines = text.splitlines()
    if any(line.startswith(key+'=') for line in lines):
        text = '\n'.join(key+'='+value if line.startswith(key+'=') else line for line in lines)+'\n'
    else:
        text += key+'='+value+'\n'
Path('.env').write_text(text)
services = ['postgres', 'redis', 'kafka', 'kafka-init', 'trading-core', 'realtime-gateway', 'ai-insights', 'web']
Path('record.yml').write_text('services:\n'+''.join(
    '  '+name+':\n    image: '+sys.argv[1]+'/'+name+':local\n'+
    ('    environment:\n      SPRING_DATA_REDIS_TIMEOUT: 5s\n      SPRING_DATA_REDIS_CONNECT_TIMEOUT: 5s\n' if name == 'trading-core' else '')
    for name in services))
PY
compose() { timeout 1500 docker compose -p "$project" --env-file .env -f docker-compose.yml -f record.yml --profile gateway --profile core --profile ai --profile web "$@"; }
cleanup() {
  compose logs --no-color --tail 100 >"$root/$output/service-diagnostics.log" 2>&1 || true
  compose down --volumes --remove-orphans >>"$root/$output/runner.log" 2>&1
}
trap cleanup EXIT
bash infrastructure/scripts/bootstrap.sh >"$root/$output/runner.log" 2>&1
{
  compose build
  compose up -d --wait --wait-timeout 300 postgres redis kafka
  compose run --rm kafka-init
  compose up --no-deps -d --wait --wait-timeout 300 realtime-gateway trading-core ai-insights web
} >>"$root/$output/runner.log" 2>&1
host_source="$(cygpath -am apps/web 2>/dev/null || realpath apps/web)"
host_output="$(cygpath -am "$root/$output" 2>/dev/null || realpath "$root/$output")"
timeout 300 docker run --rm --network host -v "$host_source:/src:ro" -v "$host_output:/artifacts" \
  -e PLAYWRIGHT_BASE_URL=http://127.0.0.1:46000 -e DEMO_CORE_ORIGIN=http://127.0.0.1:46080 \
  -e DEMO_OUTPUT=/artifacts/raw mcr.microsoft.com/playwright:v1.63.0-noble \
  sh -c 'mkdir /work && cp -r /src/. /work/ && cd /work && npm ci --ignore-scripts --no-audit --no-fund && npx playwright test --config playwright.demo.config.ts' \
  >>"$root/$output/runner.log" 2>&1
mapfile -t recordings < <(find "$root/$output/raw" -name video.webm -type f)
[[ ${#recordings[@]} -eq 1 ]] || { echo 'Expected exactly one verified recording' >&2; exit 2; }
timeout 120 python3 "$root/infrastructure/ci/encode-demo.py" --source "${recordings[0]}" \
  --output "$root/$output/media" --ffmpeg "$encoder" >>"$root/$output/runner.log" 2>&1
echo 'Real MOCK recording captured. Local output is under .artifacts/demo; no publication occurred.'
