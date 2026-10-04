#!/usr/bin/env bash
# Build each runtime role once, with immutable base inputs and local-only image tags.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export MSYS_NO_PATHCONV=1
bash infrastructure/scripts/bootstrap.sh
mkdir -p .artifacts/build
python3 - <<'PY'
from pathlib import Path
from sys import path
path.insert(0, 'infrastructure/ci')
from image_context import RECIPES, pin_dockerfile
root = Path('.').resolve()
lines = ['services:']
for role in RECIPES:
    output = root / '.artifacts/build' / (role+'.Dockerfile')
    pin_dockerfile(root, role, output)
    service = 'kafka-init' if role == 'kafka-provisioner' else role
    lines.extend(['  '+service+':', '    build:', '      dockerfile: '+output.as_posix()])
    if __import__('os').environ.get('GITHUB_ACTIONS') == 'true':
        cache = root / '.artifacts/build-cache' / role
        lines.extend(['      cache_from:', '        - type=local,src='+cache.as_posix(),
                      '      cache_to:', '        - type=local,dest='+cache.as_posix()+',mode=min'])
Path('.artifacts/build/compose.yml').write_text('\n'.join(lines)+'\n')
PY
docker compose -f docker-compose.yml -f .artifacts/build/compose.yml --profile gateway --profile core --profile ai --profile web build
# These are short-lived CI transfer artifacts, never release/publication artifacts.
docker save trading-terminal/trading-core:local trading-terminal/realtime-gateway:local \
  trading-terminal/ai-insights:local trading-terminal/web:local trading-terminal/kafka:local \
  trading-terminal/postgres:local trading-terminal/redis:local trading-terminal/kafka-provisioner:local \
  | gzip -1 >.artifacts/build/runtime-images.tar.gz
