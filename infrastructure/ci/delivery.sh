#!/usr/bin/env bash
# Destination-neutral, checksummed artifacts; no application deployment.
set -euo pipefail
[[ "${GITHUB_ACTIONS:-}" == true ]] || { echo 'Delivery requires verified GitHub CI' >&2; exit 2; }
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
revision="$(git rev-parse HEAD)"
[[ "$revision" == "${GITHUB_SHA:?Missing verified source revision}" ]] || exit 2
[[ -z "$(git status --porcelain --untracked-files=no)" ]] || { echo 'Refusing dirty tracked source' >&2; exit 2; }
output=.artifacts/delivery
mkdir -p "$output/apps"
git archive --format=tar.gz --output="$output/source-$revision.tar.gz" "$revision"
tar -czf "$output/contracts-$revision.tar.gz" contracts
tar -czf "$output/showcase-source-$revision.tar.gz" apps/showcase
extract() {
  local role="$1" source="$2" target="$3" container
  container="$(docker create --network none "trading-terminal/$role:local")"
  if ! docker cp "$container:$source" "$target"; then docker rm "$container" >/dev/null; return 1; fi
  docker rm "$container" >/dev/null
}
extract trading-core /app/trading-core.jar "$output/apps/trading-core.jar"
extract realtime-gateway /usr/local/bin/realtime-gateway "$output/apps/realtime-gateway"
extract web /app "$output/apps/web"
extract ai-insights /app/src "$output/apps/ai-insights"
cp services/ai-insights/requirements.txt "$output/apps/ai-insights-requirements.txt"
cp -r .artifacts/security "$output/security-evidence"
python3 infrastructure/ci/delivery_manifest.py "$output"
# OCI/Docker release artifacts are forbidden even for accepted runtime Critical findings.
image_policy="$(python3 - <<'PY'
import json
from pathlib import Path
import sys
sys.path.insert(0, 'infrastructure/ci')
from delivery_manifest import image_delivery_allowed
directory = Path(Path('.artifacts/security/latest-path.txt').read_text().strip())
print(str(image_delivery_allowed(json.loads((directory/'evidence.json').read_text()), json.loads((directory/'policy.json').read_text()))).lower())
PY
)"
if [[ "${RUNTIME_IMAGE_DELIVERY_ALLOWED:-false}" == true && "$image_policy" == true ]]; then
  cp .artifacts/build/runtime-images.tar.gz "$output/runtime-images.tar.gz"
else
  echo 'Runtime image release artifacts blocked by security policy; source/build delivery only.'
fi
python3 - "$revision" <<'PY'
import json
from pathlib import Path
import sys
output = Path('.artifacts/delivery')
output.joinpath('manifest.json').write_text(json.dumps({
    'commit': sys.argv[1], 'tools': json.loads(Path('infrastructure/ci/tool-versions.json').read_text()),
    'deployment': 'none', 'runtime_image_delivery': bool(output.joinpath('runtime-images.tar.gz').exists())}, indent=2)+'\n')
PY
(
  cd "$output"
  checksum_lines="$(find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum)"
  printf '%s\n' "$checksum_lines" >SHA256SUMS
  sha256sum -c SHA256SUMS
)
