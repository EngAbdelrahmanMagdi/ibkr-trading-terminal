#!/usr/bin/env bash
# Reports whether a pull request changes what decides which findings are accepted. Unchanged
# files are the reviewed base-branch versions; changed ones need a required reviewer's approval.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
paths=(
  security/accepted-findings.json
  infrastructure/ci/security_policy.py
  infrastructure/ci/collect_evidence.py
  infrastructure/ci/npm_evidence.py
  infrastructure/ci/image_context.py
)
changed=false
if [ -n "${BASE_SHA:-}" ]; then
  [[ "$BASE_SHA" =~ ^[0-9a-f]{40}$ ]] || { echo 'Invalid base commit' >&2; exit 2; }
  differences="$(git diff --name-only "$BASE_SHA" HEAD -- "${paths[@]}")"
  if [ -n "$differences" ]; then
    changed=true
    {
      echo '### Security exception changes need approval'
      echo 'These files differ from the reviewed base branch:'
      while IFS= read -r path; do printf -- '- %s\n' "$path"; done <<<"$differences"
    } >>"${GITHUB_STEP_SUMMARY:-/dev/stdout}"
  fi
fi
echo "changed=$changed" >>"${GITHUB_OUTPUT:-/dev/stdout}"
