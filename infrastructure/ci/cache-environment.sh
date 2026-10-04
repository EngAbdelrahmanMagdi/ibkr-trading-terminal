#!/usr/bin/env bash
# Cache downloads and compiler data only, never credentials or Python environments.
set -euo pipefail
root="${GITHUB_WORKSPACE:?Run inside a GitHub runner}"
base="$root/.artifacts/cache"
mkdir -p "$base"/{maven,go-mod,go-build,npm,pip}
{
  printf 'MAVEN_CACHE=%s/maven\n' "$base"
  printf 'GO_MODULE_CACHE=%s/go-mod\n' "$base"
  printf 'GO_BUILD_CACHE=%s/go-build\n' "$base"
  printf 'NPM_CACHE=%s/npm\n' "$base"
  printf 'PIP_CACHE=%s/pip\n' "$base"
} >>"${GITHUB_ENV:?Missing runner environment output}"
