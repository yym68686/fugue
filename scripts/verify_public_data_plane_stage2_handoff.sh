#!/usr/bin/env bash

set -euo pipefail

[[ $# == 4 ]] || {
  printf 'usage: %s BASELINE TRACE CURRENT_REVISION CURRENT_CANONICAL_MANIFEST\n' "$0" >&2
  exit 2
}

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
ADOPTION_TOOL="${FUGUE_PUBLIC_DATA_PLANE_ADOPTION_TOOL:-${REPO_ROOT}/bin/fugue-public-data-plane-adoption}"

"${ADOPTION_TOOL}" verify-stage2 \
  --baseline "$1" \
  --trace "$2" \
  --current-revision "$3" \
  --current-manifest "$4"
