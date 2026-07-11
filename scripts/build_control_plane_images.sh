#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

trim_field() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "${value}"
}

require_env() {
  local name="$1"
  if [[ -z "$(trim_field "${!name:-}")" ]]; then
    printf 'required environment variable %s is missing\n' "${name}" >&2
    exit 1
  fi
}

image_repository_var() {
  case "$1" in
    api) printf 'FUGUE_API_IMAGE_REPOSITORY' ;;
    controller) printf 'FUGUE_CONTROLLER_IMAGE_REPOSITORY' ;;
    drain_agent) printf 'FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY' ;;
    telemetry_agent) printf 'FUGUE_TELEMETRY_AGENT_IMAGE_REPOSITORY' ;;
    image_cache) printf 'FUGUE_IMAGE_CACHE_IMAGE_REPOSITORY' ;;
    edge) printf 'FUGUE_EDGE_IMAGE_REPOSITORY' ;;
    app_ssh) printf 'FUGUE_APP_SSH_IMAGE_REPOSITORY' ;;
    *) return 1 ;;
  esac
}

image_dockerfile() {
  case "$1" in
    api) printf 'Dockerfile.api' ;;
    controller) printf 'Dockerfile.controller' ;;
    drain_agent) printf 'Dockerfile.drain-agent' ;;
    telemetry_agent) printf 'Dockerfile.telemetry-agent' ;;
    image_cache) printf 'Dockerfile.image-cache' ;;
    edge) printf 'Dockerfile.edge' ;;
    app_ssh) printf 'Dockerfile.app-ssh' ;;
    *) return 1 ;;
  esac
}

image_digest_from_metadata() {
  local metadata_file="$1"
  python3 - "${metadata_file}" <<'PY'
import json
from pathlib import Path
import re
import sys

path = Path(sys.argv[1])
try:
    metadata = json.loads(path.read_text())
except (OSError, json.JSONDecodeError) as exc:
    print(f"invalid build metadata {path}: {exc}", file=sys.stderr)
    raise SystemExit(1)

if not isinstance(metadata, dict):
    print(f"build metadata {path} must be a JSON object", file=sys.stderr)
    raise SystemExit(1)

digest = metadata.get("containerimage.digest")
if not isinstance(digest, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
    print(f"build metadata {path} has no complete containerimage.digest", file=sys.stderr)
    raise SystemExit(1)

print(digest)
PY
}

emit_output() {
  local key="$1"
  local value="$2"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    printf '%s=%s\n' "${key}" "${value}" >>"${GITHUB_OUTPUT}"
    return
  fi
  printf '%s=%s\n' "${key}" "${value}"
}

targets="$(trim_field "${FUGUE_CONTROL_PLANE_IMAGE_TARGETS:-}")"
if [[ -z "${targets}" ]]; then
  printf 'no control-plane images selected for build\n'
  exit 0
fi

require_env FUGUE_IMAGE_TAG

metadata_root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
metadata_dir="$(mktemp -d "${metadata_root%/}/fugue-build-metadata.XXXXXX")"
cleanup() {
  rm -rf "${metadata_dir}"
}
trap cleanup EXIT

pids=()
names=()
for target in ${targets}; do
  repo_var="$(image_repository_var "${target}")" || {
    printf 'unknown image build target: %s\n' "${target}" >&2
    exit 1
  }
  dockerfile="$(image_dockerfile "${target}")"
  require_env "${repo_var}"
  repository="${!repo_var}"
  tag="${repository}:${FUGUE_IMAGE_TAG}"
  cache_scope="fugue-control-plane-${target}"
  metadata_file="${metadata_dir}/${target}.json"
  printf 'building %s -> %s\n' "${target}" "${tag}"
  (
    cd "${REPO_ROOT}"
    docker buildx build \
      --platform linux/amd64 \
      --file "${dockerfile}" \
      --tag "${tag}" \
      --metadata-file "${metadata_file}" \
      --label "org.opencontainers.image.revision=${FUGUE_IMAGE_TAG}" \
      --cache-from "type=gha,scope=${cache_scope}" \
      --cache-to "type=gha,scope=${cache_scope},mode=max,ignore-error=true" \
      --push \
      .
  ) &
  pids+=("$!")
  names+=("${target}")
done

rc=0
for index in "${!pids[@]}"; do
  if ! wait "${pids[${index}]}"; then
    printf 'image build failed: %s\n' "${names[${index}]}" >&2
    rc=1
  fi
done

if [[ "${rc}" -ne 0 ]]; then
  exit "${rc}"
fi

digests=()
for target in "${names[@]}"; do
  metadata_file="${metadata_dir}/${target}.json"
  if ! digest="$(image_digest_from_metadata "${metadata_file}")"; then
    printf 'image digest metadata verification failed: %s\n' "${target}" >&2
    rc=1
    continue
  fi
  digests+=("${digest}")
done

if [[ "${rc}" -ne 0 ]]; then
  exit "${rc}"
fi

for index in "${!names[@]}"; do
  emit_output "${names[${index}]}_image_digest" "${digests[${index}]}"
done

exit "${rc}"
