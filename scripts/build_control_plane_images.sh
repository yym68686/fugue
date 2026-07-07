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

targets="$(trim_field "${FUGUE_CONTROL_PLANE_IMAGE_TARGETS:-}")"
if [[ -z "${targets}" ]]; then
  printf 'no control-plane images selected for build\n'
  exit 0
fi

require_env FUGUE_IMAGE_TAG

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
  printf 'building %s -> %s\n' "${target}" "${tag}"
  (
    cd "${REPO_ROOT}"
    docker buildx build \
      --platform linux/amd64 \
      --file "${dockerfile}" \
      --tag "${tag}" \
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

exit "${rc}"
