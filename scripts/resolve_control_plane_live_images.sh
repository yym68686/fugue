#!/usr/bin/env bash

set -euo pipefail

trim_field() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "${value}"
}

detect_kubectl() {
  if command -v kubectl >/dev/null 2>&1; then
    printf 'kubectl'
    return 0
  fi
  if command -v k3s >/dev/null 2>&1; then
    printf 'k3s kubectl'
    return 0
  fi
  printf 'kubectl'
}

image_ref_without_digest() {
  local image_ref="$1"
  printf '%s' "${image_ref%%@*}"
}

image_ref_repository() {
  local image_ref no_digest last
  image_ref="$(trim_field "$1")"
  no_digest="$(image_ref_without_digest "${image_ref}")"
  last="${no_digest##*/}"
  if [[ "${last}" == *:* ]]; then
    printf '%s' "${no_digest%:*}"
  else
    printf '%s' "${no_digest}"
  fi
}

image_ref_tag() {
  local image_ref no_digest last
  image_ref="$(trim_field "$1")"
  no_digest="$(image_ref_without_digest "${image_ref}")"
  last="${no_digest##*/}"
  if [[ "${last}" == *:* ]]; then
    printf '%s' "${last##*:}"
  else
    printf 'latest'
  fi
}

emit_output() {
  local key="$1"
  local value="$2"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    printf '%s=%s\n' "${key}" "${value}" >>"${GITHUB_OUTPUT}"
  else
    printf '%s=%s\n' "${key}" "${value}"
  fi
}

resource_container_image() {
  local kind="$1"
  local name="$2"
  local container_name="$3"
  local namespace="${FUGUE_NAMESPACE:-fugue-system}"
  local kubectl_cmd="${KUBECTL_CMD:-kubectl}"

  ${kubectl_cmd} -n "${namespace}" get "${kind}/${name}" -o json 2>/dev/null | python3 -c '
import json
import sys

container_name = sys.argv[1]
try:
    doc = json.load(sys.stdin)
except Exception:
    raise SystemExit(0)
for container in doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
    if container.get("name") == container_name and container.get("image"):
        print(container["image"])
        raise SystemExit(0)
' "${container_name}" 2>/dev/null || true
}

deployment_container_env_value() {
  local name="$1"
  local container_name="$2"
  local env_name="$3"
  local namespace="${FUGUE_NAMESPACE:-fugue-system}"
  local kubectl_cmd="${KUBECTL_CMD:-kubectl}"

  ${kubectl_cmd} -n "${namespace}" get "deploy/${name}" -o json 2>/dev/null | python3 -c '
import json
import sys

container_name, env_name = sys.argv[1], sys.argv[2]
try:
    doc = json.load(sys.stdin)
except Exception:
    raise SystemExit(0)
for container in doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
    if container.get("name") != container_name:
        continue
    for item in container.get("env") or []:
        if item.get("name") == env_name and item.get("value") is not None:
            print(str(item["value"]))
            raise SystemExit(0)
' "${container_name}" "${env_name}" 2>/dev/null || true
}

output_image_ref() {
  local prefix="$1"
  local image_ref="$2"
  local fallback_repository="$3"
  local fallback_tag="$4"
  local repository tag

  image_ref="$(trim_field "${image_ref}")"
  if [[ -n "${image_ref}" ]]; then
    repository="$(image_ref_repository "${image_ref}")"
    tag="$(image_ref_tag "${image_ref}")"
  else
    repository="$(trim_field "${fallback_repository}")"
    tag="$(trim_field "${fallback_tag}")"
  fi
  emit_output "${prefix}_image_repository" "${repository}"
  emit_output "${prefix}_image_tag" "${tag}"
  printf '%s live image: %s:%s\n' "${prefix}" "${repository}" "${tag}"
}

FUGUE_NAMESPACE="${FUGUE_NAMESPACE:-fugue-system}"
FUGUE_RELEASE_FULLNAME="${FUGUE_RELEASE_FULLNAME:-fugue-fugue}"
KUBECTL_CMD="$(detect_kubectl)"

api_image="$(resource_container_image deploy "${FUGUE_RELEASE_FULLNAME}-api" api)"
if [[ -z "$(trim_field "${api_image}")" ]]; then
  api_image="$(resource_container_image deploy "${FUGUE_RELEASE_FULLNAME}" api)"
fi
controller_image="$(resource_container_image deploy "${FUGUE_RELEASE_FULLNAME}-controller" controller)"
telemetry_image="$(resource_container_image deploy "${FUGUE_RELEASE_FULLNAME}-telemetry-agent" telemetry-agent)"
image_cache_image="$(resource_container_image ds "${FUGUE_RELEASE_FULLNAME}-image-cache" image-cache)"
edge_image="$(resource_container_image ds "${FUGUE_RELEASE_FULLNAME}-edge" edge)"
if [[ -z "$(trim_field "${edge_image}")" ]]; then
  edge_image="$(resource_container_image ds "${FUGUE_RELEASE_FULLNAME}-edge-worker-a" edge)"
fi

drain_repository="$(deployment_container_env_value "${FUGUE_RELEASE_FULLNAME}-controller" controller FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY)"
drain_tag="$(deployment_container_env_value "${FUGUE_RELEASE_FULLNAME}-controller" controller FUGUE_DRAIN_AGENT_IMAGE_TAG)"
drain_image=""
if [[ -n "$(trim_field "${drain_repository}")" && -n "$(trim_field "${drain_tag}")" ]]; then
  drain_image="${drain_repository}:${drain_tag}"
fi

fallback_tag="${FUGUE_IMAGE_TAG:-${GITHUB_SHA:-latest}}"
output_image_ref api "${api_image}" "${FUGUE_API_IMAGE_REPOSITORY:-ghcr.io/yym68686/fugue-api}" "${fallback_tag}"
output_image_ref controller "${controller_image}" "${FUGUE_CONTROLLER_IMAGE_REPOSITORY:-ghcr.io/yym68686/fugue-controller}" "${fallback_tag}"
output_image_ref telemetry_agent "${telemetry_image}" "${FUGUE_TELEMETRY_AGENT_IMAGE_REPOSITORY:-ghcr.io/yym68686/fugue-telemetry-agent}" "${fallback_tag}"
output_image_ref drain_agent "${drain_image}" "${FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY:-ghcr.io/yym68686/fugue-drain-agent}" "${fallback_tag}"
output_image_ref image_cache "${image_cache_image}" "${FUGUE_IMAGE_CACHE_IMAGE_REPOSITORY:-ghcr.io/yym68686/fugue-image-cache}" "${fallback_tag}"
output_image_ref edge "${edge_image}" "${FUGUE_EDGE_IMAGE_REPOSITORY:-ghcr.io/yym68686/fugue-edge}" "${fallback_tag}"
