#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT_INPUT="${FUGUE_RELEASE_REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
REPO_ROOT="$(cd "${REPO_ROOT_INPUT}" && pwd -P)"

trim_field() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "${value}"
}

release_changed_files() {
  if [[ "${FUGUE_RELEASE_CHANGED_FILES_SET:-false}" == "true" || -n "${FUGUE_RELEASE_CHANGED_FILES:-}" ]]; then
    printf '%s\n' "${FUGUE_RELEASE_CHANGED_FILES}" | sed '/^[[:space:]]*$/d'
    return
  fi
  if [[ -n "${BEFORE_SHA:-}" && -n "${AFTER_SHA:-}" ]] &&
    git -C "${REPO_ROOT}" cat-file -e "${BEFORE_SHA}^{commit}" 2>/dev/null &&
    git -C "${REPO_ROOT}" cat-file -e "${AFTER_SHA}^{commit}" 2>/dev/null; then
    git -C "${REPO_ROOT}" diff --name-only "${BEFORE_SHA}" "${AFTER_SHA}"
    return
  fi
  if git -C "${REPO_ROOT}" rev-parse --verify HEAD^ >/dev/null 2>&1; then
    git -C "${REPO_ROOT}" diff --name-only HEAD^ HEAD
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

BUILD_API=false
BUILD_CONTROLLER=false
BUILD_DRAIN_AGENT=false
BUILD_TELEMETRY_AGENT=false
BUILD_IMAGE_CACHE=false
BUILD_EDGE=false
BUILD_APP_SSH=false
REASONS_API=""
REASONS_CONTROLLER=""
REASONS_DRAIN_AGENT=""
REASONS_TELEMETRY_AGENT=""
REASONS_IMAGE_CACHE=""
REASONS_EDGE=""
REASONS_APP_SSH=""

image_baseline_ref() {
  case "$1" in
    api) printf '%s' "${FUGUE_API_IMAGE_BASE_REF:-}" ;;
    controller) printf '%s' "${FUGUE_CONTROLLER_IMAGE_BASE_REF:-}" ;;
    drain_agent) printf '%s' "${FUGUE_DRAIN_AGENT_IMAGE_BASE_REF:-}" ;;
    telemetry_agent) printf '%s' "${FUGUE_TELEMETRY_AGENT_IMAGE_BASE_REF:-}" ;;
    image_cache) printf '%s' "${FUGUE_IMAGE_CACHE_IMAGE_BASE_REF:-}" ;;
    edge) printf '%s' "${FUGUE_EDGE_IMAGE_BASE_REF:-}" ;;
    app_ssh) printf '%s' "${FUGUE_APP_SSH_IMAGE_BASE_REF:-}" ;;
    *) return 1 ;;
  esac
}

image_reason_matches_component_baseline() {
  local image="$1"
  local reason="$2"
  local marker="${tmp_dir}/component-baseline-${image}"
  local changed="${tmp_dir}/component-changed-files-${image}"

  [[ "${reason}" == "unknown-change-set" ]] && return 0
  [[ -e "${marker}" ]] || return 0
  grep -Fx -- "${reason}" "${changed}" >/dev/null 2>&1
}

mark_image() {
  local image="$1"
  local reason="$2"
  image_reason_matches_component_baseline "${image}" "${reason}" || return 0
  case "${image}" in
    api)
      BUILD_API=true
      REASONS_API="${REASONS_API:+${REASONS_API},}${reason}"
      ;;
    controller)
      BUILD_CONTROLLER=true
      REASONS_CONTROLLER="${REASONS_CONTROLLER:+${REASONS_CONTROLLER},}${reason}"
      ;;
    drain_agent)
      BUILD_DRAIN_AGENT=true
      REASONS_DRAIN_AGENT="${REASONS_DRAIN_AGENT:+${REASONS_DRAIN_AGENT},}${reason}"
      ;;
    telemetry_agent)
      BUILD_TELEMETRY_AGENT=true
      REASONS_TELEMETRY_AGENT="${REASONS_TELEMETRY_AGENT:+${REASONS_TELEMETRY_AGENT},}${reason}"
      ;;
    image_cache)
      BUILD_IMAGE_CACHE=true
      REASONS_IMAGE_CACHE="${REASONS_IMAGE_CACHE:+${REASONS_IMAGE_CACHE},}${reason}"
      ;;
    edge)
      BUILD_EDGE=true
      REASONS_EDGE="${REASONS_EDGE:+${REASONS_EDGE},}${reason}"
      ;;
    app_ssh)
      BUILD_APP_SSH=true
      REASONS_APP_SSH="${REASONS_APP_SSH:+${REASONS_APP_SSH},}${reason}"
      ;;
    *)
      printf 'unknown image target: %s\n' "${image}" >&2
      exit 1
      ;;
  esac
}

mark_all_go_images() {
  local reason="$1"
  mark_image api "${reason}"
  mark_image controller "${reason}"
  mark_image drain_agent "${reason}"
  mark_image telemetry_agent "${reason}"
  mark_image image_cache "${reason}"
  mark_image edge "${reason}"
}

image_build_value() {
  case "$1" in
    api) printf '%s' "${BUILD_API}" ;;
    controller) printf '%s' "${BUILD_CONTROLLER}" ;;
    drain_agent) printf '%s' "${BUILD_DRAIN_AGENT}" ;;
    telemetry_agent) printf '%s' "${BUILD_TELEMETRY_AGENT}" ;;
    image_cache) printf '%s' "${BUILD_IMAGE_CACHE}" ;;
    edge) printf '%s' "${BUILD_EDGE}" ;;
    app_ssh) printf '%s' "${BUILD_APP_SSH}" ;;
    *) return 1 ;;
  esac
}

image_reasons_value() {
  case "$1" in
    api) printf '%s' "${REASONS_API}" ;;
    controller) printf '%s' "${REASONS_CONTROLLER}" ;;
    drain_agent) printf '%s' "${REASONS_DRAIN_AGENT}" ;;
    telemetry_agent) printf '%s' "${REASONS_TELEMETRY_AGENT}" ;;
    image_cache) printf '%s' "${REASONS_IMAGE_CACHE}" ;;
    edge) printf '%s' "${REASONS_EDGE}" ;;
    app_ssh) printf '%s' "${REASONS_APP_SSH}" ;;
    *) return 1 ;;
  esac
}

image_commands() {
  case "$1" in
    api) printf '%s\n' ./cmd/fugue-api ;;
    controller)
      printf '%s\n' ./cmd/fugue-controller
      printf '%s\n' ./cmd/fugue-registry-maintenance
      ;;
    drain_agent) printf '%s\n' ./cmd/fugue-drain-agent ;;
    telemetry_agent) printf '%s\n' ./cmd/fugue-telemetry-agent ;;
    image_cache) printf '%s\n' ./cmd/fugue-image-cache ;;
    edge)
      printf '%s\n' ./cmd/fugue-edge
      printf '%s\n' ./cmd/fugue-edge-front
      printf '%s\n' ./cmd/fugue-ssh-front
      printf '%s\n' ./cmd/fugue-dns
      printf '%s\n' ./cmd/fugue-mesh-agent
      printf '%s\n' ./cmd/fugue-mesh-recovery
      ;;
    *) return 1 ;;
  esac
}

tmp_dir="$(mktemp -d)"
cleanup() {
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT

changed_file="${tmp_dir}/changed-files"
release_changed_files >"${changed_file}"

TRUSTED_COMPONENT_BASELINE=false
target_ref="$(trim_field "${FUGUE_RELEASE_TARGET_REF:-}")"
if [[ -n "${target_ref}" ]] && git -C "${REPO_ROOT}" cat-file -e "${target_ref}^{commit}" 2>/dev/null; then
  for image in api controller drain_agent telemetry_agent image_cache edge app_ssh; do
    base_ref="$(trim_field "$(image_baseline_ref "${image}")")"
    [[ -n "${base_ref}" ]] || continue
    if ! git -C "${REPO_ROOT}" cat-file -e "${base_ref}^{commit}" 2>/dev/null; then
      printf 'component image baseline is not a local commit; using fail-safe union for %s: %s\n' "${image}" "${base_ref}" >&2
      continue
    fi
    component_changed="${tmp_dir}/component-changed-files-${image}"
    git -C "${REPO_ROOT}" diff --name-only "${base_ref}" "${target_ref}" | sort -u >"${component_changed}"
    touch "${tmp_dir}/component-baseline-${image}"
    TRUSTED_COMPONENT_BASELINE=true
  done
  sort -u "${changed_file}" -o "${changed_file}"
elif [[ -n "${target_ref}" ]]; then
  printf 'release target is not a local commit; using fail-safe union image plan: %s\n' "${target_ref}" >&2
fi

if [[ ! -s "${changed_file}" ]]; then
  if [[ "${TRUSTED_COMPONENT_BASELINE}" != "true" ]]; then
    mark_all_go_images "unknown-change-set"
    mark_image app_ssh "unknown-change-set"
  fi
else
  for image in api controller drain_agent telemetry_agent image_cache edge; do
    deps_file="${tmp_dir}/deps-${image}"
    : >"${deps_file}"
    while IFS= read -r command_path; do
      go -C "${REPO_ROOT}" list -deps -f '{{if not .Standard}}{{.Dir}}{{end}}' "${command_path}" |
        while IFS= read -r package_dir; do
          package_dir="$(trim_field "${package_dir}")"
          [[ -n "${package_dir}" ]] || continue
          if [[ "${package_dir}" == "${REPO_ROOT}" ]]; then
            printf '.\n'
          elif [[ "${package_dir}" == "${REPO_ROOT}/"* ]]; then
            printf '%s\n' "${package_dir#"${REPO_ROOT}/"}"
          fi
        done >>"${deps_file}"
    done < <(image_commands "${image}")
    sort -u "${deps_file}" -o "${deps_file}"
  done

  while IFS= read -r raw_file; do
    file="$(trim_field "${raw_file}")"
    [[ -n "${file}" ]] || continue
    case "${file}" in
      go.mod|go.sum)
        mark_all_go_images "${file}"
        continue
        ;;
      Dockerfile.api)
        mark_image api "${file}"
        continue
        ;;
      Dockerfile.controller)
        mark_image controller "${file}"
        continue
        ;;
      Dockerfile.drain-agent)
        mark_image drain_agent "${file}"
        continue
        ;;
      Dockerfile.telemetry-agent)
        mark_image telemetry_agent "${file}"
        continue
        ;;
      Dockerfile.image-cache)
        mark_image image_cache "${file}"
        continue
        ;;
      Dockerfile.edge)
        mark_image edge "${file}"
        continue
        ;;
      Dockerfile.app-ssh|images/app-ssh/*)
        mark_image app_ssh "${file}"
        continue
        ;;
      assets/*)
        mark_image api "${file}"
        continue
        ;;
    esac

    if [[ "${file}" == *_test.go ]]; then
      continue
    fi

    if [[ "${file}" == *.go && "${file}" != *_test.go ]]; then
      package_dir="$(dirname "${file}")"
      for image in api controller drain_agent telemetry_agent image_cache edge; do
        if grep -Fx -- "${package_dir}" "${tmp_dir}/deps-${image}" >/dev/null; then
          mark_image "${image}" "${file}"
        fi
      done
      continue
    fi

    case "${file}" in
      cmd/fugue-api/*)
        mark_image api "${file}"
        ;;
      cmd/fugue-controller/*|cmd/fugue-registry-maintenance/*)
        mark_image controller "${file}"
        ;;
      cmd/fugue-drain-agent/*)
        mark_image drain_agent "${file}"
        ;;
      cmd/fugue-telemetry-agent/*)
        mark_image telemetry_agent "${file}"
        ;;
      cmd/fugue-image-cache/*)
        mark_image image_cache "${file}"
        ;;
      cmd/fugue-edge/*|cmd/fugue-edge-front/*|cmd/fugue-ssh-front/*|cmd/fugue-dns/*|cmd/fugue-mesh-agent/*|cmd/fugue-mesh-recovery/*)
        mark_image edge "${file}"
        ;;
      internal/*)
        mark_all_go_images "${file}"
        ;;
    esac
  done <"${changed_file}"
fi

targets=()
for image in api controller drain_agent telemetry_agent image_cache edge app_ssh; do
  build_value="$(image_build_value "${image}")"
  emit_output "build_${image}" "${build_value}"
  if [[ "${build_value}" == "true" ]]; then
    targets+=("${image}")
  fi
done

targets_joined="${targets[*]-}"
emit_output "target_count" "${#targets[@]}"
emit_output "targets" "${targets_joined}"

for image in ${targets_joined}; do
  printf 'will build %s image' "${image}"
  reasons="$(image_reasons_value "${image}")"
  if [[ -n "${reasons}" ]]; then
    printf ' (%s)' "${reasons}"
  fi
  printf '\n'
done
if [[ "${#targets[@]}" -eq 0 ]]; then
  printf 'no control-plane images need rebuilding for this change set\n'
fi
