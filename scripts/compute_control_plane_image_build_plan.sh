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
    git -C "${REPO_ROOT}" diff --no-renames --name-only "${BEFORE_SHA}" "${AFTER_SHA}"
    return
  fi
  if git -C "${REPO_ROOT}" rev-parse --verify HEAD^ >/dev/null 2>&1; then
    git -C "${REPO_ROOT}" diff --no-renames --name-only HEAD^ HEAD
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

image_helm_drift() {
  case "$1" in
    api) printf '%s' "${FUGUE_API_IMAGE_HELM_DRIFT:-false}" ;;
    controller) printf '%s' "${FUGUE_CONTROLLER_IMAGE_HELM_DRIFT:-false}" ;;
    drain_agent) printf '%s' "${FUGUE_DRAIN_AGENT_IMAGE_HELM_DRIFT:-false}" ;;
    telemetry_agent) printf '%s' "${FUGUE_TELEMETRY_AGENT_IMAGE_HELM_DRIFT:-false}" ;;
    image_cache) printf '%s' "${FUGUE_IMAGE_CACHE_IMAGE_HELM_DRIFT:-false}" ;;
    edge) printf '%s' "${FUGUE_EDGE_IMAGE_HELM_DRIFT:-false}" ;;
    app_ssh) printf '%s' "${FUGUE_APP_SSH_IMAGE_HELM_DRIFT:-false}" ;;
    *) return 1 ;;
  esac
}

grep_membership() {
  local mode="$1"
  local pattern="$2"
  local path="$3"
  local context="$4"
  local rc=0

  case "${mode}" in
    exact)
      if grep -Fx -- "${pattern}" "${path}" >/dev/null; then
        return 0
      else
        rc=$?
      fi
      ;;
    extended)
      if grep -Eq -- "${pattern}" "${path}" >/dev/null; then
        return 0
      else
        rc=$?
      fi
      ;;
    *)
      printf 'unknown grep membership mode for %s: %s\n' "${context}" "${mode}" >&2
      exit 1
      ;;
  esac

  if [[ "${rc}" -eq 1 ]]; then
    return 1
  fi
  printf 'grep membership check failed for %s with status %s: %s\n' \
    "${context}" "${rc}" "${path}" >&2
  exit 1
}

image_reason_matches_component_baseline() {
  local image="$1"
  local reason="$2"
  local marker=""
  local changed=""

  case "${reason}" in
    unknown-change-set|helm-live-image-drift|component-source-drift|authorized-convergence-successor) return 0 ;;
  esac
  marker="${tmp_dir}/component-baseline-${image}"
  changed="${tmp_dir}/component-changed-files-${image}"
  [[ -e "${marker}" ]] || return 0
  grep_membership exact "${reason}" "${changed}" "${image} component baseline"
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

mark_all_images() {
  local reason="$1"
  mark_all_go_images "${reason}"
  mark_image app_ssh "${reason}"
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

emit_plan() {
  local image=""
  local build_value=""
  local targets_joined=""
  local reasons=""
  local -a targets=()

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

release_file_path_is_canonical() {
  local file="$1"
  local component=""
  local -a components=()

  [[ -n "${file}" ]] || return 1
  [[ "${file}" == "$(trim_field "${file}")" ]] || return 1
  [[ "${file}" != /* ]] || return 1
  [[ "${file}" != *\\* ]] || return 1
  [[ "${file}" != *//* ]] || return 1
  [[ "${file}" != */ ]] || return 1

  IFS='/' read -r -a components <<<"${file}"
  for component in "${components[@]}"; do
    [[ -n "${component}" && "${component}" != "." && "${component}" != ".." ]] || return 1
  done
}

verify_telemetry_production_release_intent() {
  local relative_path='deploy/environments/production/telemetry/release.json'
  local intent_path="${REPO_ROOT}/${relative_path}"
  local config_head
  local identity=()
  local identity_output
  local desired_source_sha
  local previous_source_sha

  [[ -f "${intent_path}" && ! -L "${intent_path}" ]] || {
    printf 'Telemetry production release intent must be a regular non-symlink file: %s\n' \
      "${relative_path}" >&2
    exit 1
  }
  identity_output="$(python3 - "${intent_path}" <<'PY'
import json
from pathlib import Path
import re
import sys

path = Path(sys.argv[1])
raw = path.read_bytes()
if not raw or len(raw) > 16 * 1024:
    raise SystemExit("telemetry-production-release-intent-size-invalid")

def reject_duplicate_keys(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate key {key}")
        value[key] = item
    return value

try:
    value = json.loads(
        raw,
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)),
    )
except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
    raise SystemExit(f"telemetry-production-release-intent-json-invalid: {exc}") from exc

fixed = {
    "apiVersion": "release.fugue.dev/v1",
    "component": "telemetry_agent",
    "deployment": "fugue-fugue-telemetry-agent",
    "environment": "production",
    "fieldManager": "fugue-telemetry-declarative",
    "kind": "ProductionComponentRelease",
    "namespace": "fugue-system",
    "ownership": "declarative",
    "replicas": 1,
    "repository": "ghcr.io/yym68686/fugue-telemetry-agent",
    "rollback": "previous-git-lkg",
    "service": "fugue-fugue-telemetry-agent",
}
expected_keys = set(fixed) | {
    "desiredSourceSha", "expectedPreviousImageDigest", "expectedPreviousSourceSha", "intentGeneration",
}
if type(value) is not dict or set(value) != expected_keys:
    raise SystemExit("telemetry-production-release-intent-schema-invalid")
for key, expected in fixed.items():
    if value[key] != expected or type(value[key]) is not type(expected):
        raise SystemExit(f"telemetry-production-release-intent-identity-invalid:{key}")
sha_pattern = re.compile(r"[0-9a-f]{40}")
digest_pattern = re.compile(r"sha256:[0-9a-f]{64}")
desired = value["desiredSourceSha"]
previous = value["expectedPreviousSourceSha"]
previous_digest = value["expectedPreviousImageDigest"]
generation = value["intentGeneration"]
if type(desired) is not str or sha_pattern.fullmatch(desired) is None:
    raise SystemExit("telemetry-production-release-intent-desired-source-invalid")
if type(previous) is not str or sha_pattern.fullmatch(previous) is None or previous == desired:
    raise SystemExit("telemetry-production-release-intent-previous-source-invalid")
if type(previous_digest) is not str or digest_pattern.fullmatch(previous_digest) is None:
    raise SystemExit("telemetry-production-release-intent-previous-image-digest-invalid")
if type(generation) is not int or generation < 1:
    raise SystemExit("telemetry-production-release-intent-generation-invalid")
canonical = json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("ascii") + b"\n"
if raw != canonical:
    raise SystemExit("telemetry-production-release-intent-canonical-bytes-invalid")
print(desired)
print(previous)
PY
  )" || exit 1
  desired_source_sha="${identity_output%%$'\n'*}"
  previous_source_sha="${identity_output#*$'\n'}"
  [[ "${identity_output}" == *$'\n'* && "${previous_source_sha}" != *$'\n'* ]] || {
    printf 'telemetry-production-release-intent-source-identity-invalid\n' >&2
    exit 1
  }
  identity=("${desired_source_sha}" "${previous_source_sha}")
  [[ "${#identity[@]}" -eq 2 ]] || {
    printf 'telemetry-production-release-intent-source-identity-invalid\n' >&2
    exit 1
  }
  config_head="$(git -C "${REPO_ROOT}" rev-parse --verify 'HEAD^{commit}')" || {
    printf 'telemetry-production-release-intent-config-head-invalid\n' >&2
    exit 1
  }
  if ! git -C "${REPO_ROOT}" cat-file -e "${identity[0]}^{commit}" 2>/dev/null ||
    ! git -C "${REPO_ROOT}" cat-file -e "${identity[1]}^{commit}" 2>/dev/null; then
    printf 'telemetry-production-release-intent-source-commit-missing\n' >&2
    exit 1
  fi
  if ! git -C "${REPO_ROOT}" merge-base --is-ancestor "${identity[1]}" "${identity[0]}"; then
    printf 'telemetry-production-release-intent-previous-not-ancestor\n' >&2
    exit 1
  fi
  if ! git -C "${REPO_ROOT}" merge-base --is-ancestor "${identity[0]}" "${config_head}"; then
    printf 'telemetry-production-release-intent-desired-not-config-ancestor\n' >&2
    exit 1
  fi
}

verify_controller_production_release_intent() {
  local relative_path='deploy/environments/production/controller/release.json'
  local intent_path="${REPO_ROOT}/${relative_path}"
  local config_head
  local identity=()
  local identity_output
  local desired_source_sha
  local previous_source_sha

  [[ -f "${intent_path}" && ! -L "${intent_path}" ]] || {
    printf 'Controller production release intent must be a regular non-symlink file: %s\n' \
      "${relative_path}" >&2
    exit 1
  }
  identity_output="$(python3 - "${intent_path}" <<'PY'
import json
from pathlib import Path
import re
import sys

path = Path(sys.argv[1])
raw = path.read_bytes()
if not raw or len(raw) > 16 * 1024:
    raise SystemExit("controller-production-release-intent-size-invalid")

def reject_duplicate_keys(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate key {key}")
        value[key] = item
    return value

try:
    value = json.loads(
        raw,
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)),
    )
except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
    raise SystemExit(f"controller-production-release-intent-json-invalid: {exc}") from exc

fixed = {
    "apiVersion": "release.fugue.dev/v1",
    "component": "controller",
    "deployment": "fugue-fugue-controller",
    "environment": "production",
    "fieldManager": "fugue-controller-declarative",
    "kind": "ProductionComponentRelease",
    "namespace": "fugue-system",
    "ownership": "declarative",
    "replicas": 2,
    "repository": "ghcr.io/yym68686/fugue-controller",
    "rollback": "previous-live-lkg",
}
expected_keys = set(fixed) | {
    "desiredSourceSha", "expectedPreviousImageDigest", "expectedPreviousSourceSha", "intentGeneration",
}
if type(value) is not dict or set(value) != expected_keys:
    raise SystemExit("controller-production-release-intent-schema-invalid")
for key, expected in fixed.items():
    if value[key] != expected or type(value[key]) is not type(expected):
        raise SystemExit(f"controller-production-release-intent-identity-invalid:{key}")
sha_pattern = re.compile(r"[0-9a-f]{40}")
digest_pattern = re.compile(r"sha256:[0-9a-f]{64}")
desired = value["desiredSourceSha"]
previous = value["expectedPreviousSourceSha"]
previous_digest = value["expectedPreviousImageDigest"]
generation = value["intentGeneration"]
if type(desired) is not str or sha_pattern.fullmatch(desired) is None:
    raise SystemExit("controller-production-release-intent-desired-source-invalid")
if type(previous) is not str or sha_pattern.fullmatch(previous) is None or previous == desired:
    raise SystemExit("controller-production-release-intent-previous-source-invalid")
if type(previous_digest) is not str or digest_pattern.fullmatch(previous_digest) is None:
    raise SystemExit("controller-production-release-intent-previous-image-digest-invalid")
if type(generation) is not int or generation < 1:
    raise SystemExit("controller-production-release-intent-generation-invalid")
canonical = json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("ascii") + b"\n"
if raw != canonical:
    raise SystemExit("controller-production-release-intent-canonical-bytes-invalid")
print(desired)
print(previous)
PY
  )" || exit 1
  desired_source_sha="${identity_output%%$'\n'*}"
  previous_source_sha="${identity_output#*$'\n'}"
  [[ "${identity_output}" == *$'\n'* && "${previous_source_sha}" != *$'\n'* ]] || {
    printf 'controller-production-release-intent-source-identity-invalid\n' >&2
    exit 1
  }
  identity=("${desired_source_sha}" "${previous_source_sha}")
  [[ "${#identity[@]}" -eq 2 ]] || {
    printf 'controller-production-release-intent-source-identity-invalid\n' >&2
    exit 1
  }
  config_head="$(git -C "${REPO_ROOT}" rev-parse --verify 'HEAD^{commit}')" || {
    printf 'controller-production-release-intent-config-head-invalid\n' >&2
    exit 1
  }
  if ! git -C "${REPO_ROOT}" cat-file -e "${identity[0]}^{commit}" 2>/dev/null ||
    ! git -C "${REPO_ROOT}" cat-file -e "${identity[1]}^{commit}" 2>/dev/null; then
    printf 'controller-production-release-intent-source-commit-missing\n' >&2
    exit 1
  fi
  if ! git -C "${REPO_ROOT}" merge-base --is-ancestor "${identity[1]}" "${identity[0]}"; then
    printf 'controller-production-release-intent-previous-not-ancestor\n' >&2
    exit 1
  fi
  if ! git -C "${REPO_ROOT}" merge-base --is-ancestor "${identity[0]}" "${config_head}"; then
    printf 'controller-production-release-intent-desired-not-config-ancestor\n' >&2
    exit 1
  fi
}

controller_declarative_atom_changed_set_is_exact() {
  local changed_file="$1"
  local expected_file="${tmp_dir}/controller-declarative-atom-files"
  cat >"${expected_file}" <<'EOF'
.github/workflows/ci.yml
Makefile
deploy/environments/production/controller/release.json
deploy/helm/fugue/chart_test.go
deploy/helm/fugue/templates/controller-deployment.yaml
deploy/helm/fugue/values.yaml
deploy/kustomize/controller/deployment.json
deploy/kustomize/controller/kustomization.yaml
internal/platformsafety/release_workflow_test.go
scripts/apply_controller_declarative.sh
scripts/compute_control_plane_image_build_plan.sh
scripts/prepush.py
scripts/test_apply_controller_declarative.sh
scripts/test_prepush.py
EOF
  [[ -z "$(comm -3 <(sort -u "${changed_file}") <(sort -u "${expected_file}"))" ]]
}

go_fixture_package_dir() {
  local file="$1"
  local package_dir=""

  case "${file}" in
    */testdata/*)
      package_dir="${file%%/testdata/*}"
      ;;
    */fixtures/*)
      package_dir="${file%%/fixtures/*}"
      ;;
    *)
      return 1
      ;;
  esac

  [[ -n "${package_dir}" && "${package_dir}" != "${file}" ]] || return 1
  printf '%s' "${package_dir}"
}

go_package_is_valid() {
  local package_dir="$1"
  local expected_dir=""
  local listed_dir=""
  local listed_physical_dir=""
  local source=""
  local has_runtime_source=false

  [[ -d "${REPO_ROOT}/${package_dir}" ]] || return 1
  for source in "${REPO_ROOT}/${package_dir}"/*.go; do
    [[ -f "${source}" && "${source}" != *_test.go ]] || continue
    has_runtime_source=true
    break
  done
  [[ "${has_runtime_source}" == "true" ]] || return 1
  if ! expected_dir="$(cd "${REPO_ROOT}/${package_dir}" && pwd -P)"; then
    printf 'could not resolve the physical Go package directory: %s\n' "${package_dir}" >&2
    exit 1
  fi

  # Keep stderr separate from the machine-readable directory. A successful
  # Go command may legitimately emit a toolchain or module warning there.
  if ! listed_dir="$(PWD="${REPO_ROOT}" go -C "${REPO_ROOT}" list -f '{{.Dir}}' "./${package_dir}")"; then
    printf 'go package metadata failed for existing path: %s\n' "${package_dir}" >&2
    exit 1
  fi
  if [[ -z "${listed_dir}" ]]; then
    printf 'go package metadata returned an empty directory for %s\n' "${package_dir}" >&2
    exit 1
  fi
  if [[ "${listed_dir}" != /* ]] ||
    ! listed_physical_dir="$(cd "${listed_dir}" 2>/dev/null && pwd -P)"; then
    printf 'go package metadata returned an unusable directory for %s: %s\n' \
      "${package_dir}" "${listed_dir}" >&2
    exit 1
  fi
  if [[ "${listed_physical_dir}" != "${expected_dir}" ]]; then
    printf 'go package metadata returned an unexpected directory for %s: %s\n' \
      "${package_dir}" "${listed_dir}" >&2
    exit 1
  fi
  return 0
}

go_package_has_runtime_embed_directive() {
  local package_dir="$1"
  local source=""

  for source in "${REPO_ROOT}/${package_dir}"/*.go; do
    [[ -f "${source}" ]] || continue
    [[ "${source}" != *_test.go ]] || continue
    if grep_membership extended '^[[:space:]]*//go:embed[[:space:]]' "${source}" \
      "${package_dir} runtime embed directive"; then
      return 0
    fi
  done
  return 1
}

go_fixture_is_runtime_asset() {
  local file="$1"
  local package_dir="$2"
  local relative_file="${file#"${package_dir}/"}"
  local embed_files=""
  local embedded_file=""

  # A deleted fixture can no longer appear in go list's EmbedFiles. If the
  # owning runtime package still contains any non-test embed directive, rebuild
  # conservatively rather than treating the deletion as test-only.
  if [[ ! -e "${REPO_ROOT}/${file}" ]]; then
    go_package_has_runtime_embed_directive "${package_dir}"
    return
  fi

  # EmbedFiles excludes TestEmbedFiles and expands Go's embed patterns using
  # the same rules as the compiler. Metadata errors cannot be distinguished
  # safely from an incomplete asset inventory, so abort without publishing a
  # build plan.
  if ! embed_files="$(PWD="${REPO_ROOT}" go -C "${REPO_ROOT}" list -f '{{range .EmbedFiles}}{{println .}}{{end}}' "./${package_dir}")"; then
    printf 'go embed metadata failed for existing package: %s\n' "${package_dir}" >&2
    exit 1
  fi
  while IFS= read -r embedded_file; do
    if [[ "${embedded_file}" == "${relative_file}" ]]; then
      return 0
    fi
  done <<<"${embed_files}"
  return 1
}

mark_go_package_images() {
  local package_dir="$1"
  local reason="$2"
  local image=""
  local matched=false

  for image in api controller drain_agent telemetry_agent image_cache edge; do
    if grep_membership exact "${package_dir}" "${tmp_dir}/deps-${image}" \
      "${image} dependency graph"; then
      matched=true
      mark_image "${image}" "${reason}"
    fi
  done
  [[ "${matched}" == "true" ]]
}

image_component_dockerfile() {
  case "$1" in
    api) printf '%s' Dockerfile.api ;;
    controller) printf '%s' Dockerfile.controller ;;
    drain_agent) printf '%s' Dockerfile.drain-agent ;;
    telemetry_agent) printf '%s' Dockerfile.telemetry-agent ;;
    image_cache) printf '%s' Dockerfile.image-cache ;;
    edge) printf '%s' Dockerfile.edge ;;
    *) return 1 ;;
  esac
}

component_source_changed_since_baseline() {
  local image="$1"
  local changed="${tmp_dir}/component-changed-files-${image}"
  local file=""
  local package_dir=""
  local fixture_package_dir=""
  local dockerfile=""

  [[ -e "${tmp_dir}/component-baseline-${image}" ]] || return 1
  [[ -s "${changed}" ]] || return 1
  dockerfile="$(image_component_dockerfile "${image}")" || return 1

  while IFS= read -r file; do
    file="$(trim_field "${file}")"
    [[ -n "${file}" ]] || continue

    case "${file}" in
      go.mod|go.sum|"${dockerfile}")
        return 0
        ;;
      *_test.go)
        continue
        ;;
    esac

    # A deleted production source file may no longer be present in the target
    # dependency graph. Treat it conservatively as component source drift.
    if [[ ! -e "${REPO_ROOT}/${file}" && ( "${file}" == cmd/* || "${file}" == internal/* ) ]]; then
      return 0
    fi

    if [[ "${file}" == *.go ]]; then
      package_dir="$(dirname "${file}")"
      if grep_membership exact "${package_dir}" "${tmp_dir}/deps-${image}" \
        "${image} component baseline source"; then
        return 0
      fi
      continue
    fi

    # Runtime assets under a package consumed by this image can change the
    # binary through //go:embed. Testdata/fixtures are checked with Go's embed
    # metadata so test-only assets do not cause an image rebuild.
    if fixture_package_dir="$(go_fixture_package_dir "${file}")" &&
      go_package_is_valid "${fixture_package_dir}"; then
      if grep_membership exact "${fixture_package_dir}" "${tmp_dir}/deps-${image}" \
        "${image} component baseline fixture" &&
        go_fixture_is_runtime_asset "${file}" "${fixture_package_dir}"; then
        return 0
      fi
      continue
    fi
    if [[ "${file}" == internal/* ]]; then
      package_dir="$(dirname "${file}")"
      if grep_membership exact "${package_dir}" "${tmp_dir}/deps-${image}" \
        "${image} component baseline runtime asset"; then
        return 0
      fi
    fi
  done <"${changed}"
  return 1
}

case "${FUGUE_RELEASE_IMAGE_CACHE_CONVERGENCE:-false}" in
  false) ;;
  true)
    # The workflow input is accepted only after release-input-guard validates
    # the source-run authorization artifact. A convergence successor must not
    # recompute an empty same-SHA plan or acquire authority over any artifact
    # other than the one named by that authorization.
    mark_image image_cache "authorized-convergence-successor"
    emit_plan
    exit 0
    ;;
  *)
    printf 'FUGUE_RELEASE_IMAGE_CACHE_CONVERGENCE must be true or false\n' >&2
    exit 1
    ;;
esac

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
      touch "${tmp_dir}/component-baseline-unavailable-${image}"
      continue
    fi
    component_changed="${tmp_dir}/component-changed-files-${image}"
    git -C "${REPO_ROOT}" diff --no-renames --name-only "${base_ref}" "${target_ref}" | sort -u >"${component_changed}"
    touch "${tmp_dir}/component-baseline-${image}"
    TRUSTED_COMPONENT_BASELINE=true
  done
  sort -u "${changed_file}" -o "${changed_file}"
elif [[ -n "${target_ref}" ]]; then
  printf 'release target is not a local commit; using fail-safe union image plan: %s\n' "${target_ref}" >&2
fi

for image in api controller drain_agent telemetry_agent image_cache edge app_ssh; do
  helm_drift="$(trim_field "$(image_helm_drift "${image}")")"
  case "${helm_drift}" in
    false) ;;
    true)
      if [[ ! -e "${tmp_dir}/component-baseline-${image}" ]]; then
        printf 'Helm/live image drift requires a trusted component baseline for %s\n' "${image}" >&2
        exit 1
      fi
      mark_image "${image}" "helm-live-image-drift"
      ;;
    *)
      printf 'Helm/live image drift flag must be true or false for %s: %s\n' \
        "${image}" "${helm_drift}" >&2
      exit 1
      ;;
  esac
done

INVALID_CHANGED_PATHS=false
while IFS= read -r raw_file; do
  [[ -n "${raw_file}" ]] || continue
  if ! release_file_path_is_canonical "${raw_file}"; then
    printf 'release changed path is not canonical; using fail-safe all-image plan: %s\n' "${raw_file}" >&2
    INVALID_CHANGED_PATHS=true
  fi
done <"${changed_file}"

if [[ "${INVALID_CHANGED_PATHS}" == "true" ]]; then
  mark_all_images "unknown-change-set"
elif [[ ! -s "${changed_file}" ]]; then
  if [[ "${TRUSTED_COMPONENT_BASELINE}" != "true" ]]; then
    mark_all_images "unknown-change-set"
  fi
else
  TELEMETRY_PRODUCTION_RELEASE_INTENT_CHANGED=false
  if grep_membership exact 'deploy/environments/production/telemetry/release.json' \
    "${changed_file}" 'Telemetry production release intent'; then
    verify_telemetry_production_release_intent
    TELEMETRY_PRODUCTION_RELEASE_INTENT_CHANGED=true
  fi
  CONTROLLER_PRODUCTION_RELEASE_INTENT_CHANGED=false
  if grep_membership exact 'deploy/environments/production/controller/release.json' \
    "${changed_file}" 'Controller production release intent'; then
    verify_controller_production_release_intent
    CONTROLLER_PRODUCTION_RELEASE_INTENT_CHANGED=true
  fi
  changed_path_count="$(sed '/^[[:space:]]*$/d' "${changed_file}" | sort -u | wc -l | tr -d '[:space:]')"
  if [[ "${TELEMETRY_PRODUCTION_RELEASE_INTENT_CHANGED}" == true && "${changed_path_count}" == 1 ]]; then
    mark_image telemetry_agent 'deploy/environments/production/telemetry/release.json'
    emit_plan
    exit 0
  fi
  if [[ "${CONTROLLER_PRODUCTION_RELEASE_INTENT_CHANGED}" == true ]] &&
    { [[ "${changed_path_count}" == 1 ]] || controller_declarative_atom_changed_set_is_exact "${changed_file}"; }; then
    mark_image controller 'deploy/environments/production/controller/release.json'
    emit_plan
    exit 0
  fi

  for image in api controller drain_agent telemetry_agent image_cache edge; do
    deps_file="${tmp_dir}/deps-${image}"
    : >"${deps_file}"
    while IFS= read -r command_path; do
      PWD="${REPO_ROOT}" go -C "${REPO_ROOT}" list -deps -f '{{if not .Standard}}{{.Dir}}{{end}}' "${command_path}" |
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

  # The live-to-target release diff is intentionally rooted at the core API
  # and controller refs. A node-local image can still be far behind those
  # refs, however, so its source changes may not appear in the release change
  # set at all. Rebuild the node-local image-cache when its own trusted live
  # baseline contains production source drift; otherwise a stale image can be
  # preserved forever. Other image domains retain their existing baseline
  # suppression semantics.
  for image in image_cache; do
    if [[ -e "${tmp_dir}/component-baseline-unavailable-${image}" ]]; then
      mark_image "${image}" "unknown-change-set"
    elif component_source_changed_since_baseline "${image}"; then
      mark_image "${image}" "component-source-drift"
    fi
  done

  while IFS= read -r raw_file; do
    file="${raw_file}"
    fixture_package_dir=""
    [[ -n "${file}" ]] || continue
    case "${file}" in
      deploy/environments/production/telemetry/release.json)
        mark_image telemetry_agent "${file}"
        continue
        ;;
      deploy/environments/production/controller/release.json)
        mark_image controller "${file}"
        continue
        ;;
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

    if [[ "${file}" == *.go ]]; then
      package_dir="$(dirname "${file}")"
      if ! mark_go_package_images "${package_dir}" "${file}"; then
        # A present source in a valid package that is absent from every image
        # dependency graph is proven not to enter a production image (for
        # example, a standalone release-tool command). Deleted files and
        # invalid packages cannot provide that target-tree proof, so they
        # retain the fail-safe all-Go-image plan.
        if [[ ! -f "${REPO_ROOT}/${file}" ]] || ! go_package_is_valid "${package_dir}"; then
          mark_all_go_images "${file}"
        fi
      fi
      continue
    fi

    # Go package testdata and fixtures are not part of a production binary
    # unless a non-test source embeds the exact target file. Only apply this
    # convention to non-Go assets in a package that go list can validate;
    # unknown internal files must continue through the fail-safe all-image rule
    # below.
    if fixture_package_dir="$(go_fixture_package_dir "${file}")" &&
      go_package_is_valid "${fixture_package_dir}"; then
      if go_fixture_is_runtime_asset "${file}" "${fixture_package_dir}"; then
        if ! mark_go_package_images "${fixture_package_dir}" "${file}"; then
          # A present embedded asset in a valid but unconsumed package cannot
          # enter an image. A deleted embedded asset has no equivalent proof.
          if [[ ! -e "${REPO_ROOT}/${file}" ]]; then
            mark_all_go_images "${file}"
          fi
        fi
      fi
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

emit_plan
