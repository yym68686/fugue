#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# GitHub Actions injects this into every step; individual output-protocol tests
# set their own file explicitly and all other fixtures must observe CLI output.
unset GITHUB_OUTPUT

fail() {
  printf '[test_release_domain_safety] ERROR: %s\n' "$*" >&2
  exit 1
}

assert_eq() {
  local got="$1"
  local want="$2"
  local label="$3"
  if [[ "${got}" != "${want}" ]]; then
    fail "${label}: got ${got}, want ${want}"
  fi
}

bash -n "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"
bash -n "${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh"
bash -n "${REPO_ROOT}/scripts/compute_control_plane_image_build_plan.sh"
bash -n "${REPO_ROOT}/scripts/compute_release_changed_files_from_live.sh"
bash -n "${REPO_ROOT}/scripts/build_control_plane_images.sh"
bash -n "${REPO_ROOT}/scripts/resolve_control_plane_live_images.sh"

python3 - "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh" <<'PY'
from pathlib import Path
import sys

source = Path(sys.argv[1]).read_text()
transaction_start = source.index("\nrun_bluegreen_release() {")
transaction_end = source.index("\nrun_front_ondelete_release()", transaction_start)
transaction = source[transaction_start:transaction_end]
if transaction.index("if ! run_smoke_urls") > transaction.index("FUGUE_PUBLIC_DATA_PLANE_ACTIVE_SLOTS_JSON="):
    raise SystemExit("blue-green transaction must pass final public smoke before publishing active slots")

main_start = source.index("\nmain() {")
blue_start = source.index('if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY}" == "blue-green" ]]', main_start)
blue_end = source.index('if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY}" == "front-ondelete" ]]', blue_start)
blue_branch = source[blue_start:blue_end]
if blue_branch.index("run_bluegreen_release") > blue_branch.index("write_release_record"):
    raise SystemExit("blue-green release record must be written only after the transaction succeeds")
if "run_smoke_urls" in blue_branch:
    raise SystemExit("blue-green final smoke belongs inside the rollback-capable transaction")
PY

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  unset FUGUE_PUBLIC_DATA_PLANE_MIN_SMOKE_HOSTS
  FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS=
  if validate_bluegreen_smoke_configuration; then
    fail "blue-green smoke validation must reject an empty smoke set"
  fi

  FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS='https://api.example.test/healthz,https://api.example.test/ready'
  if validate_bluegreen_smoke_configuration; then
    fail "blue-green smoke validation must count distinct hostnames"
  fi

  FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS='http://api.example.test/healthz,https://app.example.test/healthz'
  if validate_bluegreen_smoke_configuration; then
    fail "blue-green smoke validation must reject non-HTTPS URLs"
  fi

  FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS='https://api.example.test/healthz,https://app.example.test/healthz'
  validate_bluegreen_smoke_configuration

  FUGUE_PUBLIC_DATA_PLANE_MIN_SMOKE_HOSTS=1
  if validate_bluegreen_smoke_configuration; then
    fail "blue-green smoke validation must not allow lowering the two-host safety floor"
  fi
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  FUGUE_NAMESPACE=fugue-system
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=false
  kubectl_calls=0
  container_patch_for_worker() { return 71; }
  kubectl_cmd() { kubectl_calls=$((kubectl_calls + 1)); }

  if patch_inactive_worker test-worker; then
    fail "inactive worker patch must propagate render failures"
  fi
  assert_eq "${kubectl_calls}" "0" "render failure must stop before kubectl patch"
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  FUGUE_NAMESPACE=fugue-system
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=false
  pod_names_for_daemonset() { printf 'worker-pod\n'; }
  kubectl_cmd() { return 72; }

  if delete_worker_pods test-worker; then
    fail "inactive worker replacement must propagate kubectl delete failures"
  fi
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  FUGUE_NAMESPACE=fugue-system
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=false
  kubectl_cmd() {
    [[ "$*" == *"get ds/test-worker"* ]] && return 1
    return 73
  }

  if wait_daemonset_ready test-worker; then
    fail "daemonset readiness must propagate rollout failures"
  fi
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  FUGUE_NAMESPACE=fugue-system
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=false
  FUGUE_EDGE_BLUE_GREEN_ACTIVE_SLOT_FILE=/var/lib/fugue/edge-blue-green/active-slot
  exec_calls=0
  ready_pods_for_daemonset() { printf 'front-pod-a\nfront-pod-b\n'; }
  kubectl_cmd() {
    exec_calls=$((exec_calls + 1))
    return 74
  }

  if write_front_active_slot test-front a; then
    fail "front slot write must propagate the first pod exec failure"
  fi
  assert_eq "${exec_calls}" "1" "front slot write must stop after the first exec failure"
)

if grep -q 'warning: post-deploy release guard' "${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh"; then
  fail "post-deploy release guard must be a hard gate, not warning-only"
fi
grep -q 'release guard blocked after robustness passed' "${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh" ||
  fail "post-deploy robustness gate must surface release guard blocks"

plan_value() {
  local output_file="$1"
  local key="$2"
  awk -F= -v key="${key}" '$1 == key {print substr($0, length(key) + 2); exit}' "${output_file}"
}

assert_build_plan() {
  local changed_files="$1"
  local label="$2"
  local output_file log_file
  shift 2

  output_file="$(mktemp)"
  log_file="$(mktemp)"
  GITHUB_OUTPUT="${output_file}" \
    FUGUE_RELEASE_CHANGED_FILES="${changed_files}" \
    FUGUE_RELEASE_CHANGED_FILES_SET=true \
    "${REPO_ROOT}/scripts/compute_control_plane_image_build_plan.sh" >"${log_file}"
  while [[ "$#" -gt 0 ]]; do
    assert_eq "$(plan_value "${output_file}" "$1")" "$2" "${label} $1"
    shift 2
  done
  rm -f "${output_file}" "${log_file}"
}

assert_build_plan \
  $'internal/controller/safe_rollout.go' \
  "controller-only build plan" \
  build_api false \
  build_controller true \
  build_drain_agent false \
  build_telemetry_agent false \
  build_image_cache false \
  build_edge false \
  build_app_ssh false

assert_build_plan \
  $'Dockerfile.edge' \
  "edge Dockerfile build plan" \
  build_api false \
  build_controller false \
  build_drain_agent false \
  build_telemetry_agent false \
  build_image_cache false \
  build_edge true \
  build_app_ssh false

assert_build_plan \
  $'internal/api/robustness_test.go' \
  "test-only build plan" \
  target_count 0 \
  build_api false \
  build_controller false \
  build_drain_agent false \
  build_telemetry_agent false \
  build_image_cache false \
  build_edge false \
  build_app_ssh false

assert_build_plan \
  $'internal/api/robustness.go\ninternal/api/robustness_test.go' \
  "api source plus test build plan" \
  target_count 1 \
  build_api true \
  build_controller false \
  build_drain_agent false \
  build_telemetry_agent false \
  build_image_cache false \
  build_edge false \
  build_app_ssh false

assert_build_plan \
  $'scripts/upgrade_fugue_control_plane.sh' \
  "script-only build plan" \
  target_count 0 \
  build_api false \
  build_controller false \
  build_edge false

assert_build_plan \
  '' \
  "missing live baseline rebuilds every image" \
  target_count 7 \
  build_api true \
  build_controller true \
  build_drain_agent true \
  build_telemetry_agent true \
  build_image_cache true \
  build_edge true \
  build_app_ssh true

COMPONENT_PLAN_REPO="$(mktemp -d)"
COMPONENT_PLAN_OUTPUT="$(mktemp)"
COMPONENT_PLAN_LOG="$(mktemp)"
git clone -q --shared "${REPO_ROOT}" "${COMPONENT_PLAN_REPO}"
git -C "${COMPONENT_PLAN_REPO}" config user.email test@fugue.invalid
git -C "${COMPONENT_PLAN_REPO}" config user.name fugue-test
COMPONENT_PLAN_BASE="$(git -C "${COMPONENT_PLAN_REPO}" rev-parse HEAD)"
printf '\n// component baseline edge fixture\n' >>"${COMPONENT_PLAN_REPO}/cmd/fugue-edge/main.go"
git -C "${COMPONENT_PLAN_REPO}" add cmd/fugue-edge/main.go
git -C "${COMPONENT_PLAN_REPO}" commit -q -m edge-change
printf '\n// component baseline build-plan fixture\n' >>"${COMPONENT_PLAN_REPO}/cmd/fugue-api/main.go"
git -C "${COMPONENT_PLAN_REPO}" add cmd/fugue-api/main.go
git -C "${COMPONENT_PLAN_REPO}" commit -q -m api-change
COMPONENT_PLAN_API_LIVE="$(git -C "${COMPONENT_PLAN_REPO}" rev-parse HEAD)"
printf '\ncomponent baseline fixture\n' >>"${COMPONENT_PLAN_REPO}/docs/fugue-platform-resilience-control-loop-plan.md"
git -C "${COMPONENT_PLAN_REPO}" add docs/fugue-platform-resilience-control-loop-plan.md
git -C "${COMPONENT_PLAN_REPO}" commit -q -m docs-change
COMPONENT_PLAN_TARGET="$(git -C "${COMPONENT_PLAN_REPO}" rev-parse HEAD)"
COMPONENT_PLAN_CHANGED="$(git -C "${COMPONENT_PLAN_REPO}" diff --name-only "${COMPONENT_PLAN_BASE}" "${COMPONENT_PLAN_TARGET}")"
GITHUB_OUTPUT="${COMPONENT_PLAN_OUTPUT}" \
  FUGUE_RELEASE_REPO_ROOT="${COMPONENT_PLAN_REPO}" \
  FUGUE_RELEASE_CHANGED_FILES="${COMPONENT_PLAN_CHANGED}" \
  FUGUE_RELEASE_CHANGED_FILES_SET=true \
  FUGUE_RELEASE_TARGET_REF="${COMPONENT_PLAN_TARGET}" \
  FUGUE_API_IMAGE_BASE_REF="${COMPONENT_PLAN_API_LIVE}" \
  FUGUE_EDGE_IMAGE_BASE_REF="${COMPONENT_PLAN_API_LIVE}" \
  "${REPO_ROOT}/scripts/compute_control_plane_image_build_plan.sh" >"${COMPONENT_PLAN_LOG}"
assert_eq "$(plan_value "${COMPONENT_PLAN_OUTPUT}" target_count)" "0" "current API component baseline suppresses stale union rebuild"
assert_eq "$(plan_value "${COMPONENT_PLAN_OUTPUT}" build_api)" "false" "current API component baseline build flag"
: >"${COMPONENT_PLAN_OUTPUT}"
GITHUB_OUTPUT="${COMPONENT_PLAN_OUTPUT}" \
  FUGUE_RELEASE_REPO_ROOT="${COMPONENT_PLAN_REPO}" \
  FUGUE_RELEASE_CHANGED_FILES="${COMPONENT_PLAN_CHANGED}" \
  FUGUE_RELEASE_CHANGED_FILES_SET=true \
  FUGUE_RELEASE_TARGET_REF="${COMPONENT_PLAN_TARGET}" \
  FUGUE_API_IMAGE_BASE_REF="${COMPONENT_PLAN_BASE}" \
  FUGUE_EDGE_IMAGE_BASE_REF="${COMPONENT_PLAN_API_LIVE}" \
  "${REPO_ROOT}/scripts/compute_control_plane_image_build_plan.sh" >"${COMPONENT_PLAN_LOG}"
assert_eq "$(plan_value "${COMPONENT_PLAN_OUTPUT}" target_count)" "1" "stale API component baseline still rebuilds"
assert_eq "$(plan_value "${COMPONENT_PLAN_OUTPUT}" build_api)" "true" "stale API component baseline build flag"
: >"${COMPONENT_PLAN_OUTPUT}"
COMPONENT_PLAN_CURRENT_CORE_CHANGED="$(git -C "${COMPONENT_PLAN_REPO}" diff --name-only "${COMPONENT_PLAN_API_LIVE}" "${COMPONENT_PLAN_TARGET}")"
GITHUB_OUTPUT="${COMPONENT_PLAN_OUTPUT}" \
  FUGUE_RELEASE_REPO_ROOT="${COMPONENT_PLAN_REPO}" \
  FUGUE_RELEASE_CHANGED_FILES="${COMPONENT_PLAN_CURRENT_CORE_CHANGED}" \
  FUGUE_RELEASE_CHANGED_FILES_SET=true \
  FUGUE_RELEASE_TARGET_REF="${COMPONENT_PLAN_TARGET}" \
  FUGUE_API_IMAGE_BASE_REF="${COMPONENT_PLAN_API_LIVE}" \
  FUGUE_EDGE_IMAGE_BASE_REF="${COMPONENT_PLAN_BASE}" \
  "${REPO_ROOT}/scripts/compute_control_plane_image_build_plan.sh" >"${COMPONENT_PLAN_LOG}"
assert_eq "$(plan_value "${COMPONENT_PLAN_OUTPUT}" target_count)" "0" "stale held edge diff cannot enter an unrelated build plan"
assert_eq "$(plan_value "${COMPONENT_PLAN_OUTPUT}" build_edge)" "false" "stale held edge build flag"
rm -rf "${COMPONENT_PLAN_REPO}"
rm -f "${COMPONENT_PLAN_OUTPUT}" "${COMPONENT_PLAN_LOG}"

RESOLVE_TEST_DIR="$(mktemp -d)"
RESOLVE_TEST_OUTPUT="$(mktemp)"
cat >"${RESOLVE_TEST_DIR}/kubectl" <<'SH'
#!/usr/bin/env bash
cat <<'JSON'
{"spec":{"template":{"spec":{"containers":[
  {"name":"api","image":"ghcr.io/acme/fugue-api:api-live"},
  {"name":"controller","image":"ghcr.io/acme/fugue-controller:controller-live","env":[
    {"name":"FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY","value":"ghcr.io/acme/fugue-drain-agent"},
    {"name":"FUGUE_DRAIN_AGENT_IMAGE_TAG","value":"drain-live"}
  ]},
  {"name":"telemetry-agent","image":"ghcr.io/acme/fugue-telemetry-agent:telemetry-live"},
  {"name":"image-cache","image":"ghcr.io/acme/fugue-image-cache:image-cache-live"},
  {"name":"edge","image":"ghcr.io/acme/fugue-edge:edge-live"}
]}}}}
JSON
SH
chmod +x "${RESOLVE_TEST_DIR}/kubectl"
PATH="${RESOLVE_TEST_DIR}:${PATH}" \
  GITHUB_OUTPUT="${RESOLVE_TEST_OUTPUT}" \
  FUGUE_IMAGE_TAG=fallback-target \
  "${REPO_ROOT}/scripts/resolve_control_plane_live_images.sh" >/dev/null
release_baseline_tags="$(
  awk '
    $0 == "release_baseline_tags<<EOF" { capture = 1; next }
    capture && $0 == "EOF" { exit }
    capture { print }
  ' "${RESOLVE_TEST_OUTPUT}"
)"
assert_eq "${release_baseline_tags}" $'api-live\ncontroller-live' "release baseline only includes core control-plane images"
assert_eq "$(plan_value "${RESOLVE_TEST_OUTPUT}" api_image_baseline_ref)" "api-live" "API image baseline ref"
assert_eq "$(plan_value "${RESOLVE_TEST_OUTPUT}" controller_image_baseline_ref)" "controller-live" "controller image baseline ref"
assert_eq "$(plan_value "${RESOLVE_TEST_OUTPUT}" edge_image_baseline_ref)" "edge-live" "edge image baseline ref"
rm -rf "${RESOLVE_TEST_DIR}"
rm -f "${RESOLVE_TEST_OUTPUT}"

export FUGUE_UPGRADE_LIB_ONLY=true
# shellcheck source=scripts/upgrade_fugue_control_plane.sh
source "${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh"

(
  curl_calls=0
  curl() {
    local output_file=""
    curl_calls=$((curl_calls + 1))
    while [[ "$#" -gt 0 ]]; do
      if [[ "$1" == "-o" ]]; then
        shift
        output_file="$1"
      fi
      shift
    done
    if (( curl_calls < 3 )); then
      return 92
    fi
    printf '{"status":{"pass":true}}' >"${output_file}"
  }
  output_file="$(mktemp)"
  FUGUE_RELEASE_STATUS_TRANSPORT_ATTEMPTS=3
  FUGUE_RELEASE_STATUS_TRANSPORT_RETRY_DELAY_SECONDS=0
  release_status_request "https://api.example.test/v1/admin/robustness/status" "test-token" "${output_file}" ||
    fail "release status transport fetch must recover within its bounded retry budget"
  assert_eq "${curl_calls}" "3" "release status transport retry attempts"
  assert_eq "$(cat "${output_file}")" '{"status":{"pass":true}}' "release status transport response"
  rm -f "${output_file}"
)

(
  curl_calls=0
  curl() {
    curl_calls=$((curl_calls + 1))
    return 92
  }
  output_file="$(mktemp)"
  FUGUE_RELEASE_STATUS_TRANSPORT_ATTEMPTS=2
  FUGUE_RELEASE_STATUS_TRANSPORT_RETRY_DELAY_SECONDS=0
  if release_status_request "https://api.example.test/v1/admin/robustness/status" "test-token" "${output_file}"; then
    fail "release status transport fetch must fail closed after exhausting retries"
  fi
  assert_eq "${curl_calls}" "2" "release status exhausted retry attempts"
  rm -f "${output_file}"
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  FUGUE_NAMESPACE=fugue-system
  FUGUE_RELEASE_FULLNAME=fugue-fugue
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID=test-release
  FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN=false
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=false
  FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS=
  export FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID

  patched=""
  switched=""

  enable_bluegreen_chart_mode() { :; }
  bluegreen_worker_bases() {
    printf 'fugue-fugue-edge\n'
    printf 'fugue-fugue-edge-dynamic\n'
  }
  wait_daemonset_ready() { :; }
  daemonset_ready_counts() {
    case "$1" in
      *edge-dynamic-front|*edge-dynamic-worker-*)
        printf '0\t0\t0'
        ;;
      *)
        printf '1\t1\t0'
        ;;
    esac
  }
  current_active_slot() {
    [[ "$1" != *dynamic* ]] || fail "dynamic base with desired=0 must not read active slot"
    printf 'b'
  }
  patch_inactive_worker() {
    [[ "$1" != *dynamic* ]] || fail "dynamic base with desired=0 must not patch workers"
    patched="${patched}${1};"
  }
  delete_worker_pods() { :; }
  worker_https_port() { printf '18443'; }
  check_worker_tcp() { :; }
  check_worker_https_smoke() { :; }
  capture_daemonset_pods() { printf 'stable-pods\n'; }
  write_front_active_slot() {
    [[ "$1" != *dynamic* ]] || fail "dynamic base with desired=0 must not switch front slot"
    switched="${switched}${1}:$2;"
  }

  run_bluegreen_release
  assert_eq "${patched}" "fugue-fugue-edge-worker-a;" "blue-green release must patch only scheduled bases"
  assert_eq "${switched}" "fugue-fugue-edge-front:a;" "blue-green release must switch only scheduled bases"
  assert_eq "${FUGUE_PUBLIC_DATA_PLANE_ACTIVE_SLOTS_JSON}" '{"fugue-fugue-edge":"a"}' "blue-green release record must omit unscheduled dynamic base"
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  FUGUE_NAMESPACE=fugue-system
  FUGUE_RELEASE_FULLNAME=fugue-fugue
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID=test-release
  FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN=false
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=false
  FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS=
  export FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID

  events=""
  enable_bluegreen_chart_mode() { :; }
  bluegreen_worker_bases() {
    printf 'fugue-fugue-edge\n'
    printf 'fugue-fugue-edge-country-de\n'
  }
  wait_daemonset_ready() { :; }
  daemonset_desired_count() { printf '1'; }
  current_active_slot() { printf 'b'; }
  patch_inactive_worker() { events="${events}prepare:$1;"; }
  delete_worker_pods() { :; }
  worker_https_port() { printf '18443'; }
  check_worker_tcp() { :; }
  check_worker_https_smoke() { :; }
  capture_daemonset_pods() { printf 'stable-pods\n'; }
  write_front_active_slot() { events="${events}switch:$1:$2;"; }
  check_public_smoke_on_front_nodes() { events="${events}front-smoke:$1;"; }
  run_smoke_urls() { events="${events}public-smoke;"; }

  run_bluegreen_release
  assert_eq "${events}" "prepare:fugue-fugue-edge-worker-a;prepare:fugue-fugue-edge-country-de-worker-a;switch:fugue-fugue-edge-front:a;front-smoke:fugue-fugue-edge-front;switch:fugue-fugue-edge-country-de-front:a;front-smoke:fugue-fugue-edge-country-de-front;public-smoke;" "blue-green release must prepare every candidate, validate each switched front, then run final public smoke"
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  FUGUE_NAMESPACE=fugue-system
  FUGUE_RELEASE_FULLNAME=fugue-fugue
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID=test-release
  FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN=false
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=false
  FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS=
  export FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID

  smoke_checks=0
  switches=0
  enable_bluegreen_chart_mode() { :; }
  bluegreen_worker_bases() {
    printf 'fugue-fugue-edge\n'
    printf 'fugue-fugue-edge-country-de\n'
  }
  wait_daemonset_ready() { :; }
  daemonset_desired_count() { printf '1'; }
  current_active_slot() { printf 'b'; }
  patch_inactive_worker() { :; }
  delete_worker_pods() { :; }
  worker_https_port() { printf '18443'; }
  check_worker_tcp() { :; }
  check_worker_https_smoke() {
    smoke_checks=$((smoke_checks + 1))
    (( smoke_checks < 2 ))
  }
  capture_daemonset_pods() { printf 'stable-pods\n'; }
  write_front_active_slot() { switches=$((switches + 1)); }

  if run_bluegreen_release; then
    fail "blue-green release must fail when any candidate smoke fails"
  fi
  assert_eq "${switches}" "0" "candidate failure must occur before every slot switch"
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  FUGUE_NAMESPACE=fugue-system
  FUGUE_RELEASE_FULLNAME=fugue-fugue
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID=test-release
  FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN=false
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=false
  FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS=
  export FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID

  events=""
  public_smoke_calls=0
  enable_bluegreen_chart_mode() { :; }
  bluegreen_worker_bases() {
    printf 'fugue-fugue-edge\n'
    printf 'fugue-fugue-edge-country-de\n'
  }
  wait_daemonset_ready() { :; }
  daemonset_desired_count() { printf '1'; }
  current_active_slot() { printf 'b'; }
  patch_inactive_worker() { :; }
  delete_worker_pods() { :; }
  worker_https_port() { printf '18443'; }
  check_worker_tcp() { :; }
  check_worker_https_smoke() { :; }
  check_public_smoke_on_front_nodes() { :; }
  capture_daemonset_pods() { printf 'stable-pods\n'; }
  write_front_active_slot() { events="${events}${1}:$2;"; }
  run_smoke_urls() {
    public_smoke_calls=$((public_smoke_calls + 1))
    (( public_smoke_calls > 1 ))
  }

  if run_bluegreen_release; then
    fail "blue-green release must fail when final public smoke fails"
  fi
  assert_eq "${events}" "fugue-fugue-edge-front:a;fugue-fugue-edge-country-de-front:a;fugue-fugue-edge-country-de-front:b;fugue-fugue-edge-front:b;" "final smoke failure must restore every switched front in reverse order"
  assert_eq "${public_smoke_calls}" "2" "blue-green abort must verify public smoke after rollback"
  assert_eq "${FUGUE_PUBLIC_DATA_PLANE_ACTIVE_SLOTS_JSON:-}" "" "failed blue-green release must not publish proposed active slots"
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  FUGUE_NAMESPACE=fugue-system
  FUGUE_RELEASE_FULLNAME=fugue-fugue
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID=test-release
  FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN=false
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=false
  FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS=
  export FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID

  events=""
  front_smoke_calls=0
  enable_bluegreen_chart_mode() { :; }
  bluegreen_worker_bases() {
    printf 'fugue-fugue-edge\n'
    printf 'fugue-fugue-edge-country-de\n'
  }
  wait_daemonset_ready() { :; }
  daemonset_desired_count() { printf '1'; }
  current_active_slot() { printf 'b'; }
  patch_inactive_worker() { :; }
  delete_worker_pods() { :; }
  worker_https_port() { printf '18443'; }
  check_worker_tcp() { :; }
  check_worker_https_smoke() { :; }
  capture_daemonset_pods() { printf 'stable-pods\n'; }
  write_front_active_slot() { events="${events}${1}:$2;"; }
  check_public_smoke_on_front_nodes() {
    front_smoke_calls=$((front_smoke_calls + 1))
    return 1
  }
  run_smoke_urls() { :; }

  if run_bluegreen_release; then
    fail "blue-green release must fail when switched front smoke fails"
  fi
  assert_eq "${events}" "fugue-fugue-edge-front:a;fugue-fugue-edge-front:b;" "front smoke failure must restore the touched front before switching another base"
  assert_eq "${front_smoke_calls}" "2" "front smoke failure must stop further switches and verify the restored front"
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  events=""
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=false
  write_front_active_slot() {
    events="${events}${1}:$2;"
    [[ "$1" != "front-de" ]]
  }
  wait_daemonset_ready() { :; }
  check_public_smoke_on_front_nodes() { :; }

  if rollback_bluegreen_fronts front-us b front-de b; then
    fail "blue-green rollback must report a failed front restore"
  fi
  assert_eq "${events}" "front-de:b;front-us:b;" "rollback must continue restoring remaining fronts after one restore fails"
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  FUGUE_EDGE_BLUE_GREEN_DEFAULT_ACTIVE_SLOT=a
  active_slot_from_front() { :; }
  active_slot_from_record() { :; }
  assert_eq "$(current_active_slot "fugue-fugue-edge-dynamic" "fugue-fugue-edge-dynamic-front")" "a" "missing dynamic release record must fall back to default slot"

  FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS='https://api.fugue.pro/healthz; https://oaix.fugue.pro/healthz,https://argus.fugue.pro/healthz'
  assert_eq "$(public_data_plane_smoke_urls | wc -l | tr -d ' ')" "3" "public data-plane smoke URL parser must split comma and semicolon"
  assert_eq "$(worker_smoke_targets)" $'api.fugue.pro\t/healthz\noaix.fugue.pro\t/healthz\nargus.fugue.pro\t/healthz' "worker smoke targets must preserve every HTTPS smoke URL"
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=false
  FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS='https://api.example.test/healthz,https://app.example.test/ready?full=1'
  FUGUE_PUBLIC_DATA_PLANE_SMOKE_ATTEMPTS=1
  calls=""
  node_ips_for_daemonset() {
    printf '192.0.2.10\n192.0.2.11\n'
  }
  curl() {
    calls="${calls}${*: -1};"
  }

  check_worker_https_smoke test-worker 18443
  assert_eq "${calls}" "https://api.example.test:18443/healthz;https://api.example.test:18443/healthz;https://app.example.test:18443/ready?full=1;https://app.example.test:18443/ready?full=1;" "inactive worker smoke must cover every configured URL on every scheduled node"
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=false
  FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS='https://fail.example.test/healthz,https://pass.example.test/healthz'
  FUGUE_PUBLIC_DATA_PLANE_SMOKE_ATTEMPTS=1
  curl_calls=0
  node_ips_for_daemonset() { printf '192.0.2.10\n'; }
  curl() {
    curl_calls=$((curl_calls + 1))
    [[ "${*: -1}" != *fail.example.test* ]]
  }

  if check_worker_https_smoke test-worker 18443; then
    fail "inactive worker smoke must not let a later URL mask an earlier failure"
  fi
  assert_eq "${curl_calls}" "1" "inactive worker smoke must stop at the first exhausted failure"
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=false
  tcp_checks=0
  node_ips_for_daemonset() { printf '192.0.2.10\n192.0.2.11\n'; }
  python3() {
    tcp_checks=$((tcp_checks + 1))
    (( tcp_checks > 1 ))
  }

  if check_worker_tcp test-worker 18443; then
    fail "inactive worker TCP check must not let a later node mask an earlier failure"
  fi
  assert_eq "${tcp_checks}" "1" "inactive worker TCP check must stop at the first failure"
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=false
  FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS='https://fail.example.test/healthz,https://pass.example.test/healthz'
  FUGUE_PUBLIC_DATA_PLANE_SMOKE_ATTEMPTS=1
  curl_calls=0
  node_ips_for_daemonset() { printf '192.0.2.10\n'; }
  curl() {
    curl_calls=$((curl_calls + 1))
    [[ "${*: -1}" != *fail.example.test* ]]
  }

  if check_public_smoke_on_front_nodes test-front; then
    fail "front smoke must not let a later URL mask an earlier failure"
  fi
  assert_eq "${curl_calls}" "1" "front smoke must stop at the first exhausted failure"
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS='https://fail.example.test/healthz,https://pass.example.test/healthz'
  FUGUE_PUBLIC_DATA_PLANE_SMOKE_ATTEMPTS=1
  curl_calls=0
  curl() {
    curl_calls=$((curl_calls + 1))
    [[ "${*: -1}" != *fail.example.test* ]]
  }

  if run_smoke_urls; then
    fail "public smoke must not let a later URL mask an earlier failure"
  fi
  assert_eq "${curl_calls}" "1" "public smoke must stop at the first exhausted failure"
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  FUGUE_PUBLIC_DATA_PLANE_SMOKE_ATTEMPTS=3
  FUGUE_PUBLIC_DATA_PLANE_SMOKE_RETRY_DELAY_SECONDS=0
  curl_calls=0
  curl() {
    curl_calls=$((curl_calls + 1))
    (( curl_calls >= 3 ))
  }
  sleep() { :; }

  smoke_curl_with_retry "transient test" -fsS https://example.test/healthz
  assert_eq "${curl_calls}" "3" "public data-plane smoke must recover inside its bounded retry budget"
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  FUGUE_PUBLIC_DATA_PLANE_SMOKE_ATTEMPTS=2
  FUGUE_PUBLIC_DATA_PLANE_SMOKE_RETRY_DELAY_SECONDS=0
  curl_calls=0
  curl() {
    curl_calls=$((curl_calls + 1))
    return 22
  }
  sleep() { :; }

  if smoke_curl_with_retry "persistent test" -fsS https://example.test/healthz; then
    fail "public data-plane smoke must fail closed after exhausting retries"
  fi
  assert_eq "${curl_calls}" "2" "public data-plane smoke exhausted retry attempts"
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

  FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS='https://fail.example.test/healthz'
  FUGUE_PUBLIC_DATA_PLANE_ACTIVE_SMOKE_RETRY_DELAY_SECONDS=0
  unset FUGUE_PUBLIC_DATA_PLANE_SMOKE_ATTEMPTS
  unset FUGUE_PUBLIC_DATA_PLANE_ACTIVE_SMOKE_ATTEMPTS
  curl_calls=0
  curl() {
    curl_calls=$((curl_calls + 1))
    return 22
  }
  sleep() { :; }

  if run_smoke_urls; then
    fail "active public smoke must fail after its short retry budget"
  fi
  assert_eq "${curl_calls}" "3" "active public smoke default retry attempts"
)

(
  ORIGINAL_PATH="${PATH}"
  TMP_CURL_DIR="$(mktemp -d)"
  cat >"${TMP_CURL_DIR}/curl" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
headers=""
out=""
writeout=""
url=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    -D)
      headers="$2"
      shift 2
      ;;
    -o)
      out="$2"
      shift 2
      ;;
    -w)
      writeout="$2"
      shift 2
      ;;
    -H|--header)
      shift 2
      ;;
    http://*|https://*)
      url="$1"
      shift
      ;;
    *)
      shift
      ;;
  esac
done
case "${url}" in
  */v1/discovery/bundle)
    [[ -z "${headers}" ]] || printf 'ETag: "discovery_test"\r\n' >"${headers}"
    body='{"generation":"generation_test","schema_version":"1.0","signature":"sig"}'
    ;;
  */v1/admin/platform/autonomy/status)
    body='{"status":{"pass":false,"block_rollout":true,"control_plane_store":{"permission_verification_status":"passed","block_rollout":false},"checks":[{"name":"dns","pass":false,"message":"active=2 total=2"}]}}'
    ;;
  */v1/edge/nodes)
    body='{"nodes":[{"id":"edge-us-1","edge_group_id":"edge-group-country-us","healthy":true,"status":"healthy","last_seen_at":"2999-01-01T00:00:00Z","caddy_route_count":1,"cache_status":"ready"}]}'
    ;;
  */v1/dns/nodes)
    body='{"nodes":[{"id":"dns-us-1","edge_group_id":"edge-group-country-us","healthy":true,"status":"degraded","dns_bundle_version":"dnsgen_us","record_count":40,"cache_status":"stale","cache_write_errors":0,"last_error":""}]}'
    ;;
  */v1/cluster/node-policies/status)
    body='{"node_policies":[]}'
    ;;
  *)
    printf 'unexpected curl URL: %s\n' "${url}" >&2
    exit 22
    ;;
esac
if [[ -n "${out}" ]]; then
  printf '%s' "${body}" >"${out}"
else
  printf '%s' "${body}"
fi
[[ -z "${writeout}" ]] || printf '200'
SH
  chmod +x "${TMP_CURL_DIR}/curl"
  PATH="${TMP_CURL_DIR}:${PATH}"
  FUGUE_API_URL="https://api.example.test"
  FUGUE_API_KEY="test-token"
  FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT=
  FUGUE_REGISTRY_PULL_BASE=
  KUBECTL=
  run_release_preflight || fail "release preflight must allow serving degraded DNS nodes"
  PATH="${ORIGINAL_PATH}"
  rm -rf "${TMP_CURL_DIR}"
)

assert_eq "$(image_ref_repository 'ghcr.io/acme/fugue-edge:sha123')" "ghcr.io/acme/fugue-edge" "repository parses tagged ghcr image"
assert_eq "$(image_ref_tag 'ghcr.io/acme/fugue-edge:sha123')" "sha123" "tag parses tagged ghcr image"
assert_eq "$(image_ref_repository 'localhost:5000/acme/fugue-edge:sha123')" "localhost:5000/acme/fugue-edge" "repository keeps registry port"
assert_eq "$(image_ref_tag 'localhost:5000/acme/fugue-edge')" "latest" "missing tag defaults to latest"
assert_eq "$(image_ref_repository 'ghcr.io/acme/fugue-edge@sha256:abc')" "ghcr.io/acme/fugue-edge" "repository strips digest"

PUBLIC_DATA_PLANE_PRESERVED=false
public_data_plane_daemonset_rollout_wait_required || fail "non-preserved public data-plane releases must wait for edge/DNS daemonsets"
PUBLIC_DATA_PLANE_PRESERVED=true
if public_data_plane_daemonset_rollout_wait_required; then
  fail "preserved public data-plane daemonsets must not block control-plane rollout"
fi
PUBLIC_DATA_PLANE_PRESERVED=false

unset FUGUE_PUBLIC_DATA_PLANE_AUTO_FRONT_RELEASE
if public_data_plane_auto_front_release_enabled; then
  fail "public front auto release must be disabled by default"
fi
FUGUE_PUBLIC_DATA_PLANE_AUTO_FRONT_RELEASE=false
if public_data_plane_auto_front_release_enabled; then
  fail "public front auto release must stay disabled when explicitly false"
fi
FUGUE_PUBLIC_DATA_PLANE_AUTO_FRONT_RELEASE=true
public_data_plane_auto_front_release_enabled || fail "public front auto release must require an explicit true opt-in"
unset FUGUE_PUBLIC_DATA_PLANE_AUTO_FRONT_RELEASE

ORIGINAL_PATH="${PATH}"
TMP_CURL_DIR="$(mktemp -d)"
cat >"${TMP_CURL_DIR}/curl" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
out=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    -o)
      out="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
[[ -n "${out}" ]]
printf '%s' "${TEST_CURL_RESPONSE_JSON:-${TEST_PLATFORM_AUTONOMY_JSON}}" >"${out}"
SH
chmod +x "${TMP_CURL_DIR}/curl"
PATH="${TMP_CURL_DIR}:${PATH}"
FUGUE_API_URL="https://api.example.test"
FUGUE_API_KEY="test-token"
export TEST_PLATFORM_AUTONOMY_JSON='{"status":{"pass":true,"block_rollout":false,"checks":[]}}'
assert_eq "$(platform_autonomy_status_summary)" "pass=true block_rollout=false" "platform autonomy pass summary"
export TEST_PLATFORM_AUTONOMY_JSON='{"status":{"pass":false,"block_rollout":true,"checks":[{"name":"edge","pass":false,"message":"warming"}]}}'
if autonomy_output="$(platform_autonomy_status_summary)"; then
  fail "failed platform autonomy status must return non-zero"
fi
assert_eq "${autonomy_output}" "pass=false block_rollout=true; failing=edge: warming" "platform autonomy failure summary"
unset TEST_CURL_RESPONSE_JSON

export TEST_CURL_RESPONSE_JSON='{"status":{"pass":false,"block_rollout":true,"checks":[{"name":"node_policy","subject":"platform-autonomy","pass":false,"severity":"degraded","observed":"pass=false count=6"},{"name":"route_active","subject":"route:example","pass":true,"severity":"block_publish","observed":"route present"}],"incidents":[{"id":"robust_existing","severity":"degraded","subject":"platform-autonomy","check_name":"node_policy","observed":"pass=false count=6"}]}}'
if robustness_output="$(robustness_status_summary)"; then
  fail "strict robustness status without a baseline must return non-zero when block_rollout=true"
fi
[[ "${robustness_output}" == *"block_rollout=true"* ]] || fail "strict robustness failure summary must include block_rollout=true"
ROBUSTNESS_HEALTH_GATE_BASELINE_FILE=""
capture_pre_deploy_robustness_baseline
[[ -n "${ROBUSTNESS_HEALTH_GATE_BASELINE_FILE}" && -f "${ROBUSTNESS_HEALTH_GATE_BASELINE_FILE}" ]] || fail "pre-deploy robustness baseline must be captured"
assert_eq "$(robustness_status_summary "${ROBUSTNESS_HEALTH_GATE_BASELINE_FILE}")" "pass=false block_rollout=false checks=2 incidents=1 baseline_incidents=1 new_incidents=0; raw_block_rollout=true tolerated_by_baseline=true" "matching robustness baseline must tolerate existing incidents"

MONOTONIC_BASELINE_FILE="$(mktemp)"
printf '%s' '{"status":{"generated_at":"2026-07-09T22:00:00Z","pass":false,"block_rollout":true,"checks":[{"name":"route_active","subject":"route:example","pass":false,"severity":"block_publish","observed":"error_rate=0.01 affected_count=1","evidence":{"affected_edges":"edge-a"}}],"incidents":[]}}' >"${MONOTONIC_BASELINE_FILE}"
export TEST_CURL_RESPONSE_JSON='{"status":{"generated_at":"2026-07-09T22:01:00Z","pass":false,"block_rollout":true,"checks":[{"name":"route_active","subject":"route:example","pass":false,"severity":"block_publish","observed":"error_rate=0.02 affected_count=1","evidence":{"affected_edges":"edge-a"}}],"incidents":[]}}'
if robustness_output="$(robustness_status_summary "${MONOTONIC_BASELINE_FILE}")"; then
  fail "matching blocker identity must not tolerate a quantitative regression"
fi
[[ "${robustness_output}" == *"regressed_blockers="* && "${robustness_output}" == *"error_rate 0.01->0.02"* ]] ||
  fail "quantitative blocker regression must be reported, got ${robustness_output}"
export TEST_CURL_RESPONSE_JSON='{"status":{"generated_at":"2026-07-09T22:01:00Z","pass":false,"block_rollout":true,"checks":[{"name":"route_active","subject":"route:example","pass":false,"severity":"block_publish","observed":"error_rate=0.01 affected_count=2","evidence":{"affected_edges":"edge-a,edge-b"}}],"incidents":[]}}'
if robustness_output="$(robustness_status_summary "${MONOTONIC_BASELINE_FILE}")"; then
  fail "matching blocker identity must not tolerate affected scope expansion"
fi
[[ "${robustness_output}" == *"affected_count 1->2"* && "${robustness_output}" == *"affected_edges expanded by 1"* ]] ||
  fail "affected scope blocker regression must be reported, got ${robustness_output}"

printf '%s' '{"status":{"generated_at":"2026-07-09T22:00:00Z","pass":false,"block_rollout":false,"checks":[{"name":"route_active","subject":"route:example","pass":false,"severity":"degraded","observed":"error_rate=0.01"}],"incidents":[]}}' >"${MONOTONIC_BASELINE_FILE}"
export TEST_CURL_RESPONSE_JSON='{"status":{"generated_at":"2026-07-09T22:01:00Z","pass":false,"block_rollout":true,"checks":[{"name":"route_active","subject":"route:example","pass":false,"severity":"block_publish","observed":"error_rate=0.01"}],"incidents":[]}}'
if robustness_output="$(robustness_status_summary "${MONOTONIC_BASELINE_FILE}")"; then
  fail "matching blocker identity must not tolerate severity escalation"
fi
[[ "${robustness_output}" == *"severity degraded->block_publish"* ]] ||
  fail "severity blocker regression must be reported, got ${robustness_output}"

printf '%s' '{"status":{"generated_at":"2026-07-09T20:00:00Z","pass":false,"block_rollout":true,"checks":[{"name":"route_active","subject":"route:example","pass":false,"severity":"block_publish","observed":"error_rate=0.01"}],"incidents":[]}}' >"${MONOTONIC_BASELINE_FILE}"
export TEST_CURL_RESPONSE_JSON='{"status":{"generated_at":"2026-07-09T22:01:00Z","pass":false,"block_rollout":true,"checks":[{"name":"route_active","subject":"route:example","pass":false,"severity":"block_publish","observed":"error_rate=0.01"}],"incidents":[]}}'
if robustness_output="$(FUGUE_ROBUSTNESS_BASELINE_MAX_AGE_SECONDS=60 robustness_status_summary "${MONOTONIC_BASELINE_FILE}")"; then
  fail "expired robustness baseline must not tolerate existing blockers"
fi
[[ "${robustness_output}" == *"baseline_expired=true"* ]] ||
  fail "expired robustness baseline must be reported, got ${robustness_output}"
rm -f "${MONOTONIC_BASELINE_FILE}"

export TEST_CURL_RESPONSE_JSON='{"status":{"pass":false,"block_rollout":true,"checks":[{"name":"node_policy","subject":"platform-autonomy","pass":false,"severity":"degraded","observed":"pass=false count=7"},{"name":"route_active","subject":"route:example","pass":false,"severity":"block_publish","message":"missing route"}],"incidents":[{"id":"robust_existing_changed","severity":"degraded","subject":"platform-autonomy","check_name":"node_policy","observed":"pass=false count=7"},{"id":"robust_new","severity":"block_publish","subject":"route:example","check_name":"route_active","message":"missing route"}]}}'
if robustness_output="$(robustness_status_summary "${ROBUSTNESS_HEALTH_GATE_BASELINE_FILE}")"; then
  fail "robustness baseline must fail on new incidents"
fi
[[ "${robustness_output}" == *"new_incidents=1"* ]] || fail "robustness baseline failure must report new incident count"
[[ "${robustness_output}" == *"new_blockers=route_active(route:example): missing route"* ]] || fail "robustness baseline failure must report new block_publish blocker"
export TEST_CURL_RESPONSE_JSON='{"status":{"pass":false,"block_rollout":true,"checks":[{"name":"node_policy","subject":"platform-autonomy","pass":false,"severity":"degraded","observed":"pass=false count=7"},{"name":"app_continuity_invariant","subject":"app:example","pass":false,"severity":"block_publish","message":"ready replicas 0 below desired 1"}],"incidents":[{"id":"robust_existing_changed","severity":"degraded","subject":"platform-autonomy","check_name":"node_policy","observed":"pass=false count=7"},{"id":"robust_introduced","severity":"block_publish","subject":"app:example","check_name":"app_continuity_invariant","message":"ready replicas 0 below desired 1"}]}}'
robustness_output="$(robustness_status_summary "${ROBUSTNESS_HEALTH_GATE_BASELINE_FILE}")" || fail "tenant workload continuity must not fail a mixed-version baseline rollout"
[[ "${robustness_output}" == *"block_rollout=false"* ]] || fail "tenant workload continuity must not keep effective block_rollout=true"
[[ "${robustness_output}" == *"raw_block_rollout=true ignored_by_release_scope=true"* ]] || fail "tenant workload continuity summary must report raw block_rollout was ignored"
[[ "${robustness_output}" == *"ignored_tenant_workload_blockers=1"* ]] || fail "tenant workload blocker count must be reported"
[[ "${robustness_output}" == *"ignored_tenant_workload_incidents=1"* ]] || fail "tenant workload incident count must be reported"
export TEST_CURL_RESPONSE_JSON='{"status":{"pass":false,"block_rollout":true,"checks":[{"name":"app_continuity_invariant","subject":"app:example","pass":false,"severity":"block_publish","message":"ready replicas 0 below desired 1"}],"incidents":[{"id":"robust_app","severity":"block_publish","subject":"app:example","check_name":"app_continuity_invariant","message":"ready replicas 0 below desired 1"}]}}'
robustness_output="$(robustness_status_summary)" || fail "tenant workload continuity must not fail strict rollout mode"
[[ "${robustness_output}" == *"block_rollout=false"* ]] || fail "strict tenant workload continuity summary must not block rollout"
[[ "${robustness_output}" == *"ignored_tenant_workload_blockers=1"* ]] || fail "strict tenant workload continuity blocker count must be reported"
export TEST_CURL_RESPONSE_JSON='{"status":{"pass":false,"block_rollout":true,"checks":[{"name":"app_continuity_invariant","subject":"app:example","pass":false,"severity":"block_publish","message":"ready replicas 0 below desired 1","evidence":{"release_signal_id":"sig_example","release_gate_scope":"control_plane","report_only":"false"}}],"incidents":[{"id":"robust_app_signal","severity":"block_publish","subject":"app:example","check_name":"app_continuity_invariant","message":"ready replicas 0 below desired 1","evidence":{"release_signal_id":"sig_example","release_gate_scope":"control_plane","report_only":"false"}}]}}'
if robustness_output="$(robustness_status_summary)"; then
  fail "explicit control-plane release signal must fail strict rollout mode"
fi
[[ "${robustness_output}" == *"block_rollout=true"* ]] || fail "explicit release signal summary must keep block_rollout=true"
[[ "${robustness_output}" == *"blockers=app_continuity_invariant(app:example): ready replicas 0 below desired 1"* ]] || fail "explicit release signal must be reported as a blocker"
rm -f "${ROBUSTNESS_HEALTH_GATE_BASELINE_FILE}"
ROBUSTNESS_HEALTH_GATE_BASELINE_FILE=""
unset TEST_CURL_RESPONSE_JSON
PATH="${ORIGINAL_PATH}"
rm -rf "${TMP_CURL_DIR}"

FUGUE_RELEASE_CHANGED_FILES=$'cmd/fugue-api/main.go\ninternal/api/server.go\n.github/workflows/deploy-control-plane.yml'
if public_data_plane_changed; then
  fail "control-plane-only changes must not mark public data-plane changed"
fi
if node_local_build_plane_changed; then
  fail "control-plane-only changes must not mark build-plane changed"
fi
if node_local_build_plane_preflight_override_allowed; then
  fail "generic control-plane changes must not bypass registry/node-policy preflight"
fi
FUGUE_CONTROL_PLANE_BACKUP_DRAIN_REQUIRED=auto
control_plane_backup_drain_required || fail "API changes must require control-plane backup drain in auto mode"

FUGUE_RELEASE_CHANGED_FILES=$'scripts/upgrade_fugue_control_plane.sh\nscripts/test_release_domain_safety.sh\n.github/workflows/deploy-control-plane.yml'
if control_plane_backup_drain_required; then
  fail "deploy tooling changes must not require control-plane backup drain in auto mode"
fi
FUGUE_RELEASE_CHANGED_FILES=$'.github/workflows/release-public-data-plane.yml'
public_data_plane_changed || fail "public data-plane release workflow must be owned by the public data-plane domain"
public_workflow_subsystems="$(release_safety_changed_file_subsystems)"
[[ "${public_workflow_subsystems}" == *"deploy_script"* && "${public_workflow_subsystems}" == *"edge_worker"* && "${public_workflow_subsystems}" == *"dns_server"* ]] ||
  fail "public data-plane release workflow must select deploy, edge, and DNS safety domains, got ${public_workflow_subsystems}"
[[ -z "$(release_safety_unknown_high_risk_files)" ]] ||
  fail "public data-plane release workflow must not be classified as unknown high risk"
public_workflow_gates="$(release_safety_required_gates)"
[[ "${public_workflow_gates}" == *"inactive_worker_smoke"* && "${public_workflow_gates}" == *"authoritative_dns"* && "${public_workflow_gates}" == *"rollback_path_smoke"* ]] ||
  fail "public data-plane release workflow must require edge, DNS, and rollback gates, got ${public_workflow_gates}"
assert_eq "$(release_safety_watch_window_seconds)" "180" "public data-plane release workflow watch window"
if control_plane_backup_drain_required; then
  fail "public data-plane release workflow changes must not require control-plane backup drain"
fi
FUGUE_CONTROL_PLANE_BACKUP_DRAIN_REQUIRED=true
control_plane_backup_drain_required || fail "explicit backup drain required=true must force drain"
FUGUE_CONTROL_PLANE_BACKUP_DRAIN_REQUIRED=false
if control_plane_backup_drain_required; then
  fail "explicit backup drain required=false must skip drain"
fi
FUGUE_CONTROL_PLANE_BACKUP_DRAIN_REQUIRED=auto

FUGUE_RELEASE_CHANGED_FILES=$'internal/edge/service.go'
public_data_plane_changed || fail "edge code changes must mark public data-plane changed"
public_data_plane_worker_image_changed || fail "edge code changes must mark worker image changed"

FUGUE_RELEASE_CHANGED_FILES=$'internal/weightedselector/weighted_selector.go'
public_data_plane_changed || fail "weighted selector changes must mark public data-plane changed"
public_data_plane_worker_image_changed || fail "weighted selector changes must mark worker image changed"
public_data_plane_dns_image_changed || fail "weighted selector changes must mark DNS image changed"
if public_data_plane_front_image_changed; then
  fail "weighted selector changes must not mark front image changed"
fi
weighted_selector_subsystems="$(release_safety_changed_file_subsystems)"
[[ "${weighted_selector_subsystems}" == *"edge_worker"* && "${weighted_selector_subsystems}" == *"dns_server"* ]] ||
  fail "weighted selector must be owned by edge worker and DNS server, got ${weighted_selector_subsystems}"
[[ -z "$(release_safety_unknown_high_risk_files)" ]] ||
  fail "weighted selector must not be classified as unknown high risk"

FUGUE_RELEASE_CHANGED_FILES=$'internal/api/node_updater.go'
node_updater_subsystems="$(release_safety_changed_file_subsystems)"
[[ "${node_updater_subsystems}" == *"node_updater"* && "${node_updater_subsystems}" == *"control_plane_api"* ]] ||
  fail "node-updater changed files must select node-updater and control-plane API subsystems, got ${node_updater_subsystems}"
assert_eq "$(FUGUE_NODE_UPDATER_TIMER_CYCLE_SECONDS=123 release_safety_watch_window_seconds)" "123" "node-updater changed files must require a full timer-cycle watch window"
node_updater_gates="$(release_safety_required_gates)"
[[ "${node_updater_gates}" == *"node_deep_health"* && "${node_updater_gates}" == *"public_synthetic"* ]] ||
  fail "node-updater changed files must require node deep health and public synthetic gates, got ${node_updater_gates}"

FUGUE_RELEASE_CHANGED_FILES=$'internal/api/edge_routes.go'
edge_route_subsystems="$(release_safety_changed_file_subsystems)"
[[ "${edge_route_subsystems}" == *"edge_route"* && "${edge_route_subsystems}" == *"control_plane_api"* ]] ||
  fail "edge route changed files must select edge-route and control-plane API subsystems, got ${edge_route_subsystems}"
edge_route_gates="$(release_safety_required_gates)"
[[ "${edge_route_gates}" == *"route_check"* && "${edge_route_gates}" == *"dns_answer_audit"* ]] ||
  fail "edge route changed files must require route-check and DNS answer audit gates, got ${edge_route_gates}"

FUGUE_RELEASE_CHANGED_FILES=$'internal/model/model.go'
assert_eq "$(release_safety_changed_file_subsystems)" "shared_control_plane" "shared model changes must propagate shared control-plane risk"
shared_gates="$(release_safety_required_gates)"
[[ "${shared_gates}" == *"platform_autonomy"* && "${shared_gates}" == *"rollback_path_smoke"* ]] ||
  fail "shared control-plane changes must require autonomy and rollback gates, got ${shared_gates}"

FUGUE_RELEASE_CHANGED_FILES=$'internal/platformsafety/kernel.go'
assert_eq "$(release_safety_changed_file_subsystems)" "shared_control_plane" "platform safety kernel changes must propagate shared control-plane risk"
[[ -z "$(release_safety_unknown_high_risk_files)" ]] ||
  fail "platform safety kernel files must have explicit release-risk ownership"

FUGUE_RELEASE_CHANGED_FILES=$'internal/platformcontrol/action_safety.go\ninternal/platformcontrol/registry.go'
assert_eq "$(release_safety_changed_file_subsystems)" "shared_control_plane" "platform control policy changes must propagate shared control-plane risk"
[[ -z "$(release_safety_unknown_high_risk_files)" ]] ||
  fail "platform control policy files must have explicit release-risk ownership"

FUGUE_RELEASE_CHANGED_FILES=$'cmd/fugue-openapi-gen/main.go\ninternal/apispec/spec.go\nopenapi/openapi.yaml'
assert_eq "$(release_safety_changed_file_subsystems)" "shared_control_plane" "OpenAPI generator and contract changes must propagate shared control-plane risk"
[[ -z "$(release_safety_unknown_high_risk_files)" ]] ||
  fail "OpenAPI generator and contract files must have explicit release-risk ownership"

FUGUE_RELEASE_CHANGED_FILES=$'internal/unattributed/runtime.go'
assert_eq "$(release_safety_changed_file_subsystems)" "unknown_high_risk" "unattributed runtime changes must be classified high risk"
assert_eq "$(release_safety_unknown_high_risk_files)" "internal/unattributed/runtime.go" "unknown high-risk file list"
if require_release_safety_attribution; then
  fail "unknown high-risk runtime changes must hold without explicit approval"
fi
FUGUE_UNKNOWN_RELEASE_RISK_APPROVED=true
require_release_safety_attribution || fail "explicit unknown high-risk approval must release the hold"
unset FUGUE_UNKNOWN_RELEASE_RISK_APPROVED

assert_eq "$(public_synthetic_error_class 503 'no healthy edge groups')" "public_synthetic_503_no_healthy_edge_groups" "synthetic no healthy edge groups class"
public_synthetic_status_is_hard_rollback "$(public_synthetic_error_class 503 'edge group has no healthy non-excluded edge nodes')" ||
  fail "synthetic 503 no healthy non-excluded edge nodes must trigger hard rollback"
public_synthetic_status_is_hard_rollback "$(public_synthetic_error_class 503 'no healthy edge groups')" ||
  fail "synthetic 503 no healthy edge groups must trigger hard rollback"
if public_synthetic_status_is_hard_rollback "$(public_synthetic_error_class 503 'upstream unavailable')"; then
  fail "generic upstream unavailable must not hard rollback without Fugue routing attribution"
fi

FUGUE_RELEASE_CHANGED_FILES=$'internal/bundleauth/bundleauth.go'
public_data_plane_changed || fail "bundle auth changes must mark public data-plane changed"
public_data_plane_worker_image_changed || fail "bundle auth changes must mark worker image changed"
public_data_plane_dns_image_changed || fail "bundle auth changes must mark DNS image changed"
if public_data_plane_front_image_changed; then
  fail "bundle auth changes must not mark front image changed"
fi

FUGUE_RELEASE_CHANGED_FILES=$'internal/model/edge_routes.go'
public_data_plane_changed || fail "edge route model changes must mark public data-plane changed"
public_data_plane_worker_image_changed || fail "edge route model changes must mark worker image changed"
public_data_plane_dns_image_changed || fail "edge route model changes must mark DNS image changed"
if public_data_plane_front_image_changed; then
  fail "edge route model changes must not mark front image changed"
fi

FUGUE_RELEASE_CHANGED_FILES=$'internal/dnsserver/service.go'
public_data_plane_changed || fail "dnsserver code changes must mark public data-plane changed"
public_data_plane_dns_image_changed || fail "dnsserver code changes must mark DNS image changed"
if public_data_plane_worker_image_changed; then
  fail "dnsserver-only changes must not mark worker image changed"
fi
if public_data_plane_front_image_changed; then
  fail "dnsserver-only changes must not mark front image changed"
fi

(
  FUGUE_EDGE_ENABLED=true
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_MODE=auto
  FUGUE_PUBLIC_DATA_PLANE_AUTO_RELEASE_ELIGIBLE=false
  FUGUE_RELEASE_CHANGED_FILES=$'internal/edge/service.go'
  FUGUE_SMOKE_URLS=
  release_called=false
  public_data_plane_worker_image_changed() { return 0; }
  public_data_plane_front_image_changed() { return 1; }
  public_data_plane_dns_image_changed() { return 1; }
  public_data_plane_live_worker_image_changed() { return 1; }
  public_data_plane_live_front_image_changed() { return 1; }
  public_data_plane_live_dns_image_changed() { return 1; }
  public_data_plane_manifest_changed() { return 1; }
  public_data_plane_front_daemonsets_ready() { return 0; }
  bash() { release_called=true; }
  release_public_data_plane_if_needed
  [[ "${release_called}" == "false" ]] || fail "public data-plane auto release must skip when CI marks it ineligible"
  [[ "${PUBLIC_DATA_PLANE_RELEASED}" == "false" ]] || fail "public data-plane released flag must remain false when auto release is ineligible"
)

(
  FUGUE_EDGE_ENABLED=true
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_MODE=auto
  FUGUE_PUBLIC_DATA_PLANE_AUTO_RELEASE_ELIGIBLE=true
  FUGUE_RELEASE_CHANGED_FILES=$'internal/edge/service.go'
  FUGUE_SMOKE_URLS=
  release_called=false
  public_data_plane_worker_image_changed() { return 0; }
  public_data_plane_front_image_changed() { return 1; }
  public_data_plane_dns_image_changed() { return 1; }
  public_data_plane_live_worker_image_changed() { return 1; }
  public_data_plane_live_front_image_changed() { return 1; }
  public_data_plane_live_dns_image_changed() { return 1; }
  public_data_plane_manifest_changed() { return 1; }
  public_data_plane_front_daemonsets_ready() { return 0; }
  bash() { release_called=true; }
  release_public_data_plane_if_needed
  [[ "${release_called}" == "true" ]] || fail "eligible public data-plane worker change must still start auto release"
  [[ "${PUBLIC_DATA_PLANE_RELEASED}" == "true" ]] || fail "public data-plane released flag must be true after eligible auto release"
)

FUGUE_RELEASE_CHANGED_FILES=$'deploy/helm/fugue/templates/dns-daemonset.yaml'
public_data_plane_changed || fail "dns daemonset changes must mark public data-plane changed"

FUGUE_RELEASE_CHANGED_FILES=$'cmd/fugue-image-cache/main.go'
node_local_build_plane_changed || fail "image-cache code changes must mark build-plane changed"
node_local_build_plane_preflight_override_allowed || fail "image-cache fixes must be allowed to bypass registry/node-policy preflight"

for public_command in \
  ./cmd/fugue-edge \
  ./cmd/fugue-edge-front \
  ./cmd/fugue-dns \
  ./cmd/fugue-ssh-front \
  ./cmd/fugue-mesh-agent \
  ./cmd/fugue-mesh-recovery; do
  public_dependencies="$(go list -deps "${public_command}")"
  if grep -Fqx 'fugue/internal/store' <<<"${public_dependencies}"; then
    fail "${public_command} must not depend on the control-plane Store"
  fi
  if grep -Fqx 'fugue/internal/releaseflow' <<<"${public_dependencies}"; then
    fail "${public_command} must not depend on the control-plane releaseflow package"
  fi
done

FUGUE_RELEASE_CHANGED_FILES=$'internal/controller/image_replication_controller.go\ninternal/controller/image_replication_controller_test.go\ninternal/store/node_updater.go\ninternal/store/node_updater_pg.go'
node_local_build_plane_preflight_override_allowed || fail "image replication node-task cleanup fixes must be allowed to bypass existing node-policy preflight"

FUGUE_RELEASE_CHANGED_FILES=$'internal/api/server.go'
if strict_drain_agent_image_changed; then
  fail "generic control-plane changes must not mark strict drain-agent image changed"
fi

FUGUE_RELEASE_CHANGED_FILES=$'cmd/fugue-drain-agent/main.go'
strict_drain_agent_image_changed || fail "drain-agent code changes must mark strict drain-agent image changed"

FUGUE_RELEASE_CHANGED_FILES=$'Dockerfile.drain-agent'
strict_drain_agent_image_changed || fail "drain-agent Dockerfile changes must mark strict drain-agent image changed"

(
  FUGUE_RELEASE_CHANGED_FILES=$'internal/api/server.go'
  FUGUE_STRICT_DRAIN_MODE=connection-aware
  FUGUE_RELEASE_FULLNAME=fugue
  FUGUE_CONTROLLER_DEPLOYMENT_NAME=fugue-controller
  FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY=ghcr.io/acme/fugue-drain-agent
  FUGUE_DRAIN_AGENT_IMAGE_TAG=target
  FUGUE_DRAIN_AGENT_IMAGE_DIGEST=""
  FUGUE_DRAIN_AGENT_IMAGE_PULL_POLICY=Always
  deployment_exists() { [[ "$1" == "fugue-controller" ]]; }
  live_deployment_container_env_value() {
    case "$3" in
      FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY) printf '%s' "ghcr.io/acme/live-drain-agent" ;;
      FUGUE_DRAIN_AGENT_IMAGE_TAG) printf '%s' "live-sha" ;;
      FUGUE_DRAIN_AGENT_IMAGE_DIGEST) printf '%s' "" ;;
      FUGUE_DRAIN_AGENT_IMAGE_PULL_POLICY) printf '%s' "IfNotPresent" ;;
    esac
  }
  preserve_strict_drain_agent_image_from_live
  assert_eq "${FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY}" "ghcr.io/acme/live-drain-agent" "strict drain-agent repository preserves live image"
  assert_eq "${FUGUE_DRAIN_AGENT_IMAGE_TAG}" "live-sha" "strict drain-agent tag preserves live image"
  assert_eq "${FUGUE_DRAIN_AGENT_IMAGE_PULL_POLICY}" "IfNotPresent" "strict drain-agent pull policy preserves live image"
  assert_eq "${STRICT_DRAIN_AGENT_IMAGE_PRESERVED}" "true" "strict drain-agent preserve flag"
)

(
  FUGUE_RELEASE_CHANGED_FILES=$'cmd/fugue-drain-agent/main.go'
  FUGUE_STRICT_DRAIN_MODE=connection-aware
  FUGUE_RELEASE_FULLNAME=fugue
  FUGUE_CONTROLLER_DEPLOYMENT_NAME=fugue-controller
  FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY=ghcr.io/acme/fugue-drain-agent
  FUGUE_DRAIN_AGENT_IMAGE_TAG=target
  deployment_exists() { return 0; }
  live_deployment_container_env_value() { printf '%s' "unexpected"; }
  preserve_strict_drain_agent_image_from_live
  assert_eq "${FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY}" "ghcr.io/acme/fugue-drain-agent" "drain-agent source change must not preserve live repository"
  assert_eq "${FUGUE_DRAIN_AGENT_IMAGE_TAG}" "target" "drain-agent source change must not preserve live tag"
  assert_eq "${STRICT_DRAIN_AGENT_IMAGE_PRESERVED}" "false" "strict drain-agent source changes are not preserved"
)

FUGUE_RELEASE_CHANGED_FILES=$'cmd/fugue-image-cache/main.go\ninternal/controller/deploy_image_guard.go\ndeploy/helm/fugue/templates/controller-deployment.yaml'
node_local_build_plane_preflight_override_allowed || fail "builder registry routing fixes must be allowed to bypass registry/node-policy preflight"

FUGUE_RELEASE_CHANGED_FILES=$'internal/api/join_cluster.go\ninternal/api/node_updater.go\ninternal/api/node_updater_test.go'
node_local_build_plane_preflight_override_allowed || fail "node updater registry mirror reload fixes must be allowed to bypass registry/node-policy preflight"

FUGUE_RELEASE_CHANGED_FILES=$'.github/workflows/deploy-control-plane.yml\ncmd/fugue-image-cache/main.go\ndocs/image-cache-localpv-storage-recovery-plan.md\ninternal/api/image_cache_localpv_admin.go\ninternal/api/routes_gen.go\ninternal/apispec/spec_gen.go\ninternal/cli/admin_image_cache.go\ninternal/config/config.go\ninternal/controller/image_cache_orphan_cleanup.go\ninternal/controller/metrics.go\ninternal/model/model.go\ninternal/store/image_cache_localpv.go\ninternal/store/postgres.go\nopenapi/openapi.yaml\nscripts/prepare_fugue_lvm_localpv_node.sh\nscripts/upgrade_fugue_control_plane.sh'
node_local_build_plane_preflight_override_allowed || fail "image-cache LocalPV maintenance fixes must bypass existing node-policy preflight"

FUGUE_RELEASE_CHANGED_FILES=$'internal/api/cluster_node_policy.go\ninternal/api/cluster_node_policy_seed_test.go\ninternal/api/cluster_node_policy_status.go\ninternal/api/cluster_node_views.go\ninternal/api/cluster_node_views_test.go\ninternal/api/join_cluster.go\ninternal/api/runtime_pool.go\ninternal/api/server_test.go\ninternal/store/machines.go\ninternal/store/store_test.go'
node_local_build_plane_preflight_override_allowed || fail "node-policy join fixes must be allowed to bypass existing node-policy preflight"

FUGUE_RELEASE_CHANGED_FILES=$'internal/api/cluster_nodes.go\ninternal/api/cluster_node_policy_seed_test.go'
node_local_build_plane_preflight_override_allowed || fail "cluster node policy drift fixes must be allowed to bypass existing node-policy preflight"

FUGUE_RELEASE_CHANGED_FILES=$'internal/api/import_github_compose_test.go\ninternal/api/import_github_topology.go\ninternal/api/import_network_mode.go'
node_local_build_plane_preflight_override_allowed || fail "compose import process fixes must be allowed to bypass registry/node-policy preflight"

FUGUE_RELEASE_CHANGED_FILES=$'internal/api/app_deploy_test.go\ninternal/api/server.go'
node_local_build_plane_preflight_override_allowed || fail "deploy baseline recovery fixes must be allowed to bypass registry/node-policy preflight"

FUGUE_RELEASE_CHANGED_FILES=$'internal/controller/managed_app_reconciler.go\ninternal/controller/managed_app_reconciler_test.go\ninternal/controller/managed_app_rollout_test.go'
node_local_build_plane_preflight_override_allowed || fail "managed app rollout recovery fixes must be allowed to bypass registry/node-policy preflight"

FUGUE_RELEASE_CHANGED_FILES=$'internal/api/app_deploy_test.go\ninternal/api/server.go\nscripts/upgrade_fugue_control_plane.sh\nscripts/test_release_domain_safety.sh'
node_local_build_plane_preflight_override_allowed || fail "deploy baseline recovery release preflight fix must be allowed to publish itself"

FUGUE_RELEASE_CHANGED_FILES=$'scripts/upgrade_fugue_control_plane.sh\nscripts/test_release_domain_safety.sh'
node_local_build_plane_preflight_override_allowed || fail "release preflight fix must be allowed to publish itself while registry/node-policy preflight is degraded"

FUGUE_RELEASE_CHANGED_FILES=$'cmd/fugue-image-cache/main.go\ninternal/api/server.go'
if node_local_build_plane_preflight_override_allowed; then
  fail "mixed unrelated API changes must not bypass registry/node-policy preflight"
fi

ORIGINAL_REPO_ROOT="${REPO_ROOT}"
TMP_REPO_ROOT="$(mktemp -d)"
trap 'rm -rf "${TMP_REPO_ROOT}"' EXIT
ORIGINAL_BEFORE_SHA_SET="${BEFORE_SHA+x}"
ORIGINAL_BEFORE_SHA="${BEFORE_SHA:-}"
ORIGINAL_AFTER_SHA_SET="${AFTER_SHA+x}"
ORIGINAL_AFTER_SHA="${AFTER_SHA:-}"
ORIGINAL_FUGUE_HELM_CHART_PATH_SET="${FUGUE_HELM_CHART_PATH+x}"
ORIGINAL_FUGUE_HELM_CHART_PATH="${FUGUE_HELM_CHART_PATH:-}"

restore_temp_release_env() {
  if [[ -n "${ORIGINAL_BEFORE_SHA_SET}" ]]; then
    BEFORE_SHA="${ORIGINAL_BEFORE_SHA}"
  else
    unset BEFORE_SHA
  fi
  if [[ -n "${ORIGINAL_AFTER_SHA_SET}" ]]; then
    AFTER_SHA="${ORIGINAL_AFTER_SHA}"
  else
    unset AFTER_SHA
  fi
  if [[ -n "${ORIGINAL_FUGUE_HELM_CHART_PATH_SET}" ]]; then
    FUGUE_HELM_CHART_PATH="${ORIGINAL_FUGUE_HELM_CHART_PATH}"
  else
    unset FUGUE_HELM_CHART_PATH
  fi
}

git -C "${TMP_REPO_ROOT}" init -q
git -C "${TMP_REPO_ROOT}" config user.email test@example.com
git -C "${TMP_REPO_ROOT}" config user.name "Fugue Test"
mkdir -p "${TMP_REPO_ROOT}/cmd/fugue-image-cache" "${TMP_REPO_ROOT}/deploy/helm/fugue" "${TMP_REPO_ROOT}/scripts"
printf 'module example.com/fugue-test\n' >"${TMP_REPO_ROOT}/go.mod"
: >"${TMP_REPO_ROOT}/go.sum"
printf 'FROM scratch\n' >"${TMP_REPO_ROOT}/Dockerfile.image-cache"
printf 'package main\nfunc main() {}\n' >"${TMP_REPO_ROOT}/cmd/fugue-image-cache/main.go"
cat >"${TMP_REPO_ROOT}/deploy/helm/fugue/values.yaml" <<'YAML'
imageCache:
  enabled: true
  resources:
    requests:
      memory: 64Mi
    limits:
      memory: 512Mi

imageStore:
  imageCacheInventory:
    enabled: true
    interval: 30m
    ttl: 2h
  orphanPrune:
    mode: delete
    gracePeriod: 24h
    maxTargetsPerNode: 50
    maxDeleteBytesPerNode: "104857600"
    minReplicaCount: 1

registry:
  persistence:
    mode: hostPath
    hostPath: /var/lib/fugue/registry
    size: 50Gi
  unsafeHostPath:
    enabled: true
    reason: test fixture

registryGC:
  resources:
    requests:
      memory: 128Mi
YAML
git -C "${TMP_REPO_ROOT}" add .
git -C "${TMP_REPO_ROOT}" commit -q -m base
BASE_REF="$(git -C "${TMP_REPO_ROOT}" rev-parse HEAD)"
printf '# script-only\n' >"${TMP_REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh"
git -C "${TMP_REPO_ROOT}" add .
git -C "${TMP_REPO_ROOT}" commit -q -m scripts
SCRIPT_REF="$(git -C "${TMP_REPO_ROOT}" rev-parse HEAD)"
printf 'package main\nfunc main() { println("fixed") }\n' >"${TMP_REPO_ROOT}/cmd/fugue-image-cache/main.go"
git -C "${TMP_REPO_ROOT}" add .
git -C "${TMP_REPO_ROOT}" commit -q -m image-cache
IMAGE_CACHE_REF="$(git -C "${TMP_REPO_ROOT}" rev-parse HEAD)"
live_diff="$(
  FUGUE_RELEASE_REPO_ROOT="${TMP_REPO_ROOT}" \
  FUGUE_RELEASE_TARGET_REF="${IMAGE_CACHE_REF}" \
  FUGUE_RELEASE_BASE_REFS="${BASE_REF}
${SCRIPT_REF}" \
    "${REPO_ROOT}/scripts/compute_release_changed_files_from_live.sh"
)"
[[ "${live_diff}" == *"cmd/fugue-image-cache/main.go"* && "${live_diff}" == *"scripts/upgrade_fugue_control_plane.sh"* ]] ||
  fail "live-to-target release diff must retain changes skipped by an intervening failed deploy, got ${live_diff}"
if FUGUE_RELEASE_REPO_ROOT="${TMP_REPO_ROOT}" \
  FUGUE_RELEASE_TARGET_REF="${IMAGE_CACHE_REF}" \
  FUGUE_RELEASE_BASE_REFS="missing-live-image-tag" \
  "${REPO_ROOT}/scripts/compute_release_changed_files_from_live.sh" >/dev/null 2>&1; then
  fail "unresolvable live image refs must fail closed"
fi
if FUGUE_RELEASE_REPO_ROOT="${TMP_REPO_ROOT}" \
  FUGUE_RELEASE_TARGET_REF="${IMAGE_CACHE_REF}" \
  FUGUE_RELEASE_BASE_REFS="" \
  FUGUE_RELEASE_REQUIRE_BASELINE=true \
  "${REPO_ROOT}/scripts/compute_release_changed_files_from_live.sh" >/dev/null 2>&1; then
  fail "required live release baseline must fail closed when no core image ref is available"
fi
(
  REPO_ROOT="${TMP_REPO_ROOT}"
  FUGUE_API_DEPLOYMENT_NAME=fugue-api
  FUGUE_LEGACY_API_DEPLOYMENT_NAME=fugue
  FUGUE_API_IMAGE_TAG="${IMAGE_CACHE_REF}"
  FUGUE_RELEASE_CHANGED_FILES=$'deploy/helm/fugue/templates/image-cache-daemonset.yaml\ncmd/fugue-image-cache/main.go'
  RELEASE_CHANGED_FILES_EFFECTIVE=""
  deployment_exists() {
    [[ "$1" == "fugue-api" ]]
  }
  live_deployment_container_image() {
    printf 'ghcr.io/acme/fugue-api:%s' "${SCRIPT_REF}"
  }
  refresh_release_changed_files_from_live_api
  assert_eq "$(release_changed_files)" "cmd/fugue-image-cache/main.go" "release changed files rebase uses live API tag"
  if node_local_build_plane_manifest_changed; then
    fail "rebased live diff must not treat reverted image-cache templates as manifest changes"
  fi
)
python3 - "${TMP_REPO_ROOT}/deploy/helm/fugue/values.yaml" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
text = text.replace("      memory: 64Mi\n", "      memory: 128Mi\n", 1)
text = text.replace("      memory: 512Mi\n", "      memory: 2Gi\n", 1)
path.write_text(text)
PY
git -C "${TMP_REPO_ROOT}" add .
git -C "${TMP_REPO_ROOT}" commit -q -m image-cache-resources
IMAGE_CACHE_RESOURCES_REF="$(git -C "${TMP_REPO_ROOT}" rev-parse HEAD)"
python3 - "${TMP_REPO_ROOT}/deploy/helm/fugue/values.yaml" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
text = text.replace("    interval: 30m\n", "    interval: 15m\n", 1)
text = text.replace("    maxTargetsPerNode: 50\n", "    maxTargetsPerNode: 25\n", 1)
path.write_text(text)
PY
git -C "${TMP_REPO_ROOT}" add .
git -C "${TMP_REPO_ROOT}" commit -q -m image-store-maintenance
IMAGE_STORE_MAINTENANCE_REF="$(git -C "${TMP_REPO_ROOT}" rev-parse HEAD)"
python3 - "${TMP_REPO_ROOT}/deploy/helm/fugue/values.yaml" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
text = text.replace(
    "registryGC:\n  resources:\n    requests:\n      memory: 128Mi\n",
    "registryGC:\n  resources:\n    requests:\n      memory: 256Mi\n",
    1,
)
path.write_text(text)
PY
git -C "${TMP_REPO_ROOT}" add .
git -C "${TMP_REPO_ROOT}" commit -q -m registry-gc-resources
REGISTRY_GC_RESOURCES_REF="$(git -C "${TMP_REPO_ROOT}" rev-parse HEAD)"
python3 - "${TMP_REPO_ROOT}/deploy/helm/fugue/values.yaml" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
text = text.replace("    mode: hostPath\n", "    mode: pvc\n", 1)
text = text.replace("    size: 50Gi\n", "    size: 200Gi\n", 1)
path.write_text(text)
PY
git -C "${TMP_REPO_ROOT}" add .
git -C "${TMP_REPO_ROOT}" commit -q -m registry-persistence
REGISTRY_PERSISTENCE_REF="$(git -C "${TMP_REPO_ROOT}" rev-parse HEAD)"
REPO_ROOT="${TMP_REPO_ROOT}"
if image_cache_source_changed_between_refs "${BASE_REF}" "${SCRIPT_REF}"; then
  fail "script-only changes must not roll image-cache"
fi
image_cache_source_changed_between_refs "${SCRIPT_REF}" "${IMAGE_CACHE_REF}" || fail "image-cache source changes must allow image rollout"

mkdir -p "${TMP_REPO_ROOT}/cmd/fugue-edge" "${TMP_REPO_ROOT}/internal/edge"
printf 'FROM scratch\n' >"${TMP_REPO_ROOT}/Dockerfile.edge"
printf 'package main\nfunc main() {}\n' >"${TMP_REPO_ROOT}/cmd/fugue-edge/main.go"
printf 'package edge\n' >"${TMP_REPO_ROOT}/internal/edge/service.go"
git -C "${TMP_REPO_ROOT}" add .
git -C "${TMP_REPO_ROOT}" commit -q -m edge-base
EDGE_BASE_REF="$(git -C "${TMP_REPO_ROOT}" rev-parse HEAD)"
printf 'func weightedReleaseSelector() {}\n' >>"${TMP_REPO_ROOT}/internal/edge/service.go"
git -C "${TMP_REPO_ROOT}" add .
git -C "${TMP_REPO_ROOT}" commit -q -m edge-worker-change
EDGE_WORKER_REF="$(git -C "${TMP_REPO_ROOT}" rev-parse HEAD)"
public_data_plane_worker_source_changed_between_refs "${EDGE_BASE_REF}" "${EDGE_WORKER_REF}" || fail "edge worker source changes must be detected between live and target tags"
if public_data_plane_front_source_changed_between_refs "${EDGE_BASE_REF}" "${EDGE_WORKER_REF}"; then
  fail "edge worker-only source changes must not mark front image changed between live and target tags"
fi
mkdir -p "${TMP_REPO_ROOT}/internal/bundleauth" "${TMP_REPO_ROOT}/internal/model"
printf 'package bundleauth\n' >"${TMP_REPO_ROOT}/internal/bundleauth/bundleauth.go"
printf 'package model\n' >"${TMP_REPO_ROOT}/internal/model/edge_routes.go"
git -C "${TMP_REPO_ROOT}" add .
git -C "${TMP_REPO_ROOT}" commit -q -m edge-shared-base
EDGE_SHARED_BASE_REF="$(git -C "${TMP_REPO_ROOT}" rev-parse HEAD)"
printf 'func verifyRouteBundle() {}\n' >>"${TMP_REPO_ROOT}/internal/bundleauth/bundleauth.go"
git -C "${TMP_REPO_ROOT}" add .
git -C "${TMP_REPO_ROOT}" commit -q -m edge-shared-bundleauth-change
EDGE_SHARED_BUNDLEAUTH_REF="$(git -C "${TMP_REPO_ROOT}" rev-parse HEAD)"
public_data_plane_worker_source_changed_between_refs "${EDGE_SHARED_BASE_REF}" "${EDGE_SHARED_BUNDLEAUTH_REF}" || fail "bundle auth source changes must be detected for edge worker image rollout"
public_data_plane_dns_source_changed_between_refs "${EDGE_SHARED_BASE_REF}" "${EDGE_SHARED_BUNDLEAUTH_REF}" || fail "bundle auth source changes must be detected for DNS image rollout"
if public_data_plane_front_source_changed_between_refs "${EDGE_SHARED_BASE_REF}" "${EDGE_SHARED_BUNDLEAUTH_REF}"; then
  fail "bundle auth source changes must not mark front image changed between live and target tags"
fi
printf 'type EdgeRouteBundle struct{}\n' >>"${TMP_REPO_ROOT}/internal/model/edge_routes.go"
git -C "${TMP_REPO_ROOT}" add .
git -C "${TMP_REPO_ROOT}" commit -q -m edge-route-model-change
EDGE_ROUTE_MODEL_REF="$(git -C "${TMP_REPO_ROOT}" rev-parse HEAD)"
public_data_plane_worker_source_changed_between_refs "${EDGE_SHARED_BUNDLEAUTH_REF}" "${EDGE_ROUTE_MODEL_REF}" || fail "edge route model source changes must be detected for edge worker image rollout"
public_data_plane_dns_source_changed_between_refs "${EDGE_SHARED_BUNDLEAUTH_REF}" "${EDGE_ROUTE_MODEL_REF}" || fail "edge route model source changes must be detected for DNS image rollout"
if public_data_plane_front_source_changed_between_refs "${EDGE_SHARED_BUNDLEAUTH_REF}" "${EDGE_ROUTE_MODEL_REF}"; then
  fail "edge route model source changes must not mark front image changed between live and target tags"
fi
fake_public_kubectl() {
  if [[ "${1:-}" == "-n" ]]; then
    shift 2
  fi
  if [[ "${1:-}" == "get" && "${2:-}" == "ds" ]]; then
    printf '%s\n' "fugue-fugue-edge-worker-a"
    return 0
  fi
  if [[ "${1:-}" == "get" && "${2:-}" == "ds/fugue-fugue-edge-worker-a" ]]; then
    printf 'ghcr.io/acme/fugue-edge:%s' "${EDGE_BASE_REF}"
    return 0
  fi
  return 1
}
KUBECTL=fake_public_kubectl
FUGUE_NAMESPACE=fugue-system
FUGUE_RELEASE_FULLNAME=fugue-fugue
FUGUE_EDGE_IMAGE_TAG="${EDGE_WORKER_REF}"
public_data_plane_live_worker_image_changed || fail "live edge worker tag drift must trigger public data-plane worker release"

(
  FUGUE_RELEASE_NAME=fugue
  FUGUE_NAMESPACE=fugue-system
  helm() {
    printf '{"edge":{"image":{"repository":"ghcr.io/acme/fugue-edge","tag":"base-stable"}}}'
  }
  assert_eq "$(live_helm_release_value "edge.image.repository")" "ghcr.io/acme/fugue-edge" "live Helm nested repository value"
  assert_eq "$(live_helm_release_value "edge.image.tag")" "base-stable" "live Helm nested tag value"
  if live_helm_release_value "edge.image.missing" >/dev/null; then
    fail "missing live Helm value must fail closed"
  fi
)

(
  FUGUE_NAMESPACE=fugue-system
  FUGUE_RELEASE_FULLNAME=fugue-fugue
  KUBECTL=true
  daemonset_exists() {
    case "$1" in
      fugue-fugue-edge-front|fugue-fugue-edge-worker-a|fugue-fugue-edge-worker-b)
        return 0
        ;;
    esac
    return 1
  }
  live_bluegreen_front_pod_image() {
    printf 'ghcr.io/acme/fugue-edge:front-stable'
  }
  live_daemonset_container_image() {
    case "$1/$2" in
      fugue-fugue-edge-ssh-front/ssh-front)
        printf 'ghcr.io/acme/fugue-edge:ssh-stable'
        ;;
      fugue-fugue-edge-worker-a/edge)
        printf 'ghcr.io/acme/fugue-edge:worker-a-stable'
        ;;
      fugue-fugue-edge-worker-b/edge)
        printf 'ghcr.io/acme/fugue-edge:worker-b-stable'
        ;;
    esac
  }
  live_daemonset_container_resources_json() { :; }
  live_daemonset_container_env_value() { :; }
  live_helm_release_value() {
    case "$1" in
      edge.image.repository)
        printf 'ghcr.io/acme/fugue-edge'
        ;;
      edge.image.tag)
        printf 'base-stable'
        ;;
    esac
  }
  append_dns_group_image_args_from_live() { :; }
  FUGUE_EDGE_IMAGE_REPOSITORY=ghcr.io/acme/fugue-edge
  FUGUE_EDGE_IMAGE_TAG=unreleased-target
  preserve_public_data_plane_from_live
  joined_args="$(printf '%s\n' "${PUBLIC_DATA_PLANE_HELM_SET_ARGS[@]}")"
  [[ "${joined_args}" == *"edge.sshFront.image.repository=ghcr.io/acme/fugue-edge"* ]] ||
    fail "control-plane preserve mode must retain the live SSH front image repository"
  [[ "${joined_args}" == *"edge.sshFront.image.tag=ssh-stable"* ]] ||
    fail "control-plane preserve mode must retain the live SSH front image tag"
  [[ "${FUGUE_EDGE_HELM_IMAGE_REPOSITORY}" == "ghcr.io/acme/fugue-edge" ]] ||
    fail "control-plane preserve mode must retain the live Helm base image repository"
  [[ "${FUGUE_EDGE_HELM_IMAGE_TAG}" == "base-stable" ]] ||
    fail "control-plane preserve mode must retain the live Helm base image tag"
  [[ "${FUGUE_EDGE_IMAGE_TAG}" == "unreleased-target" ]] ||
    fail "control-plane preserve mode must not overwrite the edge release target"
)

grep -Fq -- '--set-string edge.image.repository="${FUGUE_EDGE_HELM_IMAGE_REPOSITORY:-${FUGUE_EDGE_IMAGE_REPOSITORY}}"' "${ORIGINAL_REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh" ||
  fail "Helm upgrade must use the preserved edge base image repository"
grep -Fq -- '--set-string edge.image.tag="${FUGUE_EDGE_HELM_IMAGE_TAG:-${FUGUE_EDGE_IMAGE_TAG}}"' "${ORIGINAL_REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh" ||
  fail "Helm upgrade must use the preserved edge base image tag"

BEFORE_SHA="${IMAGE_CACHE_REF}"
AFTER_SHA="${IMAGE_CACHE_RESOURCES_REF}"
FUGUE_RELEASE_CHANGED_FILES=$'deploy/helm/fugue/values.yaml'
node_local_build_plane_resource_values_changed || fail "image-cache resource values must be recognized"
node_local_build_plane_preflight_override_allowed || fail "image-cache resource values must bypass registry/node-policy preflight"
BEFORE_SHA="${IMAGE_CACHE_RESOURCES_REF}"
AFTER_SHA="${IMAGE_STORE_MAINTENANCE_REF}"
FUGUE_RELEASE_CHANGED_FILES=$'deploy/helm/fugue/values.yaml'
if node_local_build_plane_resource_values_changed; then
  fail "image-store maintenance values must not be treated as image-cache resource changes"
fi
node_local_build_plane_preflight_override_allowed || fail "image-store maintenance values must bypass registry/node-policy preflight"
BEFORE_SHA="${IMAGE_STORE_MAINTENANCE_REF}"
AFTER_SHA="${REGISTRY_GC_RESOURCES_REF}"
if node_local_build_plane_resource_values_changed; then
  fail "registryGC values must not be recognized as image-cache resource values"
fi
if node_local_build_plane_preflight_override_allowed; then
  fail "registryGC values must not bypass registry/node-policy preflight"
fi
BEFORE_SHA="${REGISTRY_GC_RESOURCES_REF}"
AFTER_SHA="${REGISTRY_PERSISTENCE_REF}"
FUGUE_RELEASE_CHANGED_FILES=$'deploy/helm/fugue/values.yaml'
stateful_dependency_changed || fail "registry persistence values must be treated as stateful dependency changes"
if node_local_build_plane_preflight_override_allowed; then
  fail "registry persistence values must not bypass registry/node-policy preflight"
fi
FUGUE_HELM_CHART_PATH="${TMP_REPO_ROOT}/deploy/helm/fugue"
IMAGE_CACHE_DESIRED_RESOURCES="$(chart_image_cache_resources_json)"
assert_eq "${IMAGE_CACHE_DESIRED_RESOURCES}" '{"limits":{"memory":"2Gi"},"requests":{"memory":"128Mi"}}' "image-cache desired resources parse from chart values"
FUGUE_RELEASE_FULLNAME=fugue-fugue
TEST_LIVE_IMAGE_CACHE_RESOURCES_JSON='{"limits":{"memory":"512Mi"},"requests":{"memory":"64Mi"}}'
live_daemonset_container_resources_json() {
  printf '%s' "${TEST_LIVE_IMAGE_CACHE_RESOURCES_JSON}"
}
image_cache_resource_values_drifted || fail "live image-cache resources below desired values must be treated as drift"
TEST_LIVE_IMAGE_CACHE_RESOURCES_JSON="${IMAGE_CACHE_DESIRED_RESOURCES}"
if image_cache_resource_values_drifted; then
  fail "matching image-cache resources must not be treated as drift"
fi
restore_temp_release_env
FUGUE_RELEASE_FULLNAME=fugue-fugue
FUGUE_IMAGE_CACHE_IMAGE_TAG="${IMAGE_CACHE_REF}"
live_daemonset_container_image() {
  printf 'ghcr.io/acme/fugue-image-cache:%s' "${SCRIPT_REF}"
}
node_local_build_plane_image_rollout_allowed || fail "live image-cache tag behind changed target must allow image rollout"
FUGUE_IMAGE_CACHE_IMAGE_TAG="${SCRIPT_REF}"
if node_local_build_plane_image_rollout_allowed; then
  fail "matching live and target image-cache tags must not roll image-cache"
fi
REPO_ROOT="${ORIGINAL_REPO_ROOT}"

FUGUE_REGISTRY_DEPLOYMENT_NAME=fugue-fugue-registry
REGISTRY_UPGRADE_VALUES_FILE="$(mktemp)"
(
  UPGRADE_OVERRIDE_VALUES_FILE="${REGISTRY_UPGRADE_VALUES_FILE}"
  FUGUE_NAMESPACE=fugue-system
  FUGUE_REGISTRY_DEPLOYMENT_NAME=fugue-fugue-registry
  fake_registry_kubectl() {
    printf '/var/lib/fugue/registry'
  }
  KUBECTL=fake_registry_kubectl
  append_registry_upgrade_values
)
grep -q 'mode: hostPath' "${REGISTRY_UPGRADE_VALUES_FILE}" || fail "registry hostPath preservation must set registry.persistence.mode"
grep -q 'hostPath: "/var/lib/fugue/registry"' "${REGISTRY_UPGRADE_VALUES_FILE}" || fail "registry hostPath preservation must keep the live path"
grep -q 'unsafeHostPath:' "${REGISTRY_UPGRADE_VALUES_FILE}" || fail "registry hostPath preservation must enable unsafeHostPath explicitly"
rm -f "${REGISTRY_UPGRADE_VALUES_FILE}"

NODE_LOCAL_BUILD_PLANE_PREFLIGHT_OVERRIDE_USED=true
FUGUE_RELEASE_CHANGED_FILES=$'scripts/upgrade_fugue_control_plane.sh'
skip_singleton_rollout_wait_for_node_local_override fugue-fugue-registry || fail "registry singleton wait must be skipped after accepted node-local build-plane override"
if skip_singleton_rollout_wait_for_node_local_override fugue-fugue-headscale; then
  fail "node-local build-plane override must not skip headscale singleton rollout waits"
fi

FUGUE_RELEASE_CHANGED_FILES=$'.github/workflows/deploy-control-plane.yml\nscripts/build_control_plane_images.sh\nscripts/compute_control_plane_image_build_plan.sh\nscripts/compute_release_changed_files_from_live.sh\nscripts/resolve_control_plane_live_images.sh\nscripts/test_release_domain_safety.sh\nscripts/upgrade_fugue_control_plane.sh'
node_local_build_plane_preflight_override_allowed || fail "deploy tooling changes must allow existing node-local build-plane preflight degradation"

live_deployment_replicas() {
  case "$1" in
    fugue-fugue-registry) printf '0' ;;
    *) printf '1' ;;
  esac
}
prepare_helm_post_renderer
[[ "${PRESERVE_REGISTRY_ZERO_REPLICAS}" == "true" ]] || fail "scaled-down registry must be preserved through a Helm post-renderer"
REGISTRY_RENDERED_MANIFEST="$("${HELM_POST_RENDERER_FILE}" <<'YAML'
apiVersion: v1
kind: Service
metadata:
  name: fugue-fugue-registry
spec:
  type: ClusterIP
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fugue-fugue-registry
spec:
  replicas: 1
  selector: {}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fugue-fugue-api
spec:
  replicas: 2
  selector: {}
YAML
)"
assert_eq "$(grep -c '^  replicas: 0$' <<<"${REGISTRY_RENDERED_MANIFEST}")" "1" "registry post-renderer forces only the registry deployment to zero"
assert_eq "$(grep -c '^  replicas: 1$' <<<"${REGISTRY_RENDERED_MANIFEST}")" "0" "registry post-renderer removes the chart registry replica"
assert_eq "$(grep -c '^  replicas: 2$' <<<"${REGISTRY_RENDERED_MANIFEST}")" "1" "registry post-renderer leaves other deployments alone"
cleanup_upgrade_override_values

FUGUE_RELEASE_CHANGED_FILES=$'deploy/helm/fugue/templates/registry-deployment.yaml'
if skip_singleton_rollout_wait_for_node_local_override fugue-fugue-registry; then
  fail "node-local build-plane override must not skip registry waits when registry manifests changed"
fi
NODE_LOCAL_BUILD_PLANE_PREFLIGHT_OVERRIDE_USED=false

FUGUE_RELEASE_CHANGED_FILES=$'deploy/helm/fugue/templates/registry-deployment.yaml'
stateful_dependency_changed || fail "registry template changes must mark stateful dependency changed"

for maintenance_template in registry-janitor-cronjob.yaml registry-gc-cronjob.yaml registry-gc-lease.yaml; do
  FUGUE_RELEASE_CHANGED_FILES="deploy/helm/fugue/templates/${maintenance_template}"
  if stateful_dependency_changed; then
    fail "${maintenance_template} must be releasable without the stateful dependency override"
  fi
done

DYNAMIC_EDGE_WAITED=""
daemonset_names_by_component_prefix() {
  case "$1" in
    edge-dynamic)
      printf 'fugue-fugue-edge-dynamic-front\n'
      printf 'fugue-fugue-edge-dynamic-worker-a\n'
      ;;
    *)
      return 0
      ;;
  esac
}
rollout_daemonset_status() {
  DYNAMIC_EDGE_WAITED="${DYNAMIC_EDGE_WAITED}${1};"
}
FUGUE_EDGE_DYNAMIC_ENABLED=true
rollout_dynamic_edge_daemonsets_if_present
assert_eq "${DYNAMIC_EDGE_WAITED}" "fugue-fugue-edge-dynamic-front;fugue-fugue-edge-dynamic-worker-a;" "explicit dynamic edge rollout checks must wait for additive dynamic edge daemonsets"
PUBLIC_DATA_PLANE_PRESERVED=true
DYNAMIC_EDGE_WAITED=""
if ! public_data_plane_daemonset_rollout_wait_required; then
  :
else
  fail "preserved public data-plane releases must skip edge and dynamic edge daemonset waits"
fi
assert_eq "${DYNAMIC_EDGE_WAITED}" "" "preserved public data-plane releases must not block on additive dynamic edge daemonsets"
PUBLIC_DATA_PLANE_PRESERVED=false
FUGUE_EDGE_DYNAMIC_ENABLED=false
DYNAMIC_EDGE_WAITED=""
rollout_dynamic_edge_daemonsets_if_present
assert_eq "${DYNAMIC_EDGE_WAITED}" "" "disabled dynamic edge must skip dynamic daemonset waits"

BACKUP_DRAIN_MARKER="$(mktemp)"
fake_backup_kubectl() {
  if [[ "$*" == *"get pods"* ]]; then
    printf 'fugue-fugue-control-plane-postgres-1'
    return 0
  fi
  local sql=""
  sql="$(cat)"
  case "${sql}" in
    *pg_terminate_backend*)
      printf '1'
      printf 'terminated\n' >"${BACKUP_DRAIN_MARKER}"
      ;;
    *"string_agg(pid::text"*)
      printf '12345'
      ;;
    *"to_regclass('public.fugue_backup_runs')"*)
      printf 'true'
      ;;
    *"status = 'succeeded'"*)
      printf 'true'
      ;;
    *"status = 'running'"*)
      printf '1'
      ;;
    *)
      printf ''
      ;;
  esac
}
KUBECTL=fake_backup_kubectl
FUGUE_NAMESPACE=fugue-system
FUGUE_RELEASE_FULLNAME=fugue-fugue
FUGUE_CONTROL_PLANE_POSTGRES_ENABLED=true
FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API=true
FUGUE_CONTROL_PLANE_POSTGRES_NAME=""
FUGUE_CONTROL_PLANE_POSTGRES_DATABASE=fugue
FUGUE_CONTROL_PLANE_BACKUP_DRAIN_MODE=terminate
FUGUE_CONTROL_PLANE_BACKUP_DRAIN_REQUIRED=true
FUGUE_CONTROL_PLANE_BACKUP_DRAIN_WAIT_SECONDS=0
FUGUE_CONTROL_PLANE_BACKUP_DRAIN_POLL_SECONDS=1
FUGUE_CONTROL_PLANE_BACKUP_DRAIN_RECENT_SUCCESS_SECONDS=90000
FUGUE_CONTROL_PLANE_BACKUP_DRAIN_POST_TERMINATE_SLEEP_SECONDS=0
drain_control_plane_backup_before_schema_rollout
grep -q '^terminated$' "${BACKUP_DRAIN_MARKER}" || fail "backup drain must terminate active pg_dump after timeout when a recent successful backup exists"
rm -f "${BACKUP_DRAIN_MARKER}"

printf '[test_release_domain_safety] ok\n'
