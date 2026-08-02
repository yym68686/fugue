#!/usr/bin/env bash

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT
mkdir -p "${TMP}/bin"
cat >"${TMP}/bin/curl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf '%q ' "$@" >>"${MOCK_ARGV}"; printf '\n' >>"${MOCK_ARGV}"
method=GET; output=""; body=""
while (($#)); do
  case "$1" in
    --request) method="$2"; shift 2;;
    --output) output="$2"; shift 2;;
    --data-binary) body="${2#@}"; shift 2;;
    --config|--write-out) shift 2;;
    *) shift;;
  esac
done
phase="$(cat "${MOCK_STATE}")"
if [[ "${method}" == POST ]]; then
  phase="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["to_phase"])' "${body}")"
  printf '%s' "${phase}" >"${MOCK_STATE}"
  printf POST >>"${MOCK_POSTS}"
fi
python3 - "${output}" "${phase}" <<'PY'
import json,os,sys
phase=sys.argv[2]
value={"activation":{"schema":"edge-activation/v1","phase":phase,"route_authority":"legacy","generation":2,"plan_digest":os.environ["FUGUE_EDGE_ACTIVATION_PLAN_DIGEST"],"release_id":os.environ["FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID"],"release_record_uid":os.environ["FUGUE_EDGE_ACTIVATION_RECORD_UID"],"release_record_version":os.environ["FUGUE_EDGE_ACTIVATION_RECORD_VERSION"],"release_record_digest":os.environ["FUGUE_EDGE_ACTIVATION_RECORD_DIGEST"],"created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-01T00:00:01Z"},"instances":[],"active_epochs":[],"legacy_nodes":[],"legacy_groups":[]}
with open(sys.argv[1],"w") as h: json.dump(value,h)
PY
if [[ "${method}" == POST && "${MOCK_MODE:-ok}" == ambiguous ]]; then exit 7; fi
printf 200
MOCK
chmod +x "${TMP}/bin/curl"

export PATH="${TMP}/bin:${PATH}"
export MOCK_ARGV="${TMP}/argv" MOCK_STATE="${TMP}/state" MOCK_POSTS="${TMP}/posts" MOCK_MODE=ok
printf legacy-authoritative >"${MOCK_STATE}"
: >"${MOCK_ARGV}"; : >"${MOCK_POSTS}"
export FUGUE_EDGE_ACTIVATION_ENABLED=true
export FUGUE_EDGE_ACTIVATION_API_URL=https://api.example.test
export FUGUE_EDGE_ACTIVATION_API_KEY=bootstrap_abcdefghijklmnopqrstuvwxyz
export FUGUE_EDGE_ACTIVATION_SIGNER_POD=fugue-api-test
export FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_UID=runner-observed-secret-uid
export FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_VERSION=17
export GITHUB_REPOSITORY=test/repo GITHUB_RUN_ID=1 GITHUB_RUN_ATTEMPT=1 GITHUB_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
export FUGUE_EDGE_ACTIVATION_DIR="${TMP}/evidence"
export FUGUE_NAMESPACE=fugue-system
export FUGUE_EDGE_ACTIVATION_PLAN_DIGEST=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
export FUGUE_EDGE_ACTIVATION_EVIDENCE_DIGEST=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
export FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID=release-test
export FUGUE_EDGE_ACTIVATION_RECORD_UID=record-uid
export FUGUE_EDGE_ACTIVATION_RECORD_VERSION=10
export FUGUE_EDGE_ACTIVATION_RECORD_DIGEST=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
export FUGUE_EDGE_ACTIVATION_LEGACY_DIGEST=sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
# shellcheck source=scripts/lib/edge_activation.sh
source "${ROOT}/scripts/lib/edge_activation.sh"
KUBECTL_TRACE="${TMP}/kubectl-trace"
: >"${KUBECTL_TRACE}"
kubectl_cmd() { printf '%q ' "$@" >>"${KUBECTL_TRACE}"; printf '\n' >>"${KUBECTL_TRACE}"; cat; }
edge_activation_init
if grep -q bootstrap_ "${MOCK_ARGV}"; then
  echo "activation secret leaked through curl argv" >&2; exit 1
fi
edge_activation_advance shadow "" "" ""
[[ "$(cat "${MOCK_POSTS}")" == POST ]]
grep -Eq -- '-n fugue-system exec -i pod/fugue-api-test -c api -- /usr/local/bin/fugue-api sign-edge-activation' "${KUBECTL_TRACE}"

export MOCK_MODE=ambiguous
edge_activation_advance active-epoch-fenced "" "" ""
[[ "$(cat "${MOCK_POSTS}")" == POSTPOST ]]
if grep -q bootstrap_ "${MOCK_ARGV}"; then
  echo "activation secret leaked through curl argv" >&2; exit 1
fi
edge_activation_cleanup
[[ ! -e "${TMP}/evidence/curl.conf" ]]

(
  unset FUGUE_EDGE_ACTIVATION_ENABLED FUGUE_EDGE_ACTIVATION_DIR
  unset FUGUE_EDGE_ACTIVATION_API_URL FUGUE_EDGE_ACTIVATION_API_KEY
  unset FUGUE_EDGE_ACTIVATION_SIGNER_POD FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_UID
  unset FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_VERSION FUGUE_EDGE_ACTIVATION_PLAN_DIGEST
  unset FUGUE_EDGE_ACTIVATION_EVIDENCE_DIGEST FUGUE_EDGE_ACTIVATION_RECORD_UID
  unset FUGUE_EDGE_ACTIVATION_RECORD_VERSION FUGUE_EDGE_ACTIVATION_RECORD_DIGEST
  unset FUGUE_EDGE_ACTIVATION_LEGACY_DIGEST
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${ROOT}/scripts/release_fugue_public_data_plane.sh"
  side_effect_file="${TMP}/default-off-side-effects"
  : >"${side_effect_file}"
  kubectl_cmd() { printf 'kubectl\n' >>"${side_effect_file}"; return 1; }
  edge_activation_get() { printf 'api\n' >>"${side_effect_file}"; return 1; }
  edge_activation_advance() { printf 'advance\n' >>"${side_effect_file}"; return 1; }
  mktemp() { printf 'mktemp\n' >>"${side_effect_file}"; return 1; }
  prepare_edge_activation_candidate_record '{}' '{}'
  [[ ! -s "${side_effect_file}" ]]
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${ROOT}/scripts/release_fugue_public_data_plane.sh"
  export FUGUE_EDGE_ACTIVATION_ENABLED=true
  export FUGUE_RELEASE_FULLNAME=fugue-fugue
  export FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=true
  unset FUGUE_EDGE_ACTIVATION_DIR
  if prepare_edge_activation_candidate_record '{}' '{}'; then
    echo "enabled activation accepted a missing evidence directory" >&2
    exit 1
  fi
  relative_dir="relative-activation-evidence"
  export FUGUE_EDGE_ACTIVATION_DIR="${relative_dir}"
  if prepare_edge_activation_candidate_record '{}' '{}'; then
    echo "enabled activation accepted a relative evidence directory" >&2
    exit 1
  fi
  activation_dir="${TMP}/candidate-evidence"
  mkdir -p "${activation_dir}"
  chmod 0755 "${activation_dir}"
  export FUGUE_EDGE_ACTIVATION_DIR="${activation_dir}"
  if prepare_edge_activation_candidate_record '{}' '{}'; then
    echo "enabled activation accepted a non-private evidence directory" >&2
    exit 1
  fi
  chmod 0700 "${activation_dir}"
  prepare_edge_activation_candidate_record '{}' '{}'
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${ROOT}/scripts/release_fugue_public_data_plane.sh"
  export FUGUE_EDGE_ACTIVATION_ENABLED=false
  export FUGUE_NAMESPACE=fugue-system
  export FUGUE_RELEASE_FULLNAME=fugue-fugue
  export FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID=legacy-fixture
  export FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN=false
  export FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS=
  trace="${TMP}/default-off-bluegreen.trace"
  kube_trace="${TMP}/default-off-bluegreen.kube"
  : >"${trace}"
  : >"${kube_trace}"
  event() { printf '%s\n' "$1" >>"${trace}"; }
  enable_bluegreen_chart_mode() { event enable-chart; }
  bluegreen_worker_bases() { event list-bases; printf 'fugue-fugue-edge\n'; }
  wait_daemonset_ready() { event "wait:$1"; }
  daemonset_desired_count() { event "desired:$1"; printf '1'; }
  current_active_slot() { event "active:$1:$2"; printf 'b'; }
  capture_daemonset_pods() { event "capture:$*"; printf 'stable-pods\n'; }
  patch_inactive_worker() { event "patch:$1"; }
  delete_worker_pods() { event "delete:$1"; }
  worker_https_port() { event "port:$1"; printf '18443'; }
  check_worker_tcp() { event "tcp:$1:$2"; }
  check_worker_https_smoke() { event "https:$1:$2"; }
  write_front_active_slot() { event "front:$1:$2"; }
  check_public_smoke_on_front_nodes() { event "front-smoke:$1"; }
  record_active_slot_json() { event "record:$2:$3"; printf '{"fugue-fugue-edge":"a"}'; }
  run_smoke_urls() { event public-smoke; }
  kubectl_cmd() { printf '%s\n' "$*" >>"${kube_trace}"; return 97; }
  forbidden_activation_helper() { event "forbidden:$1"; return 98; }
  recover_incomplete_edge_activation_before_release() { forbidden_activation_helper recover; }
  prepare_edge_activation_candidate_record() { forbidden_activation_helper prepare; }
  unretire_edge_worker() { forbidden_activation_helper unretire; }
  isolate_inactive_edge_worker() { forbidden_activation_helper isolate; }
  collect_edge_activation_candidate_material() { forbidden_activation_helper collect; }
  edge_activation_api_replica_generation() { forbidden_activation_helper api-generation; }
  edge_activation_advance() { forbidden_activation_helper advance; }
  edge_activation_get() { forbidden_activation_helper get; }
  edge_activation_wait_all_api_ack() { forbidden_activation_helper ack; }
  run_platform_release_evidence() { forbidden_activation_helper platform-evidence; }
  edge_activation_complete_cutover_and_soak() { forbidden_activation_helper soak; }
  fence_edge_worker_heartbeat() { forbidden_activation_helper fence; }
  scale_edge_worker_zero_cas() { forbidden_activation_helper scale; }

  expected_trace=$'enable-chart\nlist-bases\nwait:fugue-fugue-edge-front\ndesired:fugue-fugue-edge-front\nactive:fugue-fugue-edge:fugue-fugue-edge-front\ncapture:fugue-fugue-edge-front fugue-fugue-edge-worker-b\npatch:fugue-fugue-edge-worker-a\ndelete:fugue-fugue-edge-worker-a\nwait:fugue-fugue-edge-worker-a\nport:fugue-fugue-edge-worker-a\ntcp:fugue-fugue-edge-worker-a:18443\nhttps:fugue-fugue-edge-worker-a:18443\ncapture:fugue-fugue-edge-front fugue-fugue-edge-worker-b\nfront:fugue-fugue-edge-front:a\nwait:fugue-fugue-edge-front\nfront-smoke:fugue-fugue-edge-front\nrecord:fugue-fugue-edge:a\npublic-smoke'
  for dry_run in false true; do
    : >"${trace}"
    : >"${kube_trace}"
    export FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN="${dry_run}"
    run_bluegreen_release
    [[ "$(cat "${trace}")" == "${expected_trace}" ]]
    [[ ! -s "${kube_trace}" ]]
    [[ "${FUGUE_PUBLIC_DATA_PLANE_ACTIVE_SLOTS_JSON}" == '{"fugue-fugue-edge":"a"}' ]]
  done
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${ROOT}/scripts/release_fugue_public_data_plane.sh"
  export FUGUE_EDGE_ACTIVATION_ENABLED=true
  export FUGUE_NAMESPACE=fugue-system
  export FUGUE_RELEASE_FULLNAME=fugue-fugue
  export FUGUE_RELEASE_INSTANCE=fugue
  export FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID=activation-fixture
  export FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=false
  export FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN=false
  export FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS=
  export FUGUE_EDGE_ACTIVATION_DIR="${TMP}/enabled-bluegreen"
  export FUGUE_EDGE_ACTIVATION_EXPECTED_FILE="${TMP}/enabled-bluegreen/expected.json"
  mkdir -p "${FUGUE_EDGE_ACTIVATION_DIR}"
  chmod 0700 "${FUGUE_EDGE_ACTIVATION_DIR}"
  trace="${TMP}/enabled-bluegreen.trace"
  : >"${trace}"
  FAIL_POINT=none
  step() { printf '%s\n' "$1" >>"${trace}"; [[ "${FAIL_POINT}" != "$1" ]]; }
  enable_bluegreen_chart_mode() { :; }
  bluegreen_worker_bases() { printf 'fugue-fugue-edge\n'; }
  wait_daemonset_ready() { :; }
  daemonset_desired_count() { printf '1'; }
  current_active_slot() { printf 'b'; }
  capture_daemonset_pods() { printf 'stable-pods\n'; }
  patch_inactive_worker() { :; }
  delete_worker_pods() { :; }
  worker_https_port() { printf '18443'; }
  check_worker_tcp() { :; }
  check_worker_https_smoke() { :; }
  write_front_active_slot() { step front; }
  check_public_smoke_on_front_nodes() { :; }
  record_active_slot_json() { printf '{"fugue-fugue-edge":"a"}'; }
  run_smoke_urls() { :; }
  recover_incomplete_edge_activation_before_release() { step recover; }
  prepare_edge_activation_candidate_record() { step prepare; }
  unretire_edge_worker() { step unretire; }
  isolate_inactive_edge_worker() { step isolate; }
  collect_edge_activation_candidate_material() { step collect; }
  edge_activation_api_replica_generation() { step api-generation || return 1; printf 'api-generation-1'; }
  edge_activation_advance() { step "advance:$1"; }
  edge_activation_get() { step get; }
  edge_activation_wait_all_api_ack() { step ack; }
  run_platform_release_evidence() { step platform-evidence; }
  edge_activation_complete_cutover_and_soak() { step soak; }

  run_bluegreen_release
  expected=$'recover\nprepare\nunretire\ncollect\napi-generation\nadvance:active-epoch-authoritative\nget\nack\nfront\nplatform-evidence\nsoak'
  [[ "$(cat "${trace}")" == "${expected}" ]]

  for FAIL_POINT in recover prepare unretire collect api-generation advance:active-epoch-authoritative get ack; do
    : >"${trace}"
    unset FUGUE_EDGE_ACTIVATION_PLAN_DIGEST
    if run_bluegreen_release; then
      echo "enabled blue-green accepted failed activation helper ${FAIL_POINT}" >&2
      exit 1
    fi
    if grep -qx front "${trace}"; then
      echo "front switched after failed activation helper ${FAIL_POINT}" >&2
      exit 1
    fi
  done
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${ROOT}/scripts/release_fugue_public_data_plane.sh"
  export FUGUE_EDGE_ACTIVATION_ENABLED=true
  export FUGUE_EDGE_ACTIVATION_DIR="${TMP}/platform-evidence"
  export FUGUE_EDGE_ACTIVATION_CURL_CONFIG="${TMP}/platform-evidence-curl.conf"
  export FUGUE_EDGE_ACTIVATION_API_URL=https://api.example.test
  export FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID=release-platform-evidence
  mkdir -m 0700 "${FUGUE_EDGE_ACTIVATION_DIR}"
  install -m 0600 /dev/null "${FUGUE_EDGE_ACTIVATION_CURL_CONFIG}"
  MOCK_PLATFORM_STATUS=passed
  curl() {
    local output="" arg
    while (($#)); do
      arg="$1"; shift
      if [[ "${arg}" == --output ]]; then output="$1"; shift; fi
    done
    cat >"${output}" <<JSON
{"schema":"platform-release-evidence/v1","status":"${MOCK_PLATFORM_STATUS}","release_epoch":"release-platform-evidence","evidence_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","metrics":{"request_count":17,"hard_failure_count":0,"origin_connected_application_5xx_count":5,"platform_error_classes":["origin_connected_application_5xx"]}}
JSON
    printf 200
  }
  run_platform_release_evidence
  MOCK_PLATFORM_STATUS=unknown
  if run_platform_release_evidence >/dev/null 2>&1; then
    echo "unknown platform evidence must stop the release" >&2
    exit 1
  fi
  MOCK_PLATFORM_STATUS=failed
  if run_platform_release_evidence >/dev/null 2>&1; then
    echo "failed platform evidence must stop the release" >&2
    exit 1
  fi
)

(
  export FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
  # shellcheck source=scripts/release_fugue_public_data_plane.sh
  source "${ROOT}/scripts/release_fugue_public_data_plane.sh"
  export FUGUE_NAMESPACE=fugue-system
  export FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID=pdp-20260802T123248Z-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  export FUGUE_EDGE_RESOURCES_JSON='{"requests":{"cpu":"10m"}}'
  export FUGUE_EDGE_CADDY_RESOURCES_JSON='{}'
  export FUGUE_EDGE_IMAGE_REPOSITORY=registry.example/fugue-edge
  export FUGUE_EDGE_IMAGE_TAG=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  export FUGUE_EDGE_IMAGE_DIGEST=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  kubectl_cmd() {
    printf '%s\n' '{"spec":{"template":{"metadata":{"labels":{"fugue.io/edge-slot":"a"}},"spec":{"containers":[{"name":"edge","env":[{"name":"FUGUE_EDGE_GROUP_ID","value":"edge-group-country-us"}]},{"name":"caddy"}]}}}}'
  }
  patch="$(container_patch_for_worker fugue-fugue-edge-worker-a)"
  PATCH="${patch}" python3 - <<'PY'
import json, os
patch=json.loads(os.environ["PATCH"])["spec"]["template"]
assert patch["metadata"]["labels"] == {"fugue.io/edge-group-id":"edge-group-country-us","fugue.io/edge-slot":"a"}
edge=next(item for item in patch["spec"]["containers"] if item["name"]=="edge")
assert edge["volumeMounts"] == [{"name":"edge-workload-identity","mountPath":"/var/run/fugue/edge-identity","readOnly":True}]
volume=patch["spec"]["volumes"][0]
assert volume["name"] == "edge-workload-identity"
assert [item["path"] for item in volume["downwardAPI"]["items"]] == ["edge_id","edge_group_id","slot","instance_uid","release_epoch","heartbeat_fenced"]
PY

  captured=""
  wait_daemonset_ready() { :; }
  fence_edge_worker_heartbeat() { :; }
  scale_edge_worker_zero_cas() { captured="$2"; }
  isolate_inactive_edge_worker inactive active
  [[ "${captured}" =~ ^failed-[0-9a-f]{56}$ && ${#captured} -eq 63 ]]
)
printf '[test_edge_activation] ok\n'
