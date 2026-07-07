#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

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

export FUGUE_UPGRADE_LIB_ONLY=true
# shellcheck source=scripts/upgrade_fugue_control_plane.sh
source "${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh"

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

  FUGUE_EDGE_BLUE_GREEN_DEFAULT_ACTIVE_SLOT=a
  active_slot_from_front() { :; }
  active_slot_from_record() { :; }
  assert_eq "$(current_active_slot "fugue-fugue-edge-dynamic" "fugue-fugue-edge-dynamic-front")" "a" "missing dynamic release record must fall back to default slot"
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
assert_eq "$(robustness_status_summary "${ROBUSTNESS_HEALTH_GATE_BASELINE_FILE}")" "pass=false block_rollout=true checks=2 incidents=1 baseline_incidents=1 new_incidents=0" "matching robustness baseline must tolerate existing incidents"
export TEST_CURL_RESPONSE_JSON='{"status":{"pass":false,"block_rollout":true,"checks":[{"name":"node_policy","subject":"platform-autonomy","pass":false,"severity":"degraded","observed":"pass=false count=7"},{"name":"route_active","subject":"route:example","pass":false,"severity":"block_publish","message":"missing route"}],"incidents":[{"id":"robust_existing_changed","severity":"degraded","subject":"platform-autonomy","check_name":"node_policy","observed":"pass=false count=7"},{"id":"robust_new","severity":"block_publish","subject":"route:example","check_name":"route_active","message":"missing route"}]}}'
if robustness_output="$(robustness_status_summary "${ROBUSTNESS_HEALTH_GATE_BASELINE_FILE}")"; then
  fail "robustness baseline must fail on new incidents"
fi
[[ "${robustness_output}" == *"new_incidents=1"* ]] || fail "robustness baseline failure must report new incident count"
[[ "${robustness_output}" == *"new_blockers=route_active(route:example): missing route"* ]] || fail "robustness baseline failure must report new block_publish blocker"
export TEST_CURL_RESPONSE_JSON='{"status":{"pass":false,"block_rollout":true,"checks":[{"name":"node_policy","subject":"platform-autonomy","pass":false,"severity":"degraded","observed":"pass=false count=7"},{"name":"app_continuity_invariant","subject":"app:example","pass":false,"severity":"block_publish","message":"ready replicas 0 below desired 1"}],"incidents":[{"id":"robust_existing_changed","severity":"degraded","subject":"platform-autonomy","check_name":"node_policy","observed":"pass=false count=7"},{"id":"robust_introduced","severity":"block_publish","subject":"app:example","check_name":"app_continuity_invariant","message":"ready replicas 0 below desired 1"}]}}'
robustness_output="$(robustness_status_summary "${ROBUSTNESS_HEALTH_GATE_BASELINE_FILE}")" || fail "newly introduced robustness checks must not fail a mixed-version baseline rollout"
[[ "${robustness_output}" == *"introduced_blockers=app_continuity_invariant(app:example): ready replicas 0 below desired 1"* ]] || fail "introduced blocker summary must be reported"
[[ "${robustness_output}" == *"introduced_incidents=block_publish:app_continuity_invariant(app:example): ready replicas 0 below desired 1"* ]] || fail "introduced incident summary must be reported"
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

FUGUE_RELEASE_CHANGED_FILES=$'internal/edge/service.go'
public_data_plane_changed || fail "edge code changes must mark public data-plane changed"
public_data_plane_worker_image_changed || fail "edge code changes must mark worker image changed"

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

FUGUE_RELEASE_CHANGED_FILES=$'deploy/helm/fugue/templates/dns-daemonset.yaml'
public_data_plane_changed || fail "dns daemonset changes must mark public data-plane changed"

FUGUE_RELEASE_CHANGED_FILES=$'cmd/fugue-image-cache/main.go'
node_local_build_plane_changed || fail "image-cache code changes must mark build-plane changed"
node_local_build_plane_preflight_override_allowed || fail "image-cache fixes must be allowed to bypass registry/node-policy preflight"

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
FUGUE_CONTROL_PLANE_BACKUP_DRAIN_WAIT_SECONDS=0
FUGUE_CONTROL_PLANE_BACKUP_DRAIN_POLL_SECONDS=1
FUGUE_CONTROL_PLANE_BACKUP_DRAIN_RECENT_SUCCESS_SECONDS=90000
FUGUE_CONTROL_PLANE_BACKUP_DRAIN_POST_TERMINATE_SLEEP_SECONDS=0
drain_control_plane_backup_before_schema_rollout
grep -q '^terminated$' "${BACKUP_DRAIN_MARKER}" || fail "backup drain must terminate active pg_dump after timeout when a recent successful backup exists"
rm -f "${BACKUP_DRAIN_MARKER}"

printf '[test_release_domain_safety] ok\n'
