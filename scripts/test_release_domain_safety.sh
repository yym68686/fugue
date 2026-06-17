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

export FUGUE_UPGRADE_LIB_ONLY=true
# shellcheck source=scripts/upgrade_fugue_control_plane.sh
source "${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh"

assert_eq "$(image_ref_repository 'ghcr.io/acme/fugue-edge:sha123')" "ghcr.io/acme/fugue-edge" "repository parses tagged ghcr image"
assert_eq "$(image_ref_tag 'ghcr.io/acme/fugue-edge:sha123')" "sha123" "tag parses tagged ghcr image"
assert_eq "$(image_ref_repository 'localhost:5000/acme/fugue-edge:sha123')" "localhost:5000/acme/fugue-edge" "repository keeps registry port"
assert_eq "$(image_ref_tag 'localhost:5000/acme/fugue-edge')" "latest" "missing tag defaults to latest"
assert_eq "$(image_ref_repository 'ghcr.io/acme/fugue-edge@sha256:abc')" "ghcr.io/acme/fugue-edge" "repository strips digest"

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

FUGUE_RELEASE_CHANGED_FILES=$'cmd/fugue-image-cache/main.go\ninternal/controller/deploy_image_guard.go\ndeploy/helm/fugue/templates/controller-deployment.yaml'
node_local_build_plane_preflight_override_allowed || fail "builder registry routing fixes must be allowed to bypass registry/node-policy preflight"

FUGUE_RELEASE_CHANGED_FILES=$'internal/api/join_cluster.go\ninternal/api/node_updater.go\ninternal/api/node_updater_test.go'
node_local_build_plane_preflight_override_allowed || fail "node updater registry mirror reload fixes must be allowed to bypass registry/node-policy preflight"

FUGUE_RELEASE_CHANGED_FILES=$'internal/api/cluster_node_policy.go\ninternal/api/cluster_node_policy_seed_test.go\ninternal/api/cluster_node_policy_status.go\ninternal/api/cluster_node_views.go\ninternal/api/cluster_node_views_test.go\ninternal/api/join_cluster.go\ninternal/api/runtime_pool.go\ninternal/api/server_test.go\ninternal/store/machines.go\ninternal/store/store_test.go'
node_local_build_plane_preflight_override_allowed || fail "node-policy join fixes must be allowed to bypass existing node-policy preflight"

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
REPO_ROOT="${TMP_REPO_ROOT}"
if image_cache_source_changed_between_refs "${BASE_REF}" "${SCRIPT_REF}"; then
  fail "script-only changes must not roll image-cache"
fi
image_cache_source_changed_between_refs "${SCRIPT_REF}" "${IMAGE_CACHE_REF}" || fail "image-cache source changes must allow image rollout"
BEFORE_SHA="${IMAGE_CACHE_REF}"
AFTER_SHA="${IMAGE_CACHE_RESOURCES_REF}"
FUGUE_RELEASE_CHANGED_FILES=$'deploy/helm/fugue/values.yaml'
node_local_build_plane_resource_values_changed || fail "image-cache resource values must be recognized"
node_local_build_plane_preflight_override_allowed || fail "image-cache resource values must bypass registry/node-policy preflight"
BEFORE_SHA="${IMAGE_CACHE_RESOURCES_REF}"
AFTER_SHA="${REGISTRY_GC_RESOURCES_REF}"
if node_local_build_plane_resource_values_changed; then
  fail "registryGC values must not be recognized as image-cache resource values"
fi
if node_local_build_plane_preflight_override_allowed; then
  fail "registryGC values must not bypass registry/node-policy preflight"
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

printf '[test_release_domain_safety] ok\n'
