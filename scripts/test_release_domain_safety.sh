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

FUGUE_RELEASE_CHANGED_FILES=$'scripts/upgrade_fugue_control_plane.sh\nscripts/test_release_domain_safety.sh'
node_local_build_plane_preflight_override_allowed || fail "release preflight fix must be allowed to publish itself while registry/node-policy preflight is degraded"

FUGUE_RELEASE_CHANGED_FILES=$'cmd/fugue-image-cache/main.go\ninternal/api/server.go'
if node_local_build_plane_preflight_override_allowed; then
  fail "mixed unrelated API changes must not bypass registry/node-policy preflight"
fi

ORIGINAL_REPO_ROOT="${REPO_ROOT}"
TMP_REPO_ROOT="$(mktemp -d)"
trap 'rm -rf "${TMP_REPO_ROOT}"' EXIT
git -C "${TMP_REPO_ROOT}" init -q
git -C "${TMP_REPO_ROOT}" config user.email test@example.com
git -C "${TMP_REPO_ROOT}" config user.name "Fugue Test"
mkdir -p "${TMP_REPO_ROOT}/cmd/fugue-image-cache" "${TMP_REPO_ROOT}/scripts"
printf 'module example.com/fugue-test\n' >"${TMP_REPO_ROOT}/go.mod"
: >"${TMP_REPO_ROOT}/go.sum"
printf 'FROM scratch\n' >"${TMP_REPO_ROOT}/Dockerfile.image-cache"
printf 'package main\nfunc main() {}\n' >"${TMP_REPO_ROOT}/cmd/fugue-image-cache/main.go"
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
REPO_ROOT="${TMP_REPO_ROOT}"
if image_cache_source_changed_between_refs "${BASE_REF}" "${SCRIPT_REF}"; then
  fail "script-only changes must not roll image-cache"
fi
image_cache_source_changed_between_refs "${SCRIPT_REF}" "${IMAGE_CACHE_REF}" || fail "image-cache source changes must allow image rollout"
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
