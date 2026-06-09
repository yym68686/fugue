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

FUGUE_RELEASE_CHANGED_FILES=$'deploy/helm/fugue/templates/registry-deployment.yaml'
stateful_dependency_changed || fail "registry template changes must mark stateful dependency changed"

for maintenance_template in registry-janitor-cronjob.yaml registry-gc-cronjob.yaml registry-gc-lease.yaml; do
  FUGUE_RELEASE_CHANGED_FILES="deploy/helm/fugue/templates/${maintenance_template}"
  if stateful_dependency_changed; then
    fail "${maintenance_template} must be releasable without the stateful dependency override"
  fi
done

printf '[test_release_domain_safety] ok\n'
