#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
UPGRADE_SCRIPT="${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh"

fail() {
  printf 'test_control_plane_observability_assets.sh: %s\n' "$*" >&2
  exit 1
}

assert_contains() {
  local file="$1"
  local want="$2"
  grep -Fq -- "${want}" "${file}" || fail "expected ${file} to contain ${want}"
}

bash -n "${UPGRADE_SCRIPT}"

assert_contains "${UPGRADE_SCRIPT}" "fugue-control-plane-baseline-sample"
assert_contains "${UPGRADE_SCRIPT}" "fugue-control-plane-baseline.timer"
assert_contains "${UPGRADE_SCRIPT}" "OnUnitActiveSec=30s"
assert_contains "${UPGRADE_SCRIPT}" "fugue-k3s-incident-snapshot"
assert_contains "${UPGRADE_SCRIPT}" "OnFailure=fugue-k3s-failure@%n.service"
assert_contains "${UPGRADE_SCRIPT}" "diagnosis.txt"
assert_contains "${UPGRADE_SCRIPT}" "root_cause_status=evidence_summary_only_not_a_root_cause_claim"
assert_contains "${UPGRADE_SCRIPT}" "primary_failure_signal="
assert_contains "${UPGRADE_SCRIPT}" "memory.events"
assert_contains "${UPGRADE_SCRIPT}" "io.stat"
assert_contains "${UPGRADE_SCRIPT}" "k3s crictl stats"
assert_contains "${UPGRADE_SCRIPT}" "kube-metrics-key"
assert_contains "${UPGRADE_SCRIPT}" "etcd-metrics-key"
assert_contains "${UPGRADE_SCRIPT}" "/var/log/fugue/kubernetes/audit.log"
assert_contains "${UPGRADE_SCRIPT}" "k3s-config-redacted"
assert_contains "${UPGRADE_SCRIPT}" "latest-k3s.tar.gz"

printf 'control-plane observability asset tests passed\n'
