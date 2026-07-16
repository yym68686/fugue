#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UPGRADE_SCRIPT="${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh"

python3 - "${UPGRADE_SCRIPT}" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
source = path.read_text(encoding="utf-8")

main_start = source.index("\nmain() {\n")
activation_marker = source.index(
    "\n# The production activation is source-only", main_start
)
main_body = source[main_start:activation_marker]
dispatcher_source = source.index(
    'source "${REPO_ROOT}/scripts/lib/control_plane_release_domains.sh"',
    activation_marker,
)
production_source = source.index(
    'source "${REPO_ROOT}/scripts/lib/control_plane_release_domain_production.sh"',
    dispatcher_source,
)
lib_only_guard = source.index(
    'if [[ "${FUGUE_UPGRADE_LIB_ONLY:-false}" == "true" ]]', production_source
)
observability_guard = source.index(
    'if [[ "${FUGUE_CONTROL_PLANE_OBSERVABILITY_ONLY:-false}" == "true" ]]',
    lib_only_guard,
)
main_call = source.index('main "$@"', observability_guard)

if not (main_start < activation_marker < dispatcher_source < production_source < lib_only_guard):
    raise SystemExit("release-domain libraries are not loaded after all upgrade helpers")
if not (lib_only_guard < observability_guard < main_call):
    raise SystemExit("LIB_ONLY or observability-only execution order changed")

gate_flag = main_body.index('CONTROL_PLANE_RELEASE_DOMAIN_GATE_ACTIVE="true"')
evidence_init = main_body.index("initialize_control_plane_release_evidence")
revision_read = main_body.index('PREVIOUS_REVISION="$(helm_current_revision)"')
gate_call = main_body.index("run_control_plane_atomic_domain_gate")
if gate_flag > evidence_init:
    raise SystemExit("domain cleanup guard is not active before EXIT/signal setup")
if revision_read > gate_call:
    raise SystemExit("atomic domain gate runs before the read-only Helm revision fence")
if main_body.count("run_control_plane_atomic_domain_gate") != 1:
    raise SystemExit("main must have exactly one atomic release-domain entrypoint")

for forbidden in (
    "run_release_preflight",
    "prepare_release_domains",
    "authoritative_dns_dig_preflight",
    "acquire_control_plane_backup_coordination_lease",
    "drain_control_plane_backup_before_schema_rollout",
    "ensure_host_time_sync",
    "ensure_control_plane_observability",
    "ensure_local_registry_mirror_config",
    "recover_primary_node_if_needed",
    "relieve_primary_disk_pressure",
    "apply_chart_crds",
    "prepare_dns_manifest_transaction",
    "with_frozen_control_plane_helm_upgrade_argv",
    "run_dns_manifest_transaction_after_helm",
    "rollback_release_transaction",
):
    if forbidden in main_body:
        raise SystemExit(f"legacy mutation path remains reachable from main: {forbidden}")
PY

FUGUE_UPGRADE_LIB_ONLY=true source "${UPGRADE_SCRIPT}"

STUB_STATUS=0
STUB_CALLS=0
control_plane_release_run_atomic_domain_release() {
  STUB_CALLS=$((STUB_CALLS + 1))
  return "${STUB_STATUS}"
}

assert_forwarded_status() {
  local expected="$1"
  local actual=0

  STUB_STATUS="${expected}"
  STUB_CALLS=0
  if run_control_plane_atomic_domain_gate >/dev/null 2>&1; then
    actual=0
  else
    actual=$?
  fi
  [[ "${actual}" == "${expected}" ]] || {
    printf 'atomic main gate rewrote status %s to %s\n' "${expected}" "${actual}" >&2
    return 1
  }
  [[ "${STUB_CALLS}" == "1" ]] || {
    printf 'atomic main gate called production entrypoint %s times\n' "${STUB_CALLS}" >&2
    return 1
  }
}

for status in 0 1 2 129 130 143; do
  assert_forwarded_status "${status}"
done

printf 'control-plane atomic main wiring tests passed\n'
