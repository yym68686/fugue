#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
UPGRADE_SCRIPT="${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh"
INSTALL_SCRIPT="${REPO_ROOT}/scripts/install_fugue_ha.sh"

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
bash -n "${INSTALL_SCRIPT}"

assert_contains "${UPGRADE_SCRIPT}" "fugue-control-plane-baseline-sample"
assert_contains "${UPGRADE_SCRIPT}" "fugue-control-plane-baseline.timer"
assert_contains "${UPGRADE_SCRIPT}" "ensure_control_plane_observability_via_node_janitor"
assert_contains "${UPGRADE_SCRIPT}" "ensure_host_time_sync"
assert_contains "${UPGRADE_SCRIPT}" "try_primary_host_root_command"
assert_contains "${UPGRADE_SCRIPT}" "run_host_script_via_node_janitor"
assert_contains "${UPGRADE_SCRIPT}" "ensure_primary_host_time_sync_via_node_janitor"
assert_contains "${UPGRADE_SCRIPT}" "exec -i"
assert_contains "${UPGRADE_SCRIPT}" "nsenter -t 1 -m -u -i -n -p"
assert_contains "${UPGRADE_SCRIPT}" "configured systemd-timesyncd poll interval min=32s max=64s but could not restart time synchronization"
assert_contains "${UPGRADE_SCRIPT}" "/etc/systemd/timesyncd.conf.d/10-fugue-managed.conf"
assert_contains "${UPGRADE_SCRIPT}" "PollIntervalMaxSec=%ss"
assert_contains "${UPGRADE_SCRIPT}" "node-role.kubernetes.io/control-plane=true"
assert_contains "${UPGRADE_SCRIPT}" "fugue.io/control-plane-desired-role=member"
assert_contains "${UPGRADE_SCRIPT}" "skip primary mesh restore because host root access is unavailable"
assert_contains "${UPGRADE_SCRIPT}" "app.kubernetes.io/component=node-janitor"
assert_contains "${UPGRADE_SCRIPT}" "FUGUE_CONTROL_PLANE_OBSERVABILITY_RESTART_K3S=false"
assert_contains "${UPGRADE_SCRIPT}" "restore_local_control_plane_automation_bundle_from_secret"
assert_contains "${UPGRADE_SCRIPT}" "recovered control-plane automation SSH bundle from"
assert_contains "${UPGRADE_SCRIPT}" "bootstrap_local_control_plane_automation_bundle"
assert_contains "${UPGRADE_SCRIPT}" "bootstrapping control-plane automation SSH bundle on this server"
assert_contains "${UPGRADE_SCRIPT}" 'FUGUE_API_REPLICA_COUNT="${FUGUE_API_REPLICA_COUNT:-2}"'
assert_contains "${UPGRADE_SCRIPT}" "maxSurge: 1"
assert_contains "${UPGRADE_SCRIPT}" "memory: 768Mi"
assert_contains "${UPGRADE_SCRIPT}" "memory: 1536Mi"
assert_contains "${UPGRADE_SCRIPT}" "memory: 256Mi"
assert_contains "${UPGRADE_SCRIPT}" "memory: 512Mi"
assert_contains "${UPGRADE_SCRIPT}" "OnUnitActiveSec=30s"
assert_contains "${UPGRADE_SCRIPT}" "fugue-k3s-incident-snapshot"
assert_contains "${UPGRADE_SCRIPT}" "OnFailure=fugue-k3s-failure@%n.service"
assert_contains "${UPGRADE_SCRIPT}" "diagnosis.txt"
assert_contains "${UPGRADE_SCRIPT}" "root_cause_status=evidence_summary_only_not_a_root_cause_claim"
assert_contains "${UPGRADE_SCRIPT}" "primary_failure_signal="
assert_contains "${UPGRADE_SCRIPT}" "memory.events"
assert_contains "${UPGRADE_SCRIPT}" "io.stat"
assert_contains "${UPGRADE_SCRIPT}" "top_rss_processes"
assert_contains "${UPGRADE_SCRIPT}" "processes-by-rss"
assert_contains "${UPGRADE_SCRIPT}" "k3s crictl stats"
assert_contains "${UPGRADE_SCRIPT}" "kubectl-top-containers"
assert_contains "${UPGRADE_SCRIPT}" "kube-metrics-key"
assert_contains "${UPGRADE_SCRIPT}" "etcd-metrics-key"
assert_contains "${UPGRADE_SCRIPT}" "/var/log/fugue/kubernetes/audit.log"
assert_contains "${UPGRADE_SCRIPT}" "k3s-config-redacted"
assert_contains "${UPGRADE_SCRIPT}" "latest-k3s.tar.gz"
assert_contains "${INSTALL_SCRIPT}" "ensure_host_time_sync_on_aliases"
assert_contains "${INSTALL_SCRIPT}" "/etc/systemd/timesyncd.conf.d/10-fugue-managed.conf"
assert_contains "${INSTALL_SCRIPT}" "PollIntervalMaxSec=64s"

python3 - "${REPO_ROOT}/deploy/helm/fugue/values.yaml" "${INSTALL_SCRIPT}" "${UPGRADE_SCRIPT}" <<'PY'
from pathlib import Path
import sys

paths = tuple(Path(value) for value in sys.argv[1:])
resource_contract = (
    '  resources:\n'
    '    requests:\n'
    '      cpu: 100m\n'
    '      memory: 256Mi\n'
    '    limits:\n'
    '      cpu: "1"\n'
    '      memory: 768Mi\n'
)
old_resource_contract = resource_contract.replace('memory: 768Mi\n', 'memory: 512Mi\n')
strategy_contract = (
    '  strategy:\n'
    '    rollingUpdate:\n'
    '      maxUnavailable: 0\n'
    '      maxSurge: 2\n'
)

def controller_block(path: Path) -> str:
    body = path.read_text()
    marker = '\ncontroller:\n'
    if body.count(marker) != 1:
        raise SystemExit(f'{path}: expected exactly one top-level controller block')
    start = body.index(marker) + 1
    end = len(body)
    for offset, line in enumerate(body[start:].splitlines(keepends=True)):
        if offset == 0:
            continue
        if line.strip() and not line[0].isspace():
            end = start + sum(len(value) for value in body[start:].splitlines(keepends=True)[:offset])
            break
    return body[start:end]

blocks = {path: controller_block(path) for path in paths}
for path, block in blocks.items():
    if block.count(resource_contract) != 1:
        raise SystemExit(f'{path}: controller resource contract is not exact 100m/256Mi/1/768Mi')
    if old_resource_contract in block:
        raise SystemExit(f'{path}: controller retained the former 512Mi memory limit')

values_block = blocks[paths[0]]
upgrade_block = blocks[paths[2]]
if values_block.count(strategy_contract) != 1 or upgrade_block.count(strategy_contract) != 1:
    raise SystemExit('controller surge-first strategy drifted from maxUnavailable=0/maxSurge=2')
if '  strategy:\n' in blocks[paths[1]]:
    raise SystemExit('legacy install must inherit the chart controller rollout strategy instead of overriding it')

for replacement in ('memory: 512Mi\n', 'memory: 769Mi\n'):
    near_miss = values_block.replace('memory: 768Mi\n', replacement, 1)
    if resource_contract in near_miss:
        raise SystemExit(f'controller resource contract accepted near miss {replacement.strip()}')
PY

printf 'control-plane observability asset tests passed\n'
