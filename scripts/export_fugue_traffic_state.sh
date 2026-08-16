#!/usr/bin/env bash

set -euo pipefail

fail() {
  printf 'export_fugue_traffic_state.sh: %s\n' "$*" >&2
  exit 1
}

if [[ $# -ne 1 ]]; then
  printf 'Usage: scripts/export_fugue_traffic_state.sh <output-dir>\n' >&2
  exit 1
fi
command -v kubectl >/dev/null 2>&1 || fail "kubectl is required"
command -v jq >/dev/null 2>&1 || fail "jq is required"

output_dir="$1"
namespace="${FUGUE_NAMESPACE:-fugue-system}"
mkdir -p "${output_dir}"
[[ -z "$(find "${output_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]] || fail "output directory must be empty"

kubectl -n "${namespace}" get configmaps -l 'fugue.pro/authority-store=true' -o json >"${output_dir}/authority-and-candidate.json"
kubectl -n "${namespace}" get configmaps -l 'app.kubernetes.io/managed-by=fugue-release-guardian' -o json >"${output_dir}/guardian-current-lkg.json"
kubectl -n "${namespace}" get pods,deployments,daemonsets -o json >"${output_dir}/workload-inventory.json"
kubectl -n "${namespace}" get configmaps -o json | jq '{apiVersion,kind,items:[.items[] | select(
  (.metadata.name | test("edge-control|edge-route|dns-bundle|traffic-epoch")) or
  (.metadata.labels["fugue.pro/group"] // "" | test("edge-group-country-(de|us)"))
)]}' >"${output_dir}/route-dns-control-state.json"

jq -n \
  --arg exported_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg namespace "${namespace}" \
  --arg context "$(kubectl config current-context)" \
  --arg git_commit "$(git rev-parse HEAD 2>/dev/null || true)" \
  '{schema:"fugue-traffic-state-export/v1",exported_at:$exported_at,namespace:$namespace,
    kubernetes_context:$context,git_commit:$git_commit,
    files:["authority-and-candidate.json","guardian-current-lkg.json","workload-inventory.json","route-dns-control-state.json"]}' \
  >"${output_dir}/manifest.json"

find "${output_dir}" -type f -maxdepth 1 -print0 | sort -z | xargs -0 shasum -a 256 >"${output_dir}/SHA256SUMS"
printf 'traffic_state_export=%s complete=true\n' "${output_dir}"
