#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT
CONFIG="${tmpdir}/traffic-safety-stage0.json"
cat >"${CONFIG}" <<'EOF'
{
  "apiVersion": "release.fugue.dev/v1",
  "kind": "TrafficSafetyStage0",
  "freeze": {
    "newRecoveryExceptions": true,
    "newWitnessExceptions": true,
    "newPredecessorExceptions": true
  },
  "pins": [{
    "hostname": "api.safety.example",
    "edgeGroupId": "edge-group-country-us",
    "drainedEdgeIds": ["edge-drained"],
    "drainedEdgeGroupIds": ["edge-group-country-de"],
    "routePolicy": "edge_enabled",
    "minHealthyEdgeNodes": 1,
    "exclusionReason": "traffic-safety-test",
    "exclusionTTLSeconds": 86400
  }],
  "publicDNSGate": {
    "requiredAnswerIPs": ["192.0.2.10"],
    "forbiddenAnswerIPs": ["192.0.2.20"]
  }
}
EOF

bash "${REPO_ROOT}/scripts/apply_fugue_traffic_safety.sh" --check "${CONFIG}" >/dev/null

printf 'docs/readme.md\nscripts/probe_fugue_public_dns.sh\n' >"${tmpdir}/changed"
: >"${tmpdir}/github-output"
bash "${REPO_ROOT}/scripts/emit_traffic_safety_changed.sh" "${tmpdir}/changed" "${tmpdir}/github-output"
grep -qx 'traffic_safety_changed=true' "${tmpdir}/github-output"
