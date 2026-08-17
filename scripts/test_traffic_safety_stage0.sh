#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
grep -q 'go build -trimpath -o "${RUNNER_TEMP}/fugue-public-dns-query" ./scripts/public_dns_query.go' "${REPO_ROOT}/.github/workflows/ci.yml"
grep -q 'FUGUE_PUBLIC_DNS_QUERY_BIN="${RUNNER_TEMP}/fugue-public-dns-query"' "${REPO_ROOT}/.github/workflows/ci.yml"
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

mkdir -p "${tmpdir}/bin"
cat >"${tmpdir}/bin/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
output_file=""
payload=""
method="GET"
while (( $# > 0 )); do
  case "$1" in
    --output) output_file="$2"; shift 2 ;;
    --data) payload="$2"; shift 2 ;;
    --request) method="$2"; shift 2 ;;
    *) shift ;;
  esac
done
if [[ "${method}" == "GET" ]]; then
  cat >"${output_file}" <<'JSON'
{"policy":{"hostname":"api.safety.example","edge_group_id":"edge-group-country-us","excluded_edge_ids":["edge-existing"],"excluded_edge_group_ids":["edge-group-country-de"],"exclusion_generation":7,"exclusion_fence":"fence-7","route_policy":"edge_enabled"}}
JSON
  printf '200'
  exit 0
fi
jq -e '
  .expected_exclusion_generation == 7 and
  .expected_exclusion_fence == "fence-7" and
  .excluded_edge_ids == ["edge-drained", "edge-existing"] and
  .excluded_edge_group_ids == ["edge-group-country-de"]
' <<<"${payload}" >/dev/null
jq -n --argjson request "${payload}" '{policy:{hostname:"api.safety.example",edge_group_id:$request.edge_group_id,
  excluded_edge_ids:$request.excluded_edge_ids,excluded_edge_group_ids:$request.excluded_edge_group_ids,
  route_policy:$request.route_policy}}' >"${output_file}"
printf '200'
EOF
chmod 0755 "${tmpdir}/bin/curl"
PATH="${tmpdir}/bin:${PATH}" FUGUE_API_URL="https://api.safety.example" FUGUE_API_KEY="test-key" \
  bash "${REPO_ROOT}/scripts/apply_fugue_traffic_safety.sh" --apply "${CONFIG}" >/dev/null

printf 'docs/readme.md\nscripts/probe_fugue_public_dns.sh\n' >"${tmpdir}/changed"
: >"${tmpdir}/github-output"
bash "${REPO_ROOT}/scripts/emit_traffic_safety_changed.sh" "${tmpdir}/changed" "${tmpdir}/github-output"
grep -qx 'traffic_safety_changed=true' "${tmpdir}/github-output"

git init -q "${tmpdir}/repo"
git -C "${tmpdir}/repo" config user.email traffic-safety-test@example.com
git -C "${tmpdir}/repo" config user.name traffic-safety-test
mkdir -p "${tmpdir}/repo/.github/workflows"
cp "${REPO_ROOT}/.github/workflows/ci.yml" "${tmpdir}/repo/.github/workflows/ci.yml"
git -C "${tmpdir}/repo" add .github/workflows/ci.yml
git -C "${tmpdir}/repo" commit -qm base
base_revision="$(git -C "${tmpdir}/repo" rev-parse HEAD)"
sed -i.bak 's/PREPUSH_TIMEOUT_SECONDS: "240"/PREPUSH_TIMEOUT_SECONDS: "239"/' "${tmpdir}/repo/.github/workflows/ci.yml"
rm "${tmpdir}/repo/.github/workflows/ci.yml.bak"
git -C "${tmpdir}/repo" add .github/workflows/ci.yml
git -C "${tmpdir}/repo" commit -qm budget-only
budget_revision="$(git -C "${tmpdir}/repo" rev-parse HEAD)"

printf '.github/workflows/ci.yml\n' >"${tmpdir}/changed"
: >"${tmpdir}/github-output"
FUGUE_REPO_ROOT="${tmpdir}/repo" bash "${REPO_ROOT}/scripts/emit_traffic_safety_changed.sh" "${tmpdir}/changed" "${tmpdir}/github-output" "${base_revision}" "${budget_revision}"
grep -qx 'traffic_safety_changed=false' "${tmpdir}/github-output"

sed -i.bak 's/timeout-minutes: 30/timeout-minutes: 31/' "${tmpdir}/repo/.github/workflows/ci.yml"
rm "${tmpdir}/repo/.github/workflows/ci.yml.bak"
git -C "${tmpdir}/repo" add .github/workflows/ci.yml
git -C "${tmpdir}/repo" commit -qm traffic-job
traffic_revision="$(git -C "${tmpdir}/repo" rev-parse HEAD)"
: >"${tmpdir}/github-output"
FUGUE_REPO_ROOT="${tmpdir}/repo" bash "${REPO_ROOT}/scripts/emit_traffic_safety_changed.sh" "${tmpdir}/changed" "${tmpdir}/github-output" "${budget_revision}" "${traffic_revision}"
grep -qx 'traffic_safety_changed=true' "${tmpdir}/github-output"

printf 'scripts/emit_traffic_safety_changed.sh\n' >"${tmpdir}/changed"
: >"${tmpdir}/github-output"
bash "${REPO_ROOT}/scripts/emit_traffic_safety_changed.sh" "${tmpdir}/changed" "${tmpdir}/github-output"
grep -qx 'traffic_safety_changed=false' "${tmpdir}/github-output"
