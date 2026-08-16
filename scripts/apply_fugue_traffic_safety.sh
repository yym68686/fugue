#!/usr/bin/env bash

set -euo pipefail

fail() {
  printf 'apply_fugue_traffic_safety.sh: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage:
  scripts/apply_fugue_traffic_safety.sh --check <config.json>
  scripts/apply_fugue_traffic_safety.sh --apply <config.json>

Apply mode requires FUGUE_API_URL (or FUGUE_BASE_URL) and FUGUE_API_KEY.
Each hostname is updated with one CAS-protected PUT so the regional pin and
traffic drains cannot be observed separately.
EOF
}

mode="${1:-}"
config_path="${2:-}"
if [[ $# -ne 2 || ( "${mode}" != "--check" && "${mode}" != "--apply" ) ]]; then
  usage >&2
  exit 1
fi
[[ -f "${config_path}" ]] || fail "config does not exist: ${config_path}"
command -v jq >/dev/null 2>&1 || fail "jq is required"

jq -e '
  .apiVersion == "release.fugue.dev/v1" and
  .kind == "TrafficSafetyStage0" and
  (.freeze.newRecoveryExceptions == true) and
  (.freeze.newWitnessExceptions == true) and
  (.freeze.newPredecessorExceptions == true) and
  (.pins | type == "array" and length > 0) and
  (all(.pins[];
    (.hostname | type == "string" and test("^[A-Za-z0-9][A-Za-z0-9.-]*\\.[A-Za-z0-9.-]+$")) and
    (.edgeGroupId | type == "string" and startswith("edge-group-")) and
    (.drainedEdgeIds | type == "array") and
    (.drainedEdgeGroupIds | type == "array") and
    (.routePolicy == "edge_enabled" or .routePolicy == "edge_canary") and
    (.minHealthyEdgeNodes | type == "number" and . >= 1) and
    (.exclusionReason | type == "string" and length > 0) and
    (.exclusionTTLSeconds | type == "number" and . >= 3600))) and
  (.publicDNSGate.requiredAnswerIPs | type == "array" and length > 0) and
  (.publicDNSGate.forbiddenAnswerIPs | type == "array" and length > 0)
' "${config_path}" >/dev/null || fail "config does not satisfy the stage-0 safety contract"

if [[ "${mode}" == "--check" ]]; then
  printf 'traffic_safety_config=%s valid=true\n' "${config_path}"
  exit 0
fi

command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v python3 >/dev/null 2>&1 || fail "python3 is required"
base_url="${FUGUE_BASE_URL:-${FUGUE_API_URL:-}}"
api_key="${FUGUE_API_KEY:-}"
[[ -n "${base_url}" ]] || fail "FUGUE_API_URL or FUGUE_BASE_URL is required"
[[ -n "${api_key}" ]] || fail "FUGUE_API_KEY is required"
base_url="${base_url%/}"

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

while IFS= read -r encoded_pin; do
  pin="$(jq -rn --arg value "${encoded_pin}" '$value|@base64d')"
  hostname="$(jq -r '.hostname' <<<"${pin}")"
  escaped_hostname="$(jq -nr --arg value "${hostname}" '$value|@uri')"
  policy_url="${base_url}/v1/edge/route-policies/${escaped_hostname}"
  current_file="${tmpdir}/current.json"
  status="$(curl --silent --show-error --output "${current_file}" --write-out '%{http_code}' \
    --header "Authorization: Bearer ${api_key}" "${policy_url}")"
  case "${status}" in
    200)
      expected_generation="$(jq -r '.policy.exclusion_generation // 0' "${current_file}")"
      expected_fence="$(jq -r '.policy.exclusion_fence // ""' "${current_file}")"
      existing_edge_ids="$(jq -c '.policy.excluded_edge_ids // []' "${current_file}")"
      existing_edge_group_ids="$(jq -c '.policy.excluded_edge_group_ids // []' "${current_file}")"
      ;;
    404)
      expected_generation=0
      expected_fence=""
      existing_edge_ids='[]'
      existing_edge_group_ids='[]'
      ;;
    *)
      fail "read current policy for ${hostname} returned HTTP ${status}"
      ;;
  esac
  effective_pin="$(jq -cn \
    --argjson pin "${pin}" \
    --argjson existing_edge_ids "${existing_edge_ids}" \
    --argjson existing_edge_group_ids "${existing_edge_group_ids}" \
    '$pin
      | .drainedEdgeIds = (($existing_edge_ids + .drainedEdgeIds) | unique)
      | .drainedEdgeGroupIds = (($existing_edge_group_ids + .drainedEdgeGroupIds) | unique)')"
  expires_at="$(python3 -c 'import datetime,sys; print((datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=int(sys.argv[1]))).isoformat().replace("+00:00","Z"))' "$(jq -r '.exclusionTTLSeconds' <<<"${pin}")")"
  payload="$(jq -n \
    --argjson pin "${effective_pin}" \
    --arg expires_at "${expires_at}" \
    --arg expected_fence "${expected_fence}" \
    --argjson expected_generation "${expected_generation}" \
    '{edge_group_id:$pin.edgeGroupId, excluded_edge_ids:$pin.drainedEdgeIds,
      excluded_edge_group_ids:$pin.drainedEdgeGroupIds,
      exclusion_reason:$pin.exclusionReason, exclusion_expires_at:$expires_at,
      min_healthy_edge_nodes:$pin.minHealthyEdgeNodes, route_policy:$pin.routePolicy,
      expected_exclusion_generation:$expected_generation, expected_exclusion_fence:$expected_fence}')"
  result_file="${tmpdir}/result.json"
  status="$(curl --silent --show-error --output "${result_file}" --write-out '%{http_code}' \
    --request PUT --header "Authorization: Bearer ${api_key}" --header 'Content-Type: application/json' \
    --data "${payload}" "${policy_url}")"
  [[ "${status}" == "200" ]] || fail "apply policy for ${hostname} returned HTTP ${status}"
  jq -e --argjson pin "${effective_pin}" '
    .policy.hostname == $pin.hostname and .policy.edge_group_id == $pin.edgeGroupId and
    .policy.route_policy == $pin.routePolicy and
    ((.policy.excluded_edge_ids // []) | sort) == ($pin.drainedEdgeIds | sort) and
    ((.policy.excluded_edge_group_ids // []) | sort) == ($pin.drainedEdgeGroupIds | sort)
  ' "${result_file}" >/dev/null || fail "server response did not preserve the exact safety pin for ${hostname}"
  printf 'hostname=%s edge_group=%s drained_groups=%s applied=true\n' \
    "${hostname}" "$(jq -r '.edgeGroupId' <<<"${effective_pin}")" "$(jq -r '.drainedEdgeGroupIds|join(",")' <<<"${effective_pin}")"
done < <(jq -r '.pins[] | @base64' "${config_path}")
