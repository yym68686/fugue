#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: export_cloudflare_zone_static_records.sh [options]

Exports public Cloudflare DNS records into Fugue protected static DNS record JSON.

Options:
  --zone ZONE              Cloudflare zone name. Default: fugue.pro
  --env-file PATH          Optional .env file containing CLOUDFLARE_DNS_API_TOKEN.
  --output PATH            Output file. Default: stdout
  --nameserver HOST=IP     Inject root NS HOST and HOST A/AAAA glue record. Repeatable.
  --keep-apex-cname        Do not flatten a Cloudflare apex CNAME into A/AAAA.
  -h, --help               Show this help.

The token is read from CLOUDFLARE_DNS_API_TOKEN or CLOUDFLARE_API_TOKEN and is
never printed.
EOF
}

fail() {
  printf '[cloudflare-export] ERROR: %s\n' "$*" >&2
  exit 1
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

trim() {
  printf '%s' "$1" | awk '{$1=$1; print}'
}

load_env_file_token() {
  local env_file="$1"
  local line key value
  [[ -r "${env_file}" ]] || fail "env file is not readable: ${env_file}"
  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="$(trim "${line}")"
    [[ -z "${line}" || "${line}" == \#* ]] && continue
    line="${line#export }"
    key="${line%%=*}"
    value="${line#*=}"
    key="$(trim "${key}")"
    value="$(trim "${value}")"
    value="${value%\"}"
    value="${value#\"}"
    value="${value%\'}"
    value="${value#\'}"
    case "${key}" in
      CLOUDFLARE_DNS_API_TOKEN)
        export CLOUDFLARE_DNS_API_TOKEN="${value}"
        ;;
      CLOUDFLARE_API_TOKEN)
        export CLOUDFLARE_API_TOKEN="${value}"
        ;;
    esac
  done <"${env_file}"
}

append_injected_nameserver() {
  local file="$1"
  local zone="$2"
  local spec="$3"
  local host ip record_type tmp

  host="$(trim "${spec%%=*}")"
  ip="$(trim "${spec#*=}")"
  [[ -n "${host}" && "${host}" != "${spec}" ]] || fail "--nameserver must be HOST=IP"
  [[ -n "${ip}" ]] || fail "--nameserver must include an IP address"
  host="${host%.}"
  if [[ "${ip}" == *:* ]]; then
    record_type="AAAA"
  else
    record_type="A"
  fi

  tmp="${file}.tmp"
  jq \
    --arg zone "${zone}" \
    --arg host "${host}" \
    --arg ip "${ip}" \
    --arg record_type "${record_type}" \
    '. + [
      {
        "name": $zone,
        "type": "NS",
        "values": [$host],
        "ttl": 300,
        "record_kind": "protected",
        "status": "active"
      },
      {
        "name": $host,
        "type": $record_type,
        "values": [$ip],
        "ttl": 300,
        "record_kind": "protected",
        "status": "active"
      }
    ]' "${file}" >"${tmp}"
  mv "${tmp}" "${file}"
}

append_protected_record_value() {
  local file="$1"
  local name="$2"
  local record_type="$3"
  local value="$4"
  local ttl="${5:-300}"
  local tmp="${file}.tmp"

  jq \
    --arg name "${name%.}" \
    --arg record_type "${record_type}" \
    --arg value "${value%.}" \
    --argjson ttl "${ttl}" \
    '. + [{
      "name": $name,
      "type": $record_type,
      "values": [$value],
      "ttl": $ttl,
      "record_kind": "protected",
      "status": "active"
    }]' "${file}" >"${tmp}"
  mv "${tmp}" "${file}"
}

flatten_apex_cname_records() {
	local file="$1"
	local zone="$2"
	local resolver="$3"
	local target ip tmp
	local -a cname_targets=()

	while IFS= read -r target; do
		target="$(trim "${target}")"
		[[ -n "${target}" ]] && cname_targets+=("${target}")
	done < <(jq -r --arg zone "${zone}" '.[] | select(.name == $zone and .type == "CNAME") | .values[]' "${file}")
  if (( ${#cname_targets[@]} == 0 )); then
    return 0
  fi
  command_exists dig || fail "dig is required to flatten apex CNAME records"
  tmp="${file}.tmp"
  jq --arg zone "${zone}" 'map(select(.name != $zone or .type != "CNAME"))' "${file}" >"${tmp}"
  mv "${tmp}" "${file}"
	for target in "${cname_targets[@]}"; do
		target="${target%.}"
		while IFS= read -r ip; do
			ip="$(trim "${ip}")"
			[[ -n "${ip}" ]] || continue
			[[ "${ip}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || continue
			append_protected_record_value "${file}" "${zone}" "A" "${ip}" 300
		done < <(dig "@${resolver}" +short "${target}" A)
		while IFS= read -r ip; do
			ip="$(trim "${ip}")"
			[[ -n "${ip}" ]] || continue
			[[ "${ip}" == *:* ]] || continue
			append_protected_record_value "${file}" "${zone}" "AAAA" "${ip}" 300
		done < <(dig "@${resolver}" +short "${target}" AAAA)
	done
}

zone="fugue.pro"
env_file=""
output=""
nameservers=()
keep_apex_cname="false"
resolver="${FUGUE_DNS_EXPORT_RESOLVER:-1.1.1.1}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --zone)
      [[ $# -ge 2 ]] || fail "--zone requires a value"
      zone="$2"
      shift 2
      ;;
    --env-file)
      [[ $# -ge 2 ]] || fail "--env-file requires a value"
      env_file="$2"
      shift 2
      ;;
    --output)
      [[ $# -ge 2 ]] || fail "--output requires a value"
      output="$2"
      shift 2
      ;;
    --nameserver)
      [[ $# -ge 2 ]] || fail "--nameserver requires HOST=IP"
      nameservers+=("$2")
      shift 2
      ;;
    --keep-apex-cname)
      keep_apex_cname="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "unknown option: $1"
      ;;
  esac
done

command_exists curl || fail "curl is required"
command_exists jq || fail "jq is required"

if [[ -n "${env_file}" ]]; then
  load_env_file_token "${env_file}"
fi

token="${CLOUDFLARE_DNS_API_TOKEN:-${CLOUDFLARE_API_TOKEN:-}}"
[[ -n "${token}" ]] || fail "CLOUDFLARE_DNS_API_TOKEN or CLOUDFLARE_API_TOKEN is required"

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

zone_response="${tmpdir}/zone.json"
curl -fsS -G \
  -H "Authorization: Bearer ${token}" \
  -H "Content-Type: application/json" \
  --data-urlencode "name=${zone}" \
  "https://api.cloudflare.com/client/v4/zones" >"${zone_response}"

zone_id="$(jq -r '.result[0].id // empty' "${zone_response}")"
[[ -n "${zone_id}" ]] || fail "Cloudflare zone not found: ${zone}"

page=1
total_pages=1
while (( page <= total_pages )); do
  page_file="${tmpdir}/records-${page}.json"
  curl -fsS -G \
    -H "Authorization: Bearer ${token}" \
    -H "Content-Type: application/json" \
    --data-urlencode "per_page=100" \
    --data-urlencode "page=${page}" \
    "https://api.cloudflare.com/client/v4/zones/${zone_id}/dns_records" >"${page_file}"
  if [[ "$(jq -r '.success' "${page_file}")" != "true" ]]; then
    fail "Cloudflare DNS record export failed"
  fi
  total_pages="$(jq -r '.result_info.total_pages // 1' "${page_file}")"
  page=$((page + 1))
done

records_file="${tmpdir}/records.json"
jq -s '
  [.[].result[]]
  | map(select(.type as $type | ["A", "AAAA", "CAA", "CNAME", "MX", "NS", "TXT"] | index($type)))
  | map({
      name: (.name | ascii_downcase | sub("\\.$"; "")),
      type: .type,
      value: (if .type == "MX" then (((.priority // 10) | tostring) + " " + .content) else .content end),
      ttl: (if (.ttl // 1) == 1 then 300 else .ttl end)
    })
  | group_by([.name, .type, .ttl])
  | map({
      name: .[0].name,
      type: .[0].type,
      values: (map(.value) | unique),
      ttl: .[0].ttl,
      record_kind: "protected",
      status: "active"
    })
  | sort_by(.name, .type)
' "${tmpdir}"/records-*.json >"${records_file}"

for nameserver in "${nameservers[@]}"; do
  append_injected_nameserver "${records_file}" "${zone}" "${nameserver}"
done
if [[ "${keep_apex_cname}" != "true" ]]; then
  flatten_apex_cname_records "${records_file}" "${zone}" "${resolver}"
fi

jq 'group_by([.name, .type, .ttl]) | map({
  name: .[0].name,
  type: .[0].type,
  values: (map(.values[]) | unique),
  ttl: .[0].ttl,
  record_kind: "protected",
  status: "active"
}) | sort_by(.name, .type)' "${records_file}" >"${records_file}.dedup"

if [[ -n "${output}" ]]; then
  cp "${records_file}.dedup" "${output}"
else
  cat "${records_file}.dedup"
fi
