#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<'EOF'
Usage:
  scripts/probe_fugue_public_dns.sh <hostname>

Validates a traffic hostname through public DNS. The check requires:
  1. control-plane answer-check evidence with a loaded route generation;
  2. public A/AAAA answers from every configured resolver;
  3. valid TLS and a loaded Host route on every returned IP;
  4. a successful HTTPS request using normal DNS resolution.

Set FUGUE_PUBLIC_DNS_RESOLVERS to a comma-separated resolver list.
Set FUGUE_PUBLIC_DNS_REQUIRED_IPS and FUGUE_PUBLIC_DNS_FORBIDDEN_IPS to
enforce a temporary placement pin without bypassing public DNS.
EOF
}

fail() {
  printf 'probe_fugue_public_dns.sh: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

append_unique_ip() {
  local candidate="$1"
  local existing
  for existing in "${answer_ips[@]:-}"; do
    if [[ "${existing}" == "${candidate}" ]]; then
      return
    fi
  done
  answer_ips+=("${candidate}")
}

query_public_dns() {
  local resolver="$1"
  local hostname="$2"
  local record_type="$3"

  if [[ -n "${dns_query_bin}" ]]; then
    "${dns_query_bin}" "${resolver}" "${hostname}" "${record_type}" "${dig_timeout}"
    return
  fi
  if command -v dig >/dev/null 2>&1; then
    dig +short +time="${dig_timeout}" +tries=1 @"${resolver}" "${hostname}" "${record_type}" |
      awk '
        /^[0-9]+(\.[0-9]+){3}$/ { print; next }
        /^[0-9A-Fa-f:]+$/ && /:/ { print }
      '
    return
  fi
  require_command go
  go run "${SCRIPT_DIR}/public_dns_query.go" "${resolver}" "${hostname}" "${record_type}" "${dig_timeout}"
}

array_contains() {
	local expected="$1"
	shift
	local value
	for value in "$@"; do
		if [[ "${value}" == "${expected}" ]]; then
			return 0
		fi
	done
	return 1
}

parse_ip_list() {
	local raw="$1"
	local value
	while IFS= read -r value; do
		value="$(printf '%s' "${value}" | awk '{$1=$1; print}')"
		[[ -n "${value}" ]] || continue
		printf '%s\n' "${value}"
	done < <(printf '%s' "${raw//;/$'\n'}" | tr ',' '\n')
}

probe_exact_ip() {
  local hostname="$1"
  local ip="$2"
  local output_file="$3"
  local connect_host="${ip}"
  local http_code

  if [[ "${ip}" == *:* ]]; then
    connect_host="[${ip}]"
  fi
  http_code="$(curl \
    --silent \
    --show-error \
    --connect-timeout "${connect_timeout}" \
    --max-time "${request_timeout}" \
    --connect-to "${hostname}:443:${connect_host}:443" \
    --output "${output_file}" \
    --write-out '%{http_code}' \
    "https://${hostname}/")" || fail "TLS/Host probe failed for ${hostname} at ${ip}"
  [[ "${http_code}" != "000" ]] || fail "TLS/Host probe returned no HTTP response for ${hostname} at ${ip}"
  if grep -Fqi -- 'edge route not found' "${output_file}"; then
    fail "edge route not found for ${hostname} at ${ip}"
  fi
  printf 'candidate host=%s ip=%s http_status=%s pass=true\n' "${hostname}" "${ip}" "${http_code}"
}

hostname="${1:-}"
if [[ $# -ne 1 || -z "${hostname}" ]]; then
  usage >&2
  exit 1
fi
if [[ ! "${hostname}" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$ || "${hostname}" != *.* ]]; then
  fail "hostname must be a DNS name"
fi

require_command curl
require_command jq

fugue_bin="${FUGUE_BIN:-fugue}"
require_command "${fugue_bin}"

resolver_list="${FUGUE_PUBLIC_DNS_RESOLVERS:-1.1.1.1,8.8.8.8}"
resolver_list="${resolver_list//;/$'\n'}"
resolver_list="${resolver_list//,/$'\n'}"
resolvers=()
while IFS= read -r resolver; do
  resolver="$(printf '%s' "${resolver}" | awk '{$1=$1; print}')"
  if [[ -n "${resolver}" ]]; then
    resolvers+=("${resolver}")
  fi
done < <(printf '%s\n' "${resolver_list}")
(( ${#resolvers[@]} > 0 )) || fail "no public DNS resolvers configured"

connect_timeout="${FUGUE_PUBLIC_DNS_CONNECT_TIMEOUT_SECONDS:-5}"
request_timeout="${FUGUE_PUBLIC_DNS_REQUEST_TIMEOUT_SECONDS:-15}"
dig_timeout="${FUGUE_PUBLIC_DNS_QUERY_TIMEOUT_SECONDS:-3}"
dns_query_bin="${FUGUE_PUBLIC_DNS_QUERY_BIN:-}"
if [[ -n "${dns_query_bin}" && ! -x "${dns_query_bin}" ]]; then
  fail "public DNS query binary is not executable: ${dns_query_bin}"
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

answer_report="${tmpdir}/answer-check.json"
"${fugue_bin}" --json admin dns answer-check "${hostname}" >"${answer_report}"
jq -e '.pass == true' "${answer_report}" >/dev/null || fail "control-plane answer-check did not pass for ${hostname}"
jq -e '
  ([.route_explain.route.route_generation?, .route_explain.routes[]?.route_generation?]
    | map(select(type == "string" and length > 0))
    | unique
    | length) > 0
' "${answer_report}" >/dev/null || fail "no loaded route generation was reported for ${hostname}"
jq -e '(.route_ready_edge_groups // []) | length > 0' "${answer_report}" >/dev/null || fail "no route-ready edge group was reported for ${hostname}"

printf 'control_plane hostname=%s route_generations=%s route_ready_groups=%s pass=true\n' \
  "${hostname}" \
  "$(jq -r '[.route_explain.route.route_generation?, .route_explain.routes[]?.route_generation?] | map(select(type == "string" and length > 0)) | unique | join(",")' "${answer_report}")" \
  "$(jq -r '(.route_ready_edge_groups // []) | join(",")' "${answer_report}")"

answer_ips=()
for resolver in "${resolvers[@]}"; do
  resolver_answers=()
  for record_type in A AAAA; do
    while IFS= read -r ip; do
      [[ -n "${ip}" ]] || continue
      resolver_answers+=("${ip}")
      append_unique_ip "${ip}"
    done < <(query_public_dns "${resolver}" "${hostname}" "${record_type}")
  done
  (( ${#resolver_answers[@]} > 0 )) || fail "public resolver ${resolver} returned no A/AAAA answer for ${hostname}"
  printf 'public_dns resolver=%s hostname=%s answers=%s pass=true\n' \
    "${resolver}" "${hostname}" "$(IFS=,; printf '%s' "${resolver_answers[*]}")"
done
(( ${#answer_ips[@]} > 0 )) || fail "public DNS returned no candidate IP for ${hostname}"

required_ips=()
while IFS= read -r ip; do required_ips+=("${ip}"); done < <(parse_ip_list "${FUGUE_PUBLIC_DNS_REQUIRED_IPS:-}")
for ip in "${required_ips[@]:-}"; do
	[[ -n "${ip}" ]] || continue
	array_contains "${ip}" "${answer_ips[@]}" || fail "required public DNS answer ${ip} is absent for ${hostname}"
done
forbidden_ips=()
while IFS= read -r ip; do forbidden_ips+=("${ip}"); done < <(parse_ip_list "${FUGUE_PUBLIC_DNS_FORBIDDEN_IPS:-}")
for ip in "${forbidden_ips[@]:-}"; do
	[[ -n "${ip}" ]] || continue
	if array_contains "${ip}" "${answer_ips[@]}"; then
		fail "forbidden public DNS answer ${ip} is present for ${hostname}"
	fi
done

probe_index=0
for ip in "${answer_ips[@]}"; do
  probe_index=$((probe_index + 1))
  probe_exact_ip "${hostname}" "${ip}" "${tmpdir}/candidate-${probe_index}.body"
done

public_body="${tmpdir}/public-path.body"
public_status="$(curl \
  --silent \
  --show-error \
  --connect-timeout "${connect_timeout}" \
  --max-time "${request_timeout}" \
  --output "${public_body}" \
  --write-out '%{http_code}' \
  "https://${hostname}/")" || fail "public DNS HTTPS request failed for ${hostname}"
[[ "${public_status}" != "000" ]] || fail "public DNS HTTPS request returned no HTTP response for ${hostname}"
if grep -Fqi -- 'edge route not found' "${public_body}"; then
  fail "public DNS path returned edge route not found for ${hostname}"
fi

printf 'public_path hostname=%s http_status=%s pass=true\n' "${hostname}" "${public_status}"
