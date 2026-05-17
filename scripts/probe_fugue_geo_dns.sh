#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/probe_fugue_geo_dns.sh <hostname> <client-ip> [<client-ip>...]

The script runs `fugue admin dns answer-check` once per client IP and prints
the observed answer order and route-ready validation for each probe.
EOF
}

hostname="${1:-}"
shift || true

if [[ -z "${hostname}" ]]; then
  usage >&2
  exit 1
fi

client_ips=("$@")
if (( ${#client_ips[@]} == 0 )); then
  raw="${FUGUE_GEO_DNS_CLIENT_IPS:-}"
  raw="${raw//;/$'\n'}"
  raw="${raw//,/$'\n'}"
  while IFS= read -r value; do
    value="$(printf '%s' "${value}" | awk '{$1=$1; print}')"
    if [[ -n "${value}" ]]; then
      client_ips+=("${value}")
    fi
  done < <(printf '%s\n' "${raw}")
fi

if (( ${#client_ips[@]} == 0 )); then
  usage >&2
  exit 1
fi

for client_ip in "${client_ips[@]}"; do
  printf '\n[client_ip=%s]\n' "${client_ip}"
  fugue admin dns answer-check "${hostname}" --client-ip "${client_ip}"
done
