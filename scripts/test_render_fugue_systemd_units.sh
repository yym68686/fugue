#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

fail() {
  printf 'test_render_fugue_systemd_units.sh: %s\n' "$*" >&2
  exit 1
}

assert_contains() {
  local file="$1"
  local want="$2"
  grep -Fq -- "${want}" "${file}" || fail "expected ${file} to contain ${want}"
}

assert_not_contains() {
  local file="$1"
  local unwanted="$2"
  if grep -Fq -- "${unwanted}" "${file}"; then
    fail "expected ${file} not to contain ${unwanted}"
  fi
}

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

bash "${REPO_ROOT}/scripts/render_fugue_edge_systemd_unit.sh" \
  --output-dir "${tmpdir}" \
  --api-url "https://api.fugue.pro" \
  --edge-id "edge-us-1" \
  --edge-group-id "edge-group-country-us" \
  --region "us-east" \
  --country "US" \
  --public-ipv4 "203.0.113.10" \
  --mesh-ip "100.64.0.10" \
  --token-env-file "/etc/fugue/fugue-edge-token.env" \
  --route-cache "/var/lib/fugue/edge/routes-cache.json" \
  --caddy-config-dir "/var/lib/fugue/edge/caddy-config" \
  --caddy-cache-dir "/var/lib/fugue/edge/caddy-cache" \
  --caddy-listen-addr ":443" \
  --caddy-tls-mode "public-on-demand" >/dev/null

[[ -f "${tmpdir}/fugue-edge.env" ]] || fail "missing fugue-edge.env"
[[ -f "${tmpdir}/fugue-edge.service" ]] || fail "missing fugue-edge.service"

assert_contains "${tmpdir}/fugue-edge.env" "FUGUE_EDGE_ID=edge-us-1"
assert_contains "${tmpdir}/fugue-edge.env" "FUGUE_EDGE_GROUP_ID=edge-group-country-us"
assert_contains "${tmpdir}/fugue-edge.env" "FUGUE_EDGE_PUBLIC_IPV4=203.0.113.10"
assert_contains "${tmpdir}/fugue-edge.env" "FUGUE_EDGE_ROUTES_CACHE_PATH=/var/lib/fugue/edge/routes-cache.json"
assert_contains "${tmpdir}/fugue-edge.env" "FUGUE_EDGE_CADDY_LISTEN_ADDR=:443"
assert_contains "${tmpdir}/fugue-edge.env" "FUGUE_EDGE_CADDY_TLS_MODE=public-on-demand"
assert_contains "${tmpdir}/fugue-edge.env" "FUGUE_EDGE_CADDY_CONFIG_DIR=/var/lib/fugue/edge/caddy-config"
assert_contains "${tmpdir}/fugue-edge.env" "FUGUE_EDGE_CADDY_CACHE_DIR=/var/lib/fugue/edge/caddy-cache"
assert_not_contains "${tmpdir}/fugue-edge.env" "FUGUE_EDGE_TOKEN="

assert_contains "${tmpdir}/fugue-edge.service" "EnvironmentFile=${tmpdir}/fugue-edge.env"
assert_contains "${tmpdir}/fugue-edge.service" "EnvironmentFile=-/etc/fugue/fugue-edge-token.env"
assert_contains "${tmpdir}/fugue-edge.service" "ExecStart=/usr/local/bin/fugue-edge"
assert_contains "${tmpdir}/fugue-edge.service" "ReadWritePaths=/var/lib/fugue/edge /var/lib/fugue/edge/caddy-config /var/lib/fugue/edge/caddy-cache"

if bash "${REPO_ROOT}/scripts/render_fugue_edge_systemd_unit.sh" \
  --output-dir "${tmpdir}/bad" \
  --edge-id "edge-us-1" \
  --public-ipv4 "203.0.113.10" >"${tmpdir}/bad.out" 2>"${tmpdir}/bad.err"; then
  fail "expected missing edge group to fail"
fi

if bash "${REPO_ROOT}/scripts/render_fugue_edge_systemd_unit.sh" \
  --output-dir "${tmpdir}/bad" \
  --edge-id "edge-us-1" \
  --edge-group-id "edge-group-country-us" \
  --public-ipv4 "203.0.113.10" \
  --caddy-tls-mode "invalid" >"${tmpdir}/bad.out" 2>"${tmpdir}/bad.err"; then
  fail "expected invalid caddy tls mode to fail"
fi

printf 'render systemd unit tests passed\n'
