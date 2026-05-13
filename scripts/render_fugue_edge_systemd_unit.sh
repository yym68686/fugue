#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  render_fugue_edge_systemd_unit.sh --output-dir <dir> \
    --edge-id <id> \
    --edge-group-id <edge-group> \
    --public-ipv4 <ip>

Renders fugue-edge.service and fugue-edge.env for a systemd escape hatch.
The edge token is intentionally not written to fugue-edge.env; put
FUGUE_EDGE_TOKEN in the secret env file passed with --token-env-file.

Options:
  --output-dir <dir>              Directory to write fugue-edge.service/env.
  --api-url <url>                 Fugue API URL. Default: FUGUE_API_URL or https://api.fugue.pro.
  --edge-id <id>                  Stable edge node ID. Default: FUGUE_EDGE_ID or hostname.
  --edge-group-id <id>            Edge group ID. Required.
  --region <region>               Edge region metadata.
  --country <country>             Edge country metadata.
  --public-hostname <host>        Public hostname reported in heartbeat.
  --public-ipv4 <ip>              Public IPv4 reported in heartbeat.
  --public-ipv6 <ip>              Public IPv6 reported in heartbeat.
  --mesh-ip <ip>                  Mesh/private IP reported in heartbeat.
  --token-env-file <path>         Secret env file containing FUGUE_EDGE_TOKEN. Default: /etc/fugue/fugue-edge-token.env.
  --binary-path <path>            fugue-edge binary path. Default: /usr/local/bin/fugue-edge.
  --route-cache <path>            Route cache JSON path. Default: /var/lib/fugue/edge/routes-cache.json.
  --caddy-config-dir <path>       Local Caddy config dir. Default: /var/lib/fugue/edge/caddy-config.
  --caddy-cache-dir <path>        Local Caddy cache/data dir. Default: /var/lib/fugue/edge/caddy-cache.
  --listen-addr <addr>            fugue-edge health/metrics listen addr. Default: 127.0.0.1:7832.
  --proxy-listen-addr <addr>      fugue-edge direct proxy listen addr. Default: 127.0.0.1:7833.
  --caddy-admin-url <url>         Caddy admin API URL. Default: http://127.0.0.1:2019.
  --caddy-listen-addr <addr>      Caddy public listen addr. Default: :443.
  --caddy-tls-mode <mode>         Caddy TLS mode: off, internal, public-on-demand. Default: public-on-demand.
  --caddy-tls-ask-url <url>       Caddy on-demand TLS ask URL.
  --caddy-static-tls-cert-file <path>
                                  Static certificate path loaded by Caddy JSON config.
  --caddy-static-tls-key-file <path>
                                  Static private key path loaded by Caddy JSON config.
  --caddy-enabled <true|false>    Whether fugue-edge manages Caddy config. Default: true.
  --sync-interval <duration>      Bundle sync interval. Default: 15s.
  --heartbeat-interval <duration> Heartbeat interval. Default: 30s.
  --http-timeout <duration>       HTTP timeout. Default: 10s.
  -h, --help                      Show this help.
EOF
}

fail() {
  printf 'render_fugue_edge_systemd_unit.sh: %s\n' "$*" >&2
  exit 1
}

require_value() {
  local flag="$1"
  local value="${2:-}"
  [[ -n "${value}" ]] || fail "${flag} requires a non-empty value"
}

reject_whitespace() {
  local flag="$1"
  local value="$2"
  [[ "${value}" != *[$' \t\n\r']* ]] || fail "${flag} must not contain whitespace"
}

output_dir=""
api_url="${FUGUE_API_URL:-https://api.fugue.pro}"
edge_id="${FUGUE_EDGE_ID:-$(hostname -s 2>/dev/null || hostname)}"
edge_group_id="${FUGUE_EDGE_GROUP_ID:-}"
region="${FUGUE_EDGE_REGION:-}"
country="${FUGUE_EDGE_COUNTRY:-}"
public_hostname="${FUGUE_EDGE_PUBLIC_HOSTNAME:-}"
public_ipv4="${FUGUE_EDGE_PUBLIC_IPV4:-}"
public_ipv6="${FUGUE_EDGE_PUBLIC_IPV6:-}"
mesh_ip="${FUGUE_EDGE_MESH_IP:-}"
token_env_file="${FUGUE_EDGE_TOKEN_ENV_FILE:-/etc/fugue/fugue-edge-token.env}"
binary_path="${FUGUE_EDGE_BINARY:-/usr/local/bin/fugue-edge}"
route_cache="${FUGUE_EDGE_ROUTES_CACHE_PATH:-/var/lib/fugue/edge/routes-cache.json}"
caddy_config_dir="${FUGUE_EDGE_CADDY_CONFIG_DIR:-/var/lib/fugue/edge/caddy-config}"
caddy_cache_dir="${FUGUE_EDGE_CADDY_CACHE_DIR:-/var/lib/fugue/edge/caddy-cache}"
listen_addr="${FUGUE_EDGE_LISTEN_ADDR:-127.0.0.1:7832}"
proxy_listen_addr="${FUGUE_EDGE_PROXY_LISTEN_ADDR:-127.0.0.1:7833}"
caddy_admin_url="${FUGUE_EDGE_CADDY_ADMIN_URL:-http://127.0.0.1:2019}"
caddy_listen_addr="${FUGUE_EDGE_CADDY_LISTEN_ADDR:-:443}"
caddy_tls_mode="${FUGUE_EDGE_CADDY_TLS_MODE:-public-on-demand}"
caddy_tls_ask_url="${FUGUE_EDGE_CADDY_TLS_ASK_URL:-}"
caddy_static_tls_cert_file="${FUGUE_EDGE_CADDY_STATIC_TLS_CERT_FILE:-}"
caddy_static_tls_key_file="${FUGUE_EDGE_CADDY_STATIC_TLS_KEY_FILE:-}"
caddy_enabled="${FUGUE_EDGE_CADDY_ENABLED:-true}"
sync_interval="${FUGUE_EDGE_SYNC_INTERVAL:-15s}"
heartbeat_interval="${FUGUE_EDGE_HEARTBEAT_INTERVAL:-30s}"
http_timeout="${FUGUE_EDGE_HTTP_TIMEOUT:-10s}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir) require_value "$1" "${2:-}"; output_dir="$2"; shift 2 ;;
    --api-url) require_value "$1" "${2:-}"; api_url="$2"; shift 2 ;;
    --edge-id) require_value "$1" "${2:-}"; edge_id="$2"; shift 2 ;;
    --edge-group-id) require_value "$1" "${2:-}"; edge_group_id="$2"; shift 2 ;;
    --region) require_value "$1" "${2:-}"; region="$2"; shift 2 ;;
    --country) require_value "$1" "${2:-}"; country="$2"; shift 2 ;;
    --public-hostname) require_value "$1" "${2:-}"; public_hostname="$2"; shift 2 ;;
    --public-ipv4) require_value "$1" "${2:-}"; public_ipv4="$2"; shift 2 ;;
    --public-ipv6) require_value "$1" "${2:-}"; public_ipv6="$2"; shift 2 ;;
    --mesh-ip) require_value "$1" "${2:-}"; mesh_ip="$2"; shift 2 ;;
    --token-env-file) require_value "$1" "${2:-}"; token_env_file="$2"; shift 2 ;;
    --binary-path) require_value "$1" "${2:-}"; binary_path="$2"; shift 2 ;;
    --route-cache) require_value "$1" "${2:-}"; route_cache="$2"; shift 2 ;;
    --caddy-config-dir) require_value "$1" "${2:-}"; caddy_config_dir="$2"; shift 2 ;;
    --caddy-cache-dir) require_value "$1" "${2:-}"; caddy_cache_dir="$2"; shift 2 ;;
    --listen-addr) require_value "$1" "${2:-}"; listen_addr="$2"; shift 2 ;;
    --proxy-listen-addr) require_value "$1" "${2:-}"; proxy_listen_addr="$2"; shift 2 ;;
    --caddy-admin-url) require_value "$1" "${2:-}"; caddy_admin_url="$2"; shift 2 ;;
    --caddy-listen-addr) require_value "$1" "${2:-}"; caddy_listen_addr="$2"; shift 2 ;;
    --caddy-tls-mode) require_value "$1" "${2:-}"; caddy_tls_mode="$2"; shift 2 ;;
    --caddy-tls-ask-url) require_value "$1" "${2:-}"; caddy_tls_ask_url="$2"; shift 2 ;;
    --caddy-static-tls-cert-file) require_value "$1" "${2:-}"; caddy_static_tls_cert_file="$2"; shift 2 ;;
    --caddy-static-tls-key-file) require_value "$1" "${2:-}"; caddy_static_tls_key_file="$2"; shift 2 ;;
    --caddy-enabled) require_value "$1" "${2:-}"; caddy_enabled="$2"; shift 2 ;;
    --sync-interval) require_value "$1" "${2:-}"; sync_interval="$2"; shift 2 ;;
    --heartbeat-interval) require_value "$1" "${2:-}"; heartbeat_interval="$2"; shift 2 ;;
    --http-timeout) require_value "$1" "${2:-}"; http_timeout="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) fail "unknown argument: $1" ;;
  esac
done

[[ -n "${output_dir}" ]] || fail "--output-dir is required"
[[ -n "${edge_id}" ]] || fail "--edge-id is required"
[[ -n "${edge_group_id}" ]] || fail "--edge-group-id is required"
[[ -n "${public_ipv4}${public_ipv6}" ]] || fail "at least one of --public-ipv4 or --public-ipv6 is required"

case "${caddy_enabled}" in
  true|false) ;;
  *) fail "--caddy-enabled must be true or false" ;;
esac

case "${caddy_tls_mode}" in
  off|internal|public-on-demand) ;;
  *) fail "--caddy-tls-mode must be one of: off, internal, public-on-demand" ;;
esac
if [[ -n "${caddy_static_tls_cert_file}${caddy_static_tls_key_file}" ]]; then
  [[ -n "${caddy_static_tls_cert_file}" && -n "${caddy_static_tls_key_file}" ]] || fail "--caddy-static-tls-cert-file and --caddy-static-tls-key-file must be set together"
  [[ "${caddy_enabled}" == "true" ]] || fail "--caddy-enabled must be true when static TLS files are configured"
  [[ "${caddy_tls_mode}" != "off" ]] || fail "--caddy-tls-mode must not be off when static TLS files are configured"
fi

for pair in \
  "--api-url=${api_url}" \
  "--edge-id=${edge_id}" \
  "--edge-group-id=${edge_group_id}" \
  "--token-env-file=${token_env_file}" \
  "--binary-path=${binary_path}" \
  "--route-cache=${route_cache}" \
  "--caddy-config-dir=${caddy_config_dir}" \
  "--caddy-cache-dir=${caddy_cache_dir}" \
  "--listen-addr=${listen_addr}" \
  "--proxy-listen-addr=${proxy_listen_addr}" \
  "--caddy-admin-url=${caddy_admin_url}" \
  "--caddy-listen-addr=${caddy_listen_addr}" \
  "--caddy-static-tls-cert-file=${caddy_static_tls_cert_file}" \
  "--caddy-static-tls-key-file=${caddy_static_tls_key_file}" \
  "--sync-interval=${sync_interval}" \
  "--heartbeat-interval=${heartbeat_interval}" \
  "--http-timeout=${http_timeout}"; do
  reject_whitespace "${pair%%=*}" "${pair#*=}"
done

route_cache_dir="$(dirname "${route_cache}")"
mkdir -p "${output_dir}"

cat >"${output_dir}/fugue-edge.env" <<EOF
FUGUE_API_URL=${api_url}
FUGUE_EDGE_ID=${edge_id}
FUGUE_EDGE_GROUP_ID=${edge_group_id}
FUGUE_EDGE_REGION=${region}
FUGUE_EDGE_COUNTRY=${country}
FUGUE_EDGE_PUBLIC_HOSTNAME=${public_hostname}
FUGUE_EDGE_PUBLIC_IPV4=${public_ipv4}
FUGUE_EDGE_PUBLIC_IPV6=${public_ipv6}
FUGUE_EDGE_MESH_IP=${mesh_ip}
FUGUE_EDGE_ROUTES_CACHE_PATH=${route_cache}
FUGUE_EDGE_LISTEN_ADDR=${listen_addr}
FUGUE_EDGE_SYNC_INTERVAL=${sync_interval}
FUGUE_EDGE_HEARTBEAT_INTERVAL=${heartbeat_interval}
FUGUE_EDGE_HTTP_TIMEOUT=${http_timeout}
FUGUE_EDGE_CADDY_ENABLED=${caddy_enabled}
FUGUE_EDGE_CADDY_ADMIN_URL=${caddy_admin_url}
FUGUE_EDGE_CADDY_LISTEN_ADDR=${caddy_listen_addr}
FUGUE_EDGE_CADDY_TLS_MODE=${caddy_tls_mode}
FUGUE_EDGE_CADDY_TLS_ASK_URL=${caddy_tls_ask_url}
FUGUE_EDGE_PROXY_LISTEN_ADDR=${proxy_listen_addr}
FUGUE_EDGE_CADDY_STATIC_TLS_CERT_FILE=${caddy_static_tls_cert_file}
FUGUE_EDGE_CADDY_STATIC_TLS_KEY_FILE=${caddy_static_tls_key_file}
FUGUE_EDGE_CADDY_CONFIG_DIR=${caddy_config_dir}
FUGUE_EDGE_CADDY_CACHE_DIR=${caddy_cache_dir}
EOF

cat >"${output_dir}/fugue-edge.service" <<EOF
[Unit]
Description=Fugue regional edge escape hatch
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
EnvironmentFile=${output_dir}/fugue-edge.env
EnvironmentFile=-${token_env_file}
ExecStartPre=/usr/bin/install -d -m 0755 ${route_cache_dir} ${caddy_config_dir} ${caddy_cache_dir}
ExecStart=${binary_path}
Restart=always
RestartSec=5s
User=root
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=true
PrivateTmp=true
ReadWritePaths=${route_cache_dir} ${caddy_config_dir} ${caddy_cache_dir}

[Install]
WantedBy=multi-user.target
EOF

printf 'wrote %s\n' "${output_dir}/fugue-edge.env"
printf 'wrote %s\n' "${output_dir}/fugue-edge.service"
printf 'secret token env file: %s (must contain FUGUE_EDGE_TOKEN)\n' "${token_env_file}"
