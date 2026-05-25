#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  render_fugue_mesh_agent_systemd_unit.sh --output-dir <dir> \
    --endpoints <url[,url...]> \
    --node-id <id>

Renders fugue-mesh-agent.service and fugue-mesh-agent.env.
Secrets are intentionally not written to the public env file; put
FUGUE_MESH_AGENT_TOKEN and FUGUE_MESH_AGENT_SIGNING_KEY in the secret env file.
Prefer an ed25519-public:<base64raw-public-key> verification key on agents.
EOF
}

fail() {
  printf 'render_fugue_mesh_agent_systemd_unit.sh: %s\n' "$*" >&2
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
endpoints="${FUGUE_MESH_AGENT_ENDPOINTS:-}"
node_id="${FUGUE_MESH_AGENT_NODE_ID:-$(hostname -s 2>/dev/null || hostname)}"
hostname_value="${FUGUE_MESH_AGENT_HOSTNAME:-$(hostname -f 2>/dev/null || hostname)}"
roles="${FUGUE_MESH_AGENT_ROLES:-}"
region="${FUGUE_MESH_AGENT_REGION:-}"
country="${FUGUE_MESH_AGENT_COUNTRY:-}"
public_ipv4="${FUGUE_MESH_AGENT_PUBLIC_IPV4:-}"
public_ipv6="${FUGUE_MESH_AGENT_PUBLIC_IPV6:-}"
private_ipv4="${FUGUE_MESH_AGENT_PRIVATE_IPV4:-}"
mesh_ip="${FUGUE_MESH_AGENT_MESH_IP:-}"
api_endpoints="${FUGUE_MESH_AGENT_API_ENDPOINTS:-}"
recovery_endpoints="${FUGUE_MESH_AGENT_RECOVERY_ENDPOINTS:-}"
edge_endpoints="${FUGUE_MESH_AGENT_EDGE_ENDPOINTS:-}"
state_path="${FUGUE_MESH_AGENT_STATE_PATH:-/var/lib/fugue/mesh-agent/state.json}"
directory_path="${FUGUE_MESH_AGENT_DIRECTORY_PATH:-/var/lib/fugue/mesh-agent/peer-directory.json}"
generation_path="${FUGUE_MESH_AGENT_GENERATION_PATH:-/var/lib/fugue/mesh-agent/generation.json}"
poll_interval="${FUGUE_MESH_AGENT_POLL_INTERVAL:-15s}"
http_timeout="${FUGUE_MESH_AGENT_HTTP_TIMEOUT:-10s}"
rejoin_enabled="${FUGUE_MESH_AGENT_REJOIN_ENABLED:-false}"
login_server="${FUGUE_MESH_AGENT_LOGIN_SERVER:-}"
signing_key_id="${FUGUE_MESH_AGENT_SIGNING_KEY_ID:-mesh-recovery}"
ca_cert_file="${FUGUE_MESH_AGENT_CA_CERT_FILE:-}"
tls_insecure_skip_verify="${FUGUE_MESH_AGENT_TLS_INSECURE_SKIP_VERIFY:-false}"
secret_env_file="${FUGUE_MESH_AGENT_SECRET_ENV_FILE:-/etc/fugue/fugue-mesh-agent-secret.env}"
binary_path="${FUGUE_MESH_AGENT_BINARY:-/usr/local/bin/fugue-mesh-agent}"
tailscale_bin="${FUGUE_MESH_AGENT_TAILSCALE_BIN:-/usr/bin/tailscale}"
tailscale_args="${FUGUE_MESH_AGENT_TAILSCALE_ARGS:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir) require_value "$1" "${2:-}"; output_dir="$2"; shift 2 ;;
    --endpoints) require_value "$1" "${2:-}"; endpoints="$2"; shift 2 ;;
    --node-id) require_value "$1" "${2:-}"; node_id="$2"; shift 2 ;;
    --hostname) require_value "$1" "${2:-}"; hostname_value="$2"; shift 2 ;;
    --roles) require_value "$1" "${2:-}"; roles="$2"; shift 2 ;;
    --region) require_value "$1" "${2:-}"; region="$2"; shift 2 ;;
    --country) require_value "$1" "${2:-}"; country="$2"; shift 2 ;;
    --public-ipv4) require_value "$1" "${2:-}"; public_ipv4="$2"; shift 2 ;;
    --public-ipv6) require_value "$1" "${2:-}"; public_ipv6="$2"; shift 2 ;;
    --private-ipv4) require_value "$1" "${2:-}"; private_ipv4="$2"; shift 2 ;;
    --mesh-ip) require_value "$1" "${2:-}"; mesh_ip="$2"; shift 2 ;;
    --api-endpoints) require_value "$1" "${2:-}"; api_endpoints="$2"; shift 2 ;;
    --recovery-endpoints) require_value "$1" "${2:-}"; recovery_endpoints="$2"; shift 2 ;;
    --edge-endpoints) require_value "$1" "${2:-}"; edge_endpoints="$2"; shift 2 ;;
    --state-path) require_value "$1" "${2:-}"; state_path="$2"; shift 2 ;;
    --directory-path) require_value "$1" "${2:-}"; directory_path="$2"; shift 2 ;;
    --generation-path) require_value "$1" "${2:-}"; generation_path="$2"; shift 2 ;;
    --poll-interval) require_value "$1" "${2:-}"; poll_interval="$2"; shift 2 ;;
    --http-timeout) require_value "$1" "${2:-}"; http_timeout="$2"; shift 2 ;;
    --rejoin-enabled) require_value "$1" "${2:-}"; rejoin_enabled="$2"; shift 2 ;;
    --login-server) require_value "$1" "${2:-}"; login_server="$2"; shift 2 ;;
    --signing-key-id) require_value "$1" "${2:-}"; signing_key_id="$2"; shift 2 ;;
    --ca-cert-file) require_value "$1" "${2:-}"; ca_cert_file="$2"; shift 2 ;;
    --tls-insecure-skip-verify) require_value "$1" "${2:-}"; tls_insecure_skip_verify="$2"; shift 2 ;;
    --secret-env-file) require_value "$1" "${2:-}"; secret_env_file="$2"; shift 2 ;;
    --binary-path) require_value "$1" "${2:-}"; binary_path="$2"; shift 2 ;;
    --tailscale-bin) require_value "$1" "${2:-}"; tailscale_bin="$2"; shift 2 ;;
    --tailscale-args) require_value "$1" "${2:-}"; tailscale_args="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) fail "unknown argument: $1" ;;
  esac
done

[[ -n "${output_dir}" ]] || fail "--output-dir is required"
[[ -n "${endpoints}" ]] || fail "--endpoints is required"
[[ -n "${node_id}" ]] || fail "--node-id is required"
case "${rejoin_enabled}" in
  true|false) ;;
  *) fail "--rejoin-enabled must be true or false" ;;
esac
case "${tls_insecure_skip_verify}" in
  true|false) ;;
  *) fail "--tls-insecure-skip-verify must be true or false" ;;
esac
for pair in \
  "--endpoints=${endpoints}" \
  "--node-id=${node_id}" \
  "--hostname=${hostname_value}" \
  "--state-path=${state_path}" \
  "--directory-path=${directory_path}" \
  "--generation-path=${generation_path}" \
  "--poll-interval=${poll_interval}" \
  "--http-timeout=${http_timeout}" \
  "--login-server=${login_server}" \
  "--signing-key-id=${signing_key_id}" \
  "--ca-cert-file=${ca_cert_file}" \
  "--secret-env-file=${secret_env_file}" \
  "--binary-path=${binary_path}" \
  "--tailscale-bin=${tailscale_bin}"; do
  reject_whitespace "${pair%%=*}" "${pair#*=}"
done

state_dir="$(dirname "${state_path}")"
mkdir -p "${output_dir}"

cat >"${output_dir}/fugue-mesh-agent.env" <<EOF
FUGUE_MESH_AGENT_ENDPOINTS=${endpoints}
FUGUE_MESH_AGENT_NODE_ID=${node_id}
FUGUE_MESH_AGENT_HOSTNAME=${hostname_value}
FUGUE_MESH_AGENT_ROLES=${roles}
FUGUE_MESH_AGENT_REGION=${region}
FUGUE_MESH_AGENT_COUNTRY=${country}
FUGUE_MESH_AGENT_PUBLIC_IPV4=${public_ipv4}
FUGUE_MESH_AGENT_PUBLIC_IPV6=${public_ipv6}
FUGUE_MESH_AGENT_PRIVATE_IPV4=${private_ipv4}
FUGUE_MESH_AGENT_MESH_IP=${mesh_ip}
FUGUE_MESH_AGENT_API_ENDPOINTS=${api_endpoints}
FUGUE_MESH_AGENT_RECOVERY_ENDPOINTS=${recovery_endpoints}
FUGUE_MESH_AGENT_EDGE_ENDPOINTS=${edge_endpoints}
FUGUE_MESH_AGENT_STATE_PATH=${state_path}
FUGUE_MESH_AGENT_DIRECTORY_PATH=${directory_path}
FUGUE_MESH_AGENT_GENERATION_PATH=${generation_path}
FUGUE_MESH_AGENT_POLL_INTERVAL=${poll_interval}
FUGUE_MESH_AGENT_HTTP_TIMEOUT=${http_timeout}
FUGUE_MESH_AGENT_REJOIN_ENABLED=${rejoin_enabled}
FUGUE_MESH_AGENT_LOGIN_SERVER=${login_server}
FUGUE_MESH_AGENT_SIGNING_KEY_ID=${signing_key_id}
FUGUE_MESH_AGENT_CA_CERT_FILE=${ca_cert_file}
FUGUE_MESH_AGENT_TLS_INSECURE_SKIP_VERIFY=${tls_insecure_skip_verify}
FUGUE_MESH_AGENT_TAILSCALE_BIN=${tailscale_bin}
FUGUE_MESH_AGENT_TAILSCALE_ARGS=${tailscale_args}
EOF

cat >"${output_dir}/fugue-mesh-agent.service" <<EOF
[Unit]
Description=Fugue mesh self-recovery agent
Wants=network-online.target tailscaled.service
After=network-online.target tailscaled.service

[Service]
Type=simple
EnvironmentFile=${output_dir}/fugue-mesh-agent.env
EnvironmentFile=-${secret_env_file}
ExecStartPre=/usr/bin/install -d -m 0755 ${state_dir}
ExecStart=${binary_path}
Restart=always
RestartSec=5s
User=root
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=true
PrivateTmp=true
ReadWritePaths=${state_dir}

[Install]
WantedBy=multi-user.target
EOF

printf 'wrote %s\n' "${output_dir}/fugue-mesh-agent.env"
printf 'wrote %s\n' "${output_dir}/fugue-mesh-agent.service"
printf 'secret env file: %s\n' "${secret_env_file}"
