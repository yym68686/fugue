#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  render_fugue_mesh_recovery_systemd_unit.sh --output-dir <dir> \
    --generation <id> \
    --login-server <url>

Renders fugue-mesh-recovery.service and fugue-mesh-recovery.env.
Secrets are intentionally not written to the public env file; put
FUGUE_MESH_RECOVERY_TOKEN, FUGUE_MESH_RECOVERY_SIGNING_KEY, and optionally
FUGUE_MESH_RECOVERY_REJOIN_AUTH_KEY in the secret env file.
EOF
}

fail() {
  printf 'render_fugue_mesh_recovery_systemd_unit.sh: %s\n' "$*" >&2
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
listen_addr="${FUGUE_MESH_RECOVERY_LISTEN_ADDR:-127.0.0.1:7840}"
state_path="${FUGUE_MESH_RECOVERY_STATE_PATH:-/var/lib/fugue/mesh-recovery/state.json}"
seed_path="${FUGUE_MESH_RECOVERY_SEED_PATH:-}"
tls_cert_file="${FUGUE_MESH_RECOVERY_TLS_CERT_FILE:-}"
tls_key_file="${FUGUE_MESH_RECOVERY_TLS_KEY_FILE:-}"
generation="${FUGUE_MESH_RECOVERY_GENERATION:-}"
previous_generation="${FUGUE_MESH_RECOVERY_PREVIOUS_GENERATION:-}"
mode="${FUGUE_MESH_RECOVERY_MODE:-normal}"
login_server="${FUGUE_MESH_RECOVERY_LOGIN_SERVER:-}"
message="${FUGUE_MESH_RECOVERY_MESSAGE:-}"
signing_key_id="${FUGUE_MESH_RECOVERY_SIGNING_KEY_ID:-mesh-recovery}"
secret_env_file="${FUGUE_MESH_RECOVERY_SECRET_ENV_FILE:-/etc/fugue/fugue-mesh-recovery-secret.env}"
binary_path="${FUGUE_MESH_RECOVERY_BINARY:-/usr/local/bin/fugue-mesh-recovery}"
directory_valid_for="${FUGUE_MESH_RECOVERY_DIRECTORY_VALID_FOR:-2m}"
manifest_valid_for="${FUGUE_MESH_RECOVERY_MANIFEST_VALID_FOR:-2m}"
node_ttl="${FUGUE_MESH_RECOVERY_NODE_TTL:-2m}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir) require_value "$1" "${2:-}"; output_dir="$2"; shift 2 ;;
    --listen-addr) require_value "$1" "${2:-}"; listen_addr="$2"; shift 2 ;;
    --state-path) require_value "$1" "${2:-}"; state_path="$2"; shift 2 ;;
    --seed-path) require_value "$1" "${2:-}"; seed_path="$2"; shift 2 ;;
    --tls-cert-file) require_value "$1" "${2:-}"; tls_cert_file="$2"; shift 2 ;;
    --tls-key-file) require_value "$1" "${2:-}"; tls_key_file="$2"; shift 2 ;;
    --generation) require_value "$1" "${2:-}"; generation="$2"; shift 2 ;;
    --previous-generation) require_value "$1" "${2:-}"; previous_generation="$2"; shift 2 ;;
    --mode) require_value "$1" "${2:-}"; mode="$2"; shift 2 ;;
    --login-server) require_value "$1" "${2:-}"; login_server="$2"; shift 2 ;;
    --message) require_value "$1" "${2:-}"; message="$2"; shift 2 ;;
    --signing-key-id) require_value "$1" "${2:-}"; signing_key_id="$2"; shift 2 ;;
    --secret-env-file) require_value "$1" "${2:-}"; secret_env_file="$2"; shift 2 ;;
    --binary-path) require_value "$1" "${2:-}"; binary_path="$2"; shift 2 ;;
    --directory-valid-for) require_value "$1" "${2:-}"; directory_valid_for="$2"; shift 2 ;;
    --manifest-valid-for) require_value "$1" "${2:-}"; manifest_valid_for="$2"; shift 2 ;;
    --node-ttl) require_value "$1" "${2:-}"; node_ttl="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) fail "unknown argument: $1" ;;
  esac
done

[[ -n "${output_dir}" ]] || fail "--output-dir is required"
[[ -n "${generation}" ]] || fail "--generation is required"
[[ -n "${login_server}" ]] || fail "--login-server is required"
if [[ -n "${tls_cert_file}${tls_key_file}" ]]; then
  [[ -n "${tls_cert_file}" && -n "${tls_key_file}" ]] || fail "--tls-cert-file and --tls-key-file must be set together"
fi
case "${mode}" in
  normal|reset) ;;
  *) fail "--mode must be normal or reset" ;;
esac
for pair in \
  "--listen-addr=${listen_addr}" \
  "--state-path=${state_path}" \
  "--seed-path=${seed_path}" \
  "--tls-cert-file=${tls_cert_file}" \
  "--tls-key-file=${tls_key_file}" \
  "--generation=${generation}" \
  "--previous-generation=${previous_generation}" \
  "--login-server=${login_server}" \
  "--signing-key-id=${signing_key_id}" \
  "--secret-env-file=${secret_env_file}" \
  "--binary-path=${binary_path}" \
  "--directory-valid-for=${directory_valid_for}" \
  "--manifest-valid-for=${manifest_valid_for}" \
  "--node-ttl=${node_ttl}"; do
  reject_whitespace "${pair%%=*}" "${pair#*=}"
done

state_dir="$(dirname "${state_path}")"
mkdir -p "${output_dir}"

cat >"${output_dir}/fugue-mesh-recovery.env" <<EOF
FUGUE_MESH_RECOVERY_LISTEN_ADDR=${listen_addr}
FUGUE_MESH_RECOVERY_STATE_PATH=${state_path}
FUGUE_MESH_RECOVERY_SEED_PATH=${seed_path}
FUGUE_MESH_RECOVERY_TLS_CERT_FILE=${tls_cert_file}
FUGUE_MESH_RECOVERY_TLS_KEY_FILE=${tls_key_file}
FUGUE_MESH_RECOVERY_GENERATION=${generation}
FUGUE_MESH_RECOVERY_PREVIOUS_GENERATION=${previous_generation}
FUGUE_MESH_RECOVERY_MODE=${mode}
FUGUE_MESH_RECOVERY_LOGIN_SERVER=${login_server}
FUGUE_MESH_RECOVERY_MESSAGE=${message}
FUGUE_MESH_RECOVERY_SIGNING_KEY_ID=${signing_key_id}
FUGUE_MESH_RECOVERY_DIRECTORY_VALID_FOR=${directory_valid_for}
FUGUE_MESH_RECOVERY_MANIFEST_VALID_FOR=${manifest_valid_for}
FUGUE_MESH_RECOVERY_NODE_TTL=${node_ttl}
EOF

cat >"${output_dir}/fugue-mesh-recovery.service" <<EOF
[Unit]
Description=Fugue mesh recovery authority
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
EnvironmentFile=${output_dir}/fugue-mesh-recovery.env
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

printf 'wrote %s\n' "${output_dir}/fugue-mesh-recovery.env"
printf 'wrote %s\n' "${output_dir}/fugue-mesh-recovery.service"
printf 'secret env file: %s\n' "${secret_env_file}"
