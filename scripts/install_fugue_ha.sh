#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PRIMARY_ALIAS="${FUGUE_NODE1:-gcp1}"
SECONDARY_ALIASES=("${FUGUE_NODE2:-gcp2}" "${FUGUE_NODE3:-gcp3}")
ALL_ALIASES=("${PRIMARY_ALIAS}" "${SECONDARY_ALIASES[@]}")

RELEASE_NAME="${FUGUE_RELEASE_NAME:-fugue}"
NAMESPACE="${FUGUE_NAMESPACE:-fugue-system}"
RELEASE_FULLNAME="${FUGUE_RELEASE_FULLNAME:-${RELEASE_NAME}-fugue}"
CONTROL_PLANE_AUTOMATION_SECRET_NAME="${FUGUE_CONTROL_PLANE_AUTOMATION_SECRET_NAME:-${RELEASE_FULLNAME}-control-plane-automation}"
API_DEPLOYMENT_NAME="${FUGUE_API_DEPLOYMENT_NAME:-${RELEASE_FULLNAME}-api}"
CONTROLLER_DEPLOYMENT_NAME="${FUGUE_CONTROLLER_DEPLOYMENT_NAME:-${RELEASE_FULLNAME}-controller}"
CONFIG_SECRET_NAME="${FUGUE_CONFIG_SECRET_NAME:-${RELEASE_FULLNAME}-config}"
POSTGRES_DEPLOYMENT_NAME="${FUGUE_POSTGRES_DEPLOYMENT_NAME:-${RELEASE_FULLNAME}-postgres}"
REGISTRY_DEPLOYMENT_NAME="${FUGUE_REGISTRY_DEPLOYMENT_NAME:-${RELEASE_FULLNAME}-registry}"
HEADSCALE_DEPLOYMENT_NAME="${FUGUE_HEADSCALE_DEPLOYMENT_NAME:-${RELEASE_FULLNAME}-headscale}"
REMOTE_TMP_BASE="${FUGUE_REMOTE_TMP_BASE:-/tmp/fugue-install}"
HOSTPATH_DATA_DIR="${FUGUE_HOSTPATH_DATA_DIR:-/var/lib/fugue}"
K3S_CHANNEL="${FUGUE_K3S_CHANNEL:-stable}"
API_NODEPORT="${FUGUE_API_NODEPORT:-30080}"
REGISTRY_NODEPORT="${FUGUE_REGISTRY_NODEPORT:-30500}"
HEADSCALE_NODEPORT="${FUGUE_HEADSCALE_NODEPORT:-30443}"
REGISTRY_PERSISTENCE_MODE="${FUGUE_REGISTRY_PERSISTENCE_MODE:-hostPath}"
REGISTRY_PERSISTENCE_EXISTING_CLAIM="${FUGUE_REGISTRY_PERSISTENCE_EXISTING_CLAIM:-}"
REGISTRY_PERSISTENCE_ACCESS_MODE="${FUGUE_REGISTRY_PERSISTENCE_ACCESS_MODE:-ReadWriteOnce}"
REGISTRY_PERSISTENCE_SIZE="${FUGUE_REGISTRY_PERSISTENCE_SIZE:-50Gi}"
REGISTRY_PERSISTENCE_STORAGE_CLASS="${FUGUE_REGISTRY_PERSISTENCE_STORAGE_CLASS:-}"
REGISTRY_UPLOAD_PURGE_ENABLED="${FUGUE_REGISTRY_UPLOAD_PURGE_ENABLED:-true}"
REGISTRY_UPLOAD_PURGE_AGE="${FUGUE_REGISTRY_UPLOAD_PURGE_AGE:-168h}"
REGISTRY_UPLOAD_PURGE_INTERVAL="${FUGUE_REGISTRY_UPLOAD_PURGE_INTERVAL:-24h}"
REGISTRY_UPLOAD_PURGE_DRY_RUN="${FUGUE_REGISTRY_UPLOAD_PURGE_DRY_RUN:-false}"
IMAGE_TAG="${FUGUE_IMAGE_TAG:-local-$(date +%Y%m%d%H%M%S)}"
DIST_DIR="${FUGUE_DIST_DIR:-${REPO_ROOT}/.dist/fugue-install}"
CONTROL_PLANE_HOSTS_ENV_FILE="${FUGUE_CONTROL_PLANE_HOSTS_ENV_FILE:-${DIST_DIR}/control-plane-hosts.env}"
CONTROL_PLANE_SSH_KEY_FILE="${FUGUE_CONTROL_PLANE_SSH_KEY_FILE:-${DIST_DIR}/control-plane-id_ed25519}"
CONTROL_PLANE_SSH_PUBLIC_KEY_FILE="${FUGUE_CONTROL_PLANE_SSH_PUBLIC_KEY_FILE:-${CONTROL_PLANE_SSH_KEY_FILE}.pub}"
CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE="${FUGUE_CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE:-${DIST_DIR}/control-plane-known_hosts}"
CONTROL_PLANE_AUTOMATION_ROOT_DIR="${FUGUE_CONTROL_PLANE_AUTOMATION_ROOT_DIR:-/root/.config/fugue/control-plane-automation}"
USE_CONTROL_PLANE_AUTOMATION_SSH="${FUGUE_USE_CONTROL_PLANE_AUTOMATION_SSH:-false}"
BOOTSTRAP_KEY="${FUGUE_BOOTSTRAP_KEY:-}"
K3S_API_IP="${FUGUE_K3S_API_IP:-}"
PUBLIC_ENDPOINT_HOST="${FUGUE_PUBLIC_ENDPOINT_HOST:-}"
FUGUE_DOMAIN="${FUGUE_DOMAIN:-}"
FUGUE_APP_BASE_DOMAIN="${FUGUE_APP_BASE_DOMAIN:-fugue.pro}"
FUGUE_REGISTRY_DOMAIN="${FUGUE_REGISTRY_DOMAIN:-registry.${FUGUE_APP_BASE_DOMAIN}}"
FUGUE_EDGE_TLS_ASK_TOKEN="${FUGUE_EDGE_TLS_ASK_TOKEN:-}"
FUGUE_MESH_ENABLED="${FUGUE_MESH_ENABLED:-false}"
FUGUE_MESH_PROVIDER="${FUGUE_MESH_PROVIDER:-tailscale}"
FUGUE_MESH_DOMAIN="${FUGUE_MESH_DOMAIN:-mesh.${FUGUE_APP_BASE_DOMAIN}}"
FUGUE_MESH_AUTH_KEY="${FUGUE_MESH_AUTH_KEY:-}"
FUGUE_APP_TLS_CERT_FILE="${FUGUE_APP_TLS_CERT_FILE:-${REPO_ROOT}/secrets/cloudflare-apps-origin.crt}"
FUGUE_APP_TLS_KEY_FILE="${FUGUE_APP_TLS_KEY_FILE:-${REPO_ROOT}/secrets/cloudflare-apps-origin.key}"
RECONCILE_K3S_CLUSTER="${FUGUE_RECONCILE_K3S_CLUSTER:-false}"
IMAGE_PLATFORM="${FUGUE_IMAGE_PLATFORM:-}"
CONTAINER_TOOL="${FUGUE_CONTAINER_TOOL:-}"
SSH_CONNECT_TIMEOUT="${FUGUE_SSH_CONNECT_TIMEOUT:-15}"
SSH_SERVER_ALIVE_INTERVAL="${FUGUE_SSH_SERVER_ALIVE_INTERVAL:-15}"
SSH_SERVER_ALIVE_COUNT_MAX="${FUGUE_SSH_SERVER_ALIVE_COUNT_MAX:-3}"
UPLOAD_RETRIES="${FUGUE_UPLOAD_RETRIES:-3}"
UPLOAD_RETRY_DELAY="${FUGUE_UPLOAD_RETRY_DELAY:-3}"
REMOTE_CMD_RETRIES="${FUGUE_REMOTE_CMD_RETRIES:-3}"
REMOTE_CMD_RETRY_DELAY="${FUGUE_REMOTE_CMD_RETRY_DELAY:-2}"
PUBLIC_API_REACHABLE="unknown"
EDGE_LOCAL_HEALTH="unknown"
EDGE_UPSTREAM=""
EDGE_UPSTREAM_MODE=""
REGISTRY_EDGE_UPSTREAM=""
HEADSCALE_EDGE_UPSTREAM=""

IMAGES_TAR="${DIST_DIR}/fugue-images-${IMAGE_TAG}.tar"
CHART_TAR="${DIST_DIR}/fugue-chart-${IMAGE_TAG}.tar"
VALUES_FILE="${DIST_DIR}/values-override.yaml"
KUBECONFIG_OUT="${DIST_DIR}/kubeconfig"
SUMMARY_FILE="${DIST_DIR}/install-summary.txt"
ROUTE_A_FILE="${DIST_DIR}/route-a-next-steps.txt"

SSH_OPTS=(
  -o "BatchMode=yes"
  -o "ConnectTimeout=${SSH_CONNECT_TIMEOUT}"
  -o "ServerAliveInterval=${SSH_SERVER_ALIVE_INTERVAL}"
  -o "ServerAliveCountMax=${SSH_SERVER_ALIVE_COUNT_MAX}"
)

CONTROL_PLANE_HOSTS_ENV_LOADED="false"
RESOLVED_SSH_HOST=""
RESOLVED_SSH_USER=""
RESOLVED_SSH_PORT=""
RESOLVED_SSH_HOST_KEY_ALIAS=""
RESOLVED_SSH_OPTS=()

log() {
  printf '[fugue-install] %s\n' "$*"
}

log_stderr() {
  printf '[fugue-install] %s\n' "$*" >&2
}

fail() {
  printf '[fugue-install] ERROR: %s\n' "$*" >&2
  exit 1
}

api_public_base_url() {
  if [[ -n "${FUGUE_DOMAIN}" ]]; then
    printf 'https://%s' "${FUGUE_DOMAIN}"
    return
  fi
  printf 'http://%s:%s' "${PUBLIC_ENDPOINT_HOST}" "${API_NODEPORT}"
}

api_public_health_url() {
  printf '%s/healthz' "$(api_public_base_url)"
}

mesh_enabled() {
  [[ "${FUGUE_MESH_ENABLED}" == "true" ]]
}

mesh_login_server() {
  printf 'https://%s' "${FUGUE_MESH_DOMAIN}"
}

cluster_join_server_value() {
  local join_host="${PUBLIC_ENDPOINT_HOST}"
  if mesh_enabled; then
    local primary_mesh_ip=""
    primary_mesh_ip="$(mesh_ip_for_alias "${PRIMARY_ALIAS}")"
    if [[ -n "${primary_mesh_ip}" ]]; then
      join_host="${primary_mesh_ip}"
    fi
  fi
  printf 'https://%s:6443' "${join_host}"
}

registry_push_base_value() {
  printf '%s.%s.svc.cluster.local:5000' "${REGISTRY_DEPLOYMENT_NAME}" "${NAMESPACE}"
}

registry_pull_base_value() {
  printf '%s' "${FUGUE_REGISTRY_PULL_BASE:-registry.fugue.internal:5000}"
}

node_registry_mirror_endpoint_value() {
  printf '%s' "${FUGUE_REGISTRY_MIRROR_ENDPOINT:-127.0.0.1:${REGISTRY_NODEPORT}}"
}

registry_endpoint_url_value() {
  local endpoint="$1"
  case "${endpoint}" in
    http://*|https://*)
      printf '%s' "${endpoint}"
      ;;
    *)
      printf 'http://%s' "${endpoint}"
      ;;
  esac
}

cluster_join_registry_endpoint_value() {
  printf '%s' "${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT:-$(node_registry_mirror_endpoint_value)}"
}

append_image_prepull_values() {
  local image
  if [[ -z "${FUGUE_IMAGE_PREPULL_IMAGES:-}" ]]; then
    return 0
  fi
  cat >>"${VALUES_FILE}" <<'EOF'

imagePrePull:
  enabled: true
  images:
EOF
  printf '%s' "${FUGUE_IMAGE_PREPULL_IMAGES}" | tr ',' '\n' | while IFS= read -r image; do
    image="$(printf '%s' "${image}" | awk '{$1=$1; print}')"
    if [[ -z "${image}" ]]; then
      continue
    fi
    image="${image//\"/\\\"}"
    printf '    - "%s"\n' "${image}" >>"${VALUES_FILE}"
  done
}

run_with_retry() {
  local attempts="$1"
  local delay="$2"
  local description="$3"
  shift 3

  local try exit_code=1
  for try in $(seq 1 "${attempts}"); do
    if "$@"; then
      return 0
    else
      exit_code=$?
    fi
    if [[ "${try}" -lt "${attempts}" ]]; then
      printf '[fugue-install] %s\n' "${description} failed on attempt ${try}/${attempts} (exit ${exit_code}); retrying in ${delay}s" >&2
      sleep "${delay}"
    fi
  done
  return "${exit_code}"
}

require_cmd() {
  local cmd="$1"
  command -v "${cmd}" >/dev/null 2>&1 || fail "missing required command: ${cmd}"
}

detect_container_tool() {
  if [[ -n "${CONTAINER_TOOL}" ]]; then
    command -v "${CONTAINER_TOOL}" >/dev/null 2>&1 || fail "container tool not found: ${CONTAINER_TOOL}"
    return
  fi

  if command -v docker >/dev/null 2>&1; then
    CONTAINER_TOOL="docker"
    return
  fi
  if command -v podman >/dev/null 2>&1; then
    CONTAINER_TOOL="podman"
    return
  fi

  fail "need docker or podman on the local machine"
}

generate_secret() {
  openssl rand -hex 24
}

maybe_reuse_existing_bootstrap_key() {
  if [[ -n "${BOOTSTRAP_KEY}" ]]; then
    return
  fi

  local existing_key=""
  existing_key="$(read_existing_config_secret_value "FUGUE_BOOTSTRAP_ADMIN_KEY")"
  if [[ -n "${existing_key}" ]]; then
    BOOTSTRAP_KEY="${existing_key}"
    log "reusing existing bootstrap admin key from ${NAMESPACE}/${CONFIG_SECRET_NAME}"
  fi
}

read_existing_config_secret_value() {
  local key="$1"
  local jsonpath=""
  jsonpath="{.data.${key}}"
  ssh_root_run "${PRIMARY_ALIAS}" "KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl -n $(printf '%q' "${NAMESPACE}") get secret $(printf '%q' "${CONFIG_SECRET_NAME}") -o jsonpath=$(printf '%q' "${jsonpath}") 2>/dev/null | base64 -d 2>/dev/null || true"
}

existing_cluster_join_mesh_provider() {
  read_existing_config_secret_value "FUGUE_CLUSTER_JOIN_MESH_PROVIDER"
}

existing_cluster_join_mesh_login_server() {
  read_existing_config_secret_value "FUGUE_CLUSTER_JOIN_MESH_LOGIN_SERVER"
}

mesh_domain_from_login_server() {
  local login_server="${1:-}"
  login_server="${login_server#*://}"
  login_server="${login_server%%/*}"
  login_server="${login_server%%:*}"
  if [[ -n "${login_server}" ]]; then
    printf '%s' "${login_server}"
  fi
}

expected_edge_proxy_mesh_domain() {
  local login_server=""
  if mesh_enabled; then
    printf '%s' "${FUGUE_MESH_DOMAIN}"
    return 0
  fi
  if [[ "$(existing_cluster_join_mesh_provider)" != "tailscale" ]]; then
    return 1
  fi
  login_server="$(existing_cluster_join_mesh_login_server)"
  [[ -n "${login_server}" ]] || return 1
  mesh_domain_from_login_server "${login_server}"
}

maybe_reuse_existing_mesh_edge_settings() {
  local provider=""
  local login_server=""
  local mesh_domain=""

  if mesh_enabled; then
    return 0
  fi

  provider="$(existing_cluster_join_mesh_provider)"
  if [[ "${provider}" != "tailscale" ]]; then
    return 0
  fi

  login_server="$(existing_cluster_join_mesh_login_server)"
  [[ -n "${login_server}" ]] || return 0

  mesh_domain="$(mesh_domain_from_login_server "${login_server}")"
  [[ -n "${mesh_domain}" ]] || fail "failed to parse mesh domain from existing login server ${login_server}"

  FUGUE_MESH_ENABLED="true"
  FUGUE_MESH_PROVIDER="${provider}"
  FUGUE_MESH_DOMAIN="${mesh_domain}"
  log "reusing existing mesh edge routing from ${NAMESPACE}/${CONFIG_SECRET_NAME} (${login_server})"
}

maybe_reuse_existing_mesh_auth_key() {
  if ! mesh_enabled || [[ -n "${FUGUE_MESH_AUTH_KEY}" ]]; then
    return
  fi

  local existing_key=""
  existing_key="$(read_existing_config_secret_value "FUGUE_CLUSTER_JOIN_MESH_AUTH_KEY")"
  if [[ -n "${existing_key}" ]]; then
    FUGUE_MESH_AUTH_KEY="${existing_key}"
    log "reusing existing mesh auth key from ${NAMESPACE}/${CONFIG_SECRET_NAME}"
  fi
}

maybe_reuse_existing_edge_tls_ask_token() {
  if [[ -n "${FUGUE_EDGE_TLS_ASK_TOKEN}" ]]; then
    return
  fi

  local existing_key=""
  existing_key="$(read_existing_config_secret_value "FUGUE_EDGE_TLS_ASK_TOKEN")"
  if [[ -n "${existing_key}" ]]; then
    FUGUE_EDGE_TLS_ASK_TOKEN="${existing_key}"
    log "reusing existing edge TLS ask token from ${NAMESPACE}/${CONFIG_SECRET_NAME}"
  fi
}

has_app_tls_material() {
  [[ -f "${FUGUE_APP_TLS_CERT_FILE}" && -f "${FUGUE_APP_TLS_KEY_FILE}" ]]
}

remote_has_app_tls_material() {
  ssh_root_run "${PRIMARY_ALIAS}" "test -f /etc/caddy/tls/cloudflare-apps-origin.crt && test -f /etc/caddy/tls/cloudflare-apps-origin.key"
}

app_tls_cert_matches_domain() {
  has_app_tls_material || return 1
  local wildcard="DNS:*.${FUGUE_APP_BASE_DOMAIN}"
  local apex="DNS:${FUGUE_APP_BASE_DOMAIN}"
  openssl x509 -in "${FUGUE_APP_TLS_CERT_FILE}" -noout -ext subjectAltName 2>/dev/null | grep -F "${wildcard}" >/dev/null 2>&1 || \
    openssl x509 -in "${FUGUE_APP_TLS_CERT_FILE}" -noout -ext subjectAltName 2>/dev/null | grep -F "${apex}" >/dev/null 2>&1
}

remote_app_tls_cert_matches_domain() {
  remote_has_app_tls_material || return 1
  local wildcard="DNS:*.${FUGUE_APP_BASE_DOMAIN}"
  local apex="DNS:${FUGUE_APP_BASE_DOMAIN}"
  ssh_root_run "${PRIMARY_ALIAS}" "openssl x509 -in /etc/caddy/tls/cloudflare-apps-origin.crt -noout -ext subjectAltName 2>/dev/null | grep -F $(printf '%q' "${wildcard}") >/dev/null 2>&1 || openssl x509 -in /etc/caddy/tls/cloudflare-apps-origin.crt -noout -ext subjectAltName 2>/dev/null | grep -F $(printf '%q' "${apex}") >/dev/null 2>&1"
}

control_plane_bundle_files_present() {
  [[ -r "${CONTROL_PLANE_HOSTS_ENV_FILE}" && -r "${CONTROL_PLANE_SSH_KEY_FILE}" && -r "${CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE}" ]]
}

control_plane_bundle_available() {
  [[ "${USE_CONTROL_PLANE_AUTOMATION_SSH}" == "true" ]] || return 1
  control_plane_bundle_files_present
}

load_control_plane_hosts_env() {
  if [[ "${CONTROL_PLANE_HOSTS_ENV_LOADED}" == "true" ]]; then
    return
  fi
  CONTROL_PLANE_HOSTS_ENV_LOADED="true"
  if [[ -r "${CONTROL_PLANE_HOSTS_ENV_FILE}" ]]; then
    # shellcheck disable=SC1090
    source "${CONTROL_PLANE_HOSTS_ENV_FILE}"
  fi
}

control_plane_slot_for_host() {
  local host="$1"
  local slot alias_var host_var

  load_control_plane_hosts_env

  for slot in 1 2 3; do
    alias_var="FUGUE_NODE${slot}_ALIAS"
    host_var="FUGUE_NODE${slot}_HOST"
    if [[ "${host}" == "${!alias_var-}" || "${host}" == "${!host_var-}" ]]; then
      printf '%s' "${slot}"
      return 0
    fi
  done
  return 1
}

control_plane_role_for_slot() {
  local slot="$1"

  case "${slot}" in
    1)
      printf 'primary'
      ;;
    2)
      printf 'secondary-1'
      ;;
    3)
      printf 'secondary-2'
      ;;
  esac
}

detect_kubectl_for_ssh_fallback() {
  if command -v kubectl >/dev/null 2>&1; then
    printf 'kubectl'
    return 0
  fi
  if command -v k3s >/dev/null 2>&1; then
    printf 'k3s kubectl'
    return 0
  fi
  return 1
}

control_plane_node_address_candidates_for_slot() {
  local slot="$1"
  local role=""
  local kubectl_bin=""
  local node_name=""
  local -a kubectl_cmd=()

  role="$(control_plane_role_for_slot "${slot}")"
  [[ -n "${role}" ]] || return 0
  kubectl_bin="$(detect_kubectl_for_ssh_fallback || true)"
  [[ -n "${kubectl_bin}" ]] || return 0
  read -r -a kubectl_cmd <<<"${kubectl_bin}"
  node_name="$("${kubectl_cmd[@]}" get nodes -l "fugue.install/role=${role}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  [[ -n "${node_name}" ]] || return 0
  "${kubectl_cmd[@]}" get node "${node_name}" -o jsonpath='{range .status.addresses[?(@.type=="ExternalIP")]}{.address}{"\n"}{end}{range .status.addresses[?(@.type=="InternalIP")]}{.address}{"\n"}{end}{range .status.addresses[?(@.type=="Hostname")]}{.address}{"\n"}{end}' 2>/dev/null | awk 'NF > 0 && !seen[$0]++'
}

ssh_host_port_is_reachable() {
  local host="$1"
  local port="$2"

  [[ -n "${host}" && -n "${port}" ]] || return 1
  if command -v nc >/dev/null 2>&1; then
    nc -z -w 3 "${host}" "${port}" >/dev/null 2>&1 && return 0
    nc -z -G 3 "${host}" "${port}" >/dev/null 2>&1 && return 0
  fi
  if command -v timeout >/dev/null 2>&1; then
    timeout 3 bash -c ":</dev/tcp/${host}/${port}" >/dev/null 2>&1 && return 0
  fi
  return 1
}

apply_control_plane_ssh_node_address_fallback() {
  local slot="$1"
  local configured_host="$2"
  local port="$3"
  local candidate=""

  if [[ "${FUGUE_CONTROL_PLANE_SSH_NODE_ADDRESS_FALLBACK:-true}" != "true" ]]; then
    return
  fi
  if ssh_host_port_is_reachable "${RESOLVED_SSH_HOST}" "${port}"; then
    return
  fi

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    [[ "${candidate}" != "${configured_host}" ]] || continue
    if ssh_host_port_is_reachable "${candidate}" "${port}"; then
      RESOLVED_SSH_HOST="${candidate}"
      RESOLVED_SSH_HOST_KEY_ALIAS="${configured_host}"
      log_stderr "configured SSH host ${configured_host}:${port} for slot ${slot} is not reachable; using Kubernetes node address ${candidate}:${port}"
      return
    fi
  done < <(control_plane_node_address_candidates_for_slot "${slot}")
}

resolve_ssh_target() {
  local host="$1"
  local slot host_var user_var port_var configured_host port

  RESOLVED_SSH_HOST="${host}"
  RESOLVED_SSH_USER=""
  RESOLVED_SSH_PORT=""
  RESOLVED_SSH_HOST_KEY_ALIAS=""

  if ! control_plane_bundle_available; then
    return 0
  fi

  # Load hosts.env in the current shell before the command substitution below.
  # Otherwise the sourced FUGUE_NODE*_HOST/USER/PORT variables only exist in the
  # subshell spawned for control_plane_slot_for_host and indirect expansion
  # falls back to the unresolved alias (for example "gcp1").
  load_control_plane_hosts_env
  slot="$(control_plane_slot_for_host "${host}" || true)"
  if [[ -z "${slot}" ]]; then
    return 0
  fi

  host_var="FUGUE_NODE${slot}_HOST"
  user_var="FUGUE_NODE${slot}_USER"
  port_var="FUGUE_NODE${slot}_PORT"
  if [[ -n "${!host_var-}" ]]; then
    RESOLVED_SSH_HOST="${!host_var}"
  fi
  RESOLVED_SSH_USER="${!user_var-}"
  RESOLVED_SSH_PORT="${!port_var-}"
  configured_host="${RESOLVED_SSH_HOST}"
  port="${RESOLVED_SSH_PORT:-22}"
  apply_control_plane_ssh_node_address_fallback "${slot}" "${configured_host}" "${port}"
}

ssh_target_login() {
  if [[ -n "${RESOLVED_SSH_USER}" ]]; then
    printf '%s@%s' "${RESOLVED_SSH_USER}" "${RESOLVED_SSH_HOST}"
    return
  fi
  printf '%s' "${RESOLVED_SSH_HOST}"
}

ssh_opts_for_target() {
  RESOLVED_SSH_OPTS=("${SSH_OPTS[@]}")
  if control_plane_bundle_available; then
    RESOLVED_SSH_OPTS+=(
      -o "IdentitiesOnly=yes"
      -i "${CONTROL_PLANE_SSH_KEY_FILE}"
      -o "StrictHostKeyChecking=yes"
      -o "UserKnownHostsFile=${CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE}"
    )
  fi
  if [[ -n "${RESOLVED_SSH_HOST_KEY_ALIAS}" ]]; then
    RESOLVED_SSH_OPTS+=(-o "HostKeyAlias=${RESOLVED_SSH_HOST_KEY_ALIAS}")
  fi
  if [[ -n "${RESOLVED_SSH_PORT}" ]]; then
    RESOLVED_SSH_OPTS+=(-p "${RESOLVED_SSH_PORT}")
  fi
}

ssh_config_value_for_alias() {
  local host="$1"
  local key="$2"
  ssh -G "${host}" | awk -v key="${key}" '$1 == key { print $2; exit }'
}

ssh_host_for_alias() {
  local host="$1"
  local slot host_var

  if control_plane_bundle_available; then
    load_control_plane_hosts_env
    slot="$(control_plane_slot_for_host "${host}" || true)"
    if [[ -n "${slot}" ]]; then
      host_var="FUGUE_NODE${slot}_HOST"
      if [[ -n "${!host_var-}" ]]; then
        printf '%s' "${!host_var}"
        return 0
      fi
    fi
  fi

  ssh_config_value_for_alias "${host}" hostname
}

ssh_user_for_alias() {
  local host="$1"
  ssh_config_value_for_alias "${host}" user
}

ssh_port_for_alias() {
  local host="$1"
  local port=""
  port="$(ssh_config_value_for_alias "${host}" port)"
  if [[ -n "${port}" ]]; then
    printf '%s' "${port}"
    return 0
  fi
  printf '22'
}

ssh_run_raw() {
  local host="$1"
  shift
  resolve_ssh_target "${host}"
  ssh_opts_for_target
  ssh -n "${RESOLVED_SSH_OPTS[@]}" "$(ssh_target_login)" "$@"
}

ssh_run() {
  local host="$1"
  shift
  run_with_retry "${REMOTE_CMD_RETRIES}" "${REMOTE_CMD_RETRY_DELAY}" "ssh command on ${host}" \
    ssh_run_raw "${host}" "$@"
}

detect_remote_mode_raw() {
  local host="$1"
  resolve_ssh_target "${host}"
  ssh_opts_for_target
  ssh -n "${RESOLVED_SSH_OPTS[@]}" "$(ssh_target_login)" 'if [ "$(id -u)" -eq 0 ]; then echo root; elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then echo sudo; else echo none; fi'
}

remote_mode_cache_var() {
  local host="$1"
  printf 'FUGUE_REMOTE_MODE_%s' "$(printf '%s' "${host}" | tr -c 'A-Za-z0-9' '_')"
}

get_cached_remote_mode() {
  local host="$1"
  local var_name
  var_name="$(remote_mode_cache_var "${host}")"
  printf '%s' "${!var_name-}"
}

cache_remote_mode() {
  local host="$1"
  local mode="$2"
  local var_name
  var_name="$(remote_mode_cache_var "${host}")"
  printf -v "${var_name}" '%s' "${mode}"
}

detect_remote_mode() {
  local host="$1"
  local cached_mode
  local mode
  cached_mode="$(get_cached_remote_mode "${host}")"
  if [[ -n "${cached_mode}" ]]; then
    printf '%s' "${cached_mode}"
    return 0
  fi
  mode="$(run_with_retry "${REMOTE_CMD_RETRIES}" "${REMOTE_CMD_RETRY_DELAY}" "detect remote mode on ${host}" \
    detect_remote_mode_raw "${host}")" || return 1
  cache_remote_mode "${host}" "${mode}"
  printf '%s' "${mode}"
}

ssh_root_raw() {
  local host="$1"
  local mode="$2"
  local script="$3"
  resolve_ssh_target "${host}"
  ssh_opts_for_target
  case "${mode}" in
    root)
      printf '%s\n' "${script}" | ssh "${RESOLVED_SSH_OPTS[@]}" "$(ssh_target_login)" "bash -s"
      ;;
    sudo)
      printf '%s\n' "${script}" | ssh "${RESOLVED_SSH_OPTS[@]}" "$(ssh_target_login)" "sudo bash -s"
      ;;
    *)
      fail "${host} needs either a root SSH login or passwordless sudo"
      ;;
  esac
}

ssh_root() {
  local host="$1"
  shift
  local mode
  local script
  script="$(cat)"
  mode="$(detect_remote_mode "${host}")" || fail "failed to detect remote privilege mode on ${host}"
  run_with_retry "${REMOTE_CMD_RETRIES}" "${REMOTE_CMD_RETRY_DELAY}" "root script on ${host}" \
    ssh_root_raw "${host}" "${mode}" "${script}"
}

ssh_root_run_raw() {
  local host="$1"
  local mode="$2"
  local cmd="$3"
  resolve_ssh_target "${host}"
  ssh_opts_for_target
  case "${mode}" in
    root)
      ssh -n "${RESOLVED_SSH_OPTS[@]}" "$(ssh_target_login)" "bash -lc $(printf '%q' "${cmd}")"
      ;;
    sudo)
      ssh -n "${RESOLVED_SSH_OPTS[@]}" "$(ssh_target_login)" "sudo bash -lc $(printf '%q' "${cmd}")"
      ;;
    *)
      fail "${host} needs either a root SSH login or passwordless sudo"
      ;;
  esac
}

ssh_root_run() {
  local host="$1"
  shift
  local mode
  local cmd
  cmd="$*"
  mode="$(detect_remote_mode "${host}")" || fail "failed to detect remote privilege mode on ${host}"
  run_with_retry "${REMOTE_CMD_RETRIES}" "${REMOTE_CMD_RETRY_DELAY}" "root command on ${host}" \
    ssh_root_run_raw "${host}" "${mode}" "${cmd}"
}

scp_to() {
  local src="$1"
  local host="$2"
  local dst="$3"
  local attempt
  local local_size remote_size remote_quoted
  local_size="$(local_file_size "${src}")"
  printf -v remote_quoted '%q' "${dst}"
  resolve_ssh_target "${host}"
  ssh_opts_for_target

  for attempt in $(seq 1 "${UPLOAD_RETRIES}"); do
    if scp -q "${RESOLVED_SSH_OPTS[@]}" "${src}" "$(ssh_target_login):${dst}"; then
      remote_size="$(run_with_retry "${REMOTE_CMD_RETRIES}" "${REMOTE_CMD_RETRY_DELAY}" "verify uploaded file on ${host}" \
        ssh_run "${host}" "stat -c %s ${remote_quoted}" | tr -d '[:space:]' || true)"
      if [[ -n "${remote_size}" && "${remote_size}" == "${local_size}" ]]; then
        return 0
      fi
      log "uploaded file size mismatch for ${host}:${dst}; local=${local_size} remote=${remote_size:-missing}"
    fi
    if [[ "${attempt}" -lt "${UPLOAD_RETRIES}" ]]; then
      log "retrying upload to ${host}:${dst} in ${UPLOAD_RETRY_DELAY}s"
      sleep "${UPLOAD_RETRY_DELAY}"
    fi
  done

  log "scp upload failed for ${host}:${dst}; falling back to streamed ssh upload"
  for attempt in $(seq 1 "${UPLOAD_RETRIES}"); do
    if ssh "${RESOLVED_SSH_OPTS[@]}" "$(ssh_target_login)" "cat > ${remote_quoted}" < "${src}"; then
      remote_size="$(run_with_retry "${REMOTE_CMD_RETRIES}" "${REMOTE_CMD_RETRY_DELAY}" "verify streamed upload on ${host}" \
        ssh_run "${host}" "stat -c %s ${remote_quoted}" | tr -d '[:space:]' || true)"
      if [[ -n "${remote_size}" && "${remote_size}" == "${local_size}" ]]; then
        return 0
      fi
      log "streamed upload size mismatch for ${host}:${dst}; local=${local_size} remote=${remote_size:-missing}"
    fi
    if [[ "${attempt}" -lt "${UPLOAD_RETRIES}" ]]; then
      log "retrying streamed upload to ${host}:${dst} in ${UPLOAD_RETRY_DELAY}s"
      sleep "${UPLOAD_RETRY_DELAY}"
    fi
  done

  fail "failed to upload ${src} to ${host}:${dst}"
}

local_file_size() {
  local path="$1"
  if stat -f %z "${path}" >/dev/null 2>&1; then
    stat -f %z "${path}"
  else
    stat -c %s "${path}"
  fi
}

detect_remote_platform() {
  local arch
  arch="$(ssh_run "${PRIMARY_ALIAS}" "uname -m")"
  case "${arch}" in
    x86_64|amd64)
      IMAGE_PLATFORM="linux/amd64"
      ;;
    aarch64|arm64)
      IMAGE_PLATFORM="linux/arm64"
      ;;
    armv7l|armv7)
      IMAGE_PLATFORM="linux/arm/v7"
      ;;
    *)
      fail "unsupported remote architecture: ${arch}"
      ;;
  esac
}

detect_api_ip() {
  if [[ -n "${K3S_API_IP}" ]]; then
    return
  fi

  K3S_API_IP="$(ssh_run "${PRIMARY_ALIAS}" "ip -4 route get 1.1.1.1 | awk '{for (i=1;i<=NF;i++) if (\$i==\"src\") {print \$(i+1); exit}}'")"
  [[ -n "${K3S_API_IP}" ]] || fail "failed to detect primary node IP; set FUGUE_K3S_API_IP manually"
}

detect_public_endpoint_host() {
  if [[ -n "${PUBLIC_ENDPOINT_HOST}" ]]; then
    return
  fi

  PUBLIC_ENDPOINT_HOST="$(public_host_for_alias "${PRIMARY_ALIAS}")"
  [[ -n "${PUBLIC_ENDPOINT_HOST}" ]] || fail "failed to detect public endpoint host for ${PRIMARY_ALIAS}; set FUGUE_PUBLIC_ENDPOINT_HOST manually"
}

public_host_for_alias() {
  local host="$1"
  ssh_host_for_alias "${host}"
}

mesh_ip_for_alias() {
  local host="$1"
  ssh_root_run "${host}" "if command -v tailscale >/dev/null 2>&1; then tailscale ip -4 2>/dev/null | awk 'NR == 1 {print; exit}'; fi" | tr -d '\r'
}

control_plane_automation_host_for_alias() {
  local host="$1"
  local mesh_ip=""
  if mesh_enabled; then
    mesh_ip="$(mesh_ip_for_alias "${host}" || true)"
    if [[ -n "${mesh_ip}" ]]; then
      printf '%s' "${mesh_ip}"
      return 0
    fi
  fi
  ssh_host_for_alias "${host}"
}

node_internal_ip_for_alias() {
  local host="$1"
  local default_iface=""
  default_iface="$(ssh_root_run "${host}" "ip -4 route show default 2>/dev/null | awk 'NR == 1 {print \$5; exit}'" | tr -d '\r')"
  if [[ -n "${default_iface}" ]]; then
    ssh_root_run "${host}" "ip -o -4 addr show dev $(printf '%q' "${default_iface}") scope global 2>/dev/null | awk 'NR == 1 {split(\$4,a,\"/\"); print a[1]; exit}'" | tr -d '\r'
    return 0
  fi

  ssh_root_run "${host}" "ip -o -4 addr show scope global 2>/dev/null | awk '\$2 !~ /^(tailscale0|cni0|flannel\\.1|docker0|lo)\$/ {split(\$4,a,\"/\"); print a[1]; exit}'" | tr -d '\r'
}

node_public_ip_for_alias() {
  local host="$1"
  local public_ip=""
  public_ip="$(topology_override_for_alias "${host}" "PUBLIC_IP")"
  if [[ -n "${public_ip}" ]]; then
    printf '%s' "${public_ip}"
    return 0
  fi

  public_ip="$(ssh_root_run "${host}" "if command -v curl >/dev/null 2>&1; then curl -fsS --max-time 10 https://api.ipify.org 2>/dev/null || true; fi" | tr -d '\r')"
  if [[ -n "${public_ip}" ]]; then
    printf '%s' "${public_ip}"
    return 0
  fi

  public_ip="$(public_host_for_alias "${host}")"
  if [[ "${public_ip}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf '%s' "${public_ip}"
    return 0
  fi
  return 1
}

node_external_ip_for_alias() {
  local host="$1"
  local mesh_ip=""
  if mesh_enabled; then
    mesh_ip="$(mesh_ip_for_alias "${host}")"
    if [[ -n "${mesh_ip}" ]]; then
      printf '%s' "${mesh_ip}"
      return 0
    fi
  fi
  public_host_for_alias "${host}"
}

host_slot_for_alias() {
  local host="$1"
  if [[ "${host}" == "${PRIMARY_ALIAS}" ]]; then
    printf '1'
    return 0
  fi
  if [[ "${host}" == "${SECONDARY_ALIASES[0]}" ]]; then
    printf '2'
    return 0
  fi
  if [[ "${host}" == "${SECONDARY_ALIASES[1]}" ]]; then
    printf '3'
    return 0
  fi
}

topology_override_for_alias() {
  local host="$1"
  local kind="$2"
  local slot=""
  local var_name=""
  local value=""

  slot="$(host_slot_for_alias "${host}")"
  if [[ -n "${slot}" ]]; then
    var_name="FUGUE_NODE${slot}_${kind}"
    value="${!var_name-}"
  fi
  if [[ -z "${value}" ]]; then
    var_name="FUGUE_NODE_${kind}"
    value="${!var_name-}"
  fi
  printf '%s' "${value}"
}

detect_remote_public_country_json() {
  local host="$1"
  local geolocation_url="${FUGUE_NODE_GEO_URL:-https://ipapi.co/json/}"
  ssh_root_run "${host}" "if command -v curl >/dev/null 2>&1; then curl -fsS --max-time 5 $(printf '%q' "${geolocation_url}") 2>/dev/null || true; fi" | tr -d '\r\n'
}

extract_json_string() {
  local json="$1"
  local key="$2"
  printf '%s' "${json}" | sed -n "s/.*\"${key}\"[[:space:]]*:[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p"
}

node_country_code_for_alias() {
  local host="$1"
  local country_code=""
  local json=""
  country_code="$(topology_override_for_alias "${host}" "COUNTRY_CODE")"
  if [[ -n "${country_code}" ]]; then
    printf '%s' "${country_code}" | tr '[:upper:]' '[:lower:]'
    return 0
  fi
  json="$(detect_remote_public_country_json "${host}")"
  [[ -n "${json}" ]] || return 1
  country_code="$(extract_json_string "${json}" "country_code")"
  [[ -n "${country_code}" ]] || return 1
  printf '%s' "${country_code}" | tr '[:upper:]' '[:lower:]'
}

node_zone_for_alias() {
  local host="$1"
  local zone=""
  zone="$(topology_override_for_alias "${host}" "ZONE")"
  if [[ -n "${zone}" ]]; then
    printf '%s' "${zone}"
    return 0
  fi
  return 1
}

node_region_for_alias() {
  local host="$1"
  local region=""
  region="$(topology_override_for_alias "${host}" "REGION")"
  if [[ -n "${region}" ]]; then
    printf '%s' "${region}"
    return 0
  fi
  return 1
}

render_server_network_config() {
  local host="$1"
  local node_ip
  local node_external_ip

  node_ip="$(node_internal_ip_for_alias "${host}")"
  [[ -n "${node_ip}" ]] || fail "failed to detect private node IP for ${host}"
  node_external_ip="$(node_external_ip_for_alias "${host}")"
  [[ -n "${node_external_ip}" ]] || fail "failed to detect external node IP for ${host}"

  cat <<EOF
node-ip: "${node_ip}"
node-external-ip: "${node_external_ip}"
flannel-external-ip: true
EOF

  if mesh_enabled; then
    cat <<'EOF'
flannel-iface: "tailscale0"
EOF
  fi
}

render_tls_sans_for_host() {
  local host="$1"
  local node_external_ip
  node_external_ip="$(node_external_ip_for_alias "${host}")"
  cat <<EOF
tls-san:
  - "${K3S_API_IP}"
EOF
  if [[ -n "${PUBLIC_ENDPOINT_HOST}" && "${PUBLIC_ENDPOINT_HOST}" != "${K3S_API_IP}" ]]; then
    cat <<EOF
  - "${PUBLIC_ENDPOINT_HOST}"
EOF
  fi
  if [[ -n "${node_external_ip}" && "${node_external_ip}" != "${K3S_API_IP}" && "${node_external_ip}" != "${PUBLIC_ENDPOINT_HOST}" ]]; then
    cat <<EOF
  - "${node_external_ip}"
EOF
  fi
}

render_server_node_labels() {
  local host="$1"
  local role="$2"
  local country_code=""
  local public_ip=""
  local zone=""
  local region=""

  country_code="$(node_country_code_for_alias "${host}" || true)"
  public_ip="$(node_public_ip_for_alias "${host}" || true)"
  zone="$(node_zone_for_alias "${host}" || true)"
  region="$(node_region_for_alias "${host}" || true)"

  cat <<EOF
node-label:
  - "fugue.install/profile=combined"
  - "fugue.install/role=${role}"
EOF
  if [[ -n "${country_code}" ]]; then
    cat <<EOF
  - "fugue.io/location-country-code=${country_code}"
EOF
  fi
  if [[ -n "${public_ip}" ]]; then
    cat <<EOF
  - "fugue.io/public-ip=${public_ip}"
EOF
  fi
  if [[ -n "${region}" ]]; then
    cat <<EOF
  - "topology.kubernetes.io/region=${region}"
EOF
  fi
  if [[ -n "${zone}" ]]; then
    cat <<EOF
  - "topology.kubernetes.io/zone=${zone}"
EOF
  fi
}

check_ssh_and_sudo() {
  for host in "${ALL_ALIASES[@]}"; do
    log "checking SSH and root privileges on ${host}"
    ssh_run "${host}" "echo ok" >/dev/null
    ssh_root "${host}" <<'EOF'
set -euo pipefail
command -v systemctl >/dev/null 2>&1
EOF
  done
}

prepare_dist() {
  mkdir -p "${DIST_DIR}"
}

fetch_control_plane_automation_secret_field() {
  local field="$1"
  local template="{{index .data \"${field}\"}}"
  ssh_root_run "${PRIMARY_ALIAS}" "KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl -n $(printf '%q' "${NAMESPACE}") get secret $(printf '%q' "${CONTROL_PLANE_AUTOMATION_SECRET_NAME}") -o go-template=$(printf '%q' "${template}") 2>/dev/null || true"
}

maybe_reuse_existing_control_plane_automation_bundle() {
  local private_key_b64=""
  local known_hosts_b64=""
  local hosts_env_b64=""

  mkdir -p "$(dirname "${CONTROL_PLANE_SSH_KEY_FILE}")"

  private_key_b64="$(fetch_control_plane_automation_secret_field "ssh-private-key" | tr -d '\r\n')"
  known_hosts_b64="$(fetch_control_plane_automation_secret_field "ssh-known-hosts" | tr -d '\r\n')"
  hosts_env_b64="$(fetch_control_plane_automation_secret_field "hosts.env" | tr -d '\r\n')"

  if [[ -z "${private_key_b64}" || -z "${known_hosts_b64}" || -z "${hosts_env_b64}" ]]; then
    return
  fi

  printf '%s' "${private_key_b64}" | base64 --decode >"${CONTROL_PLANE_SSH_KEY_FILE}"
  chmod 0600 "${CONTROL_PLANE_SSH_KEY_FILE}"
  printf '%s' "${known_hosts_b64}" | base64 --decode >"${CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE}"
  chmod 0644 "${CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE}"
  printf '%s' "${hosts_env_b64}" | base64 --decode >"${CONTROL_PLANE_HOSTS_ENV_FILE}"
  chmod 0644 "${CONTROL_PLANE_HOSTS_ENV_FILE}"
  ssh-keygen -y -f "${CONTROL_PLANE_SSH_KEY_FILE}" >"${CONTROL_PLANE_SSH_PUBLIC_KEY_FILE}"
  chmod 0644 "${CONTROL_PLANE_SSH_PUBLIC_KEY_FILE}"
  CONTROL_PLANE_HOSTS_ENV_LOADED="false"
  log "reusing control-plane automation SSH bundle from ${NAMESPACE}/${CONTROL_PLANE_AUTOMATION_SECRET_NAME}"
}

generate_control_plane_automation_keypair() {
  mkdir -p "$(dirname "${CONTROL_PLANE_SSH_KEY_FILE}")"
  if [[ -r "${CONTROL_PLANE_SSH_KEY_FILE}" && -r "${CONTROL_PLANE_SSH_PUBLIC_KEY_FILE}" ]]; then
    return
  fi
  rm -f "${CONTROL_PLANE_SSH_KEY_FILE}" "${CONTROL_PLANE_SSH_PUBLIC_KEY_FILE}"
  ssh-keygen -q -t ed25519 -N "" -C "fugue-control-plane-automation" -f "${CONTROL_PLANE_SSH_KEY_FILE}" >/dev/null
}

write_control_plane_hosts_env() {
  local aliases=("${PRIMARY_ALIAS}" "${SECONDARY_ALIASES[0]}" "${SECONDARY_ALIASES[1]}")
  local slot alias host user port

  mkdir -p "$(dirname "${CONTROL_PLANE_HOSTS_ENV_FILE}")"
  : >"${CONTROL_PLANE_HOSTS_ENV_FILE}"

  for slot in 1 2 3; do
    alias="${aliases[$((slot-1))]}"
    host="$(control_plane_automation_host_for_alias "${alias}")"
    user="$(ssh_user_for_alias "${alias}")"
    port="$(ssh_port_for_alias "${alias}")"
    [[ -n "${host}" ]] || fail "failed to resolve SSH hostname for ${alias}"
    [[ -n "${user}" ]] || fail "failed to resolve SSH user for ${alias}"
    [[ -n "${port}" ]] || fail "failed to resolve SSH port for ${alias}"
    cat >>"${CONTROL_PLANE_HOSTS_ENV_FILE}" <<EOF
FUGUE_NODE${slot}_ALIAS=$(printf '%q' "${alias}")
FUGUE_NODE${slot}_HOST=$(printf '%q' "${host}")
FUGUE_NODE${slot}_USER=$(printf '%q' "${user}")
FUGUE_NODE${slot}_PORT=$(printf '%q' "${port}")
EOF
  done

  chmod 0644 "${CONTROL_PLANE_HOSTS_ENV_FILE}"
  CONTROL_PLANE_HOSTS_ENV_LOADED="false"
}

build_control_plane_known_hosts() {
  local aliases=("${PRIMARY_ALIAS}" "${SECONDARY_ALIASES[0]}" "${SECONDARY_ALIASES[1]}")
  local tmp_file alias host port

  tmp_file="${CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE}.tmp"
  : >"${tmp_file}"
  for alias in "${aliases[@]}"; do
    host="$(control_plane_automation_host_for_alias "${alias}")"
    port="$(ssh_port_for_alias "${alias}")"
    [[ -n "${host}" ]] || fail "failed to resolve SSH hostname for ${alias}"
    ssh-keyscan -p "${port}" -H "${host}" >>"${tmp_file}" 2>/dev/null || fail "failed to scan SSH host key for ${alias} (${host}:${port})"
  done
  sort -u "${tmp_file}" >"${CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE}"
  rm -f "${tmp_file}"
  chmod 0644 "${CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE}"
}

install_control_plane_automation_authorized_key_on_host() {
  local host="$1"
  local pub_remote="${REMOTE_TMP_BASE}/control-plane-automation.pub"

  prepare_remote_tmp "${host}"
  scp_to "${CONTROL_PLANE_SSH_PUBLIC_KEY_FILE}" "${host}" "${pub_remote}"
  ssh_run "${host}" "umask 077; mkdir -p ~/.ssh; touch ~/.ssh/authorized_keys; grep -Fqx -f $(printf '%q' "${pub_remote}") ~/.ssh/authorized_keys || cat $(printf '%q' "${pub_remote}") >> ~/.ssh/authorized_keys"
}

install_control_plane_automation_authorized_keys() {
  local host
  for host in "${ALL_ALIASES[@]}"; do
    log "installing control-plane automation SSH key on ${host}"
    install_control_plane_automation_authorized_key_on_host "${host}"
  done
}

publish_control_plane_automation_secret() {
  log "publishing control-plane automation SSH bundle to ${NAMESPACE}/${CONTROL_PLANE_AUTOMATION_SECRET_NAME}"
  prepare_remote_tmp "${PRIMARY_ALIAS}"
  scp_to "${CONTROL_PLANE_HOSTS_ENV_FILE}" "${PRIMARY_ALIAS}" "${REMOTE_TMP_BASE}/control-plane-hosts.env"
  scp_to "${CONTROL_PLANE_SSH_KEY_FILE}" "${PRIMARY_ALIAS}" "${REMOTE_TMP_BASE}/control-plane-id_ed25519"
  scp_to "${CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE}" "${PRIMARY_ALIAS}" "${REMOTE_TMP_BASE}/control-plane-known_hosts"
  ssh_root "${PRIMARY_ALIAS}" <<EOF
set -euo pipefail
chmod 0600 "${REMOTE_TMP_BASE}/control-plane-id_ed25519"
KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl -n "${NAMESPACE}" create secret generic "${CONTROL_PLANE_AUTOMATION_SECRET_NAME}" \
  --from-file=hosts.env="${REMOTE_TMP_BASE}/control-plane-hosts.env" \
  --from-file=ssh-private-key="${REMOTE_TMP_BASE}/control-plane-id_ed25519" \
  --from-file=ssh-known-hosts="${REMOTE_TMP_BASE}/control-plane-known_hosts" \
  --dry-run=client -o yaml | KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl apply -f -
EOF
}

install_control_plane_automation_local_bundle_on_host() {
  local host="$1"
  local runner_home=""

  runner_home="$(ssh_root_run "${host}" "getent passwd github-runner 2>/dev/null | cut -d: -f6 || true" | tr -d '\r')"
  prepare_remote_tmp "${host}"
  scp_to "${CONTROL_PLANE_HOSTS_ENV_FILE}" "${host}" "${REMOTE_TMP_BASE}/control-plane-hosts.env"
  scp_to "${CONTROL_PLANE_SSH_KEY_FILE}" "${host}" "${REMOTE_TMP_BASE}/control-plane-id_ed25519"
  scp_to "${CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE}" "${host}" "${REMOTE_TMP_BASE}/control-plane-known_hosts"
  ssh_root "${host}" <<EOF
set -euo pipefail
install_bundle() {
  local bundle_dir="\$1"
  local owner="\$2"
  local group="\$3"
  mkdir -p "\${bundle_dir}"
  install -d -m 0700 -o "\${owner}" -g "\${group}" "\${bundle_dir}"
  install -m 0600 -o "\${owner}" -g "\${group}" "${REMOTE_TMP_BASE}/control-plane-id_ed25519" "\${bundle_dir}/id_ed25519"
  install -m 0644 -o "\${owner}" -g "\${group}" "${REMOTE_TMP_BASE}/control-plane-known_hosts" "\${bundle_dir}/known_hosts"
  install -m 0644 -o "\${owner}" -g "\${group}" "${REMOTE_TMP_BASE}/control-plane-hosts.env" "\${bundle_dir}/hosts.env"
}

install_bundle "${CONTROL_PLANE_AUTOMATION_ROOT_DIR}" root root
if [[ -n "$(printf '%s' "${runner_home}")" ]] && id -u github-runner >/dev/null 2>&1; then
  install_bundle "$(printf '%s' "${runner_home}")/.config/fugue/control-plane-automation" github-runner github-runner
fi
EOF
}

install_control_plane_automation_local_bundles() {
  local host
  for host in "${ALL_ALIASES[@]}"; do
    log "installing local control-plane automation bundle on ${host}"
    install_control_plane_automation_local_bundle_on_host "${host}"
  done
}

setup_control_plane_automation() {
  maybe_reuse_existing_control_plane_automation_bundle
  if ! control_plane_bundle_files_present; then
    generate_control_plane_automation_keypair
    write_control_plane_hosts_env
    build_control_plane_known_hosts
  fi
  install_control_plane_automation_authorized_keys
  install_control_plane_automation_local_bundles
  publish_control_plane_automation_secret
}

dump_remote_k3s_debug() {
  local host="$1"
  log "collecting k3s diagnostics from ${host}"
  ssh_root "${host}" <<'EOF' || true
set -euo pipefail
echo '===== systemctl status k3s ====='
systemctl status k3s --no-pager -l || true
echo
echo '===== journalctl -u k3s ====='
journalctl -u k3s -n 120 --no-pager -l || true
echo
echo '===== /etc/rancher/k3s ====='
ls -la /etc/rancher/k3s || true
echo
echo '===== /var/lib/rancher/k3s/server ====='
ls -la /var/lib/rancher/k3s/server || true
EOF
}

build_images() {
  log "building local images with ${CONTAINER_TOOL} for ${IMAGE_PLATFORM}"
  prefetch_build_bases
  run_with_retry 3 5 "build fugue-api image" build_image "${REPO_ROOT}/Dockerfile.api" "fugue-api:${IMAGE_TAG}"
  run_with_retry 3 5 "build fugue-controller image" build_image "${REPO_ROOT}/Dockerfile.controller" "fugue-controller:${IMAGE_TAG}"
  run_with_retry 3 5 "save built images" "${CONTAINER_TOOL}" save -o "${IMAGES_TAR}" "fugue-api:${IMAGE_TAG}" "fugue-controller:${IMAGE_TAG}"
}

prefetch_build_bases() {
  local platform_args=()
  if [[ -n "${IMAGE_PLATFORM}" ]]; then
    platform_args=(--platform "${IMAGE_PLATFORM}")
  fi

  run_with_retry 5 5 "pull golang build base image" "${CONTAINER_TOOL}" pull "${platform_args[@]}" golang:1.25-alpine
}

build_image() {
  local dockerfile="$1"
  local tag="$2"

  if [[ "${CONTAINER_TOOL}" == "docker" ]]; then
    BUILDKIT_PROGRESS=plain "${CONTAINER_TOOL}" build --pull=false --platform "${IMAGE_PLATFORM}" -f "${dockerfile}" -t "${tag}" "${REPO_ROOT}"
    return
  fi

  "${CONTAINER_TOOL}" build --pull=false --platform "${IMAGE_PLATFORM}" -f "${dockerfile}" -t "${tag}" "${REPO_ROOT}"
}

build_chart_archive() {
  tar -C "${REPO_ROOT}/deploy/helm" -cf "${CHART_TAR}" fugue
}

install_edge_proxy_on_primary() {
  if [[ -z "${FUGUE_DOMAIN}" ]]; then
    return
  fi

  determine_edge_upstream
  determine_registry_upstream
  determine_headscale_upstream

  local app_host_tls_directive="tls internal"
  local app_tls_uploaded="false"
  local purge_stale_app_tls_material="false"
  if [[ -n "${FUGUE_APP_BASE_DOMAIN}" ]] && app_tls_cert_matches_domain; then
    log "uploading wildcard app TLS material to ${PRIMARY_ALIAS}"
    prepare_remote_tmp "${PRIMARY_ALIAS}"
    scp_to "${FUGUE_APP_TLS_CERT_FILE}" "${PRIMARY_ALIAS}" "${REMOTE_TMP_BASE}/cloudflare-apps-origin.crt"
    scp_to "${FUGUE_APP_TLS_KEY_FILE}" "${PRIMARY_ALIAS}" "${REMOTE_TMP_BASE}/cloudflare-apps-origin.key"
    app_host_tls_directive="tls /etc/caddy/tls/cloudflare-apps-origin.crt /etc/caddy/tls/cloudflare-apps-origin.key"
    app_tls_uploaded="true"
  elif [[ -n "${FUGUE_APP_BASE_DOMAIN}" ]] && remote_app_tls_cert_matches_domain; then
    log "reusing existing wildcard app TLS material already installed on ${PRIMARY_ALIAS}"
    app_host_tls_directive="tls /etc/caddy/tls/cloudflare-apps-origin.crt /etc/caddy/tls/cloudflare-apps-origin.key"
  elif [[ -n "${FUGUE_APP_BASE_DOMAIN}" ]] && remote_has_app_tls_material; then
    log "existing app TLS material on ${PRIMARY_ALIAS} does not match ${FUGUE_APP_BASE_DOMAIN}; deleting stale cert files and using on-demand TLS for ${FUGUE_APP_BASE_DOMAIN}"
    purge_stale_app_tls_material="true"
  elif [[ -n "${FUGUE_APP_BASE_DOMAIN}" ]] && has_app_tls_material; then
    log "wildcard app TLS cert does not match ${FUGUE_APP_BASE_DOMAIN}; deleting stale remote cert files and using on-demand TLS for ${FUGUE_APP_BASE_DOMAIN}"
    purge_stale_app_tls_material="true"
  fi

  local mesh_site_block=""
  if mesh_enabled; then
    mesh_site_block="$(cat <<EOF
https://${FUGUE_MESH_DOMAIN} {
  ${app_host_tls_directive}
  encode gzip zstd
  reverse_proxy ${HEADSCALE_EDGE_UPSTREAM}
}
EOF
)"
  fi

  log "installing Route A edge proxy on ${PRIMARY_ALIAS} for ${FUGUE_DOMAIN}, ${FUGUE_REGISTRY_DOMAIN}, *.${FUGUE_APP_BASE_DOMAIN}, and verified custom app domains"
  ssh_root "${PRIMARY_ALIAS}" <<EOF
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
if ! command -v caddy >/dev/null 2>&1; then
  apt-get update
  apt-get install -y caddy
fi
if ! command -v python3 >/dev/null 2>&1; then
  apt-get update
  apt-get install -y python3
fi
if ! command -v openssl >/dev/null 2>&1; then
  apt-get update
  apt-get install -y openssl
fi
mkdir -p /etc/caddy
mkdir -p /etc/caddy/tls
if [ "${app_tls_uploaded}" = "true" ] && [ -f "${REMOTE_TMP_BASE}/cloudflare-apps-origin.crt" ] && [ -f "${REMOTE_TMP_BASE}/cloudflare-apps-origin.key" ]; then
  install -m 0644 "${REMOTE_TMP_BASE}/cloudflare-apps-origin.crt" /etc/caddy/tls/cloudflare-apps-origin.crt
  install -m 0640 "${REMOTE_TMP_BASE}/cloudflare-apps-origin.key" /etc/caddy/tls/cloudflare-apps-origin.key
  chown root:caddy /etc/caddy/tls/cloudflare-apps-origin.crt /etc/caddy/tls/cloudflare-apps-origin.key
elif [ "${purge_stale_app_tls_material}" = "true" ]; then
  rm -f /etc/caddy/tls/cloudflare-apps-origin.crt /etc/caddy/tls/cloudflare-apps-origin.key
  rm -f "${REMOTE_TMP_BASE}/cloudflare-apps-origin.crt" "${REMOTE_TMP_BASE}/cloudflare-apps-origin.key"
fi
cat >/etc/default/fugue-custom-domains-sync <<'EDGEENV'
EDGE_UPSTREAM=${EDGE_UPSTREAM}
FUGUE_EDGE_TLS_ASK_TOKEN=${FUGUE_EDGE_TLS_ASK_TOKEN}
EDGE_DOMAINS_URL=http://${EDGE_UPSTREAM}/v1/edge/domains?token=${FUGUE_EDGE_TLS_ASK_TOKEN}
EDGE_TLS_REPORT_URL=http://${EDGE_UPSTREAM}/v1/edge/domains/tls-report?token=${FUGUE_EDGE_TLS_ASK_TOKEN}
EDGE_TLS_PROBE_ADDR=127.0.0.1:443
EDGE_CUSTOM_DOMAINS_CADDYFILE=/etc/caddy/fugue-custom-domains.caddy
EDGE_MAIN_CADDYFILE=/etc/caddy/Caddyfile
EDGE_CUSTOM_DOMAIN_UPSTREAM=${EDGE_UPSTREAM}
EDGE_DOMAINS_CACHE_JSON=/var/lib/fugue/edge/domains-cache.json
EDGEENV
cat >/usr/local/bin/fugue-sync-custom-domains <<'SYNC'
#!/usr/bin/env bash
set -euo pipefail

config_file="/etc/default/fugue-custom-domains-sync"
if [ ! -r "\${config_file}" ]; then
  exit 0
fi
# shellcheck disable=SC1091
source "\${config_file}"

tmp_json="\$(mktemp)"
tmp_caddy="\$(mktemp)"
tmp_hosts="\$(mktemp)"
tmp_main="\$(mktemp)"
previous_custom=""
cleanup() {
  rm -f "\${tmp_json}" "\${tmp_caddy}" "\${tmp_hosts}" "\${tmp_main}"
  if [ -n "\${previous_custom}" ]; then
    rm -f "\${previous_custom}"
  fi
}
trap cleanup EXIT

mkdir -p "\$(dirname "\${EDGE_DOMAINS_CACHE_JSON}")"
if curl -fsS "\${EDGE_DOMAINS_URL}" -o "\${tmp_json}"; then
  install -m 0644 "\${tmp_json}" "\${EDGE_DOMAINS_CACHE_JSON}"
elif [ -s "\${EDGE_DOMAINS_CACHE_JSON}" ]; then
  echo "warning: Fugue edge domains endpoint is unavailable; using cached domain bundle" >&2
  cp "\${EDGE_DOMAINS_CACHE_JSON}" "\${tmp_json}"
else
  echo "Fugue edge domains endpoint is unavailable and no cached domain bundle exists" >&2
  exit 1
fi
python3 - "\${tmp_json}" "\${tmp_caddy}" "\${tmp_hosts}" "\${EDGE_CUSTOM_DOMAIN_UPSTREAM}" <<'PY'
import json
import sys

src, dst, hosts_path, upstream = sys.argv[1:5]
with open(src, "r", encoding="utf-8") as handle:
    payload = json.load(handle)

domains = []
for item in payload.get("domains", []):
    hostname = (item.get("hostname") or "").strip().lower().strip(".")
    if hostname and hostname not in domains:
        domains.append(hostname)
domains.sort()

with open(dst, "w", encoding="utf-8") as handle:
    handle.write("# Managed by fugue-sync-custom-domains\n")
    for hostname in domains:
        handle.write(f"\nhttps://{hostname} {{\n")
        handle.write("  tls internal\n")
        handle.write("  @sse header Accept *text/event-stream*\n")
        handle.write("  @stream path /stream */stream\n")
        handle.write("  @compress method GET HEAD\n")
        handle.write("  handle @sse {\n")
        handle.write(f"    reverse_proxy {upstream} {{\n")
        handle.write("      flush_interval -1\n")
        handle.write("    }\n")
        handle.write("  }\n")
        handle.write("  handle @stream {\n")
        handle.write(f"    reverse_proxy {upstream} {{\n")
        handle.write("      flush_interval -1\n")
        handle.write("    }\n")
        handle.write("  }\n")
        handle.write("  handle @compress {\n")
        handle.write("    encode gzip zstd\n")
        handle.write(f"    reverse_proxy {upstream}\n")
        handle.write("  }\n")
        handle.write("  handle {\n")
        handle.write(f"    reverse_proxy {upstream} {{\n")
        handle.write("      flush_interval -1\n")
        handle.write("    }\n")
        handle.write("  }\n")
        handle.write("}\n")

with open(hosts_path, "w", encoding="utf-8") as handle:
    for hostname in domains:
        handle.write(hostname + "\n")
PY

report_tls_status() {
  local hostname="\$1"
  local tls_status="\$2"
  local tls_last_message="\$3"

  python3 - "\${hostname}" "\${tls_status}" "\${tls_last_message}" <<'PY' | \
    curl -fsS -X POST -H 'Content-Type: application/json' --data-binary @- "\${EDGE_TLS_REPORT_URL}" >/dev/null
import json
import sys

hostname, tls_status, tls_last_message = sys.argv[1:4]
payload = {
    "hostname": hostname,
    "tls_status": tls_status,
    "tls_last_message": tls_last_message,
}
sys.stdout.write(json.dumps(payload))
PY
}

probe_tls_ready() {
  local hostname="\$1"
  local output
  local leaf_cert
  output="\$(printf '' | openssl s_client -showcerts -servername "\${hostname}" -connect "\${EDGE_TLS_PROBE_ADDR}" 2>&1 || true)"
  leaf_cert="\$(printf '%s\n' "\${output}" | awk '
    /-----BEGIN CERTIFICATE-----/ { capture = 1 }
    capture { print }
    /-----END CERTIFICATE-----/ && capture { exit }
  ')"
  if [ -z "\${leaf_cert}" ]; then
    return 1
  fi
  if printf '%s\n' "\${leaf_cert}" | openssl x509 -noout -checkhost "\${hostname}" >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

render_candidate_main_caddyfile() {
  python3 - "\${EDGE_MAIN_CADDYFILE}" "\${EDGE_CUSTOM_DOMAINS_CADDYFILE}" "\${tmp_caddy}" "\${tmp_main}" <<'PY'
import sys
from pathlib import Path

main_path, current_import, candidate_import, out_path = sys.argv[1:5]
source = Path(main_path).read_text(encoding="utf-8")
needle = f"import {current_import}"
replacement = f"import {candidate_import}"
if needle not in source:
    sys.stderr.write(f"expected import line not found in {main_path}: {needle}\n")
    sys.exit(1)
Path(out_path).write_text(source.replace(needle, replacement, 1), encoding="utf-8")
PY
}

reload_caddy_config() {
  caddy reload --config "\${EDGE_MAIN_CADDYFILE}" --adapter caddyfile
}

if [ ! -f "\${EDGE_CUSTOM_DOMAINS_CADDYFILE}" ] || ! cmp -s "\${tmp_caddy}" "\${EDGE_CUSTOM_DOMAINS_CADDYFILE}"; then
  render_candidate_main_caddyfile
  caddy validate --config "\${tmp_main}" --adapter caddyfile
  previous_custom="\$(mktemp)"
  had_previous_custom=false
  if [ -f "\${EDGE_CUSTOM_DOMAINS_CADDYFILE}" ]; then
    cp -p "\${EDGE_CUSTOM_DOMAINS_CADDYFILE}" "\${previous_custom}"
    had_previous_custom=true
  fi
  install -m 0644 "\${tmp_caddy}" "\${EDGE_CUSTOM_DOMAINS_CADDYFILE}"
  caddy validate --config "\${EDGE_MAIN_CADDYFILE}" --adapter caddyfile
  if systemctl is-active --quiet caddy; then
    if ! reload_caddy_config; then
      echo "warning: caddy reload failed after custom-domain update; restoring previous custom-domain config" >&2
      if [ "\${had_previous_custom}" = "true" ]; then
        install -m 0644 "\${previous_custom}" "\${EDGE_CUSTOM_DOMAINS_CADDYFILE}"
      else
        rm -f "\${EDGE_CUSTOM_DOMAINS_CADDYFILE}"
      fi
      caddy validate --config "\${EDGE_MAIN_CADDYFILE}" --adapter caddyfile >/dev/null 2>&1 || true
      reload_caddy_config >/dev/null 2>&1 || true
      exit 1
    fi
  fi
  rm -f "\${previous_custom}"
  previous_custom=""
fi

while IFS= read -r hostname; do
  if [ -z "\${hostname}" ]; then
    continue
  fi
  if probe_tls_ready "\${hostname}"; then
    report_tls_status "\${hostname}" "ready" "" || echo "warning: failed to report ready TLS status for \${hostname}" >&2
  else
    report_tls_status "\${hostname}" "pending" "waiting for edge certificate issuance" || echo "warning: failed to report pending TLS status for \${hostname}" >&2
  fi
done < "\${tmp_hosts}"
SYNC
chmod 0755 /usr/local/bin/fugue-sync-custom-domains
cat >/etc/caddy/fugue-custom-domains.caddy <<'CUSTOMDOMAINS'
# Managed by fugue-sync-custom-domains
CUSTOMDOMAINS
cat >/etc/systemd/system/fugue-custom-domains-sync.service <<'SERVICE'
[Unit]
Description=Sync Fugue custom domains into Caddy
After=network-online.target caddy.service
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/usr/local/bin/fugue-sync-custom-domains
SERVICE
cat >/etc/systemd/system/fugue-custom-domains-sync.timer <<'TIMER'
[Unit]
Description=Periodically sync Fugue custom domains into Caddy

[Timer]
OnBootSec=30s
OnUnitActiveSec=60s
Unit=fugue-custom-domains-sync.service

[Install]
WantedBy=timers.target
TIMER
cat >/etc/caddy/Caddyfile <<'CADDY'
{
  admin 127.0.0.1:2019
  on_demand_tls {
    ask http://${EDGE_UPSTREAM}/v1/edge/tls/ask?token=${FUGUE_EDGE_TLS_ASK_TOKEN}
  }
}

https://${FUGUE_DOMAIN} {
  tls internal
  @sse header Accept *text/event-stream*
  @stream path /stream */stream
  @compress method GET HEAD
  handle @sse {
    reverse_proxy ${EDGE_UPSTREAM} {
      flush_interval -1
    }
  }
  handle @stream {
    reverse_proxy ${EDGE_UPSTREAM} {
      flush_interval -1
    }
  }
  handle @compress {
    encode gzip zstd
    reverse_proxy ${EDGE_UPSTREAM}
  }
  handle {
    reverse_proxy ${EDGE_UPSTREAM} {
      flush_interval -1
    }
  }
}

https://${FUGUE_REGISTRY_DOMAIN} {
  tls internal
  encode gzip zstd
  reverse_proxy ${REGISTRY_EDGE_UPSTREAM}
}

${mesh_site_block}

https://*.${FUGUE_APP_BASE_DOMAIN} {
  ${app_host_tls_directive}
  @sse header Accept *text/event-stream*
  @stream path /stream */stream
  @compress method GET HEAD
  handle @sse {
    reverse_proxy ${EDGE_UPSTREAM} {
      flush_interval -1
    }
  }
  handle @stream {
    reverse_proxy ${EDGE_UPSTREAM} {
      flush_interval -1
    }
  }
  handle @compress {
    encode gzip zstd
    reverse_proxy ${EDGE_UPSTREAM}
  }
  handle {
    reverse_proxy ${EDGE_UPSTREAM} {
      flush_interval -1
    }
  }
}

import /etc/caddy/fugue-custom-domains.caddy

https:// {
  tls {
    on_demand
  }
  @sse header Accept *text/event-stream*
  @stream path /stream */stream
  @compress method GET HEAD
  handle @sse {
    reverse_proxy ${EDGE_UPSTREAM} {
      flush_interval -1
    }
  }
  handle @stream {
    reverse_proxy ${EDGE_UPSTREAM} {
      flush_interval -1
    }
  }
  handle @compress {
    encode gzip zstd
    reverse_proxy ${EDGE_UPSTREAM}
  }
  handle {
    reverse_proxy ${EDGE_UPSTREAM} {
      flush_interval -1
    }
  }
}
CADDY
caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile
systemctl daemon-reload
systemctl enable caddy
if systemctl is-active --quiet caddy; then
  if ! caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile; then
    echo "warning: caddy hot reload failed; falling back to a one-time restart" >&2
    systemctl restart caddy
  fi
else
  systemctl start caddy
fi
systemctl is-active --quiet caddy
systemctl enable fugue-custom-domains-sync.timer
systemctl restart fugue-custom-domains-sync.timer
/usr/local/bin/fugue-sync-custom-domains
EOF
}

verify_edge_proxy_config_on_primary() {
  if [[ -z "${FUGUE_DOMAIN}" ]]; then
    return
  fi

  local expected_sites=(
    "on_demand_tls {"
    "https:// {"
    "https://*.${FUGUE_APP_BASE_DOMAIN} {"
    "import /etc/caddy/fugue-custom-domains.caddy"
  )
  local expected_mesh_domain=""
  expected_mesh_domain="$(expected_edge_proxy_mesh_domain || true)"
  if [[ -n "${expected_mesh_domain}" ]]; then
    expected_sites+=("https://${expected_mesh_domain} {")
  fi

  local site_pattern=""
  for site_pattern in "${expected_sites[@]}"; do
    if ! ssh_root_run "${PRIMARY_ALIAS}" "grep -F $(printf '%q' "${site_pattern}") /etc/caddy/Caddyfile >/dev/null"; then
      fail "Route A edge config on ${PRIMARY_ALIAS} is missing expected entry: ${site_pattern}"
    fi
  done
}

determine_edge_upstream() {
  EDGE_UPSTREAM="${K3S_API_IP}:${API_NODEPORT}"
  EDGE_UPSTREAM_MODE="nodeport"

  local cluster_ip=""
  cluster_ip="$(ssh_root_run "${PRIMARY_ALIAS}" "KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl -n $(printf '%q' "${NAMESPACE}") get svc $(printf '%q' "${RELEASE_FULLNAME}") -o jsonpath='{.spec.clusterIP}'" | tr -d '\r' || true)"
  if [[ -n "${cluster_ip}" && "${cluster_ip}" != "<none>" ]]; then
    if ssh_root_run "${PRIMARY_ALIAS}" "curl -fsS --max-time 10 http://$(printf '%q' "${cluster_ip}"):80/healthz >/dev/null"; then
      EDGE_UPSTREAM="${cluster_ip}:80"
      EDGE_UPSTREAM_MODE="service-clusterip"
      log "selected edge upstream via ClusterIP: ${EDGE_UPSTREAM}"
      return
    fi
    log "warning: ClusterIP ${cluster_ip}:80 exists but is not reachable from ${PRIMARY_ALIAS}; falling back to NodePort"
  fi

  if ssh_root_run "${PRIMARY_ALIAS}" "curl -fsS --max-time 10 http://$(printf '%q' "${K3S_API_IP}"):$(printf '%q' "${API_NODEPORT}")/healthz >/dev/null"; then
    log "selected edge upstream via NodePort: ${EDGE_UPSTREAM}"
    return
  fi

  log "warning: neither ClusterIP nor NodePort health checks succeeded from ${PRIMARY_ALIAS}; keeping NodePort upstream ${EDGE_UPSTREAM}"
}

determine_registry_upstream() {
  REGISTRY_EDGE_UPSTREAM="${K3S_API_IP}:${REGISTRY_NODEPORT}"

  local cluster_ip=""
  cluster_ip="$(ssh_root_run "${PRIMARY_ALIAS}" "KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl -n $(printf '%q' "${NAMESPACE}") get svc $(printf '%q' "${REGISTRY_DEPLOYMENT_NAME}") -o jsonpath='{.spec.clusterIP}'" | tr -d '\r' || true)"
  if [[ -n "${cluster_ip}" && "${cluster_ip}" != "<none>" ]]; then
    if ssh_root_run "${PRIMARY_ALIAS}" "curl -fsS --max-time 10 http://$(printf '%q' "${cluster_ip}"):5000/v2/ >/dev/null"; then
      REGISTRY_EDGE_UPSTREAM="${cluster_ip}:5000"
      log "selected registry upstream via ClusterIP: ${REGISTRY_EDGE_UPSTREAM}"
      return
    fi
    log "warning: registry ClusterIP ${cluster_ip}:5000 exists but is not reachable from ${PRIMARY_ALIAS}; falling back to NodePort"
  fi

  if ssh_root_run "${PRIMARY_ALIAS}" "curl -fsS --max-time 10 http://$(printf '%q' "${K3S_API_IP}"):$(printf '%q' "${REGISTRY_NODEPORT}")/v2/ >/dev/null"; then
    log "selected registry upstream via NodePort: ${REGISTRY_EDGE_UPSTREAM}"
    return
  fi

  log "warning: neither registry ClusterIP nor NodePort health checks succeeded from ${PRIMARY_ALIAS}; keeping NodePort upstream ${REGISTRY_EDGE_UPSTREAM}"
}

determine_headscale_upstream() {
  if ! mesh_enabled; then
    HEADSCALE_EDGE_UPSTREAM=""
    return
  fi
  HEADSCALE_EDGE_UPSTREAM="${K3S_API_IP}:${HEADSCALE_NODEPORT}"
}

check_edge_origin_health() {
  if [[ -z "${FUGUE_DOMAIN}" ]]; then
    EDGE_LOCAL_HEALTH="unknown"
    return
  fi

  local resolve_arg="${FUGUE_DOMAIN}:443:127.0.0.1"
  local url="https://${FUGUE_DOMAIN}/healthz"

  if ssh_root_run "${PRIMARY_ALIAS}" "curl -ksS --max-time 10 --resolve $(printf '%q' "${resolve_arg}") $(printf '%q' "${url}") >/dev/null"; then
    EDGE_LOCAL_HEALTH="true"
    log "verified local Route A edge origin on ${PRIMARY_ALIAS} for ${FUGUE_DOMAIN}"
    return
  fi

  EDGE_LOCAL_HEALTH="false"
  log "warning: local Route A edge proxy health check failed on ${PRIMARY_ALIAS}"
}

write_primary_config() {
  local registry_pull_base registry_mirror_endpoint registry_mirror_endpoint_url
  registry_pull_base="$(registry_pull_base_value)"
  registry_mirror_endpoint="$(node_registry_mirror_endpoint_value)"
  registry_mirror_endpoint_url="$(registry_endpoint_url_value "${registry_mirror_endpoint}")"
  ssh_root "${PRIMARY_ALIAS}" <<EOF
set -euo pipefail
mkdir -p /etc/rancher/k3s
cat >/etc/rancher/k3s/config.yaml <<'CFG'
cluster-init: true
write-kubeconfig-mode: "644"
$(render_server_network_config "${PRIMARY_ALIAS}")
$(render_tls_sans_for_host "${PRIMARY_ALIAS}")
disable:
  - traefik
  - servicelb
$(render_server_node_labels "${PRIMARY_ALIAS}" "primary")
CFG
cat >/etc/rancher/k3s/registries.yaml <<'REG'
mirrors:
  "${registry_pull_base}":
    endpoint:
      - "${registry_mirror_endpoint_url}"
configs:
  "${registry_pull_base}":
    tls:
      insecure_skip_verify: true
REG
if ! command -v k3s >/dev/null 2>&1; then
  curl -sfL https://get.k3s.io | INSTALL_K3S_CHANNEL='${K3S_CHANNEL}' INSTALL_K3S_EXEC='server' sh -
else
  systemctl enable k3s
  systemctl restart k3s
fi
systemctl is-active --quiet k3s
EOF
}

wait_for_primary_token() {
  local tries token
  for tries in $(seq 1 60); do
    token="$(ssh_root_run "${PRIMARY_ALIAS}" "test -f /var/lib/rancher/k3s/server/token && cat /var/lib/rancher/k3s/server/token || true")"
    if [[ -n "${token}" ]]; then
      printf '%s' "${token}"
      return 0
    fi
    sleep 2
  done
  dump_remote_k3s_debug "${PRIMARY_ALIAS}"
  fail "timed out waiting for primary k3s token"
}

wait_for_primary_node_token() {
  local tries token
  for tries in $(seq 1 60); do
    token="$(ssh_root_run "${PRIMARY_ALIAS}" "if [ -f /var/lib/rancher/k3s/server/node-token ]; then cat /var/lib/rancher/k3s/server/node-token; elif [ -f /var/lib/rancher/k3s/server/token ]; then cat /var/lib/rancher/k3s/server/token; fi")"
    if [[ -n "${token}" ]]; then
      printf '%s' "${token}"
      return 0
    fi
    sleep 2
  done
  dump_remote_k3s_debug "${PRIMARY_ALIAS}"
  fail "timed out waiting for primary k3s node token"
}

write_secondary_config() {
  local host="$1"
  local role="$2"
  local token="$3"
  local registry_pull_base registry_mirror_endpoint registry_mirror_endpoint_url
  registry_pull_base="$(registry_pull_base_value)"
  registry_mirror_endpoint="$(node_registry_mirror_endpoint_value)"
  registry_mirror_endpoint_url="$(registry_endpoint_url_value "${registry_mirror_endpoint}")"
  ssh_root "${host}" <<EOF
set -euo pipefail
mkdir -p /etc/rancher/k3s
cat >/etc/rancher/k3s/config.yaml <<'CFG'
server: "https://${K3S_API_IP}:6443"
token: "${token}"
write-kubeconfig-mode: "644"
$(render_server_network_config "${host}")
$(render_tls_sans_for_host "${host}")
disable:
  - traefik
  - servicelb
$(render_server_node_labels "${host}" "${role}")
CFG
cat >/etc/rancher/k3s/registries.yaml <<'REG'
mirrors:
  "${registry_pull_base}":
    endpoint:
      - "${registry_mirror_endpoint_url}"
configs:
  "${registry_pull_base}":
    tls:
      insecure_skip_verify: true
REG
if ! command -v k3s >/dev/null 2>&1; then
  curl -sfL https://get.k3s.io | INSTALL_K3S_CHANNEL='${K3S_CHANNEL}' INSTALL_K3S_EXEC='server' sh -
else
  systemctl enable k3s
  systemctl restart k3s
fi
systemctl is-active --quiet k3s
EOF
}

install_k3s_cluster() {
  log "installing primary k3s server on ${PRIMARY_ALIAS}"
  write_primary_config

  log "waiting for primary server token"
  local token
  token="$(wait_for_primary_token)"

  log "joining secondary k3s servers"
  write_secondary_config "${SECONDARY_ALIASES[0]}" "secondary-1" "${token}"
  write_secondary_config "${SECONDARY_ALIASES[1]}" "secondary-2" "${token}"
}

wait_for_cluster_ready() {
  log "waiting for all nodes to become Ready"
  if ssh_root "${PRIMARY_ALIAS}" <<'EOF'
set -euo pipefail
for _ in $(seq 1 90); do
  ready_count="$(k3s kubectl get nodes --no-headers 2>/dev/null | awk '$2 == "Ready" {count++} END {print count+0}')"
  total_count="$(k3s kubectl get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ')"
  if [ "${ready_count}" -ge 3 ] && [ "${total_count}" -ge 3 ]; then
    exit 0
  fi
  sleep 2
done
echo "cluster did not become ready in time" >&2
exit 1
EOF
  then
    return 0
  fi
  for host in "${ALL_ALIASES[@]}"; do
    dump_remote_k3s_debug "${host}"
  done
  ssh_root "${PRIMARY_ALIAS}" <<'EOF' || true
set -euo pipefail
echo '===== kubectl get nodes -o wide ====='
k3s kubectl get nodes -o wide || true
EOF
  fail "cluster did not become ready in time"
}

cluster_is_ready() {
  ssh_root "${PRIMARY_ALIAS}" <<'EOF'
set -euo pipefail
command -v k3s >/dev/null 2>&1
ready_count="$(k3s kubectl get nodes --no-headers 2>/dev/null | awk '$2 == "Ready" {count++} END {print count+0}')"
total_count="$(k3s kubectl get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ')"
[ "${ready_count}" -ge 3 ] && [ "${total_count}" -ge 3 ]
EOF
}

ensure_coredns_multinode_scheduling() {
  log "ensuring CoreDNS stays on installer-managed control-plane nodes"
  ssh_root "${PRIMARY_ALIAS}" <<'EOF'
set -euo pipefail

desired_replicas="${FUGUE_COREDNS_TARGET_REPLICAS:-2}"
if ! [[ "${desired_replicas}" =~ ^[0-9]+$ ]] || (( desired_replicas <= 0 )); then
  echo "invalid FUGUE_COREDNS_TARGET_REPLICAS=${desired_replicas}" >&2
  exit 1
fi

if ! k3s kubectl -n kube-system get deploy coredns >/dev/null 2>&1; then
  exit 0
fi

count_ready_nodes() {
  local selector="$1"
  k3s kubectl get nodes -l "${selector}" --no-headers 2>/dev/null | \
    awk '$2 == "Ready" || $2 ~ /^Ready,/ {count++} END {print count+0}'
}

coredns_selector_key="fugue.install/profile"
coredns_selector_value="combined"
# Keep CoreDNS off worker nodes so pod DNS does not vary with worker host resolvers.
ready_coredns_nodes="$(count_ready_nodes "kubernetes.io/os=linux,${coredns_selector_key}=${coredns_selector_value}")"
if ! [[ "${ready_coredns_nodes}" =~ ^[0-9]+$ ]] || (( ready_coredns_nodes == 0 )); then
  coredns_selector_key="node-role.kubernetes.io/control-plane"
  coredns_selector_value="true"
  ready_coredns_nodes="$(count_ready_nodes "kubernetes.io/os=linux,${coredns_selector_key}=${coredns_selector_value}")"
fi
if ! [[ "${ready_coredns_nodes}" =~ ^[0-9]+$ ]] || (( ready_coredns_nodes == 0 )); then
  echo "no Ready linux control-plane nodes available for CoreDNS scheduling" >&2
  exit 1
fi
if (( desired_replicas > ready_coredns_nodes )); then
  desired_replicas="${ready_coredns_nodes}"
fi
if (( desired_replicas < 1 )); then
  desired_replicas=1
fi

current_replicas="$(k3s kubectl -n kube-system get deploy coredns -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
current_profile_selector="$(k3s kubectl -n kube-system get deploy coredns -o jsonpath='{.spec.template.spec.nodeSelector.fugue\.install/profile}' 2>/dev/null || true)"
current_control_plane_selector="$(k3s kubectl -n kube-system get deploy coredns -o jsonpath='{.spec.template.spec.nodeSelector.node-role\.kubernetes\.io/control-plane}' 2>/dev/null || true)"
current_os_selector="$(k3s kubectl -n kube-system get deploy coredns -o jsonpath='{.spec.template.spec.nodeSelector.kubernetes\.io/os}' 2>/dev/null || true)"

if [[ "${current_replicas}" == "${desired_replicas}" ]] && [[ "${current_os_selector}" == "linux" ]]; then
  if [[ "${coredns_selector_key}" == "fugue.install/profile" ]] && [[ "${current_profile_selector}" == "${coredns_selector_value}" ]]; then
    exit 0
  fi
  if [[ "${coredns_selector_key}" == "node-role.kubernetes.io/control-plane" ]] && [[ "${current_control_plane_selector}" == "${coredns_selector_value}" ]]; then
    exit 0
  fi
fi

patch_payload="$(cat <<EOF_PATCH
[
  {"op":"add","path":"/spec/replicas","value":${desired_replicas}},
  {"op":"add","path":"/spec/template/spec/nodeSelector","value":{"kubernetes.io/os":"linux","${coredns_selector_key}":"${coredns_selector_value}"}}
]
EOF_PATCH
)"

k3s kubectl -n kube-system patch deploy coredns --type=json -p "${patch_payload}" >/dev/null
k3s kubectl -n kube-system rollout status deploy/coredns --timeout=180s
EOF
}

label_control_plane_node() {
  local host="$1"
  local role="$2"
  local node_name=""
  local country_code=""
  local public_ip=""
  local zone=""
  local region=""

  node_name="$(ssh_run "${host}" "hostname" | tr -d '\r')"
  [[ -n "${node_name}" ]] || fail "failed to detect Kubernetes node name for ${host}"
  country_code="$(node_country_code_for_alias "${host}" || true)"
  public_ip="$(node_public_ip_for_alias "${host}" || true)"
  zone="$(node_zone_for_alias "${host}" || true)"
  region="$(node_region_for_alias "${host}" || true)"

  ssh_root_run "${PRIMARY_ALIAS}" "k3s kubectl label node $(printf '%q' "${node_name}") $(printf '%q' "fugue.install/profile=combined") $(printf '%q' "fugue.install/role=${role}") --overwrite"
  if [[ "${role}" == "primary" ]]; then
    ssh_root_run "${PRIMARY_ALIAS}" "k3s kubectl label node $(printf '%q' "${node_name}") $(printf '%q' "fugue.io/shared-pool-") $(printf '%q' "fugue.io/build-") $(printf '%q' "fugue.io/build-tier-") --overwrite" || true
  else
    ssh_root_run "${PRIMARY_ALIAS}" "k3s kubectl label node $(printf '%q' "${node_name}") $(printf '%q' "fugue.io/shared-pool=internal") $(printf '%q' "fugue.io/build=true") $(printf '%q' "fugue.io/build-tier-") --overwrite"
  fi
  if [[ -n "${country_code}" ]]; then
    ssh_root_run "${PRIMARY_ALIAS}" "k3s kubectl label node $(printf '%q' "${node_name}") $(printf '%q' "fugue.io/location-country-code=${country_code}") --overwrite"
  fi
  if [[ -n "${public_ip}" ]]; then
    ssh_root_run "${PRIMARY_ALIAS}" "k3s kubectl label node $(printf '%q' "${node_name}") $(printf '%q' "fugue.io/public-ip=${public_ip}") --overwrite"
  fi
  if [[ -n "${region}" ]]; then
    ssh_root_run "${PRIMARY_ALIAS}" "k3s kubectl label node $(printf '%q' "${node_name}") $(printf '%q' "topology.kubernetes.io/region=${region}") --overwrite"
  fi
  if [[ -n "${zone}" ]]; then
    ssh_root_run "${PRIMARY_ALIAS}" "k3s kubectl label node $(printf '%q' "${node_name}") $(printf '%q' "topology.kubernetes.io/zone=${zone}") --overwrite"
  fi
}

label_control_plane_nodes() {
  log "labeling control-plane nodes for pinned scheduling and topology"
  label_control_plane_node "${PRIMARY_ALIAS}" "primary"
  label_control_plane_node "${SECONDARY_ALIASES[0]}" "secondary-1"
  label_control_plane_node "${SECONDARY_ALIASES[1]}" "secondary-2"
}

cleanup_disabled_addons() {
  log "cleaning up stale disabled addon resources"
  ssh_root "${PRIMARY_ALIAS}" <<'EOF'
set -euo pipefail
KUBECTL="k3s kubectl"

# Previous cluster attempts can leave the Traefik Service behind even when
# k3s is configured with `disable: [traefik, servicelb]`. That stale
# LoadBalancer Service creates kube-proxy reject rules for :80/:443 and
# blocks our Route A edge proxy on the node.
$KUBECTL -n kube-system delete svc/traefik --ignore-not-found=true || true
$KUBECTL -n kube-system patch svc/traefik --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1 || true
$KUBECTL -n kube-system delete endpoints/traefik --ignore-not-found=true || true
$KUBECTL -n kube-system delete endpointslice -l kubernetes.io/service-name=traefik --ignore-not-found=true || true
$KUBECTL -n kube-system delete helmchart.helm.cattle.io/traefik --ignore-not-found=true || true
$KUBECTL -n kube-system delete helmchartconfig.helm.cattle.io/traefik --ignore-not-found=true || true
EOF
}

prepare_remote_tmp() {
  local host="$1"
  ssh_root "${host}" <<EOF
set -euo pipefail
rm -rf "${REMOTE_TMP_BASE}"
mkdir -p "${REMOTE_TMP_BASE}"
chmod 0777 "${REMOTE_TMP_BASE}"
EOF
}

push_and_import_images() {
  local host="$1"
  local api_ref="docker.io/library/fugue-api:${IMAGE_TAG}"
  local controller_ref="docker.io/library/fugue-controller:${IMAGE_TAG}"
  log "copying images to ${host}"
  prepare_remote_tmp "${host}"
  scp_to "${IMAGES_TAR}" "${host}" "${REMOTE_TMP_BASE}/fugue-images.tar"
  run_with_retry "${REMOTE_CMD_RETRIES}" "${REMOTE_CMD_RETRY_DELAY}" "import images on ${host}" \
    ssh_root_run "${host}" "k3s ctr images import $(printf '%q' "${REMOTE_TMP_BASE}/fugue-images.tar")"
  run_with_retry "${REMOTE_CMD_RETRIES}" "${REMOTE_CMD_RETRY_DELAY}" "verify imported images on ${host}" \
    ssh_root_run "${host}" "k3s ctr images ls | grep -F $(printf '%q' "${api_ref}") >/dev/null && k3s ctr images ls | grep -F $(printf '%q' "${controller_ref}") >/dev/null"
}

install_helm_on_primary() {
  log "ensuring helm is installed on ${PRIMARY_ALIAS}"
  ssh_root "${PRIMARY_ALIAS}" <<'EOF'
set -euo pipefail
if ! command -v helm >/dev/null 2>&1; then
  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
fi
EOF
}

write_values_override() {
  local cluster_join_server
  cluster_join_server="$(cluster_join_server_value)"
  local registry_push_base
  registry_push_base="$(registry_push_base_value)"
  local registry_pull_base
  registry_pull_base="$(registry_pull_base_value)"
  local cluster_join_registry_endpoint
  cluster_join_registry_endpoint="$(cluster_join_registry_endpoint_value)"
  local cluster_join_ca_hash="${FUGUE_CLUSTER_JOIN_CA_HASH:-}"
  local cluster_join_mesh_provider=""
  local cluster_join_mesh_login_server=""
  local cluster_join_mesh_auth_key=""
  if mesh_enabled && [[ -n "${FUGUE_MESH_AUTH_KEY}" ]]; then
    cluster_join_mesh_provider="${FUGUE_MESH_PROVIDER}"
    cluster_join_mesh_login_server="$(mesh_login_server)"
    cluster_join_mesh_auth_key="${FUGUE_MESH_AUTH_KEY}"
  fi

  cat >"${VALUES_FILE}" <<EOF
bootstrapAdminKey: "${BOOTSTRAP_KEY}"

api:
  replicaCount: 2
  image:
    repository: fugue-api
    tag: "${IMAGE_TAG}"
    pullPolicy: IfNotPresent
  appBaseDomain: "${FUGUE_APP_BASE_DOMAIN}"
  apiPublicDomain: "${FUGUE_DOMAIN}"
  edgeTLSAskToken: "${FUGUE_EDGE_TLS_ASK_TOKEN}"
  registryPushBase: "${registry_push_base}"
  registryPullBase: "${registry_pull_base}"
  clusterJoinRegistryEndpoint: "${cluster_join_registry_endpoint}"
  clusterJoinServer: "${cluster_join_server}"
  clusterJoinCAHash: "${cluster_join_ca_hash}"
  clusterJoinBootstrapTokenTTL: "15m"
  clusterJoinMeshProvider: "${cluster_join_mesh_provider}"
  clusterJoinMeshLoginServer: "${cluster_join_mesh_login_server}"
  clusterJoinMeshAuthKey: "${cluster_join_mesh_auth_key}"
  importWorkDir: "/var/lib/fugue/import"
  hostNetwork: false
  minReadySeconds: 5
  terminationGracePeriodSeconds: 40
  shutdownDrainDelay: "5s"
  shutdownTimeout: "25s"
  nodeSelector:
    "node-role.kubernetes.io/control-plane": "true"
  tolerations:
    - key: node-role.kubernetes.io/control-plane
      operator: Exists
      effect: NoSchedule
    - key: node.kubernetes.io/not-ready
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
    - key: node.kubernetes.io/unreachable
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
  resources:
    requests:
      cpu: 100m
      memory: 128Mi
    limits:
      cpu: 500m
      memory: 512Mi

controller:
  replicaCount: 2
  image:
    repository: fugue-controller
    tag: "${IMAGE_TAG}"
    pullPolicy: IfNotPresent
  fallbackPollInterval: "30s"
  kubectlApply: true
  terminationGracePeriodSeconds: 30
  leaderElection:
    enabled: true
    leaseName: "${CONTROLLER_DEPLOYMENT_NAME}"
    leaseNamespace: "${NAMESPACE}"
    leaseDuration: "15s"
    renewDeadline: "10s"
    retryPeriod: "2s"
  migrationGuard:
    legacyControllerContainerName: "controller"
    checkInterval: "2s"
  nodeSelector:
    "node-role.kubernetes.io/control-plane": "true"
  tolerations:
    - key: node-role.kubernetes.io/control-plane
      operator: Exists
      effect: NoSchedule
    - key: node.kubernetes.io/not-ready
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
    - key: node.kubernetes.io/unreachable
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
  resources:
    requests:
      cpu: 100m
      memory: 128Mi
    limits:
      cpu: 500m
      memory: 512Mi

snapshotController:
  nodeSelector:
    "node-role.kubernetes.io/control-plane": "true"
  tolerations:
    - key: node-role.kubernetes.io/control-plane
      operator: Exists
      effect: NoSchedule
    - key: node.kubernetes.io/not-ready
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
    - key: node.kubernetes.io/unreachable
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300

cloudnative-pg:
  replicaCount: 2
  priorityClassName: system-cluster-critical
  nodeSelector:
    "node-role.kubernetes.io/control-plane": "true"
  tolerations:
    - key: node-role.kubernetes.io/control-plane
      operator: Exists
      effect: NoSchedule
    - key: node.kubernetes.io/not-ready
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
    - key: node.kubernetes.io/unreachable
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
  resources:
    requests:
      cpu: 100m
      memory: 128Mi
    limits:
      cpu: 500m
      memory: 512Mi
  affinity:
    nodeAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
        - weight: 100
          preference:
            matchExpressions:
              - key: fugue.install/role
                operator: NotIn
                values:
                  - primary
    podAntiAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
        - weight: 100
          podAffinityTerm:
            topologyKey: kubernetes.io/hostname
            labelSelector:
              matchLabels:
                app.kubernetes.io/name: cloudnative-pg
                app.kubernetes.io/instance: fugue

service:
  type: NodePort
  port: 80
  targetPort: 8080
  nodePort: ${API_NODEPORT}

registry:
  enabled: true
  service:
    type: NodePort
    port: 5000
    targetPort: 5000
    nodePort: ${REGISTRY_NODEPORT}
  persistence:
    mode: "${REGISTRY_PERSISTENCE_MODE}"
    hostPath: "${HOSTPATH_DATA_DIR}/registry"
    existingClaim: "${REGISTRY_PERSISTENCE_EXISTING_CLAIM}"
    accessMode: "${REGISTRY_PERSISTENCE_ACCESS_MODE}"
    size: "${REGISTRY_PERSISTENCE_SIZE}"
    storageClassName: "${REGISTRY_PERSISTENCE_STORAGE_CLASS}"
  maintenance:
    uploadPurge:
      enabled: ${REGISTRY_UPLOAD_PURGE_ENABLED}
      age: "${REGISTRY_UPLOAD_PURGE_AGE}"
      interval: "${REGISTRY_UPLOAD_PURGE_INTERVAL}"
      dryRun: ${REGISTRY_UPLOAD_PURGE_DRY_RUN}
  nodeSelector:
    "fugue.install/role": primary
  resources:
    requests:
      cpu: 50m
      memory: 64Mi
    limits:
      cpu: 250m
      memory: 256Mi

headscale:
  enabled: $(if mesh_enabled; then printf 'true'; else printf 'false'; fi)
  domain: "${FUGUE_MESH_DOMAIN}"
  service:
    type: NodePort
    port: 8080
    targetPort: 8080
    nodePort: ${HEADSCALE_NODEPORT}
  persistence:
    hostPath: "${HOSTPATH_DATA_DIR}/headscale"
  nodeSelector:
    "fugue.install/role": primary
  resources:
    requests:
      cpu: 50m
      memory: 64Mi
    limits:
      cpu: 250m
      memory: 256Mi

postgres:
  enabled: true
  database: "fugue"
  username: "fugue"
  service:
    port: 5432
  persistence:
    hostPath: "${HOSTPATH_DATA_DIR}/postgres"
  nodeSelector:
    "fugue.install/role": primary
  resources:
    requests:
      cpu: 100m
      memory: 256Mi
    limits:
      cpu: 500m
      memory: 512Mi

persistence:
  mode: hostPath
  hostPath: "${HOSTPATH_DATA_DIR}"
EOF
  append_image_prepull_values
}

copy_chart_and_values() {
  log "copying Helm chart and values to ${PRIMARY_ALIAS}"
  prepare_remote_tmp "${PRIMARY_ALIAS}"
  build_chart_archive
  scp_to "${VALUES_FILE}" "${PRIMARY_ALIAS}" "${REMOTE_TMP_BASE}/values-override.yaml"
  scp_to "${CHART_TAR}" "${PRIMARY_ALIAS}" "${REMOTE_TMP_BASE}/fugue-chart.tar"
  run_with_retry "${REMOTE_CMD_RETRIES}" "${REMOTE_CMD_RETRY_DELAY}" "extract Helm chart on ${PRIMARY_ALIAS}" \
    ssh_root_run "${PRIMARY_ALIAS}" "tar -C $(printf '%q' "${REMOTE_TMP_BASE}") -xf $(printf '%q' "${REMOTE_TMP_BASE}/fugue-chart.tar") && test -f $(printf '%q' "${REMOTE_TMP_BASE}/fugue/Chart.yaml")"
}

install_fugue_chart() {
  log "installing Fugue Helm release"
  ssh_root "${PRIMARY_ALIAS}" <<EOF
set -euo pipefail
mkdir -p "${HOSTPATH_DATA_DIR}"
if [[ -d "${REMOTE_TMP_BASE}/fugue/crds" ]] && find "${REMOTE_TMP_BASE}/fugue/crds" -maxdepth 1 -type f \( -name '*.yaml' -o -name '*.yml' \) | grep -q .; then
  KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl apply -f "${REMOTE_TMP_BASE}/fugue/crds"
  KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl wait --for=condition=Established --timeout=60s -f "${REMOTE_TMP_BASE}/fugue/crds"
fi
KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm upgrade --install "${RELEASE_NAME}" "${REMOTE_TMP_BASE}/fugue" \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  --wait \
  --timeout 300s \
  -f "${REMOTE_TMP_BASE}/values-override.yaml"
KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl -n "${NAMESPACE}" rollout status deploy/"${POSTGRES_DEPLOYMENT_NAME}" --timeout=180s
KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl -n "${NAMESPACE}" rollout status deploy/"${API_DEPLOYMENT_NAME}" --timeout=180s
KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl -n "${NAMESPACE}" rollout status deploy/"${CONTROLLER_DEPLOYMENT_NAME}" --timeout=180s
KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl -n "${NAMESPACE}" rollout status deploy/"${REGISTRY_DEPLOYMENT_NAME}" --timeout=180s
if [ "$(printf '%s' "${FUGUE_MESH_ENABLED}")" = "true" ]; then
  KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl -n "${NAMESPACE}" rollout status deploy/"${HEADSCALE_DEPLOYMENT_NAME}" --timeout=180s
fi
EOF
}

mesh_hostname_for_alias() {
  local host="$1"
  printf 'fugue-%s' "${host}" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9-' '-'
}

ensure_headscale_auth_key() {
  if ! mesh_enabled; then
    return
  fi

  maybe_reuse_existing_mesh_auth_key
  if [[ -n "${FUGUE_MESH_AUTH_KEY}" ]]; then
    return
  fi

  local user_id=""
  log "creating reusable mesh auth key from ${HEADSCALE_DEPLOYMENT_NAME}"
  ssh_root_run "${PRIMARY_ALIAS}" "KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl -n $(printf '%q' "${NAMESPACE}") exec deploy/$(printf '%q' "${HEADSCALE_DEPLOYMENT_NAME}") -- headscale users create fugue >/dev/null 2>&1 || true" >/dev/null
  user_id="$(ssh_root_run "${PRIMARY_ALIAS}" "KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl -n $(printf '%q' "${NAMESPACE}") exec deploy/$(printf '%q' "${HEADSCALE_DEPLOYMENT_NAME}") -- headscale users list -o json-line" | tr -d '\r' | grep -o '\"id\":[0-9][0-9]*' | head -n 1 | cut -d: -f2)"
  [[ -n "${user_id}" ]] || fail "failed to resolve headscale user id"
  FUGUE_MESH_AUTH_KEY="$(ssh_root_run "${PRIMARY_ALIAS}" "KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl -n $(printf '%q' "${NAMESPACE}") exec deploy/$(printf '%q' "${HEADSCALE_DEPLOYMENT_NAME}") -- headscale preauthkeys create -u $(printf '%q' "${user_id}") --reusable --expiration 8760h -o json-line" | tr -d '\r' | grep -o '\"key\":\"[^\"]*\"' | head -n 1 | cut -d: -f2 | tr -d '\"')"
  [[ -n "${FUGUE_MESH_AUTH_KEY}" ]] || fail "failed to create headscale preauth key"
}

install_tailscale_on_host() {
  local host="$1"
  local mesh_hostname
  mesh_hostname="$(mesh_hostname_for_alias "${host}")"
  log "installing tailscale mesh client on ${host} as ${mesh_hostname}"
  ssh_root "${host}" <<EOF
set -euo pipefail
if ! command -v tailscale >/dev/null 2>&1; then
  curl -fsSL https://tailscale.com/install.sh | sh
fi
systemctl enable tailscaled
systemctl restart tailscaled
systemctl is-active --quiet tailscaled
tailscale up \
  --login-server "$(mesh_login_server)" \
  --authkey "${FUGUE_MESH_AUTH_KEY}" \
  --hostname "${mesh_hostname}" \
  --accept-dns=false \
  --reset
for _ in \$(seq 1 30); do
  ip="\$(tailscale ip -4 2>/dev/null | awk 'NR == 1 {print; exit}')"
  if [ -n "\${ip}" ]; then
    echo "\${ip}"
    exit 0
  fi
  sleep 2
done
echo "tailscale IPv4 was not assigned on ${host}" >&2
exit 1
EOF
}

control_plane_mesh_ready() {
  if ! mesh_enabled; then
    return 1
  fi

  local host configured_external_ip configured_node_ip configured_flannel_iface mesh_ip node_ip
  for host in "${ALL_ALIASES[@]}"; do
    mesh_ip="$(mesh_ip_for_alias "${host}")"
    [[ -n "${mesh_ip}" ]] || return 1
    node_ip="$(node_internal_ip_for_alias "${host}")"
    [[ -n "${node_ip}" ]] || return 1
    configured_external_ip="$(ssh_root_run "${host}" "awk -F'\"' '/^node-external-ip:/ {print \$2; exit}' /etc/rancher/k3s/config.yaml 2>/dev/null || true" | tr -d '\r')"
    configured_node_ip="$(ssh_root_run "${host}" "awk -F'\"' '/^node-ip:/ {print \$2; exit}' /etc/rancher/k3s/config.yaml 2>/dev/null || true" | tr -d '\r')"
    configured_flannel_iface="$(ssh_root_run "${host}" "awk -F'\"' '/^flannel-iface:/ {print \$2; exit}' /etc/rancher/k3s/config.yaml 2>/dev/null || true" | tr -d '\r')"
    [[ "${configured_external_ip}" == "${mesh_ip}" ]] || return 1
    [[ "${configured_node_ip}" == "${node_ip}" ]] || return 1
    [[ "${configured_flannel_iface}" == "tailscale0" ]] || return 1
  done
  return 0
}

configure_control_plane_mesh() {
  if ! mesh_enabled; then
    return
  fi

  ensure_headscale_auth_key
  if control_plane_mesh_ready; then
    log "control plane already configured for mesh joins; skipping k3s server reconfigure"
    return
  fi

  for host in "${ALL_ALIASES[@]}"; do
    install_tailscale_on_host "${host}" >/dev/null
  done

  log "reconfiguring k3s servers to advertise mesh IPs"
  install_k3s_cluster
  wait_for_cluster_ready
}

fetch_kubeconfig() {
  log "fetching kubeconfig to ${KUBECONFIG_OUT}"
  ssh_root_run "${PRIMARY_ALIAS}" "cat /etc/rancher/k3s/k3s.yaml" >"${KUBECONFIG_OUT}"
  perl -0pi -e "s#https://127.0.0.1:6443#https://${PUBLIC_ENDPOINT_HOST}:6443#g" "${KUBECONFIG_OUT}"
}

write_summary() {
  cat >"${SUMMARY_FILE}" <<EOF
FUGUE_BOOTSTRAP_KEY=${BOOTSTRAP_KEY}
FUGUE_API_URL=$(api_public_base_url)
FUGUE_PUBLIC_ENDPOINT_HOST=${PUBLIC_ENDPOINT_HOST}
FUGUE_PUBLIC_API_REACHABLE=${PUBLIC_API_REACHABLE}
FUGUE_CLUSTER_INTERNAL_IP=${K3S_API_IP}
FUGUE_DOMAIN=${FUGUE_DOMAIN}
FUGUE_APP_BASE_DOMAIN=${FUGUE_APP_BASE_DOMAIN}
FUGUE_REGISTRY_DOMAIN=${FUGUE_REGISTRY_DOMAIN}
FUGUE_STATE_BACKEND=postgresql-relational
FUGUE_EDGE_LOCAL_HEALTH=${EDGE_LOCAL_HEALTH}
FUGUE_EDGE_UPSTREAM=${EDGE_UPSTREAM}
FUGUE_EDGE_UPSTREAM_MODE=${EDGE_UPSTREAM_MODE}
FUGUE_REGISTRY_EDGE_UPSTREAM=${REGISTRY_EDGE_UPSTREAM}
FUGUE_MESH_ENABLED=${FUGUE_MESH_ENABLED}
FUGUE_MESH_PROVIDER=${FUGUE_MESH_PROVIDER}
FUGUE_MESH_DOMAIN=${FUGUE_MESH_DOMAIN}
FUGUE_MESH_LOGIN_SERVER=$(if mesh_enabled; then mesh_login_server; fi)
KUBECONFIG=${KUBECONFIG_OUT}
PRIMARY_ALIAS=${PRIMARY_ALIAS}
SECONDARY_ALIASES=${SECONDARY_ALIASES[*]}
IMAGE_TAG=${IMAGE_TAG}
EOF
}

check_public_api_reachability() {
  if ! command -v curl >/dev/null 2>&1; then
    PUBLIC_API_REACHABLE="unknown"
    return
  fi

  if curl -fsS --max-time 10 "$(api_public_health_url)" >/dev/null 2>&1; then
    PUBLIC_API_REACHABLE="true"
    log "verified public Fugue API endpoint on $(api_public_base_url)"
    return
  fi

  PUBLIC_API_REACHABLE="false"
  if [[ -n "${FUGUE_DOMAIN}" ]]; then
    log "warning: public Route A endpoint $(api_public_base_url) is not reachable from this machine"
    log "warning: set Cloudflare SSL/TLS mode to Full and allow tcp/443 from Cloudflare IP ranges to ${PUBLIC_ENDPOINT_HOST}"
    return
  fi
  log "warning: public Fugue API endpoint is not reachable from this machine"
  log "warning: if you are on a cloud provider, open tcp/6443 and tcp/${API_NODEPORT} to ${PUBLIC_ENDPOINT_HOST} in the provider firewall or security group"
}

write_route_a_file() {
  if [[ -z "${FUGUE_DOMAIN}" ]]; then
    return
  fi

  local custom_domain_target_base="dns.${FUGUE_APP_BASE_DOMAIN}"

  cat >"${ROUTE_A_FILE}" <<EOF
Route A is configured on ${PRIMARY_ALIAS} with Caddy:
  https://${FUGUE_DOMAIN} -> https origin on ${PRIMARY_ALIAS}:443 -> upstream ${EDGE_UPSTREAM} (${EDGE_UPSTREAM_MODE})
  https://${FUGUE_REGISTRY_DOMAIN} -> https origin on ${PRIMARY_ALIAS}:443 -> upstream ${REGISTRY_EDGE_UPSTREAM}
$(if mesh_enabled; then printf '  https://%s -> https origin on %s:443 -> upstream %s\n' "${FUGUE_MESH_DOMAIN}" "${PRIMARY_ALIAS}" "${HEADSCALE_EDGE_UPSTREAM}"; fi)
  https://*.${FUGUE_APP_BASE_DOMAIN} -> https origin on ${PRIMARY_ALIAS}:443 -> upstream ${EDGE_UPSTREAM} (${EDGE_UPSTREAM_MODE})
  verified custom domains -> imported from /etc/caddy/fugue-custom-domains.caddy

Server-side status:
  EDGE_LOCAL_HEALTH=${EDGE_LOCAL_HEALTH}
  PUBLIC_API_REACHABLE=${PUBLIC_API_REACHABLE}

Cloudflare actions:
  1. Keep the A record for ${FUGUE_DOMAIN} pointing to ${PUBLIC_ENDPOINT_HOST}.
  2. Add a wildcard record for *.${FUGUE_APP_BASE_DOMAIN} pointing to ${PUBLIC_ENDPOINT_HOST}.
  3. Add a wildcard record for *.${custom_domain_target_base} pointing to ${PUBLIC_ENDPOINT_HOST} so dedicated custom-domain targets like d-<hash>.${custom_domain_target_base} resolve to the edge.
  4. Keep Cloudflare proxy enabled (orange cloud).
  5. Set SSL/TLS encryption mode to Full.
  6. Optional but recommended: enable "Always Use HTTPS".

GCP actions:
  1. Open tcp/443 to ${PUBLIC_ENDPOINT_HOST} from Cloudflare IP ranges.
  2. Optional: open tcp/6443 only to your own public IP if you want local kubectl access.
  3. Do not expose tcp/${API_NODEPORT} or tcp/${REGISTRY_NODEPORT} publicly for Route A.

Cloudflare IP list references:
  https://www.cloudflare.com/ips/
  https://www.cloudflare.com/ips-v4
  https://www.cloudflare.com/ips-v6

Tests:
  curl -I https://${FUGUE_DOMAIN}
  curl https://${FUGUE_DOMAIN}/healthz
  curl -I https://${FUGUE_REGISTRY_DOMAIN}/v2/
  KUBECONFIG='${KUBECONFIG_OUT}' kubectl get nodes
EOF
}

print_next_steps() {
  cat <<EOF

Install finished.

Bootstrap admin key:
  ${BOOTSTRAP_KEY}

Fugue API:
  $(api_public_base_url)

Kubeconfig written to:
  ${KUBECONFIG_OUT}

Summary file:
  ${SUMMARY_FILE}

Suggested next commands:
  export KUBECONFIG='${KUBECONFIG_OUT}'
  kubectl get nodes
  curl -sS $(api_public_health_url)
EOF
  if [[ -n "${FUGUE_DOMAIN}" ]]; then
    cat <<EOF

Route A notes file:
  ${ROUTE_A_FILE}

Registry endpoint:
  https://${FUGUE_REGISTRY_DOMAIN}/v2/
EOF
  fi
  if [[ "${PUBLIC_API_REACHABLE}" == "false" ]]; then
    if [[ -n "${FUGUE_DOMAIN}" ]]; then
      cat <<EOF

Public access note:
  The Route A edge proxy is configured on ${PRIMARY_ALIAS}, but $(api_public_base_url) is not reachable yet.
  In Cloudflare, set SSL/TLS mode to Full.
  In GCP, allow tcp/443 to ${PUBLIC_ENDPOINT_HOST} from Cloudflare IP ranges only.
  Keep tcp/${API_NODEPORT} closed to the public internet.
EOF
      return
    fi
    cat <<EOF

Public access note:
  The cluster deployment is healthy, but ${PUBLIC_ENDPOINT_HOST}:${API_NODEPORT} is not reachable from this machine.
  On cloud providers like GCP, you usually need to open tcp/6443 and tcp/${API_NODEPORT} in the provider firewall or security group.
EOF
  fi
}

main() {
  require_cmd ssh
  require_cmd scp
  require_cmd ssh-keygen
  require_cmd ssh-keyscan
  require_cmd tar
  require_cmd perl
  require_cmd openssl
  detect_container_tool
  prepare_dist
  check_ssh_and_sudo
  detect_remote_platform
  detect_api_ip
  detect_public_endpoint_host

  log "primary alias: ${PRIMARY_ALIAS}"
  log "secondary aliases: ${SECONDARY_ALIASES[*]}"
  log "detected API IP: ${K3S_API_IP}"
  log "detected public endpoint host: ${PUBLIC_ENDPOINT_HOST}"
  if [[ -n "${FUGUE_DOMAIN}" ]]; then
    log "configured Route A domain: ${FUGUE_DOMAIN}"
  fi
  log "image platform: ${IMAGE_PLATFORM}"

  build_images
  if cluster_is_ready; then
    log "existing k3s cluster is healthy; skipping cluster bootstrap"
    if [[ "${RECONCILE_K3S_CLUSTER}" == "true" ]]; then
      log "reconciling k3s server configuration for public worker joins"
      install_k3s_cluster
      wait_for_cluster_ready
    fi
  else
    install_k3s_cluster
    wait_for_cluster_ready
  fi
  cleanup_disabled_addons
  label_control_plane_nodes
  ensure_coredns_multinode_scheduling
  push_and_import_images "${PRIMARY_ALIAS}"
  push_and_import_images "${SECONDARY_ALIASES[0]}"
  push_and_import_images "${SECONDARY_ALIASES[1]}"
  install_helm_on_primary
  maybe_reuse_existing_bootstrap_key
  maybe_reuse_existing_edge_tls_ask_token
  if [[ -z "${BOOTSTRAP_KEY}" ]]; then
    BOOTSTRAP_KEY="$(generate_secret)"
  fi
  if [[ -z "${FUGUE_EDGE_TLS_ASK_TOKEN}" ]]; then
    FUGUE_EDGE_TLS_ASK_TOKEN="$(generate_secret)"
  fi
  write_values_override
  copy_chart_and_values
  install_fugue_chart
  install_edge_proxy_on_primary
  verify_edge_proxy_config_on_primary
  if mesh_enabled; then
    configure_control_plane_mesh
    ensure_coredns_multinode_scheduling
    write_values_override
    copy_chart_and_values
    install_fugue_chart
    install_edge_proxy_on_primary
    verify_edge_proxy_config_on_primary
  fi
  setup_control_plane_automation
  fetch_kubeconfig
  check_edge_origin_health
  check_public_api_reachability
  write_summary
  write_route_a_file
  print_next_steps
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
