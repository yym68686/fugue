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
IMAGE_TAG="${FUGUE_IMAGE_TAG:-local-$(date +%Y%m%d%H%M%S)}"
DIST_DIR="${FUGUE_DIST_DIR:-${REPO_ROOT}/.dist/fugue-install}"
BOOTSTRAP_KEY="${FUGUE_BOOTSTRAP_KEY:-}"
K3S_API_IP="${FUGUE_K3S_API_IP:-}"
PUBLIC_ENDPOINT_HOST="${FUGUE_PUBLIC_ENDPOINT_HOST:-}"
FUGUE_DOMAIN="${FUGUE_DOMAIN:-}"
FUGUE_APP_BASE_DOMAIN="${FUGUE_APP_BASE_DOMAIN:-fugue.pro}"
FUGUE_REGISTRY_DOMAIN="${FUGUE_REGISTRY_DOMAIN:-registry.${FUGUE_APP_BASE_DOMAIN}}"
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

log() {
  printf '[fugue-install] %s\n' "$*"
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
  existing_key="$(ssh_root_run "${PRIMARY_ALIAS}" "KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl -n $(printf '%q' "${NAMESPACE}") get secret $(printf '%q' "${CONFIG_SECRET_NAME}") -o jsonpath='{.data.FUGUE_BOOTSTRAP_ADMIN_KEY}' 2>/dev/null | base64 -d 2>/dev/null || true")"
  if [[ -n "${existing_key}" ]]; then
    BOOTSTRAP_KEY="${existing_key}"
    log "reusing existing bootstrap admin key from ${NAMESPACE}/${CONFIG_SECRET_NAME}"
  fi
}

maybe_reuse_existing_mesh_auth_key() {
  if ! mesh_enabled || [[ -n "${FUGUE_MESH_AUTH_KEY}" ]]; then
    return
  fi

  local existing_key=""
  existing_key="$(ssh_root_run "${PRIMARY_ALIAS}" "KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl -n $(printf '%q' "${NAMESPACE}") get secret $(printf '%q' "${CONFIG_SECRET_NAME}") -o jsonpath='{.data.FUGUE_CLUSTER_JOIN_MESH_AUTH_KEY}' 2>/dev/null | base64 -d 2>/dev/null || true")"
  if [[ -n "${existing_key}" ]]; then
    FUGUE_MESH_AUTH_KEY="${existing_key}"
    log "reusing existing mesh auth key from ${NAMESPACE}/${CONFIG_SECRET_NAME}"
  fi
}

backup_legacy_store_on_primary() {
  log "backing up legacy store.json on ${PRIMARY_ALIAS}"
  ssh_root "${PRIMARY_ALIAS}" <<'EOF'
set -euo pipefail
if [ -f /var/lib/fugue/store.json ]; then
  backup="/var/lib/fugue/store.json.bak-pg-$(date +%Y%m%d%H%M%S)"
  cp -a /var/lib/fugue/store.json "${backup}"
  echo "${backup}"
fi
EOF
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

ssh_run_raw() {
  local host="$1"
  shift
  ssh -n "${SSH_OPTS[@]}" "${host}" "$@"
}

ssh_run() {
  local host="$1"
  shift
  run_with_retry "${REMOTE_CMD_RETRIES}" "${REMOTE_CMD_RETRY_DELAY}" "ssh command on ${host}" \
    ssh_run_raw "${host}" "$@"
}

detect_remote_mode_raw() {
  local host="$1"
  ssh -n "${SSH_OPTS[@]}" "${host}" 'if [ "$(id -u)" -eq 0 ]; then echo root; elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then echo sudo; else echo none; fi'
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
  case "${mode}" in
    root)
      printf '%s\n' "${script}" | ssh "${SSH_OPTS[@]}" "${host}" "bash -s"
      ;;
    sudo)
      printf '%s\n' "${script}" | ssh "${SSH_OPTS[@]}" "${host}" "sudo bash -s"
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
  case "${mode}" in
    root)
      ssh -n "${SSH_OPTS[@]}" "${host}" "bash -lc $(printf '%q' "${cmd}")"
      ;;
    sudo)
      ssh -n "${SSH_OPTS[@]}" "${host}" "sudo bash -lc $(printf '%q' "${cmd}")"
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

  for attempt in $(seq 1 "${UPLOAD_RETRIES}"); do
    if scp -q "${SSH_OPTS[@]}" "${src}" "${host}:${dst}"; then
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
    if ssh "${SSH_OPTS[@]}" "${host}" "cat > ${remote_quoted}" < "${src}"; then
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
  ssh -G "${host}" | awk '/^hostname / {print $2; exit}'
}

mesh_ip_for_alias() {
  local host="$1"
  ssh_root_run "${host}" "if command -v tailscale >/dev/null 2>&1; then tailscale ip -4 2>/dev/null | awk 'NR == 1 {print; exit}'; fi" | tr -d '\r'
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

  local app_tls_directive="tls internal"
  if [[ -n "${FUGUE_APP_BASE_DOMAIN}" ]] && app_tls_cert_matches_domain; then
    log "uploading wildcard app TLS material to ${PRIMARY_ALIAS}"
    prepare_remote_tmp "${PRIMARY_ALIAS}"
    scp_to "${FUGUE_APP_TLS_CERT_FILE}" "${PRIMARY_ALIAS}" "${REMOTE_TMP_BASE}/cloudflare-apps-origin.crt"
    scp_to "${FUGUE_APP_TLS_KEY_FILE}" "${PRIMARY_ALIAS}" "${REMOTE_TMP_BASE}/cloudflare-apps-origin.key"
    app_tls_directive="tls /etc/caddy/tls/cloudflare-apps-origin.crt /etc/caddy/tls/cloudflare-apps-origin.key"
  elif [[ -n "${FUGUE_APP_BASE_DOMAIN}" ]] && remote_has_app_tls_material; then
    log "reusing existing wildcard app TLS material already installed on ${PRIMARY_ALIAS}"
    app_tls_directive="tls /etc/caddy/tls/cloudflare-apps-origin.crt /etc/caddy/tls/cloudflare-apps-origin.key"
  elif [[ -n "${FUGUE_APP_BASE_DOMAIN}" ]] && has_app_tls_material; then
    log "wildcard app TLS cert does not match ${FUGUE_APP_BASE_DOMAIN}; using tls internal for app hosts"
  fi

  local mesh_site_block=""
  if mesh_enabled; then
    mesh_site_block="$(cat <<EOF
https://${FUGUE_MESH_DOMAIN} {
  ${app_tls_directive}
  encode gzip zstd
  reverse_proxy ${HEADSCALE_EDGE_UPSTREAM}
}
EOF
)"
  fi

  log "installing Route A edge proxy on ${PRIMARY_ALIAS} for ${FUGUE_DOMAIN}, ${FUGUE_REGISTRY_DOMAIN}, and *.${FUGUE_APP_BASE_DOMAIN}"
  ssh_root "${PRIMARY_ALIAS}" <<EOF
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
if ! command -v caddy >/dev/null 2>&1; then
  apt-get update
  apt-get install -y caddy
fi
mkdir -p /etc/caddy
mkdir -p /etc/caddy/tls
if [ -f "${REMOTE_TMP_BASE}/cloudflare-apps-origin.crt" ] && [ -f "${REMOTE_TMP_BASE}/cloudflare-apps-origin.key" ]; then
  install -m 0644 "${REMOTE_TMP_BASE}/cloudflare-apps-origin.crt" /etc/caddy/tls/cloudflare-apps-origin.crt
  install -m 0640 "${REMOTE_TMP_BASE}/cloudflare-apps-origin.key" /etc/caddy/tls/cloudflare-apps-origin.key
  chown root:caddy /etc/caddy/tls/cloudflare-apps-origin.crt /etc/caddy/tls/cloudflare-apps-origin.key
fi
cat >/etc/caddy/Caddyfile <<'CADDY'
{
  admin off
}

https://${FUGUE_DOMAIN} {
  tls internal
  encode gzip zstd
  reverse_proxy ${EDGE_UPSTREAM}
}

https://${FUGUE_REGISTRY_DOMAIN} {
  tls internal
  encode gzip zstd
  reverse_proxy ${REGISTRY_EDGE_UPSTREAM}
}

${mesh_site_block}

https://*.${FUGUE_APP_BASE_DOMAIN} {
  ${app_tls_directive}
  encode gzip zstd
  reverse_proxy ${EDGE_UPSTREAM}
}
CADDY
caddy validate --config /etc/caddy/Caddyfile
systemctl enable caddy
systemctl restart caddy
systemctl is-active --quiet caddy
EOF
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
  - local-storage
node-label:
  - "fugue.install/profile=combined"
  - "fugue.install/role=primary"
CFG
cat >/etc/rancher/k3s/registries.yaml <<'REG'
mirrors:
  "${K3S_API_IP}:${REGISTRY_NODEPORT}":
    endpoint:
      - "http://${K3S_API_IP}:${REGISTRY_NODEPORT}"
configs:
  "${K3S_API_IP}:${REGISTRY_NODEPORT}":
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
  - local-storage
node-label:
  - "fugue.install/profile=combined"
  - "fugue.install/role=${role}"
CFG
cat >/etc/rancher/k3s/registries.yaml <<'REG'
mirrors:
  "${K3S_API_IP}:${REGISTRY_NODEPORT}":
    endpoint:
      - "http://${K3S_API_IP}:${REGISTRY_NODEPORT}"
configs:
  "${K3S_API_IP}:${REGISTRY_NODEPORT}":
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

label_primary_node() {
  log "labeling primary node for pinned fugue control-plane scheduling"
  local primary_node
  primary_node="$(ssh_run "${PRIMARY_ALIAS}" "hostname")"
  ssh_root "${PRIMARY_ALIAS}" <<EOF
set -euo pipefail
k3s kubectl label node "${primary_node}" fugue.install/role=primary --overwrite
k3s kubectl label node "${primary_node}" fugue.install/profile=combined --overwrite
EOF
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
  local cluster_join_token
  cluster_join_token="$(wait_for_primary_node_token)"
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
  image:
    repository: fugue-api
    tag: "${IMAGE_TAG}"
    pullPolicy: IfNotPresent
  appBaseDomain: "${FUGUE_APP_BASE_DOMAIN}"
  apiPublicDomain: "${FUGUE_DOMAIN}"
  registryPushBase: "${FUGUE_REGISTRY_DOMAIN}"
  clusterJoinServer: "${cluster_join_server}"
  clusterJoinToken: "${cluster_join_token}"
  clusterJoinMeshProvider: "${cluster_join_mesh_provider}"
  clusterJoinMeshLoginServer: "${cluster_join_mesh_login_server}"
  clusterJoinMeshAuthKey: "${cluster_join_mesh_auth_key}"
  importWorkDir: "/var/lib/fugue/import"
  hostNetwork: true
  resources:
    requests:
      cpu: 100m
      memory: 128Mi
    limits:
      cpu: 500m
      memory: 512Mi

controller:
  image:
    repository: fugue-controller
    tag: "${IMAGE_TAG}"
    pullPolicy: IfNotPresent
  kubectlApply: true
  resources:
    requests:
      cpu: 100m
      memory: 128Mi
    limits:
      cpu: 500m
      memory: 512Mi

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
    hostPath: "${HOSTPATH_DATA_DIR}/registry"
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

nodeSelector:
  "fugue.install/role": primary
EOF
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
KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm upgrade --install "${RELEASE_NAME}" "${REMOTE_TMP_BASE}/fugue" \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  --wait \
  --timeout 300s \
  -f "${REMOTE_TMP_BASE}/values-override.yaml"
KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl -n "${NAMESPACE}" rollout status deploy/"${POSTGRES_DEPLOYMENT_NAME}" --timeout=180s
KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl -n "${NAMESPACE}" rollout status deploy/"${RELEASE_FULLNAME}" --timeout=180s
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

  cat >"${ROUTE_A_FILE}" <<EOF
Route A is configured on ${PRIMARY_ALIAS} with Caddy:
  https://${FUGUE_DOMAIN} -> https origin on ${PRIMARY_ALIAS}:443 -> upstream ${EDGE_UPSTREAM} (${EDGE_UPSTREAM_MODE})
  https://${FUGUE_REGISTRY_DOMAIN} -> https origin on ${PRIMARY_ALIAS}:443 -> upstream ${REGISTRY_EDGE_UPSTREAM}
$(if mesh_enabled; then printf '  https://%s -> https origin on %s:443 -> upstream %s\n' "${FUGUE_MESH_DOMAIN}" "${PRIMARY_ALIAS}" "${HEADSCALE_EDGE_UPSTREAM}"; fi)
  https://*.${FUGUE_APP_BASE_DOMAIN} -> https origin on ${PRIMARY_ALIAS}:443 -> upstream ${EDGE_UPSTREAM} (${EDGE_UPSTREAM_MODE})

Server-side status:
  EDGE_LOCAL_HEALTH=${EDGE_LOCAL_HEALTH}
  PUBLIC_API_REACHABLE=${PUBLIC_API_REACHABLE}

Cloudflare actions:
  1. Keep the A record for ${FUGUE_DOMAIN} pointing to ${PUBLIC_ENDPOINT_HOST}.
  2. Add a wildcard record for *.${FUGUE_APP_BASE_DOMAIN} pointing to ${PUBLIC_ENDPOINT_HOST}.
  3. Keep Cloudflare proxy enabled (orange cloud).
  4. Set SSL/TLS encryption mode to Full.
  5. Optional but recommended: enable "Always Use HTTPS".

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
  label_primary_node
  push_and_import_images "${PRIMARY_ALIAS}"
  push_and_import_images "${SECONDARY_ALIASES[0]}"
  push_and_import_images "${SECONDARY_ALIASES[1]}"
  install_helm_on_primary
  maybe_reuse_existing_bootstrap_key
  if [[ -z "${BOOTSTRAP_KEY}" ]]; then
    BOOTSTRAP_KEY="$(generate_secret)"
  fi
  backup_legacy_store_on_primary
  write_values_override
  copy_chart_and_values
  install_fugue_chart
  install_edge_proxy_on_primary
  if mesh_enabled; then
    configure_control_plane_mesh
    write_values_override
    copy_chart_and_values
    install_fugue_chart
    install_edge_proxy_on_primary
  fi
  fetch_kubeconfig
  check_edge_origin_health
  check_public_api_reachability
  write_summary
  write_route_a_file
  print_next_steps
}

main "$@"
