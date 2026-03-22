#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PRIMARY_ALIAS="${FUGUE_NODE1:-gcp1}"
SECONDARY_ALIASES=("${FUGUE_NODE2:-gcp2}" "${FUGUE_NODE3:-gcp3}")
ALL_ALIASES=("${PRIMARY_ALIAS}" "${SECONDARY_ALIASES[@]}")

RELEASE_NAME="${FUGUE_RELEASE_NAME:-fugue}"
NAMESPACE="${FUGUE_NAMESPACE:-fugue-system}"
REMOTE_TMP_BASE="${FUGUE_REMOTE_TMP_BASE:-/tmp/fugue-install}"
HOSTPATH_DATA_DIR="${FUGUE_HOSTPATH_DATA_DIR:-/var/lib/fugue}"
K3S_CHANNEL="${FUGUE_K3S_CHANNEL:-stable}"
API_NODEPORT="${FUGUE_API_NODEPORT:-30080}"
IMAGE_TAG="${FUGUE_IMAGE_TAG:-local-$(date +%Y%m%d%H%M%S)}"
DIST_DIR="${FUGUE_DIST_DIR:-${REPO_ROOT}/.dist/fugue-install}"
BOOTSTRAP_KEY="${FUGUE_BOOTSTRAP_KEY:-}"
K3S_API_IP="${FUGUE_K3S_API_IP:-}"
IMAGE_PLATFORM="${FUGUE_IMAGE_PLATFORM:-}"
CONTAINER_TOOL="${FUGUE_CONTAINER_TOOL:-}"

IMAGES_TAR="${DIST_DIR}/fugue-images-${IMAGE_TAG}.tar"
VALUES_FILE="${DIST_DIR}/values-override.yaml"
KUBECONFIG_OUT="${DIST_DIR}/kubeconfig"
SUMMARY_FILE="${DIST_DIR}/install-summary.txt"

log() {
  printf '[fugue-install] %s\n' "$*"
}

fail() {
  printf '[fugue-install] ERROR: %s\n' "$*" >&2
  exit 1
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

ssh_run() {
  local host="$1"
  shift
  ssh -o BatchMode=yes "${host}" "$@"
}

ssh_root() {
  local host="$1"
  shift
  local mode
  mode="$(ssh_run "${host}" 'if [ "$(id -u)" -eq 0 ]; then echo root; elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then echo sudo; else echo none; fi')"
  case "${mode}" in
    root)
      ssh -o BatchMode=yes "${host}" "bash -s" "$@"
      ;;
    sudo)
      ssh -o BatchMode=yes "${host}" "sudo bash -s" "$@"
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
  mode="$(ssh_run "${host}" 'if [ "$(id -u)" -eq 0 ]; then echo root; elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then echo sudo; else echo none; fi')"
  case "${mode}" in
    root)
      ssh -o BatchMode=yes "${host}" "bash -lc $(printf '%q' "${cmd}")"
      ;;
    sudo)
      ssh -o BatchMode=yes "${host}" "sudo bash -lc $(printf '%q' "${cmd}")"
      ;;
    *)
      fail "${host} needs either a root SSH login or passwordless sudo"
      ;;
  esac
}

scp_to() {
  local src="$1"
  local host="$2"
  local dst="$3"
  scp -q -o BatchMode=yes "${src}" "${host}:${dst}"
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

build_images() {
  log "building local images with ${CONTAINER_TOOL} for ${IMAGE_PLATFORM}"
  "${CONTAINER_TOOL}" build --platform "${IMAGE_PLATFORM}" -f "${REPO_ROOT}/Dockerfile.api" -t "fugue-api:${IMAGE_TAG}" "${REPO_ROOT}"
  "${CONTAINER_TOOL}" build --platform "${IMAGE_PLATFORM}" -f "${REPO_ROOT}/Dockerfile.controller" -t "fugue-controller:${IMAGE_TAG}" "${REPO_ROOT}"
  "${CONTAINER_TOOL}" save -o "${IMAGES_TAR}" "fugue-api:${IMAGE_TAG}" "fugue-controller:${IMAGE_TAG}"
}

write_primary_config() {
  ssh_root "${PRIMARY_ALIAS}" <<EOF
set -euo pipefail
mkdir -p /etc/rancher/k3s
cat >/etc/rancher/k3s/config.yaml <<'CFG'
cluster-init: true
write-kubeconfig-mode: "644"
tls-san:
  - "${K3S_API_IP}"
disable:
  - traefik
  - servicelb
  - local-storage
node-label:
  - "fugue.install/profile=combined"
  - "fugue.install/role=primary"
CFG
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
  fail "timed out waiting for primary k3s token"
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
tls-san:
  - "${K3S_API_IP}"
disable:
  - traefik
  - servicelb
  - local-storage
node-label:
  - "fugue.install/profile=combined"
  - "fugue.install/role=${role}"
CFG
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
  ssh_root "${PRIMARY_ALIAS}" <<'EOF'
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

prepare_remote_tmp() {
  local host="$1"
  ssh_root "${host}" <<EOF
set -euo pipefail
rm -rf "${REMOTE_TMP_BASE}"
mkdir -p "${REMOTE_TMP_BASE}"
EOF
}

push_and_import_images() {
  local host="$1"
  log "copying images to ${host}"
  prepare_remote_tmp "${host}"
  scp_to "${IMAGES_TAR}" "${host}" "${REMOTE_TMP_BASE}/fugue-images.tar"
  ssh_root "${host}" <<EOF
set -euo pipefail
k3s ctr images import "${REMOTE_TMP_BASE}/fugue-images.tar"
EOF
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
  cat >"${VALUES_FILE}" <<EOF
bootstrapAdminKey: "${BOOTSTRAP_KEY}"

api:
  image:
    repository: fugue-api
    tag: "${IMAGE_TAG}"
    pullPolicy: IfNotPresent
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
  scp_to "${VALUES_FILE}" "${PRIMARY_ALIAS}" "${REMOTE_TMP_BASE}/values-override.yaml"
  scp -rq -o BatchMode=yes "${REPO_ROOT}/deploy/helm/fugue" "${PRIMARY_ALIAS}:${REMOTE_TMP_BASE}/"
}

install_fugue_chart() {
  log "installing Fugue Helm release"
  ssh_root "${PRIMARY_ALIAS}" <<EOF
set -euo pipefail
mkdir -p "${HOSTPATH_DATA_DIR}"
KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm upgrade --install "${RELEASE_NAME}" "${REMOTE_TMP_BASE}/fugue" \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  -f "${REMOTE_TMP_BASE}/values-override.yaml"
KUBECONFIG=/etc/rancher/k3s/k3s.yaml k3s kubectl -n "${NAMESPACE}" rollout status deploy/"${RELEASE_NAME}"-fugue --timeout=180s
EOF
}

fetch_kubeconfig() {
  log "fetching kubeconfig to ${KUBECONFIG_OUT}"
  ssh_root_run "${PRIMARY_ALIAS}" "cat /etc/rancher/k3s/k3s.yaml" >"${KUBECONFIG_OUT}"
  perl -0pi -e "s#https://127.0.0.1:6443#https://${K3S_API_IP}:6443#g" "${KUBECONFIG_OUT}"
}

write_summary() {
  cat >"${SUMMARY_FILE}" <<EOF
FUGUE_BOOTSTRAP_KEY=${BOOTSTRAP_KEY}
FUGUE_API_URL=http://${K3S_API_IP}:${API_NODEPORT}
KUBECONFIG=${KUBECONFIG_OUT}
PRIMARY_ALIAS=${PRIMARY_ALIAS}
SECONDARY_ALIASES=${SECONDARY_ALIASES[*]}
IMAGE_TAG=${IMAGE_TAG}
EOF
}

print_next_steps() {
  cat <<EOF

Install finished.

Bootstrap admin key:
  ${BOOTSTRAP_KEY}

Fugue API:
  http://${K3S_API_IP}:${API_NODEPORT}

Kubeconfig written to:
  ${KUBECONFIG_OUT}

Summary file:
  ${SUMMARY_FILE}

Suggested next commands:
  export KUBECONFIG='${KUBECONFIG_OUT}'
  kubectl get nodes
  curl -sS http://${K3S_API_IP}:${API_NODEPORT}/healthz
EOF
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
  if [[ -z "${BOOTSTRAP_KEY}" ]]; then
    BOOTSTRAP_KEY="$(generate_secret)"
  fi

  log "primary alias: ${PRIMARY_ALIAS}"
  log "secondary aliases: ${SECONDARY_ALIASES[*]}"
  log "detected API IP: ${K3S_API_IP}"
  log "image platform: ${IMAGE_PLATFORM}"

  build_images
  install_k3s_cluster
  wait_for_cluster_ready
  label_primary_node
  push_and_import_images "${PRIMARY_ALIAS}"
  push_and_import_images "${SECONDARY_ALIASES[0]}"
  push_and_import_images "${SECONDARY_ALIASES[1]}"
  install_helm_on_primary
  write_values_override
  copy_chart_and_values
  install_fugue_chart
  fetch_kubeconfig
  write_summary
  print_next_steps
}

main "$@"
