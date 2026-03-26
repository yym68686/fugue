#!/usr/bin/env bash

set -euo pipefail

log() {
  printf '[fugue-upgrade] %s\n' "$*"
}

fail() {
  printf '[fugue-upgrade] ERROR: %s\n' "$*" >&2
  exit 1
}

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    fail "missing required env ${name}"
  fi
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

detect_primary_private_ip() {
  ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i=1;i<=NF;i++) if ($i=="src") {print $(i+1); exit}}'
}

detect_existing_registry_pull_base() {
  if [[ ! -r /etc/rancher/k3s/registries.yaml ]]; then
    return 1
  fi
  awk '
    $1 == "mirrors:" { in_mirrors = 1; next }
    in_mirrors && /^[[:space:]]*"/ {
      value = $1
      gsub(/"/, "", value)
      sub(/:$/, "", value)
      print value
      exit
    }
  ' /etc/rancher/k3s/registries.yaml
}

detect_primary_mesh_ip() {
  if ! command_exists tailscale; then
    return 1
  fi
  tailscale ip -4 2>/dev/null | awk 'NR == 1 {print; exit}'
}

detect_cluster_join_server() {
  local secret_name="${FUGUE_RELEASE_FULLNAME}-config"
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get secret "${secret_name}" -o jsonpath='{.data.FUGUE_CLUSTER_JOIN_SERVER}' 2>/dev/null | base64 --decode 2>/dev/null || true
}

registry_endpoint_from_join_server() {
  local join_server="$1"
  local host=""
  join_server="${join_server#*://}"
  join_server="${join_server%%/*}"
  host="${join_server%%:*}"
  if [[ -n "${host}" ]]; then
    printf '%s:%s' "${host}" "${FUGUE_REGISTRY_NODEPORT}"
  fi
}

detect_kubectl() {
  if [[ -n "${KUBECTL_BIN:-}" ]]; then
    printf '%s' "${KUBECTL_BIN}"
    return
  fi
  if command_exists kubectl; then
    printf 'kubectl'
    return
  fi
  if command_exists k3s; then
    printf 'k3s kubectl'
    return
  fi
  fail "kubectl is not available"
}

retry() {
  local attempts="$1"
  local delay_seconds="$2"
  shift 2

  local i
  for ((i=1; i<=attempts; i++)); do
    if "$@"; then
      return 0
    fi
    if (( i == attempts )); then
      return 1
    fi
    sleep "${delay_seconds}"
  done
}

helm_current_revision() {
  helm history "${FUGUE_RELEASE_NAME}" -n "${FUGUE_NAMESPACE}" --max 1 | awk 'NR==2 {print $1}'
}

rollout_status() {
  local deployment_name="$1"
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" rollout status "deploy/${deployment_name}" --timeout="${FUGUE_ROLLOUT_TIMEOUT}"
}

apply_chart_crds() {
  local crd_dir="${FUGUE_HELM_CHART_PATH}/crds"

  if [[ ! -d "${crd_dir}" ]]; then
    log "skip CRD apply because ${crd_dir} does not exist"
    return 0
  fi

  if ! find "${crd_dir}" -maxdepth 1 -type f \( -name '*.yaml' -o -name '*.yml' \) | grep -q .; then
    log "skip CRD apply because ${crd_dir} has no manifest files"
    return 0
  fi

  log "applying Helm CRDs from ${crd_dir}"
  ${KUBECTL} apply -f "${crd_dir}"
  ${KUBECTL} wait --for=condition=Established --timeout=60s -f "${crd_dir}"
}

deployment_exists() {
  local deployment_name="$1"
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get "deploy/${deployment_name}" >/dev/null 2>&1
}

smoke_test() {
  require_env FUGUE_SMOKE_URL
  curl -fsS --max-time 10 "${FUGUE_SMOKE_URL}" >/dev/null
}

rollback_release() {
  local rollback_api_deployment="${FUGUE_API_DEPLOYMENT_NAME}"

  if [[ -z "${PREVIOUS_REVISION:-}" ]]; then
    log "skip rollback because no previous revision was captured"
    return 1
  fi

  log "rolling back release ${FUGUE_RELEASE_NAME} to revision ${PREVIOUS_REVISION}"
  helm rollback "${FUGUE_RELEASE_NAME}" "${PREVIOUS_REVISION}" \
    -n "${FUGUE_NAMESPACE}" \
    --timeout "${FUGUE_HELM_TIMEOUT}"

  if ! deployment_exists "${rollback_api_deployment}" && deployment_exists "${FUGUE_LEGACY_API_DEPLOYMENT_NAME}"; then
    rollback_api_deployment="${FUGUE_LEGACY_API_DEPLOYMENT_NAME}"
  fi

  rollout_status "${rollback_api_deployment}"
  if deployment_exists "${FUGUE_CONTROLLER_DEPLOYMENT_NAME}"; then
    rollout_status "${FUGUE_CONTROLLER_DEPLOYMENT_NAME}"
  else
    log "rollback target does not include ${FUGUE_CONTROLLER_DEPLOYMENT_NAME}; skipping controller rollout check"
  fi
  retry "${FUGUE_SMOKE_RETRIES}" "${FUGUE_SMOKE_DELAY_SECONDS}" smoke_test
}

main() {
  require_env FUGUE_API_IMAGE_REPOSITORY
  require_env FUGUE_API_IMAGE_TAG
  require_env FUGUE_CONTROLLER_IMAGE_REPOSITORY
  require_env FUGUE_CONTROLLER_IMAGE_TAG

  export KUBECONFIG="${KUBECONFIG:-${HOME}/.kube/config}"
  KUBECTL="$(detect_kubectl)"
  export KUBECTL

  FUGUE_RELEASE_NAME="${FUGUE_RELEASE_NAME:-fugue}"
  FUGUE_NAMESPACE="${FUGUE_NAMESPACE:-fugue-system}"
  FUGUE_HELM_CHART_PATH="${FUGUE_HELM_CHART_PATH:-deploy/helm/fugue}"
  FUGUE_RELEASE_FULLNAME="${FUGUE_RELEASE_FULLNAME:-${FUGUE_RELEASE_NAME}-fugue}"
  FUGUE_API_DEPLOYMENT_NAME="${FUGUE_API_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}-api}"
  FUGUE_LEGACY_API_DEPLOYMENT_NAME="${FUGUE_LEGACY_API_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}}"
  FUGUE_CONTROLLER_DEPLOYMENT_NAME="${FUGUE_CONTROLLER_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}-controller}"
  FUGUE_HELM_TIMEOUT="${FUGUE_HELM_TIMEOUT:-10m0s}"
  FUGUE_ROLLOUT_TIMEOUT="${FUGUE_ROLLOUT_TIMEOUT:-300s}"
  FUGUE_SMOKE_RETRIES="${FUGUE_SMOKE_RETRIES:-12}"
  FUGUE_SMOKE_DELAY_SECONDS="${FUGUE_SMOKE_DELAY_SECONDS:-5}"
  FUGUE_API_REPLICA_COUNT="${FUGUE_API_REPLICA_COUNT:-2}"
  FUGUE_CONTROLLER_REPLICA_COUNT="${FUGUE_CONTROLLER_REPLICA_COUNT:-2}"
  FUGUE_REGISTRY_NODEPORT="${FUGUE_REGISTRY_NODEPORT:-30500}"
  FUGUE_REGISTRY_SERVICE_PORT="${FUGUE_REGISTRY_SERVICE_PORT:-5000}"

  if [[ -z "${FUGUE_REGISTRY_PUSH_BASE:-}" ]]; then
    FUGUE_REGISTRY_PUSH_BASE="${FUGUE_RELEASE_FULLNAME}-registry.${FUGUE_NAMESPACE}.svc.cluster.local:${FUGUE_REGISTRY_SERVICE_PORT}"
  fi
  if [[ -z "${FUGUE_REGISTRY_PULL_BASE:-}" ]]; then
    FUGUE_REGISTRY_PULL_BASE="$(detect_existing_registry_pull_base || true)"
  fi
  if [[ -z "${FUGUE_REGISTRY_PULL_BASE:-}" ]]; then
    if [[ -z "${FUGUE_CLUSTER_INTERNAL_IP:-}" ]]; then
      FUGUE_CLUSTER_INTERNAL_IP="$(detect_primary_private_ip)"
    fi
    [[ -n "${FUGUE_CLUSTER_INTERNAL_IP}" ]] || fail "failed to detect cluster internal IP for registry pull base"
    FUGUE_REGISTRY_PULL_BASE="${FUGUE_CLUSTER_INTERNAL_IP}:${FUGUE_REGISTRY_NODEPORT}"
  fi
  if [[ -z "${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT:-}" ]]; then
    cluster_join_server="$(detect_cluster_join_server)"
    FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT="$(registry_endpoint_from_join_server "${cluster_join_server}")"
  fi
  if [[ -z "${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT:-}" ]]; then
    mesh_ip="$(detect_primary_mesh_ip || true)"
    if [[ -n "${mesh_ip}" ]]; then
      FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT="${mesh_ip}:${FUGUE_REGISTRY_NODEPORT}"
    else
      FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT="${FUGUE_REGISTRY_PULL_BASE}"
    fi
  fi

  if [[ -z "${FUGUE_SMOKE_URL:-}" && -n "${FUGUE_API_PUBLIC_DOMAIN:-}" ]]; then
    FUGUE_SMOKE_URL="https://${FUGUE_API_PUBLIC_DOMAIN}/healthz"
  fi
  require_env FUGUE_SMOKE_URL

  command_exists helm || fail "helm is not installed"
  ${KUBECTL} version --client >/dev/null
  helm status "${FUGUE_RELEASE_NAME}" -n "${FUGUE_NAMESPACE}" >/dev/null

  PREVIOUS_REVISION="$(helm_current_revision)"
  [[ -n "${PREVIOUS_REVISION}" ]] || fail "failed to detect current Helm revision"

  log "upgrading ${FUGUE_RELEASE_NAME} in namespace ${FUGUE_NAMESPACE}"
  log "api image: ${FUGUE_API_IMAGE_REPOSITORY}:${FUGUE_API_IMAGE_TAG}"
  log "controller image: ${FUGUE_CONTROLLER_IMAGE_REPOSITORY}:${FUGUE_CONTROLLER_IMAGE_TAG}"
  log "previous Helm revision: ${PREVIOUS_REVISION}"
  log "registry push base: ${FUGUE_REGISTRY_PUSH_BASE}"
  log "registry pull base: ${FUGUE_REGISTRY_PULL_BASE}"
  log "cluster join registry endpoint: ${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT}"

  apply_chart_crds

  # Do not use Helm's release-wide --wait here. It waits on every resource in
  # the chart, including DaemonSets scheduled onto stale/NotReady nodes. That
  # can deadlock control-plane upgrades exactly when the new API needs to clean
  # up those stale nodes. We gate success on targeted API/controller rollout
  # checks plus the smoke test below instead.
  if ! helm upgrade "${FUGUE_RELEASE_NAME}" "${FUGUE_HELM_CHART_PATH}" \
    -n "${FUGUE_NAMESPACE}" \
    --reset-then-reuse-values \
    --history-max 20 \
    --timeout "${FUGUE_HELM_TIMEOUT}" \
    --set-string api.image.repository="${FUGUE_API_IMAGE_REPOSITORY}" \
    --set-string api.image.tag="${FUGUE_API_IMAGE_TAG}" \
    --set-string controller.image.repository="${FUGUE_CONTROLLER_IMAGE_REPOSITORY}" \
    --set-string controller.image.tag="${FUGUE_CONTROLLER_IMAGE_TAG}" \
    --set-string api.registryPushBase="${FUGUE_REGISTRY_PUSH_BASE}" \
    --set-string api.registryPullBase="${FUGUE_REGISTRY_PULL_BASE}" \
    --set-string api.clusterJoinRegistryEndpoint="${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT}" \
    --set api.replicaCount="${FUGUE_API_REPLICA_COUNT}" \
    --set api.hostNetwork=false \
    --set api.minReadySeconds=5 \
    --set api.terminationGracePeriodSeconds=40 \
    --set api.podDisruptionBudget.enabled=true \
    --set api.podDisruptionBudget.minAvailable=1 \
    --set-string api.shutdownDrainDelay=5s \
    --set-string api.shutdownTimeout=25s \
    --set controller.replicaCount="${FUGUE_CONTROLLER_REPLICA_COUNT}" \
    --set-string controller.fallbackPollInterval=30s \
    --set controller.terminationGracePeriodSeconds=30 \
    --set controller.podDisruptionBudget.enabled=true \
    --set controller.podDisruptionBudget.minAvailable=1 \
    --set controller.leaderElection.enabled=true \
    --set-string controller.leaderElection.leaseName="${FUGUE_CONTROLLER_DEPLOYMENT_NAME}" \
    --set-string controller.leaderElection.leaseNamespace="${FUGUE_NAMESPACE}" \
    --set-string controller.leaderElection.leaseDuration=15s \
    --set-string controller.leaderElection.renewDeadline=10s \
    --set-string controller.leaderElection.retryPeriod=2s \
    --set-string controller.migrationGuard.legacyControllerContainerName=controller \
    --set-string controller.migrationGuard.checkInterval=2s; then
    log "helm upgrade failed; attempting rollback"
    rollback_release || true
    fail "helm upgrade failed"
  fi

  if ! rollout_status "${FUGUE_API_DEPLOYMENT_NAME}"; then
    log "api rollout check failed; attempting rollback"
    rollback_release || true
    fail "api rollout failed"
  fi

  if ! rollout_status "${FUGUE_CONTROLLER_DEPLOYMENT_NAME}"; then
    log "controller rollout check failed; attempting rollback"
    rollback_release || true
    fail "controller rollout failed"
  fi

  if ! retry "${FUGUE_SMOKE_RETRIES}" "${FUGUE_SMOKE_DELAY_SECONDS}" smoke_test; then
    log "smoke test failed; attempting rollback"
    rollback_release || true
    fail "smoke test failed"
  fi

  local current_revision
  current_revision="$(helm_current_revision)"
  log "upgrade complete; current Helm revision=${current_revision}"
}

main "$@"
