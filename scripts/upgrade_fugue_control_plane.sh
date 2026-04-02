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

CONTROL_PLANE_AUTOMATION_TMP_DIR=""
UPGRADE_OVERRIDE_VALUES_FILE=""
LOCAL_CONTROL_PLANE_AUTOMATION_DIR="${FUGUE_LOCAL_CONTROL_PLANE_AUTOMATION_DIR:-${HOME}/.config/fugue/control-plane-automation}"
LOCAL_ROOT_CONTROL_PLANE_AUTOMATION_DIR="${FUGUE_LOCAL_ROOT_CONTROL_PLANE_AUTOMATION_DIR:-/root/.config/fugue/control-plane-automation}"
CONTROL_PLANE_HOSTS_ENV_LOADED="false"
PRIMARY_CONTROL_PLANE_SSH_OPTS=()
PRIMARY_DISK_PRESSURE_CLEAR_POLL_SECONDS="${FUGUE_PRIMARY_DISK_PRESSURE_CLEAR_POLL_SECONDS:-5}"
# Kubelet delays clearing DiskPressure for evictionPressureTransitionPeriod
# (5m by default on our k3s nodes), so keep a wider recovery window here.
PRIMARY_DISK_PRESSURE_CLEAR_TIMEOUT_SECONDS="${FUGUE_PRIMARY_DISK_PRESSURE_CLEAR_TIMEOUT_SECONDS:-600}"
PRIMARY_NODE_READY_POLL_SECONDS="${FUGUE_PRIMARY_NODE_READY_POLL_SECONDS:-5}"
PRIMARY_NODE_READY_TIMEOUT_SECONDS="${FUGUE_PRIMARY_NODE_READY_TIMEOUT_SECONDS:-300}"
PRIMARY_POSTGRES_DATA_ROOT="${FUGUE_PRIMARY_POSTGRES_DATA_ROOT:-/var/lib/fugue/postgres}"
PRIMARY_POSTGRES_IMAGE="${FUGUE_PRIMARY_POSTGRES_IMAGE:-docker.io/library/postgres:16-alpine}"

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

cleanup_control_plane_automation_tmp() {
  if [[ -n "${CONTROL_PLANE_AUTOMATION_TMP_DIR}" && -d "${CONTROL_PLANE_AUTOMATION_TMP_DIR}" ]]; then
    rm -rf "${CONTROL_PLANE_AUTOMATION_TMP_DIR}"
  fi
}

cleanup_upgrade_override_values() {
  if [[ -n "${UPGRADE_OVERRIDE_VALUES_FILE}" && -f "${UPGRADE_OVERRIDE_VALUES_FILE}" ]]; then
    rm -f "${UPGRADE_OVERRIDE_VALUES_FILE}"
  fi
}

cleanup_tmp_artifacts() {
  cleanup_control_plane_automation_tmp
  cleanup_upgrade_override_values
}

write_upgrade_override_values() {
  UPGRADE_OVERRIDE_VALUES_FILE="$(mktemp -t fugue-upgrade-values.XXXXXX.yaml)"
  cat >"${UPGRADE_OVERRIDE_VALUES_FILE}" <<'EOF'
tolerations:
  - key: node.kubernetes.io/disk-pressure
    operator: Exists
    effect: NoSchedule
api:
  # Explicit non-empty tolerations prevent Helm's `default` fallback from
  # inheriting the global disk-pressure toleration onto stateless workloads.
  tolerations:
    - key: node.kubernetes.io/not-ready
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
    - key: node.kubernetes.io/unreachable
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
controller:
  tolerations:
    - key: node.kubernetes.io/not-ready
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
    - key: node.kubernetes.io/unreachable
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
snapshotController:
  tolerations:
    - key: node.kubernetes.io/not-ready
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
    - key: node.kubernetes.io/unreachable
      operator: Exists
      effect: NoExecute
      tolerationSeconds: 300
EOF
  printf '%s' "${UPGRADE_OVERRIDE_VALUES_FILE}"
}

use_local_control_plane_automation_bundle_from_dir() {
  local bundle_dir="$1"

  [[ -r "${bundle_dir}/hosts.env" ]] || return 1
  [[ -r "${bundle_dir}/id_ed25519" ]] || return 1
  [[ -r "${bundle_dir}/known_hosts" ]] || return 1

  export FUGUE_CONTROL_PLANE_HOSTS_ENV_FILE="${bundle_dir}/hosts.env"
  export FUGUE_CONTROL_PLANE_SSH_KEY_FILE="${bundle_dir}/id_ed25519"
  export FUGUE_CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE="${bundle_dir}/known_hosts"
  export FUGUE_USE_CONTROL_PLANE_AUTOMATION_SSH=true
  log "using local control-plane automation bundle from ${bundle_dir}"
  return 0
}

prepare_control_plane_automation_ssh() {
  if [[ -n "${FUGUE_CONTROL_PLANE_HOSTS_ENV_FILE:-}" && -r "${FUGUE_CONTROL_PLANE_HOSTS_ENV_FILE}" ]] && \
     [[ -n "${FUGUE_CONTROL_PLANE_SSH_KEY_FILE:-}" && -r "${FUGUE_CONTROL_PLANE_SSH_KEY_FILE}" ]] && \
     [[ -n "${FUGUE_CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE:-}" && -r "${FUGUE_CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE}" ]]; then
    export FUGUE_USE_CONTROL_PLANE_AUTOMATION_SSH=true
    return
  fi

  if use_local_control_plane_automation_bundle_from_dir "${LOCAL_CONTROL_PLANE_AUTOMATION_DIR}"; then
    return
  fi
  if [[ "${LOCAL_ROOT_CONTROL_PLANE_AUTOMATION_DIR}" != "${LOCAL_CONTROL_PLANE_AUTOMATION_DIR}" ]] && \
     use_local_control_plane_automation_bundle_from_dir "${LOCAL_ROOT_CONTROL_PLANE_AUTOMATION_DIR}"; then
    return
  fi
  fail "missing local control-plane automation bundle on this server; run scripts/bootstrap_control_plane_automation.sh or scripts/install_fugue_ha.sh to install it"
}

load_control_plane_hosts_env() {
  if [[ "${CONTROL_PLANE_HOSTS_ENV_LOADED}" == "true" ]]; then
    return
  fi
  CONTROL_PLANE_HOSTS_ENV_LOADED="true"
  # shellcheck disable=SC1090
  source "${FUGUE_CONTROL_PLANE_HOSTS_ENV_FILE}"
}

primary_control_plane_ssh_login() {
  load_control_plane_hosts_env
  local host="${FUGUE_NODE1_HOST:-${FUGUE_NODE1_ALIAS:-}}"
  local user="${FUGUE_NODE1_USER:-}"
  [[ -n "${host}" ]] || fail "primary control-plane SSH host is not configured"
  if [[ -n "${user}" ]]; then
    printf '%s@%s' "${user}" "${host}"
    return
  fi
  printf '%s' "${host}"
}

build_primary_control_plane_ssh_opts() {
  load_control_plane_hosts_env
  PRIMARY_CONTROL_PLANE_SSH_OPTS=(
    -o BatchMode=yes
    -o ConnectTimeout=15
    -o ServerAliveInterval=15
    -o ServerAliveCountMax=3
    -o IdentitiesOnly=yes
    -i "${FUGUE_CONTROL_PLANE_SSH_KEY_FILE}"
    -o StrictHostKeyChecking=yes
    -o UserKnownHostsFile="${FUGUE_CONTROL_PLANE_SSH_KNOWN_HOSTS_FILE}"
  )
  if [[ -n "${FUGUE_NODE1_PORT:-}" ]]; then
    PRIMARY_CONTROL_PLANE_SSH_OPTS+=(-p "${FUGUE_NODE1_PORT}")
  fi
}

detect_primary_node_name() {
  ${KUBECTL} get nodes -l fugue.install/role=primary -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

primary_node_is_ready() {
  local node_name="$1"
  local status=""

  if command_exists timeout; then
    status="$(timeout 15s ${KUBECTL} get node "${node_name}" -o jsonpath='{range .status.conditions[?(@.type=="Ready")]}{.status}{end}' 2>/dev/null || true)"
  else
    status="$(${KUBECTL} get node "${node_name}" -o jsonpath='{range .status.conditions[?(@.type=="Ready")]}{.status}{end}' 2>/dev/null || true)"
  fi
  [[ "${status}" == "True" ]]
}

primary_node_has_disk_pressure() {
  local node_name="$1"
  local status=""
  if command_exists timeout; then
    status="$(timeout 15s ${KUBECTL} get node "${node_name}" -o jsonpath='{range .status.conditions[?(@.type=="DiskPressure")]}{.status}{end}' 2>/dev/null || true)"
  else
    status="$(${KUBECTL} get node "${node_name}" -o jsonpath='{range .status.conditions[?(@.type=="DiskPressure")]}{.status}{end}' 2>/dev/null || true)"
  fi
  [[ "${status}" == "True" ]]
}

run_primary_host_root_command() {
  local primary_node_name="$1"
  local cmd="$2"
  local local_hostname=""
  local local_hostname_short=""

  local_hostname="$(hostname 2>/dev/null || true)"
  local_hostname_short="$(hostname -s 2>/dev/null || true)"
  if [[ "${local_hostname}" == "${primary_node_name}" || "${local_hostname_short}" == "${primary_node_name}" ]]; then
    sudo bash -lc "${cmd}"
    return
  fi

  prepare_control_plane_automation_ssh
  build_primary_control_plane_ssh_opts
  ssh -n "${PRIMARY_CONTROL_PLANE_SSH_OPTS[@]}" "$(primary_control_plane_ssh_login)" \
    "sudo bash -lc $(printf '%q' "${cmd}")"
}

wait_for_primary_node_ready() {
  local primary_node_name="$1"
  local attempt
  local max_attempts

  if ! [[ "${PRIMARY_NODE_READY_POLL_SECONDS}" =~ ^[0-9]+$ ]] || (( PRIMARY_NODE_READY_POLL_SECONDS <= 0 )); then
    fail "FUGUE_PRIMARY_NODE_READY_POLL_SECONDS must be a positive integer"
  fi
  if ! [[ "${PRIMARY_NODE_READY_TIMEOUT_SECONDS}" =~ ^[0-9]+$ ]] || (( PRIMARY_NODE_READY_TIMEOUT_SECONDS <= 0 )); then
    fail "FUGUE_PRIMARY_NODE_READY_TIMEOUT_SECONDS must be a positive integer"
  fi

  max_attempts=$(( (PRIMARY_NODE_READY_TIMEOUT_SECONDS + PRIMARY_NODE_READY_POLL_SECONDS - 1) / PRIMARY_NODE_READY_POLL_SECONDS ))
  log "waiting up to ${PRIMARY_NODE_READY_TIMEOUT_SECONDS}s for primary node ${primary_node_name} to report Ready"

  for attempt in $(seq 1 "${max_attempts}"); do
    if primary_node_is_ready "${primary_node_name}"; then
      return 0
    fi
    sleep "${PRIMARY_NODE_READY_POLL_SECONDS}"
  done
  return 1
}

wait_for_primary_disk_pressure_clear() {
  local primary_node_name="$1"
  local attempt
  local max_attempts

  if ! [[ "${PRIMARY_DISK_PRESSURE_CLEAR_POLL_SECONDS}" =~ ^[0-9]+$ ]] || (( PRIMARY_DISK_PRESSURE_CLEAR_POLL_SECONDS <= 0 )); then
    fail "FUGUE_PRIMARY_DISK_PRESSURE_CLEAR_POLL_SECONDS must be a positive integer"
  fi
  if ! [[ "${PRIMARY_DISK_PRESSURE_CLEAR_TIMEOUT_SECONDS}" =~ ^[0-9]+$ ]] || (( PRIMARY_DISK_PRESSURE_CLEAR_TIMEOUT_SECONDS <= 0 )); then
    fail "FUGUE_PRIMARY_DISK_PRESSURE_CLEAR_TIMEOUT_SECONDS must be a positive integer"
  fi

  max_attempts=$(( (PRIMARY_DISK_PRESSURE_CLEAR_TIMEOUT_SECONDS + PRIMARY_DISK_PRESSURE_CLEAR_POLL_SECONDS - 1) / PRIMARY_DISK_PRESSURE_CLEAR_POLL_SECONDS ))
  log "waiting up to ${PRIMARY_DISK_PRESSURE_CLEAR_TIMEOUT_SECONDS}s for primary node ${primary_node_name} to clear DiskPressure"

  for attempt in $(seq 1 "${max_attempts}"); do
    if ! primary_node_has_disk_pressure "${primary_node_name}"; then
      return 0
    fi
    sleep "${PRIMARY_DISK_PRESSURE_CLEAR_POLL_SECONDS}"
  done
  return 1
}

release_pod_selector() {
  printf 'app.kubernetes.io/instance=%s,app.kubernetes.io/name=fugue' "${FUGUE_RELEASE_NAME}"
}

release_pod_count_by_phase() {
  local phase="$1"
  local output=""

  if command_exists timeout; then
    output="$(timeout 30s ${KUBECTL} -n "${FUGUE_NAMESPACE}" get pods \
      -l "$(release_pod_selector)" \
      --field-selector "status.phase=${phase}" \
      --no-headers 2>/dev/null || true)"
  else
    output="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get pods \
      -l "$(release_pod_selector)" \
      --field-selector "status.phase=${phase}" \
      --no-headers 2>/dev/null || true)"
  fi

  printf '%s\n' "${output}" | awk 'NF > 0 {count++} END {print count + 0}'
}

prune_release_pods_by_phase() {
  local phase="$1"
  local count=""

  count="$(release_pod_count_by_phase "${phase}")"
  [[ "${count}" != "0" ]] || return 0

  log "deleting ${count} ${phase} Fugue release pods from ${FUGUE_NAMESPACE}"
  if command_exists timeout; then
    if timeout 60s ${KUBECTL} -n "${FUGUE_NAMESPACE}" delete pod \
      -l "$(release_pod_selector)" \
      --field-selector "status.phase=${phase}" \
      --ignore-not-found \
      --wait=false >/dev/null 2>&1; then
      return 0
    fi
    log "warning: failed to delete ${phase} Fugue release pods from ${FUGUE_NAMESPACE}"
    return 0
  fi

  if ! ${KUBECTL} -n "${FUGUE_NAMESPACE}" delete pod \
    -l "$(release_pod_selector)" \
    --field-selector "status.phase=${phase}" \
    --ignore-not-found \
    --wait=false >/dev/null 2>&1; then
    log "warning: failed to delete ${phase} Fugue release pods from ${FUGUE_NAMESPACE}"
  fi
}

prune_terminated_release_pods() {
  prune_release_pods_by_phase Failed
  prune_release_pods_by_phase Succeeded
  prune_release_pods_by_phase Unknown
}

recover_primary_node_if_needed() {
  local primary_node_name=""
  local restart_cmd=""

  primary_node_name="$(detect_primary_node_name)"
  if [[ -z "${primary_node_name}" ]]; then
    log "skip primary node recovery because the primary node could not be identified"
    return 0
  fi

  prune_terminated_release_pods

  if primary_node_is_ready "${primary_node_name}"; then
    return 0
  fi

  log "primary node ${primary_node_name} is NotReady; restarting k3s on the primary host"
  restart_cmd="$(cat <<'EOF'
set -euo pipefail

if command -v k3s >/dev/null 2>&1; then
  k3s crictl rmi --prune >/tmp/fugue-primary-node-image-prune.log 2>&1 || true
fi

systemctl restart k3s
systemctl is-active --quiet k3s
EOF
)"

  if ! run_primary_host_root_command "${primary_node_name}" "${restart_cmd}"; then
    fail "primary node recovery failed while restarting k3s on ${primary_node_name}"
  fi

  if ! wait_for_primary_node_ready "${primary_node_name}"; then
    fail "primary node ${primary_node_name} remained NotReady after restarting k3s"
  fi

  prune_terminated_release_pods
}

control_plane_postgres_selector() {
  printf 'app.kubernetes.io/component=postgres,app.kubernetes.io/instance=%s,app.kubernetes.io/name=fugue' "${FUGUE_RELEASE_NAME}"
}

control_plane_postgres_pod_status_lines() {
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get pods \
    -l "$(control_plane_postgres_selector)" \
    --sort-by=.metadata.creationTimestamp \
    -o custom-columns=NAME:.metadata.name,READY:.status.containerStatuses[0].ready,PHASE:.status.phase,RESTARTS:.status.containerStatuses[0].restartCount \
    --no-headers 2>/dev/null | tail -n 10
}

control_plane_postgres_pod_summary() {
  control_plane_postgres_pod_status_lines | awk '
    NF > 0 {
      printf "%s%s(ready=%s phase=%s restarts=%s)", sep, $1, $2, $3, $4
      sep = ", "
    }
    END {
      if (NR == 0) {
        printf "none"
      }
    }
  '
}

control_plane_postgres_pod_names() {
  control_plane_postgres_pod_status_lines | awk 'NF > 0 {lines[++count]=$1} END {for (i=count; i>=1; i--) print lines[i]}'
}

control_plane_postgres_logs() {
  local pod_name="$1"
  local logs=""

  [[ -n "${pod_name}" ]] || return 0
  if command_exists timeout; then
    logs="$(timeout 15s ${KUBECTL} -n "${FUGUE_NAMESPACE}" logs "pod/${pod_name}" --previous --tail=200 2>/dev/null || true)"
  else
    logs="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" logs "pod/${pod_name}" --previous --tail=200 2>/dev/null || true)"
  fi
  if [[ -z "${logs}" ]]; then
    if command_exists timeout; then
      logs="$(timeout 15s ${KUBECTL} -n "${FUGUE_NAMESPACE}" logs "pod/${pod_name}" --tail=200 2>/dev/null || true)"
    else
      logs="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" logs "pod/${pod_name}" --tail=200 2>/dev/null || true)"
    fi
  fi
  printf '%s' "${logs}"
}

control_plane_postgres_has_invalid_checkpoint() {
  local pod_name="$1"
  local logs=""

  logs="$(control_plane_postgres_logs "${pod_name}")"
  [[ "${logs}" == *"invalid resource manager ID in checkpoint record"* ]] || \
    [[ "${logs}" == *"could not locate a valid checkpoint record"* ]]
}

control_plane_postgres_has_ready_pod() {
  control_plane_postgres_pod_status_lines | awk 'NF > 0 && $2 == "true" && $3 == "Running" {found=1} END {exit found ? 0 : 1}'
}

invalid_checkpoint_control_plane_postgres_pod_name() {
  local pod_name=""
  local attempt=""
  local pod_summary=""

  for attempt in $(seq 1 6); do
    pod_summary="$(control_plane_postgres_pod_summary)"
    while IFS= read -r pod_name; do
      [[ -n "${pod_name}" ]] || continue
      if control_plane_postgres_has_invalid_checkpoint "${pod_name}"; then
        printf '%s' "${pod_name}"
        return 0
      fi
    done < <(control_plane_postgres_pod_names)

    if control_plane_postgres_has_ready_pod; then
      return 1
    fi

    if (( attempt == 6 )); then
      log "control-plane postgres still has no ready pods after ${attempt} checks; inspected ${pod_summary}"
      return 2
    fi

    log "control-plane postgres has no ready pods yet; inspected ${pod_summary}; waiting before checking WAL corruption again"
    sleep 5
  done

  return 1
}

wait_for_control_plane_postgres_pods_gone() {
  local attempt
  local names=""

  for attempt in $(seq 1 24); do
    names="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get pods \
      -l "$(control_plane_postgres_selector)" \
      -o go-template='{{range .items}}{{if eq .status.phase "Running"}}{{.metadata.name}} {{end}}{{end}}' 2>/dev/null || true)"
    if [[ -z "${names}" ]]; then
      return 0
    fi
    sleep 5
  done
  return 1
}

recover_primary_postgres_if_needed() {
  local primary_node_name=""
  local postgres_pod_name=""
  local original_replicas=""
  local repair_cmd=""
  local detect_status=0

  primary_node_name="$(detect_primary_node_name)"
  if [[ -z "${primary_node_name}" ]]; then
    log "skip primary postgres recovery because the primary node could not be identified"
    return 0
  fi

  postgres_pod_name="$(invalid_checkpoint_control_plane_postgres_pod_name)" || detect_status=$?
  case "${detect_status}" in
    0)
      ;;
    1)
      return 0
      ;;
    2)
      fail "control-plane postgres had no ready pods before upgrade, but no invalid-checkpoint signature was found in recent logs"
      ;;
    *)
      fail "control-plane postgres recovery pre-check failed with unexpected status ${detect_status}"
      ;;
  esac

  if [[ -z "${postgres_pod_name}" ]]; then
    fail "control-plane postgres recovery pre-check succeeded without returning a pod name"
  fi

  log "detected invalid checkpoint in control-plane postgres pod ${postgres_pod_name}; resetting WAL on the primary host"
  original_replicas="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get deploy "${FUGUE_POSTGRES_DEPLOYMENT_NAME}" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
  [[ -n "${original_replicas}" ]] || original_replicas="1"

  ${KUBECTL} -n "${FUGUE_NAMESPACE}" scale deploy "${FUGUE_POSTGRES_DEPLOYMENT_NAME}" --replicas=0 >/dev/null
  if ! wait_for_control_plane_postgres_pods_gone; then
    ${KUBECTL} -n "${FUGUE_NAMESPACE}" scale deploy "${FUGUE_POSTGRES_DEPLOYMENT_NAME}" --replicas="${original_replicas}" >/dev/null || true
    fail "control-plane postgres pods did not terminate before WAL recovery"
  fi

  repair_cmd="$(cat <<EOF
set -euo pipefail

log() {
  printf '[fugue-upgrade][primary-postgres-repair] %s\n' "\$*"
}

postgres_root=$(printf '%q' "${PRIMARY_POSTGRES_DATA_ROOT}")
pgdata="\${postgres_root}/pgdata"
backup_dir="\${postgres_root}/pgdata.pre-resetwal-\$(date -u +%Y%m%dT%H%M%SZ)"
postgres_image=$(printf '%q' "${PRIMARY_POSTGRES_IMAGE}")
repair_id="fugue-postgres-repair-\$(date +%s)"

cleanup() {
  k3s ctr tasks kill "\${repair_id}" >/dev/null 2>&1 || true
  k3s ctr containers rm "\${repair_id}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

if [[ ! -d "\${pgdata}" ]]; then
  log "postgres data directory \${pgdata} does not exist; skipping WAL recovery"
  exit 0
fi

cp -a "\${pgdata}" "\${backup_dir}"
log "backed up \${pgdata} to \${backup_dir}"

rm -f "\${pgdata}/postmaster.pid"

if ! k3s ctr images ls | awk 'NR > 1 {print \$1}' | grep -Fxq "\${postgres_image}"; then
  log "pulling \${postgres_image}"
  k3s ctr images pull "\${postgres_image}"
fi

log "running pg_resetwal against \${pgdata}"
timeout 300s k3s ctr run --rm \
  --mount type=bind,src="\${postgres_root}",dst=/var/lib/postgresql/data,options=rbind:rw \
  "\${postgres_image}" "\${repair_id}" \
  sh -lc 'set -euo pipefail; chown -R 70:70 /var/lib/postgresql/data; su-exec postgres pg_resetwal -f /var/lib/postgresql/data/pgdata'

log "pg_resetwal completed"
EOF
)"
  if ! run_primary_host_root_command "${primary_node_name}" "${repair_cmd}"; then
    ${KUBECTL} -n "${FUGUE_NAMESPACE}" scale deploy "${FUGUE_POSTGRES_DEPLOYMENT_NAME}" --replicas="${original_replicas}" >/dev/null || true
    fail "control-plane postgres WAL recovery failed"
  fi

  ${KUBECTL} -n "${FUGUE_NAMESPACE}" scale deploy "${FUGUE_POSTGRES_DEPLOYMENT_NAME}" --replicas="${original_replicas}" >/dev/null
}

relieve_primary_disk_pressure() {
  local primary_node_name=""
  local cleanup_cmd=""

  primary_node_name="$(detect_primary_node_name)"
  if [[ -z "${primary_node_name}" ]]; then
    log "skip primary disk-pressure recovery because the primary node could not be identified"
    return 0
  fi
  if ! primary_node_has_disk_pressure "${primary_node_name}"; then
    return 0
  fi

  log "primary node ${primary_node_name} is under DiskPressure; running host-level registry cleanup before upgrade"
  cleanup_cmd="$(cat <<'EOF'
set -euo pipefail

log() {
  printf '[fugue-upgrade][primary-cleanup] %s\n' "$*"
}

registry_root="/var/lib/fugue/registry"
runner_update_root="/home/github-runner/actions-runner-work/_update"
registry_image="docker.io/library/registry:2.8.3"
gc_id="fugue-registry-gc-$(date +%s)"

cleanup() {
  k3s ctr tasks kill "${gc_id}" >/dev/null 2>&1 || true
  k3s ctr containers rm "${gc_id}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

log "filesystem usage before cleanup"
df -h /
du -sh "${registry_root}" 2>/dev/null || true

if command -v k3s >/dev/null 2>&1; then
  if k3s crictl rmi --prune >/tmp/fugue-primary-image-prune.log 2>&1; then
    log "unused k3s images pruned"
  else
    status=$?
    log "image prune returned ${status}; continuing"
  fi
fi

if [[ -d "${runner_update_root}" ]] && find "${runner_update_root}" -mindepth 0 -mmin "+1440" | grep -q .; then
  rm -rf -- "${runner_update_root}"
  mkdir -p "${runner_update_root}"
  chown -R github-runner:github-runner "${runner_update_root}" >/dev/null 2>&1 || true
  log "removed stale runner update cache ${runner_update_root}"
fi

if [[ ! -d "${registry_root}/docker/registry/v2" ]]; then
  log "registry data root ${registry_root} is absent; skipping offline registry GC"
  exit 0
fi

if ! k3s ctr images ls | awk 'NR > 1 {print $1}' | grep -Fxq "${registry_image}"; then
  log "pulling ${registry_image} for offline registry GC"
  k3s ctr images pull "${registry_image}"
fi

log "running offline registry garbage-collect against ${registry_root}"
timeout 600s k3s ctr run --rm \
  --mount type=bind,src="${registry_root}",dst=/var/lib/registry,options=rbind:rw \
  "${registry_image}" "${gc_id}" \
  registry garbage-collect --delete-untagged /etc/docker/registry/config.yml

log "filesystem usage after cleanup"
du -sh "${registry_root}" 2>/dev/null || true
df -h /
EOF
)"
  run_primary_host_root_command "${primary_node_name}" "${cleanup_cmd}"

  if ! wait_for_primary_disk_pressure_clear "${primary_node_name}"; then
    fail "primary node ${primary_node_name} still reports DiskPressure after host-level registry cleanup"
  fi
}

sync_route_a_edge_proxy() {
  if [[ "${FUGUE_SYNC_EDGE_PROXY:-true}" != "true" ]]; then
    log "skip Route A edge proxy sync because FUGUE_SYNC_EDGE_PROXY=${FUGUE_SYNC_EDGE_PROXY}"
    return
  fi
  if [[ -z "${FUGUE_API_PUBLIC_DOMAIN:-}" ]]; then
    return
  fi

  prepare_control_plane_automation_ssh
  export FUGUE_DOMAIN="${FUGUE_API_PUBLIC_DOMAIN}"
  log "syncing Route A edge proxy through scripts/sync_fugue_edge_proxy.sh"
  bash ./scripts/sync_fugue_edge_proxy.sh
}

label_default_builder_nodes() {
  log "labeling shared control-plane nodes as medium builder candidates"
  ${KUBECTL} label node -l fugue.install/profile=combined \
    fugue.io/build=true \
    fugue.io/build-tier=medium \
    fugue.io/shared-pool=internal \
    --overwrite >/dev/null
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
  trap cleanup_tmp_artifacts EXIT

  FUGUE_RELEASE_NAME="${FUGUE_RELEASE_NAME:-fugue}"
  FUGUE_NAMESPACE="${FUGUE_NAMESPACE:-fugue-system}"
  FUGUE_HELM_CHART_PATH="${FUGUE_HELM_CHART_PATH:-deploy/helm/fugue}"
  FUGUE_RELEASE_FULLNAME="${FUGUE_RELEASE_FULLNAME:-${FUGUE_RELEASE_NAME}-fugue}"
  FUGUE_API_DEPLOYMENT_NAME="${FUGUE_API_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}-api}"
  FUGUE_LEGACY_API_DEPLOYMENT_NAME="${FUGUE_LEGACY_API_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}}"
  FUGUE_CONTROLLER_DEPLOYMENT_NAME="${FUGUE_CONTROLLER_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}-controller}"
  FUGUE_POSTGRES_DEPLOYMENT_NAME="${FUGUE_POSTGRES_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}-postgres}"
  FUGUE_HELM_TIMEOUT="${FUGUE_HELM_TIMEOUT:-10m0s}"
  FUGUE_ROLLOUT_TIMEOUT="${FUGUE_ROLLOUT_TIMEOUT:-300s}"
  FUGUE_SMOKE_RETRIES="${FUGUE_SMOKE_RETRIES:-12}"
  FUGUE_SMOKE_DELAY_SECONDS="${FUGUE_SMOKE_DELAY_SECONDS:-5}"
  FUGUE_API_REPLICA_COUNT="${FUGUE_API_REPLICA_COUNT:-2}"
  FUGUE_CONTROLLER_REPLICA_COUNT="${FUGUE_CONTROLLER_REPLICA_COUNT:-2}"
  FUGUE_REGISTRY_NODEPORT="${FUGUE_REGISTRY_NODEPORT:-30500}"
  FUGUE_REGISTRY_SERVICE_PORT="${FUGUE_REGISTRY_SERVICE_PORT:-5000}"
  FUGUE_API_PUBLIC_DOMAIN="${FUGUE_API_PUBLIC_DOMAIN:-}"
  FUGUE_APP_BASE_DOMAIN="${FUGUE_APP_BASE_DOMAIN:-fugue.pro}"
  FUGUE_CONTROL_PLANE_AUTOMATION_SECRET_NAME="${FUGUE_CONTROL_PLANE_AUTOMATION_SECRET_NAME:-${FUGUE_RELEASE_FULLNAME}-control-plane-automation}"

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
  log "app base domain: ${FUGUE_APP_BASE_DOMAIN}"
  log "custom domain base domain: dns.${FUGUE_APP_BASE_DOMAIN}"

  recover_primary_node_if_needed
  relieve_primary_disk_pressure
  recover_primary_postgres_if_needed

  apply_chart_crds

  upgrade_override_values_file="$(write_upgrade_override_values)"
  log "injecting disk-pressure toleration for primary-pinned hostPath control-plane pods"

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
    -f "${upgrade_override_values_file}" \
    --set-string api.image.repository="${FUGUE_API_IMAGE_REPOSITORY}" \
    --set-string api.image.tag="${FUGUE_API_IMAGE_TAG}" \
    --set-string controller.image.repository="${FUGUE_CONTROLLER_IMAGE_REPOSITORY}" \
    --set-string controller.image.tag="${FUGUE_CONTROLLER_IMAGE_TAG}" \
    --set-string api.appBaseDomain="${FUGUE_APP_BASE_DOMAIN}" \
    --set-string api.apiPublicDomain="${FUGUE_API_PUBLIC_DOMAIN}" \
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

  label_default_builder_nodes

  sync_route_a_edge_proxy

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
