#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

log() {
  printf '[fugue-upgrade] %s\n' "$*"
}

log_stderr() {
  printf '[fugue-upgrade] %s\n' "$*" >&2
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

run_privileged_host_command() {
  if [[ "$(id -u)" == "0" ]]; then
    "$@"
    return
  fi
  if command_exists sudo && sudo -n true >/dev/null 2>&1; then
    sudo -n "$@"
    return
  fi
  return 1
}

host_systemd_unit_file_exists() {
  local unit="$1"
  systemctl list-unit-files "${unit}" 2>/dev/null | awk '{print $1}' | grep -Fqx "${unit}"
}

host_active_time_sync_service() {
  local unit=""
  for unit in chrony.service chronyd.service systemd-timesyncd.service; do
    if systemctl is-active --quiet "${unit}" 2>/dev/null; then
      printf '%s' "${unit}"
      return 0
    fi
  done
  return 1
}

write_privileged_file_if_changed() {
  local source_path="$1"
  local target_path="$2"

  if [[ -f "${target_path}" ]] && cmp -s "${source_path}" "${target_path}"; then
    rm -f "${source_path}"
    return 1
  fi
  if ! run_privileged_host_command mkdir -p "$(dirname "${target_path}")"; then
    rm -f "${source_path}"
    return 2
  fi
  if ! run_privileged_host_command install -m 0644 "${source_path}" "${target_path}"; then
    rm -f "${source_path}"
    return 2
  fi
  rm -f "${source_path}"
  return 0
}

host_time_sync_root_script() {
  cat <<'EOF'
set -euo pipefail

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemctl unavailable; skipping host time synchronization hardening"
  exit 0
fi

host_command_output() {
  if "$@" 2>/dev/null; then
    return 0
  fi
  if command -v nsenter >/dev/null 2>&1 && [ -d /proc/1/ns ]; then
    nsenter -t 1 -m -u -i -n -p "$@" 2>/dev/null && return 0
  fi
  return 1
}

host_command_quiet() {
  if "$@" >/dev/null 2>&1; then
    return 0
  fi
  if command -v nsenter >/dev/null 2>&1 && [ -d /proc/1/ns ]; then
    nsenter -t 1 -m -u -i -n -p "$@" >/dev/null 2>&1 && return 0
  fi
  return 1
}

systemd_unit_file_exists() {
  local unit="$1"
  local path=""

  if host_command_output systemctl list-unit-files "${unit}" | awk '{print $1}' | grep -Fqx "${unit}"; then
    return 0
  fi
  for path in \
    "/etc/systemd/system/${unit}" \
    "/run/systemd/system/${unit}" \
    "/usr/local/lib/systemd/system/${unit}" \
    "/lib/systemd/system/${unit}" \
    "/usr/lib/systemd/system/${unit}"; do
    if [ -e "${path}" ]; then
      return 0
    fi
  done
  return 1
}

restart_time_sync_service() {
  host_command_quiet systemctl restart systemd-timesyncd.service ||
    host_command_quiet timedatectl set-ntp true
}

if ! systemd_unit_file_exists systemd-timesyncd.service; then
  echo "systemd-timesyncd unavailable; skipping host time synchronization hardening"
  exit 0
fi
for unit in chrony.service chronyd.service; do
  if host_command_quiet systemctl is-active --quiet "${unit}"; then
    echo "${unit} is active; leaving systemd-timesyncd poll interval unchanged"
    exit 0
  fi
done

dropin="/etc/systemd/timesyncd.conf.d/10-fugue-managed.conf"
tmp="$(mktemp)"
{
  printf '[Time]\n'
  printf 'PollIntervalMinSec=32s\n'
  printf 'PollIntervalMaxSec=64s\n'
} >"${tmp}"
mkdir -p "$(dirname "${dropin}")"
if [ -f "${dropin}" ] && cmp -s "${tmp}" "${dropin}"; then
  rm -f "${tmp}"
  restart_time_sync_service >/dev/null 2>&1 || true
  echo "systemd-timesyncd poll interval already configured"
  exit 0
fi
install -m 0644 "${tmp}" "${dropin}"
rm -f "${tmp}"
if restart_time_sync_service; then
  echo "configured systemd-timesyncd poll interval min=32s max=64s"
  exit 0
fi
echo "configured systemd-timesyncd poll interval min=32s max=64s but could not restart time synchronization"
exit 1
EOF
}

ensure_primary_host_time_sync_via_ssh() {
  local primary_node_name=""
  local output=""
  local cmd=""

  if [[ -z "${KUBECTL:-}" ]]; then
    return 1
  fi
  primary_node_name="$(detect_primary_node_name)"
  if [[ -z "$(trim_field "${primary_node_name}")" ]]; then
    log_stderr "primary control-plane node not found; cannot harden host time synchronization via SSH"
    return 1
  fi
  cmd="$(host_time_sync_root_script)"
  if output="$(try_primary_host_root_command "${primary_node_name}" "${cmd}" 2>&1)"; then
    while IFS= read -r line; do
      [[ -n "${line}" ]] || continue
      log "primary host time sync: ${line}"
    done <<<"${output}"
    return 0
  fi
  log_stderr "failed to harden primary host time synchronization via SSH: ${output}"
  return 1
}

ensure_primary_host_time_sync_via_node_janitor() {
  local primary_node_name=""
  local output=""
  local cmd=""

  if [[ -z "${KUBECTL:-}" ]]; then
    return 1
  fi
  primary_node_name="$(detect_primary_node_name)"
  if [[ -z "$(trim_field "${primary_node_name}")" ]]; then
    return 1
  fi
  cmd="$(host_time_sync_root_script)"
  if output="$(run_host_script_via_node_janitor "${primary_node_name}" "${cmd}" 2>&1)"; then
    while IFS= read -r line; do
      [[ -n "${line}" ]] || continue
      log "primary host time sync: ${line}"
    done <<<"${output}"
    return 0
  fi
  log_stderr "failed to harden primary host time synchronization via node-janitor: ${output}"
  return 1
}

ensure_host_time_sync() {
  if [[ "${FUGUE_HOST_TIME_SYNC_ENABLED:-true}" != "true" ]]; then
    log "host time synchronization hardening disabled"
    return 0
  fi
  if ensure_primary_host_time_sync_via_ssh; then
    return 0
  fi
  if ensure_primary_host_time_sync_via_node_janitor; then
    return 0
  fi
  if ! command_exists systemctl; then
    log "systemctl unavailable; skipping host time synchronization hardening"
    return 0
  fi
  if ! host_systemd_unit_file_exists systemd-timesyncd.service; then
    log "systemd-timesyncd unavailable; skipping host time synchronization hardening"
    return 0
  fi

  local active_unit=""
  active_unit="$(host_active_time_sync_service || true)"
  case "${active_unit}" in
    chrony.service|chronyd.service)
      log "${active_unit} is active; leaving systemd-timesyncd poll interval unchanged"
      return 0
      ;;
  esac

  local min_poll="${FUGUE_HOST_TIMESYNCD_MIN_POLL_SEC:-32}"
  local max_poll="${FUGUE_HOST_TIMESYNCD_MAX_POLL_SEC:-64}"
  local dropin="${FUGUE_HOST_TIMESYNCD_DROPIN:-/etc/systemd/timesyncd.conf.d/10-fugue-managed.conf}"
  local tmp=""

  if ! [[ "${min_poll}" =~ ^[0-9]+$ ]]; then
    min_poll=32
  fi
  if ! [[ "${max_poll}" =~ ^[0-9]+$ ]]; then
    max_poll=64
  fi
  if (( max_poll < min_poll )); then
    max_poll="${min_poll}"
  fi

  tmp="$(mktemp)"
  {
    printf '[Time]\n'
    printf 'PollIntervalMinSec=%ss\n' "${min_poll}"
    printf 'PollIntervalMaxSec=%ss\n' "${max_poll}"
  } >"${tmp}"
  if write_privileged_file_if_changed "${tmp}" "${dropin}"; then
    if run_privileged_host_command systemctl restart systemd-timesyncd.service; then
      log "configured systemd-timesyncd poll interval min=${min_poll}s max=${max_poll}s"
    elif command_exists timedatectl && run_privileged_host_command timedatectl set-ntp true; then
      log "enabled host NTP after configuring systemd-timesyncd poll interval"
    else
      log_stderr "configured ${dropin}, but could not restart systemd-timesyncd"
    fi
  else
    local rc=$?
    if [[ "${rc}" == "1" ]]; then
      log "systemd-timesyncd poll interval already configured"
    else
      log_stderr "failed to configure ${dropin}; continuing without blocking control-plane upgrade"
    fi
  fi
}

CONTROL_PLANE_AUTOMATION_TMP_DIR=""
KUBECONFIG_FALLBACK_FILE=""
UPGRADE_OVERRIDE_VALUES_FILE=""
HELM_POST_RENDERER_FILE=""
DNS_STATIC_RECORDS_FILE=""
PLATFORM_ROUTES_FILE=""
DISCOVERY_BUNDLE_FILE=""
DISCOVERY_BUNDLE_FILE_TEMP=""
LOCAL_CONTROL_PLANE_AUTOMATION_DIR="${FUGUE_LOCAL_CONTROL_PLANE_AUTOMATION_DIR:-${HOME}/.config/fugue/control-plane-automation}"
LOCAL_ROOT_CONTROL_PLANE_AUTOMATION_DIR="${FUGUE_LOCAL_ROOT_CONTROL_PLANE_AUTOMATION_DIR:-/root/.config/fugue/control-plane-automation}"
CONTROL_PLANE_HOSTS_ENV_LOADED="false"
PRIMARY_CONTROL_PLANE_SSH_OPTS=()
PRIMARY_CONTROL_PLANE_SSH_HOST=""
PRIMARY_CONTROL_PLANE_SSH_USER=""
PRIMARY_CONTROL_PLANE_SSH_PORT=""
PRIMARY_CONTROL_PLANE_SSH_HOST_KEY_ALIAS=""
PRIMARY_DISK_PRESSURE_CLEAR_POLL_SECONDS="${FUGUE_PRIMARY_DISK_PRESSURE_CLEAR_POLL_SECONDS:-5}"
# Kubelet delays clearing DiskPressure for evictionPressureTransitionPeriod
# (5m by default on our k3s nodes), so keep a wider recovery window here.
PRIMARY_DISK_PRESSURE_CLEAR_TIMEOUT_SECONDS="${FUGUE_PRIMARY_DISK_PRESSURE_CLEAR_TIMEOUT_SECONDS:-600}"
PRIMARY_NODE_READY_POLL_SECONDS="${FUGUE_PRIMARY_NODE_READY_POLL_SECONDS:-5}"
PRIMARY_NODE_READY_TIMEOUT_SECONDS="${FUGUE_PRIMARY_NODE_READY_TIMEOUT_SECONDS:-300}"
LOCAL_KUBE_API_READY_POLL_SECONDS="${FUGUE_LOCAL_KUBE_API_READY_POLL_SECONDS:-2}"
LOCAL_KUBE_API_READY_TIMEOUT_SECONDS="${FUGUE_LOCAL_KUBE_API_READY_TIMEOUT_SECONDS:-180}"
PRIMARY_POSTGRES_DATA_ROOT="${FUGUE_PRIMARY_POSTGRES_DATA_ROOT:-/var/lib/fugue/postgres}"
PRIMARY_POSTGRES_IMAGE="${FUGUE_PRIMARY_POSTGRES_IMAGE:-docker.io/library/postgres:16-alpine}"
FUGUE_DEFAULT_REGISTRY_PULL_BASE="${FUGUE_DEFAULT_REGISTRY_PULL_BASE:-}"
DNS_HELM_SET_ARGS=()
NODE_LOCAL_DNS_HELM_SET_ARGS=()
HEADSCALE_HELM_SET_ARGS=()
PUBLIC_DATA_PLANE_HELM_SET_ARGS=()
PUBLIC_DATA_PLANE_PRESERVED=false
NODE_LOCAL_BUILD_PLANE_HELM_SET_ARGS=()
MAINTENANCE_AGENT_HELM_SET_ARGS=()
HELM_POST_RENDERER_ARGS=()
NODE_LOCAL_BUILD_PLANE_PREFLIGHT_OVERRIDE_USED="false"
PRESERVE_REGISTRY_ZERO_REPLICAS="false"
RELEASE_CHANGED_FILES_EFFECTIVE=""
STRICT_DRAIN_AGENT_IMAGE_PRESERVED=false
ROBUSTNESS_HEALTH_GATE_BASELINE_FILE=""
DNS_MANIFEST_SNAPSHOT_FILE=""
DNS_MANIFEST_TRANSACTION_REQUIRED="false"
DNS_MANIFEST_TRANSACTION_COMPLETED="false"
DNS_MANIFEST_ROLLBACK_RESTORED="false"
DNS_MANIFEST_SNAPSHOT_KEEP="false"
NODE_LOCAL_DNS_PREVIOUS_ENABLED="false"
NODE_LOCAL_DNS_PREVIOUS_MODE=""
NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES=""
NODE_LOCAL_DNS_PREVIOUS_PODS_JSON=""
NODE_LOCAL_DNS_ADDED_NODES=""
NODE_LOCAL_DNS_REPLACED_NODES=""
NODE_LOCAL_DNS_FAILED_NODE=""
NODE_LOCAL_DNS_HOSTPORT_POD_SNAPSHOT=""
NODE_LOCAL_DNS_TARGET_NODES=""
NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP=""
NODE_LOCAL_DNS_PREFLIGHT_TARGET_NODES=""
NODE_LOCAL_DNS_RELEASED="false"
NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME=""
NODE_LOCAL_DNS_ACTIVE_SERVICE_NAME=""
NODE_LOCAL_DNS_ACTIVE_COMPONENT="node-local-dns"
NODE_LOCAL_DNS_UPSTREAM_SERVICE_NAME=""
NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME=""
NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES=""
NODE_LOCAL_DNS_PRESERVED_MODE=""
NODE_LOCAL_DNS_PRESERVED_PODS_JSON="[]"
NODE_LOCAL_DNS_PRESERVED_DAEMONSET_JSON=""
NODE_LOCAL_DNS_SPLIT_COHORT="false"
IMAGE_CACHE_PRE_HELM_TARGET_REVISION=""
IMAGE_CACHE_PRE_HELM_PLAN_JSON=""
IMAGE_CACHE_ONDELETE_STRATEGY_MIGRATION="false"

release_changed_files() {
  if [[ -n "${RELEASE_CHANGED_FILES_EFFECTIVE:-}" ]]; then
    printf '%s\n' "${RELEASE_CHANGED_FILES_EFFECTIVE}" | sed '/^[[:space:]]*$/d'
    return
  fi
  printf '%s\n' "${FUGUE_RELEASE_CHANGED_FILES:-}" | sed '/^[[:space:]]*$/d'
}

refresh_release_changed_files_from_live_api() {
  local api_deployment="${FUGUE_API_DEPLOYMENT_NAME}"
  local live_image_ref=""
  local live_tag=""
  local target_tag=""
  local changed_files=""

  target_tag="$(trim_field "${FUGUE_API_IMAGE_TAG:-}")"
  [[ -n "${target_tag}" ]] || return 0
  if ! deployment_exists "${api_deployment}" && deployment_exists "${FUGUE_LEGACY_API_DEPLOYMENT_NAME}"; then
    api_deployment="${FUGUE_LEGACY_API_DEPLOYMENT_NAME}"
  fi
  live_image_ref="$(trim_field "$(live_deployment_container_image "${api_deployment}" "api")")"
  live_tag="$(image_ref_tag "${live_image_ref}")"
  if [[ -z "${live_tag}" || "${live_tag}" == "${target_tag}" ]]; then
    return 0
  fi
  if ! git -C "${REPO_ROOT}" cat-file -e "${live_tag}^{commit}" 2>/dev/null ||
    ! git -C "${REPO_ROOT}" cat-file -e "${target_tag}^{commit}" 2>/dev/null; then
    return 0
  fi
  changed_files="$(git -C "${REPO_ROOT}" diff --no-renames --name-only "${live_tag}" "${target_tag}")"
  RELEASE_CHANGED_FILES_EFFECTIVE="${changed_files}"
  log "release changed files rebased from live API tag ${live_tag} to target ${target_tag}"
}

release_changed_files_exact_set() {
  local actual expected

  actual="$(release_changed_files | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | sed '/^$/d' | sort)"
  expected="$(printf '%s\n' "$@" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | sed '/^$/d' | sort)"
  [[ "${actual}" == "${expected}" ]]
}

release_changed_files_match() {
  local domain="$1"
  local file=""

  while IFS= read -r file; do
    file="$(trim_field "${file}")"
    [[ -n "${file}" ]] || continue
    case "${domain}:${file}" in
      public:cmd/fugue-edge/*|\
      public:cmd/fugue-edge-front/*|\
      public:cmd/fugue-dns/*|\
      public:internal/bundleauth/*|\
      public:internal/edge/*|\
      public:internal/edgefront/*|\
      public:internal/httpx/*|\
      public:internal/proxyproto/*|\
      public:internal/dnsserver/*|\
      public:internal/weightedselector/*|\
      public:internal/model/edge_routes.go|\
      public:Dockerfile.edge|\
      public:deploy/helm/fugue/templates/edge-*|\
      public:deploy/helm/fugue/templates/dns-*|\
      public:deploy/helm/fugue/templates/_helpers.tpl|\
      public:deploy/helm/fugue/values.yaml|\
      public:deploy/helm/fugue/values-production-ha.yaml|\
      public:scripts/render_fugue_edge_systemd_unit.sh|\
      public:scripts/render_fugue_dns_systemd_unit.sh|\
      public:scripts/release_fugue_public_data_plane.sh|\
      public:.github/workflows/release-public-data-plane.yml)
        return 0
        ;;
      build:cmd/fugue-image-cache/*|\
      build:Dockerfile.image-cache|\
      build:deploy/helm/fugue/templates/image-cache-daemonset.yaml|\
      build:deploy/helm/fugue/values.yaml|\
      build:deploy/helm/fugue/values-production-ha.yaml)
        return 0
        ;;
      stateful:deploy/helm/fugue/templates/registry-configmap.yaml|\
      stateful:deploy/helm/fugue/templates/registry-deployment.yaml|\
      stateful:deploy/helm/fugue/templates/registry-pvc.yaml|\
      stateful:deploy/helm/fugue/templates/registry-service.yaml|\
      stateful:deploy/helm/fugue/templates/headscale-*|\
      stateful:deploy/helm/fugue/templates/control-plane-postgres-*|\
      stateful:deploy/helm/fugue/templates/postgres-*|\
      stateful:deploy/helm/fugue/templates/shared-workspace-*|\
      stateful:deploy/helm/fugue/templates/snapshot-controller.yaml)
        return 0
        ;;
    esac
  done < <(release_changed_files)

  return 1
}

public_data_plane_changed() {
  release_changed_files_match public
}

public_data_plane_manifest_changed() {
  local file=""

  while IFS= read -r file; do
    file="$(trim_field "${file}")"
    [[ -n "${file}" ]] || continue
    case "${file}" in
      deploy/helm/fugue/templates/edge-*|\
      deploy/helm/fugue/templates/dns-*|\
      deploy/helm/fugue/templates/_helpers.tpl)
        return 0
        ;;
    esac
  done < <(release_changed_files)

  return 1
}

public_data_plane_worker_image_changed() {
  local file=""

  while IFS= read -r file; do
    file="$(trim_field "${file}")"
    [[ -n "${file}" ]] || continue
    case "${file}" in
      cmd/fugue-edge/*|\
      internal/bundleauth/*|\
      internal/edge/*|\
      internal/httpx/*|\
      internal/model/edge_routes.go|\
      internal/proxyproto/*|\
      internal/weightedselector/*|\
      Dockerfile.edge)
        return 0
        ;;
    esac
  done < <(release_changed_files)

  return 1
}

public_data_plane_front_image_changed() {
  local file=""

  while IFS= read -r file; do
    file="$(trim_field "${file}")"
    [[ -n "${file}" ]] || continue
    case "${file}" in
      cmd/fugue-edge-front/*|\
      internal/edgefront/*|\
      internal/proxyproto/*|\
      Dockerfile.edge)
        return 0
        ;;
    esac
  done < <(release_changed_files)

  return 1
}

public_data_plane_dns_image_changed() {
  local file=""

  while IFS= read -r file; do
    file="$(trim_field "${file}")"
    [[ -n "${file}" ]] || continue
    case "${file}" in
      cmd/fugue-dns/*|\
      internal/bundleauth/*|\
      internal/dnsserver/*|\
      internal/httpx/*|\
      internal/model/edge_routes.go|\
      internal/weightedselector/*|\
      Dockerfile.edge)
        return 0
        ;;
    esac
  done < <(release_changed_files)

  return 1
}

public_data_plane_daemonset_rollout_wait_required() {
  [[ "${PUBLIC_DATA_PLANE_PRESERVED:-false}" != "true" ]]
}

release_safety_file_is_non_runtime() {
  local file="$1"

  case "${file}" in
    docs/*|\
    *.md|\
    *_test.go|\
    */testdata/*|\
    scripts/test_*.sh|\
    scripts/test_*.py)
      return 0
      ;;
  esac
  return 1
}

release_safety_emit_subsystem() {
  local subsystem="$1"

  if [[ "${seen}" != *" ${subsystem} "* ]]; then
    printf '%s\n' "${subsystem}"
    seen="${seen}${subsystem} "
  fi
}

prepare_release_safety_runtime_intents() {
  local daemonset_ref=""

  FUGUE_RELEASE_NODE_LOCAL_DNS_INTENT=false
  export FUGUE_RELEASE_NODE_LOCAL_DNS_INTENT
  if [[ "${FUGUE_NODE_LOCAL_DNS_ENABLED:-false}" == "true" ]]; then
    FUGUE_RELEASE_NODE_LOCAL_DNS_INTENT=true
    export FUGUE_RELEASE_NODE_LOCAL_DNS_INTENT
    return 0
  fi

  if ! daemonset_ref="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get daemonset "${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME:-${FUGUE_RELEASE_FULLNAME}-node-local-dns}" --ignore-not-found -o name)"; then
    log_stderr "failed to inspect live NodeLocal DNSCache state for release safety attribution"
    return 1
  fi
  if [[ -n "$(trim_field "${daemonset_ref}")" ]]; then
    FUGUE_RELEASE_NODE_LOCAL_DNS_INTENT=true
    export FUGUE_RELEASE_NODE_LOCAL_DNS_INTENT
  fi
}

release_safety_changed_file_subsystems() {
  local file=""
  local seen=" "
  local matched="false"

  while IFS= read -r file; do
    file="$(trim_field "${file}")"
    [[ -n "${file}" ]] || continue
    release_safety_file_is_non_runtime "${file}" && continue
    matched="false"

    case "${file}" in
      internal/api/node_updater.go|\
      internal/store/node_deep_health.go|\
      internal/store/node_deep_health_pg.go|\
      internal/model/node_deep_health.go|\
      scripts/render_fugue_node_updater_systemd_unit.sh)
        release_safety_emit_subsystem node_updater
        matched="true"
        ;;
    esac
    case "${file}" in
      internal/api/edge_routes.go|\
      internal/model/edge_routes.go)
        release_safety_emit_subsystem edge_route
        matched="true"
        ;;
    esac
    case "${file}" in
      deploy/helm/fugue/templates/node-local-dns-cache.yaml|\
      deploy/helm/fugue/templates/observability-prometheus-configmap.yaml)
        release_safety_emit_subsystem cluster_dns
        matched="true"
        ;;
    esac
    case "${file}" in
      internal/dnsserver/*|\
      internal/httpx/*|\
      internal/weightedselector/*|\
      cmd/fugue-dns/*|\
      deploy/helm/fugue/templates/dns-*|\
      scripts/render_fugue_dns_systemd_unit.sh|\
      .github/workflows/release-public-data-plane.yml)
        release_safety_emit_subsystem dns_server
        matched="true"
        ;;
    esac
    case "${file}" in
      internal/edge/*|\
      internal/edgefront/*|\
      internal/httpx/*|\
      internal/proxyproto/*|\
      internal/weightedselector/*|\
      cmd/fugue-edge/*|\
      cmd/fugue-edge-front/*|\
      Dockerfile.edge|\
      deploy/helm/fugue/templates/edge-*|\
      scripts/render_fugue_edge_systemd_unit.sh|\
      scripts/sync_fugue_edge_proxy.sh|\
      .github/workflows/release-public-data-plane.yml)
        release_safety_emit_subsystem edge_worker
        matched="true"
        ;;
    esac
    case "${file}" in
      cmd/fugue-api/*|\
      internal/api/*|\
      internal/httpx/*|\
      Dockerfile.api)
        release_safety_emit_subsystem control_plane_api
        matched="true"
        ;;
    esac
    case "${file}" in
      cmd/fugue-controller/*|\
      internal/controller/*|\
      Dockerfile.controller)
        release_safety_emit_subsystem control_plane_controller
        matched="true"
        ;;
    esac
    case "${file}" in
      internal/model/*|\
      internal/store/*|\
      internal/config/*|\
      internal/auth/*|\
      internal/apispec/*|\
      internal/backupschedule/*|\
      internal/observability/*|\
      internal/bundleauth/*|\
      internal/releaseflow/*|\
      internal/platformcontrol/*|\
      internal/platformsafety/*|\
      internal/rollbackpreflight/*|\
      cmd/fugue-openapi-gen/*|\
      openapi/*|\
      go.mod|\
      go.sum)
        release_safety_emit_subsystem shared_control_plane
        matched="true"
        ;;
    esac
    case "${file}" in
      cmd/fugue-image-cache/*|\
      Dockerfile.image-cache|\
      deploy/helm/fugue/templates/image-cache-*|\
      scripts/prepare_fugue_lvm_localpv_node.sh)
        release_safety_emit_subsystem node_local_build_plane
        matched="true"
        ;;
    esac
    case "${file}" in
      cmd/fugue-drain-agent/*|\
      cmd/fugue-telemetry-agent/*|\
      cmd/fugue-registry-maintenance/*|\
      Dockerfile.drain-agent|\
      Dockerfile.telemetry-agent|\
      deploy/helm/fugue/templates/*janitor*|\
      deploy/helm/fugue/templates/*maintenance*)
        release_safety_emit_subsystem maintenance_agent
        matched="true"
        ;;
    esac
    case "${file}" in
      cmd/fugue-mesh-agent/*|\
      cmd/fugue-mesh-recovery/*|\
      internal/mesh*|\
      scripts/install_fugue_ha.sh|\
      scripts/render_fugue_mesh_agent_systemd_unit.sh|\
      scripts/render_fugue_mesh_recovery_systemd_unit.sh)
        release_safety_emit_subsystem cluster_bootstrap
        matched="true"
        ;;
    esac
    case "${file}" in
      deploy/helm/fugue/templates/registry-*|\
      deploy/helm/fugue/templates/headscale-*|\
      deploy/helm/fugue/templates/*postgres*|\
      deploy/helm/fugue/templates/shared-workspace-*|\
      deploy/helm/fugue/templates/snapshot-controller.yaml)
        release_safety_emit_subsystem stateful_dependency
        matched="true"
        ;;
    esac
    case "${file}" in
      deploy/helm/fugue/charts/*|\
      deploy/helm/fugue/crds/*|\
      deploy/helm/fugue/Chart.yaml|\
      deploy/helm/fugue/values.yaml|\
      deploy/helm/fugue/values-production-ha.yaml|\
      deploy/helm/fugue/templates/_helpers.tpl|\
      deploy/helm/fugue/templates/*)
        release_safety_emit_subsystem helm_shared
        matched="true"
        ;;
    esac
    case "${file}" in
      internal/cli/*|\
      cmd/fugue-ssh-front/*|\
      images/app-ssh/*|\
      Dockerfile.app-ssh)
        release_safety_emit_subsystem client_or_app_runtime
        matched="true"
        ;;
    esac
    case "${file}" in
      scripts/upgrade_fugue_control_plane.sh|\
      scripts/release_fugue_public_data_plane.sh|\
      scripts/build_control_plane_images.sh|\
      scripts/compute_control_plane_image_build_plan.sh|\
      scripts/compute_release_changed_files_from_live.sh|\
      scripts/resolve_control_plane_live_images.sh|\
      scripts/verify_registry_image.py|\
      .github/workflows/deploy-control-plane.yml|\
      .github/workflows/release-public-data-plane.yml)
        release_safety_emit_subsystem deploy_script
        matched="true"
        ;;
    esac
    case "${file}" in
      scripts/export_cloudflare_zone_static_records.sh|\
      scripts/issue_fugue_app_wildcard_tls.sh)
        release_safety_emit_subsystem dns_server
        matched="true"
        ;;
    esac

    if [[ "${matched}" != "true" ]]; then
      release_safety_emit_subsystem unknown_high_risk
    fi
  done < <(release_changed_files)

  if [[ "${FUGUE_RELEASE_NODE_LOCAL_DNS_INTENT:-false}" == "true" ]]; then
    release_safety_emit_subsystem cluster_dns
  fi
}

release_safety_unknown_high_risk_files() {
  local file=""

  while IFS= read -r file; do
    file="$(trim_field "${file}")"
    [[ -n "${file}" ]] || continue
    release_safety_file_is_non_runtime "${file}" && continue
    if FUGUE_RELEASE_CHANGED_FILES="${file}" RELEASE_CHANGED_FILES_EFFECTIVE="" \
      release_safety_changed_file_subsystems | grep -Fqx unknown_high_risk; then
      printf '%s\n' "${file}"
    fi
  done < <(release_changed_files)
}

require_release_safety_attribution() {
  local unknown_files=""

  unknown_files="$(release_safety_unknown_high_risk_files)"
  [[ -n "$(trim_field "${unknown_files}")" ]] || return 0
  case "${FUGUE_UNKNOWN_RELEASE_RISK_APPROVED:-false}" in
    1|true|TRUE|yes|YES)
      log "unknown high-risk release files explicitly approved: $(paste -sd, - <<<"${unknown_files}")"
      return 0
      ;;
  esac
  log_stderr "unattributed runtime release files require FUGUE_UNKNOWN_RELEASE_RISK_APPROVED=true:"
  while IFS= read -r file; do
    [[ -n "${file}" ]] || continue
    log_stderr "  ${file}"
  done <<<"${unknown_files}"
  return 1
}

release_safety_watch_window_seconds() {
  local subsystem=""
  local max_seconds=0
  local seconds=0

  while IFS= read -r subsystem; do
    subsystem="$(trim_field "${subsystem}")"
    [[ -n "${subsystem}" ]] || continue
    case "${subsystem}" in
      node_updater)
        seconds="${FUGUE_NODE_UPDATER_TIMER_CYCLE_SECONDS:-900}"
        ;;
      edge_route)
        seconds="${FUGUE_EDGE_ROUTE_WATCH_WINDOW_SECONDS:-180}"
        ;;
      dns_server)
        seconds="${FUGUE_DNS_WATCH_WINDOW_SECONDS:-180}"
        ;;
      cluster_dns)
        seconds="${FUGUE_CLUSTER_DNS_WATCH_WINDOW_SECONDS:-180}"
        ;;
      edge_worker)
        seconds="${FUGUE_PUBLIC_DATA_PLANE_WATCH_WINDOW_SECONDS:-180}"
        ;;
      deploy_script)
        seconds="${FUGUE_DEPLOY_SCRIPT_WATCH_WINDOW_SECONDS:-60}"
        ;;
      control_plane_api|control_plane_controller)
        seconds="${FUGUE_CONTROL_PLANE_COMPONENT_WATCH_WINDOW_SECONDS:-120}"
        ;;
      shared_control_plane|helm_shared)
        seconds="${FUGUE_SHARED_CONTROL_PLANE_WATCH_WINDOW_SECONDS:-180}"
        ;;
      node_local_build_plane|maintenance_agent|cluster_bootstrap|client_or_app_runtime)
        seconds="${FUGUE_COMPONENT_WATCH_WINDOW_SECONDS:-120}"
        ;;
      stateful_dependency)
        seconds="${FUGUE_STATEFUL_DEPENDENCY_WATCH_WINDOW_SECONDS:-300}"
        ;;
      unknown_high_risk)
        seconds="${FUGUE_UNKNOWN_HIGH_RISK_WATCH_WINDOW_SECONDS:-300}"
        ;;
      *)
        seconds=0
        ;;
    esac
    if (( seconds > max_seconds )); then
      max_seconds="${seconds}"
    fi
  done < <(release_safety_changed_file_subsystems)

  printf '%s\n' "${max_seconds}"
}

release_safety_required_gates() {
  local subsystem=""
  local gates=()

  while IFS= read -r subsystem; do
    subsystem="$(trim_field "${subsystem}")"
    [[ -n "${subsystem}" ]] || continue
    case "${subsystem}" in
      node_updater)
        gates+=("node_deep_health" "node_heartbeat" "release_guard" "public_synthetic")
        ;;
      edge_route)
        gates+=("route_check" "dns_answer_audit" "release_guard" "public_synthetic")
        ;;
      dns_server)
        gates+=("authoritative_dns" "dns_answer_audit" "release_guard")
        ;;
      cluster_dns)
        gates+=("central_coredns" "node_local_dns" "service_resolution" "release_guard" "public_synthetic" "rollback_path_smoke")
        ;;
      edge_worker)
        gates+=("inactive_worker_smoke" "active_slot_smoke" "edge_request_error_class")
        ;;
      deploy_script)
        gates+=("release_guard" "rollback_path_smoke")
        ;;
      control_plane_api|control_plane_controller)
        gates+=("platform_autonomy" "release_guard" "public_synthetic" "rollback_path_smoke")
        ;;
      shared_control_plane|helm_shared)
        gates+=("platform_autonomy" "release_guard" "public_synthetic" "rollback_path_smoke")
        ;;
      node_local_build_plane)
        gates+=("node_heartbeat" "release_guard" "registry_readiness")
        ;;
      maintenance_agent|cluster_bootstrap|client_or_app_runtime)
        gates+=("platform_autonomy" "release_guard")
        ;;
      stateful_dependency)
        gates+=("restore_readiness" "platform_autonomy" "release_guard" "rollback_path_smoke")
        ;;
      unknown_high_risk)
        gates+=("manual_risk_attribution" "platform_autonomy" "release_guard" "public_synthetic" "rollback_path_smoke")
        ;;
    esac
  done < <(release_safety_changed_file_subsystems)

  printf '%s\n' "${gates[@]}" | sed '/^[[:space:]]*$/d' | sort -u | paste -sd, -
}

release_safety_watch_window_summary() {
  local subsystems gates seconds

  subsystems="$(release_safety_changed_file_subsystems | paste -sd, -)"
  gates="$(release_safety_required_gates)"
  seconds="$(release_safety_watch_window_seconds)"
  printf 'subsystems=%s watch_seconds=%s gates=%s' "${subsystems:-none}" "${seconds:-0}" "${gates:-none}"
}

write_release_safety_attribution() {
  local dir="${FUGUE_RELEASE_ATTRIBUTION_DIR:-}"
  local file

  [[ -n "$(trim_field "${dir}")" ]] || return 0
  mkdir -p "${dir}"
  file="${dir}/release-safety-watch-windows.json"
  python3 - "${file}" <<'PY'
import json
import os
import subprocess
import sys

path = sys.argv[1]

def run(fn):
    out = subprocess.check_output(["bash", "-lc", f"source scripts/upgrade_fugue_control_plane.sh >/dev/null 2>&1; {fn}"], env={**os.environ, "FUGUE_UPGRADE_LIB_ONLY": "true"}, text=True)
    return [line for line in out.splitlines() if line.strip()]

payload = {
    "release_id": os.environ.get("FUGUE_RELEASE_ID") or os.environ.get("GITHUB_SHA") or "",
    "changed_files": [line for line in os.environ.get("FUGUE_RELEASE_CHANGED_FILES", "").splitlines() if line.strip()],
    "subsystems": run("release_safety_changed_file_subsystems"),
    "unknown_high_risk_files": run("release_safety_unknown_high_risk_files"),
    "required_gates": [item for line in run("release_safety_required_gates") for item in line.split(",") if item],
    "watch_seconds": int(run("release_safety_watch_window_seconds")[0] or "0"),
}
with open(path, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, indent=2, sort_keys=True)
PY
}

public_synthetic_error_class() {
  local status="$1"
  local body="$2"
  local normalized

  normalized="$(printf '%s' "${body}" | tr '[:upper:]' '[:lower:]')"
  if [[ "${status}" == "503" && "${normalized}" == *"no healthy edge groups"* ]]; then
    printf 'public_synthetic_503_no_healthy_edge_groups\n'
    return 0
  fi
  if [[ "${status}" == "503" && "${normalized}" == *"edge group has no healthy non-excluded edge nodes"* ]]; then
    printf 'public_synthetic_503_no_healthy_non_excluded_edge_nodes\n'
    return 0
  fi
  if [[ "${normalized}" == *"no active route"* ]]; then
    printf 'public_synthetic_no_active_route\n'
    return 0
  fi
  if [[ "${normalized}" == *"dns answer contains non route-ready edge"* ]]; then
    printf 'public_synthetic_dns_non_route_ready_edge\n'
    return 0
  fi
  if [[ "${status}" == "503" && "${normalized}" == *"upstream unavailable"* ]]; then
    printf 'public_synthetic_503_upstream_unavailable\n'
    return 0
  fi
  printf 'none\n'
}

public_synthetic_status_is_hard_rollback() {
  local class="$1"
  case "${class}" in
    public_synthetic_503_no_healthy_edge_groups|\
    public_synthetic_503_no_healthy_non_excluded_edge_nodes|\
    public_synthetic_no_active_route|\
    public_synthetic_dns_non_route_ready_edge)
      return 0
      ;;
  esac
  return 1
}

wait_for_release_safety_watch_windows() {
  local seconds delay deadline output release_output summary

  case "${FUGUE_RELEASE_SAFETY_WATCH_WINDOWS_ENABLED:-true}" in
    1|true|TRUE|yes|YES)
      ;;
    *)
      log "release safety watch windows disabled"
      return 0
      ;;
  esac

  seconds="$(release_safety_watch_window_seconds)"
  [[ "${seconds}" =~ ^[0-9]+$ ]] || seconds=0
  if (( seconds <= 0 )); then
    return 0
  fi
  delay="${FUGUE_RELEASE_SAFETY_WATCH_DELAY_SECONDS:-30}"
  summary="$(release_safety_watch_window_summary)"
  log "release safety watch window selected: ${summary}"
  write_release_safety_attribution || true
  deadline=$((SECONDS + seconds))
  while true; do
    if output="$(robustness_status_summary "${ROBUSTNESS_HEALTH_GATE_BASELINE_FILE:-}")" &&
      release_output="$(release_guard_status_summary 2>/dev/null)"; then
      log "release safety watch sample passed: ${output}; ${release_output}"
    else
      log "release safety watch failed: ${output:-unknown} ${release_output:-unknown}"
      return 1
    fi
    if (( SECONDS >= deadline )); then
      log "release safety watch window completed: ${summary}"
      return 0
    fi
    sleep "${delay}"
  done
}

node_local_build_plane_changed() {
  release_changed_files_match build
}

strict_drain_agent_image_changed() {
  local file=""

  while IFS= read -r file; do
    file="$(trim_field "${file}")"
    [[ -n "${file}" ]] || continue
    case "${file}" in
      cmd/fugue-drain-agent/*|\
      Dockerfile.drain-agent)
        return 0
        ;;
    esac
  done < <(release_changed_files)

  return 1
}

node_local_build_plane_manifest_changed() {
  local file=""

  while IFS= read -r file; do
    file="$(trim_field "${file}")"
    [[ -n "${file}" ]] || continue
    case "${file}" in
      deploy/helm/fugue/templates/image-cache-daemonset.yaml)
        return 0
        ;;
    esac
  done < <(release_changed_files)

  return 1
}

node_janitor_pod_on_node() {
  local node_name="$1"
  local namespace="${FUGUE_NAMESPACE:-fugue-system}"

  ${KUBECTL} -n "${namespace}" get pods \
    -l app.kubernetes.io/component=node-janitor \
    -o custom-columns=NAME:.metadata.name,NODE:.spec.nodeName,PHASE:.status.phase \
    --no-headers 2>/dev/null |
    awk -v node="${node_name}" '$2 == node && $3 == "Running" {print $1; exit}'
}

run_host_script_via_node_janitor() {
  local node_name="$1"
  local script="$2"
  local namespace="${FUGUE_NAMESPACE:-fugue-system}"
  local pod_name=""

  if [[ -z "${KUBECTL:-}" ]]; then
    return 1
  fi
  pod_name="$(node_janitor_pod_on_node "${node_name}")"
  if [[ -z "$(trim_field "${pod_name}")" ]]; then
    return 1
  fi

  printf '%s\n' "${script}" | ${KUBECTL} -n "${namespace}" exec -i "${pod_name}" -c node-janitor -- /bin/bash -lc '
set -euo pipefail
target="/host/tmp/fugue-host-root-script-$(date +%s)-$$.sh"
cleanup() {
  rm -f "${target}"
}
trap cleanup EXIT
cat >"${target}"
chmod 0700 "${target}"
chroot /host /bin/bash "${target#/host}"
'
}

release_diff_old_ref() {
  local ref="${FUGUE_RELEASE_BEFORE_SHA:-${BEFORE_SHA:-}}"

  ref="$(trim_field "${ref}")"
  if [[ -n "${ref}" ]]; then
    [[ "${ref}" != "0000000000000000000000000000000000000000" ]] || return 1
    git -C "${REPO_ROOT}" cat-file -e "${ref}^{commit}" 2>/dev/null || return 1
    printf '%s\n' "${ref}"
    return 0
  fi

  git -C "${REPO_ROOT}" rev-parse --verify HEAD^ 2>/dev/null
}

release_diff_new_ref() {
  local ref="${FUGUE_RELEASE_AFTER_SHA:-${AFTER_SHA:-${GITHUB_SHA:-}}}"

  ref="$(trim_field "${ref}")"
  if [[ -n "${ref}" ]]; then
    git -C "${REPO_ROOT}" cat-file -e "${ref}^{commit}" 2>/dev/null || return 1
    printf '%s\n' "${ref}"
    return 0
  fi

  git -C "${REPO_ROOT}" rev-parse --verify HEAD 2>/dev/null
}

release_diff_base_refs() {
  local raw="${FUGUE_RELEASE_BASE_REFS:-}"
  local ref=""
  local normalized=""

  if [[ -z "$(trim_field "${raw}")" ]]; then
    release_diff_old_ref
    return
  fi

  while IFS= read -r ref; do
    ref="$(trim_field "${ref}")"
    [[ -n "${ref}" ]] || continue
    [[ "${ref}" != "0000000000000000000000000000000000000000" ]] || return 1
    git -C "${REPO_ROOT}" cat-file -e "${ref}^{commit}" 2>/dev/null || return 1
    ref="$(git -C "${REPO_ROOT}" rev-parse "${ref}^{commit}")" || return 1
    normalized+="${normalized:+$'\n'}${ref}"
  done < <(printf '%s\n' "${raw}" | tr ',' '\n')
  [[ -n "$(trim_field "${normalized}")" ]] || return 1
  printf '%s\n' "${normalized}" | sort -u
}

image_cache_release_baseline_refs() {
  local image_cache_ref="${FUGUE_IMAGE_CACHE_IMAGE_BASELINE_REF:-}"
  local expected_ref="${FUGUE_EXPECTED_IMAGE_CACHE_IMAGE_BASELINE_REF:-}"
  local canonical_ref=""
  local canonical_expected_ref=""

  image_cache_ref="$(trim_field "${image_cache_ref}")"
  expected_ref="$(trim_field "${expected_ref}")"
  if [[ -n "${image_cache_ref}" ]]; then
    canonical_ref="$(FUGUE_RELEASE_BASE_REFS="${image_cache_ref}" release_diff_base_refs)" || return 1
    if [[ -n "$(trim_field "${FUGUE_RELEASE_BASE_REFS:-}")" ]]; then
      [[ -n "${expected_ref}" ]] || return 1
      canonical_expected_ref="$(FUGUE_RELEASE_BASE_REFS="${expected_ref}" release_diff_base_refs)" || return 1
      [[ "${canonical_ref}" == "${canonical_expected_ref}" ]] || return 1
    fi
    printf '%s\n' "${canonical_ref}"
    return
  fi
  # Once a workflow supplies a trusted multi-component baseline set, the
  # image-cache migration must not guess which component ref owns its source.
  [[ -z "$(trim_field "${FUGUE_RELEASE_BASE_REFS:-}")" ]] || return 1
  release_diff_base_refs
}

values_file_changes_limited_to_yaml_path() {
  local file="$1"
  shift
  local allowed_paths=("$@")
  local old_ref new_ref

  [[ "${#allowed_paths[@]}" -gt 0 ]] || return 1

  old_ref="$(release_diff_old_ref)" || return 1
  new_ref="$(release_diff_new_ref)" || return 1
  old_ref="$(trim_field "${old_ref}")"
  new_ref="$(trim_field "${new_ref}")"
  [[ -n "${old_ref}" && -n "${new_ref}" ]] || return 1
  [[ "${old_ref}" != "${new_ref}" ]] || return 1
  command_exists python3 || return 1

  python3 - "${REPO_ROOT}" "${old_ref}" "${new_ref}" "${file}" "${allowed_paths[@]}" <<'PY'
import difflib
import subprocess
import sys

repo, old_ref, new_ref, file_path = sys.argv[1:5]
allowed_paths = [
    tuple(part for part in raw.split(".") if part)
    for raw in sys.argv[5:]
    if raw.strip()
]
if not allowed_paths:
    raise SystemExit(1)

def read_lines(ref):
    try:
        out = subprocess.check_output(
            ["git", "-C", repo, "show", f"{ref}:{file_path}"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        raise SystemExit(1) from exc
    return out.splitlines()

def yaml_paths(lines):
    stack = []
    paths = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            paths.append(tuple(key for _, key in stack))
            continue

        indent = len(line) - len(line.lstrip(" "))
        while stack and stack[-1][0] >= indent:
            stack.pop()

        content = line.lstrip(" ")
        if content.startswith("- "):
            content = content[2:].lstrip(" ")

        key = ""
        if ":" in content:
            key = content.split(":", 1)[0].strip().strip("'\"")
            if any(ch in key for ch in "{}[]"):
                key = ""

        current = [key for _, key in stack]
        if key:
            current.append(key)
            stack.append((indent, key))
        paths.append(tuple(current))
    return paths

def line_allowed(path):
    return any(len(path) >= len(allowed) and path[: len(allowed)] == allowed for allowed in allowed_paths)

old_lines = read_lines(old_ref)
new_lines = read_lines(new_ref)
old_paths = yaml_paths(old_lines)
new_paths = yaml_paths(new_lines)
changed = False

for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(a=old_lines, b=new_lines).get_opcodes():
    if tag == "equal":
        continue
    for idx in range(i1, i2):
        if old_lines[idx].strip() and not line_allowed(old_paths[idx]):
            raise SystemExit(1)
        changed = True
    for idx in range(j1, j2):
        if new_lines[idx].strip() and not line_allowed(new_paths[idx]):
            raise SystemExit(1)
        changed = True

raise SystemExit(0 if changed else 1)
PY
}

values_file_changes_include_yaml_path() {
  local file="$1"
  local included_path="$2"
  local old_ref new_ref

  old_ref="$(release_diff_old_ref)" || return 1
  new_ref="$(release_diff_new_ref)" || return 1
  old_ref="$(trim_field "${old_ref}")"
  new_ref="$(trim_field "${new_ref}")"
  [[ -n "${old_ref}" && -n "${new_ref}" ]] || return 1
  [[ "${old_ref}" != "${new_ref}" ]] || return 1
  command_exists python3 || return 1

  python3 - "${REPO_ROOT}" "${old_ref}" "${new_ref}" "${file}" "${included_path}" <<'PY'
import difflib
import subprocess
import sys

repo, old_ref, new_ref, file_path, included_path = sys.argv[1:6]
included = tuple(part for part in included_path.split(".") if part)

def read_lines(ref):
    try:
        out = subprocess.check_output(
            ["git", "-C", repo, "show", f"{ref}:{file_path}"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        raise SystemExit(1) from exc
    return out.splitlines()

def yaml_paths(lines):
    stack = []
    paths = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            paths.append(tuple(key for _, key in stack))
            continue

        indent = len(line) - len(line.lstrip(" "))
        while stack and stack[-1][0] >= indent:
            stack.pop()

        content = line.lstrip(" ")
        if content.startswith("- "):
            content = content[2:].lstrip(" ")

        key = ""
        if ":" in content:
            key = content.split(":", 1)[0].strip().strip("'\"")
            if any(ch in key for ch in "{}[]"):
                key = ""

        current = [key for _, key in stack]
        if key:
            current.append(key)
            stack.append((indent, key))
        paths.append(tuple(current))
    return paths

def path_included(path):
    return len(path) >= len(included) and path[: len(included)] == included

old_lines = read_lines(old_ref)
new_lines = read_lines(new_ref)
old_paths = yaml_paths(old_lines)
new_paths = yaml_paths(new_lines)

for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(a=old_lines, b=new_lines).get_opcodes():
    if tag == "equal":
        continue
    for idx in range(i1, i2):
        if old_lines[idx].strip() and path_included(old_paths[idx]):
            raise SystemExit(0)
    for idx in range(j1, j2):
        if new_lines[idx].strip() and path_included(new_paths[idx]):
            raise SystemExit(0)

raise SystemExit(1)
PY
}

registry_persistence_values_changed() {
  local file=""

  while IFS= read -r file; do
    file="$(trim_field "${file}")"
    [[ -n "${file}" ]] || continue
    case "${file}" in
      deploy/helm/fugue/values.yaml|\
      deploy/helm/fugue/values-production-ha.yaml)
        values_file_changes_include_yaml_path "${file}" "registry.persistence" && return 0
        values_file_changes_include_yaml_path "${file}" "registry.unsafeHostPath" && return 0
        ;;
    esac
  done < <(release_changed_files)

  return 1
}

stateful_dependency_changed() {
  release_changed_files_match stateful || registry_persistence_values_changed
}

control_plane_backup_drain_required() {
  local required="${FUGUE_CONTROL_PLANE_BACKUP_DRAIN_REQUIRED:-auto}"
  local file=""
  local saw_file="false"

  case "${required}" in
    true)
      return 0
      ;;
    false)
      return 1
      ;;
    auto)
      ;;
    *)
      fail "FUGUE_CONTROL_PLANE_BACKUP_DRAIN_REQUIRED must be auto, true, or false"
      ;;
  esac

  while IFS= read -r file; do
    file="$(trim_field "${file}")"
    [[ -n "${file}" ]] || continue
    saw_file="true"
    case "${file}" in
      go.mod|go.sum|\
      Dockerfile.api|Dockerfile.controller|\
      cmd/fugue-api/*|cmd/fugue-controller/*|cmd/fugue-registry-maintenance/*|\
      internal/api/*|internal/apispec/*|internal/auth/*|internal/config/*|\
      internal/controller/*|internal/model/*|internal/store/*|internal/workloadidentity/*|\
      openapi/openapi.yaml|\
      deploy/helm/fugue/templates/deployment.yaml|\
      deploy/helm/fugue/templates/controller-deployment.yaml|\
      deploy/helm/fugue/templates/secret.yaml|\
      deploy/helm/fugue/templates/control-plane-postgres-*|\
      deploy/helm/fugue/templates/postgres-*|\
      deploy/helm/fugue/values.yaml|\
      deploy/helm/fugue/values-production-ha.yaml)
        return 0
        ;;
    esac
  done < <(release_changed_files)

  [[ "${saw_file}" == "true" ]] || return 0
  return 1
}

node_local_build_plane_resource_values_changed() {
  local file=""
  local saw_resource_values="false"

  while IFS= read -r file; do
    file="$(trim_field "${file}")"
    [[ -n "${file}" ]] || continue
    case "${file}" in
      deploy/helm/fugue/values.yaml|\
      deploy/helm/fugue/values-production-ha.yaml)
        values_file_changes_limited_to_yaml_path "${file}" "imageCache.resources" || return 1
        saw_resource_values="true"
        ;;
    esac
  done < <(release_changed_files)

  [[ "${saw_resource_values}" == "true" ]]
}

node_local_build_plane_shared_manifest_changed() {
  local file=""

  while IFS= read -r file; do
    file="$(trim_field "${file}")"
    [[ -n "${file}" ]] || continue
    case "${file}" in
      deploy/helm/fugue/templates/_helpers.tpl|\
      deploy/helm/fugue/templates/secret.yaml)
        return 0
        ;;
    esac
  done < <(release_changed_files)

  return 1
}

node_local_build_plane_any_values_changed() {
  release_changed_files | grep -Eq '^deploy/helm/fugue/values(-production-ha)?\.yaml$'
}

file_sha256_matches() {
  local file="$1"
  local expected="$2"

  [[ -r "${file}" && "${expected}" =~ ^[0-9a-f]{64}$ ]] || return 1
  python3 - "${file}" "${expected}" <<'PY'
import hashlib
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
expected = sys.argv[2]
actual = hashlib.sha256(path.read_bytes()).hexdigest()
raise SystemExit(0 if actual == expected else 1)
PY
}

image_cache_ondelete_strategy_template_migration_only() {
  local old_ref new_ref
  local template_path="deploy/helm/fugue/templates/image-cache-daemonset.yaml"
  local old_template=""
  local new_template=""

  old_ref="$(release_diff_old_ref)" || return 1
  new_ref="$(release_diff_new_ref)" || return 1
  old_template="$(git -C "${REPO_ROOT}" show "${old_ref}:${template_path}")" || return 1
  new_template="$(git -C "${REPO_ROOT}" show "${new_ref}:${template_path}")" || return 1
  OLD_TEMPLATE="${old_template}" NEW_TEMPLATE="${new_template}" python3 -c '
import os

old = os.environ["OLD_TEMPLATE"]
new = os.environ["NEW_TEMPLATE"]
old_preamble = """{{- if .Values.imageCache.enabled }}
apiVersion: apps/v1"""
new_preamble = """{{- if .Values.imageCache.enabled }}
{{- $updateStrategyType := required \"imageCache.updateStrategy.type is required\" .Values.imageCache.updateStrategy.type -}}
{{- if not (has $updateStrategyType (list \"OnDelete\" \"RollingUpdate\")) -}}
{{- fail \"imageCache.updateStrategy.type must be OnDelete or RollingUpdate\" -}}
{{- end -}}
apiVersion: apps/v1"""
old_strategy = """  updateStrategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 1
      maxSurge: 0"""
new_strategy = """  updateStrategy:
    type: {{ $updateStrategyType }}
    {{- if eq $updateStrategyType \"RollingUpdate\" }}
    rollingUpdate:
      maxUnavailable: 1
      maxSurge: 0
    {{- end }}"""
if old.count(old_preamble) != 1 or old.count(old_strategy) != 1:
    raise SystemExit(1)
expected = old.replace(old_preamble, new_preamble, 1).replace(old_strategy, new_strategy, 1)
raise SystemExit(0 if new == expected else 1)
'
}

image_cache_strategy_chart_changes_only_between_refs() {
  local old_ref="$1"
  local new_ref="$2"
  local file=""
  local saw_file="false"

  old_ref="$(trim_field "${old_ref}")"
  new_ref="$(trim_field "${new_ref}")"
  [[ -n "${old_ref}" && -n "${new_ref}" && "${old_ref}" != "${new_ref}" ]] || return 1
  git -C "${REPO_ROOT}" cat-file -e "${old_ref}^{commit}" 2>/dev/null || return 1
  git -C "${REPO_ROOT}" cat-file -e "${new_ref}^{commit}" 2>/dev/null || return 1

  while IFS= read -r file; do
    file="$(trim_field "${file}")"
    [[ -n "${file}" ]] || continue
    saw_file="true"
    case "${file}" in
      deploy/helm/fugue/templates/image-cache-daemonset.yaml|\
      deploy/helm/fugue/values.yaml|\
      deploy/helm/fugue/chart_test.go)
        ;;
      *)
        return 1
        ;;
    esac
  done < <(git -C "${REPO_ROOT}" diff --no-renames --name-only "${old_ref}" "${new_ref}" -- deploy/helm/fugue)
  [[ "${saw_file}" == "true" ]]
}

image_cache_strategy_target_fingerprints_match() {
  file_sha256_matches \
    "${REPO_ROOT}/deploy/helm/fugue/templates/image-cache-daemonset.yaml" \
    "75fdaa91fff878ca633d25671c3a2ae4c06753cb58e3ed9b9804176c4de145f7" || return 1
  file_sha256_matches \
    "${REPO_ROOT}/deploy/helm/fugue/values.yaml" \
    "052d042ecc31f96784cf1323bd67ccf2c1d47d6490cf7d567343536f4663dcd8" || return 1
  CHART_ROOT="${REPO_ROOT}/deploy/helm/fugue" EXPECTED_SHA256="9e0bb3c332b3544f7d90f174ace55521aec218d58c977c92b616434448cb14e7" python3 -c '
import hashlib
import os
from pathlib import Path

root = Path(os.environ["CHART_ROOT"])
digest = hashlib.sha256()
files = sorted(
    path for path in root.rglob("*")
    if path.is_file() and path.name != "chart_test.go" and not path.name.endswith("_test.go")
)
if not files:
    raise SystemExit(1)
for path in files:
    relative = path.relative_to(root).as_posix().encode()
    content = path.read_bytes()
    digest.update(len(relative).to_bytes(4, "big"))
    digest.update(relative)
    digest.update(len(content).to_bytes(8, "big"))
    digest.update(content)
raise SystemExit(0 if digest.hexdigest() == os.environ["EXPECTED_SHA256"] else 1)
'
}

image_cache_strategy_migration_already_applied() {
  local live_value=""
  local state=""
  local generation observed strategy max_unavailable max_surge

  [[ "${FUGUE_IMAGE_CACHE_ENABLED:-false}" == "true" ]] || return 1
  image_cache_strategy_target_fingerprints_match || return 1
  live_value="$(trim_field "$(live_helm_release_value imageCache.updateStrategy.type || true)")"
  [[ "${live_value}" == "OnDelete" || "${live_value}" == "RollingUpdate" ]] || return 1
  state="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get daemonset "${FUGUE_RELEASE_FULLNAME}-image-cache" \
    -o jsonpath='{.metadata.generation}{"\t"}{.status.observedGeneration}{"\t"}{.spec.updateStrategy.type}{"\t"}{.spec.updateStrategy.rollingUpdate.maxUnavailable}{"\t"}{.spec.updateStrategy.rollingUpdate.maxSurge}')" || return 1
  IFS=$'\t' read -r generation observed strategy max_unavailable max_surge <<<"${state}"
  [[ -n "${generation}" && "${generation}" == "${observed}" && "${strategy}" == "${live_value}" ]] || return 1
  if [[ "${strategy}" == "OnDelete" ]]; then
    [[ -z "${max_unavailable}" && -z "${max_surge}" ]]
  else
    [[ "${max_unavailable}" == "1" && "${max_surge}" == "0" ]]
  fi
}

image_cache_ondelete_strategy_migration_allowed() {
  local file=""
  local saw_template="false"
  local saw_values="false"
  local baseline_ref=""
  local new_ref=""
  local exact_baseline_found="false"

  node_local_dns_split_release_enabled || return 1
  [[ "${FUGUE_IMAGE_CACHE_ENABLED:-false}" == "true" ]] || return 1
  new_ref="$(release_diff_new_ref)" || return 1
  [[ -n "${new_ref}" ]] || return 1

  while IFS= read -r file; do
    file="$(trim_field "${file}")"
    [[ -n "${file}" ]] || continue
    case "${file}" in
      deploy/helm/fugue/templates/image-cache-daemonset.yaml)
        saw_template="true"
        ;;
      deploy/helm/fugue/values.yaml)
        saw_values="true"
        ;;
      deploy/helm/fugue/chart_test.go)
        ;;
      cmd/fugue-image-cache/*|\
      Dockerfile.image-cache|\
      deploy/helm/fugue/*)
        ;;
    esac
  done < <(release_changed_files)

  [[ "${saw_template}" == "true" && "${saw_values}" == "true" ]] || return 1
  while IFS= read -r baseline_ref; do
    baseline_ref="$(trim_field "${baseline_ref}")"
    [[ -n "${baseline_ref}" && "${baseline_ref}" != "${new_ref}" ]] || continue
    if FUGUE_RELEASE_BEFORE_SHA="${baseline_ref}" FUGUE_RELEASE_AFTER_SHA="${new_ref}" \
        image_cache_ondelete_strategy_template_migration_only && \
      FUGUE_RELEASE_BEFORE_SHA="${baseline_ref}" FUGUE_RELEASE_AFTER_SHA="${new_ref}" \
        values_file_changes_limited_to_yaml_path "deploy/helm/fugue/values.yaml" "imageCache.updateStrategy" && \
      image_cache_strategy_chart_changes_only_between_refs "${baseline_ref}" "${new_ref}" && \
      ! image_cache_source_changed_between_refs "${baseline_ref}" "${new_ref}"; then
      exact_baseline_found="true"
      break
    fi
  done < <(image_cache_release_baseline_refs)
  [[ "${exact_baseline_found}" == "true" ]] || return 1

  # This is intentionally a one-time, fail-closed migration. The whole-file
  # fingerprints prevent an unrelated edit in either shared chart file from
  # piggybacking on the narrow updateStrategy exception.
  image_cache_strategy_target_fingerprints_match
}

chart_image_cache_resources_json() {
  local values_file="${FUGUE_HELM_CHART_PATH:-deploy/helm/fugue}/values.yaml"

  [[ -r "${values_file}" ]] || return 1
  command_exists python3 || return 1
  python3 - "${values_file}" <<'PY'
import json
import sys

values_file = sys.argv[1]
target = ("imageCache", "resources")

with open(values_file, "r", encoding="utf-8") as fh:
    lines = fh.read().splitlines()

stack = []
start = None
start_indent = None
for idx, line in enumerate(lines):
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        continue
    indent = len(line) - len(line.lstrip(" "))
    while stack and stack[-1][0] >= indent:
        stack.pop()
    content = line.lstrip(" ")
    if content.startswith("- "):
        content = content[2:].lstrip(" ")
    key = ""
    if ":" in content:
        key = content.split(":", 1)[0].strip().strip("'\"")
    current = tuple([item[1] for item in stack] + ([key] if key else []))
    if current == target:
        start = idx + 1
        start_indent = indent
        break
    if key:
        stack.append((indent, key))

if start is None:
    raise SystemExit(1)

subtree = []
for line in lines[start:]:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        continue
    indent = len(line) - len(line.lstrip(" "))
    if indent <= start_indent:
        break
    subtree.append(line)

root = {}
stack = [(start_indent, root)]
for line in subtree:
    indent = len(line) - len(line.lstrip(" "))
    content = line.lstrip(" ")
    if content.startswith("- "):
        raise SystemExit(1)
    if ":" not in content:
        raise SystemExit(1)
    key, value = content.split(":", 1)
    key = key.strip().strip("'\"")
    value = value.strip()
    while stack and stack[-1][0] >= indent:
        stack.pop()
    if not stack:
        raise SystemExit(1)
    parent = stack[-1][1]
    if value:
        parent[key] = value.strip("'\"")
        continue
    child = {}
    parent[key] = child
    stack.append((indent, child))

if not root:
    raise SystemExit(1)
json.dump(root, sys.stdout, sort_keys=True, separators=(",", ":"))
PY
}

json_equal() {
  local left="$1"
  local right="$2"

  command_exists python3 || return 1
  python3 - "${left}" "${right}" <<'PY'
import json
import sys

try:
    left = json.loads(sys.argv[1])
    right = json.loads(sys.argv[2])
except json.JSONDecodeError as exc:
    raise SystemExit(1) from exc
raise SystemExit(0 if left == right else 1)
PY
}

image_cache_resource_values_drifted() {
  local image_cache_ds="${FUGUE_RELEASE_FULLNAME}-image-cache"
  local desired_resources live_resources

  desired_resources="$(trim_field "$(chart_image_cache_resources_json)")" || return 1
  live_resources="$(trim_field "$(live_daemonset_container_resources_json "${image_cache_ds}" "image-cache")")" || return 1
  [[ -n "${desired_resources}" && -n "${live_resources}" ]] || return 1
  ! json_equal "${desired_resources}" "${live_resources}"
}

append_node_local_build_plane_desired_resource_args() {
  local desired_resources

  desired_resources="$(trim_field "$(chart_image_cache_resources_json)")" || return 1
  [[ -n "${desired_resources}" ]] || return 1
  NODE_LOCAL_BUILD_PLANE_HELM_SET_ARGS+=(--set-json "imageCache.resources=${desired_resources}")
  log "node-local build-plane image-cache resources applying desired chart values"
}

image_cache_source_changed_between_refs() {
	local old_ref="$1"
	local new_ref="$2"

	source_changed_between_refs "${old_ref}" "${new_ref}" \
		cmd/fugue-image-cache \
		Dockerfile.image-cache \
		go.mod \
		go.sum
}

source_changed_between_refs() {
	local old_ref="$1"
	local new_ref="$2"
	local rc=0

	shift 2 || true
	old_ref="$(trim_field "${old_ref}")"
	new_ref="$(trim_field "${new_ref}")"
	[[ -n "${old_ref}" && -n "${new_ref}" ]] || return 1
	[[ "${old_ref}" != "${new_ref}" ]] || return 1
	git -C "${REPO_ROOT}" cat-file -e "${old_ref}^{commit}" 2>/dev/null || return 1
	git -C "${REPO_ROOT}" cat-file -e "${new_ref}^{commit}" 2>/dev/null || return 1
	[[ "$#" -gt 0 ]] || return 1
	git -C "${REPO_ROOT}" diff --quiet "${old_ref}" "${new_ref}" -- "$@" && return 1
	rc=$?
	[[ "${rc}" -eq 1 ]]
}

public_data_plane_worker_source_changed_between_refs() {
	source_changed_between_refs "$1" "$2" \
		cmd/fugue-edge \
		internal/bundleauth \
		internal/edge \
		internal/httpx \
		internal/model/edge_routes.go \
		internal/proxyproto \
		Dockerfile.edge \
		go.mod \
		go.sum
}

public_data_plane_front_source_changed_between_refs() {
	source_changed_between_refs "$1" "$2" \
		cmd/fugue-edge-front \
		internal/edgefront \
		internal/proxyproto \
		Dockerfile.edge \
		go.mod \
		go.sum
}

public_data_plane_dns_source_changed_between_refs() {
	source_changed_between_refs "$1" "$2" \
		cmd/fugue-dns \
		internal/bundleauth \
		internal/dnsserver \
		internal/httpx \
		internal/model/edge_routes.go \
		Dockerfile.edge \
		go.mod \
		go.sum
}

node_local_build_plane_image_rollout_allowed() {
  local image_cache_ds="${FUGUE_RELEASE_FULLNAME}-image-cache"
  local live_image_ref live_tag target_tag

  target_tag="$(trim_field "${FUGUE_IMAGE_CACHE_IMAGE_TAG:-}")"
  [[ -n "${target_tag}" ]] || return 1
  live_image_ref="$(trim_field "$(live_daemonset_container_image "${image_cache_ds}" "image-cache")")"
  [[ -n "${live_image_ref}" ]] || return 1
  live_tag="$(image_ref_tag "${live_image_ref}")"
  [[ -n "${live_tag}" ]] || return 1
  image_cache_source_changed_between_refs "${live_tag}" "${target_tag}"
}

skip_singleton_rollout_wait_for_node_local_override() {
  local singleton="$1"

  [[ "${singleton}" == "${FUGUE_REGISTRY_DEPLOYMENT_NAME}" ]] || return 1
  [[ "${NODE_LOCAL_BUILD_PLANE_PREFLIGHT_OVERRIDE_USED}" == "true" ]] || return 1
  ! stateful_dependency_changed
}

node_local_build_plane_preflight_override_allowed() {
  local file=""
  local saw_allowed="false"

  if release_changed_files_exact_set \
    internal/api/app_deploy_test.go \
    internal/api/server.go; then
    return 0
  fi
  if release_changed_files_exact_set \
    internal/api/app_deploy_test.go \
    internal/api/server.go \
    scripts/test_release_domain_safety.sh \
    scripts/upgrade_fugue_control_plane.sh; then
    return 0
  fi

  while IFS= read -r file; do
    file="$(trim_field "${file}")"
    [[ -n "${file}" ]] || continue
    case "${file}" in
      cmd/fugue-image-cache/*|\
      Dockerfile.image-cache|\
      internal/sourceimport/*|\
      internal/config/config.go|\
      internal/model/model.go|\
      internal/api/image_cache_localpv_admin.go|\
      internal/api/image_cache_localpv_admin_test.go|\
      internal/controller/controller.go|\
      internal/controller/deploy_image_guard.go|\
      internal/controller/deploy_image_guard_test.go|\
      internal/controller/image_cache_orphan_cleanup.go|\
      internal/controller/image_cache_orphan_cleanup_test.go|\
      internal/controller/image_replication_controller.go|\
      internal/controller/image_replication_controller_test.go|\
      internal/controller/import_operation.go|\
      internal/controller/import_operation_test.go|\
      internal/controller/managed_app_reconciler.go|\
      internal/controller/managed_app_reconciler_test.go|\
      internal/controller/managed_app_rollout.go|\
      internal/controller/managed_app_rollout_test.go|\
      internal/controller/metrics.go|\
      internal/store/image_cache_localpv.go|\
      internal/store/image_cache_localpv_pg.go|\
      internal/store/image_cache_localpv_test.go|\
      internal/store/machines.go|\
      internal/store/node_updater.go|\
      internal/store/node_updater_pg.go|\
      internal/store/postgres.go|\
      internal/store/store_test.go|\
      internal/api/cluster_nodes.go|\
      internal/api/cluster_nodes_test.go|\
      internal/api/cluster_node_policy.go|\
      internal/api/cluster_node_policy_seed_test.go|\
      internal/api/cluster_node_policy_status.go|\
      internal/api/cluster_node_views.go|\
      internal/api/cluster_node_views_test.go|\
      internal/api/join_cluster.go|\
      internal/api/import_github_compose_test.go|\
      internal/api/import_github_topology.go|\
      internal/api/import_network_mode.go|\
      internal/api/node_updater.go|\
	      internal/api/node_updater_test.go|\
	      internal/api/runtime_pool.go|\
	      internal/api/server_test.go|\
	      scripts/prepare_fugue_lvm_localpv_node.sh|\
	      deploy/helm/fugue/templates/_helpers.tpl|\
	      deploy/helm/fugue/templates/controller-deployment.yaml|\
	      .github/workflows/deploy-control-plane.yml|\
	      .github/workflows/release-public-data-plane.yml|\
	      scripts/build_control_plane_images.sh|\
	      scripts/compute_control_plane_image_build_plan.sh|\
	      scripts/compute_release_changed_files_from_live.sh|\
	      scripts/resolve_control_plane_live_images.sh|\
	      scripts/verify_registry_image.py|\
	      scripts/upgrade_fugue_control_plane.sh|\
	      scripts/test_release_domain_safety.sh|\
	      scripts/test_verify_registry_image.py)
	        saw_allowed="true"
	        ;;
	      docs/*|\
	      fugue-cli-acceptance.md|\
	      openapi/openapi.yaml|\
	      internal/api/routes_gen.go|\
      internal/apispec/spec_gen.go|\
      internal/cli/*)
        ;;
      deploy/helm/fugue/values.yaml|\
      deploy/helm/fugue/values-production-ha.yaml)
        values_file_changes_limited_to_yaml_path \
          "${file}" \
          "imageCache.updateStrategy" \
          "imageCache.resources" \
          "imageStore.imageCacheInventory" \
          "imageStore.orphanPrune" || return 1
        saw_allowed="true"
        ;;
      *)
        return 1
        ;;
    esac
  done < <(release_changed_files)

  [[ "${saw_allowed}" == "true" ]]
}

ensure_control_plane_observability_via_node_janitor() {
  if [[ -z "${KUBECTL:-}" ]]; then
    return 1
  fi
  if [[ ! -r "${BASH_SOURCE[0]}" ]]; then
    log "control-plane host observability node-janitor fallback unavailable because ${BASH_SOURCE[0]} is not readable"
    return 1
  fi

  local namespace="${FUGUE_NAMESPACE:-fugue-system}"
  local node_name="${FUGUE_CONTROL_PLANE_OBSERVABILITY_NODE_NAME:-}"
  local pod_name=""

  if [[ -z "$(trim_field "${node_name}")" ]]; then
    node_name="$(hostname 2>/dev/null || true)"
  fi
  if [[ -z "$(trim_field "${node_name}")" ]]; then
    return 1
  fi

  pod_name="$(node_janitor_pod_on_node "${node_name}")"
  if [[ -z "$(trim_field "${pod_name}")" ]]; then
    log "control-plane host observability node-janitor fallback unavailable because no running node-janitor pod was found on ${node_name}"
    return 1
  fi

  log "running control-plane host observability bootstrap through node-janitor pod ${namespace}/${pod_name} on ${node_name}"
  ${KUBECTL} -n "${namespace}" exec -i "${pod_name}" -c node-janitor -- /bin/bash -lc '
set -euo pipefail
target=/host/tmp/fugue-control-plane-observability-bootstrap.sh
cat >"${target}"
chmod 0700 "${target}"
chroot /host /bin/bash -lc "FUGUE_CONTROL_PLANE_OBSERVABILITY_ONLY=true FUGUE_CONTROL_PLANE_OBSERVABILITY_RESTART_K3S=false KUBECONFIG=/etc/rancher/k3s/k3s.yaml /tmp/fugue-control-plane-observability-bootstrap.sh"
rm -f "${target}"
' <"${BASH_SOURCE[0]}"
}

ensure_control_plane_observability() {
  if [[ "${FUGUE_CONTROL_PLANE_OBSERVABILITY_ENABLED:-true}" != "true" ]]; then
    log "control-plane observability bootstrap disabled"
    return 0
  fi
  if [[ "$(id -u)" != "0" ]]; then
    if command_exists sudo && sudo -n true >/dev/null 2>&1; then
      log "running control-plane host observability bootstrap with sudo"
      sudo -n env \
        FUGUE_CONTROL_PLANE_OBSERVABILITY_ONLY=true \
        "FUGUE_CONTROL_PLANE_OBSERVABILITY_ENABLED=${FUGUE_CONTROL_PLANE_OBSERVABILITY_ENABLED:-true}" \
        "FUGUE_CONTROL_PLANE_OBSERVABILITY_RESTART_K3S=${FUGUE_CONTROL_PLANE_OBSERVABILITY_RESTART_K3S:-true}" \
        "FUGUE_LOCAL_KUBE_API_READY_POLL_SECONDS=${LOCAL_KUBE_API_READY_POLL_SECONDS}" \
        "FUGUE_LOCAL_KUBE_API_READY_TIMEOUT_SECONDS=${LOCAL_KUBE_API_READY_TIMEOUT_SECONDS}" \
        KUBECONFIG=/etc/rancher/k3s/k3s.yaml \
        bash "${BASH_SOURCE[0]}"
      return 0
    fi
    if ensure_control_plane_observability_via_node_janitor; then
      return 0
    fi
    log "skip control-plane host observability bootstrap because upgrade is not running as root and passwordless sudo is unavailable"
    return 0
  fi

  mkdir -p /var/log/fugue/incidents /var/log/fugue/control-plane-baseline /var/log/journal /etc/systemd/system/k3s.service.d

  install -m 0755 /dev/stdin /usr/local/bin/fugue-control-plane-baseline-sample <<'EOF'
#!/usr/bin/env bash
set -u

out_dir="/var/log/fugue/control-plane-baseline"
out="${out_dir}/samples.log"
lock="${out_dir}/samples.lock"
max_lines="${FUGUE_CONTROL_PLANE_BASELINE_MAX_LINES:-14400}"
mkdir -p "${out_dir}"

if command -v flock >/dev/null 2>&1; then
  exec 9>"${lock}"
  flock -n 9 || exit 0
fi

field_from_meminfo() {
  local name="$1"
  awk -v name="${name}" '$1 == name ":" {print $2; exit}' /proc/meminfo 2>/dev/null || true
}

df_available_kb() {
  local path="$1"
  df -Pk "${path}" 2>/dev/null | awk 'NR == 2 {print $4; exit}' || true
}

dir_size_bytes() {
  local path="$1"
  if [[ -d "${path}" ]]; then
    du -sb "${path}" 2>/dev/null | awk '{print $1; exit}' || true
  fi
}

psi_avg10() {
  local path="$1"
  local class="$2"
  awk -v class="${class}" '$1 == class {
    for (i = 2; i <= NF; i++) {
      if ($i ~ /^avg10=/) {
        sub(/^avg10=/, "", $i)
        print $i
        exit
      }
    }
  }' "${path}" 2>/dev/null || true
}

systemd_show_flat() {
  if command -v systemctl >/dev/null 2>&1; then
    systemctl show k3s.service \
      -p ActiveState \
      -p SubState \
      -p Result \
      -p NRestarts \
      -p ExecMainStatus \
      -p MemoryCurrent \
      -p MemoryPeak \
      -p TasksCurrent \
      -p CPUUsageNSec 2>/dev/null |
      tr '\n' ' ' |
      sed 's/[[:space:]]*$//'
  fi
}

cgroup_flat() {
  local file="$1"
  if [[ -r "${file}" ]]; then
    tr '\n' ',' <"${file}" | sed 's/,$//'
  fi
}

top_processes_flat() {
  local sort_key="$1"
  local value_key="$2"
  ps -eo pid=,"${value_key}"=,comm= --sort="${sort_key}" 2>/dev/null |
    head -12 |
    awk '{printf "%s:%s:%s%s", $1, $2, $3, (NR == 12 ? "" : "|")}' || true
}

timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
boot_id="$(cat /proc/sys/kernel/random/boot_id 2>/dev/null || true)"
uptime_seconds="$(awk '{print int($1); exit}' /proc/uptime 2>/dev/null || true)"
loadavg="$(cat /proc/loadavg 2>/dev/null || true)"
mem_available_kb="$(field_from_meminfo MemAvailable)"
swap_free_kb="$(field_from_meminfo SwapFree)"
root_avail_kb="$(df_available_kb /)"
rancher_avail_kb="$(df_available_kb /var/lib/rancher)"
etcd_dir_bytes="$(dir_size_bytes /var/lib/rancher/k3s/server/db/etcd)"
cpu_psi_some_avg10="$(psi_avg10 /proc/pressure/cpu some)"
io_psi_some_avg10="$(psi_avg10 /proc/pressure/io some)"
io_psi_full_avg10="$(psi_avg10 /proc/pressure/io full)"
mem_psi_some_avg10="$(psi_avg10 /proc/pressure/memory some)"
mem_psi_full_avg10="$(psi_avg10 /proc/pressure/memory full)"
cgroup_dir="/sys/fs/cgroup/system.slice/k3s.service"
k3s_memory_current_bytes="$(cat "${cgroup_dir}/memory.current" 2>/dev/null || true)"
k3s_memory_peak_bytes="$(cat "${cgroup_dir}/memory.peak" 2>/dev/null || true)"
k3s_pids_current="$(cat "${cgroup_dir}/pids.current" 2>/dev/null || true)"
k3s_memory_events="$(cgroup_flat "${cgroup_dir}/memory.events")"
k3s_cpu_stat="$(cgroup_flat "${cgroup_dir}/cpu.stat")"
k3s_io_stat="$(cgroup_flat "${cgroup_dir}/io.stat")"
k3s_systemd="$(systemd_show_flat)"
top_rss_processes="$(top_processes_flat -rss rss)"
top_cpu_processes="$(top_processes_flat -pcpu pcpu)"

{
  printf 'timestamp_utc=%s ' "${timestamp}"
  printf 'boot_id=%s ' "${boot_id}"
  printf 'uptime_seconds=%s ' "${uptime_seconds}"
  printf 'loadavg="%s" ' "${loadavg}"
  printf 'mem_available_kb=%s swap_free_kb=%s ' "${mem_available_kb}" "${swap_free_kb}"
  printf 'root_avail_kb=%s rancher_avail_kb=%s etcd_dir_bytes=%s ' "${root_avail_kb}" "${rancher_avail_kb}" "${etcd_dir_bytes}"
  printf 'cpu_psi_some_avg10=%s io_psi_some_avg10=%s io_psi_full_avg10=%s mem_psi_some_avg10=%s mem_psi_full_avg10=%s ' \
    "${cpu_psi_some_avg10}" "${io_psi_some_avg10}" "${io_psi_full_avg10}" "${mem_psi_some_avg10}" "${mem_psi_full_avg10}"
  printf 'k3s_memory_current_bytes=%s k3s_memory_peak_bytes=%s k3s_pids_current=%s ' \
    "${k3s_memory_current_bytes}" "${k3s_memory_peak_bytes}" "${k3s_pids_current}"
  printf 'k3s_memory_events="%s" k3s_cpu_stat="%s" k3s_io_stat="%s" k3s_systemd="%s" ' \
    "${k3s_memory_events}" "${k3s_cpu_stat}" "${k3s_io_stat}" "${k3s_systemd}"
  printf 'top_rss_processes="%s" top_cpu_processes="%s"\n' \
    "${top_rss_processes}" "${top_cpu_processes}"
} >>"${out}"

if [[ "${max_lines}" =~ ^[0-9]+$ ]] && (( max_lines > 0 )); then
  line_count="$(wc -l <"${out}" 2>/dev/null || printf '0')"
  if [[ "${line_count}" =~ ^[0-9]+$ ]] && (( line_count > max_lines )); then
    tmp="${out}.tmp"
    tail -n "${max_lines}" "${out}" >"${tmp}" && mv "${tmp}" "${out}"
  fi
fi
EOF

  install -m 0755 /dev/stdin /usr/local/bin/fugue-k3s-incident-snapshot <<'EOF'
#!/usr/bin/env bash
set -u

unit="${1:-k3s.service}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
root="/var/log/fugue/incidents/k3s-${timestamp}"
mkdir -p "${root}"

run_with_timeout() {
  local duration="$1"
  shift
  if command -v timeout >/dev/null 2>&1; then
    timeout --kill-after=5s "${duration}" "$@"
  else
    "$@"
  fi
}

run_capture() {
  local name="$1"
  shift
  {
    printf '$'
    printf ' %q' "$@"
    printf '\n\n'
    run_with_timeout 45s "$@" 2>&1 || true
  } >"${root}/${name}.log"
}

count_matches() {
  local pattern="$1"
  shift
  grep -Eih "${pattern}" "$@" 2>/dev/null | wc -l | awk '{print $1}'
}

first_matches() {
  local label="$1"
  local pattern="$2"
  shift 2
  printf '  %s:\n' "${label}"
  grep -Eih "${pattern}" "$@" 2>/dev/null | head -8 | sed 's/^/    /' || true
}

write_diagnosis() {
  local k3s_log="${root}/k3s-journal.log"
  local kernel_log="${root}/kernel-journal.log"
  local systemd_log="${root}/systemctl-status.log"
  local cgroup_log="${root}/k3s-cgroup.log"
  local baseline_log="${root}/baseline-samples.log"
  local primary="no_single_failure_signal_identified"
  local systemd_failure leader_lost lease_timeout apiserver_timeout etcd_slow runtime_errors oom_memory disk_io kernel_fatal hung_task disk_pressure

  systemd_failure="$(count_matches 'Main process exited|Failed with result|status=[0-9]+/FAILURE|Result=exit-code|ExecMainStatus=[1-9]' "${k3s_log}" "${systemd_log}")"
  leader_lost="$(count_matches 'leaderelection lost|lost lease|failed to renew lease|Failed to update lock|error retrieving resource lock' "${k3s_log}")"
  lease_timeout="$(count_matches 'leader lease|Lease.*timeout|context deadline exceeded.*lease|timed out waiting for.*lease|Operation cannot be fulfilled.*Lease' "${k3s_log}")"
  apiserver_timeout="$(count_matches 'http: Handler timeout|apiserver.*timeout|context deadline exceeded|Trace\\[.*total time|too old resource version|client rate limiter Wait returned an error' "${k3s_log}")"
  etcd_slow="$(count_matches 'apply request took too long|etcdserver: request timed out|waiting for ReadIndex response took too long|slow fdatasync|slow apply|leader changed|database space exceeded|etcdserver: too many requests' "${k3s_log}")"
  runtime_errors="$(count_matches 'containerd|failed to create container|shim disconnected|PLEG|CRI|runtime service' "${k3s_log}")"
  oom_memory="$(count_matches 'Out of memory|Killed process|oom-kill|memory allocation failure|oom [1-9]|oom_kill [1-9]' "${kernel_log}" "${cgroup_log}")"
  disk_io="$(count_matches 'I/O error|Buffer I/O|blk_update_request|EXT4-fs error|nvme.*timeout|ata[0-9].*failed|read-only file system|No space left on device' "${kernel_log}" "${k3s_log}")"
  kernel_fatal="$(count_matches 'Kernel panic|kernel BUG|BUG:|Oops:|general protection fault|segfault' "${kernel_log}")"
  hung_task="$(count_matches 'blocked for more than|hung task|soft lockup|watchdog: BUG|RCU stall' "${kernel_log}")"
  disk_pressure="$(count_matches 'DiskPressure|eviction manager|image garbage collection failed|nodefs|imagefs' "${k3s_log}")"

  if (( kernel_fatal > 0 )); then
    primary="kernel_fatal_signal_evidence"
  elif (( oom_memory > 0 )); then
    primary="kernel_or_cgroup_oom_evidence"
  elif (( disk_io > 0 )); then
    primary="kernel_or_disk_io_error_evidence"
  elif (( leader_lost > 0 && (apiserver_timeout > 0 || etcd_slow > 0 || lease_timeout > 0) )); then
    primary="leader_election_lost_after_apiserver_or_etcd_timeouts"
  elif (( etcd_slow > 0 )); then
    primary="etcd_slow_request_evidence"
  elif (( apiserver_timeout > 0 )); then
    primary="apiserver_timeout_evidence"
  elif (( hung_task > 0 )); then
    primary="kernel_scheduler_or_lockup_evidence"
  elif (( systemd_failure > 0 )); then
    primary="systemd_recorded_k3s_process_failure"
  fi

  {
    printf 'timestamp_utc=%s\n' "${timestamp}"
    printf 'unit=%s\n' "${unit}"
    printf 'primary_failure_signal=%s\n' "${primary}"
    printf 'root_cause_status=evidence_summary_only_not_a_root_cause_claim\n'
    printf '\n'
    printf 'evidence_counts:\n'
    printf '  systemd_failure=%s\n' "${systemd_failure}"
    printf '  leader_election_lost=%s\n' "${leader_lost}"
    printf '  lease_timeout=%s\n' "${lease_timeout}"
    printf '  apiserver_timeout=%s\n' "${apiserver_timeout}"
    printf '  etcd_slow_or_timeout=%s\n' "${etcd_slow}"
    printf '  container_runtime_errors=%s\n' "${runtime_errors}"
    printf '  oom_or_memory_pressure=%s\n' "${oom_memory}"
    printf '  disk_or_io_errors=%s\n' "${disk_io}"
    printf '  kernel_fatal=%s\n' "${kernel_fatal}"
    printf '  hung_task_or_watchdog=%s\n' "${hung_task}"
    printf '  disk_pressure_or_eviction=%s\n' "${disk_pressure}"
    printf '\n'
    if [[ -s "${baseline_log}" ]]; then
      printf 'baseline_samples=%s\n' "$(wc -l <"${baseline_log}" 2>/dev/null || printf '0')"
      printf 'baseline_first=%s\n' "$(head -1 "${baseline_log}" 2>/dev/null || true)"
      printf 'baseline_last=%s\n' "$(tail -1 "${baseline_log}" 2>/dev/null || true)"
      printf '\n'
    fi
    printf 'selected_evidence:\n'
    first_matches 'systemd failure' 'Main process exited|Failed with result|status=[0-9]+/FAILURE|Result=exit-code|ExecMainStatus=[1-9]' "${k3s_log}" "${systemd_log}"
    first_matches 'leader lease' 'leaderelection lost|lost lease|failed to renew lease|Failed to update lock|error retrieving resource lock|leader lease' "${k3s_log}"
    first_matches 'apiserver or client timeout' 'http: Handler timeout|apiserver.*timeout|context deadline exceeded|Trace\\[.*total time|client rate limiter Wait returned an error' "${k3s_log}"
    first_matches 'etcd slow path' 'apply request took too long|etcdserver: request timed out|waiting for ReadIndex response took too long|slow fdatasync|slow apply|etcdserver: too many requests' "${k3s_log}"
    first_matches 'kernel memory or disk' 'Out of memory|Killed process|oom-kill|I/O error|Buffer I/O|EXT4-fs error|nvme.*timeout|No space left on device|read-only file system' "${kernel_log}" "${k3s_log}"
    printf '\n'
    printf 'next_checks:\n'
    case "${primary}" in
      leader_election_lost_after_apiserver_or_etcd_timeouts|etcd_slow_request_evidence|apiserver_timeout_evidence)
        printf '  - Compare baseline_last CPU/IO/memory pressure with k3s-cgroup.log and etcd-metrics-key.log.\n'
        printf '  - Inspect kube-metrics-key.log for apiserver request latency and etcd request latency near the restart.\n'
        printf '  - Inspect k3s-journal-key-events.log for the first timeout before leader election was lost.\n'
        ;;
      kernel_or_cgroup_oom_evidence)
        printf '  - Inspect kernel-key-events.log and k3s-cgroup.log memory.events for the killed process and cgroup OOM counters.\n'
        ;;
      kernel_or_disk_io_error_evidence)
        printf '  - Inspect kernel-key-events.log, filesystems.log, iostat.log, and k3s-cgroup.log io.stat for disk failure or saturation evidence.\n'
        ;;
      *)
        printf '  - Inspect k3s-journal-key-events.log first; then compare baseline-samples.log with pressure.log and systemctl-show.log.\n'
        ;;
    esac
  } >"${root}/diagnosis.txt"
}

{
  printf 'timestamp_utc=%s\n' "${timestamp}"
  printf 'unit=%s\n' "${unit}"
  printf 'boot_id=%s\n' "$(cat /proc/sys/kernel/random/boot_id 2>/dev/null || true)"
  hostnamectl 2>/dev/null || hostname
  uptime
} >"${root}/host.txt" 2>&1 || true

run_capture systemctl-status systemctl status "${unit}" --no-pager -l
run_capture systemctl-show systemctl show "${unit}" \
  -p Id \
  -p ActiveState \
  -p SubState \
  -p Result \
  -p NRestarts \
  -p ExecMainStartTimestamp \
  -p ExecMainExitTimestamp \
  -p ExecMainCode \
  -p ExecMainStatus \
  -p MemoryCurrent \
  -p MemoryPeak \
  -p TasksCurrent \
  -p CPUUsageNSec
run_capture systemd-critical-chain systemd-analyze critical-chain "${unit}"
run_capture k3s-journal journalctl -u "${unit}" --since "90 minutes ago" --no-pager -o short-iso -l
run_capture k3s-journal-key-events sh -c 'journalctl -u "$1" --since "90 minutes ago" --no-pager -o short-iso -l | grep -Ei "Main process exited|Failed with result|status=[0-9]+/FAILURE|leaderelection lost|lost lease|failed to renew lease|leader lease|apply request took too long|http: Handler timeout|context deadline exceeded|Trace\\[|etcdserver|ReadIndex|slow fdatasync|slow apply|too many requests|panic|fatal|No space left|DiskPressure|eviction|containerd|PLEG" || true' sh "${unit}"
run_capture container-runtime-journal journalctl -u k3s -u containerd --since "90 minutes ago" --no-pager -o short-iso -l
run_capture warning-journal journalctl -p warning --since "90 minutes ago" --no-pager -o short-iso -l
run_capture kernel-journal journalctl -k --since "90 minutes ago" --no-pager -o short-iso -l
run_capture kernel-key-events sh -c 'journalctl -k --since "90 minutes ago" --no-pager -o short-iso -l | grep -Ei "oom|out of memory|killed process|blocked for more than|hung task|watchdog|RCU stall|panic|BUG:|Oops:|segfault|I/O error|Buffer I/O|EXT4-fs error|nvme.*timeout|ata[0-9].*failed|read-only file system|No space left" || true'
run_capture dmesg-tail sh -c 'dmesg --ctime --color=never 2>/dev/null | tail -400'
run_capture memory free -h
run_capture filesystems df -h
run_capture processes sh -c 'ps -eo pid,ppid,stat,pcpu,pmem,rss,comm,args --sort=-pcpu | head -80'
run_capture processes-by-rss sh -c 'ps -eo pid,ppid,stat,pcpu,pmem,rss,comm,args --sort=-rss | head -80'
run_capture sockets sh -c 'ss -s; ss -tanp | head -120'
run_capture network sh -c 'ip -s link; echo; ip route; echo; command -v nstat >/dev/null 2>&1 && nstat -az || true; command -v conntrack >/dev/null 2>&1 && conntrack -S || true'
run_capture pressure sh -c 'for f in /proc/pressure/*; do echo "===== $f ====="; cat "$f"; done'
run_capture loadavg cat /proc/loadavg
run_capture meminfo cat /proc/meminfo
run_capture k3s-cgroup sh -c 'cg=/sys/fs/cgroup/system.slice/k3s.service; if [ -d "$cg" ]; then for f in memory.current memory.peak memory.events memory.events.local cpu.stat io.stat pids.current pids.max; do echo "===== $cg/$f ====="; cat "$cg/$f" 2>/dev/null || true; done; fi'
run_capture etcd-size sh -c 'du -sh /var/lib/rancher/k3s/server/db/etcd 2>/dev/null || true; find /var/lib/rancher/k3s/server/db/etcd -maxdepth 3 -type f -printf "%s %p\n" 2>/dev/null | sort -nr | head -40'
run_capture k3s-config-redacted sh -c 'if [ -r /etc/rancher/k3s/config.yaml ]; then sed -E "s/^([[:space:]]*[^#[:space:]]*(token|secret|password|key)[^:]*:).*/\\1 <redacted>/I" /etc/rancher/k3s/config.yaml | tail -200; fi'
run_capture k3s-token-stat sh -c 'if [ -e /var/lib/rancher/k3s/server/token ]; then stat -c "%n mode=%a owner=%U:%G size=%s mtime=%y" /var/lib/rancher/k3s/server/token 2>/dev/null || ls -l /var/lib/rancher/k3s/server/token; fi'
if [[ -f /var/log/fugue/control-plane-baseline/samples.log ]]; then
  tail -n 720 /var/log/fugue/control-plane-baseline/samples.log >"${root}/baseline-samples.log" 2>/dev/null || true
fi
if compgen -G "/var/log/fugue/kubernetes/audit.log*" >/dev/null 2>&1; then
  run_capture kube-audit-tail sh -c 'for f in /var/log/fugue/kubernetes/audit.log*; do echo "===== $f ====="; case "$f" in *.gz) gzip -dc "$f" 2>/dev/null | tail -400 ;; *) tail -n 400 "$f" 2>/dev/null ;; esac; done'
fi

if command -v vmstat >/dev/null 2>&1; then
  run_capture vmstat vmstat 1 10
fi
if command -v iostat >/dev/null 2>&1; then
  run_capture iostat iostat -xz 1 10
fi
if command -v sar >/dev/null 2>&1; then
  run_capture sar-cpu sar -u -r -q -b -d -S -W -n DEV 1 5
  run_capture sar-recent sh -c 'sar -A 2>/dev/null | tail -500'
fi
if command -v k3s >/dev/null 2>&1; then
  run_capture crictl-info k3s crictl info
  run_capture crictl-ps k3s crictl ps -a
  run_capture crictl-stats k3s crictl stats
  run_capture kubectl-nodes k3s kubectl get nodes -o wide
  run_capture kubectl-pods k3s kubectl get pods -A -o wide
  run_capture kubectl-top-containers k3s kubectl top pods -A --containers
  run_capture kubectl-leases k3s kubectl get leases -A
  run_capture kube-readyz k3s kubectl get --raw=/readyz?verbose
  run_capture kube-livez k3s kubectl get --raw=/livez?verbose
  run_capture kube-metrics k3s kubectl get --raw=/metrics
  run_capture kube-metrics-key sh -c 'k3s kubectl get --raw=/metrics | grep -E "^(apiserver_request_duration_seconds|apiserver_storage_objects|etcd_request_duration_seconds|etcd_db_total_size|rest_client_request_duration_seconds|workqueue_|process_|go_)" | head -5000'
  run_capture kube-events k3s kubectl get events -A --sort-by=.lastTimestamp
fi
if command -v curl >/dev/null 2>&1; then
  run_capture etcd-metrics curl -fsS --max-time 15 http://127.0.0.1:2381/metrics
  run_capture etcd-metrics-key sh -c 'curl -fsS --max-time 15 http://127.0.0.1:2381/metrics | grep -E "^(etcd_disk_wal_fsync_duration_seconds|etcd_disk_backend_commit_duration_seconds|etcd_server_leader_changes_seen_total|etcd_server_proposals|etcd_mvcc_db_total_size_in_bytes|grpc_server_handled_total|process_|go_)" | head -5000'
fi

write_diagnosis
tar -C "$(dirname "${root}")" -czf "${root}.tar.gz" "$(basename "${root}")" 2>/dev/null || true
ln -sfn "$(basename "${root}")" /var/log/fugue/incidents/latest-k3s 2>/dev/null || true
ln -sfn "$(basename "${root}.tar.gz")" /var/log/fugue/incidents/latest-k3s.tar.gz 2>/dev/null || true
find /var/log/fugue/incidents -maxdepth 1 -type f -name 'k3s-*.tar.gz' -mtime +14 -delete 2>/dev/null || true
find /var/log/fugue/incidents -maxdepth 1 -type d -name 'k3s-*' -mtime +14 -exec rm -rf {} + 2>/dev/null || true
EOF

  cat >/etc/systemd/system/fugue-control-plane-baseline.service <<'EOF'
[Unit]
Description=Record Fugue control-plane host baseline sample
Documentation=https://github.com/yym68686/fugue

[Service]
Type=oneshot
ExecStart=/usr/local/bin/fugue-control-plane-baseline-sample
Nice=10
IOSchedulingClass=best-effort
IOSchedulingPriority=7
EOF

  cat >/etc/systemd/system/fugue-control-plane-baseline.timer <<'EOF'
[Unit]
Description=Record Fugue control-plane host baseline samples
Documentation=https://github.com/yym68686/fugue

[Timer]
OnBootSec=30s
OnUnitActiveSec=30s
AccuracySec=5s
Persistent=false
Unit=fugue-control-plane-baseline.service

[Install]
WantedBy=timers.target
EOF

  cat >/etc/systemd/system/fugue-k3s-failure@.service <<'EOF'
[Unit]
Description=Capture Fugue k3s incident snapshot for %i
Documentation=https://github.com/yym68686/fugue

[Service]
Type=oneshot
ExecStart=/usr/local/bin/fugue-k3s-incident-snapshot %i
EOF

  cat >/etc/systemd/system/k3s.service.d/90-fugue-observability.conf <<'EOF'
[Unit]
OnFailure=fugue-k3s-failure@%n.service
EOF

  if command_exists systemctl; then
    systemctl daemon-reload || true
    systemctl enable --now fugue-control-plane-baseline.timer >/dev/null 2>&1 || true
  fi
  # The k3s config step can restart k3s, so the failure hook must already exist.
  ensure_local_k3s_audit_and_metrics_config

  if command_exists apt-get && ! command_exists sar; then
    log "installing sysstat for host-level control-plane history"
    DEBIAN_FRONTEND=noninteractive apt-get update -y >/dev/null 2>&1 &&
      DEBIAN_FRONTEND=noninteractive apt-get install -y sysstat >/dev/null 2>&1 ||
      log "sysstat install failed; continuing without host history package"
  fi
  if [[ -f /etc/default/sysstat ]]; then
    sed -i 's/^ENABLED=.*/ENABLED="true"/' /etc/default/sysstat || true
  fi
  if command_exists systemctl; then
    for unit in sysstat.service sysstat-collect.timer sysstat-summary.timer; do
      if systemctl list-unit-files "${unit}" >/dev/null 2>&1; then
        systemctl enable --now "${unit}" >/dev/null 2>&1 || true
      fi
    done
  fi

  log "control-plane observability bootstrap complete"
}

ensure_local_k3s_audit_and_metrics_config() {
  local config="/etc/rancher/k3s/config.yaml"
  local audit_policy="/etc/rancher/k3s/audit-policy.yaml"
  local tmp=""
  local filtered=""
  local changed="false"

  mkdir -p /etc/rancher/k3s /var/log/fugue/kubernetes
  tmp="$(mktemp)"
  cat >"${tmp}" <<'EOF'
apiVersion: audit.k8s.io/v1
kind: Policy
omitStages:
  - RequestReceived
rules:
  - level: Metadata
    verbs:
      - create
      - update
      - patch
      - delete
  - level: Metadata
    resources:
      - group: coordination.k8s.io
        resources:
          - leases
  - level: None
EOF
  if [[ ! -f "${audit_policy}" ]] || ! cmp -s "${tmp}" "${audit_policy}"; then
    install -m 0644 "${tmp}" "${audit_policy}"
    changed="true"
  fi
  rm -f "${tmp}"

  if [[ -f "${config}" ]]; then
    filtered="$(awk '
      /^# BEGIN FUGUE CONTROL PLANE OBSERVABILITY$/ {skip=1; next}
      /^# END FUGUE CONTROL PLANE OBSERVABILITY$/ {skip=0; next}
      !skip {print}
    ' "${config}")"
  else
    log "creating ${config} for Fugue control-plane observability settings"
    filtered=""
  fi

  tmp="$(mktemp)"
  {
    printf '%s\n' "${filtered}" | sed '${/^$/d;}'
    cat <<'EOF'
# BEGIN FUGUE CONTROL PLANE OBSERVABILITY
etcd-expose-metrics: true
kube-apiserver-arg:
  - "audit-policy-file=/etc/rancher/k3s/audit-policy.yaml"
  - "audit-log-path=/var/log/fugue/kubernetes/audit.log"
  - "audit-log-maxage=14"
  - "audit-log-maxbackup=10"
  - "audit-log-maxsize=100"
  - "profiling=true"
# END FUGUE CONTROL PLANE OBSERVABILITY
EOF
  } >"${tmp}"

  if ! cmp -s "${tmp}" "${config}"; then
    if [[ -f "${config}" ]]; then
      cp "${config}" "${config}.fugue-observability-$(date -u +%Y%m%d%H%M%S).bak"
    fi
    install -m 0644 "${tmp}" "${config}"
    changed="true"
  fi
  rm -f "${tmp}"

  if [[ "${changed}" != "true" ]]; then
    log "local k3s audit/metrics config already current"
    return 0
  fi

  log "updated local k3s audit log and etcd metrics config"
  if [[ "${FUGUE_CONTROL_PLANE_OBSERVABILITY_RESTART_K3S:-true}" != "true" ]]; then
    log "k3s restart for observability config disabled; changes apply on the next k3s restart"
    return 0
  fi
  if command_exists systemctl && systemctl is-active --quiet k3s; then
    log "restarting local k3s once so audit/metrics config takes effect"
    if command_exists timeout; then
      timeout --kill-after=15s 120s systemctl restart k3s
    else
      systemctl restart k3s
    fi
    wait_for_local_kube_api_ready
  fi
}

detect_primary_private_ip() {
  ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i=1;i<=NF;i++) if ($i=="src") {print $(i+1); exit}}'
}

detect_existing_registry_pull_base() {
  if [[ ! -r /etc/rancher/k3s/registries.yaml ]]; then
    return 1
  fi
  local value=""
  value="$(awk '
    $1 == "mirrors:" { in_mirrors = 1; next }
    in_mirrors && /^[[:space:]]*"/ {
      value = $1
      gsub(/"/, "", value)
      sub(/:$/, "", value)
      print value
      exit
    }
  ' /etc/rancher/k3s/registries.yaml)"
  if [[ -z "${value}" ]]; then
    return 1
  fi
  if is_legacy_nodeport_registry_pull_base "${value}"; then
    return 1
  fi
  printf '%s' "${value}"
}

is_legacy_nodeport_registry_pull_base() {
  local value="$1"
  local host="${value%:*}"
  local port="${value##*:}"
  if [[ "${host}" == "${value}" || "${port}" != "${FUGUE_REGISTRY_NODEPORT:-30500}" ]]; then
    return 1
  fi
  case "${host}" in
    10.*|192.168.*|127.*|100.64.*|100.65.*|100.66.*|100.67.*|100.68.*|100.69.*|100.70.*|100.71.*|100.72.*|100.73.*|100.74.*|100.75.*|100.76.*|100.77.*|100.78.*|100.79.*|100.80.*|100.81.*|100.82.*|100.83.*|100.84.*|100.85.*|100.86.*|100.87.*|100.88.*|100.89.*|100.90.*|100.91.*|100.92.*|100.93.*|100.94.*|100.95.*|100.96.*|100.97.*|100.98.*|100.99.*|100.100.*|100.101.*|100.102.*|100.103.*|100.104.*|100.105.*|100.106.*|100.107.*|100.108.*|100.109.*|100.110.*|100.111.*|100.112.*|100.113.*|100.114.*|100.115.*|100.116.*|100.117.*|100.118.*|100.119.*|100.120.*|100.121.*|100.122.*|100.123.*|100.124.*|100.125.*|100.126.*|100.127.*)
      return 0
      ;;
    172.16.*|172.17.*|172.18.*|172.19.*|172.20.*|172.21.*|172.22.*|172.23.*|172.24.*|172.25.*|172.26.*|172.27.*|172.28.*|172.29.*|172.30.*|172.31.*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

detect_primary_mesh_ip() {
  if ! command_exists tailscale; then
    return 1
  fi
  tailscale ip -4 2>/dev/null | awk 'NR == 1 {print; exit}'
}

detect_control_plane_k3s_version() {
  if ! command_exists k3s; then
    return 1
  fi
  k3s --version 2>/dev/null | awk 'NR == 1 {print $3; exit}'
}

detect_config_secret_value() {
  local key="$1"
  local secret_name="${FUGUE_RELEASE_FULLNAME}-config"
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get secret "${secret_name}" -o "jsonpath={.data.${key}}" 2>/dev/null | base64 --decode 2>/dev/null || true
}

detect_cluster_join_server() {
  detect_config_secret_value FUGUE_CLUSTER_JOIN_SERVER
}

detect_cluster_join_server_fallbacks() {
  detect_config_secret_value FUGUE_CLUSTER_JOIN_SERVER_FALLBACKS
}

detect_cluster_join_k3s_version() {
  detect_config_secret_value FUGUE_CLUSTER_JOIN_K3S_VERSION
}

detect_cluster_join_mesh_provider() {
  detect_config_secret_value FUGUE_CLUSTER_JOIN_MESH_PROVIDER
}

detect_cluster_join_mesh_login_server() {
  detect_config_secret_value FUGUE_CLUSTER_JOIN_MESH_LOGIN_SERVER
}

detect_cluster_join_mesh_auth_key() {
  detect_config_secret_value FUGUE_CLUSTER_JOIN_MESH_AUTH_KEY
}

control_plane_postgres_name() {
  local name=""
  name="$(trim_field "${FUGUE_CONTROL_PLANE_POSTGRES_NAME:-}")"
  if [[ -n "${name}" ]]; then
    printf '%s' "${name}"
    return
  fi
  printf '%s-control-plane-postgres' "${FUGUE_RELEASE_FULLNAME}"
}

control_plane_postgres_rw_service_host() {
  printf '%s-rw.%s.svc.cluster.local' "$(control_plane_postgres_name)" "${FUGUE_NAMESPACE}"
}

control_plane_postgres_primary_selector() {
  printf 'cnpg.io/cluster=%s,role=primary' "$(control_plane_postgres_name)"
}

control_plane_postgres_primary_pod_name() {
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get pods \
    -l "$(control_plane_postgres_primary_selector)" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

control_plane_postgres_query() {
  local pod_name="$1"
  local sql="$2"

  [[ -n "$(trim_field "${pod_name}")" ]] || return 1
  printf '%s\n' "${sql}" |
    ${KUBECTL} -n "${FUGUE_NAMESPACE}" exec -i "${pod_name}" -- \
      psql -d "${FUGUE_CONTROL_PLANE_POSTGRES_DATABASE}" -Atq -v ON_ERROR_STOP=1
}

control_plane_active_pg_dump_pids() {
  local pod_name="$1"

  control_plane_postgres_query "${pod_name}" \
    "SELECT COALESCE(string_agg(pid::text, ',' ORDER BY pid), '') FROM pg_stat_activity WHERE datname = current_database() AND application_name = 'pg_dump';"
}

control_plane_backup_runs_table_exists() {
  local pod_name="$1"
  local result=""

  result="$(control_plane_postgres_query "${pod_name}" "SELECT CASE WHEN to_regclass('public.fugue_backup_runs') IS NULL THEN 'false' ELSE 'true' END;" 2>/dev/null || true)"
  [[ "$(trim_field "${result}")" == "true" ]]
}

control_plane_running_backup_count() {
  local pod_name="$1"

  if ! control_plane_backup_runs_table_exists "${pod_name}"; then
    printf '0'
    return 0
  fi
  control_plane_postgres_query "${pod_name}" \
    "SELECT count(*) FROM fugue_backup_runs WHERE target_type = 'control-plane-db' AND status = 'running';" 2>/dev/null ||
    printf '0'
}

control_plane_recent_backup_success_exists() {
  local pod_name="$1"
  local result=""

  if ! control_plane_backup_runs_table_exists "${pod_name}"; then
    return 1
  fi
  result="$(control_plane_postgres_query "${pod_name}" \
    "SELECT CASE WHEN EXISTS (SELECT 1 FROM fugue_backup_runs WHERE target_type = 'control-plane-db' AND status = 'succeeded' AND finished_at > now() - make_interval(secs => ${FUGUE_CONTROL_PLANE_BACKUP_DRAIN_RECENT_SUCCESS_SECONDS})) THEN 'true' ELSE 'false' END;" 2>/dev/null || true)"
  [[ "$(trim_field "${result}")" == "true" ]]
}

control_plane_terminate_pg_dump_backends() {
  local pod_name="$1"

  control_plane_postgres_query "${pod_name}" \
    "SELECT count(*) FROM (SELECT pg_terminate_backend(pid) AS terminated FROM pg_stat_activity WHERE datname = current_database() AND application_name = 'pg_dump') s WHERE terminated;"
}

drain_control_plane_backup_before_schema_rollout() {
  local mode="${FUGUE_CONTROL_PLANE_BACKUP_DRAIN_MODE}"
  local pod_name=""
  local deadline
  local pids=""
  local running_count=""
  local terminated=""

  [[ "${FUGUE_CONTROL_PLANE_POSTGRES_ENABLED}" == "true" ]] || return 0
  [[ "${FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API}" == "true" ]] || return 0

  if [[ "${mode}" == "skip" ]]; then
    log "control-plane backup drain skipped by FUGUE_CONTROL_PLANE_BACKUP_DRAIN_MODE=skip"
    return 0
  fi
  if ! control_plane_backup_drain_required; then
    log "control-plane backup drain skipped; release changed files do not require a schema/runtime rollout"
    return 0
  fi

  pod_name="$(trim_field "$(control_plane_postgres_primary_pod_name)")"
  if [[ -z "${pod_name}" ]]; then
    log "control-plane backup drain skipped; no CNPG primary pod found for $(control_plane_postgres_name)"
    return 0
  fi

  deadline=$((SECONDS + FUGUE_CONTROL_PLANE_BACKUP_DRAIN_WAIT_SECONDS))
  while true; do
    pids="$(trim_field "$(control_plane_active_pg_dump_pids "${pod_name}" 2>/dev/null || true)")"
    running_count="$(trim_field "$(control_plane_running_backup_count "${pod_name}" 2>/dev/null || true)")"
    running_count="${running_count:-0}"
    if [[ -z "${pids}" ]]; then
      if [[ "${running_count}" != "0" ]]; then
        log "control-plane backup run is marked running but no active pg_dump backend is holding database locks; continuing"
      fi
      return 0
    fi
    if ((SECONDS >= deadline)); then
      break
    fi
    log "control-plane backup pg_dump active before schema rollout: pids=${pids} running_backup_runs=${running_count}; waiting"
    sleep "${FUGUE_CONTROL_PLANE_BACKUP_DRAIN_POLL_SECONDS}"
  done

  if [[ "${mode}" == "wait" ]]; then
    fail "control-plane backup pg_dump is still active after ${FUGUE_CONTROL_PLANE_BACKUP_DRAIN_WAIT_SECONDS}s; set FUGUE_CONTROL_PLANE_BACKUP_DRAIN_MODE=terminate or retry after the backup finishes"
  fi

  if ! control_plane_recent_backup_success_exists "${pod_name}"; then
    fail "refusing to terminate active control-plane backup pg_dump because no recent successful control-plane-db backup was found in the last ${FUGUE_CONTROL_PLANE_BACKUP_DRAIN_RECENT_SUCCESS_SECONDS}s"
  fi

  terminated="$(trim_field "$(control_plane_terminate_pg_dump_backends "${pod_name}" 2>/dev/null || true)")"
  terminated="${terminated:-0}"
  log "terminated ${terminated} active control-plane backup pg_dump backend(s) before schema rollout; backup runner will retry"
  sleep "${FUGUE_CONTROL_PLANE_BACKUP_DRAIN_POST_TERMINATE_SLEEP_SECONDS}"
}

detect_existing_api_database_url() {
  local secret_name="${FUGUE_RELEASE_FULLNAME}-config"
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get secret "${secret_name}" -o jsonpath='{.data.FUGUE_DATABASE_URL}' 2>/dev/null | base64 --decode 2>/dev/null || true
}

api_database_already_uses_control_plane_postgres() {
  local current_url=""
  local target_host=""
  current_url="$(detect_existing_api_database_url)"
  [[ -n "${current_url}" ]] || return 1
  target_host="$(control_plane_postgres_rw_service_host)"
  case "${current_url}" in
    *"@${target_host}:"*|*"@${target_host}/"*|*"@${target_host}?"*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
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

ensure_local_registry_mirror_config() {
  local registry_base="$1"
  local endpoint="$2"
  local endpoint_url=""
  local tmp=""
  local target="/etc/rancher/k3s/registries.yaml"

  registry_base="$(printf '%s' "${registry_base}" | awk '{$1=$1; print}')"
  endpoint="$(printf '%s' "${endpoint}" | awk '{$1=$1; print}')"
  if [[ -z "${registry_base}" || -z "${endpoint}" ]]; then
    return 0
  fi
  if [[ "$(id -u)" != "0" ]]; then
    log "skip local registries.yaml migration because upgrade is not running as root"
    return 0
  fi
  if [[ ! -d /etc/rancher/k3s ]]; then
    log "skip local registries.yaml migration because /etc/rancher/k3s is absent"
    return 0
  fi

  endpoint_url="$(registry_endpoint_url_value "${endpoint}")"
  tmp="$(mktemp)"
  cat >"${tmp}" <<EOF
mirrors:
  "${registry_base}":
    endpoint:
      - "${endpoint_url}"
configs:
  "${registry_base}":
    tls:
      insecure_skip_verify: true
EOF

  if [[ -r "${target}" ]] && cmp -s "${tmp}" "${target}"; then
    rm -f "${tmp}"
    log "local registries.yaml already points ${registry_base} at ${endpoint_url}"
    return 0
  fi

  install -m 0644 "${tmp}" "${target}"
  rm -f "${tmp}"
  log "migrated local registries.yaml to mirror ${registry_base} via ${endpoint_url}"

  if command_exists systemctl; then
    if systemctl is-active --quiet k3s; then
      log "restarting local k3s so containerd reloads registry mirror configuration"
      if command_exists timeout; then
        timeout --kill-after=15s 120s systemctl restart k3s
      else
        systemctl restart k3s
      fi
      wait_for_local_kube_api_ready
    elif systemctl is-active --quiet k3s-agent; then
      log "restarting local k3s-agent so containerd reloads registry mirror configuration"
      if command_exists timeout; then
        timeout --kill-after=15s 120s systemctl restart k3s-agent
      else
        systemctl restart k3s-agent
      fi
    fi
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

deployment_canary_ready() {
  local deployment_name="$1"
  local status updated ready available generation observed

  status="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get "deploy/${deployment_name}" \
    -o jsonpath='{.status.updatedReplicas} {.status.readyReplicas} {.status.availableReplicas} {.metadata.generation} {.status.observedGeneration}' 2>/dev/null || true)"
  read -r updated ready available generation observed <<<"${status}"
  updated="${updated:-0}"
  ready="${ready:-0}"
  available="${available:-0}"
  generation="${generation:-0}"
  observed="${observed:-0}"
  [[ "${updated}" =~ ^[0-9]+$ ]] || updated=0
  [[ "${ready}" =~ ^[0-9]+$ ]] || ready=0
  [[ "${available}" =~ ^[0-9]+$ ]] || available=0
  [[ "${generation}" =~ ^[0-9]+$ ]] || generation=0
  [[ "${observed}" =~ ^[0-9]+$ ]] || observed=0
  (( observed >= generation && updated >= 1 && ready >= 1 && available >= 1 ))
}

wait_for_deployment_canary_ready() {
  local deployment_name="$1"
  local timeout="${FUGUE_CONTROL_PLANE_CANARY_TIMEOUT_SECONDS:-120}"
  local delay="${FUGUE_CONTROL_PLANE_CANARY_DELAY_SECONDS:-5}"
  local deadline

  deployment_exists "${deployment_name}" || return 0
  deadline=$((SECONDS + timeout))
  while true; do
    if deployment_canary_ready "${deployment_name}"; then
      log "control-plane canary ready: deployment=${deployment_name} updated_ready_replicas>=1"
      return 0
    fi
    if (( SECONDS >= deadline )); then
      log "control-plane canary not ready before timeout: deployment=${deployment_name}"
      return 1
    fi
    log "waiting for control-plane canary: deployment=${deployment_name}"
    sleep "${delay}"
  done
}

control_plane_readyz_probe() {
  local api_base

  api_base="$(release_api_base_url)"
  curl -fsS "${api_base}/readyz" >/dev/null
}

control_plane_canary_readiness_gate() {
  local summary

  case "${FUGUE_CONTROL_PLANE_CANARY_GATE_ENABLED:-true}" in
    1|true|TRUE|yes|YES)
      ;;
    *)
      log "control-plane canary gate disabled"
      return 0
      ;;
  esac
  wait_for_deployment_canary_ready "${FUGUE_API_DEPLOYMENT_NAME}" || return 1
  wait_for_deployment_canary_ready "${FUGUE_CONTROLLER_DEPLOYMENT_NAME}" || return 1
  retry "${FUGUE_CONTROL_PLANE_CANARY_READY_RETRIES:-12}" "${FUGUE_CONTROL_PLANE_CANARY_READY_DELAY_SECONDS:-5}" control_plane_readyz_probe || {
    log "control-plane canary API readiness failed"
    return 1
  }
  if summary="$(release_guard_status_summary 2>/dev/null)"; then
    log "control-plane canary release guard passed: ${summary}"
  else
    # The release-guard endpoint can be introduced by the target release, while
    # the service can still route canary checks to old API pods.
    log "warning: control-plane canary release guard unavailable during mixed-version rollout: ${summary:-unknown}"
  fi
  if summary="$(robustness_status_summary "${ROBUSTNESS_HEALTH_GATE_BASELINE_FILE:-}")"; then
    log "control-plane canary robustness/route readiness passed: ${summary}"
  else
    log "control-plane canary robustness/route readiness failed: ${summary:-unknown}"
    return 1
  fi
}

daemonset_exists() {
  local daemonset_name="$1"
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" >/dev/null 2>&1
}

live_deployment_container_image() {
  local deployment_name="$1"
  local container_name="$2"

  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get "deploy/${deployment_name}" \
    -o jsonpath="{.spec.template.spec.containers[?(@.name==\"${container_name}\")].image}" 2>/dev/null || true
}

live_deployment_container_digest_ref() {
  local deployment_name="$1"
  local container_name="$2"
  local selector=""

  selector="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get "deploy/${deployment_name}" -o json 2>/dev/null | python3 -c '
import json
import sys

try:
    deployment = json.load(sys.stdin)
except Exception:
    raise SystemExit(1)
selector = deployment.get("spec", {}).get("selector", {}) or {}
labels = selector.get("matchLabels") or {}
if not labels or selector.get("matchExpressions"):
    raise SystemExit(1)
print(",".join(f"{key}={labels[key]}" for key in sorted(labels)))
')" || return 1
  selector="$(trim_field "${selector}")"
  [[ -n "${selector}" ]] || return 1

  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get pods -l "${selector}" -o json 2>/dev/null | python3 -c '
import json
import re
import sys

container_name = sys.argv[1]
try:
    pods = json.load(sys.stdin)
except Exception:
    raise SystemExit(1)
digest_refs = set()
for pod in pods.get("items", []):
    metadata = pod.get("metadata", {})
    status = pod.get("status", {})
    if metadata.get("deletionTimestamp") or status.get("phase") != "Running":
        continue
    if not any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in status.get("conditions") or []
    ):
        continue
    for container in status.get("containerStatuses") or []:
        if container.get("name") != container_name or not container.get("ready"):
            continue
        image_id = str(container.get("imageID") or "").strip()
        for prefix in ("docker-pullable://", "containerd://"):
            if image_id.startswith(prefix):
                image_id = image_id[len(prefix):]
                break
        if re.fullmatch(r"[^@\s]+@sha256:[0-9a-fA-F]{64}", image_id):
            digest_refs.add(image_id)
if len(digest_refs) != 1:
    raise SystemExit(1)
print(next(iter(digest_refs)))
' "${container_name}"
}

rollback_image_digest_ref_valid() {
  local digest_ref
  digest_ref="$(trim_field "$1")"
  [[ "${digest_ref}" =~ ^[^@[:space:]]+@sha256:[0-9a-fA-F]{64}$ ]]
}

container_runtime_socket_accessible() {
  local socket_path="$1"
  [[ -S "${socket_path}" && -r "${socket_path}" && -w "${socket_path}" ]]
}

container_runtime_pull_command() {
  local k3s_socket="${FUGUE_K3S_CONTAINERD_SOCKET:-/run/k3s/containerd/containerd.sock}"
  local containerd_socket="${FUGUE_CONTAINERD_SOCKET:-/run/containerd/containerd.sock}"

  if command_exists k3s && container_runtime_socket_accessible "${k3s_socket}"; then
    printf '%s' "k3s"
    return 0
  fi
  if command_exists ctr && container_runtime_socket_accessible "${containerd_socket}"; then
    printf '%s' "ctr"
    return 0
  fi
  return 1
}

verify_registry_image_by_digest() {
  local digest_ref="$1"
  local timeout_seconds="$2"
  local verifier="${REPO_ROOT}/scripts/verify_registry_image.py"
  local platform="${FUGUE_ROLLBACK_IMAGE_PLATFORM:-linux/amd64}"

  command_exists python3 || {
    log "rollback registry verification requires python3"
    return 1
  }
  [[ -f "${verifier}" && -r "${verifier}" ]] || {
    log "rollback registry verifier is unavailable: ${verifier}"
    return 1
  }
  python3 "${verifier}" \
    --image "${digest_ref}" \
    --platform "${platform}" \
    --timeout-seconds "${timeout_seconds}"
}

pull_rollback_image_by_digest() {
  local digest_ref="$1"
  local timeout_seconds="${FUGUE_ROLLBACK_IMAGE_PULL_TIMEOUT_SECONDS:-120}"
  local runtime_command=""

  [[ "${timeout_seconds}" =~ ^[1-9][0-9]*$ ]] || {
    log "rollback image pull timeout must be a positive integer: ${timeout_seconds}"
    return 1
  }
  if runtime_command="$(container_runtime_pull_command)"; then
    command_exists timeout || {
      log "rollback image pull requires the timeout command"
      return 1
    }
    case "${runtime_command}" in
      k3s)
        timeout --kill-after=10s "${timeout_seconds}s" \
          k3s ctr --address "${FUGUE_K3S_CONTAINERD_SOCKET:-/run/k3s/containerd/containerd.sock}" \
          images pull "${digest_ref}" >/dev/null
        ;;
      ctr)
        timeout --kill-after=10s "${timeout_seconds}s" \
          ctr --address "${FUGUE_CONTAINERD_SOCKET:-/run/containerd/containerd.sock}" \
          --namespace k8s.io images pull "${digest_ref}" >/dev/null
        ;;
      *)
        log "unsupported container runtime pull command: ${runtime_command}"
        return 1
        ;;
    esac
    return
  fi
  log "container runtime socket is unavailable to the release runner; verifying rollback image through the OCI registry"
  verify_registry_image_by_digest "${digest_ref}" "${timeout_seconds}"
}

verify_rollback_deployment_image() {
  local deployment_name="$1"
  local container_name="$2"
  local image_ref digest_ref image_repository digest_repository
  local attempts="${FUGUE_ROLLBACK_IMAGE_PULL_ATTEMPTS:-3}"
  local delay_seconds="${FUGUE_ROLLBACK_IMAGE_PULL_RETRY_DELAY_SECONDS:-2}"

  image_ref="$(trim_field "$(live_deployment_container_image "${deployment_name}" "${container_name}")")"
  [[ -n "${image_ref}" ]] || {
    log "rollback image preflight could not read live image for ${deployment_name}/${container_name}"
    return 1
  }
  if ! digest_ref="$(live_deployment_container_digest_ref "${deployment_name}" "${container_name}")"; then
    log "rollback image preflight requires one consistent Ready Pod digest for ${deployment_name}/${container_name}"
    return 1
  fi
  digest_ref="$(trim_field "${digest_ref}")"
  rollback_image_digest_ref_valid "${digest_ref}" || {
    log "rollback image preflight found an invalid digest ref for ${deployment_name}/${container_name}: ${digest_ref:-<empty>}"
    return 1
  }
  image_repository="$(image_ref_repository "${image_ref}")"
  digest_repository="$(image_ref_repository "${digest_ref}")"
  if [[ -z "${image_repository}" || "${image_repository}" != "${digest_repository}" ]]; then
    log "rollback image preflight repository mismatch for ${deployment_name}/${container_name}: live=${image_repository:-<empty>} digest=${digest_repository:-<empty>}"
    return 1
  fi
  [[ "${attempts}" =~ ^[1-9][0-9]*$ && "${delay_seconds}" =~ ^[0-9]+$ ]] || {
    log "rollback image pull retry settings are invalid: attempts=${attempts} delay_seconds=${delay_seconds}"
    return 1
  }
  if ! retry "${attempts}" "${delay_seconds}" pull_rollback_image_by_digest "${digest_ref}"; then
    log "rollback image digest is not pullable for ${deployment_name}/${container_name}: ${digest_ref}"
    return 1
  fi
  log "rollback image digest verified for ${deployment_name}/${container_name}: ${digest_ref}"
}

verify_control_plane_rollback_images() {
  verify_rollback_deployment_image "${FUGUE_API_DEPLOYMENT_NAME}" "api" || return 1
  verify_rollback_deployment_image "${FUGUE_CONTROLLER_DEPLOYMENT_NAME}" "controller"
}

run_control_plane_rollback_image_preflight() {
  local mode
  mode="$(trim_field "${FUGUE_ROLLBACK_IMAGE_PREFLIGHT_MODE:-shadow}")"

  case "${mode}" in
    off|disabled)
      log "rollback image preflight disabled"
      return 0
      ;;
    shadow)
      local FUGUE_ROLLBACK_IMAGE_PULL_TIMEOUT_SECONDS="${FUGUE_ROLLBACK_IMAGE_SHADOW_PULL_TIMEOUT_SECONDS:-20}"
      local FUGUE_ROLLBACK_IMAGE_PULL_ATTEMPTS="${FUGUE_ROLLBACK_IMAGE_SHADOW_PULL_ATTEMPTS:-1}"
      local FUGUE_ROLLBACK_IMAGE_PULL_RETRY_DELAY_SECONDS="${FUGUE_ROLLBACK_IMAGE_SHADOW_PULL_RETRY_DELAY_SECONDS:-0}"
      if verify_control_plane_rollback_images; then
        log "rollback image preflight shadow passed"
      else
        log "rollback image preflight shadow failed; continuing without enforcement"
      fi
      return 0
      ;;
    enforced)
      if ! verify_control_plane_rollback_images; then
        log "rollback image preflight enforced failure"
        return 1
      fi
      log "rollback image preflight enforced passed"
      return 0
      ;;
    *)
      log "rollback image preflight mode must be off, disabled, shadow, or enforced: ${mode:-<empty>}"
      return 1
      ;;
  esac
}

live_deployment_container_env_value() {
  local deployment_name="$1"
  local container_name="$2"
  local env_name="$3"

  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get "deploy/${deployment_name}" -o json 2>/dev/null | python3 -c '
import json
import sys

container_name, env_name = sys.argv[1], sys.argv[2]
try:
    doc = json.load(sys.stdin)
except Exception:
    raise SystemExit(0)
for container in doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
    if container.get("name") != container_name:
        continue
    for item in container.get("env") or []:
        if item.get("name") == env_name and item.get("value") is not None:
            print(str(item.get("value")))
            raise SystemExit(0)
' "${container_name}" "${env_name}" 2>/dev/null || true
}

live_daemonset_container_image() {
  local daemonset_name="$1"
  local container_name="$2"

  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" \
    -o jsonpath="{.spec.template.spec.containers[?(@.name==\"${container_name}\")].image}" 2>/dev/null || true
}

live_bluegreen_front_pod_image() {
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get pods -l fugue.io/rollout-mode=node-local-blue-green-front -o json 2>/dev/null | python3 -c '
import json
import sys

try:
    doc = json.load(sys.stdin)
except Exception:
    raise SystemExit(0)
fallback = ""
for pod in sorted(doc.get("items", []), key=lambda item: item.get("metadata", {}).get("name", "")):
    image = ""
    for container in pod.get("spec", {}).get("containers", []):
        if container.get("name") == "edge-front":
            image = container.get("image", "")
            break
    if not image:
        continue
    if not fallback:
        fallback = image
    status = pod.get("status", {})
    if status.get("phase") != "Running":
        continue
    ready = False
    for condition in status.get("conditions") or []:
        if condition.get("type") == "Ready" and condition.get("status") == "True":
            ready = True
            break
    if ready:
        print(image)
        raise SystemExit(0)
if fallback:
    print(fallback)
'
}

duration_to_seconds() {
  local duration="${1:-600s}"
  local amount=""
  local unit=""

  if [[ "${duration}" =~ ^([0-9]+)$ ]]; then
    printf '%s' "${BASH_REMATCH[1]}"
    return
  fi
  if [[ "${duration}" =~ ^([0-9]+)(ms|s|m|h)$ ]]; then
    amount="${BASH_REMATCH[1]}"
    unit="${BASH_REMATCH[2]}"
    case "${unit}" in
      ms)
        printf '1'
        ;;
      s)
        printf '%s' "${amount}"
        ;;
      m)
        printf '%s' "$((amount * 60))"
        ;;
      h)
        printf '%s' "$((amount * 3600))"
        ;;
    esac
    return
  fi

  printf '600'
}

image_ref_without_digest() {
  local image_ref="$1"
  printf '%s' "${image_ref%%@*}"
}

image_ref_repository() {
  local image_ref no_digest last
  image_ref="$(trim_field "$1")"
  no_digest="$(image_ref_without_digest "${image_ref}")"
  last="${no_digest##*/}"
  if [[ "${last}" == *:* ]]; then
    printf '%s' "${no_digest%:*}"
  else
    printf '%s' "${no_digest}"
  fi
}

image_ref_tag() {
  local image_ref no_digest last
  image_ref="$(trim_field "$1")"
  no_digest="$(image_ref_without_digest "${image_ref}")"
  last="${no_digest##*/}"
  if [[ "${last}" == *:* ]]; then
    printf '%s' "${last##*:}"
  else
    printf 'latest'
  fi
}

release_image_digest_valid() {
  local digest
  digest="$(trim_field "$1")"
  [[ "${digest}" =~ ^sha256:[0-9a-f]{64}$ ]]
}

release_image_repository_valid() {
  local repository last
  repository="$(trim_field "$1")"
  [[ -n "${repository}" && "${repository}" =~ ^[^@\|[:space:]]+$ ]] || return 1
  last="${repository##*/}"
  [[ "${last}" != *:* ]]
}

release_image_tag_valid() {
  local tag
  tag="$(trim_field "$1")"
  [[ "${tag}" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]
}

select_release_image_record() {
  local component="$1"
  local source_mode="$2"
  local candidate_repository="$3"
  local candidate_tag="$4"
  local candidate_digest="$5"
  local live_repository="$6"
  local live_tag="$7"
  local live_digest="$8"
  local allow_legacy_unpinned="$9"
  local repository tag digest runtime_ref pin_state

  component="$(trim_field "${component}")"
  source_mode="$(trim_field "${source_mode}")"
  [[ "${component}" =~ ^[a-z0-9_]+$ ]] || {
    printf 'invalid release image component: %s\n' "${component:-<empty>}" >&2
    return 1
  }

  case "${source_mode}" in
    built|existing)
      repository="$(trim_field "${candidate_repository}")"
      tag="$(trim_field "${candidate_tag}")"
      digest="$(trim_field "${candidate_digest}")"
      ;;
    preserve)
      repository="$(trim_field "${live_repository}")"
      tag="$(trim_field "${live_tag}")"
      digest="$(trim_field "${live_digest}")"
      ;;
    *)
      printf 'invalid release image source mode for %s: %s\n' "${component}" "${source_mode:-<empty>}" >&2
      return 1
      ;;
  esac

  release_image_repository_valid "${repository}" || {
    printf 'invalid release image repository for %s/%s: %s\n' "${component}" "${source_mode}" "${repository:-<empty>}" >&2
    return 1
  }
  release_image_tag_valid "${tag}" || {
    printf 'invalid release image source tag for %s/%s: %s\n' "${component}" "${source_mode}" "${tag:-<empty>}" >&2
    return 1
  }

  if [[ -n "${digest}" ]]; then
    release_image_digest_valid "${digest}" || {
      printf 'invalid release image digest for %s/%s: %s\n' "${component}" "${source_mode}" "${digest}" >&2
      return 1
    }
    runtime_ref="${repository}@${digest}"
    pin_state="pinned"
  else
    case "${allow_legacy_unpinned}" in
      1|true|TRUE|yes|YES)
        [[ "${source_mode}" == "preserve" ]] || {
          printf 'legacy unpinned release image is only valid for preserve mode: %s/%s\n' "${component}" "${source_mode}" >&2
          return 1
        }
        ;;
      0|false|FALSE|no|NO|"")
        printf 'release image digest is required for %s/%s\n' "${component}" "${source_mode}" >&2
        return 1
        ;;
      *)
        printf 'invalid legacy unpinned release image policy for %s: %s\n' "${component}" "${allow_legacy_unpinned}" >&2
        return 1
        ;;
    esac
    runtime_ref="${repository}:${tag}"
    pin_state="legacy_unpinned"
  fi

  printf '%s|%s|%s|%s|%s|%s|%s\n' \
    "${component}" \
    "${source_mode}" \
    "${repository}" \
    "${tag}" \
    "${digest}" \
    "${runtime_ref}" \
    "${pin_state}"
}

preserve_image_from_live_daemonset() {
  local domain="$1"
  local daemonset_name="$2"
  local container_name="$3"
  local repository_var="$4"
  local tag_var="$5"
  local image_ref repository tag

  image_ref="$(live_daemonset_container_image "${daemonset_name}" "${container_name}")"
  image_ref="$(trim_field "${image_ref}")"
  if [[ -z "${image_ref}" ]]; then
    log "${domain} image preserve skipped; live daemonset ${daemonset_name}/${container_name} was not found"
    return 1
  fi

  repository="$(image_ref_repository "${image_ref}")"
  tag="$(image_ref_tag "${image_ref}")"
  if [[ -z "${repository}" || -z "${tag}" ]]; then
    log "${domain} image preserve skipped; could not parse live image ${image_ref}"
    return 1
  fi

  printf -v "${repository_var}" '%s' "${repository}"
  printf -v "${tag_var}" '%s' "${tag}"
  log "${domain} image preserved from live ${daemonset_name}/${container_name}: ${repository}:${tag}"
}

live_daemonset_container_resources_json() {
  local daemonset_name="$1"
  local container_name="$2"

  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o json 2>/dev/null | python3 -c '
import json
import sys

container_name = sys.argv[1]
try:
    doc = json.load(sys.stdin)
except Exception:
    raise SystemExit(0)
for container in doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
    if container.get("name") == container_name:
        resources = container.get("resources") or {}
        if resources:
            print(json.dumps(resources, separators=(",", ":")))
        raise SystemExit(0)
' "${container_name}" 2>/dev/null || true
}

live_daemonset_container_env_value() {
  local daemonset_name="$1"
  local container_name="$2"
  local env_name="$3"

  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o json 2>/dev/null | python3 -c '
import json
import sys

container_name, env_name = sys.argv[1], sys.argv[2]
try:
    doc = json.load(sys.stdin)
except Exception:
    raise SystemExit(0)
for container in doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
    if container.get("name") != container_name:
        continue
    for item in container.get("env") or []:
        if item.get("name") == env_name and item.get("value") is not None:
            print(str(item.get("value")))
            raise SystemExit(0)
' "${container_name}" "${env_name}" 2>/dev/null || true
}

append_live_daemonset_image_helm_args() {
  local domain="$1"
  local daemonset_name="$2"
  local container_name="$3"
  local values_prefix="$4"
  local image_ref repository tag

  image_ref="$(trim_field "$(live_daemonset_container_image "${daemonset_name}" "${container_name}")")"
  if [[ -z "${image_ref}" ]]; then
    log "${domain} image helm preserve skipped; live daemonset ${daemonset_name}/${container_name} was not found"
    return 1
  fi
  repository="$(image_ref_repository "${image_ref}")"
  tag="$(image_ref_tag "${image_ref}")"
  if [[ -z "${repository}" || -z "${tag}" ]]; then
    log "${domain} image helm preserve skipped; could not parse live image ${image_ref}"
    return 1
  fi
  PUBLIC_DATA_PLANE_HELM_SET_ARGS+=(
    --set-string "${values_prefix}.repository=${repository}"
    --set-string "${values_prefix}.tag=${tag}"
  )
  log "${domain} image helm args preserved from live ${daemonset_name}/${container_name}: ${repository}:${tag}"
}

append_image_ref_helm_args() {
  local domain="$1"
  local image_ref="$2"
  local values_prefix="$3"
  local repository tag

  image_ref="$(trim_field "${image_ref}")"
  if [[ -z "${image_ref}" ]]; then
    return 1
  fi
  repository="$(image_ref_repository "${image_ref}")"
  tag="$(image_ref_tag "${image_ref}")"
  if [[ -z "${repository}" || -z "${tag}" ]]; then
    log "${domain} image helm preserve skipped; could not parse live image ${image_ref}"
    return 1
  fi
  PUBLIC_DATA_PLANE_HELM_SET_ARGS+=(
    --set-string "${values_prefix}.repository=${repository}"
    --set-string "${values_prefix}.tag=${tag}"
  )
  log "${domain} image helm args preserved from live pod: ${repository}:${tag}"
}

live_helm_release_value() {
  local value_path="$1"

  helm get values "${FUGUE_RELEASE_NAME}" \
    -n "${FUGUE_NAMESPACE}" \
    -o json 2>/dev/null | python3 -c '
import json
import sys

value = json.load(sys.stdin)
for part in sys.argv[1].split("."):
    if not isinstance(value, dict) or part not in value:
        raise SystemExit(1)
    value = value[part]
if isinstance(value, bool):
    print("true" if value else "false")
elif isinstance(value, (str, int, float)):
    print(value)
else:
    raise SystemExit(1)
' "${value_path}"
}

preserve_edge_base_image_from_live_release() {
  local repository tag image_ref

  repository="$(trim_field "$(live_helm_release_value "edge.image.repository" || true)")"
  tag="$(trim_field "$(live_helm_release_value "edge.image.tag" || true)")"
  if [[ -z "${repository}" || -z "${tag}" ]]; then
    image_ref="$(trim_field "$(live_daemonset_container_image "${FUGUE_RELEASE_FULLNAME}-edge-worker-a" "edge")")"
    repository="$(image_ref_repository "${image_ref}")"
    tag="$(image_ref_tag "${image_ref}")"
  fi
  if [[ -z "${repository}" || -z "${tag}" ]]; then
    log "public data-plane base image preserve skipped; current Helm values and live worker-a image were unavailable"
    return 1
  fi

  FUGUE_EDGE_HELM_IMAGE_REPOSITORY="${repository}"
  FUGUE_EDGE_HELM_IMAGE_TAG="${tag}"
  log "public data-plane base image Helm values preserved from live release: ${repository}:${tag}"
}

append_dns_group_image_args_from_live() {
  local dns_prefix="${FUGUE_RELEASE_FULLNAME}-dns-"
  local daemonset_name
  local suffix
  local image_ref
  local repository
  local tag
  local index=0

  while IFS= read -r daemonset_name; do
    daemonset_name="$(trim_field "${daemonset_name}")"
    [[ -n "${daemonset_name}" ]] || continue
    suffix="${daemonset_name#${dns_prefix}}"
    if [[ "${suffix}" == "${daemonset_name}" || -z "${suffix}" ]]; then
      continue
    fi
    image_ref="$(trim_field "$(live_daemonset_container_image "${daemonset_name}" "dns")")"
    [[ -n "${image_ref}" ]] || continue
    repository="$(image_ref_repository "${image_ref}")"
    tag="$(image_ref_tag "${image_ref}")"
    [[ -n "${repository}" && -n "${tag}" ]] || continue
    PUBLIC_DATA_PLANE_HELM_SET_ARGS+=(
      --set-string "dns.groups[${index}].name=${suffix}"
      --set-string "dns.groups[${index}].image.repository=${repository}"
      --set-string "dns.groups[${index}].image.tag=${tag}"
    )
    log "public data-plane dns group ${suffix} image preserved from live ${daemonset_name}/dns: ${repository}:${tag}"
    index=$((index + 1))
  done < <(${KUBECTL} -n "${FUGUE_NAMESPACE}" get ds -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)
}

preserve_public_data_plane_from_live() {
  local edge_ds="${FUGUE_RELEASE_FULLNAME}-edge"
  local edge_front_ds="${FUGUE_RELEASE_FULLNAME}-edge-front"
  local dns_ds="${FUGUE_RELEASE_FULLNAME}-dns"
  local edge_resources caddy_resources probe_enabled probe_port probe_timeout front_image_ref
  local dns_resources

  PUBLIC_DATA_PLANE_HELM_SET_ARGS=()
  PUBLIC_DATA_PLANE_PRESERVED=true
  append_live_daemonset_image_helm_args \
    "public data-plane ssh front" \
    "${FUGUE_RELEASE_FULLNAME}-edge-ssh-front" \
    "ssh-front" \
    "edge.sshFront.image" || true

  if daemonset_exists "${edge_front_ds}" && daemonset_exists "${FUGUE_RELEASE_FULLNAME}-edge-worker-a" && daemonset_exists "${FUGUE_RELEASE_FULLNAME}-edge-worker-b"; then
    log "public data-plane blue/green DaemonSets detected; preserving front and per-slot worker templates from live state"
    preserve_edge_base_image_from_live_release || true
    PUBLIC_DATA_PLANE_HELM_SET_ARGS+=(
      --set edge.blueGreen.enabled=true
      --set edge.caddy.publicHostPorts.enabled=false
    )
    front_image_ref="$(live_bluegreen_front_pod_image)"
    append_image_ref_helm_args "public data-plane front" "${front_image_ref}" "edge.blueGreen.front.image" ||
      append_live_daemonset_image_helm_args "public data-plane front" "${edge_front_ds}" "edge-front" "edge.blueGreen.front.image" || true
    append_live_daemonset_image_helm_args "public data-plane worker-a" "${FUGUE_RELEASE_FULLNAME}-edge-worker-a" "edge" "edge.blueGreen.slots.a.image" || true
    append_live_daemonset_image_helm_args "public data-plane worker-b" "${FUGUE_RELEASE_FULLNAME}-edge-worker-b" "edge" "edge.blueGreen.slots.b.image" || true
    edge_resources="$(trim_field "$(live_daemonset_container_resources_json "${FUGUE_RELEASE_FULLNAME}-edge-worker-a" "edge")")"
    if [[ -n "${edge_resources}" ]]; then
      PUBLIC_DATA_PLANE_HELM_SET_ARGS+=(--set-json "edge.resources=${edge_resources}")
      log "public data-plane edge resources preserved from live ${FUGUE_RELEASE_FULLNAME}-edge-worker-a/edge"
    fi
    caddy_resources="$(trim_field "$(live_daemonset_container_resources_json "${FUGUE_RELEASE_FULLNAME}-edge-worker-a" "caddy")")"
    if [[ -n "${caddy_resources}" ]]; then
      PUBLIC_DATA_PLANE_HELM_SET_ARGS+=(--set-json "edge.caddy.resources=${caddy_resources}")
      log "public data-plane caddy resources preserved from live ${FUGUE_RELEASE_FULLNAME}-edge-worker-a/caddy"
    fi
    append_live_daemonset_image_helm_args "public data-plane dns" "${dns_ds}" "dns" "dns.image" || true
    append_dns_group_image_args_from_live
    dns_resources="$(trim_field "$(live_daemonset_container_resources_json "${dns_ds}" "dns")")"
    if [[ -n "${dns_resources}" ]]; then
      PUBLIC_DATA_PLANE_HELM_SET_ARGS+=(--set-json "dns.resources=${dns_resources}")
      log "public data-plane dns resources preserved from live ${dns_ds}/dns"
    fi
    probe_enabled="$(trim_field "$(live_daemonset_container_env_value "${dns_ds}" "dns" "FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED")")"
    probe_port="$(trim_field "$(live_daemonset_container_env_value "${dns_ds}" "dns" "FUGUE_DNS_EDGE_HEALTH_PROBE_PORT")")"
    probe_timeout="$(trim_field "$(live_daemonset_container_env_value "${dns_ds}" "dns" "FUGUE_DNS_EDGE_HEALTH_PROBE_TIMEOUT")")"
    if [[ -n "${probe_enabled}" ]]; then
      PUBLIC_DATA_PLANE_HELM_SET_ARGS+=(--set "dns.edgeHealthProbe.enabled=${probe_enabled}")
      log "public data-plane dns edge health probe enabled flag preserved from live ${dns_ds}/dns: ${probe_enabled}"
    fi
    if [[ -n "${probe_port}" ]]; then
      PUBLIC_DATA_PLANE_HELM_SET_ARGS+=(--set "dns.edgeHealthProbe.port=${probe_port}")
    fi
    if [[ -n "${probe_timeout}" ]]; then
      PUBLIC_DATA_PLANE_HELM_SET_ARGS+=(--set-string "dns.edgeHealthProbe.timeout=${probe_timeout}")
    fi
    return 0
  fi

  preserve_image_from_live_daemonset "public data-plane" "${edge_ds}" "edge" FUGUE_EDGE_HELM_IMAGE_REPOSITORY FUGUE_EDGE_HELM_IMAGE_TAG || true
  append_live_daemonset_image_helm_args "public data-plane" "${edge_ds}" "caddy" "edge.caddy.image" || true
  append_live_daemonset_image_helm_args "public data-plane dns" "${dns_ds}" "dns" "dns.image" || true
  append_dns_group_image_args_from_live

  edge_resources="$(trim_field "$(live_daemonset_container_resources_json "${edge_ds}" "edge")")"
  if [[ -n "${edge_resources}" ]]; then
    PUBLIC_DATA_PLANE_HELM_SET_ARGS+=(--set-json "edge.resources=${edge_resources}")
    log "public data-plane edge resources preserved from live ${edge_ds}/edge"
  fi
  caddy_resources="$(trim_field "$(live_daemonset_container_resources_json "${edge_ds}" "caddy")")"
  if [[ -n "${caddy_resources}" ]]; then
    PUBLIC_DATA_PLANE_HELM_SET_ARGS+=(--set-json "edge.caddy.resources=${caddy_resources}")
    log "public data-plane caddy resources preserved from live ${edge_ds}/caddy"
  fi
  dns_resources="$(trim_field "$(live_daemonset_container_resources_json "${dns_ds}" "dns")")"
  if [[ -n "${dns_resources}" ]]; then
    PUBLIC_DATA_PLANE_HELM_SET_ARGS+=(--set-json "dns.resources=${dns_resources}")
    log "public data-plane dns resources preserved from live ${dns_ds}/dns"
  fi

  probe_enabled="$(trim_field "$(live_daemonset_container_env_value "${dns_ds}" "dns" "FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED")")"
  probe_port="$(trim_field "$(live_daemonset_container_env_value "${dns_ds}" "dns" "FUGUE_DNS_EDGE_HEALTH_PROBE_PORT")")"
  probe_timeout="$(trim_field "$(live_daemonset_container_env_value "${dns_ds}" "dns" "FUGUE_DNS_EDGE_HEALTH_PROBE_TIMEOUT")")"
  if [[ -n "${probe_enabled}" ]]; then
    PUBLIC_DATA_PLANE_HELM_SET_ARGS+=(--set "dns.edgeHealthProbe.enabled=${probe_enabled}")
    log "public data-plane dns edge health probe enabled flag preserved from live ${dns_ds}/dns: ${probe_enabled}"
  fi
  if [[ -n "${probe_port}" ]]; then
    PUBLIC_DATA_PLANE_HELM_SET_ARGS+=(--set "dns.edgeHealthProbe.port=${probe_port}")
  fi
  if [[ -n "${probe_timeout}" ]]; then
    PUBLIC_DATA_PLANE_HELM_SET_ARGS+=(--set-string "dns.edgeHealthProbe.timeout=${probe_timeout}")
  fi
}

preserve_node_local_build_plane_from_live() {
  local preserve_image="${1:-true}"
  local preserve_resources="${2:-true}"
  local strict="${3:-false}"
  local image_cache_ds="${FUGUE_RELEASE_FULLNAME}-image-cache"
  local image_cache_resources

  NODE_LOCAL_BUILD_PLANE_HELM_SET_ARGS=()

  if [[ "${preserve_image}" == "true" ]]; then
    if ! preserve_image_from_live_daemonset "node-local build-plane" "${image_cache_ds}" "image-cache" FUGUE_IMAGE_CACHE_IMAGE_REPOSITORY FUGUE_IMAGE_CACHE_IMAGE_TAG; then
      [[ "${strict}" == "true" ]] && return 1
    fi
  fi

  if [[ "${preserve_resources}" == "true" ]]; then
    image_cache_resources="$(trim_field "$(live_daemonset_container_resources_json "${image_cache_ds}" "image-cache")")"
    if [[ -n "${image_cache_resources}" ]]; then
      NODE_LOCAL_BUILD_PLANE_HELM_SET_ARGS+=(--set-json "imageCache.resources=${image_cache_resources}")
      log "node-local build-plane image-cache resources preserved from live ${image_cache_ds}/image-cache"
    elif [[ "${strict}" == "true" ]]; then
      log "node-local build-plane strict preserve failed; live ${image_cache_ds}/image-cache resources are empty or unreadable"
      return 1
    fi
  fi
}

preserve_strict_drain_agent_image_from_live() {
  local mode
  local controller_deployment
  local repository tag digest pull_policy

  STRICT_DRAIN_AGENT_IMAGE_PRESERVED=false
  mode="$(trim_field "${FUGUE_STRICT_DRAIN_MODE:-}")"
  if [[ "${mode}" != "connection-aware" ]]; then
    return 0
  fi
  if strict_drain_agent_image_changed; then
    log "strict drain-agent image source changed; allowing drain-agent image rollout to ${FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY}:${FUGUE_DRAIN_AGENT_IMAGE_TAG}"
    return 0
  fi

  controller_deployment="${FUGUE_CONTROLLER_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}-controller}"
  if ! deployment_exists "${controller_deployment}"; then
    log "strict drain-agent image preserve skipped; live controller deployment ${controller_deployment} was not found"
    return 0
  fi

  repository="$(trim_field "$(live_deployment_container_env_value "${controller_deployment}" "controller" "FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY")")"
  tag="$(trim_field "$(live_deployment_container_env_value "${controller_deployment}" "controller" "FUGUE_DRAIN_AGENT_IMAGE_TAG")")"
  digest="$(trim_field "$(live_deployment_container_env_value "${controller_deployment}" "controller" "FUGUE_DRAIN_AGENT_IMAGE_DIGEST")")"
  pull_policy="$(trim_field "$(live_deployment_container_env_value "${controller_deployment}" "controller" "FUGUE_DRAIN_AGENT_IMAGE_PULL_POLICY")")"
  if [[ -z "${repository}" || ( -z "${tag}" && -z "${digest}" ) ]]; then
    log "strict drain-agent image preserve skipped; live controller env does not contain a usable drain-agent image"
    return 0
  fi

  FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY="${repository}"
  FUGUE_DRAIN_AGENT_IMAGE_TAG="${tag}"
  FUGUE_DRAIN_AGENT_IMAGE_DIGEST="${digest}"
  if [[ -n "${pull_policy}" ]]; then
    FUGUE_DRAIN_AGENT_IMAGE_PULL_POLICY="${pull_policy}"
  fi
  STRICT_DRAIN_AGENT_IMAGE_PRESERVED=true
  if [[ -n "${digest}" ]]; then
    log "strict drain-agent image preserved from live controller ${controller_deployment}: ${repository}@${digest}"
  else
    log "strict drain-agent image preserved from live controller ${controller_deployment}: ${repository}:${tag}"
  fi
}

append_maintenance_daemonset_image_helm_args() {
  local domain="$1"
  local daemonset_name="$2"
  local container_name="$3"
  local values_prefix="$4"
  local image_ref repository tag

  image_ref="$(trim_field "$(live_daemonset_container_image "${daemonset_name}" "${container_name}")")"
  if [[ -z "${image_ref}" ]]; then
    log "${domain} image helm preserve skipped; live daemonset ${daemonset_name}/${container_name} was not found"
    return 1
  fi
  repository="$(image_ref_repository "${image_ref}")"
  tag="$(image_ref_tag "${image_ref}")"
  if [[ -z "${repository}" || -z "${tag}" ]]; then
    log "${domain} image helm preserve skipped; could not parse live image ${image_ref}"
    return 1
  fi
  MAINTENANCE_AGENT_HELM_SET_ARGS+=(
    --set-string "${values_prefix}.repository=${repository}"
    --set-string "${values_prefix}.tag=${tag}"
  )
  log "${domain} image helm args preserved from live ${daemonset_name}/${container_name}: ${repository}:${tag}"
}

append_maintenance_daemonset_resources_helm_args() {
  local domain="$1"
  local daemonset_name="$2"
  local container_name="$3"
  local values_path="$4"
  local resources

  resources="$(trim_field "$(live_daemonset_container_resources_json "${daemonset_name}" "${container_name}")")"
  if [[ -z "${resources}" ]]; then
    return 0
  fi
  MAINTENANCE_AGENT_HELM_SET_ARGS+=(--set-json "${values_path}=${resources}")
  log "${domain} resources preserved from live ${daemonset_name}/${container_name}"
}

preserve_maintenance_agents_from_live() {
  local node_janitor_ds="${FUGUE_RELEASE_FULLNAME}-node-janitor"
  local topology_labeler_ds="${FUGUE_RELEASE_FULLNAME}-topology-labeler"
  local image_prepull_ds="${FUGUE_RELEASE_FULLNAME}-image-prepull"

  MAINTENANCE_AGENT_HELM_SET_ARGS=()

  append_maintenance_daemonset_image_helm_args "maintenance agents" "${node_janitor_ds}" "node-janitor" "nodeJanitor.image" || true
  append_maintenance_daemonset_resources_helm_args "maintenance agents" "${node_janitor_ds}" "node-janitor" "nodeJanitor.resources"

  append_maintenance_daemonset_image_helm_args "maintenance agents" "${topology_labeler_ds}" "topology-labeler" "topologyLabeler.image" || true
  append_maintenance_daemonset_resources_helm_args "maintenance agents" "${topology_labeler_ds}" "topology-labeler" "topologyLabeler.resources"

  append_maintenance_daemonset_image_helm_args "maintenance agents" "${image_prepull_ds}" "image-prepull" "imagePrePull.image" || true
  append_maintenance_daemonset_resources_helm_args "maintenance agents" "${image_prepull_ds}" "image-prepull" "imagePrePull.resources"
}

rollout_daemonset_status() {
  local daemonset_name="$1"
  local namespace="${2:-${FUGUE_NAMESPACE}}"
  local strategy
  strategy="$(${KUBECTL} -n "${namespace}" get "ds/${daemonset_name}" -o jsonpath='{.spec.updateStrategy.type}' 2>/dev/null || true)"
  strategy="${strategy:-RollingUpdate}"

  if [[ "${strategy}" == "RollingUpdate" ]]; then
    ${KUBECTL} -n "${namespace}" rollout status "ds/${daemonset_name}" --timeout="${FUGUE_ROLLOUT_TIMEOUT}"
    return
  fi

  if [[ "${strategy}" != "OnDelete" ]]; then
    log "daemonset ${daemonset_name} uses unknown update strategy ${strategy}; checking observedGeneration and ready pods"
  else
    log "daemonset ${daemonset_name} uses OnDelete; checking observedGeneration and ready pods"
  fi

  local timeout_seconds
  local deadline
  local status
  local generation
  local observed_generation
  local desired
  local ready
  local unavailable
  timeout_seconds="$(duration_to_seconds "${FUGUE_ROLLOUT_TIMEOUT}")"
  deadline=$((SECONDS + timeout_seconds))

  while true; do
    status="$(${KUBECTL} -n "${namespace}" get "ds/${daemonset_name}" -o jsonpath='{.metadata.generation}{"\t"}{.status.observedGeneration}{"\t"}{.status.desiredNumberScheduled}{"\t"}{.status.numberReady}{"\t"}{.status.numberUnavailable}' 2>/dev/null || true)"
    IFS=$'\t' read -r generation observed_generation desired ready unavailable <<<"${status}"
    generation="${generation:-0}"
    observed_generation="${observed_generation:-0}"
    desired="${desired:-0}"
    ready="${ready:-0}"
    unavailable="${unavailable:-0}"

    if [[ "${generation}" == "${observed_generation}" && "${desired}" == "${ready}" && "${unavailable}" == "0" ]]; then
      log "daemonset ${daemonset_name} ready: generation=${generation} desired=${desired} ready=${ready}"
      return 0
    fi

    if ((SECONDS >= deadline)); then
      log "timed out waiting for daemonset ${daemonset_name}: generation=${generation} observed=${observed_generation} desired=${desired} ready=${ready} unavailable=${unavailable}"
      return 1
    fi

    sleep 2
  done
}

image_cache_current_controller_revision() {
  local daemonset_name="$1"
  local daemonset_json=""
  local controller_revisions_json=""

  daemonset_json="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get daemonset "${daemonset_name}" -o json)" || return 1
  controller_revisions_json="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get controllerrevisions.apps \
    -l "app.kubernetes.io/name=fugue,app.kubernetes.io/instance=${FUGUE_RELEASE_NAME},app.kubernetes.io/component=image-cache" -o json)" || return 1
  {
    printf '%s\n' "${daemonset_json}"
    printf '%s\n' "${controller_revisions_json}"
  } | python3 -c '
import copy
import json
import sys

decoder = json.JSONDecoder()
raw = sys.stdin.read()
documents = []
offset = 0
while offset < len(raw):
    while offset < len(raw) and raw[offset].isspace():
        offset += 1
    if offset >= len(raw):
        break
    document, offset = decoder.raw_decode(raw, offset)
    documents.append(document)
if len(documents) != 2:
    raise SystemExit(1)

daemonset, revision_list = documents
metadata = daemonset.get("metadata") or {}
status = daemonset.get("status") or {}
template = ((daemonset.get("spec") or {}).get("template") or {})
daemonset_name = str(metadata.get("name") or "")
daemonset_uid = str(metadata.get("uid") or "")
daemonset_namespace = str(metadata.get("namespace") or "")
if (
    not daemonset_name
    or not daemonset_uid
    or not daemonset_namespace
    or not isinstance(template, dict)
    or not template
    or str(metadata.get("generation") or "") != str(status.get("observedGeneration") or "")
    or not isinstance(revision_list, dict)
    or not isinstance(revision_list.get("items"), list)
):
    raise SystemExit(1)

matches = []
for revision in revision_list["items"]:
    if not isinstance(revision, dict):
        raise SystemExit(1)
    revision_metadata = revision.get("metadata") or {}
    controllers = [
        owner
        for owner in revision_metadata.get("ownerReferences") or []
        if isinstance(owner, dict) and owner.get("controller") is True
    ]
    if (
        str(revision.get("apiVersion") or "") != "apps/v1"
        or str(revision.get("kind") or "") != "ControllerRevision"
        or str(revision_metadata.get("namespace") or "") != daemonset_namespace
        or len(controllers) != 1
    ):
        continue
    controller = controllers[0]
    if (
        str(controller.get("apiVersion") or "") != "apps/v1"
        or str(controller.get("kind") or "") != "DaemonSet"
        or str(controller.get("name") or "") != daemonset_name
        or str(controller.get("uid") or "") != daemonset_uid
    ):
        continue
    revision_template = copy.deepcopy((((revision.get("data") or {}).get("spec") or {}).get("template")))
    if not isinstance(revision_template, dict) or revision_template.pop("$patch", None) != "replace":
        continue
    if revision_template == template:
        matches.append(revision)

if len(matches) != 1:
    raise SystemExit(1)
revision_metadata = matches[0].get("metadata") or {}
revision_name = str(revision_metadata.get("name") or "")
revision_hash = str((revision_metadata.get("labels") or {}).get("controller-revision-hash") or "")
if not revision_hash or revision_name != daemonset_name + "-" + revision_hash:
    raise SystemExit(1)
print(revision_hash)
'
}

image_cache_rollout_plan_json() (
  local daemonset_name="$1"
  local target_revision="$2"
  local inventory_dir=""

  umask 077
  inventory_dir="$(mktemp -d)" || return 1
  trap 'rm -rf -- "${inventory_dir}"' EXIT
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get daemonset "${daemonset_name}" \
    -o json >"${inventory_dir}/daemonset.json" || return 1
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get controllerrevisions.apps \
    -l "app.kubernetes.io/name=fugue,app.kubernetes.io/instance=${FUGUE_RELEASE_NAME},app.kubernetes.io/component=image-cache" \
    -o json >"${inventory_dir}/controller-revisions.json" || return 1
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get pods \
    -l "app.kubernetes.io/name=fugue,app.kubernetes.io/instance=${FUGUE_RELEASE_NAME},app.kubernetes.io/component=image-cache" \
    -o json >"${inventory_dir}/pods.json" || return 1
  ${KUBECTL} get nodes -o json >"${inventory_dir}/nodes.json" || return 1
  printf '%s\n' "${NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES}" >"${inventory_dir}/preserved-nodes.txt" || return 1
  python3 - \
    "${inventory_dir}/daemonset.json" \
    "${inventory_dir}/controller-revisions.json" \
    "${inventory_dir}/pods.json" \
    "${inventory_dir}/nodes.json" \
    "${inventory_dir}/preserved-nodes.txt" \
    "${daemonset_name}" \
    "${target_revision}" <<'PY'
import json
import sys
from pathlib import Path

def reject(reason):
    raise SystemExit("image-cache offline rollout rejected: " + reason)

try:
    daemonset = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    revision_list = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
    pod_list = json.loads(Path(sys.argv[3]).read_text(encoding="utf-8"))
    node_list = json.loads(Path(sys.argv[4]).read_text(encoding="utf-8"))
    preserved_raw = Path(sys.argv[5]).read_text(encoding="utf-8")
except (OSError, UnicodeError, json.JSONDecodeError):
    reject("cluster inventory JSON is invalid")

expected_daemonset = sys.argv[6]
preserved = [item.strip() for item in preserved_raw.splitlines() if item.strip()]
if not preserved or len(preserved) != len(set(preserved)):
    reject("preserved node cohort is empty or duplicated")
preserved_set = set(preserved)
target_revision = sys.argv[7].strip()
if not target_revision:
    reject("target ControllerRevision is empty")

metadata = daemonset.get("metadata") or {}
spec = daemonset.get("spec") or {}
status = daemonset.get("status") or {}
template = spec.get("template") or {}
template_spec = template.get("spec") or {}
annotations = metadata.get("annotations")
if (
    str(daemonset.get("apiVersion") or "") != "apps/v1"
    or str(daemonset.get("kind") or "") != "DaemonSet"
    or str(metadata.get("name") or "") != expected_daemonset
    or not str(metadata.get("uid") or "")
    or not str(metadata.get("resourceVersion") or "")
    or str(metadata.get("generation") or "") != str(status.get("observedGeneration") or "")
    or not isinstance(annotations, dict)
):
    reject("DaemonSet identity or observed generation is invalid")
if template_spec.get("nodeSelector") not in (None, {}) or template_spec.get("affinity") not in (None, {}):
    reject("DaemonSet has an unsupported bounded node membership")
containers = template_spec.get("containers") or []
if len(containers) != 1 or containers[0].get("name") != "image-cache" or not containers[0].get("image"):
    reject("DaemonSet image-cache container is invalid")
ports = containers[0].get("ports") or []
registry_ports = [
    item for item in ports
    if isinstance(item, dict) and item.get("name") == "registry" and item.get("protocol", "TCP") == "TCP"
]
if len(registry_ports) != 1 or registry_ports[0].get("containerPort") != registry_ports[0].get("hostPort"):
    reject("DaemonSet registry hostPort is invalid")
registry_port = registry_ports[0].get("hostPort")
if type(registry_port) is not int or registry_port <= 0 or registry_port > 65535:
    reject("DaemonSet registry port is invalid")

if not isinstance(revision_list, dict) or not isinstance(revision_list.get("items"), list):
    reject("ControllerRevision inventory is invalid")
daemonset_uid = str(metadata.get("uid") or "")
known_revisions = set()
for revision in revision_list["items"]:
    if not isinstance(revision, dict):
        reject("ControllerRevision inventory contains a non-object")
    revision_metadata = revision.get("metadata") or {}
    owners = [
        owner for owner in revision_metadata.get("ownerReferences") or []
        if isinstance(owner, dict) and owner.get("controller") is True
    ]
    if len(owners) != 1:
        continue
    owner = owners[0]
    if (
        revision.get("apiVersion") != "apps/v1"
        or revision.get("kind") != "ControllerRevision"
        or owner.get("apiVersion") != "apps/v1"
        or owner.get("kind") != "DaemonSet"
        or owner.get("name") != expected_daemonset
        or str(owner.get("uid") or "") != daemonset_uid
    ):
        continue
    revision_hash = str((revision_metadata.get("labels") or {}).get("controller-revision-hash") or "")
    if not revision_hash or str(revision_metadata.get("name") or "") != expected_daemonset + "-" + revision_hash:
        reject("owned ControllerRevision hash identity is invalid")
    known_revisions.add(revision_hash)
if target_revision not in known_revisions:
    reject("target revision is not owned by the current DaemonSet")

strategy = spec.get("updateStrategy") or {}
strategy_type = str(strategy.get("type") or "")
rolling = strategy.get("rollingUpdate") or {}
rolling_exact = strategy_type == "RollingUpdate" and rolling.get("maxUnavailable") == 1 and rolling.get("maxSurge") == 0
ondelete = strategy_type == "OnDelete" and set(strategy) == {"type"}

legacy_transaction_annotations = {
    "fugue.io/image-cache-rollout-transaction",
    "fugue.io/image-cache-rollout-target-revision",
    "fugue.io/image-cache-rollout-node",
    "fugue.io/image-cache-rollout-old-pod-uid",
    "fugue.io/image-cache-rollout-original-strategy",
}
if legacy_transaction_annotations.intersection(annotations):
    reject("legacy rollout transaction annotations require operator review")
if not (rolling_exact or ondelete):
    reject("DaemonSet must start from clean OnDelete or RollingUpdate maxUnavailable=1 maxSurge=0")

nodes = {}
for node in node_list.get("items") or []:
    name = str((node.get("metadata") or {}).get("name") or "").strip()
    if not name or name in nodes:
        reject("node inventory contains an empty or duplicate name")
    nodes[name] = node
if not nodes or not preserved_set.issubset(nodes):
    reject("preserved nodes are absent from the cluster inventory")

pods = []
for pod in pod_list.get("items") or []:
    pod_metadata = pod.get("metadata") or {}
    controllers = [
        owner for owner in pod_metadata.get("ownerReferences") or []
        if isinstance(owner, dict) and owner.get("controller") is True
    ]
    if len(controllers) != 1:
        continue
    owner = controllers[0]
    if (
        owner.get("apiVersion") != "apps/v1"
        or owner.get("kind") != "DaemonSet"
        or owner.get("name") != expected_daemonset
        or str(owner.get("uid") or "") != daemonset_uid
    ):
        continue
    pod_spec = pod.get("spec") or {}
    pod_status = pod.get("status") or {}
    pod_name = str(pod_metadata.get("name") or "")
    node_name = str(pod_spec.get("nodeName") or "")
    uid = str(pod_metadata.get("uid") or "")
    if not pod_name or not node_name or not uid or node_name not in nodes:
        reject("owned Pod identity is invalid")
    pod_containers = pod_spec.get("containers") or []
    if len(pod_containers) != 1 or pod_containers[0].get("name") != "image-cache" or not pod_containers[0].get("image"):
        reject("owned Pod container spec is invalid")
    container_statuses = pod_status.get("containerStatuses") or []
    container_status_valid = len(container_statuses) == 1 and container_statuses[0].get("name") == "image-cache"
    restart_count = None
    if container_status_valid:
        restart_count = container_statuses[0].get("restartCount", 0)
        if type(restart_count) is not int or restart_count < 0:
            reject("owned Pod restart count is invalid")
    ready = any(item.get("type") == "Ready" and item.get("status") == "True" for item in pod_status.get("conditions") or [])
    pod_revision = str((pod_metadata.get("labels") or {}).get("controller-revision-hash") or "")
    if pod_revision not in known_revisions:
        reject("owned Pod revision is empty or unknown: " + pod_name)
    pods.append({
        "name": pod_name,
        "uid": uid,
        "node": node_name,
        "revision": pod_revision,
        "image": str(pod_containers[0].get("image") or ""),
        "phase": str(pod_status.get("phase") or ""),
        "ready": ready,
        "deleting": bool(pod_metadata.get("deletionTimestamp")),
        "container_status_valid": container_status_valid,
        "restart_count": restart_count,
        "pod_ip": str(pod_status.get("podIP") or ""),
    })

by_node = {}
for pod in pods:
    by_node.setdefault(pod["node"], []).append(pod)
if set(by_node) != set(nodes):
    reject("DaemonSet Pod nodes do not exactly match the cluster nodes")
if any(len(items) != 1 for items in by_node.values()):
    reject("DaemonSet does not have exactly one owned Pod per node")

active = sorted(set(nodes) - preserved_set)
if not active:
    reject("active node cohort is empty")
active_pods = []
old_active_nodes = []
updated_total = 0
for name, node in nodes.items():
    node_metadata = node.get("metadata") or {}
    labels = node_metadata.get("labels") or {}
    node_spec = node.get("spec") or {}
    node_status = node.get("status") or {}
    conditions = {str(item.get("type") or ""): str(item.get("status") or "") for item in node_status.get("conditions") or []}
    taints = node_spec.get("taints") or []
    pod = by_node[name][0]
    if pod["revision"] == target_revision:
        updated_total += 1
    if name in preserved_set:
        isolated_taint = any(
            (item.get("key") == "fugue.io/node-unhealthy" and item.get("effect") == "NoSchedule")
            or (item.get("key") == "node.kubernetes.io/unreachable" and item.get("effect") in {"NoSchedule", "NoExecute"})
            for item in taints if isinstance(item, dict)
        )
        role_values = [
            labels.get("fugue.io/role.edge"), labels.get("fugue.io/role.dns"),
            labels.get("fugue.io/role.app-runtime"), labels.get("fugue.io/role.builder"),
            labels.get("fugue.io/role.internal-maintenance"),
        ]
        if (
            conditions.get("Ready") == "True"
            or labels.get("fugue.io/schedulable") != "false"
            or labels.get("fugue.io/node-health") != "blocked"
            or any(str(value or "").lower() == "true" for value in role_values)
            or not isolated_taint
            or pod["ready"]
        ):
            reject("preserved node is not exactly isolated: " + name)
        continue
    if (
        conditions.get("Ready") != "True"
        or conditions.get("DiskPressure") != "False"
        or conditions.get("MemoryPressure") != "False"
        or conditions.get("PIDPressure") != "False"
        or pod["phase"] != "Running"
        or not pod["ready"]
        or pod["deleting"]
        or not pod["pod_ip"]
        or not pod["container_status_valid"]
    ):
        reject("active node or image-cache Pod is not healthy: " + name)
    if pod["image"] != str(containers[0].get("image") or "") and pod["revision"] == target_revision:
        reject("target revision Pod image differs from the DaemonSet template: " + name)
    if pod["revision"] != target_revision:
        old_active_nodes.append(name)
    active_pods.append(pod)

def integer(field, default=0):
    value = status.get(field, default)
    if type(value) is not int:
        reject("DaemonSet status counter is not an integer: " + field)
    return value

if (
    integer("desiredNumberScheduled") != len(nodes)
    or integer("currentNumberScheduled") != len(nodes)
    or integer("numberReady") != len(active)
    or integer("numberAvailable") != len(active)
    or integer("numberUnavailable") != len(preserved)
    or integer("numberMisscheduled") != 0
    or integer("updatedNumberScheduled") != updated_total
):
    reject("DaemonSet status does not match exact active/preserved Pod state")

print(json.dumps({
    "daemonset_uid": daemonset_uid,
    "generation": str(metadata.get("generation") or ""),
    "resource_version": str(metadata.get("resourceVersion") or ""),
    "strategy": strategy_type,
    "transaction": False,
    "target_revision": target_revision,
    "target_image": str(containers[0].get("image") or ""),
    "registry_port": registry_port,
    "active_nodes": active,
    "preserved_nodes": sorted(preserved_set),
    "old_active_nodes": sorted(old_active_nodes),
    "updated_active_count": sum(1 for pod in active_pods if pod["revision"] == target_revision),
    "active_pods": sorted(active_pods, key=lambda item: item["node"]),
    "pods": sorted(pods, key=lambda item: item["node"]),
}, sort_keys=True, separators=(",", ":")))
PY
)

image_cache_plan_field() {
  local plan_json="$1"
  local field="$2"
  PLAN_JSON="${plan_json}" PLAN_FIELD="${field}" python3 -c '
import json
import os
value = json.loads(os.environ["PLAN_JSON"])[os.environ["PLAN_FIELD"]]
if isinstance(value, bool):
    print("true" if value else "false")
elif isinstance(value, list):
    for item in value:
        print(item)
else:
    print(value)
'
}

image_cache_bind_plan_to_target() {
  local daemonset_name="$1"
  local plan_json="$2"
  local target_revision="$3"
  local plan_resource_version=""
  local current_revision=""
  local current_resource_version=""

  plan_resource_version="$(image_cache_plan_field "${plan_json}" resource_version)" || return 1
  [[ -n "${plan_resource_version}" ]] || return 1
  current_revision="$(image_cache_current_controller_revision "${daemonset_name}")" || return 1
  current_resource_version="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get daemonset "${daemonset_name}" -o jsonpath='{.metadata.resourceVersion}')" || return 1
  if [[ "${current_revision}" != "${target_revision}" || "${current_resource_version}" != "${plan_resource_version}" ]]; then
    log "image-cache DaemonSet changed while binding the rollout plan: expected_revision=${target_revision} current_revision=${current_revision:-unknown} plan_resource_version=${plan_resource_version} current_resource_version=${current_resource_version:-unknown}"
    return 1
  fi
}

image_cache_validate_unchanged_pod_identities() {
  local before_plan_json="$1"
  local after_plan_json="$2"

  BEFORE_PLAN_JSON="${before_plan_json}" AFTER_PLAN_JSON="${after_plan_json}" python3 -c '
import json
import os
before = {item["node"]: item for item in json.loads(os.environ["BEFORE_PLAN_JSON"])["pods"]}
after = {item["node"]: item for item in json.loads(os.environ["AFTER_PLAN_JSON"])["pods"]}
if set(before) != set(after):
    raise SystemExit(1)
for node in before:
    if before[node]["uid"] != after[node]["uid"] or before[node].get("restart_count") != after[node].get("restart_count"):
        raise SystemExit(1)
'
}

image_cache_probe_endpoint() {
  local pod_ip="$1"
  local port="$2"
  local attempt=1
  local response=""
  local status=""

  command_exists curl || return 1
  while (( attempt <= 3 )); do
    response="$(curl --noproxy '*' -sS --connect-timeout 5 --max-time 10 -D - -o /dev/null \
      -w $'\n__FUGUE_STATUS__:%{http_code}\n' "http://${pod_ip}:${port}/v2/" 2>/dev/null || true)"
    status="$(sed -n 's/^__FUGUE_STATUS__://p' <<<"${response}" | tail -n 1 | tr -d '\r')"
    if [[ "${status}" == "200" ]] && tr -d '\r' <<<"${response}" | grep -Eiq '^Docker-Distribution-API-Version:[[:space:]]*registry/2\.0[[:space:]]*$'; then
      return 0
    fi
    sleep 2
    attempt=$((attempt + 1))
  done
  return 1
}

image_cache_probe_active_plan() {
  local plan_json="$1"
  local port=""
  local node_name=""
  local pod_ip=""
  local rows=""
  local expected_count="0"
  local actual_count="0"

  port="$(image_cache_plan_field "${plan_json}" registry_port)" || return 1
  rows="$(PLAN_JSON="${plan_json}" python3 -c '
import json
import os
plan = json.loads(os.environ["PLAN_JSON"])
for pod in plan["active_pods"]:
    print(str(pod["node"]) + "\t" + str(pod["pod_ip"]))
')" || return 1
  expected_count="$(PLAN_JSON="${plan_json}" python3 -c 'import json,os; print(len(json.loads(os.environ["PLAN_JSON"])["active_nodes"]))')" || return 1
  actual_count="$(awk 'NF {count++} END {print count+0}' <<<"${rows}")"
  if ! [[ "${expected_count}" =~ ^[1-9][0-9]*$ ]] || [[ "${actual_count}" != "${expected_count}" ]]; then
    log "image-cache active endpoint inventory is incomplete: expected=${expected_count:-unknown} actual=${actual_count:-unknown}"
    return 1
  fi
  while IFS=$'\t' read -r node_name pod_ip; do
    [[ -n "${node_name}" && -n "${pod_ip}" ]] || return 1
    if ! image_cache_probe_endpoint "${pod_ip}" "${port}"; then
      log "image-cache HTTP /v2/ probe failed on active node ${node_name} podIP=${pod_ip}"
      return 1
    fi
  done <<<"${rows}"
}

image_cache_wait_strategy_observed() {
  local daemonset_name="$1"
  local expected_strategy="$2"
  local deadline=$((SECONDS + $(duration_to_seconds "${FUGUE_ROLLOUT_TIMEOUT}")))
  local state=""
  local generation=""
  local observed=""
  local strategy=""
  local max_unavailable=""
  local max_surge=""

  while (( SECONDS < deadline )); do
    state="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get daemonset "${daemonset_name}" \
      -o jsonpath='{.metadata.generation}{"\t"}{.status.observedGeneration}{"\t"}{.spec.updateStrategy.type}{"\t"}{.spec.updateStrategy.rollingUpdate.maxUnavailable}{"\t"}{.spec.updateStrategy.rollingUpdate.maxSurge}' 2>/dev/null || true)"
    IFS=$'\t' read -r generation observed strategy max_unavailable max_surge <<<"${state}"
    if [[ -n "${generation}" && "${generation}" == "${observed}" && "${strategy}" == "${expected_strategy}" ]]; then
      if [[ "${expected_strategy}" == "OnDelete" || ( "${max_unavailable}" == "1" && "${max_surge}" == "0" ) ]]; then
        return 0
      fi
    fi
    sleep 2
  done
  log "timed out waiting for image-cache strategy observation: expected=${expected_strategy} generation=${generation:-unknown} observed=${observed:-unknown} strategy=${strategy:-unknown}"
  return 1
}

image_cache_patch_clean_ondelete() {
  local daemonset_name="$1"
  local resource_version="$2"
  local patch=""

  [[ -n "${resource_version}" ]] || return 1
  patch="$(RESOURCE_VERSION="${resource_version}" python3 -c '
import json
import os
print(json.dumps([
    {"op": "test", "path": "/metadata/resourceVersion", "value": os.environ["RESOURCE_VERSION"]},
    {"op": "test", "path": "/spec/updateStrategy/type", "value": "RollingUpdate"},
    {"op": "test", "path": "/spec/updateStrategy/rollingUpdate/maxUnavailable", "value": 1},
    {"op": "test", "path": "/spec/updateStrategy/rollingUpdate/maxSurge", "value": 0},
    {"op": "replace", "path": "/spec/updateStrategy", "value": {"type": "OnDelete"}},
], separators=(",", ":")))
')" || return 1
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" patch daemonset "${daemonset_name}" --type=json --patch "${patch}" >/dev/null
}

image_cache_rollback_freeze_snapshot_json() (
  local daemonset_name="$1"
  local expected_daemonset_uid="$2"
  local expected_target_revision="$3"
  local inventory_dir=""

  [[ -n "${expected_daemonset_uid}" && -n "${expected_target_revision}" ]] || return 1
  umask 077
  inventory_dir="$(mktemp -d)" || return 1
  trap 'rm -rf -- "${inventory_dir}"' EXIT
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get daemonset "${daemonset_name}" \
    -o json >"${inventory_dir}/daemonset.json" || return 1
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get controllerrevisions.apps \
    -l "app.kubernetes.io/name=fugue,app.kubernetes.io/instance=${FUGUE_RELEASE_NAME},app.kubernetes.io/component=image-cache" \
    -o json >"${inventory_dir}/controller-revisions.json" || return 1
  python3 - \
    "${inventory_dir}/daemonset.json" \
    "${inventory_dir}/controller-revisions.json" \
    "${daemonset_name}" \
    "${FUGUE_NAMESPACE}" \
    "${expected_daemonset_uid}" \
    "${expected_target_revision}" <<'PY'
import copy
import json
import sys
from pathlib import Path

def reject(reason):
    raise SystemExit("image-cache rollback freeze rejected: " + reason)

try:
    daemonset = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    revision_list = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
except (OSError, UnicodeError, json.JSONDecodeError):
    reject("cluster inventory JSON is invalid")

expected_daemonset = sys.argv[3]
expected_namespace = sys.argv[4]
expected_uid = sys.argv[5]
expected_target_revision = sys.argv[6]
metadata = daemonset.get("metadata") or {}
spec = daemonset.get("spec") or {}
template = spec.get("template") or {}
annotations = metadata.get("annotations")
if (
    daemonset.get("apiVersion") != "apps/v1"
    or daemonset.get("kind") != "DaemonSet"
    or str(metadata.get("name") or "") != expected_daemonset
    or str(metadata.get("namespace") or "") != expected_namespace
    or str(metadata.get("uid") or "") != expected_uid
    or not str(metadata.get("resourceVersion") or "")
    or not str(metadata.get("generation") or "")
    or not isinstance(template, dict)
    or not template
    or not isinstance(annotations, dict)
):
    reject("DaemonSet identity is not the pinned pre-Helm object")

legacy_transaction_annotations = {
    "fugue.io/image-cache-rollout-transaction",
    "fugue.io/image-cache-rollout-target-revision",
    "fugue.io/image-cache-rollout-node",
    "fugue.io/image-cache-rollout-old-pod-uid",
    "fugue.io/image-cache-rollout-original-strategy",
}
if legacy_transaction_annotations.intersection(annotations):
    reject("legacy rollout transaction annotations require operator review")

strategy = spec.get("updateStrategy") or {}
strategy_type = str(strategy.get("type") or "")
rolling = strategy.get("rollingUpdate") or {}
rolling_exact = strategy_type == "RollingUpdate" and rolling.get("maxUnavailable") == 1 and rolling.get("maxSurge") == 0
ondelete = strategy_type == "OnDelete" and set(strategy) == {"type"}
if not (rolling_exact or ondelete):
    reject("DaemonSet strategy is not clean OnDelete or RollingUpdate 1/0")

if not isinstance(revision_list, dict) or not isinstance(revision_list.get("items"), list):
    reject("ControllerRevision inventory is invalid")
matches = []
for revision in revision_list["items"]:
    if not isinstance(revision, dict):
        reject("ControllerRevision inventory contains a non-object")
    revision_metadata = revision.get("metadata") or {}
    owners = [
        owner
        for owner in revision_metadata.get("ownerReferences") or []
        if isinstance(owner, dict) and owner.get("controller") is True
    ]
    revision_hash = str((revision_metadata.get("labels") or {}).get("controller-revision-hash") or "")
    if (
        revision.get("apiVersion") != "apps/v1"
        or revision.get("kind") != "ControllerRevision"
        or str(revision_metadata.get("namespace") or "") != expected_namespace
        or revision_hash != expected_target_revision
        or str(revision_metadata.get("name") or "") != expected_daemonset + "-" + revision_hash
        or len(owners) != 1
    ):
        continue
    owner = owners[0]
    if (
        owner.get("apiVersion") != "apps/v1"
        or owner.get("kind") != "DaemonSet"
        or owner.get("name") != expected_daemonset
        or str(owner.get("uid") or "") != expected_uid
    ):
        continue
    revision_template = copy.deepcopy((((revision.get("data") or {}).get("spec") or {}).get("template")))
    if isinstance(revision_template, dict) and revision_template.pop("$patch", None) == "replace" and revision_template == template:
        matches.append(revision)
if len(matches) != 1:
    reject("live Pod template does not exactly match the pinned ControllerRevision")

print(json.dumps({
    "daemonset_uid": str(metadata["uid"]),
    "generation": str(metadata["generation"]),
    "resource_version": str(metadata["resourceVersion"]),
    "strategy": strategy_type,
    "target_revision": expected_target_revision,
}, sort_keys=True, separators=(",", ":")))
PY
)

image_cache_freeze_ondelete_after_helm_rollback() {
  local daemonset_name="$1"
  local expected_daemonset_uid="$2"
  local expected_target_revision="$3"
  local attempt=1
  local max_attempts=5
  local freeze_snapshot_json=""
  local strategy=""
  local resource_version=""

  while (( attempt <= max_attempts )); do
    freeze_snapshot_json="$(image_cache_rollback_freeze_snapshot_json \
      "${daemonset_name}" "${expected_daemonset_uid}" "${expected_target_revision}")" || return 1
    strategy="$(image_cache_plan_field "${freeze_snapshot_json}" strategy)" || return 1
    case "${strategy}" in
      OnDelete)
        return 0
        ;;
      RollingUpdate)
        resource_version="$(image_cache_plan_field "${freeze_snapshot_json}" resource_version)" || return 1
        if image_cache_patch_clean_ondelete "${daemonset_name}" "${resource_version}"; then
          return 0
        fi
        ;;
      *)
        return 1
        ;;
    esac
    if (( attempt < max_attempts )); then
      log "image-cache rollback freeze CAS patch was not confirmed; refreshing the exact snapshot before retry ${attempt}/${max_attempts}"
    fi
    attempt=$((attempt + 1))
  done
  log "image-cache rollback freeze CAS did not succeed after ${max_attempts} exact attempts"
  return 1
}

image_cache_wait_for_rollout_plan() {
  local daemonset_name="$1"
  local target_revision="$2"
  local deadline=$((SECONDS + $(duration_to_seconds "${FUGUE_ROLLOUT_TIMEOUT}")))
  local plan_json=""

  while (( SECONDS < deadline )); do
    if plan_json="$(image_cache_rollout_plan_json "${daemonset_name}" "${target_revision}" 2>/dev/null)"; then
      printf '%s\n' "${plan_json}"
      return 0
    fi
    sleep 2
  done
  image_cache_rollout_plan_json "${daemonset_name}" "${target_revision}" >/dev/null
}

image_cache_prepare_offline_safe_rollout() {
  local daemonset_name="$1"
  local target_revision=""
  local initial_plan_json=""
  local final_plan_json=""
  local strategy=""
  local resource_version=""
  local current_revision=""

  if ! node_local_dns_split_release_enabled; then
    return 0
  fi
  target_revision="$(image_cache_current_controller_revision "${daemonset_name}")" || {
    log "image-cache DaemonSet has no exact current ControllerRevision"
    return 1
  }
  initial_plan_json="$(image_cache_rollout_plan_json "${daemonset_name}" "${target_revision}")" || return 1
  image_cache_bind_plan_to_target "${daemonset_name}" "${initial_plan_json}" "${target_revision}" || return 1
  node_local_dns_verify_preserved_nodes_isolated || return 1
  image_cache_probe_active_plan "${initial_plan_json}" || return 1
  [[ "$(image_cache_plan_field "${initial_plan_json}" transaction)" == "false" ]] || return 1
  strategy="$(image_cache_plan_field "${initial_plan_json}" strategy)" || return 1

  case "${strategy}" in
    RollingUpdate)
      resource_version="$(image_cache_plan_field "${initial_plan_json}" resource_version)" || return 1
      image_cache_patch_clean_ondelete "${daemonset_name}" "${resource_version}" || return 1
      image_cache_wait_strategy_observed "${daemonset_name}" OnDelete || return 1
      final_plan_json="$(image_cache_wait_for_rollout_plan "${daemonset_name}" "${target_revision}")" || return 1
      image_cache_bind_plan_to_target "${daemonset_name}" "${final_plan_json}" "${target_revision}" || return 1
      node_local_dns_verify_preserved_nodes_isolated || return 1
      image_cache_probe_active_plan "${final_plan_json}" || return 1
      [[ "$(image_cache_plan_field "${final_plan_json}" transaction)" == "false" ]] || return 1
      [[ "$(image_cache_plan_field "${final_plan_json}" strategy)" == "OnDelete" ]] || return 1
      image_cache_validate_unchanged_pod_identities "${initial_plan_json}" "${final_plan_json}" || return 1
      ;;
    OnDelete)
      final_plan_json="${initial_plan_json}"
      ;;
    *)
      return 1
      ;;
  esac

  current_revision="$(image_cache_current_controller_revision "${daemonset_name}")" || return 1
  [[ "${current_revision}" == "${target_revision}" ]] || return 1
  IMAGE_CACHE_PRE_HELM_TARGET_REVISION="${target_revision}"
  IMAGE_CACHE_PRE_HELM_PLAN_JSON="${final_plan_json}"
  log "image-cache offline-node guard passed without Pod replacement: target_revision=${target_revision} active_updated=$(image_cache_plan_field "${final_plan_json}" updated_active_count) strategy=OnDelete replacements=0"
}

image_cache_rollout_status() {
  local daemonset_name="$1"
  local target_revision=""
  local plan_json=""

  if ! node_local_dns_split_release_enabled; then
    rollout_daemonset_status "${daemonset_name}"
    return
  fi
  [[ -n "${IMAGE_CACHE_PRE_HELM_TARGET_REVISION}" && -n "${IMAGE_CACHE_PRE_HELM_PLAN_JSON}" ]] || return 1
  target_revision="$(image_cache_current_controller_revision "${daemonset_name}")" || return 1
  if [[ "${target_revision}" != "${IMAGE_CACHE_PRE_HELM_TARGET_REVISION}" ]]; then
    log "image-cache DaemonSet template changed during Helm upgrade while an offline node was preserved: pre_helm=${IMAGE_CACHE_PRE_HELM_TARGET_REVISION} post_helm=${target_revision}"
    return 1
  fi
  plan_json="$(image_cache_wait_for_rollout_plan "${daemonset_name}" "${target_revision}")" || return 1
  image_cache_bind_plan_to_target "${daemonset_name}" "${plan_json}" "${target_revision}" || return 1
  node_local_dns_verify_preserved_nodes_isolated || return 1
  image_cache_probe_active_plan "${plan_json}" || return 1
  [[ "$(image_cache_plan_field "${plan_json}" transaction)" == "false" ]] || return 1
  [[ "$(image_cache_plan_field "${plan_json}" strategy)" == "OnDelete" ]] || return 1
  image_cache_validate_unchanged_pod_identities "${IMAGE_CACHE_PRE_HELM_PLAN_JSON}" "${plan_json}" || return 1
  log "image-cache post-Helm verification passed without additional Pod replacement: target_revision=${target_revision} active_updated=$(image_cache_plan_field "${plan_json}" updated_active_count)"
}

image_cache_restore_ondelete_after_helm_rollback() {
  local daemonset_name="${FUGUE_RELEASE_FULLNAME}-image-cache"
  local target_revision="${IMAGE_CACHE_PRE_HELM_TARGET_REVISION:-}"
  local daemonset_uid=""
  local plan_json=""

  node_local_dns_split_release_enabled || return 0
  [[ "${FUGUE_IMAGE_CACHE_ENABLED:-false}" == "true" ]] || return 0
  [[ -n "${target_revision}" && -n "${IMAGE_CACHE_PRE_HELM_PLAN_JSON:-}" ]] || return 0

  daemonset_uid="$(image_cache_plan_field "${IMAGE_CACHE_PRE_HELM_PLAN_JSON}" daemonset_uid)" || return 1
  image_cache_freeze_ondelete_after_helm_rollback "${daemonset_name}" "${daemonset_uid}" "${target_revision}" || return 1
  # Freeze first. Health and cohort validation must never be able to leave a
  # rollback-restored RollingUpdate controller running while an offline node
  # has consumed its disruption budget.
  image_cache_wait_strategy_observed "${daemonset_name}" OnDelete || return 1
  plan_json="$(image_cache_wait_for_rollout_plan "${daemonset_name}" "${target_revision}")" || return 1
  image_cache_bind_plan_to_target "${daemonset_name}" "${plan_json}" "${target_revision}" || return 1
  node_local_dns_verify_preserved_nodes_isolated || return 1
  image_cache_probe_active_plan "${plan_json}" || return 1
  [[ "$(image_cache_plan_field "${plan_json}" transaction)" == "false" ]] || return 1
  [[ "$(image_cache_plan_field "${plan_json}" strategy)" == "OnDelete" ]] || return 1
  image_cache_validate_unchanged_pod_identities "${IMAGE_CACHE_PRE_HELM_PLAN_JSON}" "${plan_json}" || return 1
  log "image-cache OnDelete guard restored after Helm rollback without changing the Pod cohort"
}

daemonset_names_by_component_prefix() {
  local component_prefix="$1"
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get ds \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.app\.kubernetes\.io/component}{"\n"}{end}' 2>/dev/null |
    awk -v prefix="${component_prefix}" '($2 == prefix || index($2, prefix "-") == 1) { print $1 }' |
    sort
}

rollout_daemonsets_by_component_prefix() {
  local component_prefix="$1"
  local display_name="$2"
  local daemonsets
  daemonsets="$(daemonset_names_by_component_prefix "${component_prefix}")"
  if [[ -z "${daemonsets}" ]]; then
    log "${display_name} rollout check skipped; no matching daemonsets"
    return 0
  fi
  local daemonset_name
  while IFS= read -r daemonset_name; do
    [[ -n "${daemonset_name}" ]] || continue
    log "waiting for ${display_name} daemonset ${daemonset_name}"
    rollout_daemonset_status "${daemonset_name}" || return 1
  done <<<"${daemonsets}"
}

rollout_dynamic_edge_daemonsets_if_present() {
  [[ "${FUGUE_EDGE_DYNAMIC_ENABLED:-false}" == "true" ]] || return 0
  rollout_daemonsets_by_component_prefix "edge-dynamic" "dynamic edge"
}

cleanup_orphaned_regional_daemonsets() {
  local daemonset_name=""
  local component=""
  local release_name=""
  local selector="app.kubernetes.io/instance=${FUGUE_RELEASE_NAME},app.kubernetes.io/name=fugue"

  while IFS=$'\t' read -r daemonset_name component release_name; do
    if [[ -z "$(trim_field "${daemonset_name}")" ]]; then
      continue
    fi
    case "${component}" in
      edge-country-*|dns-country-*)
        ;;
      *)
        continue
        ;;
    esac
    if [[ -n "$(trim_field "${release_name}")" ]]; then
      continue
    fi
    log "deleting orphaned legacy regional DaemonSet ${daemonset_name} (${component})"
    ${KUBECTL} -n "${FUGUE_NAMESPACE}" delete "ds/${daemonset_name}" --wait=true
  done < <(${KUBECTL} -n "${FUGUE_NAMESPACE}" get ds -l "${selector}" -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.app\.kubernetes\.io/component}{"\t"}{.metadata.annotations.meta\.helm\.sh/release-name}{"\n"}{end}')
}

apply_chart_crds() {
  local crd_dir="${FUGUE_HELM_CHART_PATH}/crds"
  local attempt=0
  local max_attempts=3

  if [[ ! -d "${crd_dir}" ]]; then
    log "skip CRD apply because ${crd_dir} does not exist"
    return 0
  fi

  if ! find "${crd_dir}" -maxdepth 1 -type f \( -name '*.yaml' -o -name '*.yml' \) | grep -q .; then
    log "skip CRD apply because ${crd_dir} has no manifest files"
    return 0
  fi

  while (( attempt < max_attempts )); do
    attempt=$((attempt + 1))
    log "applying Helm CRDs from ${crd_dir} (attempt ${attempt}/${max_attempts})"
    ${KUBECTL} apply -f "${crd_dir}"
    ${KUBECTL} wait --for=condition=Established --timeout=60s -f "${crd_dir}"
    if verify_chart_crds_in_sync "${crd_dir}"; then
      return 0
    fi
    if (( attempt < max_attempts )); then
      log "Helm CRDs still drift after apply; retrying"
      sleep 2
    fi
  done

  fail "Helm CRDs still drift after apply; refusing to continue upgrade"
}

verify_chart_crds_in_sync() {
  local crd_dir="$1"
  local diff_output=""

  if diff_output="$(${KUBECTL} diff -f "${crd_dir}" 2>&1)"; then
    return 0
  fi

  local status=$?
  if (( status == 1 )); then
    log_stderr "Helm CRD drift detected after apply:"
    printf '%s\n' "${diff_output}" >&2
    return 1
  fi

  log_stderr "kubectl diff failed while verifying Helm CRDs (exit=${status}):"
  printf '%s\n' "${diff_output}" >&2
  return "${status}"
}

deployment_exists() {
  local deployment_name="$1"
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get "deploy/${deployment_name}" >/dev/null 2>&1
}

smoke_test() {
  local saw_url="false"
  while IFS= read -r url; do
    url="$(trim_field "${url}")"
    if [[ -z "${url}" ]]; then
      continue
    fi
    saw_url="true"
    smoke_test_url "${url}"
  done < <(smoke_test_urls)
  if [[ "${saw_url}" != "true" ]]; then
    fail "missing required smoke URLs; set FUGUE_SMOKE_URL or FUGUE_SMOKE_URLS"
  fi
}

smoke_test_url() {
  local url="$1"
  curl -fsS --max-time 10 "${url}" >/dev/null
  smoke_test_resolve_edges "${url}"
}

smoke_test_urls() {
  local raw="${FUGUE_SMOKE_URLS:-}"
  if [[ -z "$(trim_field "${raw}")" ]]; then
    raw="${FUGUE_SMOKE_URL:-}"
  fi
  raw="${raw//;/$'\n'}"
  raw="${raw//,/$'\n'}"
  printf '%s\n' "${raw}"
}

smoke_test_resolve_edges() {
  local url="$1"
  local scheme=""
  local hostport=""
  local host=""
  local port=""
  local ip=""
  local -a ips=()

  if [[ -z "$(trim_field "${url}")" ]]; then
    return 0
  fi

  scheme="${url%%://*}"
  hostport="${url#*://}"
  hostport="${hostport%%/*}"
  host="${hostport}"
  if [[ "${hostport}" == *:* && "${hostport}" != *:*:* ]]; then
    host="${hostport%:*}"
    port="${hostport##*:}"
  fi
  host="${host#\[}"
  host="${host%\]}"
  if [[ -z "${port}" ]]; then
    case "${scheme}" in
      https)
        port="443"
        ;;
      *)
        port="80"
        ;;
    esac
  fi

  mapfile -t ips < <(smoke_resolve_edge_ips)
  if (( ${#ips[@]} == 0 )); then
    log "smoke resolve check skipped because no edge IPs were discovered from DNS/env inputs"
    return 0
  fi

  for ip in "${ips[@]}"; do
    log "smoke resolve check host=${host} port=${port} ip=${ip}"
    curl -fsS --max-time 10 --resolve "${host}:${port}:${ip}" "${url}" >/dev/null
  done
}

smoke_resolve_edge_ips() {
  declare -A seen=()
  local ip=""

  smoke_resolve_ips_from_value "${FUGUE_DNS_ANSWER_IPS:-}" seen
  if [[ -n "$(trim_field "${FUGUE_DNS_EXTRA_GROUPS:-}")" ]]; then
    while IFS= read -r entry; do
      local answer_ips=""
      IFS='|' read -r _ _ _ answer_ips _ _ <<<"${entry}"
      smoke_resolve_ips_from_value "${answer_ips}" seen
    done < <(csv_lines "${FUGUE_DNS_EXTRA_GROUPS}")
  fi
  smoke_resolve_ips_from_value "${FUGUE_EDGE_PUBLIC_IPV4:-}" seen
  smoke_resolve_ips_from_value "${FUGUE_EDGE_PUBLIC_IPV6:-}" seen

  for ip in "${!seen[@]}"; do
    printf '%s\n' "${ip}"
  done | sort -V
}

smoke_resolve_ips_from_value() {
  local raw="$1"
  local -n seen_ref="$2"
  local value=""
  while IFS= read -r value; do
    value="$(trim_field "${value}")"
    if [[ -n "${value}" ]]; then
      seen_ref["${value}"]=1
    fi
  done < <(dns_answer_ips_lines "${raw}")
}

cleanup_control_plane_automation_tmp() {
  if [[ -n "${CONTROL_PLANE_AUTOMATION_TMP_DIR}" && -d "${CONTROL_PLANE_AUTOMATION_TMP_DIR}" ]]; then
    rm -rf "${CONTROL_PLANE_AUTOMATION_TMP_DIR}"
  fi
  if [[ -n "${KUBECONFIG_FALLBACK_FILE}" && -f "${KUBECONFIG_FALLBACK_FILE}" ]]; then
    rm -f "${KUBECONFIG_FALLBACK_FILE}"
  fi
}

cleanup_upgrade_override_values() {
  if [[ -n "${UPGRADE_OVERRIDE_VALUES_FILE}" && -f "${UPGRADE_OVERRIDE_VALUES_FILE}" ]]; then
    rm -f "${UPGRADE_OVERRIDE_VALUES_FILE}"
  fi
  if [[ -n "${HELM_POST_RENDERER_FILE}" && -f "${HELM_POST_RENDERER_FILE}" ]]; then
    rm -f "${HELM_POST_RENDERER_FILE}"
  fi
  if [[ -n "${DNS_STATIC_RECORDS_FILE}" && -f "${DNS_STATIC_RECORDS_FILE}" ]]; then
    rm -f "${DNS_STATIC_RECORDS_FILE}"
  fi
  if [[ -n "${PLATFORM_ROUTES_FILE}" && -f "${PLATFORM_ROUTES_FILE}" ]]; then
    rm -f "${PLATFORM_ROUTES_FILE}"
  fi
  if [[ -n "${DISCOVERY_BUNDLE_FILE_TEMP}" && -f "${DISCOVERY_BUNDLE_FILE_TEMP}" ]]; then
    rm -f "${DISCOVERY_BUNDLE_FILE_TEMP}"
  fi
  if [[ -n "${ROBUSTNESS_HEALTH_GATE_BASELINE_FILE}" && -f "${ROBUSTNESS_HEALTH_GATE_BASELINE_FILE}" ]]; then
    rm -f "${ROBUSTNESS_HEALTH_GATE_BASELINE_FILE}"
  fi
  if [[ -n "${DNS_MANIFEST_SNAPSHOT_FILE}" && -f "${DNS_MANIFEST_SNAPSHOT_FILE}" ]]; then
    if [[ "${DNS_MANIFEST_SNAPSHOT_KEEP:-false}" == "true" ]]; then
      log_stderr "preserving DNS manifest rollback snapshot for operator recovery: ${DNS_MANIFEST_SNAPSHOT_FILE}"
    else
      rm -f "${DNS_MANIFEST_SNAPSHOT_FILE}"
    fi
  fi
}

cleanup_tmp_artifacts() {
  local exit_status=$?
  if [[ "${exit_status}" != "0" && "${DNS_MANIFEST_TRANSACTION_REQUIRED:-false}" == "true" && "${DNS_MANIFEST_ROLLBACK_RESTORED:-false}" != "true" ]]; then
    DNS_MANIFEST_SNAPSHOT_KEEP="true"
  fi
  cleanup_control_plane_automation_tmp
  cleanup_upgrade_override_values
  return "${exit_status}"
}

write_upgrade_override_values() {
  UPGRADE_OVERRIDE_VALUES_FILE="$(mktemp -t fugue-upgrade-values.XXXXXX.yaml)"
  cat >"${UPGRADE_OVERRIDE_VALUES_FILE}" <<'EOF'
nodeSelector: {}
tolerations:
  - key: node.kubernetes.io/disk-pressure
    operator: Exists
    effect: NoSchedule
api:
  # Keep the control-plane API off tenant-owned runtime nodes. When this
  # lands on a shared app node, tenant traffic can directly inflate page-load
  # latency for fugue-web and other callers.
  nodeSelector:
    node-role.kubernetes.io/control-plane: "true"
  # Explicit non-empty tolerations prevent Helm's `default` fallback from
  # inheriting the global disk-pressure toleration onto stateless workloads.
  tolerations:
    - key: node-role.kubernetes.io/control-plane
      operator: Exists
      effect: NoSchedule
  strategy:
    rollingUpdate:
      maxUnavailable: 0
      maxSurge: 1
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - key: node-role.kubernetes.io/control-plane
                operator: Exists
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
                app.kubernetes.io/name: fugue
                app.kubernetes.io/instance: fugue
                app.kubernetes.io/component: api
  resources:
    requests:
      cpu: 250m
      memory: 768Mi
    limits:
      cpu: "1"
      memory: 1536Mi
  podDisruptionBudget:
    enabled: true
    minAvailable: 2
controller:
  nodeSelector:
    node-role.kubernetes.io/control-plane: "true"
  tolerations:
    - key: node-role.kubernetes.io/control-plane
      operator: Exists
      effect: NoSchedule
  strategy:
    rollingUpdate:
      maxUnavailable: 0
      maxSurge: 2
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - key: node-role.kubernetes.io/control-plane
                operator: Exists
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
                app.kubernetes.io/name: fugue
                app.kubernetes.io/instance: fugue
                app.kubernetes.io/component: controller
  resources:
    requests:
      cpu: 100m
      memory: 256Mi
    limits:
      cpu: "1"
      memory: 512Mi
snapshotController:
  nodeSelector:
    node-role.kubernetes.io/control-plane: "true"
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
topologyLabeler:
  tolerations:
    - key: node-role.kubernetes.io/control-plane
      operator: Equal
      effect: NoSchedule
    - key: node-role.kubernetes.io/master
      operator: Equal
      effect: NoSchedule
    - key: fugue.io/dedicated
      operator: Equal
      value: internal
      effect: NoSchedule
edge:
  nodeSelector:
    fugue.io/role.edge: "true"
    fugue.io/schedulable: "true"
  tolerations:
    - key: fugue.io/dedicated
      operator: Equal
      value: edge
      effect: NoSchedule
    - key: fugue.io/tenant
      operator: Exists
      effect: NoSchedule
  caddy:
    enabled: ${FUGUE_EDGE_CADDY_ENABLED}
    listenAddr: $(yaml_quote "${FUGUE_EDGE_CADDY_LISTEN_ADDR}")
    tlsMode: $(yaml_quote "${FUGUE_EDGE_CADDY_TLS_MODE}")
    publicHostPorts:
      enabled: ${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED}
      http: ${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTP}
      https: ${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTPS}
  dynamic:
    enabled: ${FUGUE_EDGE_DYNAMIC_ENABLED}
cloudnative-pg:
  replicaCount: 2
  priorityClassName: system-cluster-critical
  nodeSelector:
    node-role.kubernetes.io/control-plane: "true"
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
EOF
  append_control_plane_singleton_values
  append_registry_upgrade_values
  append_headscale_upgrade_values
  append_upgrade_edge_dynamic_values
  append_upgrade_image_prepull_values
  append_upgrade_dns_values
  append_upgrade_mesh_recovery_values
}

append_upgrade_image_prepull_values() {
  local image
  if [[ -z "${FUGUE_IMAGE_PREPULL_IMAGES:-}" ]]; then
    return 0
  fi
  cat >>"${UPGRADE_OVERRIDE_VALUES_FILE}" <<'EOF'

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
    printf '    - "%s"\n' "${image}" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  done
}

yaml_quote() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '"%s"' "${value}"
}

helm_set_string_value() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//,/\\,}"
  printf '%s' "${value}"
}

trim_field() {
  printf '%s' "$1" | awk '{$1=$1; print}'
}

is_legacy_local_cluster_join_registry_endpoint() {
  local value=""
  local host=""
  local port=""

  value="$(trim_field "$1")"
  value="${value#http://}"
  value="${value#https://}"
  value="${value%%/*}"
  host="${value%:*}"
  port="${value##*:}"
  [[ -n "${host}" && "${host}" != "${value}" ]] || return 1
  [[ "${port}" == "${FUGUE_REGISTRY_NODEPORT:-30500}" ]] || return 1
  case "${host}" in
    127.0.0.1|localhost) return 0 ;;
    *) return 1 ;;
  esac
}

release_api_base_url() {
  local base_url=""
  base_url="$(trim_field "${FUGUE_RELEASE_PREFLIGHT_API_URL:-${FUGUE_API_URL:-${FUGUE_BASE_URL:-}}}")"
  if [[ -z "${base_url}" && -n "$(trim_field "${FUGUE_API_PUBLIC_DOMAIN:-}")" ]]; then
    base_url="https://${FUGUE_API_PUBLIC_DOMAIN}"
  fi
  if [[ -z "${base_url}" && -n "$(trim_field "${FUGUE_SMOKE_URL:-}")" ]]; then
    base_url="${FUGUE_SMOKE_URL%/healthz}"
  fi
  printf '%s' "${base_url%/}"
}

release_api_token() {
  trim_field "${FUGUE_API_KEY:-${FUGUE_TOKEN:-${FUGUE_BOOTSTRAP_KEY:-}}}"
}

discovery_bundle_source_configured() {
  if [[ -n "${DISCOVERY_BUNDLE_FILE}" && -r "${DISCOVERY_BUNDLE_FILE}" ]]; then
    return 0
  fi
  if [[ -n "$(trim_field "${FUGUE_DISCOVERY_BUNDLE_FILE:-}")" ]]; then
    return 0
  fi
  if [[ -n "$(trim_field "${FUGUE_DISCOVERY_BUNDLE_URL:-}")" ]]; then
    return 0
  fi
  if [[ -n "$(trim_field "${FUGUE_BASE_URL:-${FUGUE_API_URL:-}}")" ]]; then
    return 0
  fi
  return 1
}

release_preflight_missing_discovery_bootstrap_allowed() {
  case "${FUGUE_RELEASE_PREFLIGHT_ALLOW_MISSING_DISCOVERY_BOOTSTRAP:-true}" in
    1|true|TRUE|yes|YES)
      ;;
    *)
      return 1
      ;;
  esac
  if discovery_bundle_source_configured; then
    return 1
  fi
  [[ -n "$(trim_field "${FUGUE_REGISTRY_PULL_BASE:-}")" ]] || return 1
  [[ -n "$(trim_field "${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT:-}")" ]] || return 1
  [[ -n "$(trim_field "${FUGUE_SMOKE_URL:-}")" ]] || return 1
  return 0
}

node_local_dns_split_release_enabled() {
  [[ "${FUGUE_NODE_LOCAL_DNS_ENABLED:-false}" == "true" ]] || return 1
  [[ "${NODE_LOCAL_DNS_SPLIT_COHORT:-false}" == "true" ]] || return 1
  [[ -n "$(trim_field "${NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES:-}")" ]]
}

node_local_dns_pin_preflight_targets() {
  local active_nodes=""
  local overlap=""

  node_local_dns_split_release_enabled || return 0
  active_nodes="$(node_local_dns_candidate_nodes)" || return 1
  [[ -n "$(trim_field "${active_nodes}")" ]] || return 1
  overlap="$(comm -12 \
    <(printf '%s\n' "${active_nodes}" | sed '/^[[:space:]]*$/d' | sort -u) \
    <(printf '%s\n' "${NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES}" | sed '/^[[:space:]]*$/d' | sort -u))"
  [[ -z "$(trim_field "${overlap}")" ]] || return 1
  NODE_LOCAL_DNS_PREFLIGHT_TARGET_NODES="${active_nodes}"
}

node_local_dns_offline_preserve_policy_gate() {
  local autonomy_status_file="$1"
  local node_policies_file="$2"
  local phase="${3:-release}"

  node_local_dns_split_release_enabled || return 1
  [[ -n "$(trim_field "${NODE_LOCAL_DNS_PREFLIGHT_TARGET_NODES:-}")" ]] || return 1
  AUTONOMY_STATUS_FILE="${autonomy_status_file}" NODE_POLICIES_FILE="${node_policies_file}" \
    PRESERVED_NODES="${NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES}" ACTIVE_NODES="${NODE_LOCAL_DNS_PREFLIGHT_TARGET_NODES}" \
    GATE_PHASE="${phase}" python3 -c '
import json
import os

def reject(reason):
    raise SystemExit("NodeLocal offline-preserve policy gate rejected: " + reason)

def names(value):
    items = [item.strip() for item in value.splitlines() if item.strip()]
    if not items or len(items) != len(set(items)):
        reject("node list is empty or contains duplicates")
    return items

def load(path, label):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        reject(f"{label} JSON is unreadable or invalid")

autonomy_payload = load(os.environ["AUTONOMY_STATUS_FILE"], "autonomy status")
policy_payload = load(os.environ["NODE_POLICIES_FILE"], "node policy")
status = autonomy_payload.get("status")
if not isinstance(status, dict):
    reject("autonomy status is missing")
checks_raw = status.get("checks")
if not isinstance(checks_raw, list) or not checks_raw:
    reject("autonomy checks are missing")
checks = {}
for item in checks_raw:
    if not isinstance(item, dict):
        reject("autonomy check item is invalid")
    name = str(item.get("name") or "").strip()
    if not name or name in checks:
        reject("autonomy check name is empty or duplicated")
    checks[name] = item
failing = {name for name, item in checks.items() if item.get("pass") is not True}
if failing != {"node_policy"} or status.get("pass") is not False or status.get("block_rollout") is not False:
    reject("autonomy failures are not exactly the non-blocking node_policy check")
store = status.get("control_plane_store")
if not isinstance(store, dict) or store.get("block_rollout") is not False or str(store.get("permission_verification_status") or "").strip() != "passed":
    reject("control-plane store gate did not pass")

preserved = set(names(os.environ["PRESERVED_NODES"]))
active = set(names(os.environ["ACTIVE_NODES"]))
if preserved & active:
    reject("active and preserved cohorts overlap")
raw_statuses = policy_payload.get("node_policies")
if not isinstance(raw_statuses, list) or not raw_statuses:
    reject("node policy list is missing")
by_name = {}
for item in raw_statuses:
    if not isinstance(item, dict):
        reject("node policy item is invalid")
    name = str(item.get("node_name") or "").strip()
    if not name or name in by_name:
        reject("node policy name is empty or duplicated")
    by_name[name] = item
if not preserved.issubset(by_name) or not active.issubset(by_name):
    reject("active or preserved cohort is missing from node policy inventory")
blockers = {name for name, item in by_name.items() if item.get("block_rollout") is True}
if blockers != preserved:
    reject("rollout blockers do not exactly match the preserved cohort")

for name, item in by_name.items():
    policy = item.get("policy")
    if not isinstance(policy, dict):
        reject(f"node policy view is missing node={name}")
    filesystem_pressure = item.get("filesystem_pressure")
    if filesystem_pressure is not True and filesystem_pressure is not False:
        reject(f"filesystem pressure is not boolean node={name}")
    if item.get("reconciled") is not True:
        reject(f"node policy is not reconciled node={name}")
    if item.get("disk_pressure") is not False:
        reject(f"node reports DiskPressure node={name}")
    if name in preserved:
        labels = item.get("labels")
        taints = item.get("taints")
        if not isinstance(labels, dict) or not isinstance(taints, list):
            reject(f"preserved node labels or taints are invalid node={name}")
        isolated = any(
            isinstance(taint, dict)
            and (
                (taint.get("key") == "fugue.io/node-unhealthy" and taint.get("effect") == "NoSchedule")
                or (taint.get("key") == "node.kubernetes.io/unreachable" and taint.get("effect") in {"NoSchedule", "NoExecute"})
            )
            for taint in taints
        )
        disabled = (
            "allow_app_runtime", "allow_builds", "allow_shared_pool", "allow_edge", "allow_dns", "allow_internal_maintenance",
            "effective_app_runtime", "effective_builds", "effective_shared_pool", "effective_edge", "effective_dns", "effective_internal_maintenance",
        )
        if (
            item.get("ready") is not False
            or item.get("node_schedulable") is not False
            or item.get("block_rollout") is not True
            or str(item.get("gate_reason") or "").strip() != "node is not ready"
            or str(item.get("reconcile_error") or "").strip()
            or any(policy.get(key) is not False for key in disabled)
            or policy.get("effective_schedulable") is not False
            or str(policy.get("dedicated_mode") or "none").strip() != "none"
            or str(policy.get("effective_dedicated_mode") or "none").strip() != "none"
            or str(policy.get("desired_control_plane_role") or "none").strip() != "none"
            or str(policy.get("effective_control_plane_role") or "none").strip() != "none"
            or str(labels.get("fugue.io/schedulable") or "").lower() != "false"
            or not isolated
        ):
            reject(f"preserved node isolation invariant failed node={name}")
    else:
        if name in active and filesystem_pressure is not False:
            reject(f"active node reports filesystem pressure node={name}")
        if (
            item.get("ready") is not True
            or item.get("node_schedulable") is not True
            or item.get("block_rollout") is not False
            or policy.get("effective_schedulable") is not True
        ):
            reject(f"non-preserved node readiness invariant failed node={name}")

filesystem_pressure_count = sum(
    1 for item in by_name.values() if item.get("filesystem_pressure") is True
)
summary = policy_payload.get("summary")
if not isinstance(summary, dict):
    reject("node policy summary is missing")
def summary_count(field):
    value = summary.get(field)
    if type(value) is not int:
        reject(f"node policy summary counter is not an integer field={field}")
    return value

if (
    summary_count("total") != len(by_name)
    or summary_count("reconciled") != len(by_name)
    or summary_count("drifted") != 0
    or summary_count("ready") != len(by_name) - len(preserved)
    or summary_count("disk_pressure") != 0
    or summary_count("filesystem_pressure") != filesystem_pressure_count
    or summary_count("blocked_by_health") != len(preserved)
):
    reject("node policy summary does not match item-level state")

phase = os.environ.get("GATE_PHASE", "release")
print(
    "NodeLocal offline-preserve policy gate passed"
    + f" phase={phase}"
    + " preserved=" + ",".join(sorted(preserved))
    + " active=" + ",".join(sorted(active))
)
'
}

run_release_preflight() {
  local api_base=""
  local token=""
  local discovery_headers=""
  local discovery_body=""
  local discovery_http_status=""
  local autonomy_status_file=""
  local edge_nodes_file=""
  local dns_nodes_file=""
  local node_policies_file=""
  local image_cache_status_file=""
  local discovery_etag=""
  local discovery_missing="false"
  local node_local_dns_offline_override_allowed="false"
  local node_local_dns_offline_override_message=""

  case "${FUGUE_RELEASE_PREFLIGHT_ENABLED:-true}" in
    1|true|TRUE|yes|YES)
      ;;
    *)
      log "release preflight disabled"
      return 0
      ;;
  esac

  api_base="$(release_api_base_url)"
  [[ -n "$(trim_field "${api_base}")" ]] || fail "cannot run release preflight without FUGUE_API_URL, FUGUE_BASE_URL, FUGUE_SMOKE_URL, or FUGUE_API_PUBLIC_DOMAIN"
  token="$(release_api_token)"
  [[ -n "$(trim_field "${token}")" ]] || fail "cannot run release preflight without FUGUE_API_KEY, FUGUE_TOKEN, or FUGUE_BOOTSTRAP_KEY"
  command_exists python3 || fail "python3 is required for release preflight JSON checks"
  if node_local_dns_split_release_enabled && ! node_local_dns_pin_preflight_targets; then
    fail "cannot pin a non-overlapping active NodeLocal DNSCache cohort before release preflight"
  fi

  discovery_headers="$(mktemp)"
  discovery_body="$(mktemp)"
  discovery_http_status="$(curl -sS -D "${discovery_headers}" -H "Authorization: Bearer ${token}" "${api_base}/v1/discovery/bundle" -o "${discovery_body}" -w '%{http_code}' || true)"
  case "${discovery_http_status}" in
    200|204|304)
      ;;
    *)
      if release_preflight_missing_discovery_bootstrap_allowed && [[ "${discovery_http_status}" =~ ^(000|404|405|501)$ ]]; then
        log "release preflight bootstrap: DiscoveryBundle endpoint unavailable (HTTP ${discovery_http_status}); continuing with explicit runtime values"
        if ! node_local_dns_split_release_enabled; then
          rm -f "${discovery_headers}" "${discovery_body}"
          return 0
        fi
        discovery_missing="true"
        discovery_etag="explicit-runtime-values"
        : >"${discovery_body}"
      else
        fail "DiscoveryBundle preflight failed with HTTP ${discovery_http_status:-unknown}"
      fi
      ;;
  esac
  if [[ "${discovery_missing}" != "true" ]]; then
    discovery_etag="$(awk 'tolower($1) == "etag:" {print $2; exit}' "${discovery_headers}" | tr -d '\r')"
  fi
  if [[ "${discovery_missing}" != "true" && -z "$(trim_field "${discovery_etag}")" ]]; then
    if release_preflight_missing_discovery_bootstrap_allowed && [[ ! -s "${discovery_body}" ]]; then
      log "release preflight bootstrap: DiscoveryBundle endpoint returned an empty response without an ETag; continuing with explicit runtime values"
      if ! node_local_dns_split_release_enabled; then
        rm -f "${discovery_headers}" "${discovery_body}"
        return 0
      fi
      discovery_missing="true"
      discovery_etag="explicit-runtime-values"
    else
      fail "DiscoveryBundle preflight did not return an ETag"
    fi
  fi
  if [[ "${discovery_missing}" != "true" ]]; then
    python3 - "${discovery_body}" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as fh:
    bundle = json.load(fh)

if not str(bundle.get("generation", "")).strip():
    raise SystemExit("DiscoveryBundle generation is empty")
if not str(bundle.get("schema_version", "")).strip():
    raise SystemExit("DiscoveryBundle schema version is empty")
if str(bundle.get("schema_version", "")).split(".", 1)[0] != "1":
    raise SystemExit(f"unsupported DiscoveryBundle schema version: {bundle.get('schema_version', '')}")
if not str(bundle.get("signature", "")).strip():
    raise SystemExit("DiscoveryBundle signature is empty")
PY
  fi

  autonomy_status_file="$(mktemp)"
  curl -fsS -H "Authorization: Bearer ${token}" "${api_base}/v1/admin/platform/autonomy/status" -o "${autonomy_status_file}"
  edge_nodes_file="$(mktemp)"
  curl -fsS -H "Authorization: Bearer ${token}" "${api_base}/v1/edge/nodes" -o "${edge_nodes_file}"
  dns_nodes_file="$(mktemp)"
  curl -fsS -H "Authorization: Bearer ${token}" "${api_base}/v1/dns/nodes" -o "${dns_nodes_file}"
  node_policies_file="$(mktemp)"
  if ! curl -fsS -H "Authorization: Bearer ${token}" "${api_base}/v1/cluster/node-policies/status" -o "${node_policies_file}"; then
    if node_local_dns_split_release_enabled; then
      fail "release preflight: node policy status endpoint is required for an offline-preserve NodeLocal DNSCache rollout"
    fi
    log "release preflight: node policy status endpoint unavailable; evaluating raw edge and DNS inventory"
    : >"${node_policies_file}"
  fi
  image_cache_status_file="$(mktemp)"
  if [[ -n "${KUBECTL:-}" ]]; then
    if command_exists timeout; then
      if ! timeout "${FUGUE_RELEASE_PREFLIGHT_KUBECTL_TIMEOUT_SECONDS:-15}s" \
        ${KUBECTL} -n "${FUGUE_NAMESPACE}" get "ds/${FUGUE_RELEASE_FULLNAME}-image-cache" -o json >"${image_cache_status_file}" 2>/dev/null; then
        : >"${image_cache_status_file}"
      fi
    elif ! ${KUBECTL} -n "${FUGUE_NAMESPACE}" get "ds/${FUGUE_RELEASE_FULLNAME}-image-cache" -o json >"${image_cache_status_file}" 2>/dev/null; then
      : >"${image_cache_status_file}"
    fi
  fi
  local autonomy_override_message=""
  local node_local_build_plane_override_allowed="false"
  if node_local_dns_split_release_enabled; then
    if ! node_local_dns_offline_override_message="$(node_local_dns_offline_preserve_policy_gate "${autonomy_status_file}" "${node_policies_file}" preflight)"; then
      rm -f "${autonomy_status_file}" "${edge_nodes_file}" "${dns_nodes_file}" "${node_policies_file}" "${image_cache_status_file}"
      fail "offline-preserve NodeLocal DNSCache preflight policy gate failed"
    fi
    node_local_dns_offline_override_allowed="true"
  elif node_local_build_plane_preflight_override_allowed; then
    node_local_build_plane_override_allowed="true"
  fi
  if ! autonomy_override_message="$(python3 - "${autonomy_status_file}" "${edge_nodes_file}" "${dns_nodes_file}" "${node_policies_file}" "${node_local_build_plane_override_allowed}" "${node_local_dns_offline_override_allowed}" "${image_cache_status_file}" "${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT}" "${FUGUE_REGISTRY_PULL_BASE}" "${FUGUE_IMAGE_STORE_MIN_REPLICAS:-1}" <<'PY'
import json
import os
from datetime import datetime, timedelta, timezone
import sys
from urllib.parse import urlparse

def trim(value):
    return str(value or "").strip()

def as_int(value):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0

def parse_endpoint(value):
    value = trim(value)
    if not value:
        return None
    if not value.startswith(("http://", "https://")):
        value = "http://" + value
    parsed = urlparse(value)
    if not parsed.hostname:
        return None
    return parsed

def load_node_policy_statuses(path):
    if not trim(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return []
    statuses = payload.get("node_policies") or []
    return [status for status in statuses if isinstance(status, dict)]

def filter_nodes_for_effective_policy(nodes, statuses, role_key):
    disabled = set()
    for status in statuses:
        node_name = trim(status.get("node_name"))
        policy = status.get("policy") or {}
        if node_name and isinstance(policy, dict) and policy.get(role_key) is False:
            disabled.add(node_name)
    if not disabled:
        return nodes, []
    out = []
    retired = []
    for node in nodes:
        node_id = trim(node.get("id"))
        if node_id and node_id in disabled:
            retired.append(node_id)
            continue
        out.append(node)
    return out, retired

def heartbeat_fresh(node, now):
    seen = node.get("last_heartbeat_at") or node.get("last_seen_at")
    if not seen:
        return False
    try:
      seen_at = datetime.fromisoformat(str(seen).replace("Z", "+00:00"))
    except ValueError as exc:
      raise SystemExit(f"invalid edge node timestamp: {seen}") from exc
    if seen_at.tzinfo is None:
        seen_at = seen_at.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    if seen_at > now:
        return True
    return now - seen_at <= timedelta(seconds=90)

def edge_node_bootstrap_pending(node, now):
    if heartbeat_fresh(node, now):
        return False
    if trim(node.get("status")).lower() != "unknown":
        return False
    if trim(node.get("last_error")):
        return False
    if trim(node.get("route_bundle_version")):
        return False
    if trim(node.get("serving_generation")):
        return False
    if int(node.get("caddy_route_count") or 0) != 0:
        return False
    if trim(node.get("cache_status")):
        return False
    return True

def edge_node_has_route_state(node):
    return int(node.get("caddy_route_count") or 0) > 0 or bool(trim(node.get("route_bundle_version"))) or bool(trim(node.get("serving_generation"))) or bool(trim(node.get("lkg_generation")))

def edge_node_route_serving_capable(node, now):
    if node.get("draining"):
        return False
    if not node.get("healthy"):
        return False
    if not heartbeat_fresh(node, now):
        return False
    status = trim(node.get("status")).lower()
    if status == "healthy":
        return True
    if status == "degraded":
        return trim(node.get("caddy_last_error")) == "" and int(node.get("caddy_route_count") or 0) > 0
    return False

def edge_node_route_bootstrap_capable(node, now):
    if node.get("draining") or not node.get("healthy") or not heartbeat_fresh(node, now):
        return False
    if edge_node_route_serving_capable(node, now):
        return False
    return edge_node_has_route_state(node)

def edge_inventory_healthy(nodes):
    if not nodes:
        return False, [], [], []
    healthy_count = 0
    now = datetime.now(timezone.utc)
    bootstrap_pending = []
    route_bootstrap_pending = []
    route_bootstrap_groups = []
    for node in nodes:
        if node.get("draining"):
            continue
        if edge_node_bootstrap_pending(node, now):
            bootstrap_pending.append(trim(node.get("id")))
            group_id = trim(node.get("edge_group_id"))
            if group_id:
                route_bootstrap_groups.append(group_id)
            continue
        if edge_node_route_bootstrap_capable(node, now):
            route_bootstrap_pending.append(trim(node.get("id")))
            group_id = trim(node.get("edge_group_id"))
            if group_id:
                route_bootstrap_groups.append(group_id)
            continue
        if not edge_node_route_serving_capable(node, now):
            return False, bootstrap_pending, route_bootstrap_pending, route_bootstrap_groups
        if trim(node.get("last_error")):
            return False, bootstrap_pending, route_bootstrap_pending, route_bootstrap_groups
        cache_status = trim(node.get("cache_status")).lower()
        if cache_status and "error" in cache_status:
            return False, bootstrap_pending, route_bootstrap_pending, route_bootstrap_groups
        healthy_count += 1
    return healthy_count > 0, bootstrap_pending, route_bootstrap_pending, route_bootstrap_groups

def dns_node_route_bootstrap_capable(node, route_bootstrap_groups):
    group_id = trim(node.get("edge_group_id"))
    if not node.get("healthy"):
        return False
    last_error = trim(node.get("last_error")).lower()
    if not ("edge dns bundle invariant failed" in last_error or "without active route" in last_error or "no active route" in last_error):
        return False
    if group_id == "":
        return False
    if route_bootstrap_groups and group_id not in route_bootstrap_groups:
        return False
    return True

def dns_node_serving_capable(node):
    if not node.get("healthy"):
        return False
    status = trim(node.get("status")).lower()
    if status not in {"healthy", "degraded"}:
        return False
    if not trim(node.get("dns_bundle_version")):
        return False
    if as_int(node.get("record_count")) <= 0:
        return False
    cache_status = trim(node.get("cache_status")).lower()
    if cache_status in {"", "missing"} or "error" in cache_status:
        return False
    if as_int(node.get("cache_write_errors")) != 0:
        return False
    return True

def dns_inventory_bootstrap_healthy(nodes, route_bootstrap_groups):
    if not nodes:
        return False, []
    healthy_count = 0
    bootstrap_pending = []
    for node in nodes:
        if dns_node_serving_capable(node):
            healthy_count += 1
            continue
        if dns_node_route_bootstrap_capable(node, route_bootstrap_groups):
            bootstrap_pending.append(trim(node.get("id")))
            continue
        return False, bootstrap_pending
    return healthy_count > 0 or len(bootstrap_pending) > 0, bootstrap_pending

def node_local_image_cache_endpoint(endpoint, registry_pull_base):
    parsed = parse_endpoint(endpoint)
    pull = parse_endpoint(registry_pull_base)
    if parsed is None or pull is None:
        return False
    if parsed.hostname.lower() not in {"127.0.0.1", "localhost", "::1"}:
        return False
    return bool(parsed.port) and parsed.port == pull.port

def image_cache_daemonset_ready(path, endpoint, registry_pull_base, minimum_replicas):
    if not node_local_image_cache_endpoint(endpoint, registry_pull_base):
        return False, ""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return False, "image-cache daemonset status unavailable"
    status = payload.get("status") or {}
    desired = int(status.get("desiredNumberScheduled") or 0)
    ready = int(status.get("numberReady") or 0)
    available = int(status.get("numberAvailable") or 0)
    updated = int(status.get("updatedNumberScheduled") or 0)
    misscheduled = int(status.get("numberMisscheduled") or 0)
    required = max(1, as_int(minimum_replicas))
    if desired <= 0:
        return False, "image-cache daemonset has no scheduled nodes"
    if misscheduled > 0:
        return False, f"image-cache daemonset has {misscheduled} misscheduled pods"
    if desired < required:
        return False, f"image-cache daemonset scheduled below configured minimum: ready={ready} available={available} updated={updated} desired={desired} required={required}"
    if ready < required or available < required:
        return False, f"image-cache daemonset available below configured minimum: ready={ready} available={available} updated={updated} desired={desired} required={required}"
    if ready < desired or available < desired or updated < desired:
        return True, f"image-cache daemonset serves configured minimum with partial convergence: ready={ready} available={available} updated={updated} desired={desired} required={required}"
    return True, f"image-cache daemonset ready: ready={ready} available={available} updated={updated} desired={desired} required={required}"

status_path = sys.argv[1]
nodes_path = sys.argv[2]
dns_nodes_path = sys.argv[3]
node_policies_path = sys.argv[4]
node_local_build_plane_override_allowed = trim(sys.argv[5]).lower() == "true"
node_local_dns_offline_override_allowed = trim(sys.argv[6]).lower() == "true"
image_cache_status_path = sys.argv[7]
cluster_join_registry_endpoint = sys.argv[8]
registry_pull_base = sys.argv[9]
image_store_min_replicas = sys.argv[10]
with open(status_path, "r", encoding="utf-8") as fh:
    payload = json.load(fh)
status = payload.get("status") or {}
with open(nodes_path, "r", encoding="utf-8") as fh:
    nodes_payload = json.load(fh)
nodes = nodes_payload.get("nodes") or []
with open(dns_nodes_path, "r", encoding="utf-8") as fh:
    dns_nodes_payload = json.load(fh)
dns_nodes = dns_nodes_payload.get("nodes") or []
node_policy_statuses = load_node_policy_statuses(node_policies_path)
nodes, retired_edge_nodes = filter_nodes_for_effective_policy(nodes, node_policy_statuses, "effective_edge")
dns_nodes, retired_dns_nodes = filter_nodes_for_effective_policy(dns_nodes, node_policy_statuses, "effective_dns")
checks = {str(item.get("name", "")).strip(): item for item in status.get("checks") or [] if isinstance(item, dict)}
failing_checks = [name for name, check in checks.items() if not check.get("pass", False)]
bootstrap_override, bootstrap_pending, route_bootstrap_pending, route_bootstrap_groups = edge_inventory_healthy(nodes)
dns_bootstrap_override, dns_bootstrap_pending = dns_inventory_bootstrap_healthy(dns_nodes, set(filter(None, route_bootstrap_groups)))
image_cache_override, image_cache_message = image_cache_daemonset_ready(image_cache_status_path, cluster_join_registry_endpoint, registry_pull_base, image_store_min_replicas)
changed_files = {trim(line) for line in os.environ.get("FUGUE_RELEASE_CHANGED_FILES", "").splitlines() if trim(line)}
edge_control_plane_repair_files = {
    "internal/api/dns_nodes_test.go",
    "internal/api/edge_nodes.go",
    "internal/api/edge_nodes_test.go",
    "internal/bundleauth/bundleauth.go",
    "internal/bundleauth/bundleauth_test.go",
    "scripts/test_release_domain_safety.sh",
    "scripts/upgrade_fugue_control_plane.sh",
}
edge_control_plane_repair_override = bool(changed_files) and changed_files.issubset(edge_control_plane_repair_files)
store = status.get("control_plane_store") or {}
if store.get("block_rollout", False):
    raise SystemExit("control-plane store promotion gate is blocked")
if str(store.get("permission_verification_status", "")).strip() != "passed":
    raise SystemExit("control-plane store permission verification did not pass")
allowed_checks = {"edge", "dns"}
if node_local_dns_offline_override_allowed and set(failing_checks) == {"node_policy"}:
    print("release preflight offline-preserve NodeLocal DNSCache override: exact preserved blocker and active cohort policy gate passed")
elif set(failing_checks).issubset(allowed_checks) and bootstrap_override and (not any(name == "dns" for name in failing_checks) or dns_bootstrap_override):
    pending = ", ".join(sorted(filter(None, bootstrap_pending))) or "<none>"
    route_pending = ", ".join(sorted(filter(None, route_bootstrap_pending))) or "<none>"
    dns_pending = ", ".join(sorted(filter(None, dns_bootstrap_pending))) or "<none>"
    retired_note = ""
    if retired_edge_nodes or retired_dns_nodes:
        retired_edge = ", ".join(sorted(set(filter(None, retired_edge_nodes)))) or "<none>"
        retired_dns = ", ".join(sorted(set(filter(None, retired_dns_nodes)))) or "<none>"
        retired_note = f"; node-policy-retired edge nodes ignored: {retired_edge}; node-policy-retired DNS nodes ignored: {retired_dns}"
    print(f"release preflight serviceability override: edge inventory is serving or bootstrap-safe; dns inventory is serving or bootstrap-safe; edge bootstrap nodes {pending}; route-bootstrap nodes {route_pending}; dns bootstrap nodes {dns_pending}{retired_note}; continuing with explicit rollout")
elif node_local_build_plane_override_allowed and set(failing_checks).issubset({"registry", "node_policy"}):
    registry_message = trim((checks.get("registry") or {}).get("message"))
    node_policy_message = trim((checks.get("node_policy") or {}).get("message"))
    detail = "; ".join(item for item in [registry_message, node_policy_message] if item)
    if detail:
        detail = f": {detail}"
    print(f"release preflight node-local build-plane override: allowing rollout despite registry/node_policy gates{detail}")
elif set(failing_checks).issubset({"registry"}) and image_cache_override:
    registry_message = trim((checks.get("registry") or {}).get("message"))
    detail = f": {registry_message}" if registry_message else ""
    print(f"release preflight node-local image-cache override: allowing rollout despite legacy registry gate{detail}; {image_cache_message}")
elif set(failing_checks).issubset({"edge"}) and edge_control_plane_repair_override:
    edge_message = trim((checks.get("edge") or {}).get("message"))
    detail = f": {edge_message}" if edge_message else ""
    print(f"release preflight edge control-plane repair override: allowing rollout despite edge gate{detail}; changed_files={','.join(sorted(changed_files))}")
else:
    if not status.get("pass", False):
        raise SystemExit("platform autonomy status did not pass")
    if status.get("block_rollout", False):
        raise SystemExit("platform autonomy status blocks rollout")
    for name in ("discovery_bundle", "node_policy", "edge", "dns", "registry", "headscale", "restore_readiness", "route_fallback"):
        check = checks.get(name)
        if not check or not check.get("pass", False):
            raise SystemExit(f"{name} gate did not pass")
    if failing_checks:
        raise SystemExit("platform autonomy status did not pass")
PY
  )"; then
    rm -f "${autonomy_status_file}" "${edge_nodes_file}" "${dns_nodes_file}" "${node_policies_file}" "${image_cache_status_file}"
    return 1
  fi
  if [[ -n "$(trim_field "${autonomy_override_message}")" ]]; then
    log "${autonomy_override_message}"
    if [[ "${autonomy_override_message}" == release\ preflight\ node-local\ build-plane\ override:* ]]; then
      NODE_LOCAL_BUILD_PLANE_PREFLIGHT_OVERRIDE_USED="true"
    fi
  fi
  if [[ -n "$(trim_field "${node_local_dns_offline_override_message}")" ]]; then
    log "${node_local_dns_offline_override_message}"
  fi

  log "release preflight passed for ${api_base}; discovery_etag=${discovery_etag}"
  rm -f "${discovery_headers}" "${discovery_body}" "${autonomy_status_file}"
  rm -f "${edge_nodes_file}" "${dns_nodes_file}" "${node_policies_file}" "${image_cache_status_file}"
}

fetch_discovery_bundle() {
  local source_file="${FUGUE_DISCOVERY_BUNDLE_FILE:-}"
  local source_url="${FUGUE_DISCOVERY_BUNDLE_URL:-}"
  local base_url=""
  local auth_args=()

  if [[ -n "${DISCOVERY_BUNDLE_FILE}" && -r "${DISCOVERY_BUNDLE_FILE}" ]]; then
    return 0
  fi

  if [[ -n "$(trim_field "${source_file}")" ]]; then
    [[ -r "${source_file}" ]] || fail "FUGUE_DISCOVERY_BUNDLE_FILE is not readable: ${source_file}"
    DISCOVERY_BUNDLE_FILE="${source_file}"
    DISCOVERY_BUNDLE_FILE_TEMP=""
    verify_discovery_bundle_file "${DISCOVERY_BUNDLE_FILE}"
    log "using DiscoveryBundle from ${DISCOVERY_BUNDLE_FILE}"
    return 0
  fi

  if [[ -z "$(trim_field "${source_url}")" ]]; then
    base_url="$(trim_field "${FUGUE_BASE_URL:-${FUGUE_API_URL:-}}")"
    if [[ -n "${base_url}" ]]; then
      source_url="${base_url%/}/v1/discovery/bundle"
    fi
  fi
  if [[ -z "$(trim_field "${source_url}")" ]]; then
    return 1
  fi

  DISCOVERY_BUNDLE_FILE="$(mktemp -t fugue-discovery-bundle.XXXXXX.json)"
  DISCOVERY_BUNDLE_FILE_TEMP="${DISCOVERY_BUNDLE_FILE}"
  if [[ -n "$(trim_field "${FUGUE_API_KEY:-${FUGUE_TOKEN:-${FUGUE_BOOTSTRAP_KEY:-}}}")" ]]; then
    auth_args=(-H "Authorization: Bearer ${FUGUE_API_KEY:-${FUGUE_TOKEN:-${FUGUE_BOOTSTRAP_KEY:-}}}")
  fi
  curl -fsS "${auth_args[@]}" "${source_url}" -o "${DISCOVERY_BUNDLE_FILE}"
  verify_discovery_bundle_file "${DISCOVERY_BUNDLE_FILE}"
  log "fetched DiscoveryBundle from ${source_url}"
  return 0
}

verify_discovery_bundle_file() {
  local bundle_file="$1"
  [[ -r "${bundle_file}" ]] || fail "DiscoveryBundle is not readable: ${bundle_file}"
  command_exists python3 || fail "python3 is required to verify DiscoveryBundle"
  python3 - "${bundle_file}" <<'PY'
import base64
import hashlib
import hmac
import json
import os
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as fh:
    bundle = json.load(fh)

schema_version = str(bundle.get("schema_version", "")).strip()
if not schema_version:
    raise SystemExit("DiscoveryBundle schema_version is empty")
if schema_version.split(".", 1)[0] != "1":
    raise SystemExit(f"unsupported DiscoveryBundle schema_version: {schema_version}")
if not str(bundle.get("signature", "")).strip():
    raise SystemExit("DiscoveryBundle signature is empty")

def payload_for_signature(key_id, valid_until):
    payload = {}
    for key in (
        "schema_version",
        "generation",
        "previous_generation",
        "generated_at",
        "valid_until",
        "issuer",
        "key_id",
        "api_endpoints",
        "kubernetes",
        "registry",
        "edge_groups",
        "edge_nodes",
        "dns_nodes",
        "platform_routes",
        "public_runtime_env",
    ):
        value = bundle.get(key)
        if key == "valid_until":
            value = valid_until
        elif key == "key_id":
            value = key_id
        if value in ("", None, [], {}):
            continue
        payload[key] = value
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

keys = {}
active_key = os.environ.get("FUGUE_BUNDLE_SIGNING_KEY", "").strip()
active_key_id = os.environ.get("FUGUE_BUNDLE_SIGNING_KEY_ID", bundle.get("key_id", "")).strip()
previous_key = os.environ.get("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY", "").strip()
previous_key_id = os.environ.get("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID", "").strip()
if active_key and active_key_id:
    keys[active_key_id.lower()] = active_key
if previous_key and previous_key_id:
    keys[previous_key_id.lower()] = previous_key
if not keys:
    raise SystemExit(0)
revoked = {item.strip().lower() for item in os.environ.get("FUGUE_BUNDLE_REVOKED_KEY_IDS", "").replace(";", ",").split(",") if item.strip()}
candidates = [(bundle.get("key_id", ""), bundle.get("signature", ""), bundle.get("valid_until", ""))]
for item in bundle.get("signatures") or []:
    candidates.append((item.get("key_id", ""), item.get("signature", ""), item.get("valid_until", bundle.get("valid_until", ""))))
for key_id, signature, valid_until in candidates:
    key_id = str(key_id or "").strip()
    signature = str(signature or "").strip()
    if not key_id or not signature or key_id.lower() in revoked:
        continue
    key = keys.get(key_id.lower())
    if not key:
        continue
    digest = hmac.new(key.encode("utf-8"), payload_for_signature(key_id, valid_until), hashlib.sha256).digest()
    expected = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    if hmac.compare_digest(signature, expected):
        raise SystemExit(0)
raise SystemExit("DiscoveryBundle signature verification failed")
PY
}

discovery_bundle_value() {
  local key="$1"
  [[ -n "${DISCOVERY_BUNDLE_FILE}" && -r "${DISCOVERY_BUNDLE_FILE}" ]] || return 1
  command_exists python3 || fail "python3 is required to read DiscoveryBundle; install python3 or pass explicit release variables"
  python3 - "${DISCOVERY_BUNDLE_FILE}" "${key}" <<'PY'
import json
import sys

path, key = sys.argv[1], sys.argv[2]
with open(path, "r", encoding="utf-8") as fh:
    bundle = json.load(fh)

def first_named(items, name):
    for item in items or []:
        if item.get("name") == name:
            return item
    return (items or [{}])[0] if items else {}

runtime = bundle.get("public_runtime_env") or {}
if key == "api_url":
    value = runtime.get("FUGUE_API_URL") or first_named(bundle.get("api_endpoints"), "public").get("url", "")
elif key == "app_base_domain":
    value = runtime.get("FUGUE_APP_BASE_DOMAIN", "")
elif key == "registry_push_base":
    value = runtime.get("FUGUE_REGISTRY_PUSH_BASE") or first_named(bundle.get("registry"), "registry").get("push_base", "")
elif key == "registry_pull_base":
    value = runtime.get("FUGUE_REGISTRY_PULL_BASE") or first_named(bundle.get("registry"), "registry").get("pull_base", "")
elif key == "registry_mirror":
    value = runtime.get("FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT") or first_named(bundle.get("registry"), "registry").get("mirror", "")
elif key == "cluster_join_server_fallbacks":
    value = runtime.get("FUGUE_CLUSTER_JOIN_SERVER_FALLBACKS", "")
    if not value:
        value = ",".join(first_named(bundle.get("kubernetes"), "cluster-join").get("fallback_servers") or [])
else:
    value = ""
if isinstance(value, list):
    value = ",".join(str(item) for item in value if str(item).strip())
print(str(value).strip())
PY
}

apply_discovery_bundle_defaults() {
  local value=""
  if ! fetch_discovery_bundle; then
    return 1
  fi

  if [[ -z "$(trim_field "${FUGUE_API_PUBLIC_DOMAIN:-}")" ]]; then
    value="$(discovery_bundle_value api_url || true)"
    if [[ "${value}" == https://* ]]; then
      FUGUE_API_PUBLIC_DOMAIN="${value#https://}"
      FUGUE_API_PUBLIC_DOMAIN="${FUGUE_API_PUBLIC_DOMAIN%%/*}"
    fi
  fi
  if [[ -z "$(trim_field "${FUGUE_APP_BASE_DOMAIN:-}")" ]]; then
    value="$(discovery_bundle_value app_base_domain || true)"
    [[ -z "$(trim_field "${value}")" ]] || FUGUE_APP_BASE_DOMAIN="${value}"
  fi
  if [[ -z "$(trim_field "${FUGUE_REGISTRY_PUSH_BASE:-}")" ]]; then
    value="$(discovery_bundle_value registry_push_base || true)"
    [[ -z "$(trim_field "${value}")" ]] || FUGUE_REGISTRY_PUSH_BASE="${value}"
  fi
  if [[ -z "$(trim_field "${FUGUE_REGISTRY_PULL_BASE:-}")" ]]; then
    value="$(discovery_bundle_value registry_pull_base || true)"
    [[ -z "$(trim_field "${value}")" ]] || FUGUE_REGISTRY_PULL_BASE="${value}"
  fi
  if [[ -z "$(trim_field "${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT:-}")" ]]; then
    value="$(discovery_bundle_value registry_mirror || true)"
    [[ -z "$(trim_field "${value}")" ]] || FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT="${value}"
  fi
  if [[ -z "$(trim_field "${FUGUE_CONTROL_PLANE_KUBE_API_FALLBACK_SERVERS:-}")" ]]; then
    value="$(discovery_bundle_value cluster_join_server_fallbacks || true)"
    [[ -z "$(trim_field "${value}")" ]] || FUGUE_CONTROL_PLANE_KUBE_API_FALLBACK_SERVERS="${value}"
  fi
  if [[ -z "$(trim_field "${FUGUE_CLUSTER_JOIN_SERVER_FALLBACKS:-}")" ]]; then
    value="$(discovery_bundle_value cluster_join_server_fallbacks || true)"
    [[ -z "$(trim_field "${value}")" ]] || FUGUE_CLUSTER_JOIN_SERVER_FALLBACKS="${value}"
  fi
  if [[ -z "$(trim_field "${FUGUE_SMOKE_URL:-}")" ]]; then
    value="$(discovery_bundle_value api_url || true)"
    [[ -z "$(trim_field "${value}")" ]] || FUGUE_SMOKE_URL="${value%/}/healthz"
  fi
  return 0
}

selector_yaml() {
  local raw="$1"
  local indent="$2"
  local entry key value

  raw="${raw//;/$'\n'}"
  raw="${raw//,/$'\n'}"
  while IFS= read -r entry; do
    entry="$(trim_field "${entry}")"
    if [[ -z "${entry}" ]]; then
      continue
    fi
    if [[ "${entry}" != *"="* ]]; then
      fail "node selector entry ${entry} must be key=value"
    fi
    key="$(trim_field "${entry%%=*}")"
    value="$(trim_field "${entry#*=}")"
    if [[ -z "${key}" || -z "${value}" ]]; then
      fail "node selector entry ${entry} must be key=value"
    fi
    printf '%s%s: %s\n' "${indent}" "${key}" "$(yaml_quote "${value}")"
  done
}

append_control_plane_singleton_values() {
  if [[ "${FUGUE_CONTROL_PLANE_SINGLETONS_ENABLED}" != "true" ]]; then
    return 0
  fi

  cat >>"${UPGRADE_OVERRIDE_VALUES_FILE}" <<EOF

registry:
  nodeSelector: null
  controlPlaneSingletonNodeSelector:
$(selector_yaml "${FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR}" "    ")
postgres:
  nodeSelector: null
  controlPlaneSingletonNodeSelector:
$(selector_yaml "${FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR}" "    ")
sharedWorkspaceStorage:
  server:
    nodeSelector: null
    controlPlaneSingletonNodeSelector:
$(selector_yaml "${FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR}" "      ")
  provisioner:
    nodeSelector: null
    controlPlaneSingletonNodeSelector:
$(selector_yaml "${FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR}" "      ")
EOF
}

live_deployment_replicas() {
  local deployment_name="$1"

  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get "deploy/${deployment_name}" -o jsonpath='{.spec.replicas}' 2>/dev/null || true
}

prepare_helm_post_renderer() {
  local registry_replicas=""

  HELM_POST_RENDERER_FILE=""
  HELM_POST_RENDERER_ARGS=()
  PRESERVE_REGISTRY_ZERO_REPLICAS="false"
  if [[ "${NODE_LOCAL_BUILD_PLANE_PREFLIGHT_OVERRIDE_USED}" != "true" ]]; then
    return 0
  fi
  registry_replicas="$(trim_field "$(live_deployment_replicas "${FUGUE_REGISTRY_DEPLOYMENT_NAME}")")"
  if [[ "${registry_replicas}" != "0" ]]; then
    return 0
  fi
  if stateful_dependency_changed; then
    return 0
  fi

  HELM_POST_RENDERER_FILE="$(mktemp -t fugue-helm-post-renderer.XXXXXX.py)"
  cat >"${HELM_POST_RENDERER_FILE}" <<'PY'
#!/usr/bin/env python3
import os
import sys

target_name = os.environ.get("FUGUE_HELM_POST_RENDERER_REGISTRY_DEPLOYMENT", "").strip()
if not target_name:
    sys.stdout.write(sys.stdin.read())
    raise SystemExit(0)

target = f"  name: {target_name}"
lines = sys.stdin.read().splitlines()
out = []
in_doc = False
in_spec = False
doc_is_target = False
doc_is_deployment = False
spec_indent = None
replicas_set = False

def target_doc():
    return doc_is_deployment and doc_is_target

def flush_missing_replicas():
    global in_spec, replicas_set
    if in_spec and target_doc() and not replicas_set:
        out.append("  replicas: 0")
    in_spec = False
    replicas_set = False

for line in lines:
    if line == "---":
        flush_missing_replicas()
        in_doc = False
        doc_is_target = False
        doc_is_deployment = False
        spec_indent = None
        out.append(line)
        continue

    if not in_doc and line.strip():
        in_doc = True
    if in_doc and line == target:
        doc_is_target = True
    if in_doc and line == "kind: Deployment":
        doc_is_deployment = True

    if target_doc():
        stripped = line.strip()
        indent = len(line) - len(line.lstrip(" "))
        if not in_spec and line == "spec:":
            in_spec = True
            spec_indent = indent
            replicas_set = False
        elif in_spec and indent <= spec_indent and stripped and not line.startswith(" "):
            flush_missing_replicas()
        elif in_spec and indent == spec_indent + 2 and stripped.startswith("replicas:"):
            out.append("  replicas: 0")
            replicas_set = True
            continue

    out.append(line)

flush_missing_replicas()
sys.stdout.write("\n".join(out))
if lines:
    sys.stdout.write("\n")
PY
  chmod 0700 "${HELM_POST_RENDERER_FILE}"
  export FUGUE_HELM_POST_RENDERER_REGISTRY_DEPLOYMENT="${FUGUE_REGISTRY_DEPLOYMENT_NAME}"
  HELM_POST_RENDERER_ARGS=(--post-renderer "${HELM_POST_RENDERER_FILE}")
  PRESERVE_REGISTRY_ZERO_REPLICAS="true"
  log "preserving ${FUGUE_REGISTRY_DEPLOYMENT_NAME} at replicas=0 during this Helm upgrade"
}

deployment_node_selector_pairs() {
  local deployment_name="$1"
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get deploy "${deployment_name}" \
    -o go-template='{{range $key, $value := .spec.template.spec.nodeSelector}}{{printf "%s=%s\n" $key $value}}{{end}}' 2>/dev/null || true
}

deployment_host_path_volume_path() {
  local deployment_name="$1"
  local volume_name="$2"
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get deploy "${deployment_name}" \
    -o go-template='{{range .spec.template.spec.volumes}}{{if eq .name "'"${volume_name}"'"}}{{if .hostPath}}{{.hostPath.path}}{{end}}{{end}}{{end}}' 2>/dev/null || true
}

append_registry_upgrade_values() {
  local existing_host_path=""

  existing_host_path="$(trim_field "$(deployment_host_path_volume_path "${FUGUE_REGISTRY_DEPLOYMENT_NAME}" "registry-data")")"
  if [[ -z "${existing_host_path}" ]]; then
    return 0
  fi

  cat >>"${UPGRADE_OVERRIDE_VALUES_FILE}" <<EOF

registry:
  persistence:
    mode: hostPath
    hostPath: $(yaml_quote "${existing_host_path}")
  unsafeHostPath:
    enabled: true
    reason: "preserve existing live registry hostPath until explicit migration"
EOF
  log "preserving registry hostPath storage at ${existing_host_path}; migrate explicitly before switching registry.persistence.mode"
}

append_headscale_upgrade_values() {
  local existing_host_path=""
  local existing_selector=""
  local selector_summary=""
  local wrote_block="false"

  HEADSCALE_HELM_SET_ARGS=()
  existing_host_path="$(trim_field "$(deployment_host_path_volume_path "${FUGUE_HEADSCALE_DEPLOYMENT_NAME}" "headscale-data")")"
  existing_selector="$(deployment_node_selector_pairs "${FUGUE_HEADSCALE_DEPLOYMENT_NAME}")"

  if [[ -z "${existing_host_path}" && "${FUGUE_CONTROL_PLANE_SINGLETONS_ENABLED}" != "true" && -z "$(trim_field "${existing_selector}")" ]]; then
    return 0
  fi

  printf '\nheadscale:\n' >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  wrote_block="true"

  if [[ -n "${existing_host_path}" ]]; then
    cat >>"${UPGRADE_OVERRIDE_VALUES_FILE}" <<EOF
  persistence:
    mode: hostPath
    hostPath: $(yaml_quote "${existing_host_path}")
EOF
    HEADSCALE_HELM_SET_ARGS+=(
      --set-string "headscale.persistence.mode=hostPath"
      --set-string "headscale.persistence.hostPath=${existing_host_path}"
    )
  fi

  if [[ "${FUGUE_CONTROL_PLANE_SINGLETONS_ENABLED}" == "true" ]]; then
    cat >>"${UPGRADE_OVERRIDE_VALUES_FILE}" <<EOF
  nodeSelector: null
  controlPlaneSingletonNodeSelector:
$(selector_yaml "${FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR}" "    ")
EOF
    HEADSCALE_HELM_SET_ARGS+=(
      --set-json "headscale.controlPlaneSingletonNodeSelector=$(selector_json "${FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR}")"
    )
  elif [[ -n "$(trim_field "${existing_selector}")" ]]; then
    cat >>"${UPGRADE_OVERRIDE_VALUES_FILE}" <<EOF
  nodeSelector:
$(selector_yaml "${existing_selector}" "    ")
EOF
    HEADSCALE_HELM_SET_ARGS+=(
      --set-json "headscale.nodeSelector=$(selector_json "${existing_selector}")"
    )
  fi

  if [[ "${wrote_block}" == "true" && -n "${existing_host_path}" && "${FUGUE_CONTROL_PLANE_SINGLETONS_ENABLED}" != "true" && -z "$(trim_field "${existing_selector}")" ]]; then
    fail "existing ${FUGUE_HEADSCALE_DEPLOYMENT_NAME} uses hostPath storage at ${existing_host_path} but has no nodeSelector; set FUGUE_CONTROL_PLANE_SINGLETONS_ENABLED=true with FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR or add a stable headscale.nodeSelector before upgrading"
  fi

  if [[ -n "${existing_host_path}" ]]; then
    selector_summary="$(printf '%s' "${existing_selector}" | tr '\n' ',' | sed 's/,$//')"
    if [[ "${FUGUE_CONTROL_PLANE_SINGLETONS_ENABLED}" == "true" ]]; then
      selector_summary="${FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR}"
    fi
    log "preserving headscale hostPath storage at ${existing_host_path} with nodeSelector ${selector_summary}"
  fi
}

selector_json() {
  local raw="$1"
  local entry key value first=1

  raw="${raw//;/$'\n'}"
  raw="${raw//,/$'\n'}"
  printf '{'
  while IFS= read -r entry; do
    entry="$(trim_field "${entry}")"
    if [[ -z "${entry}" ]]; then
      continue
    fi
    if [[ "${entry}" != *"="* ]]; then
      fail "node selector entry ${entry} must be key=value"
    fi
    key="$(trim_field "${entry%%=*}")"
    value="$(trim_field "${entry#*=}")"
    if [[ -z "${key}" || -z "${value}" ]]; then
      fail "node selector entry ${entry} must be key=value"
    fi
    if [[ "${first}" -eq 0 ]]; then
      printf ','
    fi
    first=0
    printf '%s:%s' "$(yaml_quote "${key}")" "$(yaml_quote "${value}")"
  done <<<"${raw}"
  printf '}'
}

patch_singleton_deployment_node_selector() {
  local deployment_name="$1"
  local selector_json_value="${2}"
  local patch

  if ! ${KUBECTL} -n "${FUGUE_NAMESPACE}" get deploy "${deployment_name}" >/dev/null 2>&1; then
    return 0
  fi

  patch="$(printf '[{"op":"replace","path":"/spec/template/spec/nodeSelector","value":%s}]' "${selector_json_value}")"
  if ${KUBECTL} -n "${FUGUE_NAMESPACE}" patch deploy "${deployment_name}" --type=json -p "${patch}" >/dev/null 2>&1; then
    log "patched ${deployment_name} singleton nodeSelector"
    return 0
  fi

  patch="$(printf '[{"op":"add","path":"/spec/template/spec/nodeSelector","value":%s}]' "${selector_json_value}")"
  ${KUBECTL} -n "${FUGUE_NAMESPACE}" patch deploy "${deployment_name}" --type=json -p "${patch}" >/dev/null
  log "added ${deployment_name} singleton nodeSelector"
}

patch_control_plane_singleton_deployments() {
  if [[ "${FUGUE_CONTROL_PLANE_SINGLETONS_ENABLED}" != "true" ]]; then
    return 0
  fi

  local selector_json_value deployment_name
  selector_json_value="$(selector_json "${FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR}")"
  for deployment_name in "${FUGUE_REGISTRY_DEPLOYMENT_NAME}" "${FUGUE_HEADSCALE_DEPLOYMENT_NAME}" "${FUGUE_POSTGRES_DEPLOYMENT_NAME}" "${FUGUE_SHARED_WORKSPACE_NFS_DEPLOYMENT_NAME}" "${FUGUE_SHARED_WORKSPACE_PROVISIONER_DEPLOYMENT_NAME}"; do
    patch_singleton_deployment_node_selector "${deployment_name}" "${selector_json_value}"
  done
}

node_selector_country_yaml() {
  local country_code="$1"
  local indent="$2"

  country_code="$(trim_field "${country_code}")"
  if [[ -z "${country_code}" ]]; then
    return 0
  fi
  printf '%sfugue.io/location-country-code: %s\n' "${indent}" "$(yaml_quote "${country_code}")"
}

edge_extra_groups_yaml() {
  local raw="${FUGUE_EDGE_EXTRA_GROUPS:-}"
  local entry name edge_group country_code token_secret edge_region edge_country edge_public_ipv4 edge_public_ipv6 edge_mesh_ip

  raw="${raw//;/$'\n'}"
  if [[ -z "$(trim_field "${raw}")" ]]; then
    return 0
  fi

  printf '  groups:\n'
  while IFS= read -r entry; do
    entry="$(trim_field "${entry}")"
    if [[ -z "${entry}" ]]; then
      continue
    fi
    IFS='|' read -r name edge_group country_code token_secret edge_region edge_country edge_public_ipv4 edge_public_ipv6 edge_mesh_ip _ <<<"${entry}"
    name="$(trim_field "${name}")"
    edge_group="$(trim_field "${edge_group}")"
    country_code="$(trim_field "${country_code}")"
    token_secret="$(trim_field "${token_secret}")"
    edge_region="$(trim_field "${edge_region}")"
    edge_country="$(trim_field "${edge_country}")"
    edge_public_ipv4="$(trim_field "${edge_public_ipv4}")"
    edge_public_ipv6="$(trim_field "${edge_public_ipv6}")"
    edge_mesh_ip="$(trim_field "${edge_mesh_ip}")"
    if [[ -z "${name}" || -z "${edge_group}" || -z "${country_code}" || -z "${token_secret}" ]]; then
      fail "FUGUE_EDGE_EXTRA_GROUPS entries must be name|edge_group_id|country_code|token_secret_name[|region|country|public_ipv4|public_ipv6|mesh_ip]"
    fi
    if [[ -z "${edge_country}" ]]; then
      edge_country="${country_code}"
    fi
    printf '    - name: %s\n' "$(yaml_quote "${name}")"
    printf '      edgeGroupID: %s\n' "$(yaml_quote "${edge_group}")"
    printf '      tokenSecret:\n'
    printf '        name: %s\n' "$(yaml_quote "${token_secret}")"
    printf '        key: "FUGUE_EDGE_TOKEN"\n'
    printf '      nodeSelector:\n'
    printf '        fugue.io/role.edge: "true"\n'
    printf '        fugue.io/schedulable: "true"\n'
    printf '        fugue.io/location-country-code: %s\n' "$(yaml_quote "${country_code}")"
    if [[ -n "${edge_region}" || -n "${edge_country}" || -n "${edge_public_ipv4}" || -n "${edge_public_ipv6}" || -n "${edge_mesh_ip}" ]]; then
      printf '      extraEnv:\n'
      if [[ -n "${edge_region}" ]]; then
        printf '        - name: FUGUE_EDGE_REGION\n'
        printf '          value: %s\n' "$(yaml_quote "${edge_region}")"
      fi
      if [[ -n "${edge_country}" ]]; then
        printf '        - name: FUGUE_EDGE_COUNTRY\n'
        printf '          value: %s\n' "$(yaml_quote "${edge_country}")"
      fi
      if [[ -n "${edge_public_ipv4}" ]]; then
        printf '        - name: FUGUE_EDGE_PUBLIC_IPV4\n'
        printf '          value: %s\n' "$(yaml_quote "${edge_public_ipv4}")"
      fi
      if [[ -n "${edge_public_ipv6}" ]]; then
        printf '        - name: FUGUE_EDGE_PUBLIC_IPV6\n'
        printf '          value: %s\n' "$(yaml_quote "${edge_public_ipv6}")"
      fi
      if [[ -n "${edge_mesh_ip}" ]]; then
        printf '        - name: FUGUE_EDGE_MESH_IP\n'
        printf '          value: %s\n' "$(yaml_quote "${edge_mesh_ip}")"
      fi
    fi
  done <<<"${raw}"
}

dns_extra_groups_yaml() {
  local raw="${FUGUE_DNS_EXTRA_GROUPS:-}"
  local entry name edge_group country_code answer_ips token_secret host_ip answer_ip

  raw="${raw//;/$'\n'}"
  if [[ -z "$(trim_field "${raw}")" ]]; then
    return 0
  fi

  printf '  groups:\n'
  while IFS= read -r entry; do
    entry="$(trim_field "${entry}")"
    if [[ -z "${entry}" ]]; then
      continue
    fi
    IFS='|' read -r name edge_group country_code answer_ips token_secret host_ip _ <<<"${entry}"
    name="$(trim_field "${name}")"
    edge_group="$(trim_field "${edge_group}")"
    country_code="$(trim_field "${country_code}")"
    answer_ips="$(trim_field "${answer_ips}")"
    token_secret="$(trim_field "${token_secret}")"
    host_ip="$(trim_field "${host_ip}")"
    if [[ -z "${name}" || -z "${edge_group}" || -z "${country_code}" || -z "${answer_ips}" || -z "${token_secret}" ]]; then
      fail "FUGUE_DNS_EXTRA_GROUPS entries must be name|edge_group_id|country_code|answer_ips|token_secret_name[|host_ip]"
    fi
    if [[ "$(dns_answer_ip_count "${answer_ips}")" == "0" ]]; then
      fail "FUGUE_DNS_EXTRA_GROUPS entry ${name} must contain at least one answer IP"
    fi
    if [[ "${FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED}" == "true" ]]; then
      if [[ -z "${host_ip}" && "$(dns_answer_ip_count "${answer_ips}")" == "1" ]]; then
        host_ip="$(dns_answer_ips_lines "${answer_ips}")"
      fi
      [[ -n "${host_ip}" ]] || fail "FUGUE_DNS_EXTRA_GROUPS entry ${name} requires an explicit host IP when it has multiple answers and public host ports are enabled"
      if ! grep -Fqx "${host_ip}" < <(dns_answer_ips_lines "${answer_ips}"); then
        fail "FUGUE_DNS_EXTRA_GROUPS entry ${name} host IP must be one of its authoritative answer IPs"
      fi
    fi
    printf '    - name: %s\n' "$(yaml_quote "${name}")"
    printf '      edgeGroupID: %s\n' "$(yaml_quote "${edge_group}")"
    printf '      tokenSecret:\n'
    printf '        name: %s\n' "$(yaml_quote "${token_secret}")"
    printf '        key: "FUGUE_EDGE_TOKEN"\n'
    printf '      nodeSelector:\n'
    printf '        fugue.io/role.dns: "true"\n'
    printf '        fugue.io/schedulable: "true"\n'
    printf '        fugue.io/location-country-code: %s\n' "$(yaml_quote "${country_code}")"
    if [[ "${FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED}" == "true" ]]; then
      printf '      publicHostPorts:\n'
      printf '        hostIP: %s\n' "$(yaml_quote "${host_ip}")"
    fi
    printf '      answerIPs:\n'
    while IFS= read -r answer_ip; do
      printf '        - %s\n' "$(yaml_quote "${answer_ip}")"
    done < <(dns_answer_ips_lines "${answer_ips}")
  done <<<"${raw}"
}

validate_dns_public_host_port_target() {
  local display_name="$1"
  local country_code="$2"
  local host_ip="$3"
  local selector=""
  local nodes_json=""
  local node_name=""

  [[ -n "$(trim_field "${country_code}")" ]] || fail "${display_name} requires a DNS node selector country code"
  [[ -n "$(trim_field "${host_ip}")" ]] || fail "${display_name} requires a public DNS host IP"
  selector="fugue.io/role.dns=true,fugue.io/schedulable=true,fugue.io/location-country-code=${country_code}"
  if ! nodes_json="$(${KUBECTL} get nodes -l "${selector}" -o json)"; then
    fail "cannot inspect the Kubernetes node selected by ${display_name}"
  fi
  if ! node_name="$(printf '%s' "${nodes_json}" | python3 -c '
import ipaddress
import json
import sys

display_name, host_ip = sys.argv[1:]
try:
    expected_ip = ipaddress.ip_address(host_ip)
except ValueError as exc:
    raise SystemExit(f"{display_name} host IP is invalid: {exc}")
if expected_ip.version != 4:
    raise SystemExit(f"{display_name} host IP must be IPv4")

payload = json.load(sys.stdin)
nodes = payload.get("items") or []
if len(nodes) != 1:
    raise SystemExit(f"{display_name} selector must match exactly one node; matched {len(nodes)}")
node = nodes[0]
name = (node.get("metadata") or {}).get("name") or "<unknown>"
conditions = (node.get("status") or {}).get("conditions") or []
ready = any(item.get("type") == "Ready" and item.get("status") == "True" for item in conditions)
if not ready:
    raise SystemExit(f"{display_name} selected node {name} is not Ready")
external_ipv4 = set()
for address in (node.get("status") or {}).get("addresses") or []:
    if address.get("type") != "ExternalIP":
        continue
    raw = str(address.get("address") or "").strip()
    try:
        parsed = ipaddress.ip_address(raw)
    except ValueError:
        continue
    if parsed.version == 4:
        external_ipv4.add(str(parsed))
if str(expected_ip) not in external_ipv4:
    rendered = ",".join(sorted(external_ipv4)) or "<none>"
    raise SystemExit(f"{display_name} host IP {expected_ip} is not the ExternalIPv4 of selected node {name}; external IPv4={rendered}")
print(name)
' "${display_name}" "${host_ip}")"; then
    fail "public DNS host-port target validation failed for ${display_name}"
  fi
  printf '%s\n' "${node_name}"
}

validate_dns_public_host_port_targets() {
  local entry=""
  local name=""
  local edge_group=""
  local country_code=""
  local answer_ips=""
  local token_secret=""
  local host_ip=""
  local node_name=""
  local target_key=""
  local raw=""
  local selected_targets=""

  if [[ "${FUGUE_DNS_ENABLED}" != "true" || "${FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED}" != "true" ]]; then
    return 0
  fi
  command_exists python3 || fail "python3 is required for public DNS host-port target validation"

  if ! node_name="$(validate_dns_public_host_port_target "primary DNS" "${FUGUE_DNS_NODE_SELECTOR_COUNTRY_CODE}" "${FUGUE_DNS_PUBLIC_HOST_IP}")"; then
    return 1
  fi
  selected_targets="primary DNS|${node_name}|${FUGUE_DNS_PUBLIC_HOST_IP}"
  log "public DNS host-port target validated: primary DNS node=${node_name} host_ip=${FUGUE_DNS_PUBLIC_HOST_IP}"

  raw="${FUGUE_DNS_EXTRA_GROUPS:-}"
  raw="${raw//;/$'\n'}"
  while IFS= read -r entry; do
    entry="$(trim_field "${entry}")"
    [[ -n "${entry}" ]] || continue
    IFS='|' read -r name edge_group country_code answer_ips token_secret host_ip _ <<<"${entry}"
    name="$(trim_field "${name}")"
    country_code="$(trim_field "${country_code}")"
    answer_ips="$(trim_field "${answer_ips}")"
    host_ip="$(trim_field "${host_ip}")"
    if [[ -z "${host_ip}" && "$(dns_answer_ip_count "${answer_ips}")" == "1" ]]; then
      host_ip="$(dns_answer_ips_lines "${answer_ips}")"
    fi
    if ! node_name="$(validate_dns_public_host_port_target "DNS group ${name}" "${country_code}" "${host_ip}")"; then
      return 1
    fi
    target_key="${node_name}|${host_ip}"
    if grep -Fq "|${target_key}" <<<"${selected_targets}"; then
      fail "multiple public DNS DaemonSets target ${node_name} host IP ${host_ip}; public port 53 cannot be shared"
    fi
    selected_targets+=$'\n'"DNS group ${name}|${target_key}"
    log "public DNS host-port target validated: group=${name} node=${node_name} host_ip=${host_ip}"
  done <<<"${raw}"
}

append_upgrade_edge_dynamic_values() {
  local edge_region edge_country edge_public_hostname edge_public_ipv4 edge_public_ipv6 edge_mesh_ip edge_asset_cache_path edge_asset_cache_max_bytes

  edge_region="$(trim_field "${FUGUE_EDGE_REGION:-}")"
  edge_country="$(trim_field "${FUGUE_EDGE_COUNTRY:-}")"
  edge_public_hostname="$(trim_field "${FUGUE_EDGE_PUBLIC_HOSTNAME:-}")"
  edge_public_ipv4="$(trim_field "${FUGUE_EDGE_PUBLIC_IPV4:-}")"
  edge_public_ipv6="$(trim_field "${FUGUE_EDGE_PUBLIC_IPV6:-}")"
  edge_mesh_ip="$(trim_field "${FUGUE_EDGE_MESH_IP:-}")"
  edge_asset_cache_path="$(trim_field "${FUGUE_EDGE_ASSET_CACHE_PATH:-}")"
  edge_asset_cache_max_bytes="$(trim_field "${FUGUE_EDGE_ASSET_CACHE_MAX_BYTES:-}")"

  if [[ -z "$(trim_field "${FUGUE_EDGE_NODE_SELECTOR_COUNTRY_CODE:-}")" && -z "$(trim_field "${FUGUE_EDGE_EXTRA_GROUPS:-}")" && -z "${edge_region}${edge_country}${edge_public_hostname}${edge_public_ipv4}${edge_public_ipv6}${edge_mesh_ip}${edge_asset_cache_path}${edge_asset_cache_max_bytes}" ]]; then
    return 0
  fi

  cat >>"${UPGRADE_OVERRIDE_VALUES_FILE}" <<EOF

edge:
EOF
  if [[ -n "$(trim_field "${FUGUE_EDGE_TOKEN_SECRET_NAME:-}")" ]]; then
    {
      printf '  tokenSecret:\n'
      printf '    name: %s\n' "$(yaml_quote "${FUGUE_EDGE_TOKEN_SECRET_NAME}")"
      printf '    key: %s\n' "$(yaml_quote "${FUGUE_EDGE_TOKEN_SECRET_KEY}")"
    } >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  fi
  if [[ -n "${edge_region}" ]]; then
    printf '  region: %s\n' "$(yaml_quote "${edge_region}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  fi
  if [[ -n "${edge_country}" ]]; then
    printf '  country: %s\n' "$(yaml_quote "${edge_country}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  fi
  if [[ -n "${edge_public_hostname}" ]]; then
    printf '  publicHostname: %s\n' "$(yaml_quote "${edge_public_hostname}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  fi
  if [[ -n "${edge_public_ipv4}" ]]; then
    printf '  publicIPv4: %s\n' "$(yaml_quote "${edge_public_ipv4}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  fi
  if [[ -n "${edge_public_ipv6}" ]]; then
    printf '  publicIPv6: %s\n' "$(yaml_quote "${edge_public_ipv6}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  fi
  if [[ -n "${edge_mesh_ip}" ]]; then
    printf '  meshIP: %s\n' "$(yaml_quote "${edge_mesh_ip}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  fi
  if [[ -n "${edge_asset_cache_path}${edge_asset_cache_max_bytes}" ]]; then
    printf '  extraEnv:\n' >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
    if [[ -n "${edge_asset_cache_path}" ]]; then
      printf '    - name: FUGUE_EDGE_ASSET_CACHE_PATH\n' >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
      printf '      value: %s\n' "$(yaml_quote "${edge_asset_cache_path}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
    fi
    if [[ -n "${edge_asset_cache_max_bytes}" ]]; then
      printf '    - name: FUGUE_EDGE_ASSET_CACHE_MAX_BYTES\n' >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
      printf '      value: %s\n' "$(yaml_quote "${edge_asset_cache_max_bytes}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
    fi
  fi
  cat >>"${UPGRADE_OVERRIDE_VALUES_FILE}" <<EOF
  nodeSelector:
    fugue.io/role.edge: "true"
    fugue.io/schedulable: "true"
EOF
  node_selector_country_yaml "${FUGUE_EDGE_NODE_SELECTOR_COUNTRY_CODE}" "    " >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  edge_extra_groups_yaml >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  cat >>"${UPGRADE_OVERRIDE_VALUES_FILE}" <<EOF
  dynamic:
    enabled: ${FUGUE_EDGE_DYNAMIC_ENABLED}
EOF
}

dns_answer_ips_lines() {
  local raw="$1"
  local answer_ip
  raw="${raw//;/,}"

  printf '%s\n' "${raw}" | tr ',' '\n' | while IFS= read -r answer_ip; do
    answer_ip="$(printf '%s' "${answer_ip}" | awk '{$1=$1; print}')"
    if [[ -n "${answer_ip}" ]]; then
      printf '%s\n' "${answer_ip}"
    fi
  done
}

dns_answer_ip_count() {
  dns_answer_ips_lines "$1" | awk 'NF > 0 {count++} END {print count + 0}'
}

csv_lines() {
  local raw="$1"
  raw="${raw//;/,}"
  printf '%s\n' "${raw}" | tr ',' '\n' | while IFS= read -r value; do
    value="$(trim_field "${value}")"
    if [[ -n "${value}" ]]; then
      printf '%s\n' "${value}"
    fi
  done
}

append_upgrade_dns_values() {
  local answer_ip
  local extra_zone
  local nameserver
  local rendered_answer_ips=0

  cat >>"${UPGRADE_OVERRIDE_VALUES_FILE}" <<EOF

dns:
  enabled: ${FUGUE_DNS_ENABLED}
EOF

  if [[ "${FUGUE_DNS_ENABLED}" != "true" ]]; then
    return 0
  fi

  if [[ -n "$(trim_field "${FUGUE_DNS_TOKEN_SECRET_NAME:-}")" ]]; then
    {
      printf '  tokenSecret:\n'
      printf '    name: %s\n' "$(yaml_quote "${FUGUE_DNS_TOKEN_SECRET_NAME}")"
      printf '    key: %s\n' "$(yaml_quote "${FUGUE_DNS_TOKEN_SECRET_KEY}")"
    } >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  fi

  cat >>"${UPGRADE_OVERRIDE_VALUES_FILE}" <<'EOF'
  answerIPs:
EOF
  while IFS= read -r answer_ip; do
    printf '    - %s\n' "$(yaml_quote "${answer_ip}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
    rendered_answer_ips=$((rendered_answer_ips + 1))
  done < <(dns_answer_ips_lines "${FUGUE_DNS_ANSWER_IPS}")
  if (( rendered_answer_ips == 0 )); then
    fail "FUGUE_DNS_ANSWER_IPS must contain at least one non-empty IP when FUGUE_DNS_ENABLED=true"
  fi
  if [[ -n "$(trim_field "${FUGUE_DNS_ROUTE_A_ANSWER_IPS:-}")" ]]; then
    printf '  routeAAnswerIPs:\n' >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
    while IFS= read -r answer_ip; do
      printf '    - %s\n' "$(yaml_quote "${answer_ip}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
    done < <(dns_answer_ips_lines "${FUGUE_DNS_ROUTE_A_ANSWER_IPS}")
  fi
  if [[ -n "$(trim_field "${FUGUE_DNS_EXTRA_ZONES:-}")" ]]; then
    printf '  extraZones:\n' >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
    while IFS= read -r extra_zone; do
      printf '    - %s\n' "$(yaml_quote "${extra_zone}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
    done < <(csv_lines "${FUGUE_DNS_EXTRA_ZONES}")
  fi
  if [[ -n "$(trim_field "${FUGUE_DNS_GEOIP_OVERRIDES_JSON:-}")" ]]; then
    printf '  extraEnv:\n' >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
    printf '    - name: FUGUE_DNS_GEOIP_OVERRIDES_JSON\n' >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
    printf '      value: %s\n' "$(yaml_quote "${FUGUE_DNS_GEOIP_OVERRIDES_JSON}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  fi

  {
    printf '  nodeSelector:\n'
    printf '    fugue.io/role.dns: "true"\n'
    printf '    fugue.io/schedulable: "true"\n'
    node_selector_country_yaml "${FUGUE_DNS_NODE_SELECTOR_COUNTRY_CODE}" "    "
    printf '  tolerations:\n'
    printf '    - key: fugue.io/dedicated\n'
    printf '      operator: Equal\n'
    printf '      value: dns\n'
    printf '      effect: NoSchedule\n'
    printf '    - key: fugue.io/dedicated\n'
    printf '      operator: Equal\n'
    printf '      value: edge\n'
    printf '      effect: NoSchedule\n'
    printf '    - key: fugue.io/tenant\n'
    printf '      operator: Exists\n'
    printf '      effect: NoSchedule\n'
    printf '  publicHostPorts:\n'
    printf '    enabled: %s\n' "${FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED}"
    if [[ "${FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED}" == "true" ]]; then
      printf '    hostIP: %s\n' "$(yaml_quote "${FUGUE_DNS_PUBLIC_HOST_IP}")"
    fi
    printf '  udpAddr: %s\n' "$(yaml_quote "${FUGUE_DNS_UDP_ADDR}")"
    printf '  tcpAddr: %s\n' "$(yaml_quote "${FUGUE_DNS_TCP_ADDR}")"
    printf '  zone: %s\n' "$(yaml_quote "${FUGUE_DNS_ZONE}")"
    printf '  ttl: %s\n' "${FUGUE_DNS_TTL}"
  } >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
  if [[ -n "$(trim_field "${FUGUE_DNS_NAMESERVERS:-}")" ]]; then
    printf '  nameservers:\n' >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
    while IFS= read -r nameserver; do
      printf '    - %s\n' "$(yaml_quote "${nameserver}")" >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
    done < <(csv_lines "${FUGUE_DNS_NAMESERVERS}")
  fi
  dns_extra_groups_yaml >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
}

append_upgrade_mesh_recovery_values() {
  cat >>"${UPGRADE_OVERRIDE_VALUES_FILE}" <<EOF

meshRecovery:
  enabled: ${FUGUE_MESH_RECOVERY_ENABLED}
EOF

  if [[ "${FUGUE_MESH_RECOVERY_ENABLED}" != "true" ]]; then
    return 0
  fi

  cat >>"${UPGRADE_OVERRIDE_VALUES_FILE}" <<EOF
  listenAddr: $(yaml_quote "${FUGUE_MESH_RECOVERY_LISTEN_ADDR}")
  generation: $(yaml_quote "${FUGUE_MESH_RECOVERY_GENERATION}")
  previousGeneration: $(yaml_quote "${FUGUE_MESH_RECOVERY_PREVIOUS_GENERATION}")
  mode: $(yaml_quote "${FUGUE_MESH_RECOVERY_MODE}")
  loginServer: $(yaml_quote "${FUGUE_MESH_RECOVERY_LOGIN_SERVER}")
  message: $(yaml_quote "${FUGUE_MESH_RECOVERY_MESSAGE}")
  directoryValidFor: $(yaml_quote "${FUGUE_MESH_RECOVERY_DIRECTORY_VALID_FOR}")
  manifestValidFor: $(yaml_quote "${FUGUE_MESH_RECOVERY_MANIFEST_VALID_FOR}")
  nodeTTL: $(yaml_quote "${FUGUE_MESH_RECOVERY_NODE_TTL}")
  signingKeyID: $(yaml_quote "${FUGUE_MESH_RECOVERY_SIGNING_KEY_ID}")
  tokenSecret:
    name: $(yaml_quote "${FUGUE_MESH_RECOVERY_TOKEN_SECRET_NAME}")
    key: $(yaml_quote "${FUGUE_MESH_RECOVERY_TOKEN_SECRET_KEY}")
  signingKeySecret:
    name: $(yaml_quote "${FUGUE_MESH_RECOVERY_SIGNING_KEY_SECRET_NAME}")
    key: $(yaml_quote "${FUGUE_MESH_RECOVERY_SIGNING_KEY_SECRET_KEY}")
  rejoinAuthKeySecret:
    name: $(yaml_quote "${FUGUE_MESH_RECOVERY_REJOIN_AUTH_KEY_SECRET_NAME}")
    key: $(yaml_quote "${FUGUE_MESH_RECOVERY_REJOIN_AUTH_KEY_SECRET_KEY}")
    optional: ${FUGUE_MESH_RECOVERY_REJOIN_AUTH_KEY_SECRET_OPTIONAL}
EOF
}

build_dns_helm_set_args() {
  local answer_ip
  local extra_zone
  local nameserver
  local index=0

  DNS_HELM_SET_ARGS=(
    --set "dns.enabled=${FUGUE_DNS_ENABLED}"
  )
  if [[ -n "$(trim_field "${FUGUE_DNS_STATIC_RECORDS_JSON:-}")" ]]; then
    DNS_STATIC_RECORDS_FILE="$(mktemp -t fugue-dns-static-records.XXXXXX.json)"
    printf '%s' "${FUGUE_DNS_STATIC_RECORDS_JSON}" >"${DNS_STATIC_RECORDS_FILE}"
    DNS_HELM_SET_ARGS+=(--set-file "api.dnsStaticRecordsJSON=${DNS_STATIC_RECORDS_FILE}")
  fi
  if [[ -n "$(trim_field "${FUGUE_PLATFORM_ROUTES_JSON:-}")" ]]; then
    PLATFORM_ROUTES_FILE="$(mktemp -t fugue-platform-routes.XXXXXX.json)"
    printf '%s' "${FUGUE_PLATFORM_ROUTES_JSON}" >"${PLATFORM_ROUTES_FILE}"
    DNS_HELM_SET_ARGS+=(--set-file "api.platformRoutesJSON=${PLATFORM_ROUTES_FILE}")
  fi
  if [[ "${FUGUE_DNS_ENABLED}" != "true" ]]; then
    return 0
  fi

  DNS_HELM_SET_ARGS+=(
    --set "dns.publicHostPorts.enabled=${FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED}"
    --set-string "dns.udpAddr=${FUGUE_DNS_UDP_ADDR}"
    --set-string "dns.tcpAddr=${FUGUE_DNS_TCP_ADDR}"
    --set-string "dns.zone=${FUGUE_DNS_ZONE}"
    --set "dns.ttl=${FUGUE_DNS_TTL}"
  )
  if [[ "${FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED}" == "true" ]]; then
    DNS_HELM_SET_ARGS+=(--set-string "dns.publicHostPorts.hostIP=${FUGUE_DNS_PUBLIC_HOST_IP}")
  fi
  if [[ -n "$(trim_field "${FUGUE_DNS_TOKEN_SECRET_NAME:-}")" ]]; then
    DNS_HELM_SET_ARGS+=(
      --set-string "dns.tokenSecret.name=${FUGUE_DNS_TOKEN_SECRET_NAME}"
      --set-string "dns.tokenSecret.key=${FUGUE_DNS_TOKEN_SECRET_KEY}"
    )
  fi
  while IFS= read -r answer_ip; do
    DNS_HELM_SET_ARGS+=(--set-string "dns.answerIPs[${index}]=${answer_ip}")
    index=$((index + 1))
  done < <(dns_answer_ips_lines "${FUGUE_DNS_ANSWER_IPS}")
  if (( index == 0 )); then
    fail "FUGUE_DNS_ANSWER_IPS must contain at least one non-empty IP when FUGUE_DNS_ENABLED=true"
  fi
  index=0
  while IFS= read -r answer_ip; do
    DNS_HELM_SET_ARGS+=(--set-string "dns.routeAAnswerIPs[${index}]=${answer_ip}")
    index=$((index + 1))
  done < <(dns_answer_ips_lines "${FUGUE_DNS_ROUTE_A_ANSWER_IPS:-}")
  index=0
  while IFS= read -r extra_zone; do
    DNS_HELM_SET_ARGS+=(--set-string "dns.extraZones[${index}]=${extra_zone}")
    index=$((index + 1))
  done < <(csv_lines "${FUGUE_DNS_EXTRA_ZONES:-}")
  index=0
  while IFS= read -r nameserver; do
    DNS_HELM_SET_ARGS+=(--set-string "dns.nameservers[${index}]=${nameserver}")
    index=$((index + 1))
  done < <(csv_lines "${FUGUE_DNS_NAMESERVERS:-}")
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
  log_stderr "using local control-plane automation bundle from ${bundle_dir}"
  return 0
}

restore_local_control_plane_automation_bundle_from_secret() {
  local bundle_dir="$1"
  local private_key_b64=""
  local known_hosts_b64=""
  local hosts_env_b64=""
  local tmp_dir=""

  if [[ -z "${FUGUE_NAMESPACE:-}" || -z "${FUGUE_CONTROL_PLANE_AUTOMATION_SECRET_NAME:-}" ]]; then
    return 1
  fi

  if ! private_key_b64="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get secret "${FUGUE_CONTROL_PLANE_AUTOMATION_SECRET_NAME}" -o go-template='{{index .data "ssh-private-key"}}' 2>/dev/null | tr -d '\r\n')"; then
    return 1
  fi
  if ! known_hosts_b64="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get secret "${FUGUE_CONTROL_PLANE_AUTOMATION_SECRET_NAME}" -o go-template='{{index .data "ssh-known-hosts"}}' 2>/dev/null | tr -d '\r\n')"; then
    return 1
  fi
  if ! hosts_env_b64="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get secret "${FUGUE_CONTROL_PLANE_AUTOMATION_SECRET_NAME}" -o go-template='{{index .data "hosts.env"}}' 2>/dev/null | tr -d '\r\n')"; then
    return 1
  fi

  if [[ -z "${private_key_b64}" || -z "${known_hosts_b64}" || -z "${hosts_env_b64}" ]]; then
    return 1
  fi

  tmp_dir="$(mktemp -d)" || return 1
  if ! printf '%s' "${private_key_b64}" | base64 --decode >"${tmp_dir}/id_ed25519"; then
    rm -rf "${tmp_dir}"
    return 1
  fi
  if ! printf '%s' "${known_hosts_b64}" | base64 --decode >"${tmp_dir}/known_hosts"; then
    rm -rf "${tmp_dir}"
    return 1
  fi
  if ! printf '%s' "${hosts_env_b64}" | base64 --decode >"${tmp_dir}/hosts.env"; then
    rm -rf "${tmp_dir}"
    return 1
  fi

  mkdir -p "${bundle_dir}"
  chmod 0700 "${bundle_dir}" 2>/dev/null || true
  mv "${tmp_dir}/id_ed25519" "${bundle_dir}/id_ed25519"
  mv "${tmp_dir}/known_hosts" "${bundle_dir}/known_hosts"
  mv "${tmp_dir}/hosts.env" "${bundle_dir}/hosts.env"
  rm -rf "${tmp_dir}"
  chmod 0600 "${bundle_dir}/id_ed25519"
  chmod 0644 "${bundle_dir}/known_hosts"
  chmod 0644 "${bundle_dir}/hosts.env"
  log_stderr "recovered control-plane automation SSH bundle from ${FUGUE_NAMESPACE}/${FUGUE_CONTROL_PLANE_AUTOMATION_SECRET_NAME}"
  return 0
}

bootstrap_local_control_plane_automation_bundle() {
  if [[ ! -x "./scripts/bootstrap_control_plane_automation.sh" ]]; then
    return 1
  fi

  log_stderr "bootstrapping control-plane automation SSH bundle on this server"
  bash ./scripts/bootstrap_control_plane_automation.sh
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
  if restore_local_control_plane_automation_bundle_from_secret "${LOCAL_CONTROL_PLANE_AUTOMATION_DIR}" && \
     use_local_control_plane_automation_bundle_from_dir "${LOCAL_CONTROL_PLANE_AUTOMATION_DIR}"; then
    return
  fi
  if [[ "${LOCAL_ROOT_CONTROL_PLANE_AUTOMATION_DIR}" != "${LOCAL_CONTROL_PLANE_AUTOMATION_DIR}" ]] && \
     restore_local_control_plane_automation_bundle_from_secret "${LOCAL_ROOT_CONTROL_PLANE_AUTOMATION_DIR}" && \
     use_local_control_plane_automation_bundle_from_dir "${LOCAL_ROOT_CONTROL_PLANE_AUTOMATION_DIR}"; then
    return
  fi
  if bootstrap_local_control_plane_automation_bundle && \
     use_local_control_plane_automation_bundle_from_dir "${LOCAL_CONTROL_PLANE_AUTOMATION_DIR}"; then
    return
  fi
  log_stderr "missing local control-plane automation bundle on this server; run scripts/bootstrap_control_plane_automation.sh or scripts/install_fugue_ha.sh to install it"
  return 1
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
  if [[ -z "${PRIMARY_CONTROL_PLANE_SSH_HOST}" ]]; then
    resolve_primary_control_plane_ssh_target ""
  fi
  [[ -n "${PRIMARY_CONTROL_PLANE_SSH_HOST}" ]] || fail "primary control-plane SSH host is not configured"
  if [[ -n "${PRIMARY_CONTROL_PLANE_SSH_USER}" ]]; then
    printf '%s@%s' "${PRIMARY_CONTROL_PLANE_SSH_USER}" "${PRIMARY_CONTROL_PLANE_SSH_HOST}"
    return
  fi
  printf '%s' "${PRIMARY_CONTROL_PLANE_SSH_HOST}"
}

ssh_host_port_is_reachable() {
  local host="$1"
  local port="$2"

  [[ -n "${host}" && -n "${port}" ]] || return 1
  if command_exists nc; then
    nc -z -w 3 "${host}" "${port}" >/dev/null 2>&1 && return 0
    nc -z -G 3 "${host}" "${port}" >/dev/null 2>&1 && return 0
  fi
  if command_exists timeout; then
    timeout 3 bash -c ":</dev/tcp/${host}/${port}" >/dev/null 2>&1 && return 0
  fi
  return 1
}

primary_node_address_candidates() {
  local primary_node_name="$1"
  local label_public_ip=""
  local gcp_public_ip=""

  [[ -n "${primary_node_name}" ]] || return 0
  label_public_ip="$(${KUBECTL} get node "${primary_node_name}" -o jsonpath='{.metadata.labels.fugue\.io/public-ip}' 2>/dev/null || true)"
  if [[ -n "${label_public_ip}" ]]; then
    printf '%s\n' "${label_public_ip}"
  fi
  if command_exists gcloud; then
    gcp_public_ip="$(gcloud compute instances list \
      --filter="name=${primary_node_name}" \
      --format='value(networkInterfaces[0].accessConfigs[0].natIP)' 2>/dev/null | awk 'NF > 0 {print; exit}' || true)"
    if [[ -n "${gcp_public_ip}" ]]; then
      printf '%s\n' "${gcp_public_ip}"
    fi
  fi
  ${KUBECTL} get node "${primary_node_name}" -o jsonpath='{range .status.addresses[?(@.type=="ExternalIP")]}{.address}{"\n"}{end}{range .status.addresses[?(@.type=="InternalIP")]}{.address}{"\n"}{end}{range .status.addresses[?(@.type=="Hostname")]}{.address}{"\n"}{end}' 2>/dev/null
}

resolve_primary_control_plane_ssh_target() {
  local primary_node_name="$1"
  local configured_host=""
  local candidate=""

  load_control_plane_hosts_env
  PRIMARY_CONTROL_PLANE_SSH_HOST="${FUGUE_NODE1_HOST:-${FUGUE_NODE1_ALIAS:-}}"
  PRIMARY_CONTROL_PLANE_SSH_USER="${FUGUE_NODE1_USER:-}"
  PRIMARY_CONTROL_PLANE_SSH_PORT="${FUGUE_NODE1_PORT:-22}"
  PRIMARY_CONTROL_PLANE_SSH_HOST_KEY_ALIAS=""
  configured_host="${PRIMARY_CONTROL_PLANE_SSH_HOST}"

  [[ -n "${PRIMARY_CONTROL_PLANE_SSH_HOST}" ]] || fail "primary control-plane SSH host is not configured"
  if [[ "${FUGUE_CONTROL_PLANE_SSH_NODE_ADDRESS_FALLBACK:-true}" != "true" ]]; then
    return
  fi
  if ssh_host_port_is_reachable "${PRIMARY_CONTROL_PLANE_SSH_HOST}" "${PRIMARY_CONTROL_PLANE_SSH_PORT}"; then
    return
  fi

  while IFS= read -r candidate; do
    [[ -n "${candidate}" ]] || continue
    [[ "${candidate}" != "${configured_host}" ]] || continue
    if ssh_host_port_is_reachable "${candidate}" "${PRIMARY_CONTROL_PLANE_SSH_PORT}"; then
      PRIMARY_CONTROL_PLANE_SSH_HOST="${candidate}"
      PRIMARY_CONTROL_PLANE_SSH_HOST_KEY_ALIAS="${configured_host}"
      log_stderr "configured primary SSH host ${configured_host}:${PRIMARY_CONTROL_PLANE_SSH_PORT} is not reachable; using Kubernetes node address ${candidate}:${PRIMARY_CONTROL_PLANE_SSH_PORT}"
      return
    fi
  done < <(primary_node_address_candidates "${primary_node_name}" | awk 'NF > 0 && !seen[$0]++')
}

build_primary_control_plane_ssh_opts() {
  local primary_node_name="$1"

  resolve_primary_control_plane_ssh_target "${primary_node_name}"
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
  if [[ -n "${PRIMARY_CONTROL_PLANE_SSH_HOST_KEY_ALIAS}" ]]; then
    PRIMARY_CONTROL_PLANE_SSH_OPTS+=(-o "HostKeyAlias=${PRIMARY_CONTROL_PLANE_SSH_HOST_KEY_ALIAS}")
  fi
  if [[ -n "${PRIMARY_CONTROL_PLANE_SSH_PORT}" ]]; then
    PRIMARY_CONTROL_PLANE_SSH_OPTS+=(-p "${PRIMARY_CONTROL_PLANE_SSH_PORT}")
  fi
}

detect_primary_node_name() {
  local node_name=""
  local selector=""

  for selector in \
    "fugue.install/role=primary" \
    "node-role.kubernetes.io/control-plane=true" \
    "node-role.kubernetes.io/control-plane" \
    "fugue.io/control-plane-desired-role=member"; do
    node_name="$(${KUBECTL} get nodes -l "${selector}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    node_name="$(trim_field "${node_name}")"
    if [[ -n "${node_name}" ]]; then
      printf '%s' "${node_name}"
      return
    fi
  done
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

try_primary_host_root_command() {
  local primary_node_name="$1"
  local cmd="$2"
  local local_hostname=""
  local local_hostname_short=""

  local_hostname="$(hostname 2>/dev/null || true)"
  local_hostname_short="$(hostname -s 2>/dev/null || true)"
  if [[ "${local_hostname}" == "${primary_node_name}" || "${local_hostname_short}" == "${primary_node_name}" ]]; then
    if [[ "$(id -u)" == "0" ]]; then
      bash -lc "${cmd}"
      return
    fi
    if sudo -n true >/dev/null 2>&1; then
      sudo -n bash -lc "${cmd}"
      return
    fi
    log_stderr "local primary host ${primary_node_name} requires interactive sudo; falling back to automation SSH"
  fi

  if ! prepare_control_plane_automation_ssh; then
    return 1
  fi
  build_primary_control_plane_ssh_opts "${primary_node_name}"
  ssh -n "${PRIMARY_CONTROL_PLANE_SSH_OPTS[@]}" "$(primary_control_plane_ssh_login)" \
    "sudo -n bash -lc $(printf '%q' "${cmd}")"
}

run_primary_host_root_command() {
  local primary_node_name="$1"

  if try_primary_host_root_command "$@"; then
    return 0
  fi
  fail "primary host root command failed on ${primary_node_name}"
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

local_kube_api_is_ready() {
  local readyz=""

  if command_exists timeout; then
    readyz="$(timeout 10s ${KUBECTL} get --raw='/readyz' 2>/dev/null || true)"
  else
    readyz="$(${KUBECTL} get --raw='/readyz' 2>/dev/null || true)"
  fi
  [[ "${readyz}" == *"ok"* ]]
}

ensure_kube_api_access() {
  local fallback_servers=""
  local -a server_list=()
  local kubeconfig_source=""
  local server=""
  local candidate_kubeconfig=""

  if local_kube_api_is_ready; then
    return 0
  fi

  fallback_servers="${FUGUE_CONTROL_PLANE_KUBE_API_FALLBACK_SERVERS:-}"
  [[ -n "${fallback_servers}" ]] || return 1

  kubeconfig_source="${KUBECONFIG:-${HOME}/.kube/config}"
  [[ -r "${kubeconfig_source}" ]] || return 1

  IFS=',' read -r -a server_list <<< "${fallback_servers}"
  for server in "${server_list[@]}"; do
    server="$(trim_field "${server}")"
    [[ -n "${server}" ]] || continue

    candidate_kubeconfig="$(mktemp -t fugue-kubeconfig-fallback.XXXXXX.yaml)"
    KUBECONFIG_FALLBACK_FILE="${candidate_kubeconfig}"
    cp "${kubeconfig_source}" "${candidate_kubeconfig}"
    sed -i.bak "s#server: .*#server: ${server}#" "${candidate_kubeconfig}"
    rm -f "${candidate_kubeconfig}.bak"
    if KUBECONFIG="${candidate_kubeconfig}" ${KUBECTL} get --raw='/readyz' >/dev/null 2>&1; then
      export KUBECONFIG="${candidate_kubeconfig}"
      log "using fallback Kubernetes API server ${server}"
      return 0
    fi
    rm -f "${candidate_kubeconfig}"
    KUBECONFIG_FALLBACK_FILE=""
  done

  return 1
}

wait_for_local_kube_api_ready() {
  local attempt
  local max_attempts

  if ! [[ "${LOCAL_KUBE_API_READY_POLL_SECONDS}" =~ ^[0-9]+$ ]] || (( LOCAL_KUBE_API_READY_POLL_SECONDS <= 0 )); then
    fail "FUGUE_LOCAL_KUBE_API_READY_POLL_SECONDS must be a positive integer"
  fi
  if ! [[ "${LOCAL_KUBE_API_READY_TIMEOUT_SECONDS}" =~ ^[0-9]+$ ]] || (( LOCAL_KUBE_API_READY_TIMEOUT_SECONDS <= 0 )); then
    fail "FUGUE_LOCAL_KUBE_API_READY_TIMEOUT_SECONDS must be a positive integer"
  fi

  max_attempts=$(( (LOCAL_KUBE_API_READY_TIMEOUT_SECONDS + LOCAL_KUBE_API_READY_POLL_SECONDS - 1) / LOCAL_KUBE_API_READY_POLL_SECONDS ))
  log "waiting up to ${LOCAL_KUBE_API_READY_TIMEOUT_SECONDS}s for local kube-apiserver to answer /readyz"

  for attempt in $(seq 1 "${max_attempts}"); do
    if local_kube_api_is_ready; then
      return 0
    fi
    sleep "${LOCAL_KUBE_API_READY_POLL_SECONDS}"
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

release_pod_names_by_phase() {
  local phase="$1"
  local output=""

  if command_exists timeout; then
    output="$(timeout 30s ${KUBECTL} -n "${FUGUE_NAMESPACE}" get pods \
      -l "$(release_pod_selector)" \
      --field-selector "status.phase=${phase}" \
      -o name 2>/dev/null || true)"
  else
    output="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get pods \
      -l "$(release_pod_selector)" \
      --field-selector "status.phase=${phase}" \
      -o name 2>/dev/null || true)"
  fi

  printf '%s\n' "${output}" | awk 'NF > 0'
}

delete_release_pod_batch() {
  local phase="$1"
  shift
  [[ "$#" -gt 0 ]] || return 0

  if command_exists timeout; then
    if timeout 60s ${KUBECTL} -n "${FUGUE_NAMESPACE}" delete \
      --ignore-not-found \
      --wait=false "$@" >/dev/null 2>&1; then
      return 0
    fi
    log "warning: failed to delete a batch of ${phase} Fugue release pods from ${FUGUE_NAMESPACE}"
    return 0
  fi

  if ! ${KUBECTL} -n "${FUGUE_NAMESPACE}" delete \
    --ignore-not-found \
    --wait=false "$@" >/dev/null 2>&1; then
    log "warning: failed to delete a batch of ${phase} Fugue release pods from ${FUGUE_NAMESPACE}"
  fi
}

prune_release_pods_by_phase() {
  local phase="$1"
  local names=""
  local count=""
  local name=""
  local -a batch=()

  names="$(release_pod_names_by_phase "${phase}")"
  count="$(printf '%s\n' "${names}" | awk 'NF > 0 {count++} END {print count + 0}')"
  [[ "${count}" != "0" ]] || return 0
  log "deleting ${count} ${phase} Fugue release pods from ${FUGUE_NAMESPACE}"

  while IFS= read -r name; do
    [[ -n "${name}" ]] || continue
    batch+=("${name}")
    if (( ${#batch[@]} == 50 )); then
      delete_release_pod_batch "${phase}" "${batch[@]}"
      batch=()
    fi
  done <<< "${names}"

  if (( ${#batch[@]} > 0 )); then
    delete_release_pod_batch "${phase}" "${batch[@]}"
  fi
}

prune_terminated_release_pods() {
  prune_release_pods_by_phase Failed
  prune_release_pods_by_phase Succeeded
  prune_release_pods_by_phase Unknown
}

unhealthy_node_names() {
  ${KUBECTL} get nodes --no-headers 2>/dev/null | awk '$2 !~ /^Ready/ {print $1}'
}

is_stateless_release_component() {
  local component="$1"
  case "${component}" in
    api|controller|node-janitor|topology-labeler|shared-workspace-provisioner|edge|dns|edge-*|dns-*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

stateless_release_pod_names_on_node() {
  local node_name="$1"
  local line=""
  local pod_name=""
  local component=""

  ${KUBECTL} -n "${FUGUE_NAMESPACE}" get pods \
    -l "$(release_pod_selector)" \
    --field-selector "spec.nodeName=${node_name}" \
    -o go-template='{{range .items}}{{.metadata.name}}{{"\t"}}{{index .metadata.labels "app.kubernetes.io/component"}}{{"\n"}}{{end}}' 2>/dev/null |
    while IFS=$'\t' read -r pod_name component; do
      [[ -n "${pod_name}" ]] || continue
      if is_stateless_release_component "${component}"; then
        printf 'pod/%s\n' "${pod_name}"
      fi
    done
}

force_delete_release_pods_on_unhealthy_nodes() {
  local node_name=""
  local names=""
  local count=""
  local pod=""
  local -a batch=()

  while IFS= read -r node_name; do
    [[ -n "${node_name}" ]] || continue
    names="$(stateless_release_pod_names_on_node "${node_name}")"
    count="$(printf '%s\n' "${names}" | awk 'NF > 0 {count++} END {print count + 0}')"
    [[ "${count}" != "0" ]] || continue

    log "force deleting ${count} stateless Fugue release pods on unhealthy node ${node_name}"
    batch=()
    while IFS= read -r pod; do
      [[ -n "${pod}" ]] || continue
      batch+=("${pod}")
      if (( ${#batch[@]} == 50 )); then
        ${KUBECTL} -n "${FUGUE_NAMESPACE}" delete \
          --ignore-not-found \
          --force \
          --grace-period=0 \
          --wait=false "${batch[@]}" >/dev/null 2>&1 || true
        batch=()
      fi
    done <<< "${names}"
    if (( ${#batch[@]} > 0 )); then
      ${KUBECTL} -n "${FUGUE_NAMESPACE}" delete \
        --ignore-not-found \
        --force \
        --grace-period=0 \
        --wait=false "${batch[@]}" >/dev/null 2>&1 || true
    fi
  done < <(unhealthy_node_names)
}

recover_primary_node_if_needed() {
  local primary_node_name=""

  primary_node_name="$(detect_primary_node_name)"
  if [[ -z "${primary_node_name}" ]]; then
    log "skip primary node recovery because the primary node could not be identified"
    return 0
  fi

  prune_terminated_release_pods
  force_delete_release_pods_on_unhealthy_nodes

  if primary_node_is_ready "${primary_node_name}"; then
    return 0
  fi

  log "skip primary node recovery because primary node ${primary_node_name} is NotReady; HA upgrade will continue on remaining Ready control-plane nodes"
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

  if [[ "${FUGUE_POSTGRES_ENABLED}" != "true" ]]; then
    log "skip legacy postgres recovery because legacy postgres is disabled"
    return 0
  fi

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

restore_primary_mesh_network_if_needed() {
  local primary_node_name=""
  local primary_config=""
  local primary_private_ip=""
  local primary_private_ip_cmd=""
  local primary_mesh_ip=""
  local current_node_ip=""
  local current_external_ip=""
  local current_flannel_iface=""
  local current_flannel_external_ip=""
  local filtered_config=""
  local patched_config=""
  local restore_cmd=""
  local backup_path=""

  primary_node_name="$(detect_primary_node_name)"
  if [[ -z "${primary_node_name}" ]]; then
    log "skip primary mesh restore because the primary node could not be identified"
    return 0
  fi
  if ! primary_node_is_ready "${primary_node_name}"; then
    log "skip primary mesh restore because primary node ${primary_node_name} is NotReady"
    return 0
  fi

  if ! primary_mesh_ip="$(try_primary_host_root_command "${primary_node_name}" "if command -v tailscale >/dev/null 2>&1; then tailscale ip -4 2>/dev/null | awk 'NR == 1 {print; exit}'; fi" | tr -d '\r')"; then
    log "skip primary mesh restore because host root access is unavailable on ${primary_node_name}"
    return 0
  fi
  if [[ -z "${primary_mesh_ip}" ]]; then
    log "skip primary mesh restore because tailscale has no IPv4 on ${primary_node_name}"
    return 0
  fi

  primary_private_ip_cmd="$(cat <<'EOF'
ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i=1;i<=NF;i++) if ($i=="src") {print $(i+1); exit}}'
EOF
)"
  primary_private_ip="$(run_primary_host_root_command "${primary_node_name}" "${primary_private_ip_cmd}" | tr -d '\r')"
  [[ -n "${primary_private_ip}" ]] || fail "failed to detect the primary private IP while restoring mesh networking"

  primary_config="$(run_primary_host_root_command "${primary_node_name}" "cat /etc/rancher/k3s/config.yaml 2>/dev/null || true")"
  if [[ -z "${primary_config}" ]]; then
    log "skip primary mesh restore because /etc/rancher/k3s/config.yaml is absent on ${primary_node_name}"
    return 0
  fi

  current_node_ip="$(printf '%s\n' "${primary_config}" | awk '$1 == "node-ip:" {line=$0; sub(/^[^:]+:[[:space:]]*/, "", line); gsub(/^"|"$/, "", line); print line; exit}')"
  current_external_ip="$(printf '%s\n' "${primary_config}" | awk '$1 == "node-external-ip:" {line=$0; sub(/^[^:]+:[[:space:]]*/, "", line); gsub(/^"|"$/, "", line); print line; exit}')"
  current_flannel_iface="$(printf '%s\n' "${primary_config}" | awk '$1 == "flannel-iface:" {line=$0; sub(/^[^:]+:[[:space:]]*/, "", line); gsub(/^"|"$/, "", line); print line; exit}')"
  current_flannel_external_ip="$(printf '%s\n' "${primary_config}" | awk '$1 == "flannel-external-ip:" {line=$0; sub(/^[^:]+:[[:space:]]*/, "", line); gsub(/^"|"$/, "", line); print line; exit}')"

  if [[ "${current_node_ip}" == "${primary_private_ip}" ]] && \
     [[ "${current_external_ip}" == "${primary_mesh_ip}" ]] && \
     [[ "${current_flannel_iface}" == "tailscale0" ]] && \
     [[ "${current_flannel_external_ip}" == "true" ]]; then
    return 0
  fi

  filtered_config="$(printf '%s\n' "${primary_config}" | awk '
    $1 == "node-ip:" {next}
    $1 == "node-external-ip:" {next}
    $1 == "flannel-external-ip:" {next}
    $1 == "flannel-iface:" {next}
    {print}
  ')"

  patched_config="$(printf '%s\n' "${filtered_config}" | awk -v node_ip="${primary_private_ip}" -v mesh_ip="${primary_mesh_ip}" '
    function print_mesh_block() {
      printf "node-ip: \"%s\"\n", node_ip
      printf "node-external-ip: \"%s\"\n", mesh_ip
      print "flannel-external-ip: true"
      print "flannel-iface: \"tailscale0\""
    }
    {
      print
      if (!inserted && $1 == "write-kubeconfig-mode:") {
        print_mesh_block()
        inserted = 1
      }
    }
    END {
      if (!inserted) {
        print_mesh_block()
      }
    }
  ')"

  log "restoring primary k3s server ${primary_node_name} to mesh networking (${primary_mesh_ip})"
  restore_cmd="$(cat <<EOF
set -euo pipefail

config=/etc/rancher/k3s/config.yaml
backup="\${config}.mesh-restore-\$(date +%Y%m%d%H%M%S)"
cp "\${config}" "\${backup}"
cat >"\${config}" <<'CFG'
${patched_config}
CFG
if command -v timeout >/dev/null 2>&1; then
  timeout --kill-after=15s 120s systemctl restart k3s
else
  systemctl restart k3s
fi
systemctl is-active --quiet k3s
printf '%s\n' "\${backup}"
EOF
)"
  backup_path="$(run_primary_host_root_command "${primary_node_name}" "${restore_cmd}" | tr -d '\r' | tail -n 1)"
  if [[ -n "${backup_path}" ]]; then
    log "backed up primary k3s config to ${backup_path} before restoring mesh networking"
  fi

  if ! wait_for_local_kube_api_ready; then
    fail "local kube-apiserver did not recover after restoring mesh networking on ${primary_node_name}"
  fi

  if ! wait_for_primary_node_ready "${primary_node_name}"; then
    fail "primary node ${primary_node_name} did not become Ready after restoring mesh networking"
  fi
}

ready_nodes_matching_selector() {
  local selector="$1"
  ${KUBECTL} get nodes -l "${selector}" --no-headers 2>/dev/null | \
    awk '$2 == "Ready" || $2 ~ /^Ready,/ {count++} END {print count + 0}'
}

ensure_coredns_multinode_scheduling() {
  local desired_replicas="${FUGUE_COREDNS_TARGET_REPLICAS}"
  local coredns_selector_key="fugue.install/profile"
  local coredns_selector_value="combined"
  local ready_coredns_nodes="0"
  local current_replicas=""
  local current_profile_selector=""
  local current_control_plane_selector=""
  local current_os_selector=""
  local patch_payload=""

  if ! [[ "${desired_replicas}" =~ ^[0-9]+$ ]] || (( desired_replicas <= 0 )); then
    fail "FUGUE_COREDNS_TARGET_REPLICAS must be a positive integer"
  fi

  if ! ${KUBECTL} -n "${FUGUE_COREDNS_NAMESPACE}" get deploy "${FUGUE_COREDNS_DEPLOYMENT_NAME}" >/dev/null 2>&1; then
    log "skip CoreDNS HA normalization because deploy/${FUGUE_COREDNS_DEPLOYMENT_NAME} is absent from ${FUGUE_COREDNS_NAMESPACE}"
    return 0
  fi

  # Keep CoreDNS off worker nodes so pod DNS does not vary with worker host resolvers.
  ready_coredns_nodes="$(ready_nodes_matching_selector "kubernetes.io/os=linux,${coredns_selector_key}=${coredns_selector_value}")"
  if ! [[ "${ready_coredns_nodes}" =~ ^[0-9]+$ ]] || (( ready_coredns_nodes == 0 )); then
    coredns_selector_key="node-role.kubernetes.io/control-plane"
    coredns_selector_value="true"
    ready_coredns_nodes="$(ready_nodes_matching_selector "kubernetes.io/os=linux,${coredns_selector_key}=${coredns_selector_value}")"
  fi
  if ! [[ "${ready_coredns_nodes}" =~ ^[0-9]+$ ]] || (( ready_coredns_nodes == 0 )); then
    fail "no Ready linux control-plane nodes available for CoreDNS scheduling"
  fi
  if (( desired_replicas > ready_coredns_nodes )); then
    desired_replicas="${ready_coredns_nodes}"
  fi
  if (( desired_replicas < 1 )); then
    desired_replicas=1
  fi

  current_replicas="$(${KUBECTL} -n "${FUGUE_COREDNS_NAMESPACE}" get deploy "${FUGUE_COREDNS_DEPLOYMENT_NAME}" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
  current_profile_selector="$(${KUBECTL} -n "${FUGUE_COREDNS_NAMESPACE}" get deploy "${FUGUE_COREDNS_DEPLOYMENT_NAME}" -o jsonpath='{.spec.template.spec.nodeSelector.fugue\.install/profile}' 2>/dev/null || true)"
  current_control_plane_selector="$(${KUBECTL} -n "${FUGUE_COREDNS_NAMESPACE}" get deploy "${FUGUE_COREDNS_DEPLOYMENT_NAME}" -o jsonpath='{.spec.template.spec.nodeSelector.node-role\.kubernetes\.io/control-plane}' 2>/dev/null || true)"
  current_os_selector="$(${KUBECTL} -n "${FUGUE_COREDNS_NAMESPACE}" get deploy "${FUGUE_COREDNS_DEPLOYMENT_NAME}" -o jsonpath='{.spec.template.spec.nodeSelector.kubernetes\.io/os}' 2>/dev/null || true)"

  if [[ "${current_replicas}" == "${desired_replicas}" ]] && [[ "${current_os_selector}" == "linux" ]]; then
    if [[ "${coredns_selector_key}" == "fugue.install/profile" ]] && [[ "${current_profile_selector}" == "${coredns_selector_value}" ]]; then
      return 0
    fi
    if [[ "${coredns_selector_key}" == "node-role.kubernetes.io/control-plane" ]] && [[ "${current_control_plane_selector}" == "${coredns_selector_value}" ]]; then
      return 0
    fi
  fi

  log "ensuring CoreDNS stays on control-plane nodes (replicas=${desired_replicas})"
  patch_payload="$(cat <<EOF
[
  {"op":"add","path":"/spec/replicas","value":${desired_replicas}},
  {"op":"add","path":"/spec/template/spec/nodeSelector","value":{"kubernetes.io/os":"linux","${coredns_selector_key}":"${coredns_selector_value}"}}
]
EOF
)"
  ${KUBECTL} -n "${FUGUE_COREDNS_NAMESPACE}" patch deploy "${FUGUE_COREDNS_DEPLOYMENT_NAME}" --type=json -p "${patch_payload}" >/dev/null
  ${KUBECTL} -n "${FUGUE_COREDNS_NAMESPACE}" rollout status "deploy/${FUGUE_COREDNS_DEPLOYMENT_NAME}" --timeout=180s
}

node_local_dns_normalize_hostname_list() {
  local raw="$1"
  local source_name="$2"
  local node_name=""
  local normalized=""

  raw="${raw//;/$'\n'}"
  raw="${raw//,/$'\n'}"
  while IFS= read -r node_name; do
    node_name="$(trim_field "${node_name}")"
    [[ -n "${node_name}" ]] || continue
    [[ "${node_name}" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$ ]] || fail "invalid Kubernetes hostname in ${source_name}: ${node_name}"
    if grep -Fqx "${node_name}" <<<"${normalized}"; then
      fail "duplicate Kubernetes hostname in ${source_name}: ${node_name}"
    fi
    normalized+="${normalized:+$'\n'}${node_name}"
  done <<<"${raw}"
  printf '%s\n' "${normalized}"
}

node_local_dns_preserved_offline_nodes() {
  local raw="${FUGUE_NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES:-}"

  [[ -n "$(trim_field "${raw}")" ]] || return 0
  node_local_dns_normalize_hostname_list "${raw}" FUGUE_NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES
}

node_local_dns_configure_cohort_names() {
  local base_name="${FUGUE_RELEASE_FULLNAME}-node-local-dns"

  base_name="${base_name:0:63}"
  base_name="${base_name%-}"

  NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME="${base_name}"
  NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES="$(node_local_dns_preserved_offline_nodes)"
  if [[ -n "$(trim_field "${NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES}")" ]]; then
    NODE_LOCAL_DNS_SPLIT_COHORT="true"
    NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME="${base_name:0:56}"
    NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME="${NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME%-}-active"
    NODE_LOCAL_DNS_ACTIVE_COMPONENT="node-local-dns-active"
    [[ "${NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME}" != "${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME}" ]] || fail "active and preserved NodeLocal DNSCache DaemonSet names must be distinct"
  else
    NODE_LOCAL_DNS_SPLIT_COHORT="false"
    NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME="${base_name}"
    NODE_LOCAL_DNS_ACTIVE_COMPONENT="node-local-dns"
  fi
  NODE_LOCAL_DNS_ACTIVE_SERVICE_NAME="${NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME}"
  NODE_LOCAL_DNS_UPSTREAM_SERVICE_NAME="${FUGUE_RELEASE_FULLNAME}-dns-upstream"
  NODE_LOCAL_DNS_UPSTREAM_SERVICE_NAME="${NODE_LOCAL_DNS_UPSTREAM_SERVICE_NAME:0:63}"
  NODE_LOCAL_DNS_UPSTREAM_SERVICE_NAME="${NODE_LOCAL_DNS_UPSTREAM_SERVICE_NAME%-}"
}

node_local_dns_active_pod_selector() {
  printf 'app.kubernetes.io/name=fugue,app.kubernetes.io/instance=%s,app.kubernetes.io/component=%s' \
    "${FUGUE_RELEASE_NAME}" "${NODE_LOCAL_DNS_ACTIVE_COMPONENT}"
}

node_local_dns_same_node_set() {
  local left="$1"
  local right="$2"
  local normalized_left=""
  local normalized_right=""

  normalized_left="$(printf '%s\n' "${left}" | sed '/^[[:space:]]*$/d' | sort -u)"
  normalized_right="$(printf '%s\n' "${right}" | sed '/^[[:space:]]*$/d' | sort -u)"
  [[ -n "$(trim_field "${normalized_left}")" && "${normalized_left}" == "${normalized_right}" ]]
}

node_local_dns_candidate_nodes() {
  local raw="${FUGUE_NODE_LOCAL_DNS_NODE_NAMES:-}"

  if [[ -n "$(trim_field "${raw}")" ]]; then
    [[ -z "$(trim_field "${FUGUE_NODE_LOCAL_DNS_NODE_NAME:-}")" ]] || fail "FUGUE_NODE_LOCAL_DNS_NODE_NAME and FUGUE_NODE_LOCAL_DNS_NODE_NAMES are mutually exclusive"
    raw="$(node_local_dns_normalize_hostname_list "${raw}" FUGUE_NODE_LOCAL_DNS_NODE_NAMES)"
    [[ -n "$(trim_field "${raw}")" ]] || fail "FUGUE_NODE_LOCAL_DNS_NODE_NAMES must contain at least one hostname"
    printf '%s\n' "${raw}"
    return
  fi
  if [[ -n "$(trim_field "${FUGUE_NODE_LOCAL_DNS_NODE_NAME:-}")" ]]; then
    raw="$(node_local_dns_normalize_hostname_list "${FUGUE_NODE_LOCAL_DNS_NODE_NAME}" FUGUE_NODE_LOCAL_DNS_NODE_NAME)"
    [[ -n "$(trim_field "${raw}")" ]] || fail "FUGUE_NODE_LOCAL_DNS_NODE_NAME must contain one hostname"
    printf '%s\n' "${raw}"
    return
  fi
  [[ "${FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES}" == "true" ]] || fail "a bounded NodeLocal DNSCache hostname or hostname cohort is required"
  ${KUBECTL} get nodes -l kubernetes.io/os=linux -o json 2>/dev/null | python3 -c '
import json
import sys

payload = json.load(sys.stdin)
selected = []
for node in payload.get("items") or []:
    metadata = node.get("metadata") or {}
    spec = node.get("spec") or {}
    status = node.get("status") or {}
    labels = metadata.get("labels") or {}
    conditions = {str(item.get("type") or ""): str(item.get("status") or "") for item in status.get("conditions") or []}
    name = str(metadata.get("name") or "").strip()
    if (
        name
        and labels.get("kubernetes.io/os") == "linux"
        and labels.get("kubernetes.io/arch") == "amd64"
        and conditions.get("Ready") == "True"
        and spec.get("unschedulable") is not True
    ):
        selected.append(name)
for name in sorted(selected):
    print(name)
'
}

node_local_dns_daemonset_target_nodes() {
  local daemonset_json="${1:-}"

  if [[ -z "$(trim_field "${daemonset_json}")" ]]; then
    daemonset_json="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get daemonset "${NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME}" -o json)" || return 1
  fi
  printf '%s' "${daemonset_json}" | python3 -c '
import json
import sys

doc = json.load(sys.stdin)
spec = (((doc.get("spec") or {}).get("template") or {}).get("spec") or {})
selector = spec.get("nodeSelector") or {}
legacy = str(selector.get("kubernetes.io/hostname") or "").strip()
if legacy:
    if selector != {"kubernetes.io/os": "linux", "kubernetes.io/hostname": legacy} or spec.get("affinity") not in (None, {}):
        raise SystemExit(1)
    print(legacy)
    raise SystemExit(0)

if selector != {"kubernetes.io/os": "linux"}:
    raise SystemExit(1)
affinity = spec.get("affinity") or {}
if set(affinity) != {"nodeAffinity"}:
    raise SystemExit(1)
node_affinity = affinity.get("nodeAffinity") or {}
if set(node_affinity) != {"requiredDuringSchedulingIgnoredDuringExecution"}:
    raise SystemExit(1)
required = node_affinity.get("requiredDuringSchedulingIgnoredDuringExecution") or {}
if set(required) != {"nodeSelectorTerms"}:
    raise SystemExit(1)
terms = required.get("nodeSelectorTerms") or []
if len(terms) != 1 or set(terms[0]) != {"matchExpressions"}:
    raise SystemExit(1)
expressions = terms[0].get("matchExpressions") or []
if len(expressions) != 1:
    raise SystemExit(1)
expression = expressions[0]
if set(expression) != {"key", "operator", "values"} or expression.get("key") != "kubernetes.io/hostname" or expression.get("operator") != "In":
    raise SystemExit(1)
values = [str(value or "").strip() for value in expression.get("values") or []]
if not values or any(not value for value in values) or len(set(values)) != len(values):
    raise SystemExit(1)
for value in values:
    print(value)
'
}

node_local_dns_daemonset_template_mode() {
  local daemonset_json="$1"

  printf '%s' "${daemonset_json}" | python3 -c '
import json
import sys

doc = json.load(sys.stdin)
print(str((((doc.get("spec") or {}).get("template") or {}).get("metadata") or {}).get("labels", {}).get("fugue.io/node-local-dns-mode") or ""))
'
}

node_local_dns_verify_preserved_daemonset_template() {
  local daemonset_json=""
  local expected_image="${FUGUE_NODE_LOCAL_DNS_IMAGE_REPOSITORY}@${FUGUE_NODE_LOCAL_DNS_IMAGE_DIGEST}"
  local expected_listen_ips="${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}"
  local expected_targets_json=""

  [[ "${NODE_LOCAL_DNS_SPLIT_COHORT}" == "true" ]] || return 0
  if [[ "${NODE_LOCAL_DNS_PRESERVED_MODE}" == "iptables" ]]; then
    expected_listen_ips="${FUGUE_NODE_LOCAL_DNS_LOCAL_IP},${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}"
  fi
  expected_targets_json="$(node_local_dns_targets_json "${NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES}")" || return 1
  daemonset_json="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get daemonset "${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME}" -o json)" || return 1
  local expected_uid=""
  expected_uid="$(printf '%s' "${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_JSON}" | python3 -c 'import json,sys; print(str((json.load(sys.stdin).get("metadata") or {}).get("uid") or ""))')" || return 1
  [[ -n "${expected_uid}" ]] || return 1
  DAEMONSET_JSON="${daemonset_json}" EXPECTED_NAME="${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME}" \
    EXPECTED_UID="${expected_uid}" \
    EXPECTED_MODE="${NODE_LOCAL_DNS_PRESERVED_MODE}" EXPECTED_IMAGE="${expected_image}" \
    EXPECTED_LISTEN_IPS="${expected_listen_ips}" EXPECTED_TARGETS_JSON="${expected_targets_json}" \
    EXPECTED_UPSTREAM="${NODE_LOCAL_DNS_UPSTREAM_SERVICE_NAME}" EXPECTED_CONFIG_MAP="${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME}" \
    EXPECTED_RESOURCE_NAME="${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME}" python3 -c '
import json
import os

doc = json.loads(os.environ["DAEMONSET_JSON"])
metadata = doc.get("metadata") or {}
spec = doc.get("spec") or {}
template = spec.get("template") or {}
template_metadata = template.get("metadata") or {}
template_spec = template.get("spec") or {}
labels = template_metadata.get("labels") or {}
containers = template_spec.get("containers") or []
status = doc.get("status") or {}
if (
    metadata.get("name") != os.environ["EXPECTED_NAME"]
    or metadata.get("uid") != os.environ["EXPECTED_UID"]
    or str(metadata.get("generation") or "") != str(status.get("observedGeneration") or "")
    or spec.get("updateStrategy") != {"type": "OnDelete"}
    or labels.get("app.kubernetes.io/component") != "node-local-dns"
    or labels.get("fugue.io/node-local-dns-mode") != os.environ["EXPECTED_MODE"]
    or labels.get("fugue.io/node-local-dns-cohort") != "preserved"
    or len(containers) != 1
):
    raise SystemExit(1)
container = containers[0]
expected_args = ["-localip", os.environ["EXPECTED_LISTEN_IPS"], "-conf", "/etc/Corefile", "-upstreamsvc", os.environ["EXPECTED_UPSTREAM"]]
if container.get("name") != "node-cache" or container.get("image") != os.environ["EXPECTED_IMAGE"] or container.get("args") != expected_args:
    raise SystemExit(1)
if template_spec.get("nodeSelector") != {"kubernetes.io/os": "linux"}:
    raise SystemExit(1)
expected_targets = json.loads(os.environ["EXPECTED_TARGETS_JSON"])
expected_affinity = {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [{"matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "In", "values": expected_targets}]}]}}}
if template_spec.get("affinity") != expected_affinity:
    raise SystemExit(1)
volumes = {item.get("name"): item for item in template_spec.get("volumes") or []}
config_map = (volumes.get("config-volume") or {}).get("configMap") or {}
expected_items = [{"key": "Corefile." + os.environ["EXPECTED_MODE"], "path": "Corefile.base"}]
if config_map.get("name") != os.environ["EXPECTED_CONFIG_MAP"] or config_map.get("items") != expected_items:
    raise SystemExit(1)
if template_spec.get("priorityClassName") != os.environ["EXPECTED_RESOURCE_NAME"] or template_spec.get("serviceAccountName") != os.environ["EXPECTED_RESOURCE_NAME"]:
    raise SystemExit(1)
'
}

node_local_dns_verify_preserved_state_unchanged() {
  local current_pods=""

  [[ "${NODE_LOCAL_DNS_SPLIT_COHORT}" == "true" ]] || return 0
  node_local_dns_verify_preserved_nodes_isolated || return 1
  node_local_dns_verify_preserved_daemonset_template || return 1
  current_pods="$(node_local_dns_capture_preserved_pods_json)" || return 1
  node_local_dns_validate_preserved_pod_snapshot \
    "${current_pods}" "${NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES}" "${NODE_LOCAL_DNS_PRESERVED_MODE}" "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}" || return 1
  node_local_dns_verify_preserved_pod_snapshot_unchanged "${current_pods}"
}

node_local_dns_verify_preserved_pod_snapshot_unchanged() {
  local current_pods="$1"

  [[ "${NODE_LOCAL_DNS_SPLIT_COHORT}" == "true" ]] || return 0
  CURRENT_PODS="${current_pods}" SNAPSHOT_PODS="${NODE_LOCAL_DNS_PRESERVED_PODS_JSON}" python3 -c '
import json
import os

fields = (
    "name", "uid", "owner_references", "deletion_timestamp", "node", "component", "cohort", "mode", "revision",
    "image", "image_pull_policy", "args", "security_context", "volume_mounts", "host_network", "dns_policy",
    "priority_class_name", "service_account_name", "automount_service_account_token", "volumes",
    "config_map_name", "config_items", "ready", "phase", "restart_count", "waiting_reasons",
    "terminated_reasons", "last_waiting_reasons", "last_terminated_reasons",
)
current = sorted(json.loads(os.environ["CURRENT_PODS"]), key=lambda item: str(item.get("node") or ""))
snapshot = sorted(json.loads(os.environ["SNAPSHOT_PODS"]), key=lambda item: str(item.get("node") or ""))
if len(current) != len(snapshot):
    raise SystemExit(1)
for live, old in zip(current, snapshot):
    if any(live.get(field) != old.get(field) for field in fields):
        raise SystemExit(1)
'
}

node_local_dns_verify_preserved_snapshot_after_helm_rollback() {
  local deadline=$((SECONDS + FUGUE_NODE_LOCAL_DNS_PROBE_TIMEOUT_SECONDS))
  local daemonset_json=""
  local current_pods=""
  local observed="false"

  [[ "${NODE_LOCAL_DNS_SPLIT_COHORT}" == "true" ]] || return 0
  node_local_dns_verify_preserved_nodes_isolated || return 1
  while (( SECONDS < deadline )); do
    daemonset_json="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get daemonset "${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME}" -o json 2>/dev/null || true)"
    if [[ -n "${daemonset_json}" ]] && DAEMONSET_JSON="${daemonset_json}" SNAPSHOT_JSON="${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_JSON}" python3 -c '
import json
import os

current = json.loads(os.environ["DAEMONSET_JSON"])
snapshot = json.loads(os.environ["SNAPSHOT_JSON"])
current_metadata = current.get("metadata") or {}
snapshot_metadata = snapshot.get("metadata") or {}
status = current.get("status") or {}
if str(current_metadata.get("generation") or "") != str(status.get("observedGeneration") or ""):
    raise SystemExit(1)
for field in ("name", "namespace", "uid", "labels"):
    if current_metadata.get(field) != snapshot_metadata.get(field):
        raise SystemExit(1)
ignored_annotations = {"deprecated.daemonset.template.generation"}
current_annotations = {key: value for key, value in (current_metadata.get("annotations") or {}).items() if key not in ignored_annotations}
snapshot_annotations = {key: value for key, value in (snapshot_metadata.get("annotations") or {}).items() if key not in ignored_annotations}
if current_annotations != snapshot_annotations:
    raise SystemExit(1)
if current.get("spec") != snapshot.get("spec"):
    raise SystemExit(1)
'; then
      observed="true"
      break
    fi
    sleep 2
  done
  [[ "${observed}" == "true" ]] || return 1
  current_pods="$(node_local_dns_capture_preserved_pods_json)" || return 1
  node_local_dns_validate_preserved_pod_snapshot \
    "${current_pods}" "${NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES}" "${NODE_LOCAL_DNS_PRESERVED_MODE}" "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}" || return 1
  node_local_dns_verify_preserved_pod_snapshot_unchanged "${current_pods}"
}

node_local_dns_capture_pods_json_for_selector() {
  local selector="$1"

  ${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get pods -l "${selector}" -o json | python3 -c '
import json
import sys

payload = json.load(sys.stdin)
result = []
for pod in payload.get("items") or []:
    metadata = pod.get("metadata") or {}
    spec = pod.get("spec") or {}
    status = pod.get("status") or {}
    ready = any(item.get("type") == "Ready" and item.get("status") == "True" for item in status.get("conditions") or [])
    volumes = {item.get("name"): item for item in spec.get("volumes") or []}
    config_map = (volumes.get("config-volume") or {}).get("configMap") or {}
    config_items = config_map.get("items") or []
    containers = spec.get("containers") or []
    node_cache = next((item for item in containers if item.get("name") == "node-cache"), {})
    result.append({
        "name": metadata.get("name") or "",
        "uid": metadata.get("uid") or "",
        "owner_references": sorted(
            [
                {
                    "api_version": str(item.get("apiVersion") or ""),
                    "kind": str(item.get("kind") or ""),
                    "name": str(item.get("name") or ""),
                    "uid": str(item.get("uid") or ""),
                    "controller": bool(item.get("controller")),
                }
                for item in metadata.get("ownerReferences") or []
            ],
            key=lambda item: (item["kind"], item["name"], item["uid"]),
        ),
        "deletion_timestamp": metadata.get("deletionTimestamp") or "",
        "node": spec.get("nodeName") or "",
        "component": (metadata.get("labels") or {}).get("app.kubernetes.io/component") or "",
        "cohort": (metadata.get("labels") or {}).get("fugue.io/node-local-dns-cohort") or "",
        "mode": (metadata.get("labels") or {}).get("fugue.io/node-local-dns-mode") or "",
        "revision": (metadata.get("labels") or {}).get("controller-revision-hash") or "",
        "image": node_cache.get("image") or "",
        "image_pull_policy": node_cache.get("imagePullPolicy") or "",
        "args": node_cache.get("args") or [],
        "security_context": node_cache.get("securityContext") or {},
        "volume_mounts": node_cache.get("volumeMounts") or [],
        "host_network": spec.get("hostNetwork"),
        "dns_policy": spec.get("dnsPolicy") or "",
        "priority_class_name": spec.get("priorityClassName") or "",
        "service_account_name": spec.get("serviceAccountName") or "",
        "automount_service_account_token": spec.get("automountServiceAccountToken"),
        "volumes": spec.get("volumes") or [],
        "config_map_name": config_map.get("name") or "",
        "ready": ready,
        "phase": status.get("phase") or "",
        "restart_count": sum(int(item.get("restartCount") or 0) for item in status.get("containerStatuses") or []),
        "waiting_reasons": sorted({
            str(((item.get("state") or {}).get("waiting") or {}).get("reason") or "")
            for item in status.get("containerStatuses") or []
            if ((item.get("state") or {}).get("waiting") or {}).get("reason")
        }),
        "terminated_reasons": sorted({
            str(((item.get("state") or {}).get("terminated") or {}).get("reason") or "")
            for item in status.get("containerStatuses") or []
            if ((item.get("state") or {}).get("terminated") or {}).get("reason")
        }),
        "last_waiting_reasons": sorted({
            str(((item.get("lastState") or {}).get("waiting") or {}).get("reason") or "")
            for item in status.get("containerStatuses") or []
            if ((item.get("lastState") or {}).get("waiting") or {}).get("reason")
        }),
        "last_terminated_reasons": sorted({
            str(((item.get("lastState") or {}).get("terminated") or {}).get("reason") or "")
            for item in status.get("containerStatuses") or []
            if ((item.get("lastState") or {}).get("terminated") or {}).get("reason")
        }),
        "config_items": config_items,
    })
result.sort(key=lambda item: item["node"])
print(json.dumps(result, separators=(",", ":")))
'
}

node_local_dns_capture_pods_json() {
  node_local_dns_capture_pods_json_for_selector "$(node_local_dns_active_pod_selector)"
}

node_local_dns_capture_preserved_pods_json() {
  node_local_dns_capture_pods_json_for_selector \
    "app.kubernetes.io/name=fugue,app.kubernetes.io/instance=${FUGUE_RELEASE_NAME},app.kubernetes.io/component=node-local-dns"
}

node_local_dns_set_difference() {
  local left="$1"
  local right="$2"
  comm -23 \
    <(printf '%s\n' "${left}" | sed '/^[[:space:]]*$/d' | sort -u) \
    <(printf '%s\n' "${right}" | sed '/^[[:space:]]*$/d' | sort -u)
}

node_local_dns_validate_pure_pod_snapshot() {
  local pods_json="$1"
  local target_nodes="$2"
  local expected_mode="$3"

  PODS_JSON="${pods_json}" TARGET_NODES="${target_nodes}" EXPECTED_MODE="${expected_mode}" python3 -c '
import json
import os

pods = json.loads(os.environ["PODS_JSON"])
targets = [item.strip() for item in os.environ["TARGET_NODES"].splitlines() if item.strip()]
expected_mode = os.environ["EXPECTED_MODE"]
if expected_mode not in {"shadow", "iptables"} or not targets or len(set(targets)) != len(targets):
    raise SystemExit(1)
by_node = {}
for pod in pods:
    node = str(pod.get("node") or "").strip()
    if not node or node in by_node:
        raise SystemExit(1)
    by_node[node] = pod
if set(by_node) != set(targets):
    raise SystemExit(1)
for node in targets:
    pod = by_node[node]
    if (
        not pod.get("name")
        or not pod.get("uid")
        or pod.get("deletion_timestamp")
        or not pod.get("revision")
        or not pod.get("ready")
        or pod.get("phase") != "Running"
        or pod.get("mode") != expected_mode
        or int(pod.get("restart_count") or 0) != 0
        or pod.get("waiting_reasons")
        or pod.get("terminated_reasons")
        or pod.get("last_waiting_reasons")
        or pod.get("last_terminated_reasons")
    ):
        raise SystemExit(1)
'
}

node_local_dns_validate_active_pod_runtime() {
  local pods_json="$1"
  local target_nodes="$2"
  local expected_mode="$3"
  local service_ip="$4"
  local require_current_layout="${5:-true}"
  local expected_image="${FUGUE_NODE_LOCAL_DNS_IMAGE_REPOSITORY}@${FUGUE_NODE_LOCAL_DNS_IMAGE_DIGEST}"
  local expected_listen_ips="${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}"

  if [[ "${expected_mode}" == "iptables" ]]; then
    expected_listen_ips="${FUGUE_NODE_LOCAL_DNS_LOCAL_IP},${service_ip}"
  fi
  PODS_JSON="${pods_json}" TARGET_NODES="${target_nodes}" EXPECTED_MODE="${expected_mode}" \
    EXPECTED_IMAGE="${expected_image}" EXPECTED_LISTEN_IPS="${expected_listen_ips}" \
    EXPECTED_UPSTREAM="${NODE_LOCAL_DNS_UPSTREAM_SERVICE_NAME}" EXPECTED_CONFIG_MAP="${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME}" \
    EXPECTED_RESOURCE_NAME="${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME}" EXPECTED_COMPONENT="${NODE_LOCAL_DNS_ACTIVE_COMPONENT}" \
    EXPECTED_PULL_POLICY="${FUGUE_NODE_LOCAL_DNS_IMAGE_PULL_POLICY}" REQUIRE_CURRENT_LAYOUT="${require_current_layout}" python3 -c '
import json
import os

pods = json.loads(os.environ["PODS_JSON"])
targets = {item.strip() for item in os.environ["TARGET_NODES"].splitlines() if item.strip()}
if not targets or len(pods) != len(targets):
    raise SystemExit(1)
expected_args = ["-localip", os.environ["EXPECTED_LISTEN_IPS"], "-conf", "/etc/Corefile", "-upstreamsvc", os.environ["EXPECTED_UPSTREAM"]]
expected_current_items = [{"key": "Corefile." + os.environ["EXPECTED_MODE"], "path": "Corefile.base"}]
expected_legacy_items = [{"key": "Corefile", "path": "Corefile.base"}]
by_node = {str(pod.get("node") or ""): pod for pod in pods}
if set(by_node) != targets or "" in by_node:
    raise SystemExit(1)
for pod in pods:
    current_layout = pod.get("config_items") == expected_current_items
    if not current_layout and not (os.environ["REQUIRE_CURRENT_LAYOUT"] != "true" and pod.get("config_items") == expected_legacy_items):
        raise SystemExit(1)
    if (
        pod.get("component") != os.environ["EXPECTED_COMPONENT"]
        or (os.environ["REQUIRE_CURRENT_LAYOUT"] == "true" and pod.get("cohort") != "active")
        or pod.get("mode") != os.environ["EXPECTED_MODE"]
        or pod.get("image") != os.environ["EXPECTED_IMAGE"]
        or pod.get("image_pull_policy") != os.environ["EXPECTED_PULL_POLICY"]
        or pod.get("args") != expected_args
        or pod.get("config_map_name") != os.environ["EXPECTED_CONFIG_MAP"]
        or pod.get("host_network") is not True
        or pod.get("dns_policy") != "Default"
        or pod.get("priority_class_name") != os.environ["EXPECTED_RESOURCE_NAME"]
        or pod.get("service_account_name") != os.environ["EXPECTED_RESOURCE_NAME"]
        or pod.get("automount_service_account_token") is not False
        or pod.get("security_context") != {"capabilities": {"add": ["NET_ADMIN"]}}
    ):
        raise SystemExit(1)
    mounts = {(item.get("name"), item.get("mountPath"), bool(item.get("readOnly", False))) for item in pod.get("volume_mounts") or []}
    if mounts != {("xtables-lock", "/run/xtables.lock", False), ("config-volume", "/etc/coredns", False)}:
        raise SystemExit(1)
    volumes = {item.get("name"): item for item in pod.get("volumes") or []}
    if set(volumes) != {"xtables-lock", "config-volume"}:
        raise SystemExit(1)
    if (volumes["xtables-lock"].get("hostPath") or {}) != {"path": "/run/xtables.lock", "type": "FileOrCreate"}:
        raise SystemExit(1)
'
}

node_local_dns_validate_preserved_pod_snapshot() {
  local pods_json="$1"
  local target_nodes="$2"
  local expected_mode="$3"
  local service_ip="${4:-}"
  local expected_image="${FUGUE_NODE_LOCAL_DNS_IMAGE_REPOSITORY}@${FUGUE_NODE_LOCAL_DNS_IMAGE_DIGEST}"
  local expected_listen_ips="${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}"

  if [[ "${expected_mode}" == "iptables" ]]; then
    [[ -n "${service_ip}" ]] || return 1
    expected_listen_ips="${FUGUE_NODE_LOCAL_DNS_LOCAL_IP},${service_ip}"
  fi
  PODS_JSON="${pods_json}" TARGET_NODES="${target_nodes}" EXPECTED_MODE="${expected_mode}" \
    EXPECTED_IMAGE="${expected_image}" EXPECTED_LISTEN_IPS="${expected_listen_ips}" \
    EXPECTED_UPSTREAM="${NODE_LOCAL_DNS_UPSTREAM_SERVICE_NAME}" EXPECTED_CONFIG_MAP="${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME}" \
    python3 -c '
import json
import os

pods = json.loads(os.environ["PODS_JSON"])
targets = [item.strip() for item in os.environ["TARGET_NODES"].splitlines() if item.strip()]
if os.environ["EXPECTED_MODE"] not in {"shadow", "iptables"} or not targets or len(set(targets)) != len(targets):
    raise SystemExit(1)
by_node = {}
for pod in pods:
    node = str(pod.get("node") or "").strip()
    if not node or node in by_node:
        raise SystemExit(1)
    by_node[node] = pod
if set(by_node) != set(targets):
    raise SystemExit(1)
expected_args = ["-localip", os.environ["EXPECTED_LISTEN_IPS"], "-conf", "/etc/Corefile", "-upstreamsvc", os.environ["EXPECTED_UPSTREAM"]]
allowed_items = (
    [{"key": "Corefile", "path": "Corefile.base"}],
    [{"key": "Corefile." + os.environ["EXPECTED_MODE"], "path": "Corefile.base"}],
)
for node in targets:
    pod = by_node[node]
    if (
        not pod.get("name")
        or not pod.get("uid")
        or pod.get("deletion_timestamp")
        or not pod.get("revision")
        or pod.get("phase") != "Running"
        or pod.get("ready")
        or pod.get("mode") != os.environ["EXPECTED_MODE"]
        or pod.get("image") != os.environ["EXPECTED_IMAGE"]
        or pod.get("args") != expected_args
        or pod.get("config_map_name") != os.environ["EXPECTED_CONFIG_MAP"]
        or pod.get("config_items") not in allowed_items
    ):
        raise SystemExit(1)
'
}

node_local_dns_verify_preserved_nodes_isolated() {
  local nodes_json=""

  [[ "${NODE_LOCAL_DNS_SPLIT_COHORT}" == "true" ]] || return 0
  nodes_json="$(${KUBECTL} get nodes -o json)" || return 1
  printf '%s' "${nodes_json}" | TARGET_NODES="${NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES}" python3 -c '
import json
import os
import sys

payload = json.load(sys.stdin)
targets = [item.strip() for item in os.environ["TARGET_NODES"].splitlines() if item.strip()]
nodes = {str((item.get("metadata") or {}).get("name") or ""): item for item in payload.get("items") or []}
if not targets or len(set(targets)) != len(targets) or any(name not in nodes for name in targets):
    raise SystemExit(1)
for name in targets:
    node = nodes[name]
    metadata = node.get("metadata") or {}
    labels = metadata.get("labels") or {}
    spec = node.get("spec") or {}
    status = node.get("status") or {}
    conditions = {str(item.get("type") or ""): str(item.get("status") or "") for item in status.get("conditions") or []}
    taints = spec.get("taints") or []
    isolated_taint = any(
        (item.get("key") == "fugue.io/node-unhealthy" and item.get("effect") == "NoSchedule")
        or (item.get("key") == "node.kubernetes.io/unreachable" and item.get("effect") in {"NoSchedule", "NoExecute"})
        for item in taints
    )
    role_values = [
        labels.get("fugue.io/role.edge"),
        labels.get("fugue.io/role.dns"),
        labels.get("fugue.io/role.app-runtime"),
        labels.get("fugue.io/role.builder"),
        labels.get("fugue.io/role.internal-maintenance"),
    ]
    if (
        conditions.get("Ready") == "True"
        or labels.get("fugue.io/schedulable") != "false"
        or any(str(value or "").lower() == "true" for value in role_values)
        or not isolated_taint
    ):
        raise SystemExit(1)
'
}

node_local_dns_snapshot_uses_current_layout() {
  local pods_json="$1"
  local expected_mode="$2"

  PODS_JSON="${pods_json}" EXPECTED_MODE="${expected_mode}" python3 -c '
import json
import os

expected = [{"key": "Corefile." + os.environ["EXPECTED_MODE"], "path": "Corefile.base"}]
pods = json.loads(os.environ["PODS_JSON"])
if not pods or any(pod.get("config_items") != expected for pod in pods):
    raise SystemExit(1)
'
}

node_local_dns_targets_json() {
  TARGET_NODES="$1" python3 -c '
import json
import os

values = [item.strip() for item in os.environ["TARGET_NODES"].splitlines() if item.strip()]
if not values or len(set(values)) != len(values):
    raise SystemExit(1)
print(json.dumps(values, separators=(",", ":")))
'
}

node_local_dns_pod_dns_host_port_inventory() {
  local node_name="$1"
  local output_mode="$2"

  {
    ${KUBECTL} get pods --all-namespaces --field-selector "spec.nodeName=${node_name}" -o json &&
      printf '\n' &&
      ${KUBECTL} get node "${node_name}" -o json
  } | OUTPUT_MODE="${output_mode}" LOCAL_IP="${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}" \
    SERVICE_IP="${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}" python3 -c '
import ipaddress
import json
import os
import sys

document = sys.stdin.read()
decoder = json.JSONDecoder()

def decode_next(offset):
    while offset < len(document) and document[offset].isspace():
        offset += 1
    if offset >= len(document):
        raise ValueError("missing JSON inventory document")
    return decoder.raw_decode(document, offset)

payload, offset = decode_next(0)
node, offset = decode_next(offset)
if document[offset:].strip():
    raise ValueError("unexpected trailing JSON inventory data")
external_ipv4 = set()
for address in (node.get("status") or {}).get("addresses") or []:
    if address.get("type") != "ExternalIP":
        continue
    try:
        parsed = ipaddress.ip_address(str(address.get("address") or "").strip())
    except ValueError:
        continue
    if parsed.version == 4:
        external_ipv4.add(str(parsed))
conflicts = []
scoped = []
for pod in payload.get("items") or []:
    metadata = pod.get("metadata") or {}
    status = pod.get("status") or {}
    if status.get("phase") in {"Succeeded", "Failed"}:
        continue
    spec = pod.get("spec") or {}
    host_network = bool(spec.get("hostNetwork"))
    for container in (spec.get("initContainers") or []) + (spec.get("containers") or []):
        for port in container.get("ports") or []:
            container_port = int(port.get("containerPort") or 0)
            host_port = int(port.get("hostPort") or (container_port if host_network else 0))
            protocol = str(port.get("protocol") or "TCP").upper()
            if host_port == 53 and protocol in {"TCP", "UDP"}:
                host_ip = str(port.get("hostIP") or "").strip()
                safe = False
                try:
                    parsed = ipaddress.ip_address(host_ip)
                    safe = parsed.version == 4 and str(parsed) in external_ipv4 and str(parsed) not in {os.environ["LOCAL_IP"], os.environ["SERVICE_IP"]}
                except ValueError:
                    safe = False
                owner = "{}/{}:{}/53".format(metadata.get("namespace", "default"), metadata.get("name", "<unknown>"), protocol)
                if safe:
                    ready = any(item.get("type") == "Ready" and item.get("status") == "True" for item in status.get("conditions") or [])
                    restarts = sum(int(item.get("restartCount") or 0) for item in status.get("containerStatuses") or [])
                    scoped.append((host_ip, protocol, owner, str(metadata.get("uid") or ""), str(restarts), "true" if ready else "false"))
                else:
                    conflicts.append(owner + ("@" + host_ip if host_ip else "@all-addresses"))
if os.environ["OUTPUT_MODE"] == "conflicts":
    print(" ".join(sorted(set(conflicts))))
elif os.environ["OUTPUT_MODE"] == "scoped":
    for row in sorted(set(scoped)):
        print("\t".join(row))
else:
    raise SystemExit(1)
'
}

node_local_dns_pod_dns_host_port_conflicts() {
  node_local_dns_pod_dns_host_port_inventory "$1" conflicts
}

node_local_dns_verify_scoped_hostport_rules() {
  local node_name="$1"
  local host_ip="$2"
  local script=""

  script="$(cat <<EOF
set -eu
command -v iptables-save >/dev/null 2>&1
iptables-save -t nat > /tmp/fugue-nld-cni-hostports.rules
for protocol in udp tcp; do
  rules="\$(grep -F -- '-A CNI-HOSTPORT-DNAT ' /tmp/fugue-nld-cni-hostports.rules | grep -F -- "-p \${protocol}" | grep -F -- '--dports 53' || true)"
  [ -n "\${rules}" ] || {
    echo "missing CNI hostPort \${protocol}/53 rule for ${host_ip}" >&2
    exit 1
  }
  while IFS= read -r rule; do
    [ -n "\${rule}" ] || continue
    printf '%s\n' "\${rule}" | grep -F -- '-d ${host_ip}/32' >/dev/null || {
      echo "unscoped CNI hostPort \${protocol}/53 rule: \${rule}" >&2
      exit 1
    }
  done <<RULES
\${rules}
RULES
done
if grep -F -- '-A CNI-HOSTPORT-DNAT ' /tmp/fugue-nld-cni-hostports.rules | grep -E -- '-p (udp|tcp)' | grep -F -- '--dports 53' | grep -E -- '-d (${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}|${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP})/32' >/dev/null; then
  echo 'CNI hostPort 53 overlaps a NodeLocal DNSCache address' >&2
  exit 1
fi
EOF
)"
  node_local_dns_run_probe_pod "${node_name}" hostport-scope "${FUGUE_NODE_LOCAL_DNS_IMAGE_REPOSITORY}@${FUGUE_NODE_LOCAL_DNS_IMAGE_DIGEST}" true true "${script}"
}

node_local_dns_verify_artifact() {
  local expected_mode="$1"
  local service_ip="$2"
  local require_current_layout="${3:-false}"
  local daemonset_json=""
  local configmap_json=""
  local target_nodes_json=""
  local expected_image="${FUGUE_NODE_LOCAL_DNS_IMAGE_REPOSITORY}@${FUGUE_NODE_LOCAL_DNS_IMAGE_DIGEST}"
  local expected_listen_ips="${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}"
  local expected_bind_ips="${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}"
  local upstream_service="${NODE_LOCAL_DNS_UPSTREAM_SERVICE_NAME}"
  local workload_name="${NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME}"
  local resource_name="${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME}"
  local legacy_mode="${expected_mode}"

  [[ "${expected_mode}" == "shadow" || "${expected_mode}" == "iptables" ]] || return 1
  if [[ "${expected_mode}" == "iptables" ]]; then
    expected_listen_ips="${FUGUE_NODE_LOCAL_DNS_LOCAL_IP},${service_ip}"
    expected_bind_ips="${FUGUE_NODE_LOCAL_DNS_LOCAL_IP} ${service_ip}"
  fi
  if [[ "${NODE_LOCAL_DNS_SPLIT_COHORT}" == "true" ]]; then
    legacy_mode="${NODE_LOCAL_DNS_PRESERVED_MODE}"
  fi
  target_nodes_json="$(node_local_dns_targets_json "${NODE_LOCAL_DNS_TARGET_NODES}")" || return 1
  daemonset_json="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get daemonset "${NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME}" -o json 2>/dev/null || true)"
  [[ -n "${daemonset_json}" ]] || return 1
  if ! DAEMONSET_JSON="${daemonset_json}" EXPECTED_IMAGE="${expected_image}" \
    EXPECTED_LISTEN_IPS="${expected_listen_ips}" EXPECTED_MODE="${expected_mode}" \
    EXPECTED_UPSTREAM="${upstream_service}" EXPECTED_TARGETS_JSON="${target_nodes_json}" \
    EXPECTED_WORKLOAD_NAME="${workload_name}" EXPECTED_RESOURCE_NAME="${resource_name}" \
    EXPECTED_COMPONENT="${NODE_LOCAL_DNS_ACTIVE_COMPONENT}" EXPECTED_PULL_POLICY="${FUGUE_NODE_LOCAL_DNS_IMAGE_PULL_POLICY}" \
    REQUIRE_CURRENT_LAYOUT="${require_current_layout}" python3 -c '
import json
import os

daemonset = json.loads(os.environ["DAEMONSET_JSON"])
daemonset_metadata = daemonset.get("metadata") or {}
template = ((daemonset.get("spec") or {}).get("template") or {})
metadata = template.get("metadata") or {}
labels = metadata.get("labels") or {}
spec = template.get("spec") or {}
containers = spec.get("containers") or []
if (
    daemonset_metadata.get("name") != os.environ["EXPECTED_WORKLOAD_NAME"]
    or labels.get("app.kubernetes.io/component") != os.environ["EXPECTED_COMPONENT"]
    or labels.get("fugue.io/node-local-dns-mode") != os.environ["EXPECTED_MODE"]
    or len(containers) != 1
):
    raise SystemExit(1)
container = containers[0]
expected_args = ["-localip", os.environ["EXPECTED_LISTEN_IPS"], "-conf", "/etc/Corefile", "-upstreamsvc", os.environ["EXPECTED_UPSTREAM"]]
if container.get("name") != "node-cache" or container.get("image") != os.environ["EXPECTED_IMAGE"] or container.get("args") != expected_args:
    raise SystemExit(1)
if container.get("imagePullPolicy") != os.environ["EXPECTED_PULL_POLICY"] or container.get("ports"):
    raise SystemExit(1)
selector = spec.get("nodeSelector") or {}
expected_targets = json.loads(os.environ["EXPECTED_TARGETS_JSON"])
current_layout = selector == {"kubernetes.io/os": "linux"}
if current_layout:
    expected_affinity = {
        "nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [{
                    "matchExpressions": [{
                        "key": "kubernetes.io/hostname",
                        "operator": "In",
                        "values": expected_targets,
                    }],
                }],
            },
        },
    }
    if spec.get("affinity") != expected_affinity:
        raise SystemExit(1)
    if (daemonset.get("spec") or {}).get("updateStrategy") != {"type": "OnDelete"}:
        raise SystemExit(1)
    if os.environ["REQUIRE_CURRENT_LAYOUT"] == "true" and labels.get("fugue.io/node-local-dns-cohort") != "active":
        raise SystemExit(1)
else:
    legacy_layout = len(expected_targets) == 1 and selector == {"kubernetes.io/os": "linux", "kubernetes.io/hostname": expected_targets[0]}
    if not legacy_layout or os.environ["REQUIRE_CURRENT_LAYOUT"] == "true":
        raise SystemExit(1)
if spec.get("hostNetwork") is not True or spec.get("dnsPolicy") != "Default":
    raise SystemExit(1)
if spec.get("priorityClassName") != os.environ["EXPECTED_RESOURCE_NAME"] or spec.get("serviceAccountName") != os.environ["EXPECTED_RESOURCE_NAME"]:
    raise SystemExit(1)
if spec.get("automountServiceAccountToken") is not False:
    raise SystemExit(1)
security = container.get("securityContext") or {}
if security.get("privileged") is True or (security.get("capabilities") or {}).get("add") != ["NET_ADMIN"]:
    raise SystemExit(1)
mounts = {(item.get("name"), item.get("mountPath"), bool(item.get("readOnly", False))) for item in container.get("volumeMounts") or []}
if mounts != {("xtables-lock", "/run/xtables.lock", False), ("config-volume", "/etc/coredns", False)}:
    raise SystemExit(1)
volumes = {item.get("name"): item for item in spec.get("volumes") or []}
if set(volumes) != {"xtables-lock", "config-volume"}:
    raise SystemExit(1)
host_path = volumes["xtables-lock"].get("hostPath") or {}
if host_path.get("path") != "/run/xtables.lock" or host_path.get("type") != "FileOrCreate":
    raise SystemExit(1)
config_map = volumes["config-volume"].get("configMap") or {}
expected_key = "Corefile." + os.environ["EXPECTED_MODE"] if current_layout else "Corefile"
if config_map.get("name") != os.environ["EXPECTED_RESOURCE_NAME"] or config_map.get("items") != [{"key": expected_key, "path": "Corefile.base"}]:
    raise SystemExit(1)
'; then
    return 1
  fi

  configmap_json="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get configmap "${resource_name}" -o json 2>/dev/null || true)"
  [[ -n "${configmap_json}" ]] || return 1
  if ! CONFIGMAP_JSON="${configmap_json}" CLUSTER_DOMAIN="${FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN}" LOCAL_IP="${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}" \
    SERVICE_IP="${service_ip}" EXPECTED_MODE="${expected_mode}" EXPECTED_LEGACY_MODE="${legacy_mode}" \
    REQUIRE_CURRENT_LAYOUT="${require_current_layout}" python3 -c '
import json
import os

def normalize(value):
    return "\n".join(line.strip() for line in value.splitlines() if line.strip())

def expected_corefile(bind_ips):
    domain = os.environ["CLUSTER_DOMAIN"]
    local_ip = os.environ["LOCAL_IP"]
    return f"""
{domain}:53 {{
errors
cache {{
success 9984 30
denial 9984 5
}}
reload
loop
bind {bind_ips}
forward . __PILLAR__CLUSTER__DNS__ {{
force_tcp
}}
prometheus :9253
health {local_ip}:8080
}}
in-addr.arpa:53 {{
errors
cache 30
reload
loop
bind {bind_ips}
forward . __PILLAR__CLUSTER__DNS__ {{
force_tcp
}}
prometheus :9253
}}
ip6.arpa:53 {{
errors
cache 30
reload
loop
bind {bind_ips}
forward . __PILLAR__CLUSTER__DNS__ {{
force_tcp
}}
prometheus :9253
}}
.:53 {{
errors
cache 30
reload
loop
bind {bind_ips}
forward . __PILLAR__CLUSTER__DNS__ {{
force_tcp
}}
prometheus :9253
}}
"""

data = (json.loads(os.environ["CONFIGMAP_JSON"]).get("data") or {})
shadow = expected_corefile(os.environ["LOCAL_IP"])
iptables = expected_corefile(os.environ["LOCAL_IP"] + " " + os.environ["SERVICE_IP"])
selected = shadow if os.environ["EXPECTED_LEGACY_MODE"] == "shadow" else iptables
has_current = bool(data.get("Corefile.shadow")) or bool(data.get("Corefile.iptables"))
if has_current:
    if normalize(data.get("Corefile.shadow", "")) != normalize(shadow):
        raise SystemExit(1)
    if normalize(data.get("Corefile.iptables", "")) != normalize(iptables):
        raise SystemExit(1)
    if normalize(data.get("Corefile", "")) != normalize(selected):
        raise SystemExit(1)
elif os.environ["REQUIRE_CURRENT_LAYOUT"] == "true" or normalize(data.get("Corefile", "")) != normalize(selected):
    raise SystemExit(1)
'; then
    return 1
  fi
  return 0
}

node_local_dns_verify_shadow_artifact() {
  node_local_dns_verify_artifact shadow "$1"
}

node_local_dns_capture_authoritative_hostport_snapshot() {
  local target_nodes="$1"
  local node_name=""
  local host_port_conflicts=""
  local scoped_host_ports=""
  local scoped_host_ip=""
  local scoped_protocol=""
  local scoped_owner=""
  local scoped_uid=""
  local scoped_restarts=""
  local scoped_ready=""
  local scoped_row=""

  NODE_LOCAL_DNS_HOSTPORT_POD_SNAPSHOT=""
  while IFS= read -r node_name; do
    [[ -n "${node_name}" ]] || continue
    host_port_conflicts="$(node_local_dns_pod_dns_host_port_conflicts "${node_name}")" || return 1
    [[ -z "$(trim_field "${host_port_conflicts}")" ]] || return 1
    scoped_host_ports="$(node_local_dns_pod_dns_host_port_inventory "${node_name}" scoped)" || return 1
    while IFS=$'\t' read -r scoped_host_ip scoped_protocol scoped_owner scoped_uid scoped_restarts scoped_ready; do
      [[ -n "${scoped_host_ip}" ]] || continue
      [[ -n "${scoped_uid}" && "${scoped_ready}" == "true" ]] || return 1
      scoped_row="${node_name}"$'\t'"${scoped_host_ip}"$'\t'"${scoped_protocol}"$'\t'"${scoped_owner}"$'\t'"${scoped_uid}"$'\t'"${scoped_restarts}"$'\t'"${scoped_ready}"
      NODE_LOCAL_DNS_HOSTPORT_POD_SNAPSHOT+="${NODE_LOCAL_DNS_HOSTPORT_POD_SNAPSHOT:+$'\n'}${scoped_row}"
    done <<<"${scoped_host_ports}"
    while IFS= read -r scoped_host_ip; do
      [[ -n "${scoped_host_ip}" ]] || continue
      node_local_dns_verify_scoped_hostport_rules "${node_name}" "${scoped_host_ip}" || return 1
    done < <(cut -f1 <<<"${scoped_host_ports}" | sed '/^[[:space:]]*$/d' | sort -u)
  done <<<"${target_nodes}"
}

node_local_dns_refresh_authoritative_hostport_snapshot() {
  local target_nodes="$1"
  local phase="${2:-release}"

  [[ -n "$(trim_field "${target_nodes}")" ]] || return 0
  if ! node_local_dns_capture_authoritative_hostport_snapshot "${target_nodes}"; then
    log "cannot refresh the authoritative DNS hostPort baseline after the ${phase} transaction"
    return 1
  fi
  if ! node_local_dns_verify_authoritative_cohort "${target_nodes}"; then
    log "refreshed authoritative DNS hostPort baseline failed UDP/TCP coexistence verification after the ${phase} transaction"
    return 1
  fi
  log "refreshed the authoritative DNS hostPort baseline after the verified ${phase} transaction"
}

prepare_node_local_dns_helm_args() {
  local service_data=""
  local live_daemonset_ref=""
  local live_daemonset_json=""
  local kube_dns_service_ip=""
  local upstream_selector_json=""
  local endpoint_count="0"
  local coredns_corefile=""
  local node_name=""
  local ready=""
  local node_os=""
  local node_arch=""
  local node_unschedulable=""
  local desired_nodes=""
  local desired_node_count="0"
  local previous_node_count="0"
  local added_nodes=""
  local removed_nodes=""
  local overlapping_nodes=""
  local node_selector_json=""
  local target_nodes_json=""
  local preserved_nodes_json="[]"
  local previous_layout_current="false"
  local preserved_daemonset_ref=""
  local preserved_target_nodes=""
  local split_active_ref=""
  local possible_split_active_name=""

  node_local_dns_configure_cohort_names
  if [[ "${NODE_LOCAL_DNS_SPLIT_COHORT}" == "true" ]]; then
    if ! preserved_daemonset_ref="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get daemonset "${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME}" --ignore-not-found -o name)"; then
      fail "cannot determine the preserved NodeLocal DNSCache DaemonSet state before preparing the release"
    fi
    [[ -n "$(trim_field "${preserved_daemonset_ref}")" ]] || fail "preserved offline NodeLocal DNSCache nodes require the existing ${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME} DaemonSet"
    if ! NODE_LOCAL_DNS_PRESERVED_DAEMONSET_JSON="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get daemonset "${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME}" -o json)"; then
      fail "cannot capture the preserved NodeLocal DNSCache DaemonSet before preparing the release"
    fi
    if ! preserved_target_nodes="$(node_local_dns_daemonset_target_nodes "${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_JSON}")"; then
      fail "cannot inspect the exact preserved NodeLocal DNSCache target cohort"
    fi
    node_local_dns_same_node_set "${preserved_target_nodes}" "${NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES}" || fail "configured preserved offline NodeLocal DNSCache nodes do not exactly match the live legacy DaemonSet target cohort"
    NODE_LOCAL_DNS_PRESERVED_MODE="$(node_local_dns_daemonset_template_mode "${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_JSON}")" || fail "cannot inspect the preserved NodeLocal DNSCache mode"
    [[ "${NODE_LOCAL_DNS_PRESERVED_MODE}" == "shadow" || "${NODE_LOCAL_DNS_PRESERVED_MODE}" == "iptables" ]] || fail "preserved NodeLocal DNSCache mode is missing or unsupported"
    if ! NODE_LOCAL_DNS_PRESERVED_PODS_JSON="$(node_local_dns_capture_preserved_pods_json)"; then
      fail "cannot capture the preserved offline NodeLocal DNSCache Pods before preparing the release"
    fi
  else
    NODE_LOCAL_DNS_PRESERVED_MODE=""
    NODE_LOCAL_DNS_PRESERVED_PODS_JSON="[]"
    NODE_LOCAL_DNS_PRESERVED_DAEMONSET_JSON=""
    possible_split_active_name="${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME:0:56}"
    possible_split_active_name="${possible_split_active_name%-}-active"
    if ! split_active_ref="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get daemonset "${possible_split_active_name}" --ignore-not-found -o name)"; then
      fail "cannot determine whether a split NodeLocal DNSCache cohort already exists"
    fi
    [[ -z "$(trim_field "${split_active_ref}")" ]] || fail "live split NodeLocal DNSCache state requires FUGUE_NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES; refusing to collapse cohorts"
  fi

  if ! live_daemonset_ref="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get daemonset "${NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME}" --ignore-not-found -o name)"; then
    fail "cannot determine the live active NodeLocal DNSCache state before preparing the release"
  fi
  if [[ -n "$(trim_field "${live_daemonset_ref}")" ]]; then
    NODE_LOCAL_DNS_PREVIOUS_ENABLED="true"
    if ! live_daemonset_json="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get daemonset "${NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME}" -o json)"; then
      fail "cannot capture the live active NodeLocal DNSCache DaemonSet before preparing the release"
    fi
    if ! NODE_LOCAL_DNS_PREVIOUS_MODE="$(printf '%s' "${live_daemonset_json}" | python3 -c 'import json,sys; print((((json.load(sys.stdin).get("spec") or {}).get("template") or {}).get("metadata") or {}).get("labels",{}).get("fugue.io/node-local-dns-mode", ""))')"; then
      fail "cannot inspect the live active NodeLocal DNSCache mode before preparing the release"
    fi
    [[ "${NODE_LOCAL_DNS_PREVIOUS_MODE}" == "shadow" || "${NODE_LOCAL_DNS_PREVIOUS_MODE}" == "iptables" ]] || fail "live active NodeLocal DNSCache mode is missing or unsupported"
    if ! NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES="$(node_local_dns_daemonset_target_nodes "${live_daemonset_json}")"; then
      fail "cannot inspect the exact live active NodeLocal DNSCache target cohort"
    fi
    [[ -n "$(trim_field "${NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES}")" ]] || fail "live active NodeLocal DNSCache target cohort is empty"
    if ! NODE_LOCAL_DNS_PREVIOUS_PODS_JSON="$(node_local_dns_capture_pods_json)"; then
      fail "cannot capture the live active NodeLocal DNSCache Pods before preparing the release"
    fi
    if ! node_local_dns_validate_pure_pod_snapshot "${NODE_LOCAL_DNS_PREVIOUS_PODS_JSON}" "${NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES}" "${NODE_LOCAL_DNS_PREVIOUS_MODE}"; then
      fail "live active NodeLocal DNSCache Pods are not a pure, Ready ${NODE_LOCAL_DNS_PREVIOUS_MODE} cohort; refusing a mixed-state release"
    fi
    if node_local_dns_snapshot_uses_current_layout "${NODE_LOCAL_DNS_PREVIOUS_PODS_JSON}" "${NODE_LOCAL_DNS_PREVIOUS_MODE}"; then
      previous_layout_current="true"
    fi
  else
    NODE_LOCAL_DNS_PREVIOUS_ENABLED="false"
    NODE_LOCAL_DNS_PREVIOUS_MODE=""
    NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES=""
    NODE_LOCAL_DNS_PREVIOUS_PODS_JSON="[]"
  fi
  NODE_LOCAL_DNS_HELM_SET_ARGS=(--set "nodeLocalDNS.enabled=${FUGUE_NODE_LOCAL_DNS_ENABLED}")
  if [[ "${FUGUE_NODE_LOCAL_DNS_ENABLED}" != "true" ]]; then
    if [[ "${NODE_LOCAL_DNS_PREVIOUS_ENABLED}" == "true" ]]; then
      NODE_LOCAL_DNS_TARGET_NODES="${NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES}"
      if ! NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get service "${FUGUE_NODE_LOCAL_DNS_KUBE_DNS_SERVICE_NAME}" -o jsonpath='{.spec.clusterIP}')"; then
        fail "cannot inspect the live kube-dns ServiceIP before disabling NodeLocal DNSCache"
      fi
      [[ "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "cannot safely disable NodeLocal DNSCache because the live kube-dns ServiceIP is unavailable"
      command_exists python3 || fail "python3 is required for NodeLocal DNSCache disable preflight"
      if ! node_local_dns_capture_authoritative_hostport_snapshot "${NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES}"; then
        fail "cannot pin the authoritative DNS coexistence state before disabling NodeLocal DNSCache"
      fi
      if ! node_local_dns_verify_artifact "${NODE_LOCAL_DNS_PREVIOUS_MODE}" "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}"; then
        fail "live NodeLocal DNSCache artifact differs from its declared mode before disable"
      fi
      if ! node_local_dns_verify_running "${NODE_LOCAL_DNS_PREVIOUS_MODE}" "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}"; then
        fail "live NodeLocal DNSCache cohort is not healthy enough for a reversible disable"
      fi
    fi
    return 0
  fi

  command_exists python3 || fail "python3 is required for NodeLocal DNSCache preflight"
  case "${FUGUE_NODE_LOCAL_DNS_MODE}" in
    shadow|iptables) ;;
    *) fail "FUGUE_NODE_LOCAL_DNS_MODE must be shadow or iptables" ;;
  esac
  service_data="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get service "${FUGUE_NODE_LOCAL_DNS_KUBE_DNS_SERVICE_NAME}" -o json 2>/dev/null || true)"
  [[ -n "${service_data}" ]] || fail "NodeLocal DNSCache requires service/${FUGUE_NODE_LOCAL_DNS_KUBE_DNS_SERVICE_NAME} in ${FUGUE_NODE_LOCAL_DNS_NAMESPACE}"
  IFS=$'\t' read -r kube_dns_service_ip upstream_selector_json < <(
    printf '%s' "${service_data}" | python3 -c '
import json
import sys

service = json.load(sys.stdin)
spec = service.get("spec") or {}
cluster_ip = str(spec.get("clusterIP") or "").strip()
selector = spec.get("selector") or {}
print(cluster_ip + "\t" + json.dumps(selector, sort_keys=True, separators=(",", ":")))
'
  )
  [[ "${kube_dns_service_ip}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "live kube-dns Service has no IPv4 ClusterIP"
  NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP="${kube_dns_service_ip}"
  [[ "${upstream_selector_json}" != "{}" ]] || fail "live kube-dns Service has no selector for the central CoreDNS upstream"
  if [[ -n "$(trim_field "${FUGUE_NODE_LOCAL_DNS_EXPECTED_KUBE_DNS_SERVICE_IP:-}")" &&
        "${FUGUE_NODE_LOCAL_DNS_EXPECTED_KUBE_DNS_SERVICE_IP}" != "${kube_dns_service_ip}" ]]; then
    fail "live kube-dns ServiceIP ${kube_dns_service_ip} differs from expected ${FUGUE_NODE_LOCAL_DNS_EXPECTED_KUBE_DNS_SERVICE_IP}"
  fi

  endpoint_count="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get endpoints "${FUGUE_NODE_LOCAL_DNS_KUBE_DNS_SERVICE_NAME}" -o json 2>/dev/null |
    python3 -c 'import json,sys; payload=json.load(sys.stdin); print(sum(len(s.get("addresses") or []) for s in payload.get("subsets") or []))' 2>/dev/null || printf '0')"
  [[ "${endpoint_count}" =~ ^[0-9]+$ ]] && (( endpoint_count > 0 )) || fail "central CoreDNS has no Ready kube-dns endpoints"

  coredns_corefile="$(${KUBECTL} -n "${FUGUE_COREDNS_NAMESPACE}" get configmap "${FUGUE_NODE_LOCAL_DNS_COREDNS_CONFIGMAP_NAME}" -o jsonpath='{.data.Corefile}' 2>/dev/null || true)"
  [[ -n "${coredns_corefile}" ]] || fail "central CoreDNS Corefile is unavailable"
  if ! grep -Eq "kubernetes[[:space:]]+${FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN}([[:space:]]|$)" <<<"${coredns_corefile}"; then
    fail "central CoreDNS Corefile does not serve ${FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN}"
  fi

  if [[ "${NODE_LOCAL_DNS_SPLIT_COHORT}" == "true" ]]; then
    if ! node_local_dns_verify_preserved_nodes_isolated; then
      fail "preserved offline NodeLocal DNSCache nodes are not fully isolated from scheduling and runtime roles"
    fi
    if ! node_local_dns_validate_preserved_pod_snapshot \
      "${NODE_LOCAL_DNS_PRESERVED_PODS_JSON}" "${NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES}" "${NODE_LOCAL_DNS_PRESERVED_MODE}" "${kube_dns_service_ip}"; then
      fail "preserved offline NodeLocal DNSCache Pods do not exactly match the live legacy mode and immutable configuration"
    fi
  fi

  desired_nodes="$(node_local_dns_candidate_nodes)"
  if [[ "${NODE_LOCAL_DNS_SPLIT_COHORT}" == "true" ]]; then
    if [[ -z "$(trim_field "${NODE_LOCAL_DNS_PREFLIGHT_TARGET_NODES:-}")" ]]; then
      NODE_LOCAL_DNS_PREFLIGHT_TARGET_NODES="${desired_nodes}"
    elif ! node_local_dns_same_node_set "${desired_nodes}" "${NODE_LOCAL_DNS_PREFLIGHT_TARGET_NODES}"; then
      fail "active NodeLocal DNSCache target cohort changed after release preflight"
    fi
  fi
  NODE_LOCAL_DNS_TARGET_NODES="${desired_nodes}"
  desired_node_count="$(wc -l <<<"${desired_nodes}" | awk '{print $1}')"
  [[ -n "$(trim_field "${desired_nodes}")" ]] && (( desired_node_count > 0 )) || fail "NodeLocal DNSCache selector has no Ready linux nodes"
  if [[ "${NODE_LOCAL_DNS_SPLIT_COHORT}" == "true" ]]; then
    overlapping_nodes="$(comm -12 \
      <(printf '%s\n' "${desired_nodes}" | sed '/^[[:space:]]*$/d' | sort -u) \
      <(printf '%s\n' "${NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES}" | sed '/^[[:space:]]*$/d' | sort -u))"
    [[ -z "$(trim_field "${overlapping_nodes}")" ]] || fail "active and preserved offline NodeLocal DNSCache cohorts overlap: $(paste -sd, <<<"${overlapping_nodes}")"
  fi
  NODE_LOCAL_DNS_HOSTPORT_POD_SNAPSHOT=""
  if [[ "${FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES}" != "true" && "${desired_node_count}" != "1" ]]; then
    fail "multi-node NodeLocal DNSCache cohorts require FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES=true"
  fi
  if [[ "${NODE_LOCAL_DNS_PREVIOUS_ENABLED}" != "true" ]]; then
    [[ "${FUGUE_NODE_LOCAL_DNS_MODE}" == "shadow" && "${desired_node_count}" == "1" ]] || fail "the first NodeLocal DNSCache release must be a single-node shadow canary"
    NODE_LOCAL_DNS_ADDED_NODES="${desired_nodes}"
  else
    previous_node_count="$(printf '%s\n' "${NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES}" | sed '/^[[:space:]]*$/d' | wc -l | awk '{print $1}')"
    if [[ "${previous_layout_current}" != "true" && "${FUGUE_NODE_LOCAL_DNS_MODE}" != "${NODE_LOCAL_DNS_PREVIOUS_MODE}" ]]; then
      fail "legacy NodeLocal DNSCache Corefile layout must be migrated without changing mode in the same release"
    fi
    added_nodes="$(node_local_dns_set_difference "${desired_nodes}" "${NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES}")"
    removed_nodes="$(node_local_dns_set_difference "${NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES}" "${desired_nodes}")"
    [[ -z "$(trim_field "${removed_nodes}")" ]] || fail "NodeLocal DNSCache target removal requires an isolated teardown workflow; refusing to remove $(paste -sd, <<<"${removed_nodes}")"
    NODE_LOCAL_DNS_ADDED_NODES="${added_nodes}"
    if [[ -n "$(trim_field "${added_nodes}")" ]]; then
      [[ "$(printf '%s\n' "${added_nodes}" | sed '/^[[:space:]]*$/d' | wc -l | awk '{print $1}')" == "1" ]] || fail "NodeLocal DNSCache shadow expansion may add exactly one node per release"
      [[ "${desired_node_count}" == "$((previous_node_count + 1))" ]] || fail "NodeLocal DNSCache target expansion must preserve the complete previous cohort"
      [[ "${NODE_LOCAL_DNS_PREVIOUS_MODE}" == "shadow" && "${FUGUE_NODE_LOCAL_DNS_MODE}" == "shadow" ]] || fail "NodeLocal DNSCache target expansion requires a pure shadow cohort before and after the release"
      [[ "${previous_layout_current}" == "true" ]] || fail "NodeLocal DNSCache target expansion requires every existing shadow Pod to use the mode-specific Corefile layout"
    else
      [[ "${desired_node_count}" == "${previous_node_count}" ]] || fail "NodeLocal DNSCache target cohort changed without one exact additive node"
    fi
  fi
  while IFS= read -r node_name; do
    [[ -n "${node_name}" ]] || continue
    ready="$(${KUBECTL} get node "${node_name}" -o jsonpath='{range .status.conditions[?(@.type=="Ready")]}{.status}{end}' 2>/dev/null || true)"
    node_os="$(${KUBECTL} get node "${node_name}" -o jsonpath='{.metadata.labels.kubernetes\.io/os}' 2>/dev/null || true)"
    node_arch="$(${KUBECTL} get node "${node_name}" -o jsonpath='{.metadata.labels.kubernetes\.io/arch}' 2>/dev/null || true)"
    node_unschedulable="$(${KUBECTL} get node "${node_name}" -o jsonpath='{.spec.unschedulable}' 2>/dev/null || true)"
    [[ "${ready}" == "True" && "${node_os}" == "linux" && "${node_arch}" == "amd64" && "${node_unschedulable}" != "true" ]] || fail "NodeLocal DNSCache target ${node_name} must be a Ready, schedulable linux/amd64 node"
  done <<<"${desired_nodes}"
  if ! node_local_dns_capture_authoritative_hostport_snapshot "${desired_nodes}"; then
    fail "cannot pin the authoritative DNS hostPort coexistence state for the NodeLocal DNSCache target cohort"
  fi

  if [[ "${NODE_LOCAL_DNS_PREVIOUS_ENABLED}" == "true" ]]; then
    NODE_LOCAL_DNS_TARGET_NODES="${NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES}"
    if ! node_local_dns_verify_artifact "${NODE_LOCAL_DNS_PREVIOUS_MODE}" "${kube_dns_service_ip}"; then
      fail "live NodeLocal DNSCache artifact differs from its declared ${NODE_LOCAL_DNS_PREVIOUS_MODE} image, args, selector, or Corefile"
    fi
    if ! node_local_dns_verify_running "${NODE_LOCAL_DNS_PREVIOUS_MODE}" "${kube_dns_service_ip}"; then
      fail "live NodeLocal DNSCache cohort failed DNS, metrics, host-network, or rule verification"
    fi
    NODE_LOCAL_DNS_TARGET_NODES="${desired_nodes}"
  fi

  node_selector_json="$(python3 -c '
import json
print(json.dumps({"kubernetes.io/os": "linux"}, sort_keys=True, separators=(",", ":")))
')"
  target_nodes_json="$(node_local_dns_targets_json "${desired_nodes}")" || fail "cannot encode the NodeLocal DNSCache target cohort"
  if [[ "${NODE_LOCAL_DNS_SPLIT_COHORT}" == "true" ]]; then
    preserved_nodes_json="$(node_local_dns_targets_json "${NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES}")" || fail "cannot encode the preserved offline NodeLocal DNSCache cohort"
  fi

  NODE_LOCAL_DNS_HELM_SET_ARGS+=(
    --set-string "nodeLocalDNS.mode=${FUGUE_NODE_LOCAL_DNS_MODE}"
    --set-string "nodeLocalDNS.legacyMode=${NODE_LOCAL_DNS_PRESERVED_MODE:-${FUGUE_NODE_LOCAL_DNS_MODE}}"
    --set-string "nodeLocalDNS.namespace=${FUGUE_NODE_LOCAL_DNS_NAMESPACE}"
    --set-string "nodeLocalDNS.localIP=${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}"
    --set-string "nodeLocalDNS.kubeDNSServiceIP=${kube_dns_service_ip}"
    --set-string "nodeLocalDNS.clusterDomain=${FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN}"
    --set-json "nodeLocalDNS.upstreamSelector=${upstream_selector_json}"
    --set-string "nodeLocalDNS.image.repository=${FUGUE_NODE_LOCAL_DNS_IMAGE_REPOSITORY}"
    --set-string "nodeLocalDNS.image.tag=${FUGUE_NODE_LOCAL_DNS_IMAGE_TAG}"
    --set-string "nodeLocalDNS.image.digest=${FUGUE_NODE_LOCAL_DNS_IMAGE_DIGEST}"
    --set-string "nodeLocalDNS.image.pullPolicy=${FUGUE_NODE_LOCAL_DNS_IMAGE_PULL_POLICY}"
    --set-json "nodeLocalDNS.nodeSelector=${node_selector_json}"
    # Helm recursively reuses legacy map keys, so explicitly shadow the retired
    # hostname selector. The chart accepts only this empty migration sentinel.
    --set-string "nodeLocalDNS.nodeSelector.kubernetes\\.io/hostname="
    --set-json "nodeLocalDNS.targetNodes=${target_nodes_json}"
    --set-json "nodeLocalDNS.preservedOfflineNodes=${preserved_nodes_json}"
    --set-string "nodeLocalDNS.updateStrategy.type=OnDelete"
  )
  log "NodeLocal DNSCache preflight passed: mode=${FUGUE_NODE_LOCAL_DNS_MODE} nodes=${desired_node_count} kube_dns=${kube_dns_service_ip} central_endpoints=${endpoint_count}"
}

node_local_dns_run_probe_pod() {
  local node_name="$1"
  local purpose="$2"
  local image_ref="$3"
  local host_network="$4"
  local net_admin="$5"
  local script="$6"
  local host_pid="${7:-false}"
  local safe_node=""
  local pod_name=""
  local phase=""
  local deadline=0
  local result=1

  safe_node="$(printf '%s' "${node_name}" | tr '[:upper:]_' '[:lower:]-' | tr -cd 'a-z0-9.-' | cut -c1-32 | sed 's/^[^a-z0-9]*//; s/[^a-z0-9]*$//')"
  pod_name="fugue-nld-${purpose}-${safe_node}-$$"
  pod_name="${pod_name:0:63}"
  POD_NAME="${pod_name}" NODE_NAME="${node_name}" POD_IMAGE="${image_ref}" POD_SCRIPT="${script}" \
    POD_HOST_NETWORK="${host_network}" POD_NET_ADMIN="${net_admin}" POD_TIMEOUT="${FUGUE_NODE_LOCAL_DNS_PROBE_TIMEOUT_SECONDS}" \
    POD_HOST_PID="${host_pid}" POD_NAMESPACE="${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" \
    python3 -c '
import json
import os

container = {
    "name": "probe",
    "image": os.environ["POD_IMAGE"],
    "imagePullPolicy": "IfNotPresent",
    "command": ["/bin/sh", "-ec", os.environ["POD_SCRIPT"]],
    "resources": {"requests": {"cpu": "1m", "memory": "4Mi"}, "limits": {"memory": "64Mi"}},
}
if os.environ.get("POD_NET_ADMIN") == "true":
    container["securityContext"] = {"capabilities": {"add": ["NET_ADMIN"]}}
manifest = {
    "apiVersion": "v1",
    "kind": "Pod",
    "metadata": {"name": os.environ["POD_NAME"], "namespace": os.environ["POD_NAMESPACE"], "labels": {"fugue.io/purpose": "node-local-dns-release-probe"}},
    "spec": {
        "automountServiceAccountToken": False,
        "restartPolicy": "Never",
        "activeDeadlineSeconds": int(os.environ["POD_TIMEOUT"]),
        "terminationGracePeriodSeconds": 1,
        "nodeName": os.environ["NODE_NAME"],
        "hostNetwork": os.environ.get("POD_HOST_NETWORK") == "true",
        "dnsPolicy": "Default" if os.environ.get("POD_HOST_NETWORK") == "true" else "ClusterFirst",
        "tolerations": [{"operator": "Exists", "effect": "NoSchedule"}, {"operator": "Exists", "effect": "NoExecute"}],
        "containers": [container],
    },
}
if os.environ.get("POD_HOST_PID") == "true":
    manifest["spec"]["hostPID"] = True
print(json.dumps(manifest, separators=(",", ":")))
' | ${KUBECTL} apply -f - >/dev/null

  deadline=$((SECONDS + FUGUE_NODE_LOCAL_DNS_PROBE_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    phase="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get pod "${pod_name}" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    case "${phase}" in
      Succeeded)
        result=0
        break
        ;;
      Failed)
        break
        ;;
    esac
    sleep 2
  done
  ${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" logs "${pod_name}" 2>/dev/null || true
  if (( result != 0 )); then
    ${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" describe pod "${pod_name}" 2>/dev/null || true
    log "NodeLocal DNSCache ${purpose} probe failed on ${node_name} with phase=${phase:-unknown}"
  fi
  ${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" delete pod "${pod_name}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  return "${result}"
}

node_local_dns_proc_ipv4_hex() {
  local address="$1"
  ADDRESS="${address}" python3 -c '
import ipaddress
import os

packed = ipaddress.IPv4Address(os.environ["ADDRESS"]).packed
print(packed[::-1].hex().upper())
'
}

node_local_dns_shadow_host_preflight() {
  local service_ip="$1"
  local local_hex=""
  local service_hex=""
  local node_name=""
  local live_nodes=""
  local script=""
  [[ "${FUGUE_NODE_LOCAL_DNS_MODE}" == "shadow" ]] || return 0
  if [[ "${NODE_LOCAL_DNS_PREVIOUS_ENABLED}" == "true" ]]; then
    live_nodes="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get pods -l "$(node_local_dns_active_pod_selector)" -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' 2>/dev/null | sort -u)"
  fi
  local_hex="$(node_local_dns_proc_ipv4_hex "${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}")"
  service_hex="$(node_local_dns_proc_ipv4_hex "${service_ip}")"
  read -r -d '' script <<EOF || true
set -eu
command -v iptables-save >/dev/null 2>&1
if [ -e /sys/class/net/nodelocaldns ]; then
  echo 'nodelocaldns interface already exists' >&2
  exit 1
fi
if grep -Fq '${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}' /proc/net/fib_trie 2>/dev/null; then
  echo 'NodeLocal link-local address is already present' >&2
  exit 1
fi
for table in /proc/net/tcp /proc/net/udp /proc/net/tcp6 /proc/net/udp6; do
  [ -r "\${table}" ] || continue
  while read -r slot endpoint remainder; do
    [ "\${slot}" = 'sl' ] && continue
    address="\${endpoint%:*}"
    port="\${endpoint##*:}"
    case "\${port}" in
      0035|1F90|2425|2489)
        case "\${address}" in
          00000000|00000000000000000000000000000000|${local_hex}|${service_hex})
            echo "conflicting listener \${endpoint} in \${table}" >&2
            exit 1
            ;;
        esac
        ;;
    esac
  done <"\${table}"
done
iptables-save -t nat > /tmp/fugue-nld-nat.rules 2>/dev/null || exit 1
for protocol in udp tcp; do
  if ! grep -F -- '-A KUBE-SERVICES ' /tmp/fugue-nld-nat.rules | grep -F -- '-d ${service_ip}/32' | grep -F -- "-p \${protocol}" | grep -F -- '--dport 53' | grep -E -- '-j KUBE-SVC-[A-Z0-9]+' >/dev/null; then
    echo "live kube-dns \${protocol}/53 is not implemented by kube-proxy iptables rules" >&2
    exit 1
  fi
done
if grep -E -- '-d ${service_ip}/32 .*--dport 53 .* -j DNAT .*--to-destination' /tmp/fugue-nld-nat.rules; then
  echo 'legacy kube-dns DNAT remains' >&2
  exit 1
fi
iptables-save > /tmp/fugue-nld-all.rules 2>/dev/null || exit 1
if grep -F 'NodeLocal DNS Cache:' /tmp/fugue-nld-all.rules; then
  echo 'stale NodeLocal DNSCache rules already exist' >&2
  exit 1
fi
EOF
  while IFS= read -r node_name; do
    [[ -n "${node_name}" ]] || continue
    if grep -Fqx "${node_name}" <<<"${live_nodes}"; then
      log "NodeLocal DNSCache shadow host preflight already satisfied by the live pod on ${node_name}"
      continue
    fi
    node_local_dns_run_probe_pod "${node_name}" preflight "${FUGUE_NODE_LOCAL_DNS_IMAGE_REPOSITORY}@${FUGUE_NODE_LOCAL_DNS_IMAGE_DIGEST}" true true "${script}" || return 1
  done <<<"${NODE_LOCAL_DNS_TARGET_NODES}"
}

node_local_dns_verify_one_node() {
  local pod_name="$1"
  local node_name="$2"
  local mode="$3"
  local service_ip="$4"
  local query_server="${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}"
  local pod_json=""
  local exec_script=""
  local probe_script=""

  [[ "${mode}" == "shadow" || "${mode}" == "iptables" ]] || return 1
  [[ "${mode}" == "shadow" ]] || query_server="${service_ip}"
  pod_json="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get pod "${pod_name}" -o json 2>/dev/null || true)"
  [[ -n "${pod_json}" ]] || return 1
  if ! POD_JSON="${pod_json}" EXPECTED_NODE="${node_name}" EXPECTED_MODE="${mode}" python3 -c '
import json
import os

pod = json.loads(os.environ["POD_JSON"])
metadata = pod.get("metadata") or {}
spec = pod.get("spec") or {}
status = pod.get("status") or {}
ready = any(item.get("type") == "Ready" and item.get("status") == "True" for item in status.get("conditions") or [])
container_statuses = status.get("containerStatuses") or []
if len(container_statuses) != 1 or container_statuses[0].get("name") != "node-cache":
    raise SystemExit(1)
container_status = container_statuses[0]
state = container_status.get("state") or {}
last_state = container_status.get("lastState") or {}
if (
    spec.get("nodeName") != os.environ["EXPECTED_NODE"]
    or (metadata.get("labels") or {}).get("fugue.io/node-local-dns-mode") != os.environ["EXPECTED_MODE"]
    or metadata.get("deletionTimestamp")
    or status.get("phase") != "Running"
    or not ready
    or int(container_status.get("restartCount") or 0) != 0
    or not state.get("running")
    or state.get("waiting")
    or state.get("terminated")
    or last_state.get("waiting")
    or last_state.get("terminated")
):
    raise SystemExit(1)
'; then
    return 1
  fi

  exec_script="$(cat <<EOF
set -eu
command -v iptables-save >/dev/null 2>&1
test -e /sys/class/net/nodelocaldns
iptables-save -t nat > /tmp/fugue-nld-kube-proxy.rules 2>/dev/null || exit 1
for protocol in udp tcp; do
  if ! grep -F -- '-A KUBE-SERVICES ' /tmp/fugue-nld-kube-proxy.rules | grep -F -- '-d ${service_ip}/32' | grep -F -- "-p \${protocol}" | grep -F -- '--dport 53' | grep -E -- '-j KUBE-SVC-[A-Z0-9]+' >/dev/null; then
    echo "live kube-dns \${protocol}/53 iptables service rule disappeared" >&2
    exit 1
  fi
done
for attempt in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50; do
  iptables-save > /tmp/fugue-nld-running.rules 2>/dev/null || exit 1
  if grep -F 'NodeLocal DNS Cache:' /tmp/fugue-nld-running.rules >/dev/null; then
    break
  fi
  sleep 2
done
grep -F 'NodeLocal DNS Cache:' /tmp/fugue-nld-running.rules >/dev/null
if [ '${mode}' = 'shadow' ]; then
  if grep -F 'NodeLocal DNS Cache:' /tmp/fugue-nld-running.rules | grep -F '${service_ip}' >/dev/null; then
    echo 'shadow mode retained NodeLocal rules for the kube-dns ServiceIP' >&2
    exit 1
  fi
  if grep -Fq '${service_ip}' /proc/net/fib_trie 2>/dev/null; then
    echo 'shadow mode retained the kube-dns ServiceIP as a local address' >&2
    exit 1
  fi
else
  grep -F 'NodeLocal DNS Cache:' /tmp/fugue-nld-running.rules | grep -F '${service_ip}' >/dev/null
  grep -Fq '${service_ip}' /proc/net/fib_trie
fi
EOF
)"
  probe_script="$(cat <<EOF
set -eu
metric_total() {
  metric_name="\$1"
  metric_file="\$2"
  marker='server="dns://${query_server}:53"'
  awk -v metric="\${metric_name}" -v marker="\${marker}" 'index(\$0, metric) == 1 && index(\$0, marker) {sum += \$NF} END {print sum + 0}' "\${metric_file}"
}
dns_tcp_query() {
  query_name="\$1"
  : > /tmp/fugue-nld-tcp-query
  {
    printf '\\022\\064\\001\\000\\000\\001\\000\\000\\000\\000\\000\\000'
    old_ifs="\${IFS}"
    IFS=.
    set -- \${query_name}
    IFS="\${old_ifs}"
    for label in "\$@"; do
      length="\${#label}"
      [ "\${length}" -ge 1 ] && [ "\${length}" -le 63 ]
      printf "\\\\\$(printf '%03o' "\${length}")"
      printf '%s' "\${label}"
    done
    printf '\\000\\000\\001\\000\\001'
  } > /tmp/fugue-nld-tcp-query
  length="\$(wc -c < /tmp/fugue-nld-tcp-query | tr -d '[:space:]')"
  high="\$((length / 256))"
  low="\$((length % 256))"
  {
    printf "\\\\\$(printf '%03o' "\${high}")"
    printf "\\\\\$(printf '%03o' "\${low}")"
    cat /tmp/fugue-nld-tcp-query
  } | nc -w 5 '${query_server}' 53 > /tmp/fugue-nld-tcp-response
  [ "\$(wc -c < /tmp/fugue-nld-tcp-response | tr -d '[:space:]')" -ge 14 ]
  set -- \$(od -An -t u1 -N 6 /tmp/fugue-nld-tcp-response)
  [ "\${3}" = '18' ] && [ "\${4}" = '52' ]
  [ "\$((\${5} & 128))" -ne 0 ]
  [ "\$((\${6} & 15))" -eq 0 ]
}
grep -Eq '^nameserver[[:space:]]+${service_ip}([[:space:]]|$)' /etc/resolv.conf
wget -T 5 -qO /tmp/fugue-nld-before.metrics http://${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}:9253/metrics
before_requests="\$(metric_total coredns_dns_requests_total /tmp/fugue-nld-before.metrics)"
before_responses="\$(metric_total coredns_dns_responses_total /tmp/fugue-nld-before.metrics)"
nslookup kubernetes.default.svc.${FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN} ${query_server}
dns_tcp_query kubernetes.default.svc.${FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN}
nslookup ${NODE_LOCAL_DNS_ACTIVE_SERVICE_NAME}.${FUGUE_NODE_LOCAL_DNS_NAMESPACE}.svc.${FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN} ${query_server}
nslookup -type=SRV _metrics._tcp.${NODE_LOCAL_DNS_ACTIVE_SERVICE_NAME}.${FUGUE_NODE_LOCAL_DNS_NAMESPACE}.svc.${FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN} ${query_server}
nslookup ${FUGUE_NODE_LOCAL_DNS_EXTERNAL_PROBE_NAME} ${query_server}
nslookup kubernetes.default.svc.${FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN}
nslookup ${FUGUE_NODE_LOCAL_DNS_EXTERNAL_PROBE_NAME}
if nslookup fugue-node-local-dns-\$\$.invalid ${query_server}; then
  echo 'reserved .invalid name unexpectedly resolved' >&2
  exit 1
fi
wget -T 5 -qO /tmp/fugue-nld-setup.metrics http://${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}:9353/metrics
wget -T 5 -qO /tmp/fugue-nld-after.metrics http://${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}:9253/metrics
after_requests="\$(metric_total coredns_dns_requests_total /tmp/fugue-nld-after.metrics)"
after_responses="\$(metric_total coredns_dns_responses_total /tmp/fugue-nld-after.metrics)"
[ "\${after_requests}" -gt "\${before_requests}" ]
[ "\${after_responses}" -gt "\${before_responses}" ]
grep -F 'coredns_nodecache_setup_errors_total' /tmp/fugue-nld-setup.metrics >/dev/null
grep -F 'coredns_cache_' /tmp/fugue-nld-after.metrics >/dev/null
grep -F 'coredns_panics_total' /tmp/fugue-nld-after.metrics >/dev/null
grep -F 'coredns_reload_failed_total' /tmp/fugue-nld-after.metrics >/dev/null
errors="\$(awk '/^coredns_nodecache_setup_errors_total/ {sum += \$NF} END {print sum + 0}' /tmp/fugue-nld-setup.metrics)"
[ "\${errors}" = '0' ]
panics="\$(awk '/^coredns_panics_total/ {sum += \$NF} END {print sum + 0}' /tmp/fugue-nld-after.metrics)"
reload_failures="\$(awk '/^coredns_reload_failed_total/ {sum += \$NF} END {print sum + 0}' /tmp/fugue-nld-after.metrics)"
[ "\${panics}" = '0' ]
[ "\${reload_failures}" = '0' ]
EOF
)"

  ${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" exec "${pod_name}" -- /bin/sh -ec "${exec_script}" || return 1
  node_local_dns_run_probe_pod "${node_name}" dns "${FUGUE_NODE_LOCAL_DNS_PROBE_IMAGE}" false false "${probe_script}" || return 1
}

node_local_dns_verify_running() {
  local mode="$1"
  local service_ip="$2"
  local query_server="${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}"
  local upstream_service="${NODE_LOCAL_DNS_UPSTREAM_SERVICE_NAME}"
  local endpoint_count="0"
  local pod_rows=""
  local pods_json=""
  local pod_name=""
  local node_name=""
  local actual_nodes=""
  local expected_nodes=""
  local exec_script=""
  local probe_script=""

  [[ "${mode}" == "shadow" || "${mode}" == "iptables" ]] || return 1
  [[ "${mode}" == "shadow" ]] || query_server="${service_ip}"
  if ! rollout_daemonset_status "${NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME}" "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}"; then
    return 1
  fi
  endpoint_count="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get endpoints "${upstream_service}" -o json 2>/dev/null |
    python3 -c 'import json,sys; payload=json.load(sys.stdin); print(sum(len(s.get("addresses") or []) for s in payload.get("subsets") or []))' 2>/dev/null || printf '0')"
  [[ "${endpoint_count}" =~ ^[0-9]+$ ]] && (( endpoint_count > 0 )) || return 1
  pod_rows="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get pods -l "$(node_local_dns_active_pod_selector)" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.nodeName}{"\n"}{end}' 2>/dev/null || true)"
  [[ -n "$(trim_field "${pod_rows}")" ]] || return 1
  if ! pods_json="$(node_local_dns_capture_pods_json)" || ! node_local_dns_validate_pure_pod_snapshot "${pods_json}" "${NODE_LOCAL_DNS_TARGET_NODES}" "${mode}"; then
    log "NodeLocal DNSCache Pods are not a pure, Ready ${mode} cohort"
    return 1
  fi
  if ! node_local_dns_validate_active_pod_runtime "${pods_json}" "${NODE_LOCAL_DNS_TARGET_NODES}" "${mode}" "${service_ip}" true; then
    log "NodeLocal DNSCache Pods do not match the exact active runtime template"
    return 1
  fi
  actual_nodes="$(cut -f2 <<<"${pod_rows}" | sed '/^[[:space:]]*$/d' | sort -u)"
  expected_nodes="$(printf '%s\n' "${NODE_LOCAL_DNS_TARGET_NODES}" | sed '/^[[:space:]]*$/d' | sort -u)"
  [[ "${actual_nodes}" == "${expected_nodes}" ]] || {
    log "NodeLocal DNSCache actual pod nodes differ from preflight targets: expected=$(paste -sd, <<<"${expected_nodes}") actual=$(paste -sd, <<<"${actual_nodes}")"
    return 1
  }
  exec_script="$(cat <<EOF
set -eu
command -v iptables-save >/dev/null 2>&1
test -e /sys/class/net/nodelocaldns
iptables-save -t nat > /tmp/fugue-nld-kube-proxy.rules 2>/dev/null || exit 1
for protocol in udp tcp; do
  if ! grep -F -- '-A KUBE-SERVICES ' /tmp/fugue-nld-kube-proxy.rules | grep -F -- '-d ${service_ip}/32' | grep -F -- "-p \${protocol}" | grep -F -- '--dport 53' | grep -E -- '-j KUBE-SVC-[A-Z0-9]+' >/dev/null; then
    echo "live kube-dns \${protocol}/53 iptables service rule disappeared" >&2
    exit 1
  fi
done
for attempt in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50; do
  iptables-save > /tmp/fugue-nld-running.rules 2>/dev/null || exit 1
  if grep -F 'NodeLocal DNS Cache:' /tmp/fugue-nld-running.rules >/dev/null; then
    break
  fi
  sleep 2
done
grep -F 'NodeLocal DNS Cache:' /tmp/fugue-nld-running.rules >/dev/null
if [ '${mode}' = 'shadow' ]; then
  if grep -F 'NodeLocal DNS Cache:' /tmp/fugue-nld-running.rules | grep -F '${service_ip}' >/dev/null; then
    echo 'shadow mode retained NodeLocal rules for the kube-dns ServiceIP' >&2
    exit 1
  fi
  if grep -Fq '${service_ip}' /proc/net/fib_trie 2>/dev/null; then
    echo 'shadow mode retained the kube-dns ServiceIP as a local address' >&2
    exit 1
  fi
else
  grep -F 'NodeLocal DNS Cache:' /tmp/fugue-nld-running.rules | grep -F '${service_ip}' >/dev/null
fi
EOF
)"
  probe_script="$(cat <<EOF
set -eu
grep -Eq '^nameserver[[:space:]]+${service_ip}([[:space:]]|$)' /etc/resolv.conf
nslookup kubernetes.default.svc.${FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN} ${query_server}
nslookup ${NODE_LOCAL_DNS_ACTIVE_SERVICE_NAME}.${FUGUE_NODE_LOCAL_DNS_NAMESPACE}.svc.${FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN} ${query_server}
nslookup -type=SRV _metrics._tcp.${NODE_LOCAL_DNS_ACTIVE_SERVICE_NAME}.${FUGUE_NODE_LOCAL_DNS_NAMESPACE}.svc.${FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN} ${query_server}
nslookup ${FUGUE_NODE_LOCAL_DNS_EXTERNAL_PROBE_NAME} ${query_server}
nslookup kubernetes.default.svc.${FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN}
nslookup ${FUGUE_NODE_LOCAL_DNS_EXTERNAL_PROBE_NAME}
if nslookup fugue-node-local-dns-$$.invalid ${query_server}; then
  echo 'reserved .invalid name unexpectedly resolved' >&2
  exit 1
fi
wget -T 5 -qO /tmp/fugue-nld-setup.metrics http://${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}:9353/metrics
wget -T 5 -qO /tmp/fugue-nld-core.metrics http://${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}:9253/metrics
grep -F 'coredns_nodecache_setup_errors_total' /tmp/fugue-nld-setup.metrics >/dev/null
grep -F 'coredns_cache_' /tmp/fugue-nld-core.metrics >/dev/null
awk '/^coredns_nodecache_setup_errors_total/ {sum += \$NF} END {print sum + 0}' /tmp/fugue-nld-setup.metrics > /tmp/fugue-nld-errors
read -r errors < /tmp/fugue-nld-errors
[ "\${errors}" = '0' ]
EOF
)"
  while IFS=$'\t' read -r pod_name node_name; do
    [[ -n "${pod_name}" && -n "${node_name}" ]] || continue
    if ! node_local_dns_verify_one_node "${pod_name}" "${node_name}" "${mode}" "${service_ip}"; then
      NODE_LOCAL_DNS_FAILED_NODE="${node_name}"
      return 1
    fi
    if ! node_local_dns_verify_authoritative_coexistence "${node_name}"; then
      NODE_LOCAL_DNS_FAILED_NODE="${node_name}"
      return 1
    fi
  done <<<"${pod_rows}"
  log "NodeLocal DNSCache ${mode} verification passed on $(wc -l <<<"${pod_rows}" | awk '{print $1}') node(s)"
}

node_local_dns_replacement_order() {
  local target_nodes="$1"
  local nodes_json=""

  nodes_json="$(${KUBECTL} get nodes -o json)" || return 1
  printf '%s' "${nodes_json}" | TARGET_NODES="${target_nodes}" python3 -c '
import json
import os
import sys

payload = json.load(sys.stdin)
targets = [item.strip() for item in os.environ["TARGET_NODES"].splitlines() if item.strip()]
nodes = {str((item.get("metadata") or {}).get("name") or ""): item for item in payload.get("items") or []}
if not targets or any(name not in nodes for name in targets):
    raise SystemExit(1)

def priority(name):
    labels = (nodes[name].get("metadata") or {}).get("labels") or {}
    if "node-role.kubernetes.io/control-plane" in labels or "node-role.kubernetes.io/master" in labels:
        return 2
    if str(labels.get("fugue.io/role.edge") or "").lower() == "true" or str(labels.get("fugue.io/role.dns") or "").lower() == "true":
        return 1
    return 0

for _, _, name in sorted((priority(name), index, name) for index, name in enumerate(targets)):
    print(name)
'
}

node_local_dns_wait_for_pod_on_node() {
  local node_name="$1"
  local old_uid="$2"
  local expected_mode="$3"
  local expected_revision="${4:-}"
  local wait_seconds="${5:-${FUGUE_NODE_LOCAL_DNS_PROBE_TIMEOUT_SECONDS}}"
  local deadline=$((SECONDS + wait_seconds))
  local pods_json=""
  local row=""

  while (( SECONDS < deadline )); do
    pods_json="$(node_local_dns_capture_pods_json 2>/dev/null || true)"
    if [[ -n "${pods_json}" ]]; then
      row="$(PODS_JSON="${pods_json}" NODE_NAME="${node_name}" OLD_UID="${old_uid}" EXPECTED_MODE="${expected_mode}" EXPECTED_REVISION="${expected_revision}" python3 -c '
import json
import os

matches = [pod for pod in json.loads(os.environ["PODS_JSON"]) if pod.get("node") == os.environ["NODE_NAME"]]
if len(matches) != 1:
    raise SystemExit(1)
pod = matches[0]
fatal_reasons = {
    "CrashLoopBackOff",
    "CreateContainerConfigError",
    "CreateContainerError",
    "ErrImagePull",
    "ImagePullBackOff",
    "InvalidImageName",
    "RunContainerError",
}
observed_fatal = sorted(fatal_reasons.intersection(set(pod.get("waiting_reasons") or [])))
fatal_terminated = sorted({"ContainerCannotRun", "Error", "OOMKilled"}.intersection(set((pod.get("terminated_reasons") or []) + (pod.get("last_terminated_reasons") or []))))
if pod.get("phase") == "Failed" or observed_fatal or (fatal_terminated and int(pod.get("restart_count") or 0) > 0):
    reason = observed_fatal or fatal_terminated or [str(pod.get("phase") or "Failed")]
    print("__fatal__\t" + ",".join(reason))
    raise SystemExit(0)
if not pod.get("ready") or pod.get("mode") != os.environ["EXPECTED_MODE"] or not pod.get("name") or not pod.get("uid"):
    raise SystemExit(1)
if os.environ["EXPECTED_REVISION"] and pod.get("revision") != os.environ["EXPECTED_REVISION"]:
    raise SystemExit(1)
if os.environ["OLD_UID"] and pod.get("uid") == os.environ["OLD_UID"]:
    raise SystemExit(1)
print(str(pod["name"]) + "\t" + str(pod["uid"]))
' 2>/dev/null || true)"
      if [[ -n "${row}" ]]; then
        if [[ "${row}" == __fatal__$'\t'* ]]; then
          log "NodeLocal DNSCache Pod on ${node_name} entered a terminal startup state: ${row#*$'\t'}"
          return 1
        fi
        printf '%s\n' "${row}"
        return 0
      fi
    fi
    sleep 2
  done
  return 1
}

node_local_dns_current_controller_revision() {
  local expected_mode="$1"
  local expected_targets="${2:-${NODE_LOCAL_DNS_TARGET_NODES}}"
  local daemonset_json=""
  local controller_revisions_json=""
  local live_targets=""
  local state=""
  local mode=""
  local generation=""
  local observed_generation=""
  local controller_revision=""

  daemonset_json="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get daemonset "${NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME}" -o json)" || return 1
  live_targets="$(node_local_dns_daemonset_target_nodes "${daemonset_json}")" || return 1
  [[ "${live_targets}" == "${expected_targets}" ]] || return 1
  controller_revisions_json="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get controllerrevisions.apps \
    -l "$(node_local_dns_active_pod_selector)" -o json)" || return 1
  state="$({
    printf '%s\n' "${daemonset_json}"
    printf '%s\n' "${controller_revisions_json}"
  } | python3 -c '
import copy
import json
import sys

decoder = json.JSONDecoder()
raw = sys.stdin.read()
documents = []
offset = 0
while offset < len(raw):
    while offset < len(raw) and raw[offset].isspace():
        offset += 1
    if offset >= len(raw):
        break
    document, offset = decoder.raw_decode(raw, offset)
    documents.append(document)
if len(documents) != 2:
    raise SystemExit(1)

daemonset, revision_list = documents
metadata = daemonset.get("metadata") or {}
status = daemonset.get("status") or {}
template = ((daemonset.get("spec") or {}).get("template") or {})
labels = (template.get("metadata") or {}).get("labels") or {}
daemonset_name = str(metadata.get("name") or "")
daemonset_uid = str(metadata.get("uid") or "")
daemonset_namespace = str(metadata.get("namespace") or "")
if not daemonset_name or not daemonset_uid or not daemonset_namespace or not isinstance(template, dict) or not template:
    raise SystemExit(1)
if not isinstance(revision_list, dict) or not isinstance(revision_list.get("items"), list):
    raise SystemExit(1)

matches = []
for revision in revision_list["items"]:
    if not isinstance(revision, dict):
        raise SystemExit(1)
    revision_metadata = revision.get("metadata") or {}
    if (
        str(revision.get("apiVersion") or "") != "apps/v1"
        or str(revision.get("kind") or "") != "ControllerRevision"
        or str(revision_metadata.get("namespace") or "") != daemonset_namespace
    ):
        continue
    controllers = [
        owner
        for owner in revision_metadata.get("ownerReferences") or []
        if isinstance(owner, dict) and owner.get("controller") is True
    ]
    if len(controllers) != 1:
        continue
    controller = controllers[0]
    if (
        str(controller.get("apiVersion") or "") != "apps/v1"
        or str(controller.get("kind") or "") != "DaemonSet"
        or str(controller.get("name") or "") != daemonset_name
        or str(controller.get("uid") or "") != daemonset_uid
    ):
        continue
    revision_template = copy.deepcopy((((revision.get("data") or {}).get("spec") or {}).get("template")))
    if not isinstance(revision_template, dict) or revision_template.pop("$patch", None) != "replace":
        continue
    if revision_template == template:
        matches.append(revision)

if len(matches) != 1:
    raise SystemExit(1)
revision_metadata = matches[0].get("metadata") or {}
revision_name = str(revision_metadata.get("name") or "")
revision_hash = str((revision_metadata.get("labels") or {}).get("controller-revision-hash") or "")
if not revision_hash or revision_name != daemonset_name + "-" + revision_hash:
    raise SystemExit(1)
print("\t".join([
    str(labels.get("fugue.io/node-local-dns-mode") or ""),
    str(metadata.get("generation") or ""),
    str(status.get("observedGeneration") or ""),
    revision_hash,
]))
')" || return 1
  IFS=$'\t' read -r mode generation observed_generation controller_revision <<<"${state}"
  [[ "${mode}" == "${expected_mode}" && -n "${generation}" && "${generation}" == "${observed_generation}" && -n "${controller_revision}" ]] || return 1
  printf '%s\n' "${controller_revision}"
}

node_local_dns_verify_authoritative_coexistence() {
  local node_name="$1"
  local current_rows=""
  local expected_rows=""
  local host_ip=""
  local protocol=""
  local owner=""
  local uid=""
  local restarts=""
  local ready=""
  local transport=""
  local attempt=0
  local transport_args=()
  local output=""

  current_rows="$(node_local_dns_pod_dns_host_port_inventory "${node_name}" scoped)" || return 1
  expected_rows="$(awk -F $'\t' -v node="${node_name}" '$1 == node {sub(/^[^\t]*\t/, "", $0); print $0}' <<<"${NODE_LOCAL_DNS_HOSTPORT_POD_SNAPSHOT}" | sort)"
  current_rows="$(printf '%s\n' "${current_rows}" | sed '/^[[:space:]]*$/d' | sort)"
  [[ "${current_rows}" == "${expected_rows}" ]] || {
    log "authoritative DNS Pod UID, readiness, restart count, or scoped hostPort inventory changed on ${node_name}"
    return 1
  }
  [[ -n "${current_rows}" ]] || return 0
  command_exists host || {
    log "host is required to verify authoritative DNS coexistence on ${node_name}"
    return 1
  }
  while IFS=$'\t' read -r host_ip protocol owner uid restarts ready; do
    [[ -n "${host_ip}" ]] || continue
    node_local_dns_verify_scoped_hostport_rules "${node_name}" "${host_ip}" || return 1
    for transport in udp tcp; do
      transport_args=(-W 3)
      [[ "${transport}" != "tcp" ]] || transport_args+=(-T)
      attempt=1
      while (( attempt <= 3 )); do
        if output="$(host "${transport_args[@]}" -t SOA "${FUGUE_DNS_ZONE}" "${host_ip}" 2>&1)" && grep -Fq ' has SOA record ' <<<"${output}"; then
          break
        fi
        if (( attempt == 3 )); then
          log "authoritative DNS ${transport} SOA probe failed on ${node_name} hostIP=${host_ip}: ${output}"
          return 1
        fi
        sleep 2
        attempt=$((attempt + 1))
      done
    done
  done < <(awk -F $'\t' '!seen[$1]++' <<<"${current_rows}")
}

node_local_dns_verify_authoritative_cohort() {
  local target_nodes="$1"
  local node_name=""

  while IFS= read -r node_name; do
    [[ -n "${node_name}" ]] || continue
    node_local_dns_verify_authoritative_coexistence "${node_name}" || return 1
  done <<<"${target_nodes}"
}

node_local_dns_observe_one_node() {
  local pod_name="$1"
  local node_name="$2"
  local mode="$3"
  local service_ip="$4"
  local watch_seconds="${FUGUE_NODE_LOCAL_DNS_NODE_WATCH_SECONDS}"
  local deadline=$((SECONDS + watch_seconds))

  while true; do
    node_local_dns_verify_one_node "${pod_name}" "${node_name}" "${mode}" "${service_ip}" || return 1
    node_local_dns_verify_authoritative_coexistence "${node_name}" || return 1
    retry "${FUGUE_SMOKE_RETRIES}" "${FUGUE_SMOKE_DELAY_SECONDS}" smoke_test || return 1
    if (( SECONDS >= deadline )); then
      return 0
    fi
    sleep 10
  done
}

node_local_dns_node_safe_for_replacement() {
  local node_name="$1"
  local node_json=""

  node_json="$(${KUBECTL} get node "${node_name}" -o json)" || return 1
  printf '%s' "${node_json}" | python3 -c '
import json
import sys

node = json.load(sys.stdin)
metadata = node.get("metadata") or {}
spec = node.get("spec") or {}
status = node.get("status") or {}
labels = metadata.get("labels") or {}
conditions = {str(item.get("type") or ""): str(item.get("status") or "") for item in status.get("conditions") or []}
if labels.get("kubernetes.io/os") != "linux" or labels.get("kubernetes.io/arch") != "amd64":
    raise SystemExit(1)
if spec.get("unschedulable") is True or conditions.get("Ready") != "True":
    raise SystemExit(1)
for condition in ("MemoryPressure", "DiskPressure", "PIDPressure"):
    if conditions.get(condition) != "False":
        raise SystemExit(1)
'
}

node_local_dns_replace_one_node() {
  local node_name="$1"
  local expected_mode="$2"
  local service_ip="$3"
  local expected_revision="$4"
  local pods_json=""
  local old_row=""
  local old_name=""
  local old_uid=""
  local new_row=""
  local new_name=""
  local new_uid=""
  local live_revision=""

  node_local_dns_node_safe_for_replacement "${node_name}" || return 1
  node_local_dns_verify_artifact "${expected_mode}" "${service_ip}" true || return 1
  live_revision="$(node_local_dns_current_controller_revision "${expected_mode}")" || return 1
  [[ "${live_revision}" == "${expected_revision}" ]] || return 1
  node_local_dns_verify_authoritative_coexistence "${node_name}" || return 1
  pods_json="$(node_local_dns_capture_pods_json)" || return 1
  old_row="$(PODS_JSON="${pods_json}" SNAPSHOT_PODS="${NODE_LOCAL_DNS_PREVIOUS_PODS_JSON}" NODE_NAME="${node_name}" python3 -c '
import json
import os

matches = [pod for pod in json.loads(os.environ["PODS_JSON"]) if pod.get("node") == os.environ["NODE_NAME"]]
snapshot = [pod for pod in json.loads(os.environ["SNAPSHOT_PODS"]) if pod.get("node") == os.environ["NODE_NAME"]]
if len(matches) != 1 or len(snapshot) != 1:
    raise SystemExit(1)
live = matches[0]
old = snapshot[0]
for field in ("name", "uid", "mode", "revision", "config_items"):
    if live.get(field) != old.get(field):
        raise SystemExit(1)
if not live.get("name") or not live.get("uid") or not live.get("ready") or not old.get("ready"):
    raise SystemExit(1)
print(str(matches[0]["name"]) + "\t" + str(matches[0]["uid"]))
')" || return 1
  IFS=$'\t' read -r old_name old_uid <<<"${old_row}"
  [[ -n "${old_name}" && -n "${old_uid}" ]] || return 1
  node_local_dns_verify_one_node "${old_name}" "${node_name}" "${NODE_LOCAL_DNS_PREVIOUS_MODE}" "${service_ip}" || return 1
  node_local_dns_node_safe_for_replacement "${node_name}" || return 1
  live_revision="$(node_local_dns_current_controller_revision "${expected_mode}")" || return 1
  [[ "${live_revision}" == "${expected_revision}" ]] || return 1
  node_local_dns_verify_preserved_state_unchanged || return 1

  log "replacing NodeLocal DNSCache Pod ${old_name} on ${node_name} with verified ${expected_mode} template"
  NODE_LOCAL_DNS_FAILED_NODE="${node_name}"
  ${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" delete pod "${old_name}" --wait=false >/dev/null || return 1
  new_row="$(node_local_dns_wait_for_pod_on_node "${node_name}" "${old_uid}" "${expected_mode}" "${expected_revision}" "${FUGUE_NODE_LOCAL_DNS_CRITICAL_READY_TIMEOUT_SECONDS}")" || return 1
  IFS=$'\t' read -r new_name new_uid <<<"${new_row}"
  [[ -n "${new_name}" && -n "${new_uid}" && "${new_uid}" != "${old_uid}" ]] || return 1
  NODE_LOCAL_DNS_REPLACED_NODES+="${NODE_LOCAL_DNS_REPLACED_NODES:+$'\n'}${node_name}"
  node_local_dns_observe_one_node "${new_name}" "${node_name}" "${expected_mode}" "${service_ip}" || return 1
  node_local_dns_verify_preserved_state_unchanged || return 1
  NODE_LOCAL_DNS_FAILED_NODE=""
}

node_local_dns_reconcile_after_helm() {
  local desired_mode="${FUGUE_NODE_LOCAL_DNS_MODE}"
  local service_ip="${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}"
  local live_targets=""
  local replacement_nodes=""
  local node_name=""
  local added_node=""
  local pod_row=""
  local pod_name=""
  local pod_uid=""
  local needs_replacement="false"
  local expected_revision=""
  local deadline=$((SECONDS + FUGUE_NODE_LOCAL_DNS_PROBE_TIMEOUT_SECONDS))

  [[ "${FUGUE_NODE_LOCAL_DNS_ENABLED}" == "true" ]] || return 0
  NODE_LOCAL_DNS_REPLACED_NODES=""
  NODE_LOCAL_DNS_FAILED_NODE=""
  if ! node_local_dns_verify_artifact "${desired_mode}" "${service_ip}" true; then
    log "NodeLocal DNSCache Helm artifact does not match the exact current ${desired_mode} layout"
    return 1
  fi
  if ! node_local_dns_verify_preserved_state_unchanged; then
    log "preserved offline NodeLocal DNSCache DaemonSet or Pod changed during the Helm transaction"
    return 1
  fi
  live_targets="$(node_local_dns_daemonset_target_nodes)" || return 1
  [[ "${live_targets}" == "${NODE_LOCAL_DNS_TARGET_NODES}" ]] || {
    log "NodeLocal DNSCache Helm target cohort differs from the preflight cohort"
    return 1
  }
  while (( SECONDS < deadline )); do
    expected_revision="$(node_local_dns_current_controller_revision "${desired_mode}" 2>/dev/null || true)"
    [[ -n "${expected_revision}" ]] && break
    sleep 2
  done
  [[ -n "${expected_revision}" ]] || {
    log "NodeLocal DNSCache DaemonSet did not expose one exact current ControllerRevision for the verified ${desired_mode} template"
    return 1
  }
  node_local_dns_verify_central_coredns_ready || return 1

  if [[ -n "$(trim_field "${NODE_LOCAL_DNS_ADDED_NODES}")" ]]; then
    added_node="$(trim_field "${NODE_LOCAL_DNS_ADDED_NODES}")"
    pod_row="$(node_local_dns_wait_for_pod_on_node "${added_node}" "" shadow "${expected_revision}")" || return 1
    IFS=$'\t' read -r pod_name pod_uid <<<"${pod_row}"
    [[ -n "${pod_name}" && -n "${pod_uid}" ]] || return 1
    node_local_dns_observe_one_node "${pod_name}" "${added_node}" shadow "${service_ip}" || return 1
  else
    if [[ "${NODE_LOCAL_DNS_PREVIOUS_ENABLED}" != "true" || "${NODE_LOCAL_DNS_PREVIOUS_MODE}" != "${desired_mode}" ]]; then
      needs_replacement="true"
    elif ! node_local_dns_snapshot_uses_current_layout "${NODE_LOCAL_DNS_PREVIOUS_PODS_JSON}" "${desired_mode}"; then
      needs_replacement="true"
    elif ! node_local_dns_validate_active_pod_runtime "${NODE_LOCAL_DNS_PREVIOUS_PODS_JSON}" "${NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES}" "${desired_mode}" "${service_ip}" true; then
      needs_replacement="true"
    fi
    if [[ "${needs_replacement}" == "true" ]]; then
      replacement_nodes="$(node_local_dns_replacement_order "${NODE_LOCAL_DNS_TARGET_NODES}")" || return 1
      while IFS= read -r node_name; do
        [[ -n "${node_name}" ]] || continue
        node_local_dns_replace_one_node "${node_name}" "${desired_mode}" "${service_ip}" "${expected_revision}" || return 1
      done <<<"${replacement_nodes}"
    fi
  fi

  node_local_dns_verify_running "${desired_mode}" "${service_ip}" || return 1
  node_local_dns_verify_preserved_state_unchanged
}

node_local_dns_verify_teardown() {
  local service_ip="$1"
  local node_name=""
  local script=""
  script="$(cat <<EOF
set -eu
command -v iptables-save >/dev/null 2>&1
test ! -e /sys/class/net/nodelocaldns
iptables-save > /tmp/fugue-nld-teardown.rules 2>/dev/null || exit 1
if grep -F 'NodeLocal DNS Cache:' /tmp/fugue-nld-teardown.rules; then
  echo 'NodeLocal DNSCache iptables rules remain after teardown' >&2
  exit 1
fi
EOF
)"
  while IFS= read -r node_name; do
    [[ -n "${node_name}" ]] || continue
    node_local_dns_run_probe_pod "${node_name}" teardown "${FUGUE_NODE_LOCAL_DNS_IMAGE_REPOSITORY}@${FUGUE_NODE_LOCAL_DNS_IMAGE_DIGEST}" true true "${script}" || return 1
    node_local_dns_run_probe_pod "${node_name}" fallback "${FUGUE_NODE_LOCAL_DNS_PROBE_IMAGE}" false false \
      "set -eu; grep -Eq '^nameserver[[:space:]]+${service_ip}([[:space:]]|$)' /etc/resolv.conf; nslookup kubernetes.default.svc.${FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN} ${service_ip}; nslookup ${FUGUE_NODE_LOCAL_DNS_EXTERNAL_PROBE_NAME} ${service_ip}" || return 1
  done <<<"${NODE_LOCAL_DNS_TARGET_NODES}"
}

node_local_dns_verify_central_coredns_ready() {
  local endpoint_ips=""
  local node_name=""
  local probe_script=""

  if ! endpoint_ips="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get endpoints "${FUGUE_NODE_LOCAL_DNS_KUBE_DNS_SERVICE_NAME}" -o json |
    python3 -c 'import json,sys; payload=json.load(sys.stdin); print(" ".join(address["ip"] for subset in payload.get("subsets") or [] for address in subset.get("addresses") or [] if address.get("ip")))')"; then
    log "cannot verify central CoreDNS because its Ready endpoint inventory is unavailable"
    return 1
  fi
  [[ -n "$(trim_field "${endpoint_ips}")" ]] || {
    log "cannot remove NodeLocal DNSCache because central CoreDNS has no Ready endpoints"
    return 1
  }
  probe_script="$(cat <<EOF
set -eu
for endpoint in ${endpoint_ips}; do
  nc -z -w 5 "\${endpoint}" 53
  nslookup kubernetes.default.svc.${FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN} "\${endpoint}"
  nslookup ${FUGUE_NODE_LOCAL_DNS_EXTERNAL_PROBE_NAME} "\${endpoint}"
done
EOF
)"
  while IFS= read -r node_name; do
    [[ -n "${node_name}" ]] || continue
    node_local_dns_run_probe_pod "${node_name}" central-coredns "${FUGUE_NODE_LOCAL_DNS_PROBE_IMAGE}" false false "${probe_script}" || return 1
  done <<<"${NODE_LOCAL_DNS_TARGET_NODES}"
  log "central CoreDNS direct UDP/TCP and internal/external resolution checks passed before NodeLocal DNSCache removal"
}

node_local_dns_restore_daemonset_snapshot() {
  local daemonset_snapshot="$1"
  local previous_mode="$2"
  local service_ip="$3"

  [[ -n "$(trim_field "${daemonset_snapshot}")" ]] || return 1
  log "restoring the exact pre-removal NodeLocal DNSCache DaemonSet snapshot"
  printf '%s' "${daemonset_snapshot}" | ${KUBECTL} apply -f - >/dev/null || return 1
  node_local_dns_verify_running "${previous_mode}" "${service_ip}"
}

node_local_dns_recover_exact_residue() {
  local service_ip="$1"
  local local_hex=""
  local service_hex=""
  local node_name=""
  local rules_script=""
  local interface_script=""
  local remaining_pods=""

  if ! remaining_pods="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get pods \
    -l "$(node_local_dns_active_pod_selector)" --no-headers)"; then
    log "refusing NodeLocal DNSCache residue cleanup because the Pod inventory is unavailable"
    return 1
  fi
  [[ -z "$(trim_field "${remaining_pods}")" ]] || {
    log "refusing NodeLocal DNSCache residue cleanup while node-cache pods still exist"
    return 1
  }
  local_hex="$(node_local_dns_proc_ipv4_hex "${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}")"
  service_hex="$(node_local_dns_proc_ipv4_hex "${service_ip}")"
  read -r -d '' rules_script <<'EOF' || true
set -eu
command -v iptables >/dev/null 2>&1
command -v iptables-save >/dev/null 2>&1
iptables_version=unavailable
iptables_save_version=unavailable
iptables --version > /tmp/fugue-nld-iptables.version 2>&1 || true
iptables-save --version > /tmp/fugue-nld-iptables-save.version 2>&1 || true
IFS= read -r iptables_version < /tmp/fugue-nld-iptables.version || iptables_version=unavailable
IFS= read -r iptables_save_version < /tmp/fugue-nld-iptables-save.version || iptables_save_version=unavailable
iptables_backend=unknown
iptables_save_backend=unknown
if printf '%s\n' "${iptables_version}" | grep -Fq nf_tables; then
  iptables_backend=nf_tables
elif printf '%s\n' "${iptables_version}" | grep -Fq legacy; then
  iptables_backend=legacy
fi
if printf '%s\n' "${iptables_save_version}" | grep -Fq nf_tables; then
  iptables_save_backend=nf_tables
elif printf '%s\n' "${iptables_save_version}" | grep -Fq legacy; then
  iptables_save_backend=legacy
fi
printf 'fugue-nld-residue tool=iptables version=%s backend=%s\n' "${iptables_version:-unavailable}" "${iptables_backend}"
printf 'fugue-nld-residue tool=iptables-save version=%s backend=%s\n' "${iptables_save_version:-unavailable}" "${iptables_save_backend}"

node_cache_count=0
node_cache_shown=0
node_cache_limit=32
for comm_file in /proc/[0-9]*/comm; do
  [ -r "${comm_file}" ] || continue
  comm=''
  IFS= read -r comm < "${comm_file}" || true
  [ "${comm}" = 'node-cache' ] || continue
  node_cache_count=$((node_cache_count + 1))
  if [ "${node_cache_shown}" -lt "${node_cache_limit}" ]; then
    pid="${comm_file#/proc/}"
    pid="${pid%/comm}"
    printf 'fugue-nld-residue node-cache-comm pid=%s comm=node-cache\n' "${pid}"
    node_cache_shown=$((node_cache_shown + 1))
  fi
done
node_cache_truncated=false
if [ "${node_cache_count}" -gt "${node_cache_shown}" ]; then
  node_cache_truncated=true
fi
printf 'fugue-nld-residue node-cache-comm-count total=%s shown=%s limit=%s truncated=%s\n' \
  "${node_cache_count}" "${node_cache_shown}" "${node_cache_limit}" "${node_cache_truncated}"

for table_file in /proc/net/tcp /proc/net/udp /proc/net/tcp6 /proc/net/udp6; do
  [ -r "${table_file}" ] || continue
  while read -r slot endpoint remainder; do
    [ "${slot}" = 'sl' ] && continue
    address="${endpoint%:*}"
    port="${endpoint##*:}"
    if [ "${port}" = '0035' ]; then
      case "${address}" in
        00000000|00000000000000000000000000000000|__FUGUE_LOCAL_HEX__|__FUGUE_SERVICE_HEX__)
          echo "refusing residue cleanup while DNS listener ${endpoint} remains" >&2
          exit 1
          ;;
      esac
    fi
  done <"${table_file}"
done
rule_diagnostic_limit=64
rule_diagnostic_shown=0
rule_match_total=0
for table in raw filter; do
  iptables-save -t "${table}" > "/tmp/fugue-nld-${table}.rules" 2>/dev/null || exit 1
  while IFS= read -r line; do
    case "${line}" in
      -A\ *"NodeLocal DNS Cache:"*)
        rule_match_total=$((rule_match_total + 1))
        diagnose_rule=false
        if [ "${rule_diagnostic_shown}" -lt "${rule_diagnostic_limit}" ]; then
          diagnose_rule=true
          rule_diagnostic_shown=$((rule_diagnostic_shown + 1))
          before_count=unavailable
          if iptables-save -t "${table}" > /tmp/fugue-nld-rule-before.rules 2>/dev/null; then
            grep -Fxc -- "${line}" /tmp/fugue-nld-rule-before.rules > /tmp/fugue-nld-rule-before.count 2>/dev/null || true
            IFS= read -r before_count < /tmp/fugue-nld-rule-before.count || before_count=unavailable
          fi
          printf 'fugue-nld-residue rule-before table=%s index=%s exact-count=%s rule=%s\n' \
            "${table}" "${rule_match_total}" "${before_count:-unavailable}" "${line}"
        fi
        delete_args="${line#-A }"
        set +e
        eval "iptables -t ${table} -D ${delete_args}"
        delete_rc=$?
        set -e
        if [ "${diagnose_rule}" = true ]; then
          printf 'fugue-nld-residue rule-delete table=%s index=%s rc=%s\n' \
            "${table}" "${rule_match_total}" "${delete_rc}"
          after_count=unavailable
          if iptables-save -t "${table}" > /tmp/fugue-nld-rule-after.rules 2>/dev/null; then
            grep -Fxc -- "${line}" /tmp/fugue-nld-rule-after.rules > /tmp/fugue-nld-rule-after.count 2>/dev/null || true
            IFS= read -r after_count < /tmp/fugue-nld-rule-after.count || after_count=unavailable
          fi
          printf 'fugue-nld-residue rule-after table=%s index=%s exact-count=%s rule=%s\n' \
            "${table}" "${rule_match_total}" "${after_count:-unavailable}" "${line}"
        fi
        [ "${delete_rc}" -eq 0 ] || exit "${delete_rc}"
        ;;
    esac
  done < "/tmp/fugue-nld-${table}.rules"
done
rule_diagnostics_truncated=false
if [ "${rule_match_total}" -gt "${rule_diagnostic_shown}" ]; then
  rule_diagnostics_truncated=true
fi
printf 'fugue-nld-residue rule-diagnostics matches=%s shown=%s limit=%s truncated=%s\n' \
  "${rule_match_total}" "${rule_diagnostic_shown}" "${rule_diagnostic_limit}" "${rule_diagnostics_truncated}"
iptables-save > /tmp/fugue-nld-remaining.rules 2>/dev/null || exit 1
if grep -Fq 'NodeLocal DNS Cache:' /tmp/fugue-nld-remaining.rules; then
  remaining_count=unavailable
  grep -Fc 'NodeLocal DNS Cache:' /tmp/fugue-nld-remaining.rules > /tmp/fugue-nld-remaining.count 2>/dev/null || true
  IFS= read -r remaining_count < /tmp/fugue-nld-remaining.count || remaining_count=unavailable
  printf 'fugue-nld-residue rule-observation sample=0 exact-comment-count=%s\n' "${remaining_count:-unavailable}"
  observation_sample=1
  observation_limit=4
  while [ "${observation_sample}" -lt "${observation_limit}" ]; do
    sleep 1
    if iptables-save > /tmp/fugue-nld-observation.rules 2>/dev/null; then
      observed_count=unavailable
      grep -Fc 'NodeLocal DNS Cache:' /tmp/fugue-nld-observation.rules > /tmp/fugue-nld-observation.count 2>/dev/null || true
      IFS= read -r observed_count < /tmp/fugue-nld-observation.count || observed_count=unavailable
    else
      observed_count=unavailable
    fi
    printf 'fugue-nld-residue rule-observation sample=%s exact-comment-count=%s\n' \
      "${observation_sample}" "${observed_count:-unavailable}"
    observation_sample=$((observation_sample + 1))
  done
  echo 'exact NodeLocal DNSCache rules remain after controlled cleanup' >&2
  exit 1
fi
EOF
  rules_script="${rules_script//__FUGUE_LOCAL_HEX__/${local_hex}}"
  rules_script="${rules_script//__FUGUE_SERVICE_HEX__/${service_hex}}"
  interface_script="$(cat <<'EOF'
set -eu
command -v ip >/dev/null 2>&1
if ip link show nodelocaldns >/dev/null 2>&1; then
  ip link delete nodelocaldns
fi
test ! -e /sys/class/net/nodelocaldns
EOF
)"
  while IFS= read -r node_name; do
    [[ -n "${node_name}" ]] || continue
    node_local_dns_run_probe_pod "${node_name}" rule-cleanup "${FUGUE_NODE_LOCAL_DNS_IMAGE_REPOSITORY}@${FUGUE_NODE_LOCAL_DNS_IMAGE_DIGEST}" true true "${rules_script}" true || return 1
    node_local_dns_run_probe_pod "${node_name}" interface-cleanup "${FUGUE_NODE_LOCAL_DNS_PROBE_IMAGE}" true true "${interface_script}" || return 1
  done <<<"${NODE_LOCAL_DNS_TARGET_NODES}"
  node_local_dns_verify_teardown "${service_ip}"
}

node_local_dns_delete_daemonset_safely() {
  local service_ip="$1"
  local daemonset_name="${NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME}"
  local daemonset_ref=""
  local daemonset_json=""
  local daemonset_snapshot=""
  local previous_mode=""
  local deadline=""
  local remaining_pods=""
  local pod_selector="$(node_local_dns_active_pod_selector)"

  if [[ "${NODE_LOCAL_DNS_SPLIT_COHORT}" == "true" && "${daemonset_name}" == "${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME}" ]]; then
    log "refusing to delete the preserved offline NodeLocal DNSCache DaemonSet"
    return 1
  fi

  if ! daemonset_ref="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get daemonset "${daemonset_name}" --ignore-not-found -o name)"; then
    log "cannot determine whether the NodeLocal DNSCache DaemonSet exists"
    return 1
  fi
  if [[ -n "$(trim_field "${daemonset_ref}")" ]]; then
    node_local_dns_verify_central_coredns_ready || return 1
    if ! daemonset_json="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get daemonset "${daemonset_name}" -o json)"; then
      log "cannot snapshot the NodeLocal DNSCache DaemonSet before removal"
      return 1
    fi
    if ! daemonset_snapshot="$(printf '%s' "${daemonset_json}" | python3 -c '
import json
import sys

source = json.load(sys.stdin)
metadata = source.get("metadata") or {}
snapshot = {
    "apiVersion": source["apiVersion"],
    "kind": source["kind"],
    "metadata": {
        "name": metadata["name"],
        "namespace": metadata["namespace"],
        "labels": metadata.get("labels") or {},
        "annotations": metadata.get("annotations") or {},
    },
    "spec": source["spec"],
}
print(json.dumps(snapshot, separators=(",", ":")))
')"; then
      log "cannot normalize the NodeLocal DNSCache DaemonSet rollback snapshot"
      return 1
    fi
    previous_mode="$(printf '%s' "${daemonset_json}" | python3 -c 'import json,sys; print((((json.load(sys.stdin).get("spec") or {}).get("template") or {}).get("metadata") or {}).get("labels", {}).get("fugue.io/node-local-dns-mode", ""))')"
    [[ "${previous_mode}" == "shadow" || "${previous_mode}" == "iptables" ]] || {
      log "cannot safely remove NodeLocal DNSCache because its current mode is unknown"
      return 1
    }
    log "deleting NodeLocal DNSCache DaemonSet before removing its central CoreDNS upstream"
    if ! ${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" delete daemonset "${daemonset_name}" \
      --cascade=foreground --wait=true --timeout="${FUGUE_ROLLOUT_TIMEOUT}"; then
      log "NodeLocal DNSCache DaemonSet deletion failed"
      return 1
    fi
  fi
  deadline=$((SECONDS + FUGUE_NODE_LOCAL_DNS_PROBE_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    if ! remaining_pods="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get pods -l "${pod_selector}" --no-headers)"; then
      log "cannot verify NodeLocal DNSCache Pod termination"
      [[ -z "${daemonset_snapshot}" ]] || node_local_dns_restore_daemonset_snapshot "${daemonset_snapshot}" "${previous_mode}" "${service_ip}" || true
      return 1
    fi
    [[ -z "$(trim_field "${remaining_pods}")" ]] && break
    sleep 2
  done
  if ! remaining_pods="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get pods -l "${pod_selector}" --no-headers)"; then
    log "cannot complete NodeLocal DNSCache Pod termination verification"
    [[ -z "${daemonset_snapshot}" ]] || node_local_dns_restore_daemonset_snapshot "${daemonset_snapshot}" "${previous_mode}" "${service_ip}" || true
    return 1
  fi
  if [[ -n "$(trim_field "${remaining_pods}")" ]]; then
    log "NodeLocal DNSCache Pods remain after foreground DaemonSet deletion; refusing teardown while they can still own host networking state"
    [[ -z "${daemonset_snapshot}" ]] || node_local_dns_restore_daemonset_snapshot "${daemonset_snapshot}" "${previous_mode}" "${service_ip}" || true
    return 1
  fi
  if ! daemonset_ref="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get daemonset "${daemonset_name}" --ignore-not-found -o name)"; then
    log "cannot verify final NodeLocal DNSCache DaemonSet absence"
    [[ -z "${daemonset_snapshot}" ]] || node_local_dns_restore_daemonset_snapshot "${daemonset_snapshot}" "${previous_mode}" "${service_ip}" || true
    return 1
  fi
  if [[ -n "$(trim_field "${daemonset_ref}")" ]]; then
    log "NodeLocal DNSCache DaemonSet still exists after deletion"
    return 1
  fi
  if node_local_dns_verify_teardown "${service_ip}"; then
    if node_local_dns_verify_authoritative_cohort "${NODE_LOCAL_DNS_TARGET_NODES}"; then
      return 0
    fi
    log "NodeLocal DNSCache teardown completed, but authoritative DNS coexistence changed; restoring the pre-removal DaemonSet"
    if [[ -n "${daemonset_snapshot}" ]]; then
      node_local_dns_restore_daemonset_snapshot "${daemonset_snapshot}" "${previous_mode}" "${service_ip}" || true
    fi
    return 1
  fi
  log "normal NodeLocal DNSCache teardown left residue; attempting exact-comment and exact-interface cleanup"
  if node_local_dns_recover_exact_residue "${service_ip}"; then
    if node_local_dns_verify_authoritative_cohort "${NODE_LOCAL_DNS_TARGET_NODES}"; then
      return 0
    fi
    log "NodeLocal DNSCache residue cleanup completed, but authoritative DNS coexistence changed"
  fi
  if [[ -n "${daemonset_snapshot}" ]]; then
    if ! node_local_dns_restore_daemonset_snapshot "${daemonset_snapshot}" "${previous_mode}" "${service_ip}"; then
      log "NodeLocal DNSCache removal failed and its pre-removal DaemonSet snapshot could not be restored"
      return 1
    fi
    log "NodeLocal DNSCache removal postconditions failed; the exact pre-removal DaemonSet was restored and verified"
  fi
  return 1
}

relieve_primary_disk_pressure() {
  local primary_node_name=""
  local cleanup_cmd=""

  primary_node_name="$(detect_primary_node_name)"
  if [[ -z "${primary_node_name}" ]]; then
    log "skip primary disk-pressure recovery because the primary node could not be identified"
    return 0
  fi
  if ! primary_node_is_ready "${primary_node_name}"; then
    log "skip primary disk-pressure recovery because primary node ${primary_node_name} is NotReady"
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

run_bounded_host_command() {
  local timeout_seconds="$1"
  local pid=""
  local state=""
  local deadline=""
  shift

  "$@" &
  pid="$!"
  deadline=$((SECONDS + timeout_seconds))

  while true; do
    state="$(awk '{print $3}' "/proc/${pid}/stat" 2>/dev/null || true)"
    if [[ -z "${state}" || "${state}" == "Z" ]]; then
      wait "${pid}"
      return $?
    fi
    if (( SECONDS >= deadline )); then
      log "timed out after ${timeout_seconds}s: $*"
      kill "${pid}" >/dev/null 2>&1 || true
      sleep 2
      kill -KILL "${pid}" >/dev/null 2>&1 || true
      return 124
    fi
    sleep 1
  done
}

registry_root="/var/lib/fugue/registry"
runner_update_root="/home/github-runner/actions-runner-work/_update"
registry_image="docker.io/library/registry:2.8.3"
stale_upload_minutes="${FUGUE_REGISTRY_UPLOAD_STALE_MINUTES:-1440}"
gc_id="fugue-registry-gc-$(date +%s)"

cleanup() {
  k3s ctr tasks kill "${gc_id}" >/dev/null 2>&1 || true
  k3s ctr containers rm "${gc_id}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

purge_stale_registry_uploads() {
  local repositories_root="${registry_root}/docker/registry/v2/repositories"
  local uploads_root=""
  local path=""

  [[ -d "${repositories_root}" ]] || return 0

  while IFS= read -r uploads_root; do
    while IFS= read -r path; do
      [[ -n "${path}" ]] || continue
      rm -rf -- "${path}"
      log "removed stale registry upload ${path}"
    done < <(find "${uploads_root}" -mindepth 1 -maxdepth 1 -type d -mmin "+${stale_upload_minutes}" -print)
  done < <(find "${repositories_root}" -type d -name '_uploads' -print)
}

log "filesystem usage before cleanup"
df -h /
du -sh "${registry_root}" 2>/dev/null || true

if command -v k3s >/dev/null 2>&1; then
  if run_bounded_host_command 90 k3s crictl rmi --prune >/tmp/fugue-primary-image-prune.log 2>&1; then
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

purge_stale_registry_uploads

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
  local primary_node_name=""

  if [[ "${FUGUE_SYNC_EDGE_PROXY:-false}" != "true" ]]; then
    log "skip Route A edge proxy sync because FUGUE_SYNC_EDGE_PROXY=${FUGUE_SYNC_EDGE_PROXY:-false}"
    return
  fi
  if [[ -z "${FUGUE_API_PUBLIC_DOMAIN:-}" ]]; then
    return
  fi
  primary_node_name="$(detect_primary_node_name)"
  if [[ -n "${primary_node_name}" ]] && ! primary_node_is_ready "${primary_node_name}"; then
    log "skip Route A edge proxy sync because primary node ${primary_node_name} is NotReady"
    return 0
  fi

  if ! prepare_control_plane_automation_ssh; then
    log "warning: Route A edge proxy sync skipped because local control-plane automation SSH is unavailable"
    return 0
  fi
  export FUGUE_DOMAIN="${FUGUE_API_PUBLIC_DOMAIN}"
  log "syncing Route A edge proxy through scripts/sync_fugue_edge_proxy.sh"
  if ! bash ./scripts/sync_fugue_edge_proxy.sh; then
    log "warning: Route A edge proxy sync failed; continuing because edge/API rollout already completed"
  fi
}

label_default_builder_nodes() {
  log "keeping primary control-plane node out of the shared runtime and builder pools"
  ${KUBECTL} label node -l fugue.install/role=primary \
    fugue.io/shared-pool- \
    fugue.io/build- \
    fugue.io/build-tier- \
    --overwrite >/dev/null || true

  log "labeling non-primary combined nodes as builder candidates"
  ${KUBECTL} label node -l 'fugue.install/profile=combined,fugue.install/role!=primary' \
    fugue.io/build=true \
    fugue.io/build-tier- \
    --overwrite >/dev/null
}

validate_control_plane_singleton_anchor() {
  if [[ "${FUGUE_CONTROL_PLANE_SINGLETONS_ENABLED}" != "true" ]]; then
    return 0
  fi

  local selector node_count node_name ready control_plane_role
  selector="$(trim_field "${FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR}")"
  [[ -n "${selector}" ]] || fail "FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR is required when FUGUE_CONTROL_PLANE_SINGLETONS_ENABLED=true"
  selector_yaml "${selector}" "" >/dev/null

  node_count="$(${KUBECTL} get nodes -l "${selector}" --no-headers 2>/dev/null | wc -l | awk '{print $1}')"
  if [[ "${node_count}" != "1" ]]; then
    fail "FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR must match exactly one node; selector=${selector} matched ${node_count}"
  fi

  node_name="$(${KUBECTL} get nodes -l "${selector}" -o jsonpath='{.items[0].metadata.name}')"
  ready="$(${KUBECTL} get node "${node_name}" -o jsonpath='{range .status.conditions[?(@.type=="Ready")]}{.status}{end}')"
  if [[ "${ready}" != "True" ]]; then
    fail "control-plane singleton anchor node ${node_name} is not Ready"
  fi

  control_plane_role="$(${KUBECTL} get node "${node_name}" -o jsonpath='{.metadata.labels.node-role\.kubernetes\.io/control-plane}' 2>/dev/null || true)"
  if [[ -n "${control_plane_role}" ]]; then
    fail "control-plane singleton anchor node ${node_name} must not be a control-plane node"
  fi

  log "control-plane singleton anchor: node=${node_name} selector=${selector}"
}

node_local_dns_restore_previous_after_helm_rollback() {
  local service_ip="${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}"
  local daemonset_json=""
  local template_mode=""
  local live_targets=""
  local generation=""
  local observed_generation=""
  local restored_revision=""
  local current_pods=""
  local restore_rows=""
  local node_name=""
  local current_name=""
  local current_uid=""
  local current_mode=""
  local current_revision=""
  local current_ready=""
  local current_clean=""
  local snapshot_uid=""
  local snapshot_revision=""
  local replacement_row=""
  local replacement_name=""
  local replacement_uid=""
  local deadline=0
  local added_present=""
  local saved_targets="${NODE_LOCAL_DNS_TARGET_NODES}"

  [[ "${NODE_LOCAL_DNS_PREVIOUS_ENABLED}" == "true" ]] || return 1
  deadline=$((SECONDS + FUGUE_NODE_LOCAL_DNS_PROBE_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    daemonset_json="$(${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" get daemonset "${NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME}" -o json 2>/dev/null || true)"
    if [[ -n "${daemonset_json}" ]]; then
      IFS=$'\t' read -r template_mode generation observed_generation < <(
        printf '%s' "${daemonset_json}" | python3 -c '
import json
import sys

doc = json.load(sys.stdin)
metadata = doc.get("metadata") or {}
status = doc.get("status") or {}
template = ((doc.get("spec") or {}).get("template") or {})
labels = (template.get("metadata") or {}).get("labels") or {}
print("\t".join([
    str(labels.get("fugue.io/node-local-dns-mode") or ""),
    str(metadata.get("generation") or ""),
    str(status.get("observedGeneration") or ""),
]))
'
      )
      live_targets="$(node_local_dns_daemonset_target_nodes "${daemonset_json}" 2>/dev/null || true)"
      if [[ "${template_mode}" == "${NODE_LOCAL_DNS_PREVIOUS_MODE}" &&
            -n "${generation}" && "${generation}" == "${observed_generation}" &&
            "${live_targets}" == "${NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES}" ]]; then
        restored_revision="$(node_local_dns_current_controller_revision \
          "${NODE_LOCAL_DNS_PREVIOUS_MODE}" "${NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES}" 2>/dev/null || true)"
        [[ -z "${restored_revision}" ]] || break
      fi
    fi
    sleep 2
  done
  [[ "${template_mode}" == "${NODE_LOCAL_DNS_PREVIOUS_MODE}" &&
      -n "${generation}" && "${generation}" == "${observed_generation}" &&
      -n "${restored_revision}" ]] || {
    log "refusing NodeLocal DNSCache rollback Pod replacement because the restored DaemonSet was not observed at the expected ${NODE_LOCAL_DNS_PREVIOUS_MODE} template"
    return 1
  }
  [[ "${live_targets}" == "${NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES}" ]] || {
    log "refusing NodeLocal DNSCache rollback Pod replacement because the restored target cohort differs from the pre-release snapshot"
    return 1
  }
  NODE_LOCAL_DNS_TARGET_NODES="${NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES}"
  node_local_dns_verify_artifact "${NODE_LOCAL_DNS_PREVIOUS_MODE}" "${service_ip}" false || {
    NODE_LOCAL_DNS_TARGET_NODES="${saved_targets}"
    return 1
  }

  if [[ -n "$(trim_field "${NODE_LOCAL_DNS_ADDED_NODES}")" ]]; then
    deadline=$((SECONDS + FUGUE_NODE_LOCAL_DNS_PROBE_TIMEOUT_SECONDS))
    while (( SECONDS < deadline )); do
      current_pods="$(node_local_dns_capture_pods_json 2>/dev/null || true)"
      added_present="$(PODS_JSON="${current_pods:-[]}" ADDED_NODES="${NODE_LOCAL_DNS_ADDED_NODES}" python3 -c '
import json
import os

added = {item.strip() for item in os.environ["ADDED_NODES"].splitlines() if item.strip()}
print("true" if any(pod.get("node") in added for pod in json.loads(os.environ["PODS_JSON"])) else "false")
' 2>/dev/null || printf true)"
      [[ "${added_present}" != "true" ]] && break
      sleep 2
    done
    [[ "${added_present}" != "true" ]] || {
      NODE_LOCAL_DNS_TARGET_NODES="${saved_targets}"
      return 1
    }
    NODE_LOCAL_DNS_TARGET_NODES="${NODE_LOCAL_DNS_ADDED_NODES}"
    node_local_dns_verify_teardown "${service_ip}" || {
      NODE_LOCAL_DNS_TARGET_NODES="${saved_targets}"
      return 1
    }
    while IFS= read -r node_name; do
      [[ -n "${node_name}" ]] || continue
      node_local_dns_verify_authoritative_coexistence "${node_name}" || {
        NODE_LOCAL_DNS_TARGET_NODES="${saved_targets}"
        return 1
      }
    done <<<"${NODE_LOCAL_DNS_ADDED_NODES}"
    NODE_LOCAL_DNS_TARGET_NODES="${NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES}"
  fi

  current_pods="$(node_local_dns_capture_pods_json)" || {
    NODE_LOCAL_DNS_TARGET_NODES="${saved_targets}"
    return 1
  }
  restore_rows="$(CURRENT_PODS="${current_pods}" SNAPSHOT_PODS="${NODE_LOCAL_DNS_PREVIOUS_PODS_JSON}" TARGET_NODES="${NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES}" EXPECTED_MODE="${NODE_LOCAL_DNS_PREVIOUS_MODE}" RESTORED_REVISION="${restored_revision}" REPLACED_NODES="${NODE_LOCAL_DNS_REPLACED_NODES}" FAILED_NODE="${NODE_LOCAL_DNS_FAILED_NODE}" python3 -c '
import json
import os

current = {}
for pod in json.loads(os.environ["CURRENT_PODS"]):
    node = str(pod.get("node") or "")
    if not node or node in current:
        raise SystemExit(1)
    current[node] = pod
snapshot = {}
for pod in json.loads(os.environ["SNAPSHOT_PODS"]):
    node = str(pod.get("node") or "")
    if not node or node in snapshot:
        raise SystemExit(1)
    snapshot[node] = pod
targets = [item.strip() for item in os.environ["TARGET_NODES"].splitlines() if item.strip()]
replaced = [item.strip() for item in os.environ["REPLACED_NODES"].splitlines() if item.strip()]
replaced_order = {node: index for index, node in enumerate(replaced)}
failed_node = os.environ["FAILED_NODE"].strip()
restored_revision = os.environ["RESTORED_REVISION"]
if set(snapshot) != set(targets) or any(node not in set(targets) for node in current):
    raise SystemExit(1)
rows = []
for target_index, node in enumerate(targets):
    old = snapshot[node]
    if (
        not old.get("name")
        or not old.get("uid")
        or not old.get("revision")
        or not old.get("ready")
        or old.get("phase") != "Running"
        or old.get("mode") != os.environ["EXPECTED_MODE"]
        or int(old.get("restart_count") or 0) != 0
        or old.get("waiting_reasons")
        or old.get("terminated_reasons")
        or old.get("last_waiting_reasons")
        or old.get("last_terminated_reasons")
    ):
        raise SystemExit(1)
    live = current.get(node)
    if live is None:
        values = [node, "__missing__", "__missing__", "__missing__", "__missing__", "false", "false", str(old["uid"]), str(old["revision"])]
    else:
        if not live.get("name") or not live.get("uid"):
            raise SystemExit(1)
        clean = (
            live.get("phase") == "Running"
            and int(live.get("restart_count") or 0) == 0
            and not live.get("waiting_reasons")
            and not live.get("terminated_reasons")
            and not live.get("last_waiting_reasons")
            and not live.get("last_terminated_reasons")
        )
        values = [
            node,
            str(live["name"]),
            str(live["uid"]),
            str(live.get("mode") or "__missing__"),
            str(live.get("revision") or "__missing__"),
            "true" if live.get("ready") else "false",
            "true" if clean else "false",
            str(old["uid"]),
            str(old["revision"]),
        ]
    if node == failed_node:
        priority = -1
        secondary = 0
    elif live is None or not live.get("ready") or not clean:
        priority = 0
        secondary = target_index
    elif live.get("uid") == old.get("uid"):
        priority = 3
        secondary = target_index
    elif live.get("mode") == os.environ["EXPECTED_MODE"] and live.get("revision") == restored_revision:
        priority = 2
        secondary = target_index
    elif node in replaced_order:
        priority = 1
        secondary = -replaced_order[node]
    else:
        priority = 0
        secondary = target_index
    rows.append((priority, secondary, target_index, values))
for _, _, _, values in sorted(rows):
    print("|".join(values))
')" || {
    NODE_LOCAL_DNS_TARGET_NODES="${saved_targets}"
    return 1
  }
  while IFS='|' read -r node_name current_name current_uid current_mode current_revision current_ready current_clean snapshot_uid snapshot_revision; do
    node_local_dns_verify_preserved_snapshot_after_helm_rollback || {
      NODE_LOCAL_DNS_TARGET_NODES="${saved_targets}"
      return 1
    }
    [[ -n "${node_name}" && -n "${snapshot_uid}" && -n "${snapshot_revision}" ]] || {
      NODE_LOCAL_DNS_TARGET_NODES="${saved_targets}"
      return 1
    }
    if [[ "${current_name}" == "__missing__" ]]; then
      log "waiting for the restored NodeLocal DNSCache DaemonSet to recreate its Pod on ${node_name}"
      replacement_row="$(node_local_dns_wait_for_pod_on_node "${node_name}" "" "${NODE_LOCAL_DNS_PREVIOUS_MODE}" "${restored_revision}")" || {
        NODE_LOCAL_DNS_TARGET_NODES="${saved_targets}"
        return 1
      }
      IFS=$'\t' read -r replacement_name replacement_uid <<<"${replacement_row}"
      node_local_dns_verify_one_node "${replacement_name}" "${node_name}" "${NODE_LOCAL_DNS_PREVIOUS_MODE}" "${service_ip}" || {
        NODE_LOCAL_DNS_TARGET_NODES="${saved_targets}"
        return 1
      }
      continue
    fi
    if [[ "${current_uid}" == "${snapshot_uid}" ]]; then
      [[ "${current_mode}" == "${NODE_LOCAL_DNS_PREVIOUS_MODE}" && "${current_revision}" == "${snapshot_revision}" && "${current_ready}" == "true" && "${current_clean}" == "true" ]] || {
        log "original NodeLocal DNSCache Pod ${current_name} changed health, mode, or revision during rollback; refusing to delete its pinned pre-release UID"
        NODE_LOCAL_DNS_TARGET_NODES="${saved_targets}"
        return 1
      }
      continue
    fi
    if [[ "${current_mode}" == "${NODE_LOCAL_DNS_PREVIOUS_MODE}" && "${current_revision}" == "${restored_revision}" && "${current_ready}" == "true" && "${current_clean}" == "true" ]]; then
      node_local_dns_verify_one_node "${current_name}" "${node_name}" "${NODE_LOCAL_DNS_PREVIOUS_MODE}" "${service_ip}" || {
        NODE_LOCAL_DNS_TARGET_NODES="${saved_targets}"
        return 1
      }
      log "preserving already-restored NodeLocal DNSCache Pod on ${node_name} uid=${current_uid}"
      continue
    fi
    log "restoring NodeLocal DNSCache Pod on ${node_name} from the verified rollback template"
    ${KUBECTL} -n "${FUGUE_NODE_LOCAL_DNS_NAMESPACE}" delete pod "${current_name}" --wait=false >/dev/null || {
      NODE_LOCAL_DNS_TARGET_NODES="${saved_targets}"
      return 1
    }
    replacement_row="$(node_local_dns_wait_for_pod_on_node "${node_name}" "${current_uid}" "${NODE_LOCAL_DNS_PREVIOUS_MODE}" "${restored_revision}")" || {
      NODE_LOCAL_DNS_TARGET_NODES="${saved_targets}"
      return 1
    }
    IFS=$'\t' read -r replacement_name replacement_uid <<<"${replacement_row}"
    node_local_dns_verify_one_node "${replacement_name}" "${node_name}" "${NODE_LOCAL_DNS_PREVIOUS_MODE}" "${service_ip}" || {
      NODE_LOCAL_DNS_TARGET_NODES="${saved_targets}"
      return 1
    }
  done <<<"${restore_rows}"
  node_local_dns_verify_preserved_snapshot_after_helm_rollback || {
    NODE_LOCAL_DNS_TARGET_NODES="${saved_targets}"
    return 1
  }
  node_local_dns_verify_running "${NODE_LOCAL_DNS_PREVIOUS_MODE}" "${service_ip}" || {
    NODE_LOCAL_DNS_TARGET_NODES="${saved_targets}"
    return 1
  }
  NODE_LOCAL_DNS_TARGET_NODES="${saved_targets}"
}

dns_manifest_transaction_smoke_urls() {
  printf '%s' "${FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS:-${FUGUE_SMOKE_URLS:-${FUGUE_SMOKE_URL:-}}}"
}

run_dns_manifest_library_action() {
  local action="$1"
  local snapshot_file="$2"
  local smoke_urls

  smoke_urls="$(dns_manifest_transaction_smoke_urls)"
  FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true \
    FUGUE_NAMESPACE="${FUGUE_NAMESPACE}" \
    FUGUE_RELEASE_NAME="${FUGUE_RELEASE_NAME}" \
    FUGUE_RELEASE_FULLNAME="${FUGUE_RELEASE_FULLNAME}" \
    FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=false \
    FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS="${smoke_urls}" \
    KUBECTL="${KUBECTL:-}" \
    bash -c '
set -euo pipefail
script_path="$1"
action="$2"
snapshot_file="$3"
source "${script_path}"
detect_kubectl
command_exists host || fail "host is required for authoritative DNS transaction validation"
case "${action}" in
  capture)
    validate_representative_smoke_configuration
    capture_dns_manifest_snapshot "${snapshot_file}"
    ;;
  restore)
    restore_dns_manifest_snapshot "${snapshot_file}"
    ;;
  *)
    fail "unsupported DNS manifest library action: ${action}"
    ;;
esac
' bash "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh" "${action}" "${snapshot_file}"
}

prepare_dns_manifest_transaction() {
  local public_mode="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_MODE:-auto}"
  local snapshot_dir

  DNS_MANIFEST_TRANSACTION_REQUIRED="false"
  DNS_MANIFEST_TRANSACTION_COMPLETED="false"
  DNS_MANIFEST_ROLLBACK_RESTORED="false"
  DNS_MANIFEST_SNAPSHOT_KEEP="false"
  if [[ "${public_mode}" != "allow" || "${FUGUE_DNS_ENABLED}" != "true" ]]; then
    return 0
  fi

  snapshot_dir="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
  mkdir -p "${snapshot_dir}" || return $?
  DNS_MANIFEST_SNAPSHOT_FILE="$(mktemp "${snapshot_dir%/}/fugue-dns-manifest-snapshot.XXXXXX.json")" || return $?
  chmod 600 "${DNS_MANIFEST_SNAPSHOT_FILE}" || return $?
  if ! run_dns_manifest_library_action capture "${DNS_MANIFEST_SNAPSHOT_FILE}"; then
    rm -f "${DNS_MANIFEST_SNAPSHOT_FILE}"
    DNS_MANIFEST_SNAPSHOT_FILE=""
    return 1
  fi
  DNS_MANIFEST_TRANSACTION_REQUIRED="true"
  log "DNS public manifest transaction armed with a verified pre-Helm snapshot"
}

run_dns_manifest_transaction_after_helm() {
  local smoke_urls
  local node_local_dns_snapshot_targets=""

  [[ "${DNS_MANIFEST_TRANSACTION_REQUIRED:-false}" == "true" ]] || return 0
  [[ -n "${DNS_MANIFEST_SNAPSHOT_FILE:-}" && -f "${DNS_MANIFEST_SNAPSHOT_FILE}" ]] || {
    log_stderr "DNS manifest transaction is armed without its pre-Helm snapshot"
    DNS_MANIFEST_SNAPSHOT_KEEP="true"
    return 1
  }
  smoke_urls="$(dns_manifest_transaction_smoke_urls)"
  if ! FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY=dns-manifest-ondelete \
    FUGUE_PUBLIC_DATA_PLANE_DNS_SNAPSHOT_FILE="${DNS_MANIFEST_SNAPSHOT_FILE}" \
    FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=false \
    FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS="${smoke_urls}" \
    FUGUE_NAMESPACE="${FUGUE_NAMESPACE}" \
    FUGUE_RELEASE_NAME="${FUGUE_RELEASE_NAME}" \
    FUGUE_RELEASE_FULLNAME="${FUGUE_RELEASE_FULLNAME}" \
    KUBECTL="${KUBECTL:-}" \
    bash "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"; then
    DNS_MANIFEST_SNAPSHOT_KEEP="true"
    return 1
  fi
  DNS_MANIFEST_TRANSACTION_COMPLETED="true"
  PUBLIC_DATA_PLANE_RELEASED="true"
  if [[ "${FUGUE_NODE_LOCAL_DNS_ENABLED:-false}" == "true" ]]; then
    node_local_dns_snapshot_targets="${NODE_LOCAL_DNS_TARGET_NODES:-}"
  elif [[ "${NODE_LOCAL_DNS_PREVIOUS_ENABLED:-false}" == "true" ]]; then
    node_local_dns_snapshot_targets="${NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES:-}"
  fi
  if ! node_local_dns_refresh_authoritative_hostport_snapshot "${node_local_dns_snapshot_targets}" "DNS manifest"; then
    DNS_MANIFEST_SNAPSHOT_KEEP="true"
    return 1
  fi
  log "DNS public manifest transaction completed and remains rollback-pinned until all release gates pass"
}

restore_dns_manifest_transaction_after_helm_rollback() {
  local node_local_dns_snapshot_targets=""

  [[ "${DNS_MANIFEST_TRANSACTION_REQUIRED:-false}" == "true" ]] || return 0
  [[ -n "${DNS_MANIFEST_SNAPSHOT_FILE:-}" && -f "${DNS_MANIFEST_SNAPSHOT_FILE}" ]] || {
    log_stderr "cannot restore DNS manifest transaction because the pre-Helm snapshot is missing"
    DNS_MANIFEST_SNAPSHOT_KEEP="true"
    return 1
  }
  if ! run_dns_manifest_library_action restore "${DNS_MANIFEST_SNAPSHOT_FILE}"; then
    DNS_MANIFEST_SNAPSHOT_KEEP="true"
    return 1
  fi
  if [[ "${NODE_LOCAL_DNS_PREVIOUS_ENABLED:-false}" == "true" ]]; then
    node_local_dns_snapshot_targets="${NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES:-}"
  fi
  if ! node_local_dns_refresh_authoritative_hostport_snapshot "${node_local_dns_snapshot_targets}" "DNS manifest rollback"; then
    DNS_MANIFEST_SNAPSHOT_KEEP="true"
    return 1
  fi
  DNS_MANIFEST_TRANSACTION_COMPLETED="false"
  DNS_MANIFEST_ROLLBACK_RESTORED="true"
  DNS_MANIFEST_SNAPSHOT_KEEP="false"
  log "DNS public manifest transaction restored to the verified pre-Helm template and Pod revisions"
}

finalize_dns_manifest_transaction() {
  [[ "${DNS_MANIFEST_TRANSACTION_REQUIRED:-false}" == "true" ]] || return 0
  [[ "${DNS_MANIFEST_TRANSACTION_COMPLETED:-false}" == "true" ]] || {
    log_stderr "refusing to finalize an incomplete DNS manifest transaction"
    DNS_MANIFEST_SNAPSHOT_KEEP="true"
    return 1
  }
  DNS_MANIFEST_SNAPSHOT_KEEP="false"
  if [[ -n "${DNS_MANIFEST_SNAPSHOT_FILE:-}" && -f "${DNS_MANIFEST_SNAPSHOT_FILE}" ]]; then
    rm -f "${DNS_MANIFEST_SNAPSHOT_FILE}"
  fi
  DNS_MANIFEST_SNAPSHOT_FILE=""
  log "DNS public manifest transaction finalized after all release gates passed"
}

rollback_release() {
  local rollback_api_deployment="${FUGUE_API_DEPLOYMENT_NAME}"
  local helm_rollback_failed="false"
  local dns_restore_failed="false"
  local image_cache_restore_failed="false"

  if [[ -z "${PREVIOUS_REVISION:-}" ]]; then
    log "skip rollback because no previous revision was captured"
    return 1
  fi

  if [[ "${NODE_LOCAL_DNS_PREVIOUS_ENABLED:-false}" != "true" && "${FUGUE_NODE_LOCAL_DNS_ENABLED:-false}" == "true" ]]; then
    if ! node_local_dns_delete_daemonset_safely "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}"; then
      log "refusing Helm rollback until NodeLocal DNSCache interception is safely removed"
      return 1
    fi
  fi

  log "rolling back release ${FUGUE_RELEASE_NAME} to revision ${PREVIOUS_REVISION}"
  if ! helm rollback "${FUGUE_RELEASE_NAME}" "${PREVIOUS_REVISION}" \
    -n "${FUGUE_NAMESPACE}" \
    --timeout "${FUGUE_HELM_TIMEOUT}"; then
    log "Helm rollback failed; still attempting exact DNS manifest restoration"
    helm_rollback_failed="true"
  fi

  if ! image_cache_restore_ondelete_after_helm_rollback; then
    log "image-cache OnDelete guard could not be restored after Helm rollback"
    image_cache_restore_failed="true"
  fi

  if ! restore_dns_manifest_transaction_after_helm_rollback; then
    log "DNS manifest rollback could not restore the exact pre-Helm cohort"
    dns_restore_failed="true"
  fi
  if [[ "${helm_rollback_failed}" == "true" || "${dns_restore_failed}" == "true" || "${image_cache_restore_failed}" == "true" ]]; then
    return 1
  fi
  if ! node_local_dns_verify_preserved_snapshot_after_helm_rollback; then
    log "preserved offline NodeLocal DNSCache DaemonSet or Pod changed before active rollback reconciliation"
    return 1
  fi

  if [[ "${NODE_LOCAL_DNS_PREVIOUS_ENABLED:-false}" == "true" ]]; then
    if ! node_local_dns_restore_previous_after_helm_rollback; then
      log "NodeLocal DNSCache rollback could not restore the exact previous cohort; refusing broad teardown or further Pod deletion"
      return 1
    fi
  elif [[ "${FUGUE_NODE_LOCAL_DNS_ENABLED:-false}" == "true" && -n "$(trim_field "${NODE_LOCAL_DNS_TARGET_NODES:-}")" ]]; then
    if ! node_local_dns_verify_teardown "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}"; then
      log "NodeLocal DNSCache rollback left network state or failed central DNS fallback verification"
      return 1
    fi
  fi
  if ! node_local_dns_verify_preserved_snapshot_after_helm_rollback; then
    log "preserved offline NodeLocal DNSCache DaemonSet or Pod changed across Helm rollback"
    return 1
  fi

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

prepare_release_domains() {
  local public_mode build_mode stateful_mode maintenance_mode

  IMAGE_CACHE_ONDELETE_STRATEGY_MIGRATION="false"
  refresh_release_changed_files_from_live_api
  if ! prepare_release_safety_runtime_intents; then
    fail "failed to attribute runtime release safety intents"
  fi
  log "release safety watch selection: $(release_safety_watch_window_summary)"
  write_release_safety_attribution || true
  if ! require_release_safety_attribution; then
    fail "release contains unattributed runtime changes; classify them or explicitly approve the high-risk hold"
  fi

  public_mode="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_MODE:-auto}"
  build_mode="${FUGUE_NODE_LOCAL_BUILD_PLANE_RELEASE_MODE:-auto}"
  stateful_mode="${FUGUE_STATEFUL_DEPENDENCY_RELEASE_MODE:-guard}"
  maintenance_mode="${FUGUE_MAINTENANCE_AGENT_RELEASE_MODE:-preserve}"
  FUGUE_EDGE_HELM_IMAGE_REPOSITORY="${FUGUE_EDGE_IMAGE_REPOSITORY}"
  FUGUE_EDGE_HELM_IMAGE_TAG="${FUGUE_EDGE_IMAGE_TAG}"

  case "${public_mode}" in
    auto|preserve|allow)
      ;;
    *)
      fail "FUGUE_PUBLIC_DATA_PLANE_RELEASE_MODE must be auto, preserve, or allow"
      ;;
  esac
  case "${build_mode}" in
    auto|preserve|allow)
      ;;
    *)
      fail "FUGUE_NODE_LOCAL_BUILD_PLANE_RELEASE_MODE must be auto, preserve, or allow"
      ;;
  esac
  case "${stateful_mode}" in
    guard|allow)
      ;;
    *)
      fail "FUGUE_STATEFUL_DEPENDENCY_RELEASE_MODE must be guard or allow"
      ;;
  esac
  case "${maintenance_mode}" in
    preserve|allow)
      ;;
    *)
      fail "FUGUE_MAINTENANCE_AGENT_RELEASE_MODE must be preserve or allow"
      ;;
  esac

  if node_local_dns_split_release_enabled && [[ "${build_mode}" == "allow" ]]; then
    fail "node-local build-plane release cannot be forced while a preserved offline node exists; restore or remove that node before changing the image-cache Pod template"
  fi

  if stateful_dependency_changed && [[ "${stateful_mode}" != "allow" ]]; then
    fail "stateful dependency manifests changed; ship registry, mesh, postgres, or shared storage through an isolated release window"
  fi

  if [[ "${public_mode}" != "allow" ]]; then
    if public_data_plane_manifest_changed; then
      log "public data-plane manifests changed; preserving live edge/DNS rendered values so this control-plane release does not roll public traffic"
    fi
    if public_data_plane_changed; then
      log "public data-plane files changed; preserving live edge/DNS DaemonSet spec for this control-plane release"
    fi
    preserve_public_data_plane_from_live
  else
    log "public data-plane release explicitly allowed"
  fi

  if [[ "${build_mode}" != "allow" ]]; then
    if node_local_build_plane_manifest_changed || \
      { node_local_dns_split_release_enabled && { node_local_build_plane_any_values_changed || node_local_build_plane_shared_manifest_changed; }; }; then
      if image_cache_ondelete_strategy_migration_allowed; then
        IMAGE_CACHE_ONDELETE_STRATEGY_MIGRATION="true"
        log "allowing the exact image-cache OnDelete strategy-only migration while strictly preserving the live image and resources"
      elif image_cache_strategy_migration_already_applied; then
        log "image-cache strategy migration is already recorded in the live Helm release; ignoring the stale image-tag chart baseline"
      else
        fail "node-local build-plane manifests changed; ship image-cache through an isolated worker/front release before changing its rendered pod spec"
      fi
    fi
    if node_local_dns_split_release_enabled; then
      log "preserved offline node is present; deferring all image-cache image/resource rollout and freezing the live Pod template"
      preserve_node_local_build_plane_from_live true true true || fail "failed to strictly preserve the live image-cache image and resources"
    elif node_local_build_plane_image_rollout_allowed; then
      log "node-local build-plane image-cache source changed since the live tag; allowing image-cache image rollout to ${FUGUE_IMAGE_CACHE_IMAGE_TAG} while preserving live non-image settings"
      preserve_node_local_build_plane_from_live false
    elif node_local_build_plane_resource_values_changed; then
      log "node-local build-plane image-cache resources changed in values; preserving live image while applying rendered resources"
      preserve_node_local_build_plane_from_live true false
      append_node_local_build_plane_desired_resource_args || fail "failed to apply desired image-cache resources"
    elif image_cache_resource_values_drifted; then
      log "node-local build-plane image-cache resources drift from chart values; preserving live image while reconciling resources"
      preserve_node_local_build_plane_from_live true false
      append_node_local_build_plane_desired_resource_args || fail "failed to apply desired image-cache resources"
    else
      if node_local_build_plane_changed; then
        log "node-local build-plane files changed without a safe image-only rollout target; preserving live image-cache DaemonSet spec for this control-plane release"
      fi
      preserve_node_local_build_plane_from_live true
    fi
  else
    log "node-local build-plane release explicitly allowed"
  fi

  if [[ "${maintenance_mode}" != "allow" ]]; then
    preserve_maintenance_agents_from_live
  else
    log "maintenance agent release explicitly allowed"
  fi

  preserve_strict_drain_agent_image_from_live
}

public_data_plane_front_daemonsets_ready() {
  local rows=""
  local name component generation observed desired ready unavailable
  local found=0

  rows="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get ds \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.app\.kubernetes\.io/component}{"\t"}{.metadata.generation}{"\t"}{.status.observedGeneration}{"\t"}{.status.desiredNumberScheduled}{"\t"}{.status.numberReady}{"\t"}{.status.numberUnavailable}{"\n"}{end}' 2>/dev/null || true)"
  while IFS=$'\t' read -r name component generation observed desired ready unavailable; do
    name="$(trim_field "${name}")"
    component="$(trim_field "${component}")"
    [[ -n "${name}" ]] || continue
    case "${component}" in
      edge-front|edge-*-front)
        ;;
      *)
        continue
        ;;
    esac
    found=1
    generation="${generation:-0}"
    observed="${observed:-0}"
    desired="${desired:-0}"
    ready="${ready:-0}"
    unavailable="${unavailable:-0}"
    if [[ "${generation}" != "${observed}" || "${desired}" != "${ready}" || "${unavailable}" != "0" ]]; then
      log "public data-plane front ${name} is not fully ready: generation=${generation} observed=${observed} desired=${desired} ready=${ready} unavailable=${unavailable}"
      return 1
    fi
  done <<<"${rows}"

  if [[ "${found}" == "0" ]]; then
    log "public data-plane front readiness check skipped; no front DaemonSets found"
  fi
  return 0
}

public_data_plane_live_worker_image_changed() {
  local target_tag live_image live_tag daemonset_name

  target_tag="$(trim_field "${FUGUE_EDGE_IMAGE_TAG:-}")"
  [[ -n "${target_tag}" ]] || return 1
  while IFS= read -r daemonset_name; do
    daemonset_name="$(trim_field "${daemonset_name}")"
    [[ -n "${daemonset_name}" ]] || continue
    case "${daemonset_name}" in
      "${FUGUE_RELEASE_FULLNAME}-edge-worker-a"|\
      "${FUGUE_RELEASE_FULLNAME}-edge-worker-b"|\
      "${FUGUE_RELEASE_FULLNAME}-edge-country-"*"-worker-a"|\
      "${FUGUE_RELEASE_FULLNAME}-edge-country-"*"-worker-b")
        live_image="$(trim_field "$(live_daemonset_container_image "${daemonset_name}" "edge")")"
        live_tag="$(image_ref_tag "${live_image}")"
        if public_data_plane_worker_source_changed_between_refs "${live_tag}" "${target_tag}"; then
          return 0
        fi
        ;;
    esac
  done < <(${KUBECTL} -n "${FUGUE_NAMESPACE}" get ds -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)
  return 1
}

public_data_plane_live_front_image_changed() {
  local target_tag live_image live_tag daemonset_name

  target_tag="$(trim_field "${FUGUE_EDGE_IMAGE_TAG:-}")"
  [[ -n "${target_tag}" ]] || return 1
  while IFS= read -r daemonset_name; do
    daemonset_name="$(trim_field "${daemonset_name}")"
    [[ -n "${daemonset_name}" ]] || continue
    case "${daemonset_name}" in
      "${FUGUE_RELEASE_FULLNAME}-edge-front"|\
      "${FUGUE_RELEASE_FULLNAME}-edge-country-"*"-front")
        live_image="$(trim_field "$(live_daemonset_container_image "${daemonset_name}" "edge-front")")"
        live_tag="$(image_ref_tag "${live_image}")"
        if public_data_plane_front_source_changed_between_refs "${live_tag}" "${target_tag}"; then
          return 0
        fi
        ;;
    esac
  done < <(${KUBECTL} -n "${FUGUE_NAMESPACE}" get ds -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)
  return 1
}

public_data_plane_live_dns_image_changed() {
  local target_tag live_image live_tag daemonset_name

  target_tag="$(trim_field "${FUGUE_EDGE_IMAGE_TAG:-}")"
  [[ -n "${target_tag}" ]] || return 1
  while IFS= read -r daemonset_name; do
    daemonset_name="$(trim_field "${daemonset_name}")"
    [[ -n "${daemonset_name}" ]] || continue
    case "${daemonset_name}" in
      "${FUGUE_RELEASE_FULLNAME}-dns"|\
      "${FUGUE_RELEASE_FULLNAME}-dns-country-"*)
        live_image="$(trim_field "$(live_daemonset_container_image "${daemonset_name}" "dns")")"
        live_tag="$(image_ref_tag "${live_image}")"
        if public_data_plane_dns_source_changed_between_refs "${live_tag}" "${target_tag}"; then
          return 0
        fi
        ;;
    esac
  done < <(${KUBECTL} -n "${FUGUE_NAMESPACE}" get ds -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)
  return 1
}

public_data_plane_auto_front_release_enabled() {
  [[ "${FUGUE_PUBLIC_DATA_PLANE_AUTO_FRONT_RELEASE:-false}" == "true" ]]
}

release_public_data_plane_if_needed() {
  local public_mode="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_MODE:-auto}"
  local auto_release_eligible="${FUGUE_PUBLIC_DATA_PLANE_AUTO_RELEASE_ELIGIBLE:-true}"
  local worker_changed="false"
  local front_changed="false"
  local dns_changed="false"
  PUBLIC_DATA_PLANE_RELEASED="${DNS_MANIFEST_TRANSACTION_COMPLETED:-false}"

  if [[ "${FUGUE_EDGE_ENABLED}" != "true" ]]; then
    return 0
  fi
  if [[ "${public_mode}" == "preserve" ]]; then
    log "skip public data-plane auto release because FUGUE_PUBLIC_DATA_PLANE_RELEASE_MODE=preserve"
    return 0
  fi
  if [[ "${public_mode}" == "allow" ]]; then
    log "skip public data-plane auto release because release was explicitly allowed during Helm upgrade"
    return 0
  fi
  case "${auto_release_eligible}" in
    true)
      ;;
    false)
      log "skip public data-plane auto release because FUGUE_PUBLIC_DATA_PLANE_AUTO_RELEASE_ELIGIBLE=false"
      return 0
      ;;
    *)
      fail "FUGUE_PUBLIC_DATA_PLANE_AUTO_RELEASE_ELIGIBLE must be true or false"
      ;;
  esac
  if public_data_plane_worker_image_changed; then
    worker_changed="true"
  fi
  if public_data_plane_front_image_changed; then
    front_changed="true"
  fi
	if public_data_plane_dns_image_changed; then
		dns_changed="true"
	fi
	if [[ "${worker_changed}" != "true" ]] && public_data_plane_live_worker_image_changed; then
		worker_changed="true"
		log "public data-plane worker image is behind target and worker source changed since the live tag"
	fi
	if [[ "${front_changed}" != "true" ]] && public_data_plane_live_front_image_changed; then
		front_changed="true"
		log "public data-plane front image is behind target and front source changed since the live tag"
	fi
	if [[ "${dns_changed}" != "true" ]] && public_data_plane_live_dns_image_changed; then
		dns_changed="true"
		log "public data-plane DNS image is behind target and DNS source changed since the live tag"
	fi
	if [[ "${worker_changed}" != "true" && "${front_changed}" != "true" && "${dns_changed}" != "true" ]]; then
		return 0
	fi
  if public_data_plane_manifest_changed; then
    log "skip public data-plane auto release because manifest files changed; use scripts/release_fugue_public_data_plane.sh explicitly"
    return 0
  fi
  if [[ "${worker_changed}" == "true" || "${front_changed}" == "true" ]] && ! public_data_plane_front_daemonsets_ready; then
    log "skip public data-plane auto release because one or more front DaemonSets are not fully ready; repair bootstrap nodes and run scripts/release_fugue_public_data_plane.sh explicitly"
    return 0
  fi

  export FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS="${FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS:-${FUGUE_SMOKE_URLS:-${FUGUE_SMOKE_URL:-}}}"
  if [[ "${worker_changed}" == "true" ]]; then
    export FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY="blue-green"
    log "public data-plane worker image changed; starting isolated blue-green release"
    bash ./scripts/release_fugue_public_data_plane.sh
    PUBLIC_DATA_PLANE_RELEASED="true"
  fi
  if [[ "${front_changed}" == "true" ]]; then
    if public_data_plane_auto_front_release_enabled; then
      export FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY="front-ondelete"
      export FUGUE_PUBLIC_DATA_PLANE_FRONT_RESTART_CONFIRM="true"
      log "public data-plane front image changed; starting isolated front-ondelete release after worker readiness checks"
      bash ./scripts/release_fugue_public_data_plane.sh
      PUBLIC_DATA_PLANE_RELEASED="true"
    else
      log "skip public data-plane auto front release because front pods own public 80/443; run scripts/release_fugue_public_data_plane.sh with FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY=front-ondelete during an explicit maintenance window"
    fi
  fi
  if [[ "${dns_changed}" == "true" ]]; then
    export FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY="dns-ondelete"
    log "public data-plane DNS image changed; starting isolated dns-ondelete release"
    bash ./scripts/release_fugue_public_data_plane.sh
    PUBLIC_DATA_PLANE_RELEASED="true"
  fi
}

release_status_request() {
  local url="$1"
  local token="$2"
  local output_file="$3"
  local attempts="${FUGUE_RELEASE_STATUS_TRANSPORT_ATTEMPTS:-3}"
  local delay="${FUGUE_RELEASE_STATUS_TRANSPORT_RETRY_DELAY_SECONDS:-2}"
  local connect_timeout="${FUGUE_RELEASE_STATUS_CONNECT_TIMEOUT_SECONDS:-5}"
  local request_timeout="${FUGUE_RELEASE_STATUS_REQUEST_TIMEOUT_SECONDS:-30}"
  local attempt=1

  [[ "${attempts}" =~ ^[1-9][0-9]*$ ]] || attempts=3
  [[ "${delay}" =~ ^[0-9]+$ ]] || delay=2
  [[ "${connect_timeout}" =~ ^[1-9][0-9]*$ ]] || connect_timeout=5
  [[ "${request_timeout}" =~ ^[1-9][0-9]*$ ]] || request_timeout=30

  while (( attempt <= attempts )); do
    : >"${output_file}"
    if curl --http1.1 -fsS \
      --connect-timeout "${connect_timeout}" \
      --max-time "${request_timeout}" \
      -H "Authorization: Bearer ${token}" \
      "${url}" \
      -o "${output_file}"; then
      return 0
    fi
    if (( attempt >= attempts )); then
      log_stderr "release status transport request failed after ${attempts} attempt(s)"
      return 1
    fi
    log_stderr "release status transport request failed; retrying attempt $((attempt + 1))/${attempts}"
    sleep "${delay}"
    attempt=$((attempt + 1))
  done
  return 1
}

platform_autonomy_status_summary() {
  local api_base token status_file node_policies_file rc summary

  api_base="$(release_api_base_url)"
  token="$(release_api_token)"
  status_file="$(mktemp)"
  if ! release_status_request "${api_base}/v1/admin/platform/autonomy/status" "${token}" "${status_file}"; then
    rm -f "${status_file}"
    printf 'platform autonomy status request failed'
    return 1
  fi
  if node_local_dns_split_release_enabled; then
    node_policies_file="$(mktemp)"
    if ! release_status_request "${api_base}/v1/cluster/node-policies/status" "${token}" "${node_policies_file}"; then
      rm -f "${status_file}" "${node_policies_file}"
      printf 'NodeLocal offline-preserve node policy status request failed'
      return 1
    fi
    if ! summary="$(node_local_dns_offline_preserve_policy_gate "${status_file}" "${node_policies_file}" post-deploy)"; then
      rm -f "${status_file}" "${node_policies_file}"
      printf 'NodeLocal offline-preserve policy gate failed'
      return 1
    fi
    if ! node_local_dns_verify_preserved_state_unchanged; then
      rm -f "${status_file}" "${node_policies_file}"
      printf 'preserved offline NodeLocal DNSCache state changed'
      return 1
    fi
    rm -f "${status_file}" "${node_policies_file}"
    printf '%s' "${summary}"
    return 0
  fi
  python3 - "${status_file}" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as fh:
    payload = json.load(fh)
status = payload.get("status") or {}
passed = bool(status.get("pass")) and not bool(status.get("block_rollout"))
checks = status.get("checks") or []
failing = []
for check in checks:
    if not isinstance(check, dict) or check.get("pass"):
        continue
    name = str(check.get("name") or "").strip() or "<unknown>"
    message = str(check.get("message") or "").strip()
    failing.append(f"{name}: {message}" if message else name)
if passed:
    print("pass=true block_rollout=false")
    raise SystemExit(0)
summary = f"pass={str(bool(status.get('pass'))).lower()} block_rollout={str(bool(status.get('block_rollout'))).lower()}"
if failing:
    summary += "; failing=" + "; ".join(failing)
print(summary)
raise SystemExit(1)
PY
  rc=$?
  rm -f "${status_file}"
  return "${rc}"
}

write_release_guard_sample() {
  local status_file="$1"
  local dir="${FUGUE_RELEASE_ATTRIBUTION_DIR:-}"
  local evidence_dir

  [[ -n "$(trim_field "${dir}")" ]] || return 0
  evidence_dir="${dir}/release-guard-samples"
  mkdir -p "${evidence_dir}"
  python3 - "${status_file}" "${evidence_dir}" <<'PY'
import json
import os
import re
import sys
import time
import uuid

source_path, evidence_dir = sys.argv[1:3]
with open(source_path, "r", encoding="utf-8") as fh:
    payload = json.load(fh)

status = payload.get("status") or {}
baseline = status.get("robustness_baseline") or {}
autonomy = baseline.get("autonomy") or {}
store = autonomy.get("control_plane_store") or {}
hard_checks = {"discovery_bundle", "registry", "headscale", "restore_readiness"}
max_incidents = 32
max_samples = 200

def bounded_count(value):
    try:
        return max(-1_000_000_000, min(1_000_000_000, int(value or 0)))
    except (TypeError, ValueError, OverflowError):
        return 0

def bounded_identifier(value, fallback="unknown"):
    normalized = re.sub(r"[^A-Za-z0-9_.:-]+", "_", str(value or "").strip())[:96]
    return normalized or fallback

def error_class(message):
    value = str(message or "").lower()
    if "context deadline exceeded" in value or "timeout" in value or "timed out" in value:
        return "timeout"
    if "connection refused" in value:
        return "connection_refused"
    if "connection reset" in value:
        return "connection_reset"
    if "no route to host" in value:
        return "no_route_to_host"
    if "not configured" in value:
        return "not_configured"
    if "invalid" in value:
        return "invalid"
    if "missing" in value:
        return "missing"
    if "unavailable" in value:
        return "unavailable"
    return "check_failed"

blocking_checks = []
seen_checks = set()
for check in autonomy.get("checks") or []:
    if not isinstance(check, dict) or bool(check.get("pass")):
        continue
    name = str(check.get("name") or "").strip()
    if name not in hard_checks or name in seen_checks:
        continue
    seen_checks.add(name)
    blocking_checks.append({
        "name": name,
        "pass": False,
        "count": bounded_count(check.get("count")),
        "error_class": error_class(check.get("message")),
    })

block_publish_incidents = []
block_publish_incident_count = 0
for incident in baseline.get("incidents") or []:
    if not isinstance(incident, dict) or incident.get("severity") != "block_publish":
        continue
    block_publish_incident_count += 1
    if len(block_publish_incidents) >= max_incidents:
        continue
    block_publish_incidents.append({
        "check_name": bounded_identifier(incident.get("check_name")),
        "severity": "block_publish",
    })

sample = {
    "generated_at": str(status.get("generated_at") or "")[:64],
    "pass": bool(status.get("pass")),
    "block_rollout": bool(status.get("block_rollout")),
    "platform_artifact_validation_failures": bounded_count(status.get("platform_artifact_validation_failures")),
    "platform_consumer_drift": bounded_count(status.get("platform_consumer_drift")),
    "gate_policy_violation_count": min(1_000_000, len(status.get("gate_policy_violations") or [])),
    "robustness_baseline": {
        "pass": bool(baseline.get("pass")),
        "block_rollout": bool(baseline.get("block_rollout")),
        "block_publish_incident_count": block_publish_incident_count,
        "block_publish_incidents_truncated": block_publish_incident_count > len(block_publish_incidents),
        "block_publish_incidents": block_publish_incidents,
        "autonomy": {
            "pass": bool(autonomy.get("pass")),
            "block_rollout": bool(autonomy.get("block_rollout")),
            "control_plane_store_block_rollout": bool(store.get("block_rollout")),
            "blocking_checks": blocking_checks,
        },
    },
}

sample_name = f"{time.time_ns():020d}-{os.getpid()}-{uuid.uuid4().hex}.json"
evidence_path = os.path.join(evidence_dir, sample_name)
temporary_path = evidence_path + ".tmp"
try:
    with open(temporary_path, "w", encoding="utf-8") as fh:
        json.dump(sample, fh, indent=2, sort_keys=True)
        fh.write("\n")
    os.replace(temporary_path, evidence_path)
finally:
    try:
        os.remove(temporary_path)
    except FileNotFoundError:
        pass

sample_files = sorted(
    (name for name in os.listdir(evidence_dir) if name.endswith(".json")),
    reverse=True,
)
for stale_name in sample_files[max_samples:]:
    try:
        os.remove(os.path.join(evidence_dir, stale_name))
    except FileNotFoundError:
        pass
PY
}

release_guard_status_summary() {
  local api_base token status_file evidence_status rc

  api_base="$(release_api_base_url)"
  token="$(release_api_token)"
  status_file="$(mktemp)"
  if ! release_status_request "${api_base}/v1/admin/release-guard/status" "${token}" "${status_file}"; then
    rm -f "${status_file}"
    printf 'release guard status request failed'
    return 1
  fi
  if [[ -z "$(trim_field "${FUGUE_RELEASE_ATTRIBUTION_DIR:-}")" ]]; then
    evidence_status="disabled"
  elif write_release_guard_sample "${status_file}"; then
    evidence_status="recorded"
  else
    evidence_status="failed"
  fi
  python3 - "${status_file}" "${evidence_status}" <<'PY'
import json
import sys

path, evidence_status = sys.argv[1:3]
with open(path, "r", encoding="utf-8") as fh:
    payload = json.load(fh)
status = payload.get("status") or {}
baseline = status.get("robustness_baseline") or {}
autonomy = baseline.get("autonomy") or {}
hard_checks = {"discovery_bundle", "registry", "headscale", "restore_readiness"}

def error_class(message):
    value = str(message or "").lower()
    if "context deadline exceeded" in value or "timeout" in value or "timed out" in value:
        return "timeout"
    if "connection refused" in value:
        return "connection_refused"
    if "connection reset" in value:
        return "connection_reset"
    if "no route to host" in value:
        return "no_route_to_host"
    if "not configured" in value:
        return "not_configured"
    if "invalid" in value:
        return "invalid"
    if "missing" in value:
        return "missing"
    if "unavailable" in value:
        return "unavailable"
    return "check_failed"

autonomy_failures = []
seen = set()
for check in autonomy.get("checks") or []:
    if not isinstance(check, dict) or bool(check.get("pass")):
        continue
    name = str(check.get("name") or "").strip()
    if name not in hard_checks or name in seen:
        continue
    seen.add(name)
    autonomy_failures.append(f"{name}:{error_class(check.get('message'))}")

safe_blockers = []
if int(status.get("platform_artifact_validation_failures") or 0) > 0:
    safe_blockers.append("platform_artifact_validation")
if int(status.get("platform_consumer_drift") or 0) > 0:
    safe_blockers.append("platform_consumer_drift")
if status.get("gate_policy_violations") or []:
    safe_blockers.append("gate_policy_validation")
for incident in baseline.get("incidents") or []:
    if not isinstance(incident, dict) or incident.get("severity") != "block_publish":
        continue
    name = str(incident.get("check_name") or "").strip()
    name = "".join(char for char in name if char.isalnum() or char in "_.:-")[:64]
    safe_blockers.append("block_publish:" + (name or "unknown"))
if bool(autonomy.get("block_rollout")):
    store = autonomy.get("control_plane_store") or {}
    if bool(store.get("block_rollout")):
        safe_blockers.append("control_plane_store")
    safe_blockers.extend("autonomy:" + item.split(":", 1)[0] for item in autonomy_failures)
    if not bool(store.get("block_rollout")) and not autonomy_failures:
        safe_blockers.append("autonomy:unclassified")
safe_blockers = list(dict.fromkeys(safe_blockers))
if bool(status.get("block_rollout")) and not safe_blockers:
    safe_blockers.append("unclassified")
safe_blocker_count = len(safe_blockers)
safe_blockers = safe_blockers[:32]
safe_blockers_truncated = safe_blocker_count > len(safe_blockers)
print(
    "pass={pass_} block_rollout={block} artifact_failures={artifact_failures} "
    "consumer_drift={consumer_drift} failure_contracts={contracts} gate_policies={gates} "
    "enforced_gates={enforced_gates} gate_violations={gate_violations} "
    "baseline_block_rollout={baseline_block} autonomy_block_rollout={autonomy_block} "
    "autonomy_failures={autonomy_failures} blocker_count={blocker_count} "
    "blockers_truncated={blockers_truncated} blockers={blockers} evidence={evidence}".format(
        pass_=str(bool(status.get("pass"))).lower(),
        block=str(bool(status.get("block_rollout"))).lower(),
        artifact_failures=int(status.get("platform_artifact_validation_failures") or 0),
        consumer_drift=int(status.get("platform_consumer_drift") or 0),
        contracts=int(status.get("failure_contract_count") or 0),
        gates=int(status.get("gate_policy_count") or 0),
        enforced_gates=int(status.get("enforced_gate_count") or 0),
        gate_violations=len(status.get("gate_policy_violations") or []),
        baseline_block=str(bool(baseline.get("block_rollout"))).lower(),
        autonomy_block=str(bool(autonomy.get("block_rollout"))).lower(),
        autonomy_failures=",".join(autonomy_failures) or "none",
        blocker_count=safe_blocker_count,
        blockers_truncated=str(safe_blockers_truncated).lower(),
        blockers=",".join(safe_blockers) or "none",
        evidence=evidence_status,
    )
)
raise SystemExit(0 if bool(status.get("pass")) and not bool(status.get("block_rollout")) else 1)
PY
  rc=$?
  rm -f "${status_file}"
  return "${rc}"
}

capture_pre_deploy_robustness_baseline() {
  local api_base token status_file summary

  case "${FUGUE_ROBUSTNESS_HEALTH_GATE_ENABLED:-true}" in
    1|true|TRUE|yes|YES)
      ;;
    *)
      ROBUSTNESS_HEALTH_GATE_BASELINE_FILE=""
      return 0
      ;;
  esac
  command_exists python3 || fail "python3 is required for post-deploy robustness health gate"

  api_base="$(release_api_base_url)"
  token="$(release_api_token)"
  status_file="$(mktemp)"
  if ! release_status_request "${api_base}/v1/admin/robustness/status" "${token}" "${status_file}"; then
    rm -f "${status_file}"
    ROBUSTNESS_HEALTH_GATE_BASELINE_FILE=""
    log "warning: failed to capture pre-deploy robustness baseline; post-deploy robustness gate will use strict mode"
    return 0
  fi
  if ! summary="$(python3 - "${status_file}" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as fh:
    payload = json.load(fh)
status = payload.get("status")
if not isinstance(status, dict):
    raise SystemExit("missing status object")
checks = status.get("checks") or []
incidents = status.get("incidents") or []
print(
    f"pass={str(bool(status.get('pass'))).lower()} "
    f"block_rollout={str(bool(status.get('block_rollout'))).lower()} "
    f"checks={len(checks)} incidents={len(incidents)}"
)
PY
  )"; then
    rm -f "${status_file}"
    ROBUSTNESS_HEALTH_GATE_BASELINE_FILE=""
    log "warning: pre-deploy robustness baseline response was invalid; post-deploy robustness gate will use strict mode"
    return 0
  fi
  ROBUSTNESS_HEALTH_GATE_BASELINE_FILE="${status_file}"
  log "captured pre-deploy robustness baseline: ${summary}"
  if summary="$(release_guard_status_summary 2>/dev/null)"; then
    log "pre-deploy release guard summary: ${summary}"
  else
    log "warning: pre-deploy release guard summary unavailable or blocked: ${summary:-unknown}"
  fi
}

robustness_status_summary() {
  local api_base token status_file baseline_file rc

  baseline_file="${1:-}"
  api_base="$(release_api_base_url)"
  token="$(release_api_token)"
  status_file="$(mktemp)"
  if ! release_status_request "${api_base}/v1/admin/robustness/status" "${token}" "${status_file}"; then
    rm -f "${status_file}"
    printf 'robustness status request failed'
    return 1
  fi
  python3 - "${status_file}" "${baseline_file}" <<'PY'
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone

path = sys.argv[1]
baseline_path = sys.argv[2] if len(sys.argv) > 2 else ""
with open(path, "r", encoding="utf-8") as fh:
    payload = json.load(fh)
status = payload.get("status")
if not isinstance(status, dict):
    print("robustness status response is missing status object")
    raise SystemExit(1)

def stable_text(value):
    return str(value or "").strip()

def check_key(check):
    return "\x00".join(
        [
            stable_text(check.get("name")),
            stable_text(check.get("subject")),
            stable_text(check.get("severity")),
        ]
    )

def check_identity(check):
    return "\x00".join(
        [
            stable_text(check.get("name")),
            stable_text(check.get("subject")),
        ]
    )

def incident_key(incident):
    return "\x00".join(
        [
            stable_text(incident.get("check_name") or incident.get("name")),
            stable_text(incident.get("subject")),
            stable_text(incident.get("severity")),
        ]
    )

def incident_id(incident):
    return stable_text(incident.get("id") or incident.get("incident_id"))

def incident_label(incident):
    name = stable_text(incident.get("check_name") or incident.get("name")) or "<unknown>"
    subject = stable_text(incident.get("subject"))
    severity = stable_text(incident.get("severity")) or "<unknown>"
    message = stable_text(incident.get("message") or incident.get("observed"))
    label = f"{severity}:{name}({subject})" if subject else f"{severity}:{name}"
    return f"{label}: {message}" if message else label

def evidence_map(item):
    evidence = item.get("evidence") if isinstance(item, dict) else {}
    return evidence if isinstance(evidence, dict) else {}

def explicit_control_plane_release_signal(item):
    evidence = evidence_map(item)
    if stable_text(evidence.get("release_gate_scope")) != "control_plane":
        return False
    return stable_text(evidence.get("release_signal_id")) != ""

def release_gate_ignored_tenant_workload(item):
    if explicit_control_plane_release_signal(item):
        return False
    name = stable_text(item.get("check_name") or item.get("name"))
    if name == "app_continuity_invariant":
        return True
    evidence = evidence_map(item)
    if stable_text(evidence.get("release_gate_scope")) == "tenant_workload":
        return True
    return stable_text(evidence.get("report_only")).lower() == "true"

def incident_blocks_release(incident):
    if release_gate_ignored_tenant_workload(incident):
        return False
    return stable_text(incident.get("severity")) == "block_publish"

def severity_rank(value):
    return {
        "": 0,
        "info": 1,
        "warning": 2,
        "degraded": 3,
        "block_publish": 4,
    }.get(stable_text(value).lower(), 5)

def parse_generated_at(value):
    text = stable_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed

def stable_evidence_fingerprint(item):
    volatile_tokens = (
        "time",
        "timestamp",
        "generated_at",
        "observed_at",
        "updated_at",
        "created_at",
        "first_observed",
        "last_observed",
        "nonce",
        "request_id",
        "trace_id",
    )
    evidence = {
        stable_text(key): stable_text(value)
        for key, value in evidence_map(item).items()
        if not any(token in stable_text(key).lower() for token in volatile_tokens)
    }
    payload = {
        "expected": stable_text(item.get("expected")),
        "evidence": evidence,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()

number_pattern = re.compile(r"(?<![A-Za-z0-9_.-])([A-Za-z_][A-Za-z0-9_.-]*)=(-?[0-9]+(?:\.[0-9]+)?)")

def numeric_measurements(item):
    values = {}
    for key, value in evidence_map(item).items():
        text = stable_text(value)
        try:
            values[stable_text(key).lower()] = float(text)
        except ValueError:
            pass
    for field in ("observed", "message"):
        for key, value in number_pattern.findall(stable_text(item.get(field))):
            values[key.lower()] = float(value)
    return values

def metric_worse_direction(key):
    normalized = key.lower().replace("-", "_").replace(".", "_")
    bad_tokens = (
        "affected",
        "blocked",
        "corrupt",
        "debt",
        "drift",
        "error",
        "expired",
        "fail",
        "invalid",
        "lag",
        "missing",
        "mismatch",
        "orphan",
        "pending",
        "pressure",
        "quarantin",
        "reject",
        "retry",
        "stale",
        "suspect",
        "timeout",
        "unavailable",
        "unhealthy",
        "violation",
    )
    good_tokens = (
        "available",
        "connected",
        "current",
        "healthy",
        "ready",
        "success",
        "valid",
    )
    if any(token in normalized for token in bad_tokens):
        return "higher"
    if any(token in normalized for token in good_tokens):
        return "lower"
    return ""

def scope_values(item):
    values = {}
    for key, value in evidence_map(item).items():
        normalized = stable_text(key).lower().replace("-", "_").replace(".", "_")
        if not (
            normalized.startswith("affected_")
            or normalized.endswith("_nodes")
            or normalized.endswith("_edges")
            or normalized.endswith("_consumers")
            or normalized.endswith("_failure_domains")
            or normalized in {"affected_scope", "failure_domains"}
        ):
            continue
        members = {
            member.strip()
            for member in re.split(r"[,;\s]+", stable_text(value))
            if member.strip()
        }
        if members:
            values[normalized] = members
    return values

def blocker_regressions(previous, current):
    regressions = []
    previous_severity = severity_rank(previous.get("severity"))
    current_severity = severity_rank(current.get("severity"))
    if current_severity > previous_severity:
        regressions.append(
            f"severity {stable_text(previous.get('severity')) or '<empty>'}"
            f"->{stable_text(current.get('severity')) or '<empty>'}"
        )

    previous_metrics = numeric_measurements(previous)
    current_metrics = numeric_measurements(current)
    for key in sorted(set(previous_metrics).intersection(current_metrics)):
        before = previous_metrics[key]
        after = current_metrics[key]
        direction = metric_worse_direction(key)
        if (direction == "higher" and after > before) or (direction == "lower" and after < before):
            regressions.append(f"{key} {before:g}->{after:g}")

    previous_scopes = scope_values(previous)
    current_scopes = scope_values(current)
    for key in sorted(set(previous_scopes).intersection(current_scopes)):
        added = current_scopes[key] - previous_scopes[key]
        if added:
            regressions.append(f"{key} expanded by {len(added)}")

    comparison_mode = stable_text(evidence_map(current).get("baseline_comparison")).lower()
    if comparison_mode == "exact" and stable_evidence_fingerprint(previous) != stable_evidence_fingerprint(current):
        regressions.append("evidence fingerprint changed")
    return regressions

baseline_status = None
baseline_check_names = set()
baseline_incident_ids = set()
baseline_incident_keys = set()
baseline_blocker_keys = set()
baseline_checks_by_identity = {}
baseline_expired = False
if baseline_path and os.path.exists(baseline_path) and os.path.getsize(baseline_path) > 0:
    with open(baseline_path, "r", encoding="utf-8") as fh:
        baseline_payload = json.load(fh)
    candidate = baseline_payload.get("status")
    if isinstance(candidate, dict):
        baseline_status = candidate
        for incident in candidate.get("incidents") or []:
            if not isinstance(incident, dict):
                continue
            ident = incident_id(incident)
            if ident:
                baseline_incident_ids.add(ident)
            key = incident_key(incident)
            if key:
                baseline_incident_keys.add(key)
        for check in candidate.get("checks") or []:
            if not isinstance(check, dict):
                continue
            name = stable_text(check.get("name"))
            if name:
                baseline_check_names.add(name)
            identity = check_identity(check)
            if identity:
                baseline_checks_by_identity[identity] = check
            if (
                not check.get("pass")
                and stable_text(check.get("severity")) == "block_publish"
                and not release_gate_ignored_tenant_workload(check)
            ):
                key = check_key(check)
                if key:
                    baseline_blocker_keys.add(key)
        baseline_generated_at = parse_generated_at(candidate.get("generated_at"))
        current_generated_at = parse_generated_at(status.get("generated_at"))
        try:
            baseline_max_age_seconds = max(1, int(os.environ.get("FUGUE_ROBUSTNESS_BASELINE_MAX_AGE_SECONDS", "3600")))
        except ValueError:
            baseline_max_age_seconds = 3600
        if baseline_generated_at and current_generated_at:
            baseline_expired = (current_generated_at - baseline_generated_at).total_seconds() > baseline_max_age_seconds

checks = status.get("checks") or []
incidents = status.get("incidents") or []
raw_block_rollout = bool(status.get("block_rollout"))
blockers = []
new_blockers = []
introduced_blockers = []
regressed_blockers = []
ignored_tenant_workload_blockers = []
for check in checks:
    if not isinstance(check, dict) or check.get("pass"):
        continue
    severity = str(check.get("severity") or "").strip()
    if severity != "block_publish":
        continue
    raw_name = str(check.get("name") or "").strip()
    name = raw_name or "<unknown>"
    subject = str(check.get("subject") or "").strip()
    message = str(check.get("message") or check.get("observed") or "").strip()
    label = f"{name}({subject})" if subject else name
    description = f"{label}: {message}" if message else label
    if release_gate_ignored_tenant_workload(check):
        ignored_tenant_workload_blockers.append(description)
        continue
    blockers.append(description)
    key = check_key(check)
    if not baseline_status:
        new_blockers.append(description)
    elif baseline_expired:
        new_blockers.append(f"{description} [baseline expired]")
    else:
        previous = baseline_checks_by_identity.get(check_identity(check))
        regressions = blocker_regressions(previous, check) if isinstance(previous, dict) else []
        if regressions:
            regressed_blockers.append(f"{description} [{', '.join(regressions)}]")
        elif key not in baseline_blocker_keys:
            if raw_name and raw_name not in baseline_check_names:
                introduced_blockers.append(description)
            else:
                new_blockers.append(description)

new_incidents = []
new_blocking_incidents = []
introduced_incidents = []
introduced_blocking_incidents = []
ignored_tenant_workload_incidents = []
if baseline_status is not None:
    for incident in incidents:
        if not isinstance(incident, dict):
            continue
        ident = incident_id(incident)
        key = incident_key(incident)
        if (ident and ident in baseline_incident_ids) or (key and key in baseline_incident_keys):
            continue
        name = stable_text(incident.get("check_name") or incident.get("name"))
        label = incident_label(incident)
        if release_gate_ignored_tenant_workload(incident):
            ignored_tenant_workload_incidents.append(label)
            continue
        blocks_release = incident_blocks_release(incident)
        if name and name not in baseline_check_names:
            introduced_incidents.append(label)
            if blocks_release:
                introduced_blocking_incidents.append(label)
        else:
            new_incidents.append(label)
            if blocks_release:
                new_blocking_incidents.append(label)

if baseline_status is not None:
    block_rollout = bool(new_blockers or regressed_blockers or new_blocking_incidents)
else:
    block_rollout = raw_block_rollout and (bool(blockers) or not ignored_tenant_workload_blockers)
summary = (
    f"pass={str(bool(status.get('pass'))).lower()} "
    f"block_rollout={str(block_rollout).lower()} "
    f"checks={len(checks)} incidents={len(incidents)}"
)
if baseline_status is not None:
    summary += (
        f" baseline_incidents={len(baseline_status.get('incidents') or [])}"
        f" new_incidents={len(new_incidents)}"
    )
    if baseline_expired:
        summary += " baseline_expired=true"
if baseline_status is not None and raw_block_rollout and not block_rollout and not ignored_tenant_workload_blockers:
    summary += "; raw_block_rollout=true tolerated_by_baseline=true"
if raw_block_rollout and not block_rollout and (ignored_tenant_workload_blockers or ignored_tenant_workload_incidents):
    summary += "; raw_block_rollout=true ignored_by_release_scope=true"
if ignored_tenant_workload_blockers:
    summary += f"; ignored_tenant_workload_blockers={len(ignored_tenant_workload_blockers)}"
if ignored_tenant_workload_incidents:
    summary += f"; ignored_tenant_workload_incidents={len(ignored_tenant_workload_incidents)}"
if new_blockers:
    summary += "; new_blockers=" + "; ".join(new_blockers)
elif regressed_blockers:
    summary += "; regressed_blockers=" + "; ".join(regressed_blockers)
elif new_blocking_incidents:
    summary += "; new_blocking_incidents=" + "; ".join(new_blocking_incidents[:5])
    if len(new_blocking_incidents) > 5:
        summary += f"; +{len(new_blocking_incidents) - 5} more"
elif introduced_blockers:
    summary += "; introduced_blockers=" + "; ".join(introduced_blockers)
elif introduced_blocking_incidents:
    summary += "; introduced_blocking_incidents=" + "; ".join(introduced_blocking_incidents[:5])
    if len(introduced_blocking_incidents) > 5:
        summary += f"; +{len(introduced_blocking_incidents) - 5} more"
elif blockers:
    summary += "; blockers=" + "; ".join(blockers)
if new_incidents:
    summary += "; new_incidents_detail=" + "; ".join(new_incidents[:5])
    if len(new_incidents) > 5:
        summary += f"; +{len(new_incidents) - 5} more"
elif introduced_incidents:
    summary += "; introduced_incidents=" + "; ".join(introduced_incidents[:5])
    if len(introduced_incidents) > 5:
        summary += f"; +{len(introduced_incidents) - 5} more"
print(summary)

if baseline_status is not None:
    if new_blockers or regressed_blockers or new_blocking_incidents:
        raise SystemExit(1)
else:
    if block_rollout or blockers:
        raise SystemExit(1)
PY
  rc=$?
  rm -f "${status_file}"
  return "${rc}"
}

wait_for_platform_autonomy_after_public_data_plane_release() {
  local timeout="${FUGUE_PUBLIC_DATA_PLANE_AUTONOMY_WAIT_SECONDS:-180}"
  local delay="${FUGUE_PUBLIC_DATA_PLANE_AUTONOMY_WAIT_DELAY_SECONDS:-10}"
  local deadline output

  deadline=$((SECONDS + timeout))
  while true; do
    if output="$(platform_autonomy_status_summary)"; then
      log "platform autonomy passed after public data-plane release: ${output}"
      return 0
    fi
    if (( SECONDS >= deadline )); then
      fail "platform autonomy did not recover after public data-plane release: ${output}"
    fi
    log "waiting for platform autonomy after public data-plane release: ${output}"
    sleep "${delay}"
  done
}

wait_for_post_deploy_robustness() {
  local timeout="${FUGUE_ROBUSTNESS_HEALTH_GATE_TIMEOUT_SECONDS:-180}"
  local delay="${FUGUE_ROBUSTNESS_HEALTH_GATE_DELAY_SECONDS:-10}"
  local deadline output

  case "${FUGUE_ROBUSTNESS_HEALTH_GATE_ENABLED:-true}" in
    1|true|TRUE|yes|YES)
      ;;
    *)
      log "post-deploy robustness gate disabled"
      return 0
      ;;
  esac
  command_exists python3 || fail "python3 is required for post-deploy robustness health gate"

  deadline=$((SECONDS + timeout))
  while true; do
    if output="$(robustness_status_summary "${ROBUSTNESS_HEALTH_GATE_BASELINE_FILE:-}")"; then
      log "post-deploy robustness gate passed: ${output}"
      if release_output="$(release_guard_status_summary 2>/dev/null)"; then
        log "post-deploy release guard summary: ${release_output}"
        return 0
      fi
      output="release guard blocked after robustness passed: ${release_output:-unknown}"
    fi
    if (( SECONDS >= deadline )); then
      log "post-deploy robustness gate did not pass: ${output}"
      return 1
    fi
    log "waiting for post-deploy robustness gate: ${output}"
    sleep "${delay}"
  done
}

main() {
  require_env FUGUE_API_IMAGE_REPOSITORY
  require_env FUGUE_API_IMAGE_TAG
  require_env FUGUE_CONTROLLER_IMAGE_REPOSITORY
  require_env FUGUE_CONTROLLER_IMAGE_TAG
  FUGUE_OBSERVABILITY_ENABLED="${FUGUE_OBSERVABILITY_ENABLED:-false}"
  FUGUE_OBSERVABILITY_RETENTION="${FUGUE_OBSERVABILITY_RETENTION:-24h}"
  FUGUE_OBSERVABILITY_EXPORTER_SECRET_NAME="${FUGUE_OBSERVABILITY_EXPORTER_SECRET_NAME:-}"
  FUGUE_OBSERVABILITY_RUNTIME_LOG_PATHS="${FUGUE_OBSERVABILITY_RUNTIME_LOG_PATHS:-}"
  FUGUE_OBSERVABILITY_PROMETHEUS_SCRAPE_URLS="${FUGUE_OBSERVABILITY_PROMETHEUS_SCRAPE_URLS:-}"
  FUGUE_OBSERVABILITY_SCRAPE_INTERVAL="${FUGUE_OBSERVABILITY_SCRAPE_INTERVAL:-30s}"
  FUGUE_OBSERVABILITY_KUBERNETES_LOGS_ENABLED="${FUGUE_OBSERVABILITY_KUBERNETES_LOGS_ENABLED:-true}"
  FUGUE_OBSERVABILITY_KUBERNETES_LOG_NAMESPACES="${FUGUE_OBSERVABILITY_KUBERNETES_LOG_NAMESPACES:-}"
  FUGUE_OBSERVABILITY_KUBERNETES_LOG_NAMESPACE_PREFIXES="${FUGUE_OBSERVABILITY_KUBERNETES_LOG_NAMESPACE_PREFIXES:-fg-}"
  FUGUE_OBSERVABILITY_KUBERNETES_LOG_LABEL_SELECTOR="${FUGUE_OBSERVABILITY_KUBERNETES_LOG_LABEL_SELECTOR:-}"
  FUGUE_OBSERVABILITY_KUBERNETES_LOG_POLL_INTERVAL="${FUGUE_OBSERVABILITY_KUBERNETES_LOG_POLL_INTERVAL:-15s}"
  FUGUE_OBSERVABILITY_KUBERNETES_LOG_TAIL_LINES="${FUGUE_OBSERVABILITY_KUBERNETES_LOG_TAIL_LINES:-2000}"
  FUGUE_OBSERVABILITY_KUBERNETES_LOG_MAX_PODS="${FUGUE_OBSERVABILITY_KUBERNETES_LOG_MAX_PODS:-500}"
  FUGUE_OBSERVABILITY_KUBERNETES_LOG_MAX_LINES_PER_CYCLE="${FUGUE_OBSERVABILITY_KUBERNETES_LOG_MAX_LINES_PER_CYCLE:-20000}"
  FUGUE_OBSERVABILITY_QUEUE_SIZE="${FUGUE_OBSERVABILITY_QUEUE_SIZE:-32768}"
  FUGUE_OBSERVABILITY_BATCH_SIZE="${FUGUE_OBSERVABILITY_BATCH_SIZE:-512}"
  FUGUE_OBSERVABILITY_MAX_PAYLOAD_BYTES="${FUGUE_OBSERVABILITY_MAX_PAYLOAD_BYTES:-1048576}"
  FUGUE_OBSERVABILITY_MEMORY_LIMIT_BYTES="${FUGUE_OBSERVABILITY_MEMORY_LIMIT_BYTES:-134217728}"
  FUGUE_OBSERVABILITY_RETRY_MAX_ATTEMPTS="${FUGUE_OBSERVABILITY_RETRY_MAX_ATTEMPTS:-3}"
  FUGUE_OBSERVABILITY_TENANT_ID="${FUGUE_OBSERVABILITY_TENANT_ID:-}"
  FUGUE_OBSERVABILITY_PROJECT_ID="${FUGUE_OBSERVABILITY_PROJECT_ID:-}"
  FUGUE_OBSERVABILITY_APP_ID="${FUGUE_OBSERVABILITY_APP_ID:-}"
  FUGUE_OBSERVABILITY_RUNTIME_ID="${FUGUE_OBSERVABILITY_RUNTIME_ID:-}"
  FUGUE_OBSERVABILITY_COMPONENT="${FUGUE_OBSERVABILITY_COMPONENT:-telemetry-agent}"
  FUGUE_OBSERVABILITY_METRICS_ENABLED="${FUGUE_OBSERVABILITY_METRICS_ENABLED:-false}"
  FUGUE_OBSERVABILITY_METRICS_IMAGE_REPOSITORY="${FUGUE_OBSERVABILITY_METRICS_IMAGE_REPOSITORY:-prom/prometheus}"
  FUGUE_OBSERVABILITY_METRICS_IMAGE_TAG="${FUGUE_OBSERVABILITY_METRICS_IMAGE_TAG:-latest}"
  FUGUE_OBSERVABILITY_METRICS_RETENTION="${FUGUE_OBSERVABILITY_METRICS_RETENTION:-24h}"
  FUGUE_OBSERVABILITY_METRICS_SCRAPE_INTERVAL="${FUGUE_OBSERVABILITY_METRICS_SCRAPE_INTERVAL:-30s}"
  FUGUE_OBSERVABILITY_METRICS_EVALUATION_INTERVAL="${FUGUE_OBSERVABILITY_METRICS_EVALUATION_INTERVAL:-30s}"
  FUGUE_OBSERVABILITY_ALERTS_ENABLED="${FUGUE_OBSERVABILITY_ALERTS_ENABLED:-false}"
  FUGUE_OBSERVABILITY_ALERTS_IMAGE_REPOSITORY="${FUGUE_OBSERVABILITY_ALERTS_IMAGE_REPOSITORY:-prom/alertmanager}"
  FUGUE_OBSERVABILITY_ALERTS_IMAGE_TAG="${FUGUE_OBSERVABILITY_ALERTS_IMAGE_TAG:-latest}"
  FUGUE_OBSERVABILITY_ALERTS_WEBHOOK_URL="${FUGUE_OBSERVABILITY_ALERTS_WEBHOOK_URL:-}"
  FUGUE_OBSERVABILITY_LOGS_ENABLED="${FUGUE_OBSERVABILITY_LOGS_ENABLED:-false}"
  FUGUE_OBSERVABILITY_LOGS_IMAGE_REPOSITORY="${FUGUE_OBSERVABILITY_LOGS_IMAGE_REPOSITORY:-grafana/loki}"
  FUGUE_OBSERVABILITY_LOGS_IMAGE_TAG="${FUGUE_OBSERVABILITY_LOGS_IMAGE_TAG:-latest}"
  FUGUE_OBSERVABILITY_LOGS_RETENTION="${FUGUE_OBSERVABILITY_LOGS_RETENTION:-24h}"
  FUGUE_OBSERVABILITY_ANALYTICS_ENABLED="${FUGUE_OBSERVABILITY_ANALYTICS_ENABLED:-false}"
  FUGUE_OBSERVABILITY_ANALYTICS_IMAGE_REPOSITORY="${FUGUE_OBSERVABILITY_ANALYTICS_IMAGE_REPOSITORY:-clickhouse/clickhouse-server}"
  FUGUE_OBSERVABILITY_ANALYTICS_IMAGE_TAG="${FUGUE_OBSERVABILITY_ANALYTICS_IMAGE_TAG:-latest}"
  FUGUE_OBSERVABILITY_ANALYTICS_RETENTION="${FUGUE_OBSERVABILITY_ANALYTICS_RETENTION:-24h}"
  FUGUE_TELEMETRY_AGENT_ENABLED="${FUGUE_TELEMETRY_AGENT_ENABLED:-false}"
  if [[ "${FUGUE_TELEMETRY_AGENT_ENABLED}" == "true" ]]; then
    require_env FUGUE_TELEMETRY_AGENT_IMAGE_REPOSITORY
    require_env FUGUE_TELEMETRY_AGENT_IMAGE_TAG
  else
    FUGUE_TELEMETRY_AGENT_IMAGE_REPOSITORY="${FUGUE_TELEMETRY_AGENT_IMAGE_REPOSITORY:-fugue-telemetry-agent}"
    FUGUE_TELEMETRY_AGENT_IMAGE_TAG="${FUGUE_TELEMETRY_AGENT_IMAGE_TAG:-latest}"
  fi
  FUGUE_STRICT_DRAIN_MODE="${FUGUE_STRICT_DRAIN_MODE:-connection-aware}"
  FUGUE_STRICT_DRAIN_TIMEOUT_SECONDS="${FUGUE_STRICT_DRAIN_TIMEOUT_SECONDS:-600}"
  FUGUE_STRICT_DRAIN_TERMINATION_GRACE_BUFFER_SECONDS="${FUGUE_STRICT_DRAIN_TERMINATION_GRACE_BUFFER_SECONDS:-30}"
  FUGUE_STRICT_DRAIN_MIN_READY_SECONDS="${FUGUE_STRICT_DRAIN_MIN_READY_SECONDS:-10}"
  FUGUE_STRICT_DRAIN_QUIET_PERIOD_SECONDS="${FUGUE_STRICT_DRAIN_QUIET_PERIOD_SECONDS:-2}"
  FUGUE_STRICT_DRAIN_POLL_INTERVAL_MS="${FUGUE_STRICT_DRAIN_POLL_INTERVAL_MS:-200}"
  FUGUE_STRICT_DRAIN_NATIVE_SIDECAR_ENABLED="${FUGUE_STRICT_DRAIN_NATIVE_SIDECAR_ENABLED:-true}"
  FUGUE_DRAIN_AGENT_PORT="${FUGUE_DRAIN_AGENT_PORT:-19090}"
  if [[ "${FUGUE_STRICT_DRAIN_MODE}" == "connection-aware" ]]; then
    require_env FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY
    require_env FUGUE_DRAIN_AGENT_IMAGE_TAG
  else
    FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY="${FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY:-ghcr.io/yym68686/fugue-drain-agent}"
    FUGUE_DRAIN_AGENT_IMAGE_TAG="${FUGUE_DRAIN_AGENT_IMAGE_TAG:-latest}"
  fi
  FUGUE_DRAIN_AGENT_IMAGE_DIGEST="${FUGUE_DRAIN_AGENT_IMAGE_DIGEST:-}"
  FUGUE_DRAIN_AGENT_IMAGE_PULL_POLICY="${FUGUE_DRAIN_AGENT_IMAGE_PULL_POLICY:-IfNotPresent}"
  FUGUE_IMAGE_CACHE_ENABLED="${FUGUE_IMAGE_CACHE_ENABLED:-true}"
  FUGUE_IMAGE_CACHE_PORT="${FUGUE_IMAGE_CACHE_PORT:-5000}"
  FUGUE_IMAGE_STORE_MODE="${FUGUE_IMAGE_STORE_MODE:-distributed}"
  FUGUE_IMAGE_STORE_MIN_REPLICAS="${FUGUE_IMAGE_STORE_MIN_REPLICAS:-1}"
  FUGUE_IMAGE_STORE_TARGET_REPLICAS="${FUGUE_IMAGE_STORE_TARGET_REPLICAS:-1}"
  FUGUE_IMAGE_STORE_SCHEDULER_INTERVAL="${FUGUE_IMAGE_STORE_SCHEDULER_INTERVAL:-30s}"
  FUGUE_IMAGE_STORE_REPLICA_LEASE_TTL="${FUGUE_IMAGE_STORE_REPLICA_LEASE_TTL:-30m}"
  FUGUE_IMAGE_STORE_VERIFY_INTERVAL="${FUGUE_IMAGE_STORE_VERIFY_INTERVAL:-10m}"
  FUGUE_IMAGE_STORE_PRUNE_ENABLED="${FUGUE_IMAGE_STORE_PRUNE_ENABLED:-true}"
  FUGUE_IMAGE_STORE_PRUNE_MAX_DELETE_BYTES_PER_RUN="${FUGUE_IMAGE_STORE_PRUNE_MAX_DELETE_BYTES_PER_RUN:-10Gi}"
  FUGUE_IMAGE_CACHE_INVENTORY_ENABLED="${FUGUE_IMAGE_CACHE_INVENTORY_ENABLED:-true}"
  FUGUE_IMAGE_CACHE_INVENTORY_INTERVAL="${FUGUE_IMAGE_CACHE_INVENTORY_INTERVAL:-30m}"
  FUGUE_IMAGE_CACHE_INVENTORY_TTL="${FUGUE_IMAGE_CACHE_INVENTORY_TTL:-2h}"
  FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MODE="${FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MODE:-delete}"
  FUGUE_IMAGE_STORE_ORPHAN_PRUNE_GRACE_PERIOD="${FUGUE_IMAGE_STORE_ORPHAN_PRUNE_GRACE_PERIOD:-24h}"
  FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MAX_TARGETS_PER_NODE="${FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MAX_TARGETS_PER_NODE:-50}"
  FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MAX_DELETE_BYTES_PER_NODE="${FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MAX_DELETE_BYTES_PER_NODE:-10Gi}"
  FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MIN_REPLICA_COUNT="${FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MIN_REPLICA_COUNT:-1}"
  FUGUE_REGISTRY_ENABLED="${FUGUE_REGISTRY_ENABLED:-}"
  if [[ -z "$(trim_field "${FUGUE_REGISTRY_ENABLED}")" ]]; then
    if [[ "${FUGUE_IMAGE_STORE_MODE}" == "distributed" ]]; then
      FUGUE_REGISTRY_ENABLED="false"
    else
      FUGUE_REGISTRY_ENABLED="true"
    fi
  fi
  if [[ "${FUGUE_IMAGE_CACHE_ENABLED}" == "true" ]]; then
    require_env FUGUE_IMAGE_CACHE_IMAGE_REPOSITORY
    require_env FUGUE_IMAGE_CACHE_IMAGE_TAG
  else
    FUGUE_IMAGE_CACHE_IMAGE_REPOSITORY="${FUGUE_IMAGE_CACHE_IMAGE_REPOSITORY:-fugue-image-cache}"
    FUGUE_IMAGE_CACHE_IMAGE_TAG="${FUGUE_IMAGE_CACHE_IMAGE_TAG:-latest}"
  fi
  if [[ "${FUGUE_IMAGE_STORE_MODE}" == "distributed" ]]; then
    FUGUE_REGISTRY_JANITOR_ENABLED="${FUGUE_REGISTRY_JANITOR_ENABLED:-false}"
    FUGUE_REGISTRY_GC_ENABLED="${FUGUE_REGISTRY_GC_ENABLED:-false}"
  else
    FUGUE_REGISTRY_JANITOR_ENABLED="${FUGUE_REGISTRY_JANITOR_ENABLED:-true}"
    FUGUE_REGISTRY_GC_ENABLED="${FUGUE_REGISTRY_GC_ENABLED:-true}"
  fi
  FUGUE_EDGE_ENABLED="${FUGUE_EDGE_ENABLED:-true}"
  if [[ "${FUGUE_EDGE_ENABLED}" == "true" ]]; then
    require_env FUGUE_EDGE_IMAGE_REPOSITORY
    require_env FUGUE_EDGE_IMAGE_TAG
  else
    FUGUE_EDGE_IMAGE_REPOSITORY="${FUGUE_EDGE_IMAGE_REPOSITORY:-fugue-edge}"
    FUGUE_EDGE_IMAGE_TAG="${FUGUE_EDGE_IMAGE_TAG:-latest}"
  fi

  export KUBECONFIG="${KUBECONFIG:-${HOME}/.kube/config}"
  KUBECTL="$(detect_kubectl)"
  export KUBECTL
  trap cleanup_tmp_artifacts EXIT
  apply_discovery_bundle_defaults || log "DiscoveryBundle not configured; release will require explicit runtime values"
  if ! ensure_kube_api_access; then
    log "continuing with the default Kubernetes API endpoint because no fallback server was configured or reachable"
  fi

  FUGUE_RELEASE_NAME="${FUGUE_RELEASE_NAME:-fugue}"
  FUGUE_NAMESPACE="${FUGUE_NAMESPACE:-fugue-system}"
  FUGUE_HELM_CHART_PATH="${FUGUE_HELM_CHART_PATH:-deploy/helm/fugue}"
  FUGUE_RELEASE_FULLNAME="${FUGUE_RELEASE_FULLNAME:-${FUGUE_RELEASE_NAME}-fugue}"
  FUGUE_API_DEPLOYMENT_NAME="${FUGUE_API_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}-api}"
  FUGUE_LEGACY_API_DEPLOYMENT_NAME="${FUGUE_LEGACY_API_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}}"
  FUGUE_CONTROLLER_DEPLOYMENT_NAME="${FUGUE_CONTROLLER_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}-controller}"
  FUGUE_REGISTRY_DEPLOYMENT_NAME="${FUGUE_REGISTRY_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}-registry}"
  FUGUE_HEADSCALE_DEPLOYMENT_NAME="${FUGUE_HEADSCALE_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}-headscale}"
  FUGUE_SHARED_WORKSPACE_NFS_DEPLOYMENT_NAME="${FUGUE_SHARED_WORKSPACE_NFS_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}-shared-workspace-nfs}"
  FUGUE_SHARED_WORKSPACE_PROVISIONER_DEPLOYMENT_NAME="${FUGUE_SHARED_WORKSPACE_PROVISIONER_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}-shared-workspace-provisioner}"
  FUGUE_POSTGRES_DEPLOYMENT_NAME="${FUGUE_POSTGRES_DEPLOYMENT_NAME:-${FUGUE_RELEASE_FULLNAME}-postgres}"
  FUGUE_HELM_TIMEOUT="${FUGUE_HELM_TIMEOUT:-10m0s}"
  FUGUE_ROLLOUT_TIMEOUT="${FUGUE_ROLLOUT_TIMEOUT:-600s}"
  FUGUE_SMOKE_RETRIES="${FUGUE_SMOKE_RETRIES:-12}"
  FUGUE_SMOKE_DELAY_SECONDS="${FUGUE_SMOKE_DELAY_SECONDS:-5}"
  FUGUE_API_REPLICA_COUNT="${FUGUE_API_REPLICA_COUNT:-2}"
  FUGUE_CONTROLLER_REPLICA_COUNT="${FUGUE_CONTROLLER_REPLICA_COUNT:-2}"
  FUGUE_API_DATABASE_URL="${FUGUE_API_DATABASE_URL:-}"
  FUGUE_DATA_BACKEND_PROVIDER="${FUGUE_DATA_BACKEND_PROVIDER:-cloudflare-r2}"
  FUGUE_DATA_BACKEND_BUCKET="${FUGUE_DATA_BACKEND_BUCKET:-}"
  FUGUE_DATA_BACKEND_REGION="${FUGUE_DATA_BACKEND_REGION:-}"
  FUGUE_DATA_BACKEND_ENDPOINT="${FUGUE_DATA_BACKEND_ENDPOINT:-}"
  FUGUE_DATA_R2_ACCOUNT_ID="${FUGUE_DATA_R2_ACCOUNT_ID:-}"
  FUGUE_DATA_BACKEND_PREFIX="${FUGUE_DATA_BACKEND_PREFIX:-}"
  FUGUE_DATA_BACKEND_ACCESS_KEY_ID="${FUGUE_DATA_BACKEND_ACCESS_KEY_ID:-}"
  FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY="${FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY:-}"
  FUGUE_DATA_BACKEND_SESSION_TOKEN="${FUGUE_DATA_BACKEND_SESSION_TOKEN:-}"
  FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY="${FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY:-}"
  FUGUE_DATA_PRESIGN_TTL="${FUGUE_DATA_PRESIGN_TTL:-6h}"
  FUGUE_POSTGRES_ENABLED="${FUGUE_POSTGRES_ENABLED:-true}"
  FUGUE_CONTROL_PLANE_POSTGRES_ENABLED="${FUGUE_CONTROL_PLANE_POSTGRES_ENABLED:-false}"
  FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API="${FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API:-false}"
  FUGUE_CONTROL_PLANE_POSTGRES_NAME="${FUGUE_CONTROL_PLANE_POSTGRES_NAME:-}"
  FUGUE_CONTROL_PLANE_POSTGRES_IMAGE_NAME="${FUGUE_CONTROL_PLANE_POSTGRES_IMAGE_NAME:-ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie}"
  FUGUE_CONTROL_PLANE_POSTGRES_INSTANCES="${FUGUE_CONTROL_PLANE_POSTGRES_INSTANCES:-3}"
  FUGUE_CONTROL_PLANE_POSTGRES_STORAGE_SIZE="${FUGUE_CONTROL_PLANE_POSTGRES_STORAGE_SIZE:-10Gi}"
  FUGUE_CONTROL_PLANE_POSTGRES_STORAGE_CLASS="${FUGUE_CONTROL_PLANE_POSTGRES_STORAGE_CLASS:-fugue-postgres-rwo}"
  FUGUE_CONTROL_PLANE_POSTGRES_EXISTING_SECRET_NAME="${FUGUE_CONTROL_PLANE_POSTGRES_EXISTING_SECRET_NAME:-}"
  FUGUE_CONTROL_PLANE_POSTGRES_BOOTSTRAP_SOURCE_URL="${FUGUE_CONTROL_PLANE_POSTGRES_BOOTSTRAP_SOURCE_URL:-}"
  FUGUE_CONTROL_PLANE_POSTGRES_DATABASE="${FUGUE_CONTROL_PLANE_POSTGRES_DATABASE:-fugue}"
  FUGUE_CONTROL_PLANE_BACKUP_DRAIN_MODE="${FUGUE_CONTROL_PLANE_BACKUP_DRAIN_MODE:-terminate}"
  FUGUE_CONTROL_PLANE_BACKUP_DRAIN_REQUIRED="${FUGUE_CONTROL_PLANE_BACKUP_DRAIN_REQUIRED:-auto}"
  FUGUE_CONTROL_PLANE_BACKUP_DRAIN_WAIT_SECONDS="${FUGUE_CONTROL_PLANE_BACKUP_DRAIN_WAIT_SECONDS:-120}"
  FUGUE_CONTROL_PLANE_BACKUP_DRAIN_POLL_SECONDS="${FUGUE_CONTROL_PLANE_BACKUP_DRAIN_POLL_SECONDS:-5}"
  FUGUE_CONTROL_PLANE_BACKUP_DRAIN_RECENT_SUCCESS_SECONDS="${FUGUE_CONTROL_PLANE_BACKUP_DRAIN_RECENT_SUCCESS_SECONDS:-90000}"
  FUGUE_CONTROL_PLANE_BACKUP_DRAIN_POST_TERMINATE_SLEEP_SECONDS="${FUGUE_CONTROL_PLANE_BACKUP_DRAIN_POST_TERMINATE_SLEEP_SECONDS:-5}"
  FUGUE_CONTROL_PLANE_SINGLETONS_ENABLED="${FUGUE_CONTROL_PLANE_SINGLETONS_ENABLED:-false}"
  FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR="${FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR:-}"
  FUGUE_REGISTRY_NODEPORT="${FUGUE_REGISTRY_NODEPORT:-30500}"
  FUGUE_REGISTRY_SERVICE_PORT="${FUGUE_REGISTRY_SERVICE_PORT:-5000}"
  FUGUE_API_PUBLIC_DOMAIN="${FUGUE_API_PUBLIC_DOMAIN:-}"
  FUGUE_APP_BASE_DOMAIN="${FUGUE_APP_BASE_DOMAIN:-}"
  FUGUE_CONTROL_PLANE_AUTOMATION_SECRET_NAME="${FUGUE_CONTROL_PLANE_AUTOMATION_SECRET_NAME:-${FUGUE_RELEASE_FULLNAME}-control-plane-automation}"
  FUGUE_COREDNS_NAMESPACE="${FUGUE_COREDNS_NAMESPACE:-kube-system}"
  FUGUE_COREDNS_DEPLOYMENT_NAME="${FUGUE_COREDNS_DEPLOYMENT_NAME:-coredns}"
  FUGUE_COREDNS_TARGET_REPLICAS="${FUGUE_COREDNS_TARGET_REPLICAS:-2}"
  FUGUE_NODE_LOCAL_DNS_ENABLED="${FUGUE_NODE_LOCAL_DNS_ENABLED:-false}"
  FUGUE_NODE_LOCAL_DNS_MODE="${FUGUE_NODE_LOCAL_DNS_MODE:-shadow}"
  FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES="${FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES:-false}"
  FUGUE_NODE_LOCAL_DNS_NODE_NAME="${FUGUE_NODE_LOCAL_DNS_NODE_NAME:-}"
  FUGUE_NODE_LOCAL_DNS_NODE_NAMES="${FUGUE_NODE_LOCAL_DNS_NODE_NAMES:-}"
  FUGUE_NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES="${FUGUE_NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES:-}"
  FUGUE_NODE_LOCAL_DNS_NAMESPACE="${FUGUE_NODE_LOCAL_DNS_NAMESPACE:-kube-system}"
  FUGUE_NODE_LOCAL_DNS_LOCAL_IP="${FUGUE_NODE_LOCAL_DNS_LOCAL_IP:-169.254.20.10}"
  FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN="${FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN:-cluster.local}"
  FUGUE_NODE_LOCAL_DNS_KUBE_DNS_SERVICE_NAME="${FUGUE_NODE_LOCAL_DNS_KUBE_DNS_SERVICE_NAME:-kube-dns}"
  FUGUE_NODE_LOCAL_DNS_COREDNS_CONFIGMAP_NAME="${FUGUE_NODE_LOCAL_DNS_COREDNS_CONFIGMAP_NAME:-coredns}"
  FUGUE_NODE_LOCAL_DNS_EXPECTED_KUBE_DNS_SERVICE_IP="${FUGUE_NODE_LOCAL_DNS_EXPECTED_KUBE_DNS_SERVICE_IP:-}"
  FUGUE_NODE_LOCAL_DNS_EXTERNAL_PROBE_NAME="${FUGUE_NODE_LOCAL_DNS_EXTERNAL_PROBE_NAME:-${FUGUE_API_PUBLIC_DOMAIN:-kubernetes.io}}"
  FUGUE_NODE_LOCAL_DNS_IMAGE_REPOSITORY="${FUGUE_NODE_LOCAL_DNS_IMAGE_REPOSITORY:-registry.k8s.io/dns/k8s-dns-node-cache}"
  FUGUE_NODE_LOCAL_DNS_IMAGE_TAG="${FUGUE_NODE_LOCAL_DNS_IMAGE_TAG:-1.26.8}"
  FUGUE_NODE_LOCAL_DNS_IMAGE_DIGEST="${FUGUE_NODE_LOCAL_DNS_IMAGE_DIGEST:-sha256:bc6e64e2c85956af2fcc0aa720086410d41b4f31f378c9a92646fecc85cd4739}"
  FUGUE_NODE_LOCAL_DNS_IMAGE_PULL_POLICY="${FUGUE_NODE_LOCAL_DNS_IMAGE_PULL_POLICY:-IfNotPresent}"
  FUGUE_NODE_LOCAL_DNS_PROBE_IMAGE="${FUGUE_NODE_LOCAL_DNS_PROBE_IMAGE:-docker.io/library/busybox@sha256:9532d8c39891ca2ecde4d30d7710e01fb739c87a8b9299685c63704296b16028}"
  FUGUE_NODE_LOCAL_DNS_PROBE_TIMEOUT_SECONDS="${FUGUE_NODE_LOCAL_DNS_PROBE_TIMEOUT_SECONDS:-180}"
  FUGUE_NODE_LOCAL_DNS_CRITICAL_READY_TIMEOUT_SECONDS="${FUGUE_NODE_LOCAL_DNS_CRITICAL_READY_TIMEOUT_SECONDS:-45}"
  FUGUE_NODE_LOCAL_DNS_NODE_WATCH_SECONDS="${FUGUE_NODE_LOCAL_DNS_NODE_WATCH_SECONDS:-30}"
  node_local_dns_configure_cohort_names
  if [[ "${NODE_LOCAL_DNS_SPLIT_COHORT}" == "true" && "${FUGUE_NODE_LOCAL_DNS_ENABLED}" != "true" ]]; then
    fail "FUGUE_NODE_LOCAL_DNS_ENABLED must be true when preserved offline NodeLocal DNSCache nodes are configured"
  fi
  if [[ "${NODE_LOCAL_DNS_SPLIT_COHORT}" == "true" && "${FUGUE_IMAGE_CACHE_ENABLED}" != "true" ]]; then
    fail "image-cache cannot be disabled while a preserved offline node exists"
  fi
  FUGUE_SHARED_WORKSPACE_STORAGE_ENABLED="${FUGUE_SHARED_WORKSPACE_STORAGE_ENABLED:-false}"
  FUGUE_SHARED_WORKSPACE_STORAGE_CLASS="${FUGUE_SHARED_WORKSPACE_STORAGE_CLASS:-fugue-rwx}"
  FUGUE_SHARED_WORKSPACE_NFS_CLUSTER_IP="${FUGUE_SHARED_WORKSPACE_NFS_CLUSTER_IP:-}"
  FUGUE_EDGE_GROUP_ID="${FUGUE_EDGE_GROUP_ID:-}"
  FUGUE_EDGE_CADDY_ENABLED="${FUGUE_EDGE_CADDY_ENABLED:-true}"
  FUGUE_EDGE_CADDY_LISTEN_ADDR="${FUGUE_EDGE_CADDY_LISTEN_ADDR:-:18443}"
  FUGUE_EDGE_CADDY_TLS_MODE="${FUGUE_EDGE_CADDY_TLS_MODE:-internal}"
  FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED="${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED:-false}"
  FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTP="${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTP:-80}"
  FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTPS="${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTPS:-443}"
  FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED="${FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED:-false}"
  FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME="${FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME:-}"
  FUGUE_EDGE_CADDY_STATIC_TLS_MOUNT_PATH="${FUGUE_EDGE_CADDY_STATIC_TLS_MOUNT_PATH:-/etc/caddy/static-tls}"
  FUGUE_EDGE_CADDY_STATIC_TLS_CERTIFICATE_KEY="${FUGUE_EDGE_CADDY_STATIC_TLS_CERTIFICATE_KEY:-tls.crt}"
  FUGUE_EDGE_CADDY_STATIC_TLS_PRIVATE_KEY_KEY="${FUGUE_EDGE_CADDY_STATIC_TLS_PRIVATE_KEY_KEY:-tls.key}"
  FUGUE_EDGE_DYNAMIC_ENABLED="${FUGUE_EDGE_DYNAMIC_ENABLED:-true}"
  FUGUE_EDGE_REGION="${FUGUE_EDGE_REGION:-}"
  FUGUE_EDGE_COUNTRY="${FUGUE_EDGE_COUNTRY:-}"
  FUGUE_EDGE_PUBLIC_HOSTNAME="${FUGUE_EDGE_PUBLIC_HOSTNAME:-}"
  FUGUE_EDGE_PUBLIC_IPV4="${FUGUE_EDGE_PUBLIC_IPV4:-}"
  FUGUE_EDGE_PUBLIC_IPV6="${FUGUE_EDGE_PUBLIC_IPV6:-}"
  FUGUE_EDGE_MESH_IP="${FUGUE_EDGE_MESH_IP:-}"
  FUGUE_EDGE_ASSET_CACHE_PATH="${FUGUE_EDGE_ASSET_CACHE_PATH:-}"
  FUGUE_EDGE_ASSET_CACHE_MAX_BYTES="${FUGUE_EDGE_ASSET_CACHE_MAX_BYTES:-}"
  FUGUE_EDGE_NODE_SELECTOR_COUNTRY_CODE="${FUGUE_EDGE_NODE_SELECTOR_COUNTRY_CODE:-}"
  FUGUE_EDGE_EXTRA_GROUPS="${FUGUE_EDGE_EXTRA_GROUPS:-}"
  FUGUE_EDGE_TOKEN_SECRET_NAME="${FUGUE_EDGE_TOKEN_SECRET_NAME:-}"
  if [[ -n "$(trim_field "${FUGUE_EDGE_TOKEN_SECRET_NAME}")" ]]; then
    FUGUE_EDGE_TOKEN_SECRET_KEY="${FUGUE_EDGE_TOKEN_SECRET_KEY:-FUGUE_EDGE_TOKEN}"
  else
    FUGUE_EDGE_TOKEN_SECRET_KEY="${FUGUE_EDGE_TOKEN_SECRET_KEY:-FUGUE_EDGE_TLS_ASK_TOKEN}"
  fi
  FUGUE_DNS_ENABLED="${FUGUE_DNS_ENABLED:-false}"
  FUGUE_DNS_ANSWER_IPS="${FUGUE_DNS_ANSWER_IPS:-}"
  FUGUE_DNS_ROUTE_A_ANSWER_IPS="${FUGUE_DNS_ROUTE_A_ANSWER_IPS:-}"
  FUGUE_DNS_STATIC_RECORDS_JSON="${FUGUE_DNS_STATIC_RECORDS_JSON:-}"
  FUGUE_PLATFORM_ROUTES_JSON="${FUGUE_PLATFORM_ROUTES_JSON:-}"
  FUGUE_EDGE_QUALITY_RANKING_MODE="${FUGUE_EDGE_QUALITY_RANKING_MODE:-shadow}"
  FUGUE_DNS_NODE_SELECTOR_COUNTRY_CODE="${FUGUE_DNS_NODE_SELECTOR_COUNTRY_CODE:-}"
  FUGUE_DNS_EXTRA_GROUPS="${FUGUE_DNS_EXTRA_GROUPS:-}"
  FUGUE_DNS_EXTRA_ZONES="${FUGUE_DNS_EXTRA_ZONES:-}"
  FUGUE_DNS_GEOIP_OVERRIDES_JSON="${FUGUE_DNS_GEOIP_OVERRIDES_JSON:-}"
  FUGUE_DNS_TOKEN_SECRET_NAME="${FUGUE_DNS_TOKEN_SECRET_NAME:-${FUGUE_EDGE_TOKEN_SECRET_NAME}}"
  if [[ -n "$(trim_field "${FUGUE_DNS_TOKEN_SECRET_NAME}")" ]]; then
    FUGUE_DNS_TOKEN_SECRET_KEY="${FUGUE_DNS_TOKEN_SECRET_KEY:-FUGUE_DNS_TOKEN}"
  else
    FUGUE_DNS_TOKEN_SECRET_KEY="${FUGUE_DNS_TOKEN_SECRET_KEY:-FUGUE_EDGE_TLS_ASK_TOKEN}"
  fi
  FUGUE_DNS_NAMESERVERS="${FUGUE_DNS_NAMESERVERS:-}"
  FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED="${FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED:-false}"
  FUGUE_DNS_PUBLIC_HOST_IP="${FUGUE_DNS_PUBLIC_HOST_IP:-}"
  if [[ "${FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED}" == "true" ]]; then
    FUGUE_DNS_UDP_ADDR="${FUGUE_DNS_UDP_ADDR:-:53}"
    FUGUE_DNS_TCP_ADDR="${FUGUE_DNS_TCP_ADDR:-:53}"
  else
    FUGUE_DNS_UDP_ADDR="${FUGUE_DNS_UDP_ADDR:-127.0.0.1:5353}"
    FUGUE_DNS_TCP_ADDR="${FUGUE_DNS_TCP_ADDR:-127.0.0.1:5353}"
  fi
  if [[ -z "$(trim_field "${FUGUE_DNS_ZONE:-}")" && -n "$(trim_field "${FUGUE_APP_BASE_DOMAIN}")" ]]; then
    FUGUE_DNS_ZONE="dns.${FUGUE_APP_BASE_DOMAIN}"
  else
    FUGUE_DNS_ZONE="${FUGUE_DNS_ZONE:-}"
  fi
  FUGUE_DNS_TTL="${FUGUE_DNS_TTL:-60}"
  FUGUE_MESH_RECOVERY_ENABLED="${FUGUE_MESH_RECOVERY_ENABLED:-false}"
  FUGUE_MESH_RECOVERY_LISTEN_ADDR="${FUGUE_MESH_RECOVERY_LISTEN_ADDR:-:7840}"
  FUGUE_MESH_RECOVERY_GENERATION="${FUGUE_MESH_RECOVERY_GENERATION:-meshgen-initial}"
  FUGUE_MESH_RECOVERY_PREVIOUS_GENERATION="${FUGUE_MESH_RECOVERY_PREVIOUS_GENERATION:-}"
  FUGUE_MESH_RECOVERY_MODE="${FUGUE_MESH_RECOVERY_MODE:-normal}"
  FUGUE_MESH_RECOVERY_LOGIN_SERVER="${FUGUE_MESH_RECOVERY_LOGIN_SERVER:-${FUGUE_CLUSTER_JOIN_MESH_LOGIN_SERVER:-}}"
  FUGUE_MESH_RECOVERY_MESSAGE="${FUGUE_MESH_RECOVERY_MESSAGE:-}"
  FUGUE_MESH_RECOVERY_DIRECTORY_VALID_FOR="${FUGUE_MESH_RECOVERY_DIRECTORY_VALID_FOR:-2m}"
  FUGUE_MESH_RECOVERY_MANIFEST_VALID_FOR="${FUGUE_MESH_RECOVERY_MANIFEST_VALID_FOR:-2m}"
  FUGUE_MESH_RECOVERY_NODE_TTL="${FUGUE_MESH_RECOVERY_NODE_TTL:-2m}"
  FUGUE_MESH_RECOVERY_SIGNING_KEY_ID="${FUGUE_MESH_RECOVERY_SIGNING_KEY_ID:-mesh-recovery}"
  FUGUE_MESH_RECOVERY_TOKEN_SECRET_NAME="${FUGUE_MESH_RECOVERY_TOKEN_SECRET_NAME:-}"
  FUGUE_MESH_RECOVERY_TOKEN_SECRET_KEY="${FUGUE_MESH_RECOVERY_TOKEN_SECRET_KEY:-FUGUE_MESH_RECOVERY_TOKEN}"
  FUGUE_MESH_RECOVERY_SIGNING_KEY_SECRET_NAME="${FUGUE_MESH_RECOVERY_SIGNING_KEY_SECRET_NAME:-}"
  FUGUE_MESH_RECOVERY_SIGNING_KEY_SECRET_KEY="${FUGUE_MESH_RECOVERY_SIGNING_KEY_SECRET_KEY:-FUGUE_MESH_RECOVERY_SIGNING_KEY}"
  FUGUE_MESH_RECOVERY_REJOIN_AUTH_KEY_SECRET_NAME="${FUGUE_MESH_RECOVERY_REJOIN_AUTH_KEY_SECRET_NAME:-}"
  FUGUE_MESH_RECOVERY_REJOIN_AUTH_KEY_SECRET_KEY="${FUGUE_MESH_RECOVERY_REJOIN_AUTH_KEY_SECRET_KEY:-FUGUE_MESH_RECOVERY_REJOIN_AUTH_KEY}"
  FUGUE_MESH_RECOVERY_REJOIN_AUTH_KEY_SECRET_OPTIONAL="${FUGUE_MESH_RECOVERY_REJOIN_AUTH_KEY_SECRET_OPTIONAL:-true}"

  case "${FUGUE_EDGE_CADDY_TLS_MODE}" in
    off|internal|public-on-demand) ;;
    *) fail "FUGUE_EDGE_CADDY_TLS_MODE must be off, internal, or public-on-demand" ;;
  esac
  if [[ "${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED}" == "true" && "${FUGUE_EDGE_CADDY_ENABLED}" != "true" ]]; then
    fail "FUGUE_EDGE_CADDY_ENABLED must be true when FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED=true"
  fi
  if [[ "${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED}" == "true" && "${FUGUE_EDGE_CADDY_LISTEN_ADDR}" != ":443" ]]; then
    fail "FUGUE_EDGE_CADDY_LISTEN_ADDR must be :443 when FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED=true"
  fi
  if [[ "${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED}" == "true" && "${FUGUE_EDGE_CADDY_TLS_MODE}" != "public-on-demand" ]]; then
    fail "FUGUE_EDGE_CADDY_TLS_MODE must be public-on-demand when FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED=true"
  fi
  case "${FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED}" in
    true|false) ;;
    *) fail "FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED must be true or false" ;;
  esac
  if [[ "${FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED}" == "true" && "${FUGUE_EDGE_CADDY_ENABLED}" != "true" ]]; then
    fail "FUGUE_EDGE_CADDY_ENABLED must be true when FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED=true"
  fi
  if [[ "${FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED}" == "true" && "${FUGUE_EDGE_CADDY_TLS_MODE}" == "off" ]]; then
    fail "FUGUE_EDGE_CADDY_TLS_MODE must not be off when FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED=true"
  fi
  if [[ "${FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED}" == "true" && -z "$(trim_field "${FUGUE_APP_BASE_DOMAIN}")" ]]; then
    fail "FUGUE_APP_BASE_DOMAIN is required when FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED=true"
  fi
  if [[ "${FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED}" == "true" && -z "$(trim_field "${FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME}")" ]]; then
    fail "FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME is required when FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED=true"
  fi
  if ! [[ "${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTP}" =~ ^[0-9]+$ ]] || (( FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTP <= 0 || FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTP > 65535 )); then
    fail "FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTP must be an integer between 1 and 65535"
  fi
  if ! [[ "${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTPS}" =~ ^[0-9]+$ ]] || (( FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTPS <= 0 || FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTPS > 65535 )); then
    fail "FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTPS must be an integer between 1 and 65535"
  fi
  case "${FUGUE_POSTGRES_ENABLED}" in
    true|false) ;;
    *) fail "FUGUE_POSTGRES_ENABLED must be true or false" ;;
  esac
  case "${FUGUE_CONTROL_PLANE_POSTGRES_ENABLED}" in
    true|false) ;;
    *) fail "FUGUE_CONTROL_PLANE_POSTGRES_ENABLED must be true or false" ;;
  esac
  case "${FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API}" in
    true|false) ;;
    *) fail "FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API must be true or false" ;;
  esac
  if [[ "${FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API}" == "true" && "${FUGUE_CONTROL_PLANE_POSTGRES_ENABLED}" != "true" ]]; then
    fail "FUGUE_CONTROL_PLANE_POSTGRES_ENABLED must be true when FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API=true"
  fi
  case "${FUGUE_CONTROL_PLANE_BACKUP_DRAIN_MODE}" in
    skip|wait|terminate) ;;
    *) fail "FUGUE_CONTROL_PLANE_BACKUP_DRAIN_MODE must be skip, wait, or terminate" ;;
  esac
  case "${FUGUE_CONTROL_PLANE_BACKUP_DRAIN_REQUIRED}" in
    auto|true|false) ;;
    *) fail "FUGUE_CONTROL_PLANE_BACKUP_DRAIN_REQUIRED must be auto, true, or false" ;;
  esac
  for numeric_var in \
    FUGUE_CONTROL_PLANE_BACKUP_DRAIN_WAIT_SECONDS \
    FUGUE_CONTROL_PLANE_BACKUP_DRAIN_POLL_SECONDS \
    FUGUE_CONTROL_PLANE_BACKUP_DRAIN_RECENT_SUCCESS_SECONDS \
    FUGUE_CONTROL_PLANE_BACKUP_DRAIN_POST_TERMINATE_SLEEP_SECONDS; do
    numeric_value="${!numeric_var}"
    if ! [[ "${numeric_value}" =~ ^[0-9]+$ ]]; then
      fail "${numeric_var} must be an integer"
    fi
    if [[ "${numeric_var}" == "FUGUE_CONTROL_PLANE_BACKUP_DRAIN_POST_TERMINATE_SLEEP_SECONDS" ]]; then
      continue
    fi
    if (( numeric_value < 1 )); then
      fail "${numeric_var} must be an integer >= 1"
    fi
  done
  if [[ "${FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API}" == "true" ]] &&
    ! api_database_already_uses_control_plane_postgres &&
    [[ -z "$(trim_field "${FUGUE_CONTROL_PLANE_POSTGRES_BOOTSTRAP_SOURCE_URL}")" ]]; then
    fail "FUGUE_CONTROL_PLANE_POSTGRES_BOOTSTRAP_SOURCE_URL is required before first promoting control-plane Postgres to the API store"
  fi
  case "${FUGUE_CONTROL_PLANE_SINGLETONS_ENABLED}" in
    true|false) ;;
    *) fail "FUGUE_CONTROL_PLANE_SINGLETONS_ENABLED must be true or false" ;;
  esac
  case "${FUGUE_NODE_LOCAL_DNS_ENABLED}" in
    true|false) ;;
    *) fail "FUGUE_NODE_LOCAL_DNS_ENABLED must be true or false" ;;
  esac
  case "${FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES}" in
    true|false) ;;
    *) fail "FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES must be true or false" ;;
  esac
  case "${FUGUE_NODE_LOCAL_DNS_MODE}" in
    shadow|iptables) ;;
    *) fail "FUGUE_NODE_LOCAL_DNS_MODE must be shadow or iptables" ;;
  esac
  if ! [[ "${FUGUE_NODE_LOCAL_DNS_LOCAL_IP}" =~ ^169\.254\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
    fail "FUGUE_NODE_LOCAL_DNS_LOCAL_IP must be an IPv4 link-local address"
  fi
  if ! [[ "${FUGUE_NODE_LOCAL_DNS_IMAGE_DIGEST}" =~ ^sha256:[a-f0-9]{64}$ ]]; then
    fail "FUGUE_NODE_LOCAL_DNS_IMAGE_DIGEST must be a sha256 digest"
  fi
  if ! [[ "${FUGUE_NODE_LOCAL_DNS_EXTERNAL_PROBE_NAME}" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])$ ]]; then
    fail "FUGUE_NODE_LOCAL_DNS_EXTERNAL_PROBE_NAME must be a DNS name"
  fi
  if ! [[ "${FUGUE_NODE_LOCAL_DNS_PROBE_TIMEOUT_SECONDS}" =~ ^[0-9]+$ ]] || (( FUGUE_NODE_LOCAL_DNS_PROBE_TIMEOUT_SECONDS < 30 )); then
    fail "FUGUE_NODE_LOCAL_DNS_PROBE_TIMEOUT_SECONDS must be an integer >= 30"
  fi
  if ! [[ "${FUGUE_NODE_LOCAL_DNS_CRITICAL_READY_TIMEOUT_SECONDS}" =~ ^[0-9]+$ ]] ||
    (( FUGUE_NODE_LOCAL_DNS_CRITICAL_READY_TIMEOUT_SECONDS < 15 || FUGUE_NODE_LOCAL_DNS_CRITICAL_READY_TIMEOUT_SECONDS > FUGUE_NODE_LOCAL_DNS_PROBE_TIMEOUT_SECONDS )); then
    fail "FUGUE_NODE_LOCAL_DNS_CRITICAL_READY_TIMEOUT_SECONDS must be an integer between 15 and FUGUE_NODE_LOCAL_DNS_PROBE_TIMEOUT_SECONDS"
  fi
  if ! [[ "${FUGUE_NODE_LOCAL_DNS_NODE_WATCH_SECONDS}" =~ ^[0-9]+$ ]] || (( FUGUE_NODE_LOCAL_DNS_NODE_WATCH_SECONDS < 10 )); then
    fail "FUGUE_NODE_LOCAL_DNS_NODE_WATCH_SECONDS must be an integer >= 10"
  fi
  case "${FUGUE_EDGE_DYNAMIC_ENABLED}" in
    true|false) ;;
    *) fail "FUGUE_EDGE_DYNAMIC_ENABLED must be true or false" ;;
  esac
  case "${FUGUE_OBSERVABILITY_ENABLED}" in
    true|false) ;;
    *) fail "FUGUE_OBSERVABILITY_ENABLED must be true or false" ;;
  esac
  case "${FUGUE_TELEMETRY_AGENT_ENABLED}" in
    true|false) ;;
    *) fail "FUGUE_TELEMETRY_AGENT_ENABLED must be true or false" ;;
  esac
  case "${FUGUE_OBSERVABILITY_KUBERNETES_LOGS_ENABLED}" in
    true|false) ;;
    *) fail "FUGUE_OBSERVABILITY_KUBERNETES_LOGS_ENABLED must be true or false" ;;
  esac
  case "${FUGUE_OBSERVABILITY_METRICS_ENABLED}" in
    true|false) ;;
    *) fail "FUGUE_OBSERVABILITY_METRICS_ENABLED must be true or false" ;;
  esac
  case "${FUGUE_OBSERVABILITY_ALERTS_ENABLED}" in
    true|false) ;;
    *) fail "FUGUE_OBSERVABILITY_ALERTS_ENABLED must be true or false" ;;
  esac
  if [[ "${FUGUE_OBSERVABILITY_ALERTS_ENABLED}" == "true" && "${FUGUE_OBSERVABILITY_METRICS_ENABLED}" != "true" ]]; then
    fail "FUGUE_OBSERVABILITY_METRICS_ENABLED must be true when FUGUE_OBSERVABILITY_ALERTS_ENABLED=true"
  fi
  case "${FUGUE_OBSERVABILITY_LOGS_ENABLED}" in
    true|false) ;;
    *) fail "FUGUE_OBSERVABILITY_LOGS_ENABLED must be true or false" ;;
  esac
  case "${FUGUE_OBSERVABILITY_ANALYTICS_ENABLED}" in
    true|false) ;;
    *) fail "FUGUE_OBSERVABILITY_ANALYTICS_ENABLED must be true or false" ;;
  esac
  for numeric_var in \
    FUGUE_OBSERVABILITY_QUEUE_SIZE \
    FUGUE_OBSERVABILITY_BATCH_SIZE \
    FUGUE_OBSERVABILITY_KUBERNETES_LOG_TAIL_LINES \
    FUGUE_OBSERVABILITY_KUBERNETES_LOG_MAX_PODS \
    FUGUE_OBSERVABILITY_KUBERNETES_LOG_MAX_LINES_PER_CYCLE \
    FUGUE_OBSERVABILITY_MAX_PAYLOAD_BYTES \
    FUGUE_OBSERVABILITY_MEMORY_LIMIT_BYTES \
    FUGUE_OBSERVABILITY_RETRY_MAX_ATTEMPTS; do
    numeric_value="${!numeric_var}"
    if ! [[ "${numeric_value}" =~ ^[0-9]+$ ]] || (( numeric_value < 1 )); then
      fail "${numeric_var} must be an integer >= 1"
    fi
  done
  if [[ "${FUGUE_POSTGRES_ENABLED}" != "true" && "${FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API}" != "true" && -z "$(trim_field "${FUGUE_API_DATABASE_URL}")" ]]; then
    fail "FUGUE_API_DATABASE_URL or FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API=true is required when FUGUE_POSTGRES_ENABLED=false"
  fi
  if ! [[ "${FUGUE_CONTROL_PLANE_POSTGRES_INSTANCES}" =~ ^[0-9]+$ ]] || (( FUGUE_CONTROL_PLANE_POSTGRES_INSTANCES < 2 )); then
    fail "FUGUE_CONTROL_PLANE_POSTGRES_INSTANCES must be an integer >= 2"
  fi
  if [[ -n "$(trim_field "${FUGUE_EDGE_EXTRA_GROUPS}")" && -z "$(trim_field "${FUGUE_EDGE_NODE_SELECTOR_COUNTRY_CODE}")" ]]; then
    fail "FUGUE_EDGE_NODE_SELECTOR_COUNTRY_CODE must be set when FUGUE_EDGE_EXTRA_GROUPS is set"
  fi

  if [[ "${FUGUE_DNS_ENABLED}" == "true" ]]; then
    [[ -n "$(trim_field "${FUGUE_DNS_ZONE}")" ]] || fail "FUGUE_DNS_ZONE or FUGUE_APP_BASE_DOMAIN is required when FUGUE_DNS_ENABLED=true"
    require_env FUGUE_DNS_ANSWER_IPS
    if [[ "$(dns_answer_ip_count "${FUGUE_DNS_ANSWER_IPS}")" == "0" ]]; then
      fail "FUGUE_DNS_ANSWER_IPS must contain at least one non-empty IP when FUGUE_DNS_ENABLED=true"
    fi
    if [[ "${FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED}" == "true" ]]; then
      if [[ -z "$(trim_field "${FUGUE_DNS_PUBLIC_HOST_IP}")" && "$(dns_answer_ip_count "${FUGUE_DNS_ANSWER_IPS}")" == "1" ]]; then
        FUGUE_DNS_PUBLIC_HOST_IP="$(dns_answer_ips_lines "${FUGUE_DNS_ANSWER_IPS}")"
      fi
      [[ -n "$(trim_field "${FUGUE_DNS_PUBLIC_HOST_IP}")" ]] || fail "FUGUE_DNS_PUBLIC_HOST_IP is required when public DNS host ports are enabled with multiple answer IPs"
      if ! grep -Fqx "${FUGUE_DNS_PUBLIC_HOST_IP}" < <(dns_answer_ips_lines "${FUGUE_DNS_ANSWER_IPS}"); then
        fail "FUGUE_DNS_PUBLIC_HOST_IP must be one of FUGUE_DNS_ANSWER_IPS"
      fi
    fi
    if [[ -n "$(trim_field "${FUGUE_DNS_ROUTE_A_ANSWER_IPS}")" && "$(dns_answer_ip_count "${FUGUE_DNS_ROUTE_A_ANSWER_IPS}")" == "0" ]]; then
      fail "FUGUE_DNS_ROUTE_A_ANSWER_IPS must contain only non-empty IP entries"
    fi
    if ! [[ "${FUGUE_DNS_TTL}" =~ ^[0-9]+$ ]] || (( FUGUE_DNS_TTL <= 0 || FUGUE_DNS_TTL > 3600 )); then
      fail "FUGUE_DNS_TTL must be an integer between 1 and 3600"
    fi
  fi
  edge_extra_groups_yaml >/dev/null
  if [[ -n "$(trim_field "${FUGUE_DNS_EXTRA_GROUPS}")" && "${FUGUE_DNS_ENABLED}" != "true" ]]; then
    fail "FUGUE_DNS_ENABLED must be true when FUGUE_DNS_EXTRA_GROUPS is set"
  fi
  if [[ -n "$(trim_field "${FUGUE_DNS_EXTRA_ZONES}")" && "${FUGUE_DNS_ENABLED}" != "true" ]]; then
    fail "FUGUE_DNS_ENABLED must be true when FUGUE_DNS_EXTRA_ZONES is set"
  fi
  if [[ -n "$(trim_field "${FUGUE_DNS_EXTRA_GROUPS}")" && -z "$(trim_field "${FUGUE_DNS_NODE_SELECTOR_COUNTRY_CODE}")" ]]; then
    fail "FUGUE_DNS_NODE_SELECTOR_COUNTRY_CODE must be set when FUGUE_DNS_EXTRA_GROUPS is set"
  fi
  if [[ -n "$(trim_field "${FUGUE_DNS_GEOIP_OVERRIDES_JSON}")" ]]; then
    command_exists python3 || fail "python3 is required to validate FUGUE_DNS_GEOIP_OVERRIDES_JSON"
    FUGUE_DNS_GEOIP_OVERRIDES_JSON="${FUGUE_DNS_GEOIP_OVERRIDES_JSON}" python3 - <<'PY' >/dev/null
import json
import os
import sys

raw = os.environ.get("FUGUE_DNS_GEOIP_OVERRIDES_JSON", "")
try:
    json.loads(raw)
except Exception as exc:
    print(f"FUGUE_DNS_GEOIP_OVERRIDES_JSON must be valid JSON: {exc}", file=sys.stderr)
    raise SystemExit(1)
PY
  fi
  case "${FUGUE_EDGE_QUALITY_RANKING_MODE}" in
    shadow|active|legacy|off|disabled) ;;
    *) fail "FUGUE_EDGE_QUALITY_RANKING_MODE must be shadow, active, legacy, off, or disabled" ;;
  esac
  if [[ -n "$(trim_field "${FUGUE_EDGE_ASSET_CACHE_MAX_BYTES}")" ]] && ! [[ "${FUGUE_EDGE_ASSET_CACHE_MAX_BYTES}" =~ ^[0-9]+$ ]]; then
    fail "FUGUE_EDGE_ASSET_CACHE_MAX_BYTES must be an integer"
  fi
  dns_extra_groups_yaml >/dev/null
  if [[ -z "${FUGUE_CLUSTER_JOIN_K3S_VERSION:-}" ]]; then
    FUGUE_CLUSTER_JOIN_K3S_VERSION="$(detect_control_plane_k3s_version || true)"
  fi
  if [[ -z "${FUGUE_CLUSTER_JOIN_K3S_VERSION:-}" ]]; then
    FUGUE_CLUSTER_JOIN_K3S_VERSION="$(detect_cluster_join_k3s_version || true)"
  fi
  if [[ -z "${FUGUE_CLUSTER_JOIN_MESH_PROVIDER:-}" ]]; then
    FUGUE_CLUSTER_JOIN_MESH_PROVIDER="$(detect_cluster_join_mesh_provider || true)"
  fi
  if [[ -z "${FUGUE_CLUSTER_JOIN_MESH_LOGIN_SERVER:-}" ]]; then
    FUGUE_CLUSTER_JOIN_MESH_LOGIN_SERVER="$(detect_cluster_join_mesh_login_server || true)"
  fi
  if [[ -z "${FUGUE_CLUSTER_JOIN_MESH_AUTH_KEY:-}" ]]; then
    FUGUE_CLUSTER_JOIN_MESH_AUTH_KEY="$(detect_cluster_join_mesh_auth_key || true)"
  fi
  if [[ -z "$(trim_field "${FUGUE_MESH_RECOVERY_LOGIN_SERVER:-}")" && -n "$(trim_field "${FUGUE_CLUSTER_JOIN_MESH_LOGIN_SERVER:-}")" ]]; then
    FUGUE_MESH_RECOVERY_LOGIN_SERVER="${FUGUE_CLUSTER_JOIN_MESH_LOGIN_SERVER}"
  fi
  if [[ -n "$(trim_field "${FUGUE_CLUSTER_JOIN_MESH_PROVIDER:-}")" ]]; then
    [[ -n "$(trim_field "${FUGUE_CLUSTER_JOIN_MESH_LOGIN_SERVER:-}")" ]] || fail "FUGUE_CLUSTER_JOIN_MESH_LOGIN_SERVER is required when FUGUE_CLUSTER_JOIN_MESH_PROVIDER is set"
    [[ -n "$(trim_field "${FUGUE_CLUSTER_JOIN_MESH_AUTH_KEY:-}")" ]] || fail "FUGUE_CLUSTER_JOIN_MESH_AUTH_KEY is required when FUGUE_CLUSTER_JOIN_MESH_PROVIDER is set"
  fi
  case "${FUGUE_MESH_RECOVERY_ENABLED}" in
    true|false) ;;
    *) fail "FUGUE_MESH_RECOVERY_ENABLED must be true or false" ;;
  esac
  case "${FUGUE_MESH_RECOVERY_MODE}" in
    normal|reset) ;;
    *) fail "FUGUE_MESH_RECOVERY_MODE must be normal or reset" ;;
  esac
  case "${FUGUE_MESH_RECOVERY_REJOIN_AUTH_KEY_SECRET_OPTIONAL}" in
    true|false) ;;
    *) fail "FUGUE_MESH_RECOVERY_REJOIN_AUTH_KEY_SECRET_OPTIONAL must be true or false" ;;
  esac
  if [[ "${FUGUE_MESH_RECOVERY_ENABLED}" == "true" ]]; then
    [[ -n "$(trim_field "${FUGUE_MESH_RECOVERY_TOKEN_SECRET_NAME}")" ]] || fail "FUGUE_MESH_RECOVERY_TOKEN_SECRET_NAME is required when FUGUE_MESH_RECOVERY_ENABLED=true"
    [[ -n "$(trim_field "${FUGUE_MESH_RECOVERY_SIGNING_KEY_SECRET_NAME}")" ]] || fail "FUGUE_MESH_RECOVERY_SIGNING_KEY_SECRET_NAME is required when FUGUE_MESH_RECOVERY_ENABLED=true"
    [[ -n "$(trim_field "${FUGUE_MESH_RECOVERY_GENERATION}")" ]] || fail "FUGUE_MESH_RECOVERY_GENERATION is required when FUGUE_MESH_RECOVERY_ENABLED=true"
    [[ -n "$(trim_field "${FUGUE_MESH_RECOVERY_LOGIN_SERVER}")" ]] || fail "FUGUE_MESH_RECOVERY_LOGIN_SERVER or FUGUE_CLUSTER_JOIN_MESH_LOGIN_SERVER is required when FUGUE_MESH_RECOVERY_ENABLED=true"
  fi
  if [[ "${FUGUE_MESH_RECOVERY_ENABLED}" == "true" && "${FUGUE_MESH_RECOVERY_MODE}" == "reset" && -z "$(trim_field "${FUGUE_MESH_RECOVERY_REJOIN_AUTH_KEY_SECRET_NAME}")" ]]; then
    fail "FUGUE_MESH_RECOVERY_REJOIN_AUTH_KEY_SECRET_NAME is required when FUGUE_MESH_RECOVERY_MODE=reset"
  fi

  if [[ -z "${FUGUE_REGISTRY_PULL_BASE:-}" ]]; then
    FUGUE_REGISTRY_PULL_BASE="$(detect_existing_registry_pull_base || true)"
  fi
  if [[ -z "${FUGUE_REGISTRY_PULL_BASE:-}" ]]; then
    FUGUE_REGISTRY_PULL_BASE="${FUGUE_DEFAULT_REGISTRY_PULL_BASE}"
  fi
  [[ -n "$(trim_field "${FUGUE_REGISTRY_PULL_BASE}")" ]] || fail "FUGUE_REGISTRY_PULL_BASE must come from DiscoveryBundle, an explicit env var, or FUGUE_DEFAULT_REGISTRY_PULL_BASE"
  if [[ -z "${FUGUE_REGISTRY_PUSH_BASE:-}" ]]; then
    if [[ "${FUGUE_IMAGE_STORE_MODE}" == "distributed" ]]; then
      FUGUE_REGISTRY_PUSH_BASE="${FUGUE_REGISTRY_PULL_BASE}"
    else
      FUGUE_REGISTRY_PUSH_BASE="${FUGUE_RELEASE_FULLNAME}-registry.${FUGUE_NAMESPACE}.svc.cluster.local:${FUGUE_REGISTRY_SERVICE_PORT}"
    fi
  fi
  if [[ -z "${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT:-}" ]]; then
    FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT="${FUGUE_DEFAULT_CLUSTER_JOIN_REGISTRY_ENDPOINT:-}"
  fi
  if [[ "${FUGUE_IMAGE_CACHE_ENABLED}" == "true" ]] &&
    { [[ -z "$(trim_field "${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT}")" ]] ||
      is_legacy_local_cluster_join_registry_endpoint "${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT}"; }; then
    if [[ -n "$(trim_field "${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT}")" ]]; then
      log "replacing legacy local registry endpoint ${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT} with image cache endpoint"
    fi
    FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT="http://127.0.0.1:${FUGUE_IMAGE_CACHE_PORT}"
  fi
  [[ -n "$(trim_field "${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT}")" ]] || fail "FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT must come from DiscoveryBundle or an explicit env var"
  if [[ -z "${FUGUE_CLUSTER_JOIN_SERVER_FALLBACKS:-}" ]]; then
    FUGUE_CLUSTER_JOIN_SERVER_FALLBACKS="$(detect_cluster_join_server_fallbacks || true)"
  fi

  if [[ -z "${FUGUE_SMOKE_URL:-}" && -n "${FUGUE_API_PUBLIC_DOMAIN:-}" ]]; then
    FUGUE_SMOKE_URL="https://${FUGUE_API_PUBLIC_DOMAIN}/healthz"
  fi
  require_env FUGUE_SMOKE_URL
  ensure_host_time_sync
  ensure_control_plane_observability
  run_release_preflight

  command_exists helm || fail "helm is not installed"
  effective_cluster_join_registry_endpoint="${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT}"
  if [[ -z "${effective_cluster_join_registry_endpoint}" && "${FUGUE_IMAGE_CACHE_ENABLED}" == "true" ]]; then
    effective_cluster_join_registry_endpoint="http://127.0.0.1:5000"
  fi
  ensure_local_registry_mirror_config "${FUGUE_REGISTRY_PULL_BASE}" "${effective_cluster_join_registry_endpoint}"
  wait_for_local_kube_api_ready
  ${KUBECTL} version --client >/dev/null
  helm status "${FUGUE_RELEASE_NAME}" -n "${FUGUE_NAMESPACE}" >/dev/null
  validate_control_plane_singleton_anchor
  prepare_release_domains
  if node_local_dns_split_release_enabled; then
    [[ "${FUGUE_IMAGE_CACHE_ENABLED}" == "true" ]] || fail "image-cache cannot be disabled while a preserved offline node exists"
    daemonset_exists "${FUGUE_RELEASE_FULLNAME}-image-cache" || fail "live image-cache DaemonSet is required while a preserved offline node exists"
  fi
  capture_pre_deploy_robustness_baseline

  PREVIOUS_REVISION="$(helm_current_revision)"
  [[ -n "${PREVIOUS_REVISION}" ]] || fail "failed to detect current Helm revision"
  if ! run_control_plane_rollback_image_preflight; then
    fail "rollback image preflight failed"
  fi

  log "upgrading ${FUGUE_RELEASE_NAME} in namespace ${FUGUE_NAMESPACE}"
  log "api image: ${FUGUE_API_IMAGE_REPOSITORY}:${FUGUE_API_IMAGE_TAG}"
  log "controller image: ${FUGUE_CONTROLLER_IMAGE_REPOSITORY}:${FUGUE_CONTROLLER_IMAGE_TAG}"
  log "strict drain: mode=${FUGUE_STRICT_DRAIN_MODE} timeout=${FUGUE_STRICT_DRAIN_TIMEOUT_SECONDS}s min_ready=${FUGUE_STRICT_DRAIN_MIN_READY_SECONDS}s agent=${FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY}:${FUGUE_DRAIN_AGENT_IMAGE_TAG}"
  log "telemetry agent image: ${FUGUE_TELEMETRY_AGENT_IMAGE_REPOSITORY}:${FUGUE_TELEMETRY_AGENT_IMAGE_TAG} enabled=${FUGUE_TELEMETRY_AGENT_ENABLED} observability=${FUGUE_OBSERVABILITY_ENABLED} retention=${FUGUE_OBSERVABILITY_RETENTION}"
  log "telemetry Kubernetes logs: enabled=${FUGUE_OBSERVABILITY_KUBERNETES_LOGS_ENABLED} namespaces=${FUGUE_OBSERVABILITY_KUBERNETES_LOG_NAMESPACES:-${FUGUE_NAMESPACE}} prefixes=${FUGUE_OBSERVABILITY_KUBERNETES_LOG_NAMESPACE_PREFIXES:-<none>} poll=${FUGUE_OBSERVABILITY_KUBERNETES_LOG_POLL_INTERVAL} tail=${FUGUE_OBSERVABILITY_KUBERNETES_LOG_TAIL_LINES} max_lines=${FUGUE_OBSERVABILITY_KUBERNETES_LOG_MAX_LINES_PER_CYCLE} queue=${FUGUE_OBSERVABILITY_QUEUE_SIZE} batch=${FUGUE_OBSERVABILITY_BATCH_SIZE} memory_limit_bytes=${FUGUE_OBSERVABILITY_MEMORY_LIMIT_BYTES}"
  log "observability metrics plane: enabled=${FUGUE_OBSERVABILITY_METRICS_ENABLED} image=${FUGUE_OBSERVABILITY_METRICS_IMAGE_REPOSITORY}:${FUGUE_OBSERVABILITY_METRICS_IMAGE_TAG} retention=${FUGUE_OBSERVABILITY_METRICS_RETENTION}"
  log "observability alerts plane: enabled=${FUGUE_OBSERVABILITY_ALERTS_ENABLED} image=${FUGUE_OBSERVABILITY_ALERTS_IMAGE_REPOSITORY}:${FUGUE_OBSERVABILITY_ALERTS_IMAGE_TAG} webhook=$([[ -n "$(trim_field "${FUGUE_OBSERVABILITY_ALERTS_WEBHOOK_URL}")" ]] && printf configured || printf '<none>')"
  log "observability logs plane: enabled=${FUGUE_OBSERVABILITY_LOGS_ENABLED} image=${FUGUE_OBSERVABILITY_LOGS_IMAGE_REPOSITORY}:${FUGUE_OBSERVABILITY_LOGS_IMAGE_TAG} retention=${FUGUE_OBSERVABILITY_LOGS_RETENTION}"
  log "observability analytics plane: enabled=${FUGUE_OBSERVABILITY_ANALYTICS_ENABLED} image=${FUGUE_OBSERVABILITY_ANALYTICS_IMAGE_REPOSITORY}:${FUGUE_OBSERVABILITY_ANALYTICS_IMAGE_TAG} retention=${FUGUE_OBSERVABILITY_ANALYTICS_RETENTION}"
  log "observability exporter secret: $([[ -n "$(trim_field "${FUGUE_OBSERVABILITY_EXPORTER_SECRET_NAME}")" ]] && printf '%s' "${FUGUE_OBSERVABILITY_EXPORTER_SECRET_NAME}" || printf '<none>')"
  log "image cache image: ${FUGUE_IMAGE_CACHE_IMAGE_REPOSITORY}:${FUGUE_IMAGE_CACHE_IMAGE_TAG} enabled=${FUGUE_IMAGE_CACHE_ENABLED}"
  log "edge image: ${FUGUE_EDGE_IMAGE_REPOSITORY}:${FUGUE_EDGE_IMAGE_TAG} enabled=${FUGUE_EDGE_ENABLED} edge_group_id=${FUGUE_EDGE_GROUP_ID:-<empty>}"
  log "edge caddy: enabled=${FUGUE_EDGE_CADDY_ENABLED} listen=${FUGUE_EDGE_CADDY_LISTEN_ADDR} tls_mode=${FUGUE_EDGE_CADDY_TLS_MODE} public_hostports=${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED} http=${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTP} https=${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTPS} static_tls=${FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED} static_tls_secret=${FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME:-<none>}"
  log "control-plane postgres: legacy_enabled=${FUGUE_POSTGRES_ENABLED} cnpg_enabled=${FUGUE_CONTROL_PLANE_POSTGRES_ENABLED} cnpg_use_for_api=${FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API} cnpg_instances=${FUGUE_CONTROL_PLANE_POSTGRES_INSTANCES}"
  log "control-plane singletons: enabled=${FUGUE_CONTROL_PLANE_SINGLETONS_ENABLED} selector=${FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR:-<none>}"
  log "edge scheduling: primary_country=${FUGUE_EDGE_NODE_SELECTOR_COUNTRY_CODE:-<none>} public_ipv4=${FUGUE_EDGE_PUBLIC_IPV4:-<none>} extra_groups=${FUGUE_EDGE_EXTRA_GROUPS:-<none>}"
  log "previous Helm revision: ${PREVIOUS_REVISION}"
  log "registry push base: ${FUGUE_REGISTRY_PUSH_BASE}"
  log "registry pull base: ${FUGUE_REGISTRY_PULL_BASE}"
  log "cluster join registry endpoint: ${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT}"
  log "cluster join server fallbacks: ${FUGUE_CLUSTER_JOIN_SERVER_FALLBACKS:-<none>}"
  log "cluster join k3s version: ${FUGUE_CLUSTER_JOIN_K3S_VERSION:-<none>}"
  log "cluster join mesh provider: ${FUGUE_CLUSTER_JOIN_MESH_PROVIDER:-<none>}"
  log "cluster join mesh login server: ${FUGUE_CLUSTER_JOIN_MESH_LOGIN_SERVER:-<none>}"
  log "cluster join mesh auth key: $([[ -n "$(trim_field "${FUGUE_CLUSTER_JOIN_MESH_AUTH_KEY:-}")" ]] && printf configured || printf '<none>')"
  log "app base domain: ${FUGUE_APP_BASE_DOMAIN}"
  log "custom domain base domain: dns.${FUGUE_APP_BASE_DOMAIN}"
  log "dns shadow: enabled=${FUGUE_DNS_ENABLED} zone=${FUGUE_DNS_ZONE} extra_zones=${FUGUE_DNS_EXTRA_ZONES:-<none>} answer_ips=${FUGUE_DNS_ANSWER_IPS:-<none>} route_a_answer_ips=${FUGUE_DNS_ROUTE_A_ANSWER_IPS:-<none>} nameservers=${FUGUE_DNS_NAMESERVERS:-<none>} static_records=$([[ -n "$(trim_field "${FUGUE_DNS_STATIC_RECORDS_JSON}")" ]] && printf enabled || printf disabled) platform_routes=$([[ -n "$(trim_field "${FUGUE_PLATFORM_ROUTES_JSON}")" ]] && printf enabled || printf disabled) edge_quality_ranking=${FUGUE_EDGE_QUALITY_RANKING_MODE} public_hostports=${FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED} udp=${FUGUE_DNS_UDP_ADDR} tcp=${FUGUE_DNS_TCP_ADDR}"
  log "dns scheduling: primary_country=${FUGUE_DNS_NODE_SELECTOR_COUNTRY_CODE:-<none>} extra_groups=${FUGUE_DNS_EXTRA_GROUPS:-<none>}"
  log "mesh recovery: enabled=${FUGUE_MESH_RECOVERY_ENABLED} generation=${FUGUE_MESH_RECOVERY_GENERATION} mode=${FUGUE_MESH_RECOVERY_MODE} login_server=${FUGUE_MESH_RECOVERY_LOGIN_SERVER:-<none>}"
  log "shared workspace storage: enabled=${FUGUE_SHARED_WORKSPACE_STORAGE_ENABLED} class=${FUGUE_SHARED_WORKSPACE_STORAGE_CLASS}"

  recover_primary_node_if_needed
  relieve_primary_disk_pressure
  recover_primary_postgres_if_needed
  restore_primary_mesh_network_if_needed
  ensure_coredns_multinode_scheduling
  validate_dns_public_host_port_targets
  prepare_node_local_dns_helm_args
  if [[ "${FUGUE_NODE_LOCAL_DNS_ENABLED}" == "true" && "${FUGUE_NODE_LOCAL_DNS_MODE}" == "shadow" ]]; then
    if ! node_local_dns_shadow_host_preflight "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}"; then
      fail "NodeLocal DNSCache shadow host preflight failed before Helm mutation"
    fi
  elif [[ "${FUGUE_NODE_LOCAL_DNS_ENABLED}" != "true" && "${NODE_LOCAL_DNS_PREVIOUS_ENABLED}" == "true" ]]; then
    if ! node_local_dns_delete_daemonset_safely "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}"; then
      fail "NodeLocal DNSCache could not be safely removed before deleting its upstream resources"
    fi
  fi

  apply_chart_crds

  if [[ "${FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED}" == "true" ]]; then
    log "checking edge static TLS secret ${FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME} for the 7-day release safety horizon"
    if ! bash ./scripts/issue_fugue_app_wildcard_tls.sh \
      --namespace "${FUGUE_NAMESPACE}" \
      --secret-name "${FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME}" \
      --domain "${FUGUE_APP_BASE_DOMAIN}" \
      --check-only \
      --renew-before-days 7; then
      fail "edge static TLS certificate preflight failed"
    fi
  fi

  write_upgrade_override_values
  upgrade_override_values_file="${UPGRADE_OVERRIDE_VALUES_FILE}"
  build_dns_helm_set_args
  prepare_helm_post_renderer
  drain_control_plane_backup_before_schema_rollout
  if ! prepare_dns_manifest_transaction; then
    fail "DNS manifest transaction preflight failed before Helm mutation"
  fi
  if node_local_dns_split_release_enabled; then
    [[ "${FUGUE_IMAGE_CACHE_ENABLED}" == "true" ]] || fail "image-cache cannot be disabled while a preserved offline node exists"
    daemonset_exists "${FUGUE_RELEASE_FULLNAME}-image-cache" || fail "live image-cache DaemonSet is required while a preserved offline node exists"
    if ! image_cache_prepare_offline_safe_rollout "${FUGUE_RELEASE_FULLNAME}-image-cache"; then
      fail "image-cache offline-node guard failed before Helm mutation"
    fi
    NODE_LOCAL_BUILD_PLANE_HELM_SET_ARGS+=(--set-string imageCache.updateStrategy.type=OnDelete)
  elif [[ "${FUGUE_IMAGE_CACHE_ENABLED}" == "true" ]]; then
    # --reset-then-reuse-values retains the last release override. Clear the
    # split-cohort OnDelete guard explicitly once no preserved node remains.
    NODE_LOCAL_BUILD_PLANE_HELM_SET_ARGS+=(--set-string imageCache.updateStrategy.type=RollingUpdate)
  fi
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
    "${HELM_POST_RENDERER_ARGS[@]}" \
    -f "${upgrade_override_values_file}" \
    "${HEADSCALE_HELM_SET_ARGS[@]}" \
    "${DNS_HELM_SET_ARGS[@]}" \
    "${NODE_LOCAL_DNS_HELM_SET_ARGS[@]}" \
    "${PUBLIC_DATA_PLANE_HELM_SET_ARGS[@]}" \
    "${NODE_LOCAL_BUILD_PLANE_HELM_SET_ARGS[@]}" \
    "${MAINTENANCE_AGENT_HELM_SET_ARGS[@]}" \
    --set-string api.image.repository="${FUGUE_API_IMAGE_REPOSITORY}" \
    --set-string api.image.tag="${FUGUE_API_IMAGE_TAG}" \
    --set-string controller.image.repository="${FUGUE_CONTROLLER_IMAGE_REPOSITORY}" \
    --set-string controller.image.tag="${FUGUE_CONTROLLER_IMAGE_TAG}" \
    --set-string runtime.strictDrain.mode="${FUGUE_STRICT_DRAIN_MODE}" \
    --set runtime.strictDrain.timeoutSeconds="${FUGUE_STRICT_DRAIN_TIMEOUT_SECONDS}" \
    --set runtime.strictDrain.terminationGraceBufferSeconds="${FUGUE_STRICT_DRAIN_TERMINATION_GRACE_BUFFER_SECONDS}" \
    --set runtime.strictDrain.minReadySeconds="${FUGUE_STRICT_DRAIN_MIN_READY_SECONDS}" \
    --set runtime.strictDrain.quietPeriodSeconds="${FUGUE_STRICT_DRAIN_QUIET_PERIOD_SECONDS}" \
    --set runtime.strictDrain.pollIntervalMilliseconds="${FUGUE_STRICT_DRAIN_POLL_INTERVAL_MS}" \
    --set runtime.strictDrain.nativeSidecarEnabled="${FUGUE_STRICT_DRAIN_NATIVE_SIDECAR_ENABLED}" \
    --set runtime.strictDrain.agent.port="${FUGUE_DRAIN_AGENT_PORT}" \
    --set-string runtime.strictDrain.agent.image.repository="${FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY}" \
    --set-string runtime.strictDrain.agent.image.tag="${FUGUE_DRAIN_AGENT_IMAGE_TAG}" \
    --set-string runtime.strictDrain.agent.image.digest="${FUGUE_DRAIN_AGENT_IMAGE_DIGEST}" \
    --set-string runtime.strictDrain.agent.image.pullPolicy="${FUGUE_DRAIN_AGENT_IMAGE_PULL_POLICY}" \
    --set observability.enabled="${FUGUE_OBSERVABILITY_ENABLED}" \
    --set-string observability.retention="${FUGUE_OBSERVABILITY_RETENTION}" \
    --set-string observability.exporterSecret.existingSecretName="${FUGUE_OBSERVABILITY_EXPORTER_SECRET_NAME}" \
    --set-string observability.identity.tenantID="${FUGUE_OBSERVABILITY_TENANT_ID}" \
    --set-string observability.identity.projectID="${FUGUE_OBSERVABILITY_PROJECT_ID}" \
    --set-string observability.identity.appID="${FUGUE_OBSERVABILITY_APP_ID}" \
    --set-string observability.identity.runtimeID="${FUGUE_OBSERVABILITY_RUNTIME_ID}" \
    --set-string observability.identity.component="${FUGUE_OBSERVABILITY_COMPONENT}" \
    --set observability.metrics.enabled="${FUGUE_OBSERVABILITY_METRICS_ENABLED}" \
    --set-string observability.metrics.image.repository="${FUGUE_OBSERVABILITY_METRICS_IMAGE_REPOSITORY}" \
    --set-string observability.metrics.image.tag="${FUGUE_OBSERVABILITY_METRICS_IMAGE_TAG}" \
    --set-string observability.metrics.retention="${FUGUE_OBSERVABILITY_METRICS_RETENTION}" \
    --set-string observability.metrics.scrapeInterval="${FUGUE_OBSERVABILITY_METRICS_SCRAPE_INTERVAL}" \
    --set-string observability.metrics.evaluationInterval="${FUGUE_OBSERVABILITY_METRICS_EVALUATION_INTERVAL}" \
    --set observability.alerts.enabled="${FUGUE_OBSERVABILITY_ALERTS_ENABLED}" \
    --set-string observability.alerts.image.repository="${FUGUE_OBSERVABILITY_ALERTS_IMAGE_REPOSITORY}" \
    --set-string observability.alerts.image.tag="${FUGUE_OBSERVABILITY_ALERTS_IMAGE_TAG}" \
    --set-string observability.alerts.webhookURL="${FUGUE_OBSERVABILITY_ALERTS_WEBHOOK_URL}" \
    --set observability.logs.enabled="${FUGUE_OBSERVABILITY_LOGS_ENABLED}" \
    --set-string observability.logs.image.repository="${FUGUE_OBSERVABILITY_LOGS_IMAGE_REPOSITORY}" \
    --set-string observability.logs.image.tag="${FUGUE_OBSERVABILITY_LOGS_IMAGE_TAG}" \
    --set-string observability.logs.retention="${FUGUE_OBSERVABILITY_LOGS_RETENTION}" \
    --set observability.analytics.enabled="${FUGUE_OBSERVABILITY_ANALYTICS_ENABLED}" \
    --set-string observability.analytics.image.repository="${FUGUE_OBSERVABILITY_ANALYTICS_IMAGE_REPOSITORY}" \
    --set-string observability.analytics.image.tag="${FUGUE_OBSERVABILITY_ANALYTICS_IMAGE_TAG}" \
    --set-string observability.analytics.retention="${FUGUE_OBSERVABILITY_ANALYTICS_RETENTION}" \
    --set observability.agent.enabled="${FUGUE_TELEMETRY_AGENT_ENABLED}" \
    --set-string observability.agent.image.repository="${FUGUE_TELEMETRY_AGENT_IMAGE_REPOSITORY}" \
    --set-string observability.agent.image.tag="${FUGUE_TELEMETRY_AGENT_IMAGE_TAG}" \
    --set-string observability.agent.runtimeLogPaths="${FUGUE_OBSERVABILITY_RUNTIME_LOG_PATHS}" \
    --set-string observability.agent.prometheusScrapeURLs="${FUGUE_OBSERVABILITY_PROMETHEUS_SCRAPE_URLS}" \
    --set-string observability.agent.scrapeInterval="${FUGUE_OBSERVABILITY_SCRAPE_INTERVAL}" \
    --set observability.agent.kubernetesLogs.enabled="${FUGUE_OBSERVABILITY_KUBERNETES_LOGS_ENABLED}" \
    --set-string observability.agent.kubernetesLogs.namespaces="$(helm_set_string_value "${FUGUE_OBSERVABILITY_KUBERNETES_LOG_NAMESPACES}")" \
    --set-string observability.agent.kubernetesLogs.namespacePrefixes="$(helm_set_string_value "${FUGUE_OBSERVABILITY_KUBERNETES_LOG_NAMESPACE_PREFIXES}")" \
    --set-string observability.agent.kubernetesLogs.labelSelector="$(helm_set_string_value "${FUGUE_OBSERVABILITY_KUBERNETES_LOG_LABEL_SELECTOR}")" \
    --set-string observability.agent.kubernetesLogs.pollInterval="${FUGUE_OBSERVABILITY_KUBERNETES_LOG_POLL_INTERVAL}" \
    --set-string observability.agent.kubernetesLogs.tailLines="${FUGUE_OBSERVABILITY_KUBERNETES_LOG_TAIL_LINES}" \
    --set-string observability.agent.kubernetesLogs.maxPods="${FUGUE_OBSERVABILITY_KUBERNETES_LOG_MAX_PODS}" \
    --set-string observability.agent.kubernetesLogs.maxLinesPerCycle="${FUGUE_OBSERVABILITY_KUBERNETES_LOG_MAX_LINES_PER_CYCLE}" \
    --set-string observability.agent.queueSize="${FUGUE_OBSERVABILITY_QUEUE_SIZE}" \
    --set-string observability.agent.batchSize="${FUGUE_OBSERVABILITY_BATCH_SIZE}" \
    --set-string observability.agent.maxPayloadBytes="${FUGUE_OBSERVABILITY_MAX_PAYLOAD_BYTES}" \
    --set-string observability.agent.memoryLimitBytes="${FUGUE_OBSERVABILITY_MEMORY_LIMIT_BYTES}" \
    --set-string observability.agent.retryMaxAttempts="${FUGUE_OBSERVABILITY_RETRY_MAX_ATTEMPTS}" \
    --set-string imageStore.mode="${FUGUE_IMAGE_STORE_MODE}" \
    --set imageStore.minReplicas="${FUGUE_IMAGE_STORE_MIN_REPLICAS}" \
    --set imageStore.targetReplicas="${FUGUE_IMAGE_STORE_TARGET_REPLICAS}" \
    --set-string imageStore.schedulerInterval="${FUGUE_IMAGE_STORE_SCHEDULER_INTERVAL}" \
    --set-string imageStore.replicaLeaseTTL="${FUGUE_IMAGE_STORE_REPLICA_LEASE_TTL}" \
    --set-string imageStore.verifyInterval="${FUGUE_IMAGE_STORE_VERIFY_INTERVAL}" \
    --set imageStore.prune.enabled="${FUGUE_IMAGE_STORE_PRUNE_ENABLED}" \
    --set-string imageStore.prune.maxDeleteBytesPerRun="${FUGUE_IMAGE_STORE_PRUNE_MAX_DELETE_BYTES_PER_RUN}" \
    --set imageStore.imageCacheInventory.enabled="${FUGUE_IMAGE_CACHE_INVENTORY_ENABLED}" \
    --set-string imageStore.imageCacheInventory.interval="${FUGUE_IMAGE_CACHE_INVENTORY_INTERVAL}" \
    --set-string imageStore.imageCacheInventory.ttl="${FUGUE_IMAGE_CACHE_INVENTORY_TTL}" \
    --set-string imageStore.orphanPrune.mode="${FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MODE}" \
    --set-string imageStore.orphanPrune.gracePeriod="${FUGUE_IMAGE_STORE_ORPHAN_PRUNE_GRACE_PERIOD}" \
    --set imageStore.orphanPrune.maxTargetsPerNode="${FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MAX_TARGETS_PER_NODE}" \
    --set-string imageStore.orphanPrune.maxDeleteBytesPerNode="${FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MAX_DELETE_BYTES_PER_NODE}" \
    --set imageStore.orphanPrune.minReplicaCount="${FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MIN_REPLICA_COUNT}" \
    --set imageCache.enabled="${FUGUE_IMAGE_CACHE_ENABLED}" \
    --set imageCache.port="${FUGUE_IMAGE_CACHE_PORT}" \
    --set-string imageCache.image.repository="${FUGUE_IMAGE_CACHE_IMAGE_REPOSITORY}" \
    --set-string imageCache.image.tag="${FUGUE_IMAGE_CACHE_IMAGE_TAG}" \
    --set-string imageCache.registryBase="${FUGUE_REGISTRY_PULL_BASE}" \
    --set-string imageCache.upstreamBase="$(helm_set_string_value "${FUGUE_IMAGE_CACHE_UPSTREAM_BASE:-}")" \
    --set registry.enabled="${FUGUE_REGISTRY_ENABLED}" \
    --set registryJanitor.enabled="${FUGUE_REGISTRY_JANITOR_ENABLED}" \
    --set registryGC.enabled="${FUGUE_REGISTRY_GC_ENABLED}" \
    --set edge.enabled="${FUGUE_EDGE_ENABLED}" \
    --set-string edge.image.repository="${FUGUE_EDGE_HELM_IMAGE_REPOSITORY:-${FUGUE_EDGE_IMAGE_REPOSITORY}}" \
    --set-string edge.image.tag="${FUGUE_EDGE_HELM_IMAGE_TAG:-${FUGUE_EDGE_IMAGE_TAG}}" \
    --set-string edge.edgeGroupID="${FUGUE_EDGE_GROUP_ID}" \
    --set-string edge.region="${FUGUE_EDGE_REGION}" \
    --set-string edge.country="${FUGUE_EDGE_COUNTRY}" \
    --set-string edge.publicHostname="${FUGUE_EDGE_PUBLIC_HOSTNAME}" \
    --set-string edge.publicIPv4="${FUGUE_EDGE_PUBLIC_IPV4}" \
    --set-string edge.publicIPv6="${FUGUE_EDGE_PUBLIC_IPV6}" \
    --set-string edge.meshIP="${FUGUE_EDGE_MESH_IP}" \
    --set edge.caddy.enabled="${FUGUE_EDGE_CADDY_ENABLED}" \
    --set-string edge.caddy.listenAddr="${FUGUE_EDGE_CADDY_LISTEN_ADDR}" \
    --set-string edge.caddy.tlsMode="${FUGUE_EDGE_CADDY_TLS_MODE}" \
    --set edge.caddy.publicHostPorts.enabled="${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORTS_ENABLED}" \
    --set edge.caddy.publicHostPorts.http="${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTP}" \
    --set edge.caddy.publicHostPorts.https="${FUGUE_EDGE_CADDY_PUBLIC_HOSTPORT_HTTPS}" \
    --set edge.caddy.staticTLS.enabled="${FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED}" \
    --set-string edge.caddy.staticTLS.secretName="${FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME}" \
    --set-string edge.caddy.staticTLS.mountPath="${FUGUE_EDGE_CADDY_STATIC_TLS_MOUNT_PATH}" \
    --set-string edge.caddy.staticTLS.certificateKey="${FUGUE_EDGE_CADDY_STATIC_TLS_CERTIFICATE_KEY}" \
    --set-string edge.caddy.staticTLS.privateKeyKey="${FUGUE_EDGE_CADDY_STATIC_TLS_PRIVATE_KEY_KEY}" \
    --set-string api.appBaseDomain="${FUGUE_APP_BASE_DOMAIN}" \
    --set-string api.apiPublicDomain="${FUGUE_API_PUBLIC_DOMAIN}" \
    --set-string api.databaseURL="${FUGUE_API_DATABASE_URL}" \
    --set-string api.edgeQualityRankingMode="${FUGUE_EDGE_QUALITY_RANKING_MODE}" \
    --set-string api.registryPushBase="${FUGUE_REGISTRY_PUSH_BASE}" \
    --set-string api.registryPullBase="${FUGUE_REGISTRY_PULL_BASE}" \
    --set-string api.clusterJoinRegistryEndpoint="${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT}" \
    --set-string api.clusterJoinServerFallbacks="$(helm_set_string_value "${FUGUE_CLUSTER_JOIN_SERVER_FALLBACKS}")" \
    --set-string api.clusterJoinK3SVersion="$(helm_set_string_value "${FUGUE_CLUSTER_JOIN_K3S_VERSION:-}")" \
    --set-string api.clusterJoinMeshProvider="$(helm_set_string_value "${FUGUE_CLUSTER_JOIN_MESH_PROVIDER:-}")" \
    --set-string api.clusterJoinMeshLoginServer="$(helm_set_string_value "${FUGUE_CLUSTER_JOIN_MESH_LOGIN_SERVER:-}")" \
    --set-string api.clusterJoinMeshAuthKey="$(helm_set_string_value "${FUGUE_CLUSTER_JOIN_MESH_AUTH_KEY:-}")" \
    --set-string api.dataBackend.provider="${FUGUE_DATA_BACKEND_PROVIDER}" \
    --set-string api.dataBackend.bucket="${FUGUE_DATA_BACKEND_BUCKET}" \
    --set-string api.dataBackend.region="${FUGUE_DATA_BACKEND_REGION}" \
    --set-string api.dataBackend.endpoint="${FUGUE_DATA_BACKEND_ENDPOINT}" \
    --set-string api.dataBackend.accountID="${FUGUE_DATA_R2_ACCOUNT_ID}" \
    --set-string api.dataBackend.prefix="${FUGUE_DATA_BACKEND_PREFIX}" \
    --set-string api.dataBackend.accessKeyID="${FUGUE_DATA_BACKEND_ACCESS_KEY_ID}" \
    --set-string api.dataBackend.secretAccessKey="${FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY}" \
    --set-string api.dataBackend.sessionToken="${FUGUE_DATA_BACKEND_SESSION_TOKEN}" \
    --set-string api.dataBackend.credentialEncryptionKey="${FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY}" \
    --set-string api.dataBackend.presignTTL="${FUGUE_DATA_PRESIGN_TTL}" \
    --set api.replicaCount="${FUGUE_API_REPLICA_COUNT}" \
    --set api.hostNetwork=false \
    --set api.minReadySeconds=5 \
    --set api.terminationGracePeriodSeconds=40 \
    --set api.podDisruptionBudget.enabled=true \
    --set api.podDisruptionBudget.minAvailable=2 \
    --set-string api.shutdownDrainDelay=5s \
    --set-string api.shutdownTimeout=25s \
    --set controller.replicaCount="${FUGUE_CONTROLLER_REPLICA_COUNT}" \
    --set-string controller.pollInterval=15s \
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
    --set-string controller.migrationGuard.checkInterval=2s \
    --set postgres.enabled="${FUGUE_POSTGRES_ENABLED}" \
    --set controlPlanePostgres.enabled="${FUGUE_CONTROL_PLANE_POSTGRES_ENABLED}" \
    --set controlPlanePostgres.useForAPI="${FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API}" \
    --set-string controlPlanePostgres.name="${FUGUE_CONTROL_PLANE_POSTGRES_NAME}" \
    --set-string controlPlanePostgres.imageName="${FUGUE_CONTROL_PLANE_POSTGRES_IMAGE_NAME}" \
    --set controlPlanePostgres.instances="${FUGUE_CONTROL_PLANE_POSTGRES_INSTANCES}" \
    --set-string controlPlanePostgres.storage.size="${FUGUE_CONTROL_PLANE_POSTGRES_STORAGE_SIZE}" \
    --set-string controlPlanePostgres.storage.storageClassName="${FUGUE_CONTROL_PLANE_POSTGRES_STORAGE_CLASS}" \
    --set-string controlPlanePostgres.existingSecretName="${FUGUE_CONTROL_PLANE_POSTGRES_EXISTING_SECRET_NAME}" \
    --set sharedWorkspaceStorage.enabled="${FUGUE_SHARED_WORKSPACE_STORAGE_ENABLED}" \
    --set-string sharedWorkspaceStorage.storageClassName="${FUGUE_SHARED_WORKSPACE_STORAGE_CLASS}" \
    --set-string sharedWorkspaceStorage.server.clusterIP="${FUGUE_SHARED_WORKSPACE_NFS_CLUSTER_IP}"; then
    log "helm upgrade failed; attempting rollback"
    rollback_release || true
    fail "helm upgrade failed"
  fi

  if ! run_dns_manifest_transaction_after_helm; then
    log "DNS manifest OnDelete transaction failed; attempting complete rollback"
    rollback_release || true
    fail "DNS manifest OnDelete transaction failed"
  fi

  if [[ "${FUGUE_NODE_LOCAL_DNS_ENABLED}" == "true" ]]; then
    if ! node_local_dns_reconcile_after_helm; then
      log "NodeLocal DNSCache rollout verification failed; attempting rollback"
      rollback_release || true
      fail "NodeLocal DNSCache rollout verification failed"
    fi
    NODE_LOCAL_DNS_RELEASED="true"
    if node_local_dns_split_release_enabled; then
      if ! platform_autonomy_status_summary >/dev/null; then
        log "NodeLocal DNSCache post-reconcile offline-preserve policy gate failed; attempting rollback"
        rollback_release || true
        fail "NodeLocal DNSCache post-reconcile offline-preserve policy gate failed"
      fi
    fi
  elif [[ "${NODE_LOCAL_DNS_PREVIOUS_ENABLED}" == "true" ]]; then
    if ! node_local_dns_verify_teardown "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}"; then
      log "NodeLocal DNSCache teardown verification failed; attempting rollback"
      rollback_release || true
      fail "NodeLocal DNSCache teardown verification failed"
    fi
  fi

  patch_control_plane_singleton_deployments

  force_delete_release_pods_on_unhealthy_nodes
  cleanup_orphaned_regional_daemonsets

  if ! control_plane_canary_readiness_gate; then
    log "control-plane canary gate failed; attempting rollback"
    rollback_release || true
    fail "control-plane canary gate failed"
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

  for singleton in \
    "${FUGUE_REGISTRY_DEPLOYMENT_NAME}" \
    "${FUGUE_HEADSCALE_DEPLOYMENT_NAME}" \
    "${FUGUE_SHARED_WORKSPACE_NFS_DEPLOYMENT_NAME}" \
    "${FUGUE_SHARED_WORKSPACE_PROVISIONER_DEPLOYMENT_NAME}"; do
    if deployment_exists "${singleton}"; then
      if skip_singleton_rollout_wait_for_node_local_override "${singleton}"; then
        log "skipping registry singleton rollout wait because node-local build-plane preflight override accepted the pre-existing registry/node_policy degradation"
        continue
      fi
      log "waiting for isolated singleton dependency ${singleton}"
      if ! rollout_status "${singleton}"; then
        log "${singleton} rollout check failed; attempting rollback"
        rollback_release || true
        fail "${singleton} rollout failed"
      fi
    fi
  done

  if [[ "${FUGUE_EDGE_ENABLED}" == "true" ]]; then
    if ! public_data_plane_daemonset_rollout_wait_required; then
      log "skipping edge daemonset rollout wait because public data-plane DaemonSet templates were preserved from live state"
    elif ! rollout_daemonsets_by_component_prefix "edge" "edge"; then
      log "edge rollout check failed; attempting rollback"
      rollback_release || true
      fail "edge rollout failed"
    fi
  fi

  if [[ "${FUGUE_DNS_ENABLED}" == "true" ]]; then
    if ! public_data_plane_daemonset_rollout_wait_required; then
      log "skipping dns daemonset rollout wait because public data-plane DaemonSet templates were preserved from live state"
    elif ! rollout_daemonsets_by_component_prefix "dns" "dns"; then
      log "dns rollout check failed; attempting rollback"
      rollback_release || true
      fail "dns rollout failed"
    fi
  fi

  if [[ "${FUGUE_IMAGE_CACHE_ENABLED}" == "true" ]] && daemonset_exists "${FUGUE_RELEASE_FULLNAME}-image-cache"; then
    if ! image_cache_rollout_status "${FUGUE_RELEASE_FULLNAME}-image-cache"; then
      log "image cache rollout check failed; attempting rollback"
      rollback_release || true
      fail "image cache rollout failed"
    fi
  fi

  if [[ "${FUGUE_MESH_RECOVERY_ENABLED}" == "true" ]] && daemonset_exists "${FUGUE_RELEASE_FULLNAME}-mesh-recovery"; then
    if ! rollout_daemonset_status "${FUGUE_RELEASE_FULLNAME}-mesh-recovery"; then
      log "mesh recovery rollout check failed; attempting rollback"
      rollback_release || true
      fail "mesh recovery rollout failed"
    fi
  fi

  if daemonset_exists "${FUGUE_RELEASE_FULLNAME}-node-janitor"; then
    if rollout_daemonset_status "${FUGUE_RELEASE_FULLNAME}-node-janitor"; then
      ensure_host_time_sync
    else
      log "warning: node-janitor rollout check failed; control-plane host time sync hardening may remain pending"
    fi
  fi

  label_default_builder_nodes

  sync_route_a_edge_proxy

  if ! retry "${FUGUE_SMOKE_RETRIES}" "${FUGUE_SMOKE_DELAY_SECONDS}" smoke_test; then
    log "smoke test failed; attempting rollback"
    rollback_release || true
    fail "smoke test failed"
  fi

  if ! wait_for_post_deploy_robustness; then
    log "post-deploy robustness gate failed; attempting rollback"
    rollback_release || true
    fail "post-deploy robustness gate failed"
  fi

  if ! wait_for_release_safety_watch_windows; then
    log "release safety watch window failed; attempting rollback"
    rollback_release || true
    fail "release safety watch window failed"
  fi

  release_public_data_plane_if_needed
  if node_local_dns_split_release_enabled && [[ "${PUBLIC_DATA_PLANE_RELEASED:-false}" != "true" ]]; then
    if ! wait_for_platform_autonomy_after_public_data_plane_release; then
      log "post-NodeLocal-DNS offline-preserve autonomy gate failed; attempting rollback"
      rollback_release || true
      fail "post-NodeLocal-DNS offline-preserve autonomy gate failed"
    fi
  fi
  if [[ "${PUBLIC_DATA_PLANE_RELEASED:-false}" == "true" ]]; then
    if ! wait_for_platform_autonomy_after_public_data_plane_release; then
      log "post-public-data-plane autonomy gate failed; attempting rollback"
      rollback_release || true
      fail "post-public-data-plane autonomy gate failed"
    fi
    if ! wait_for_post_deploy_robustness; then
      log "post-public-data-plane robustness gate failed; attempting rollback"
      rollback_release || true
      fail "post-public-data-plane robustness gate failed"
    fi
  fi

  if ! finalize_dns_manifest_transaction; then
    log "DNS manifest transaction finalization failed; attempting rollback"
    rollback_release || true
    fail "DNS manifest transaction finalization failed"
  fi

  local current_revision
  current_revision="$(helm_current_revision)"
  log "upgrade complete; current Helm revision=${current_revision}"
}

if [[ "${FUGUE_UPGRADE_LIB_ONLY:-false}" == "true" ]]; then
  return 0 2>/dev/null || exit 0
fi

if [[ "${FUGUE_CONTROL_PLANE_OBSERVABILITY_ONLY:-false}" == "true" ]]; then
  export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"
  KUBECTL="${KUBECTL:-$(detect_kubectl)}"
  export KUBECTL
  ensure_control_plane_observability
  exit 0
fi

main "$@"
