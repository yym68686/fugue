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
  changed_files="$(git -C "${REPO_ROOT}" diff --name-only "${live_tag}" "${target_tag}")"
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
      public:internal/proxyproto/*|\
      public:internal/dnsserver/*|\
      public:internal/model/edge_routes.go|\
      public:Dockerfile.edge|\
      public:deploy/helm/fugue/templates/edge-*|\
      public:deploy/helm/fugue/templates/dns-*|\
      public:deploy/helm/fugue/templates/_helpers.tpl|\
      public:deploy/helm/fugue/values.yaml|\
      public:deploy/helm/fugue/values-production-ha.yaml|\
      public:scripts/render_fugue_edge_systemd_unit.sh|\
      public:scripts/render_fugue_dns_systemd_unit.sh|\
      public:scripts/release_fugue_public_data_plane.sh)
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
      internal/model/edge_routes.go|\
      internal/proxyproto/*|\
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
      internal/model/edge_routes.go|\
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
  if [[ -n "${ref}" && "${ref}" != "0000000000000000000000000000000000000000" ]] &&
    git -C "${REPO_ROOT}" cat-file -e "${ref}^{commit}" 2>/dev/null; then
    printf '%s\n' "${ref}"
    return 0
  fi

  git -C "${REPO_ROOT}" rev-parse --verify HEAD^ 2>/dev/null
}

release_diff_new_ref() {
  local ref="${FUGUE_RELEASE_AFTER_SHA:-${AFTER_SHA:-${GITHUB_SHA:-}}}"

  ref="$(trim_field "${ref}")"
  if [[ -n "${ref}" ]] && git -C "${REPO_ROOT}" cat-file -e "${ref}^{commit}" 2>/dev/null; then
    printf '%s\n' "${ref}"
    return 0
  fi

  git -C "${REPO_ROOT}" rev-parse --verify HEAD 2>/dev/null
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
	      scripts/build_control_plane_images.sh|\
	      scripts/compute_control_plane_image_build_plan.sh|\
	      scripts/resolve_control_plane_live_images.sh|\
	      scripts/upgrade_fugue_control_plane.sh|\
	      scripts/test_release_domain_safety.sh)
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

  if daemonset_exists "${edge_front_ds}" && daemonset_exists "${FUGUE_RELEASE_FULLNAME}-edge-worker-a" && daemonset_exists "${FUGUE_RELEASE_FULLNAME}-edge-worker-b"; then
    log "public data-plane blue/green DaemonSets detected; preserving front and per-slot worker templates from live state"
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

  preserve_image_from_live_daemonset "public data-plane" "${edge_ds}" "edge" FUGUE_EDGE_IMAGE_REPOSITORY FUGUE_EDGE_IMAGE_TAG || true
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
  local image_cache_ds="${FUGUE_RELEASE_FULLNAME}-image-cache"
  local image_cache_resources

  NODE_LOCAL_BUILD_PLANE_HELM_SET_ARGS=()

  if [[ "${preserve_image}" == "true" ]]; then
    preserve_image_from_live_daemonset "node-local build-plane" "${image_cache_ds}" "image-cache" FUGUE_IMAGE_CACHE_IMAGE_REPOSITORY FUGUE_IMAGE_CACHE_IMAGE_TAG || true
  fi

  if [[ "${preserve_resources}" == "true" ]]; then
    image_cache_resources="$(trim_field "$(live_daemonset_container_resources_json "${image_cache_ds}" "image-cache")")"
    if [[ -n "${image_cache_resources}" ]]; then
      NODE_LOCAL_BUILD_PLANE_HELM_SET_ARGS+=(--set-json "imageCache.resources=${image_cache_resources}")
      log "node-local build-plane image-cache resources preserved from live ${image_cache_ds}/image-cache"
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
  local strategy
  strategy="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o jsonpath='{.spec.updateStrategy.type}' 2>/dev/null || true)"
  strategy="${strategy:-RollingUpdate}"

  if [[ "${strategy}" == "RollingUpdate" ]]; then
    ${KUBECTL} -n "${FUGUE_NAMESPACE}" rollout status "ds/${daemonset_name}" --timeout="${FUGUE_ROLLOUT_TIMEOUT}"
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
    status="$(${KUBECTL} -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o jsonpath='{.metadata.generation}{"\t"}{.status.observedGeneration}{"\t"}{.status.desiredNumberScheduled}{"\t"}{.status.numberReady}{"\t"}{.status.numberUnavailable}' 2>/dev/null || true)"
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
}

cleanup_tmp_artifacts() {
  cleanup_control_plane_automation_tmp
  cleanup_upgrade_override_values
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

  discovery_headers="$(mktemp)"
  discovery_body="$(mktemp)"
  discovery_http_status="$(curl -sS -D "${discovery_headers}" -H "Authorization: Bearer ${token}" "${api_base}/v1/discovery/bundle" -o "${discovery_body}" -w '%{http_code}' || true)"
  case "${discovery_http_status}" in
    200|204|304)
      ;;
    *)
      if release_preflight_missing_discovery_bootstrap_allowed && [[ "${discovery_http_status}" =~ ^(000|404|405|501)$ ]]; then
        log "release preflight bootstrap: DiscoveryBundle endpoint unavailable (HTTP ${discovery_http_status}); continuing with explicit runtime values"
        rm -f "${discovery_headers}" "${discovery_body}"
        return 0
      fi
      fail "DiscoveryBundle preflight failed with HTTP ${discovery_http_status:-unknown}"
      ;;
  esac
  discovery_etag="$(awk 'tolower($1) == "etag:" {print $2; exit}' "${discovery_headers}" | tr -d '\r')"
  if [[ -z "$(trim_field "${discovery_etag}")" ]]; then
    if release_preflight_missing_discovery_bootstrap_allowed && [[ ! -s "${discovery_body}" ]]; then
      log "release preflight bootstrap: DiscoveryBundle endpoint returned an empty response without an ETag; continuing with explicit runtime values"
      rm -f "${discovery_headers}" "${discovery_body}"
      return 0
    fi
    fail "DiscoveryBundle preflight did not return an ETag"
  fi
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

  autonomy_status_file="$(mktemp)"
  curl -fsS -H "Authorization: Bearer ${token}" "${api_base}/v1/admin/platform/autonomy/status" -o "${autonomy_status_file}"
  edge_nodes_file="$(mktemp)"
  curl -fsS -H "Authorization: Bearer ${token}" "${api_base}/v1/edge/nodes" -o "${edge_nodes_file}"
  dns_nodes_file="$(mktemp)"
  curl -fsS -H "Authorization: Bearer ${token}" "${api_base}/v1/dns/nodes" -o "${dns_nodes_file}"
  node_policies_file="$(mktemp)"
  if ! curl -fsS -H "Authorization: Bearer ${token}" "${api_base}/v1/cluster/node-policies/status" -o "${node_policies_file}"; then
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
  if node_local_build_plane_preflight_override_allowed; then
    node_local_build_plane_override_allowed="true"
  fi
  if ! autonomy_override_message="$(python3 - "${autonomy_status_file}" "${edge_nodes_file}" "${dns_nodes_file}" "${node_policies_file}" "${node_local_build_plane_override_allowed}" "${image_cache_status_file}" "${FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT}" "${FUGUE_REGISTRY_PULL_BASE}" <<'PY'
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

def image_cache_daemonset_ready(path, endpoint, registry_pull_base):
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
    if desired <= 0:
        return False, "image-cache daemonset has no scheduled nodes"
    if misscheduled > 0:
        return False, f"image-cache daemonset has {misscheduled} misscheduled pods"
    if ready < desired or available < desired or updated < desired:
        return False, f"image-cache daemonset not ready: ready={ready} available={available} updated={updated} desired={desired}"
    return True, f"image-cache daemonset ready: ready={ready} available={available} desired={desired}"

status_path = sys.argv[1]
nodes_path = sys.argv[2]
dns_nodes_path = sys.argv[3]
node_policies_path = sys.argv[4]
node_local_build_plane_override_allowed = trim(sys.argv[5]).lower() == "true"
image_cache_status_path = sys.argv[6]
cluster_join_registry_endpoint = sys.argv[7]
registry_pull_base = sys.argv[8]
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
image_cache_override, image_cache_message = image_cache_daemonset_ready(image_cache_status_path, cluster_join_registry_endpoint, registry_pull_base)
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
if set(failing_checks).issubset(allowed_checks) and bootstrap_override and (not any(name == "dns" for name in failing_checks) or dns_bootstrap_override):
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
  local entry name edge_group country_code answer_ips token_secret answer_ip

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
    IFS='|' read -r name edge_group country_code answer_ips token_secret _ <<<"${entry}"
    name="$(trim_field "${name}")"
    edge_group="$(trim_field "${edge_group}")"
    country_code="$(trim_field "${country_code}")"
    answer_ips="$(trim_field "${answer_ips}")"
    token_secret="$(trim_field "${token_secret}")"
    if [[ -z "${name}" || -z "${edge_group}" || -z "${country_code}" || -z "${answer_ips}" || -z "${token_secret}" ]]; then
      fail "FUGUE_DNS_EXTRA_GROUPS entries must be name|edge_group_id|country_code|answer_ips|token_secret_name"
    fi
    if [[ "$(dns_answer_ip_count "${answer_ips}")" == "0" ]]; then
      fail "FUGUE_DNS_EXTRA_GROUPS entry ${name} must contain at least one answer IP"
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
    printf '      answerIPs:\n'
    while IFS= read -r answer_ip; do
      printf '        - %s\n' "$(yaml_quote "${answer_ip}")"
    done < <(dns_answer_ips_lines "${answer_ips}")
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

prepare_release_domains() {
  local public_mode build_mode stateful_mode maintenance_mode

  refresh_release_changed_files_from_live_api

  public_mode="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_MODE:-auto}"
  build_mode="${FUGUE_NODE_LOCAL_BUILD_PLANE_RELEASE_MODE:-auto}"
  stateful_mode="${FUGUE_STATEFUL_DEPENDENCY_RELEASE_MODE:-guard}"
  maintenance_mode="${FUGUE_MAINTENANCE_AGENT_RELEASE_MODE:-preserve}"

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
    if node_local_build_plane_manifest_changed; then
      fail "node-local build-plane manifests changed; ship image-cache through an isolated worker/front release before changing its rendered pod spec"
    fi
    if node_local_build_plane_image_rollout_allowed; then
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
  local worker_changed="false"
  local front_changed="false"
  local dns_changed="false"
  PUBLIC_DATA_PLANE_RELEASED="false"

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

platform_autonomy_status_summary() {
  local api_base token status_file rc

  api_base="$(release_api_base_url)"
  token="$(release_api_token)"
  status_file="$(mktemp)"
  if ! curl -fsS -H "Authorization: Bearer ${token}" "${api_base}/v1/admin/platform/autonomy/status" -o "${status_file}"; then
    rm -f "${status_file}"
    printf 'platform autonomy status request failed'
    return 1
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

release_guard_status_summary() {
  local api_base token status_file rc

  api_base="$(release_api_base_url)"
  token="$(release_api_token)"
  status_file="$(mktemp)"
  if ! curl -fsS -H "Authorization: Bearer ${token}" "${api_base}/v1/admin/release-guard/status" -o "${status_file}"; then
    rm -f "${status_file}"
    printf 'release guard status request failed'
    return 1
  fi
  python3 - "${status_file}" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as fh:
    payload = json.load(fh)
status = payload.get("status") or {}
blocked = status.get("blocked_reasons") or []
print(
    "pass={pass_} block_rollout={block} artifact_failures={artifact_failures} "
    "consumer_drift={consumer_drift} failure_contracts={contracts} blockers={blockers}".format(
        pass_=str(bool(status.get("pass"))).lower(),
        block=str(bool(status.get("block_rollout"))).lower(),
        artifact_failures=int(status.get("platform_artifact_validation_failures") or 0),
        consumer_drift=int(status.get("platform_consumer_drift") or 0),
        contracts=int(status.get("failure_contract_count") or 0),
        blockers="; ".join(str(item) for item in blocked) if blocked else "none",
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
  if ! curl -fsS -H "Authorization: Bearer ${token}" "${api_base}/v1/admin/robustness/status" -o "${status_file}"; then
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
  if ! curl -fsS -H "Authorization: Bearer ${token}" "${api_base}/v1/admin/robustness/status" -o "${status_file}"; then
    rm -f "${status_file}"
    printf 'robustness status request failed'
    return 1
  fi
  python3 - "${status_file}" "${baseline_file}" <<'PY'
import json
import os
import sys

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

baseline_status = None
baseline_check_names = set()
baseline_incident_ids = set()
baseline_incident_keys = set()
baseline_blocker_keys = set()
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
            if (
                not check.get("pass")
                and stable_text(check.get("severity")) == "block_publish"
                and not release_gate_ignored_tenant_workload(check)
            ):
                key = check_key(check)
                if key:
                    baseline_blocker_keys.add(key)

checks = status.get("checks") or []
incidents = status.get("incidents") or []
raw_block_rollout = bool(status.get("block_rollout"))
blockers = []
new_blockers = []
introduced_blockers = []
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
    block_rollout = bool(new_blockers or new_blocking_incidents)
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
    if new_blockers or new_blocking_incidents:
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
      else
        log "warning: post-deploy release guard summary unavailable or blocked: ${release_output:-unknown}"
      fi
      return 0
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
  FUGUE_DNS_GEOIP_OVERRIDES_JSON="${FUGUE_DNS_GEOIP_OVERRIDES_JSON:-}"
  FUGUE_DNS_TOKEN_SECRET_NAME="${FUGUE_DNS_TOKEN_SECRET_NAME:-${FUGUE_EDGE_TOKEN_SECRET_NAME}}"
  if [[ -n "$(trim_field "${FUGUE_DNS_TOKEN_SECRET_NAME}")" ]]; then
    FUGUE_DNS_TOKEN_SECRET_KEY="${FUGUE_DNS_TOKEN_SECRET_KEY:-FUGUE_DNS_TOKEN}"
  else
    FUGUE_DNS_TOKEN_SECRET_KEY="${FUGUE_DNS_TOKEN_SECRET_KEY:-FUGUE_EDGE_TLS_ASK_TOKEN}"
  fi
  FUGUE_DNS_NAMESERVERS="${FUGUE_DNS_NAMESERVERS:-}"
  FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED="${FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED:-false}"
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
  capture_pre_deploy_robustness_baseline

  PREVIOUS_REVISION="$(helm_current_revision)"
  [[ -n "${PREVIOUS_REVISION}" ]] || fail "failed to detect current Helm revision"

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
  log "dns shadow: enabled=${FUGUE_DNS_ENABLED} zone=${FUGUE_DNS_ZONE} answer_ips=${FUGUE_DNS_ANSWER_IPS:-<none>} route_a_answer_ips=${FUGUE_DNS_ROUTE_A_ANSWER_IPS:-<none>} nameservers=${FUGUE_DNS_NAMESERVERS:-<none>} static_records=$([[ -n "$(trim_field "${FUGUE_DNS_STATIC_RECORDS_JSON}")" ]] && printf enabled || printf disabled) platform_routes=$([[ -n "$(trim_field "${FUGUE_PLATFORM_ROUTES_JSON}")" ]] && printf enabled || printf disabled) edge_quality_ranking=${FUGUE_EDGE_QUALITY_RANKING_MODE} public_hostports=${FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED} udp=${FUGUE_DNS_UDP_ADDR} tcp=${FUGUE_DNS_TCP_ADDR}"
  log "dns scheduling: primary_country=${FUGUE_DNS_NODE_SELECTOR_COUNTRY_CODE:-<none>} extra_groups=${FUGUE_DNS_EXTRA_GROUPS:-<none>}"
  log "mesh recovery: enabled=${FUGUE_MESH_RECOVERY_ENABLED} generation=${FUGUE_MESH_RECOVERY_GENERATION} mode=${FUGUE_MESH_RECOVERY_MODE} login_server=${FUGUE_MESH_RECOVERY_LOGIN_SERVER:-<none>}"
  log "shared workspace storage: enabled=${FUGUE_SHARED_WORKSPACE_STORAGE_ENABLED} class=${FUGUE_SHARED_WORKSPACE_STORAGE_CLASS}"

  recover_primary_node_if_needed
  relieve_primary_disk_pressure
  recover_primary_postgres_if_needed
  restore_primary_mesh_network_if_needed
  ensure_coredns_multinode_scheduling

  apply_chart_crds

  if [[ "${FUGUE_EDGE_CADDY_STATIC_TLS_ENABLED}" == "true" ]]; then
    log "renewing edge static TLS secret ${FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME} if needed"
    if ! bash ./scripts/issue_fugue_app_wildcard_tls.sh \
      --dns-provider fugue \
      --api-url "$(release_api_base_url)" \
      --api-key "$(release_api_token)" \
      --namespace "${FUGUE_NAMESPACE}" \
      --secret-name "${FUGUE_EDGE_CADDY_STATIC_TLS_SECRET_NAME}" \
      --domain "${FUGUE_APP_BASE_DOMAIN}" \
      --renew-before-days 30; then
      fail "edge static TLS renewal failed"
    fi
  fi

  write_upgrade_override_values
  upgrade_override_values_file="${UPGRADE_OVERRIDE_VALUES_FILE}"
  build_dns_helm_set_args
  prepare_helm_post_renderer
  drain_control_plane_backup_before_schema_rollout
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
    --set-string edge.image.repository="${FUGUE_EDGE_IMAGE_REPOSITORY}" \
    --set-string edge.image.tag="${FUGUE_EDGE_IMAGE_TAG}" \
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
    if ! rollout_daemonset_status "${FUGUE_RELEASE_FULLNAME}-image-cache"; then
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

  release_public_data_plane_if_needed
  if [[ "${PUBLIC_DATA_PLANE_RELEASED:-false}" == "true" ]]; then
    wait_for_platform_autonomy_after_public_data_plane_release
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
