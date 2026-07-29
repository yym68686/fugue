package api

// hostMemorySafetyShellLibrary is shared by the cluster join installer and the
// node updater. Keep this fragment side-effect free: callers explicitly run
// plan, stage, and activate so k3s can accept host swap before zram is enabled.
func hostMemorySafetyShellLibrary() string {
	return `
FUGUE_HOST_ZRAM_MODE="${FUGUE_HOST_ZRAM_MODE:-auto}"
FUGUE_HOST_ZRAM_PERCENT="${FUGUE_HOST_ZRAM_PERCENT:-25}"
FUGUE_HOST_ZRAM_MIN_NODE_BYTES="${FUGUE_HOST_ZRAM_MIN_NODE_BYTES:-4294967296}"
FUGUE_HOST_ZRAM_MIN_BYTES="${FUGUE_HOST_ZRAM_MIN_BYTES:-1073741824}"
FUGUE_HOST_ZRAM_MAX_BYTES="${FUGUE_HOST_ZRAM_MAX_BYTES:-4294967296}"
FUGUE_HOST_ZRAM_ROUND_BYTES="${FUGUE_HOST_ZRAM_ROUND_BYTES:-268435456}"
FUGUE_HOST_ZRAM_PRIORITY="${FUGUE_HOST_ZRAM_PRIORITY:-100}"
FUGUE_HOST_ZRAM_HARD_MIN_NODE_BYTES="4294967296"
FUGUE_HOST_ZRAM_HARD_MAX_BYTES="4294967296"
FUGUE_HOST_ZRAM_HARD_MAX_PERCENT="50"
FUGUE_HOST_ZRAM_MEMINFO="${FUGUE_HOST_ZRAM_MEMINFO:-/proc/meminfo}"
FUGUE_HOST_ZRAM_PROC_SWAPS="${FUGUE_HOST_ZRAM_PROC_SWAPS:-/proc/swaps}"
FUGUE_HOST_ZRAM_CGROUP_CONTROLLERS="${FUGUE_HOST_ZRAM_CGROUP_CONTROLLERS:-/sys/fs/cgroup/cgroup.controllers}"
FUGUE_HOST_ZRAM_DEVICE="${FUGUE_HOST_ZRAM_DEVICE:-/dev/zram0}"
FUGUE_HOST_ZRAM_SYS_BLOCK="${FUGUE_HOST_ZRAM_SYS_BLOCK:-/sys/block/zram0}"
FUGUE_HOST_ZRAM_HELPER="${FUGUE_HOST_ZRAM_HELPER:-/usr/local/sbin/fugue-host-zram}"
FUGUE_HOST_ZRAM_ENV_FILE="${FUGUE_HOST_ZRAM_ENV_FILE:-/etc/fugue/host-zram.env}"
FUGUE_HOST_ZRAM_UNIT_FILE="${FUGUE_HOST_ZRAM_UNIT_FILE:-/etc/systemd/system/fugue-host-zram.service}"
FUGUE_HOST_ZRAM_SYSTEMD_RUNTIME_DIR="${FUGUE_HOST_ZRAM_SYSTEMD_RUNTIME_DIR:-/run/systemd/system}"
FUGUE_HOST_ZRAM_HOST_ETC="${FUGUE_HOST_ZRAM_HOST_ETC:-/etc}"
FUGUE_HOST_ZRAM_HOST_USR_LIB="${FUGUE_HOST_ZRAM_HOST_USR_LIB:-/usr/lib}"
FUGUE_HOST_ZRAM_ELIGIBLE="false"
FUGUE_HOST_ZRAM_STATE="not-planned"
FUGUE_HOST_ZRAM_REASON=""
FUGUE_HOST_ZRAM_SIZE_BYTES="0"
FUGUE_HOST_ZRAM_STAGED="false"
FUGUE_HOST_ZRAM_RESTART_NEEDED="false"
FUGUE_HOST_ZRAM_ROLLBACK_DIR=""
FUGUE_HOST_ZRAM_PREVIOUS_UNIT_ACTIVE="false"
FUGUE_HOST_ZRAM_PREVIOUS_UNIT_ENABLED="false"

fugue_host_zram_set_state() {
  FUGUE_HOST_ZRAM_STATE="$1"
  FUGUE_HOST_ZRAM_REASON="${2:-}"
}

fugue_host_zram_positive_integer() {
  case "${1:-}" in
    ''|*[!0-9]*|0)
      return 1
      ;;
    *)
      return 0
      ;;
  esac
}

fugue_host_zram_k3s_minor() {
  local raw="${FUGUE_HOST_ZRAM_K3S_VERSION:-${FUGUE_JOIN_K3S_VERSION:-${FUGUE_K3S_VERSION:-}}}"
  if [ -z "${raw}" ] && command -v k3s >/dev/null 2>&1; then
    raw="$(k3s --version 2>/dev/null | head -n 1 || true)"
  fi
  printf '%s\n' "${raw}" | sed -n 's/.*v1\.\([0-9][0-9]*\).*/\1/p' | head -n 1
}

fugue_host_zram_is_managed() {
  [ -r "${FUGUE_HOST_ZRAM_ENV_FILE}" ] &&
    grep -Eq '^FUGUE_HOST_ZRAM_MANAGED=(1|true)$' "${FUGUE_HOST_ZRAM_ENV_FILE}"
}

fugue_host_zram_swap_active() {
  [ -r "${FUGUE_HOST_ZRAM_PROC_SWAPS}" ] || return 1
  awk -v device="${FUGUE_HOST_ZRAM_DEVICE}" 'NR > 1 && $1 == device { found = 1 } END { exit(found ? 0 : 1) }' "${FUGUE_HOST_ZRAM_PROC_SWAPS}"
}

fugue_host_zram_plan() {
  local total_kib=""
  local total_bytes=""
  local desired_bytes=""
  local minor=""
  local foreign_swap=""
  local current_disksize="0"

  FUGUE_HOST_ZRAM_ELIGIBLE="false"
  FUGUE_HOST_ZRAM_SIZE_BYTES="0"
  fugue_host_zram_set_state "skipped" "not evaluated"

  case "${FUGUE_HOST_ZRAM_MODE}" in
    auto)
      ;;
    off|disabled|false|0)
      fugue_host_zram_set_state "disabled" "disabled by FUGUE_HOST_ZRAM_MODE"
      return 1
      ;;
    *)
      fugue_host_zram_set_state "skipped" "invalid FUGUE_HOST_ZRAM_MODE=${FUGUE_HOST_ZRAM_MODE}"
      return 1
      ;;
  esac
  if [ "$(id -u)" -ne 0 ]; then
    fugue_host_zram_set_state "skipped" "root privileges are required"
    return 1
  fi
  if [ ! -d "${FUGUE_HOST_ZRAM_SYSTEMD_RUNTIME_DIR}" ] || ! command -v systemctl >/dev/null 2>&1; then
    fugue_host_zram_set_state "skipped" "systemd is unavailable"
    return 1
  fi
  if [ ! -r "${FUGUE_HOST_ZRAM_CGROUP_CONTROLLERS}" ]; then
    fugue_host_zram_set_state "skipped" "cgroup v2 is unavailable"
    return 1
  fi
  minor="$(fugue_host_zram_k3s_minor || true)"
  if ! fugue_host_zram_positive_integer "${minor}" || [ "${minor}" -lt 34 ]; then
    fugue_host_zram_set_state "skipped" "k3s/Kubernetes 1.34 or newer is required"
    return 1
  fi
  for command_name in modprobe mkswap swapon swapoff; do
    if ! command -v "${command_name}" >/dev/null 2>&1; then
      fugue_host_zram_set_state "skipped" "missing command: ${command_name}"
      return 1
    fi
  done
  if [ ! -d /sys/module/zram ] && [ ! -d /sys/class/zram-control ] && [ ! -e "${FUGUE_HOST_ZRAM_SYS_BLOCK}/disksize" ]; then
    if ! modprobe -n zram >/dev/null 2>&1; then
      fugue_host_zram_set_state "skipped" "zram kernel support is unavailable"
      return 1
    fi
  fi
  if [ ! -r "${FUGUE_HOST_ZRAM_MEMINFO}" ]; then
    fugue_host_zram_set_state "skipped" "memory inventory is unavailable"
    return 1
  fi
  total_kib="$(awk '/^MemTotal:/ {print $2; exit}' "${FUGUE_HOST_ZRAM_MEMINFO}" 2>/dev/null || true)"
  if ! fugue_host_zram_positive_integer "${total_kib}"; then
    fugue_host_zram_set_state "skipped" "memory inventory is invalid"
    return 1
  fi
  total_bytes=$((total_kib * 1024))
  for value in "${FUGUE_HOST_ZRAM_PERCENT}" "${FUGUE_HOST_ZRAM_MIN_NODE_BYTES}" "${FUGUE_HOST_ZRAM_MIN_BYTES}" "${FUGUE_HOST_ZRAM_MAX_BYTES}" "${FUGUE_HOST_ZRAM_ROUND_BYTES}" "${FUGUE_HOST_ZRAM_PRIORITY}"; do
    if ! fugue_host_zram_positive_integer "${value}"; then
      fugue_host_zram_set_state "skipped" "invalid zram sizing policy"
      return 1
    fi
  done
  if [ "${FUGUE_HOST_ZRAM_PERCENT}" -gt "${FUGUE_HOST_ZRAM_HARD_MAX_PERCENT}" ] ||
    [ "${FUGUE_HOST_ZRAM_MIN_NODE_BYTES}" -lt "${FUGUE_HOST_ZRAM_HARD_MIN_NODE_BYTES}" ] ||
    [ "${FUGUE_HOST_ZRAM_MAX_BYTES}" -gt "${FUGUE_HOST_ZRAM_HARD_MAX_BYTES}" ] ||
    [ "${FUGUE_HOST_ZRAM_MIN_BYTES}" -gt "${FUGUE_HOST_ZRAM_MAX_BYTES}" ]; then
    fugue_host_zram_set_state "skipped" "invalid zram sizing bounds"
    return 1
  fi
  if [ "${total_bytes}" -lt "${FUGUE_HOST_ZRAM_MIN_NODE_BYTES}" ]; then
    fugue_host_zram_set_state "skipped" "node memory is below the automatic zram threshold"
    return 1
  fi

  if ! fugue_host_zram_is_managed; then
    for managed_path in "${FUGUE_HOST_ZRAM_HELPER}" "${FUGUE_HOST_ZRAM_ENV_FILE}" "${FUGUE_HOST_ZRAM_UNIT_FILE}"; do
      if [ -e "${managed_path}" ]; then
        fugue_host_zram_set_state "skipped" "existing zram artifact is not managed by Fugue: ${managed_path}"
        return 1
      fi
    done
    for foreign_config in \
      "${FUGUE_HOST_ZRAM_HOST_ETC}/systemd/zram-generator.conf" \
      "${FUGUE_HOST_ZRAM_HOST_USR_LIB}/systemd/zram-generator.conf" \
      "${FUGUE_HOST_ZRAM_HOST_ETC}/default/zramswap" \
      "${FUGUE_HOST_ZRAM_HOST_ETC}"/systemd/zram-generator.conf.d/*.conf; do
      if [ -e "${foreign_config}" ]; then
        fugue_host_zram_set_state "skipped" "another zram manager is configured: ${foreign_config}"
        return 1
      fi
    done
    for foreign_unit in zramswap.service zram-swap.service zram0.service; do
      if systemctl is-active --quiet "${foreign_unit}" >/dev/null 2>&1 || systemctl is-enabled "${foreign_unit}" >/dev/null 2>&1; then
        fugue_host_zram_set_state "skipped" "another zram manager owns ${foreign_unit}"
        return 1
      fi
    done
    if systemctl is-active --quiet systemd-zram-setup@zram0.service >/dev/null 2>&1; then
      fugue_host_zram_set_state "skipped" "systemd zram generator owns zram0"
      return 1
    fi
  fi

  if [ -r "${FUGUE_HOST_ZRAM_PROC_SWAPS}" ]; then
    foreign_swap="$(awk -v device="${FUGUE_HOST_ZRAM_DEVICE}" 'NR > 1 && $1 != device { print $1; exit }' "${FUGUE_HOST_ZRAM_PROC_SWAPS}" 2>/dev/null || true)"
    if [ -n "${foreign_swap}" ]; then
      fugue_host_zram_set_state "skipped" "existing non-Fugue swap is active: ${foreign_swap}"
      return 1
    fi
    if fugue_host_zram_swap_active && ! fugue_host_zram_is_managed; then
      fugue_host_zram_set_state "skipped" "zram0 is active but is not managed by Fugue"
      return 1
    fi
  fi
  if [ -r "${FUGUE_HOST_ZRAM_SYS_BLOCK}/disksize" ]; then
    current_disksize="$(cat "${FUGUE_HOST_ZRAM_SYS_BLOCK}/disksize" 2>/dev/null || printf '0')"
    case "${current_disksize}" in
      ''|*[!0-9]*) current_disksize=0 ;;
    esac
    if [ "${current_disksize}" -gt 0 ] && ! fugue_host_zram_is_managed; then
      fugue_host_zram_set_state "skipped" "zram0 is configured but is not managed by Fugue"
      return 1
    fi
  fi

  desired_bytes=$((total_bytes * FUGUE_HOST_ZRAM_PERCENT / 100))
  if [ "${desired_bytes}" -lt "${FUGUE_HOST_ZRAM_MIN_BYTES}" ]; then
    desired_bytes="${FUGUE_HOST_ZRAM_MIN_BYTES}"
  fi
  if [ "${desired_bytes}" -gt "${FUGUE_HOST_ZRAM_MAX_BYTES}" ]; then
    desired_bytes="${FUGUE_HOST_ZRAM_MAX_BYTES}"
  fi
  desired_bytes=$((((desired_bytes + FUGUE_HOST_ZRAM_ROUND_BYTES / 2) / FUGUE_HOST_ZRAM_ROUND_BYTES) * FUGUE_HOST_ZRAM_ROUND_BYTES))
  if [ "${desired_bytes}" -gt "${FUGUE_HOST_ZRAM_MAX_BYTES}" ]; then
    desired_bytes="${FUGUE_HOST_ZRAM_MAX_BYTES}"
  fi

  FUGUE_HOST_ZRAM_SIZE_BYTES="${desired_bytes}"
  FUGUE_HOST_ZRAM_ELIGIBLE="true"
  fugue_host_zram_set_state "planned" "compatible host"
  return 0
}

fugue_host_zram_backup_file() {
  local path="$1"
  local key="$2"
  if [ -e "${path}" ]; then
    if ! cp -p "${path}" "${FUGUE_HOST_ZRAM_ROLLBACK_DIR}/${key}"; then
      return 1
    fi
    printf 'present\n' >"${FUGUE_HOST_ZRAM_ROLLBACK_DIR}/${key}.state" || return 1
  else
    printf 'absent\n' >"${FUGUE_HOST_ZRAM_ROLLBACK_DIR}/${key}.state" || return 1
  fi
}

fugue_host_zram_restore_file() {
  local path="$1"
  local key="$2"
  local state=""
  state="$(cat "${FUGUE_HOST_ZRAM_ROLLBACK_DIR}/${key}.state" 2>/dev/null || true)"
  case "${state}" in
    present)
      mkdir -p "$(dirname "${path}")" || return 1
      cp -p "${FUGUE_HOST_ZRAM_ROLLBACK_DIR}/${key}" "${path}"
      ;;
    absent)
      rm -f "${path}"
      ;;
    *)
      return 1
      ;;
  esac
}

fugue_host_zram_install_file() {
  local source_path="$1"
  local target_path="$2"
  local mode="$3"
  local target_dir=""
  local staged_path=""
  if [ -f "${target_path}" ] && cmp -s "${source_path}" "${target_path}"; then
    rm -f "${source_path}"
    return 1
  fi
  target_dir="$(dirname "${target_path}")" || return 2
  mkdir -p "${target_dir}" || return 2
  staged_path="$(mktemp "${target_dir}/.fugue-zram.XXXXXX")" || return 2
  if ! install -m "${mode}" "${source_path}" "${staged_path}"; then
    rm -f "${staged_path}"
    return 2
  fi
  if ! mv -f "${staged_path}" "${target_path}"; then
    rm -f "${staged_path}"
    return 2
  fi
  rm -f "${source_path}" || true
  return 0
}

fugue_host_zram_stage() {
  local helper_tmp=""
  local env_tmp=""
  local unit_tmp=""
  local unit_name=""
  local env_changed=0
  local install_rc=0

  if [ "${FUGUE_HOST_ZRAM_ELIGIBLE}" != "true" ]; then
    fugue_host_zram_set_state "failed" "zram was not planned"
    return 1
  fi
  unit_name="$(basename "${FUGUE_HOST_ZRAM_UNIT_FILE}")" || {
    fugue_host_zram_set_state "failed" "failed to resolve the Fugue zram unit name"
    return 1
  }
  FUGUE_HOST_ZRAM_ROLLBACK_DIR="$(mktemp -d)" || {
    fugue_host_zram_set_state "failed" "failed to create a zram rollback directory"
    return 1
  }
  FUGUE_HOST_ZRAM_PREVIOUS_UNIT_ACTIVE="false"
  FUGUE_HOST_ZRAM_PREVIOUS_UNIT_ENABLED="false"
  if systemctl is-active --quiet "${unit_name}" >/dev/null 2>&1; then
    FUGUE_HOST_ZRAM_PREVIOUS_UNIT_ACTIVE="true"
  fi
  if systemctl is-enabled "${unit_name}" >/dev/null 2>&1; then
    FUGUE_HOST_ZRAM_PREVIOUS_UNIT_ENABLED="true"
  fi
  if ! fugue_host_zram_backup_file "${FUGUE_HOST_ZRAM_HELPER}" helper ||
    ! fugue_host_zram_backup_file "${FUGUE_HOST_ZRAM_ENV_FILE}" env ||
    ! fugue_host_zram_backup_file "${FUGUE_HOST_ZRAM_UNIT_FILE}" unit; then
    fugue_host_zram_set_state "failed" "failed to snapshot existing zram files"
    rm -rf "${FUGUE_HOST_ZRAM_ROLLBACK_DIR}"
    FUGUE_HOST_ZRAM_ROLLBACK_DIR=""
    return 1
  fi

  helper_tmp="$(mktemp)" || {
    fugue_host_zram_set_state "failed" "failed to stage the Fugue zram helper"
    fugue_host_zram_rollback
    return 1
  }
  if ! cat >"${helper_tmp}" <<'FUGUE_ZRAM_HELPER'
#!/usr/bin/env bash
set -euo pipefail

device="${FUGUE_HOST_ZRAM_DEVICE:-/dev/zram0}"
sys_block="${FUGUE_HOST_ZRAM_SYS_BLOCK:-/sys/block/zram0}"
size_bytes="${FUGUE_HOST_ZRAM_SIZE_BYTES:?missing FUGUE_HOST_ZRAM_SIZE_BYTES}"
priority="${FUGUE_HOST_ZRAM_PRIORITY:-100}"
proc_swaps="${FUGUE_HOST_ZRAM_PROC_SWAPS:-/proc/swaps}"

active() {
  [ -r "${proc_swaps}" ] && awk -v device="${device}" 'NR > 1 && $1 == device { found = 1 } END { exit(found ? 0 : 1) }' "${proc_swaps}"
}

reset_device() {
  if [ -w "${sys_block}/reset" ]; then
    printf '1\n' >"${sys_block}/reset" || true
  fi
}

start_zram() {
  local current_size="0"
  local algorithms=""
  local waited=0
  if active; then
    current_size="$(cat "${sys_block}/disksize" 2>/dev/null || printf '0')"
    if [ "${current_size}" != "${size_bytes}" ]; then
      echo "active ${device} size ${current_size} does not match Fugue policy ${size_bytes}" >&2
      return 1
    fi
    return 0
  fi
  modprobe zram num_devices=1 >/dev/null 2>&1 || modprobe zram >/dev/null
  while [ ! -w "${sys_block}/disksize" ] || [ ! -e "${device}" ]; do
    waited=$((waited + 1))
    if [ "${waited}" -ge 10 ]; then
      echo "zram0 did not become available" >&2
      return 1
    fi
    sleep 1
  done
  current_size="$(cat "${sys_block}/disksize" 2>/dev/null || printf '0')"
  if [ "${current_size}" != "0" ]; then
    reset_device
  fi
  if [ -w "${sys_block}/comp_algorithm" ]; then
    algorithms="$(tr '[]' '  ' <"${sys_block}/comp_algorithm" 2>/dev/null || true)"
    if printf '%s\n' "${algorithms}" | grep -qw zstd; then
      printf 'zstd\n' >"${sys_block}/comp_algorithm"
    elif printf '%s\n' "${algorithms}" | grep -qw lz4; then
      printf 'lz4\n' >"${sys_block}/comp_algorithm"
    fi
  fi
  printf '%s\n' "${size_bytes}" >"${sys_block}/disksize"
  if ! mkswap -f -L fugue-zram "${device}" >/dev/null; then
    reset_device
    return 1
  fi
  if ! swapon -p "${priority}" "${device}"; then
    reset_device
    return 1
  fi
}

stop_zram() {
  if active; then
    swapoff "${device}"
  fi
  reset_device
}

case "${1:-start}" in
  start) start_zram ;;
  stop) stop_zram ;;
  status) active ;;
  *) echo "usage: fugue-host-zram [start|stop|status]" >&2; exit 2 ;;
esac
FUGUE_ZRAM_HELPER

  then
    rm -f "${helper_tmp}"
    fugue_host_zram_set_state "failed" "failed to render the Fugue zram helper"
    fugue_host_zram_rollback
    return 1
  fi

  env_tmp="$(mktemp)" || {
    rm -f "${helper_tmp}"
    fugue_host_zram_set_state "failed" "failed to stage the Fugue zram environment"
    fugue_host_zram_rollback
    return 1
  }
  if ! {
    printf 'FUGUE_HOST_ZRAM_MANAGED=true\n'
    printf 'FUGUE_HOST_ZRAM_DEVICE=%s\n' "${FUGUE_HOST_ZRAM_DEVICE}"
    printf 'FUGUE_HOST_ZRAM_SYS_BLOCK=%s\n' "${FUGUE_HOST_ZRAM_SYS_BLOCK}"
    printf 'FUGUE_HOST_ZRAM_SIZE_BYTES=%s\n' "${FUGUE_HOST_ZRAM_SIZE_BYTES}"
    printf 'FUGUE_HOST_ZRAM_PRIORITY=%s\n' "${FUGUE_HOST_ZRAM_PRIORITY}"
    printf 'FUGUE_HOST_ZRAM_PROC_SWAPS=%s\n' "${FUGUE_HOST_ZRAM_PROC_SWAPS}"
  } >"${env_tmp}"; then
    rm -f "${helper_tmp}" "${env_tmp}"
    fugue_host_zram_set_state "failed" "failed to render the Fugue zram environment"
    fugue_host_zram_rollback
    return 1
  fi
  if [ ! -f "${FUGUE_HOST_ZRAM_ENV_FILE}" ] || ! cmp -s "${env_tmp}" "${FUGUE_HOST_ZRAM_ENV_FILE}"; then
    env_changed=1
  fi

  unit_tmp="$(mktemp)" || {
    rm -f "${helper_tmp}" "${env_tmp}"
    fugue_host_zram_set_state "failed" "failed to stage the Fugue zram unit"
    fugue_host_zram_rollback
    return 1
  }
  if ! cat >"${unit_tmp}" <<FUGUE_ZRAM_UNIT
[Unit]
Description=Fugue host-only compressed swap safety net
Documentation=https://kubernetes.io/docs/reference/node/swap-behavior/
After=systemd-modules-load.service
Before=k3s.service k3s-agent.service

[Service]
Type=oneshot
EnvironmentFile=-${FUGUE_HOST_ZRAM_ENV_FILE}
ExecStart=${FUGUE_HOST_ZRAM_HELPER} start
ExecStop=${FUGUE_HOST_ZRAM_HELPER} stop
RemainAfterExit=yes
TimeoutStartSec=30
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
FUGUE_ZRAM_UNIT

  then
    rm -f "${helper_tmp}" "${env_tmp}" "${unit_tmp}"
    fugue_host_zram_set_state "failed" "failed to render the Fugue zram unit"
    fugue_host_zram_rollback
    return 1
  fi

  if fugue_host_zram_install_file "${helper_tmp}" "${FUGUE_HOST_ZRAM_HELPER}" 0755; then
    :
  else
    install_rc=$?
    if [ "${install_rc}" -ne 1 ]; then
      rm -f "${helper_tmp}" "${env_tmp}" "${unit_tmp}"
      fugue_host_zram_set_state "failed" "failed to install the Fugue zram helper"
      fugue_host_zram_rollback
      return 1
    fi
  fi
  if fugue_host_zram_install_file "${env_tmp}" "${FUGUE_HOST_ZRAM_ENV_FILE}" 0600; then
    :
  else
    install_rc=$?
    if [ "${install_rc}" -ne 1 ]; then
      rm -f "${env_tmp}" "${unit_tmp}"
      fugue_host_zram_set_state "failed" "failed to install the Fugue zram environment"
      fugue_host_zram_rollback
      return 1
    fi
  fi
  if fugue_host_zram_install_file "${unit_tmp}" "${FUGUE_HOST_ZRAM_UNIT_FILE}" 0644; then
    :
  else
    install_rc=$?
    if [ "${install_rc}" -ne 1 ]; then
      rm -f "${unit_tmp}"
      fugue_host_zram_set_state "failed" "failed to install the Fugue zram unit"
      fugue_host_zram_rollback
      return 1
    fi
  fi
  if ! systemctl daemon-reload || ! systemctl enable "${unit_name}" >/dev/null; then
    fugue_host_zram_set_state "failed" "failed to install the Fugue zram systemd unit"
    fugue_host_zram_rollback
    return 1
  fi
  if [ "${env_changed}" -eq 1 ]; then
    FUGUE_HOST_ZRAM_RESTART_NEEDED="true"
  fi
  FUGUE_HOST_ZRAM_STAGED="true"
  fugue_host_zram_set_state "staged" "systemd unit installed"
  return 0
}

fugue_host_zram_rollback() {
  local unit_name=""
  local original_reason="${FUGUE_HOST_ZRAM_REASON}"
  local rollback_failed=0
  if [ -z "${FUGUE_HOST_ZRAM_ROLLBACK_DIR}" ] || [ ! -d "${FUGUE_HOST_ZRAM_ROLLBACK_DIR}" ]; then
    return 0
  fi
  unit_name="$(basename "${FUGUE_HOST_ZRAM_UNIT_FILE}")"
  systemctl stop "${unit_name}" >/dev/null 2>&1 || true
  if fugue_host_zram_swap_active && [ -x "${FUGUE_HOST_ZRAM_HELPER}" ] && [ -r "${FUGUE_HOST_ZRAM_ENV_FILE}" ]; then
    (
      set -a
      # shellcheck disable=SC1090
      . "${FUGUE_HOST_ZRAM_ENV_FILE}"
      set +a
      "${FUGUE_HOST_ZRAM_HELPER}" stop
    ) >/dev/null 2>&1 || true
  fi
  if fugue_host_zram_swap_active; then
    fugue_host_zram_set_state "failed" "${original_reason}; rollback could not deactivate ${FUGUE_HOST_ZRAM_DEVICE}"
    return 1
  fi
  systemctl disable "${unit_name}" >/dev/null 2>&1 || rollback_failed=1
  if ! fugue_host_zram_restore_file "${FUGUE_HOST_ZRAM_HELPER}" helper; then
    rollback_failed=1
  fi
  if ! fugue_host_zram_restore_file "${FUGUE_HOST_ZRAM_ENV_FILE}" env; then
    rollback_failed=1
  fi
  if ! fugue_host_zram_restore_file "${FUGUE_HOST_ZRAM_UNIT_FILE}" unit; then
    rollback_failed=1
  fi
  systemctl daemon-reload >/dev/null 2>&1 || rollback_failed=1
  if [ "${FUGUE_HOST_ZRAM_PREVIOUS_UNIT_ENABLED}" = "true" ]; then
    systemctl enable "${unit_name}" >/dev/null 2>&1 || rollback_failed=1
  fi
  if [ "${FUGUE_HOST_ZRAM_PREVIOUS_UNIT_ACTIVE}" = "true" ]; then
    systemctl start "${unit_name}" >/dev/null 2>&1 || rollback_failed=1
  fi
  rm -rf "${FUGUE_HOST_ZRAM_ROLLBACK_DIR}"
  FUGUE_HOST_ZRAM_ROLLBACK_DIR=""
  FUGUE_HOST_ZRAM_STAGED="false"
  if [ "${rollback_failed}" -ne 0 ]; then
    fugue_host_zram_set_state "failed" "${original_reason}; zram file or systemd rollback was incomplete"
    return 1
  fi
  return 0
}

fugue_host_zram_activate() {
  local unit_name=""
  if [ "${FUGUE_HOST_ZRAM_STAGED}" != "true" ]; then
    fugue_host_zram_set_state "failed" "zram systemd unit was not staged"
    return 1
  fi
  unit_name="$(basename "${FUGUE_HOST_ZRAM_UNIT_FILE}")"
  if systemctl is-active --quiet "${unit_name}" >/dev/null 2>&1; then
    if [ "${FUGUE_HOST_ZRAM_RESTART_NEEDED}" = "true" ] && ! systemctl restart "${unit_name}"; then
      fugue_host_zram_set_state "failed" "failed to restart the Fugue zram unit"
      fugue_host_zram_rollback
      return 1
    fi
  elif ! systemctl start "${unit_name}"; then
    fugue_host_zram_set_state "failed" "failed to start the Fugue zram unit"
    fugue_host_zram_rollback
    return 1
  fi
  if ! systemctl is-active --quiet "${unit_name}" || ! fugue_host_zram_swap_active; then
    fugue_host_zram_set_state "failed" "Fugue zram did not become active"
    fugue_host_zram_rollback
    return 1
  fi
  rm -rf "${FUGUE_HOST_ZRAM_ROLLBACK_DIR}"
  FUGUE_HOST_ZRAM_ROLLBACK_DIR=""
  FUGUE_HOST_ZRAM_STAGED="false"
  FUGUE_HOST_ZRAM_RESTART_NEEDED="false"
  fugue_host_zram_set_state "active" "host-only compressed swap is active"
  return 0
}

fugue_k3s_config_ensure_fail_swap_on_false() {
  local file="$1"
  local tmp=""
  local target_dir=""
  local staged_path=""
  if [ -r "${file}" ] && grep -Eq '^[[:space:]]*kubelet-arg:[[:space:]]*[^[:space:]#]' "${file}"; then
    echo "refusing inline kubelet-arg syntax in ${file}; convert it to a YAML list first" >&2
    return 2
  fi
  tmp="$(mktemp)"
  if [ -r "${file}" ]; then
    awk '
      function emit_arg() {
        if (!arg_emitted) {
          print "  - \"fail-swap-on=false\""
          arg_emitted = 1
        }
      }
      /^[[:space:]]*kubelet-arg:[[:space:]]*($|#)/ {
        if (in_block) emit_arg()
        print
        in_block = 1
        saw_block = 1
        arg_emitted = 0
        next
      }
      in_block && /^[^[:space:]]/ {
        emit_arg()
        in_block = 0
      }
      in_block && /fail-swap-on=/ { next }
      { print }
      END {
        if (in_block) emit_arg()
        if (!saw_block) {
          print "kubelet-arg:"
          print "  - \"fail-swap-on=false\""
        }
      }
    ' "${file}" >"${tmp}"
  else
    printf 'kubelet-arg:\n  - "fail-swap-on=false"\n' >"${tmp}"
  fi
  if [ -f "${file}" ] && cmp -s "${tmp}" "${file}"; then
    rm -f "${tmp}"
    return 1
  fi
  target_dir="$(dirname "${file}")"
  mkdir -p "${target_dir}"
  staged_path="$(mktemp "${target_dir}/.fugue-k3s-zram.XXXXXX")"
  install -m 0600 "${tmp}" "${staged_path}"
  mv -f "${staged_path}" "${file}"
  rm -f "${tmp}"
  return 0
}
`
}
