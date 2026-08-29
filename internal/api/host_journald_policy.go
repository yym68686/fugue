package api

// hostJournaldPolicyShellLibrary is embedded in the privileged node updater.
// It keeps the mutation transactional: a configuration that journald cannot
// load is rolled back before the task reports failure.
func hostJournaldPolicyShellLibrary() string {
	return `
FUGUE_JOURNALD_POLICY_FILE="${FUGUE_JOURNALD_POLICY_FILE:-/etc/systemd/journald.conf.d/90-fugue-retention.conf}"
FUGUE_JOURNALD_POLICY_SERVICE="${FUGUE_JOURNALD_POLICY_SERVICE:-systemd-journald.service}"
FUGUE_JOURNALD_POLICY_CHANGED="false"
FUGUE_JOURNALD_POLICY_STATE="not-run"
FUGUE_JOURNALD_POLICY_REASON=""
FUGUE_JOURNALD_POLICY_BEFORE_USAGE="unknown"
FUGUE_JOURNALD_POLICY_AFTER_USAGE="unknown"
FUGUE_JOURNALD_POLICY_EFFECTIVE_MAX_RETENTION_SEC=""
FUGUE_JOURNALD_POLICY_EFFECTIVE_SYSTEM_MAX_USE=""

fugue_journald_policy_set_state() {
  FUGUE_JOURNALD_POLICY_STATE="$1"
  FUGUE_JOURNALD_POLICY_REASON="${2:-}"
}

fugue_journald_policy_validate() {
  local max_retention_sec="${FUGUE_JOURNALD_MAX_RETENTION_SEC:-}"
  local system_max_use="${FUGUE_JOURNALD_SYSTEM_MAX_USE:-}"
  if [[ ! "${max_retention_sec}" =~ ^[1-9][0-9]*(s|min|h|d|day|week|month|year)$ ]]; then
    fugue_journald_policy_set_state "refused" "invalid MaxRetentionSec value: ${max_retention_sec:-empty}"
    return 1
  fi
  if [[ ! "${system_max_use}" =~ ^[1-9][0-9]*(K|M|G|T)$ ]]; then
    fugue_journald_policy_set_state "refused" "invalid SystemMaxUse value: ${system_max_use:-empty}"
    return 1
  fi
  return 0
}

fugue_journald_policy_render() {
  printf '%s\n' \
    '# Managed by Fugue. Local edits will be replaced.' \
    '[Journal]' \
    "MaxRetentionSec=${FUGUE_JOURNALD_MAX_RETENTION_SEC}" \
    "SystemMaxUse=${FUGUE_JOURNALD_SYSTEM_MAX_USE}"
}

fugue_journald_policy_disk_usage() {
  local usage=""
  usage="$(journalctl --disk-usage 2>&1 | tail -n 1 || true)"
  usage="${usage//$'\n'/ }"
  printf '%s' "${usage:-unknown}"
}

fugue_journald_policy_effective_value() {
  local key="$1"
  systemd-analyze cat-config systemd/journald.conf 2>/dev/null | awk -F= -v wanted="${key}" '
    $0 ~ "^[[:space:]]*" wanted "[[:space:]]*=" {
      value = $0
      sub("^[^=]*=[[:space:]]*", "", value)
      sub("[[:space:]]*$", "", value)
    }
    END { print value }
  '
}

fugue_journald_policy_restore() {
  local backup_dir="$1"
  local policy_file="${FUGUE_JOURNALD_POLICY_FILE}"
  if [ "$(cat "${backup_dir}/state" 2>/dev/null || true)" = "present" ]; then
    install -m 0644 "${backup_dir}/policy" "${policy_file}"
  else
    rm -f "${policy_file}"
  fi
  systemctl restart "${FUGUE_JOURNALD_POLICY_SERVICE}" >/dev/null 2>&1 || true
}

fugue_journald_policy_reconcile() {
  local dry_run="${FUGUE_JOURNALD_DRY_RUN:-false}"
  local policy_file="${FUGUE_JOURNALD_POLICY_FILE}"
  local policy_dir=""
  local desired=""
  local backup_dir=""
  local current_retention=""
  local current_max_use=""

  FUGUE_JOURNALD_POLICY_CHANGED="false"
  FUGUE_JOURNALD_POLICY_BEFORE_USAGE="unknown"
  FUGUE_JOURNALD_POLICY_AFTER_USAGE="unknown"
  FUGUE_JOURNALD_POLICY_EFFECTIVE_MAX_RETENTION_SEC=""
  FUGUE_JOURNALD_POLICY_EFFECTIVE_SYSTEM_MAX_USE=""
  fugue_journald_policy_set_state "checking" ""

  if ! fugue_journald_policy_validate; then
    return 2
  fi
  for command_name in systemctl journalctl systemd-analyze install awk cmp mktemp; do
    if ! command -v "${command_name}" >/dev/null 2>&1; then
      fugue_journald_policy_set_state "unsupported" "required command is unavailable: ${command_name}"
      return 2
    fi
  done
  if ! systemctl is-active --quiet "${FUGUE_JOURNALD_POLICY_SERVICE}"; then
    fugue_journald_policy_set_state "refused" "${FUGUE_JOURNALD_POLICY_SERVICE} is not active"
    return 1
  fi

  FUGUE_JOURNALD_POLICY_BEFORE_USAGE="$(fugue_journald_policy_disk_usage)"
  desired="$(mktemp)" || {
    fugue_journald_policy_set_state "failed" "could not allocate desired policy file"
    return 1
  }
  fugue_journald_policy_render >"${desired}"
  if [ ! -f "${policy_file}" ] || ! cmp -s "${desired}" "${policy_file}"; then
    FUGUE_JOURNALD_POLICY_CHANGED="true"
  fi

  if [ "${dry_run}" = "true" ]; then
    rm -f "${desired}"
    fugue_journald_policy_set_state "dry-run" "policy inspected without host mutation"
    FUGUE_JOURNALD_POLICY_AFTER_USAGE="${FUGUE_JOURNALD_POLICY_BEFORE_USAGE}"
    return 0
  fi

  policy_dir="$(dirname "${policy_file}")"
  install -d -m 0755 "${policy_dir}" || {
    rm -f "${desired}"
    fugue_journald_policy_set_state "failed" "could not create ${policy_dir}"
    return 1
  }
  backup_dir="$(mktemp -d)" || {
    rm -f "${desired}"
    fugue_journald_policy_set_state "failed" "could not allocate rollback directory"
    return 1
  }
  if [ -f "${policy_file}" ]; then
    cp -p "${policy_file}" "${backup_dir}/policy" || {
      rm -rf "${backup_dir}" "${desired}"
      fugue_journald_policy_set_state "failed" "could not back up ${policy_file}"
      return 1
    }
    printf 'present\n' >"${backup_dir}/state"
  else
    printf 'absent\n' >"${backup_dir}/state"
  fi

  if [ "${FUGUE_JOURNALD_POLICY_CHANGED}" = "true" ]; then
    if ! install -m 0644 "${desired}" "${policy_file}"; then
      rm -rf "${backup_dir}" "${desired}"
      fugue_journald_policy_set_state "failed" "could not install ${policy_file}"
      return 1
    fi
    if ! systemctl restart "${FUGUE_JOURNALD_POLICY_SERVICE}" ||
       ! systemctl is-active --quiet "${FUGUE_JOURNALD_POLICY_SERVICE}"; then
      fugue_journald_policy_restore "${backup_dir}"
      rm -rf "${backup_dir}" "${desired}"
      fugue_journald_policy_set_state "rolled-back" "journald rejected the managed policy; previous configuration restored"
      return 1
    fi
  fi
  rm -f "${desired}"

  current_retention="$(fugue_journald_policy_effective_value MaxRetentionSec)" || true
  current_max_use="$(fugue_journald_policy_effective_value SystemMaxUse)" || true
  FUGUE_JOURNALD_POLICY_EFFECTIVE_MAX_RETENTION_SEC="${current_retention}"
  FUGUE_JOURNALD_POLICY_EFFECTIVE_SYSTEM_MAX_USE="${current_max_use}"
  if [ "${current_retention}" != "${FUGUE_JOURNALD_MAX_RETENTION_SEC}" ] ||
     [ "${current_max_use}" != "${FUGUE_JOURNALD_SYSTEM_MAX_USE}" ]; then
    if [ "${FUGUE_JOURNALD_POLICY_CHANGED}" = "true" ]; then
      fugue_journald_policy_restore "${backup_dir}"
    fi
    rm -rf "${backup_dir}"
    fugue_journald_policy_set_state "rolled-back" "effective journald policy differs from Fugue intent: MaxRetentionSec=${current_retention:-unset} SystemMaxUse=${current_max_use:-unset}"
    return 1
  fi
  rm -rf "${backup_dir}"

  if ! journalctl --rotate; then
    fugue_journald_policy_set_state "failed" "journald rotation failed after policy activation"
    return 1
  fi
  if ! journalctl --vacuum-time="${FUGUE_JOURNALD_MAX_RETENTION_SEC}" --vacuum-size="${FUGUE_JOURNALD_SYSTEM_MAX_USE}"; then
    fugue_journald_policy_set_state "failed" "journald vacuum failed after policy activation"
    return 1
  fi
  FUGUE_JOURNALD_POLICY_AFTER_USAGE="$(fugue_journald_policy_disk_usage)"
  fugue_journald_policy_set_state "applied" "managed policy is effective and archived journals were vacuumed"
  return 0
}
`
}
