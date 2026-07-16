#!/usr/bin/env bash

# Side-effect-free argv construction for the release-domain render gate. This
# seam does not execute Helm or claim manifest equivalence by itself.
# The callback is responsible for privately consuming command stdout. Both the
# stored base release and dry-run target contain sensitive rendered values and
# must never be logged or uploaded as raw artifacts.

control_plane_release_valid_live_revision() {
  (( $# == 1 )) || return 1
  local value="$1"
  local LC_ALL=C
  [[ "${value}" =~ ^[1-9][0-9]*$ ]] || return 1
  (( ${#value} < 10 )) && return 0
  (( ${#value} == 10 )) || return 1
  [[ "${value}" < "2147483647" ]]
}

control_plane_release_decimal_less_than() {
  (( $# == 2 )) || return 1
  local left="$1"
  local right="$2"
  local LC_ALL=C
  [[ "${left}" =~ ^[0-9]+$ && "${right}" =~ ^[0-9]+$ ]] || return 1
  left="${left#"${left%%[!0]*}"}"
  right="${right#"${right%%[!0]*}"}"
  [[ -n "${left}" ]] || left="0"
  [[ -n "${right}" ]] || right="0"
  (( ${#left} < ${#right} )) && return 0
  (( ${#left} == ${#right} )) || return 1
  [[ "${left}" < "${right}" ]]
}

# Selects the only two supported hook policies from a frozen Helm upgrade
# argv. Helm 4 can retain release.hooks in JSON even when --no-hooks is set, so
# callers must carry this fixed policy into canonicalization instead of
# inferring it from the renderer's output.
control_plane_release_fixed_hook_policy_from_upgrade_argv() {
  (( $# > 0 )) || return 2
  local argument=""
  local no_hooks_count=0
  local previous_no_hooks=0
  for argument in "$@"; do
    if (( previous_no_hooks != 0 )) &&
      [[ "${argument}" == "true" || "${argument}" == "false" ]]; then
      return 2
    fi
    previous_no_hooks=0
    case "${argument}" in
      --no-hooks)
        no_hooks_count=$((no_hooks_count + 1))
        previous_no_hooks=1
        ;;
      --no-hooks=*)
        return 2
        ;;
    esac
  done
  (( no_hooks_count <= 1 )) || return 2
  if (( no_hooks_count == 1 )); then
    printf '%s\n' exclude-hooks
  else
    printf '%s\n' include-hooks
  fi
}

control_plane_release_with_private_manifest_render_argv() {
  (( $# >= 2 )) || return 2

  local live_revision="$1"
  local consumer="$2"
  local argument=""
  local hook_policy=""
  local reset_then_reuse_count=0
  local -a CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV=()
  local -a CONTROL_PLANE_RELEASE_BASE_RENDER_ARGV=()
  local -a CONTROL_PLANE_RELEASE_TARGET_RENDER_ARGV=()
  local -a CONTROL_PLANE_RELEASE_REPEATED_TARGET_RENDER_ARGV=()
  shift 2
  CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV=("$@")

  control_plane_release_valid_live_revision "${live_revision}" || return 2
  [[ -n "${consumer}" ]] && declare -F "${consumer}" >/dev/null 2>&1 || return 2
  (( ${#CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[@]} >= 7 )) || return 2
  [[ "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[0]}" == "helm" &&
    "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[1]}" == "upgrade" &&
    -n "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[2]}" &&
    -n "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[3]}" &&
    "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[4]}" == "-n" &&
    -n "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[5]}" &&
    "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[6]}" == "--reset-then-reuse-values" ]] || return 2

  hook_policy="$(control_plane_release_fixed_hook_policy_from_upgrade_argv \
    "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[@]}")" || return 2
  [[ "${hook_policy}" == "include-hooks" || "${hook_policy}" == "exclude-hooks" ]] || return 2

  for argument in "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[@]}"; do
    case "${argument}" in
      --reset-then-reuse-values)
        reset_then_reuse_count=$((reset_then_reuse_count + 1))
        ;;
      --dry-run|--dry-run=*|--output|--output=*|-o|-o=*|-o?*|\
      --hide-secret|--hide-secret=*|--install|--install=*|-i|-i=*|\
      --debug|--debug=*)
        return 2
        ;;
    esac
  done
  (( reset_then_reuse_count == 1 )) || return 2

  CONTROL_PLANE_RELEASE_TARGET_RENDER_ARGV=(
    "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[@]}"
    --dry-run=server
    --output json
  )
  if [[ "${hook_policy}" == "exclude-hooks" ]]; then
    CONTROL_PLANE_RELEASE_BASE_RENDER_ARGV=(
      helm get all "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[2]}"
      -n "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[5]}"
      --revision "${live_revision}"
      --template '{{ .Release.Manifest }}'
    )
  else
    CONTROL_PLANE_RELEASE_BASE_RENDER_ARGV=(
      helm get all "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[2]}"
      -n "${CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV[5]}"
      --revision "${live_revision}"
      --template '{{ .Release.Manifest }}{{ range .Release.Hooks }}{{ printf "\n---\n%s\n" .Manifest }}{{ end }}'
    )
  fi
  CONTROL_PLANE_RELEASE_REPEATED_TARGET_RENDER_ARGV=(
    "${CONTROL_PLANE_RELEASE_TARGET_RENDER_ARGV[@]}"
  )

  readonly -a CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV
  readonly -a CONTROL_PLANE_RELEASE_BASE_RENDER_ARGV
  readonly -a CONTROL_PLANE_RELEASE_TARGET_RENDER_ARGV
  readonly -a CONTROL_PLANE_RELEASE_REPEATED_TARGET_RENDER_ARGV

  # Bracket the pinned base read with two independently executed target dry
  # runs. A later consumer compares their canonical manifests, so lookup or
  # cluster-state drift during the evidence window fails closed.
  "${consumer}" target "${CONTROL_PLANE_RELEASE_TARGET_RENDER_ARGV[@]}" || return
  "${consumer}" base "${CONTROL_PLANE_RELEASE_BASE_RENDER_ARGV[@]}" || return
  "${consumer}" repeated-target "${CONTROL_PLANE_RELEASE_REPEATED_TARGET_RENDER_ARGV[@]}"
}

# Keeps a process-group anchor alive after a callback returns, publishes its
# status through a private file, and then kills the still-anchored group before
# reaping it. Trusted callbacks are synchronous and must not daemonize, reparent,
# or create a new session; any same-group descendant is terminated before the
# anchored PGID can be reused, and no private stream is exposed.
control_plane_release_run_private_render_callback() {
  (( $# >= 3 )) || return 2
  local stdout_file="$1"
  local stderr_file="$2"
  local callback="$3"
  local callback_status=0
  local file_limit_kib=16384
  local current_file_limit=""
  local caller_lost=0
  local status_file="${stdout_file}.status"
  local status_temporary="${stdout_file}.status.tmp"
  local hold_fifo="${stdout_file}.hold"
  local process_file="${stdout_file}.processes"
  local process_pid=""
  local process_group=""
  local descendant_found=0
  local anchor_found=0
  shift 3

  [[ ! -e "${status_file}" && ! -L "${status_file}" &&
    ! -e "${status_temporary}" && ! -L "${status_temporary}" &&
    ! -e "${hold_fifo}" && ! -L "${hold_fifo}" &&
    ! -e "${process_file}" && ! -L "${process_file}" ]] || return 2
  command mkfifo -m 600 "${hold_fifo}" || return 2
  set -m || return 2
  (
    set +m || exit 2
    exec 9<>"${hold_fifo}" || exit 2
    rm -f -- "${hold_fifo}"
    if (
      exec 9>&-
      unset status_file status_temporary hold_fifo process_file process_pid process_group descendant_found anchor_found
      for current_file_limit in "$(ulimit -S -f)" "$(ulimit -H -f)"; do
        if [[ "${current_file_limit}" != "unlimited" ]]; then
          [[ "${current_file_limit}" =~ ^[0-9]+$ ]] || exit 2
          if control_plane_release_decimal_less_than "${current_file_limit}" "${file_limit_kib}"; then
            file_limit_kib="${current_file_limit}"
          fi
        fi
      done
      ulimit -S -f "${file_limit_kib}" || exit 2
      ulimit -H -f "${file_limit_kib}" || exit 2
      "${callback}" "$@"
    ); then
      callback_status=0
    else
      callback_status=$?
    fi
    umask 077
    if printf '%s\n' "${callback_status}" >"${status_temporary}" &&
      mv -f -- "${status_temporary}" "${status_file}"; then
      IFS= read -r _ <&9 || true
    fi
    exit 125
  ) >"${stdout_file}" 2>"${stderr_file}" &
  CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID=$!
  if ! set +m; then
    kill -KILL -- "-${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID}" >/dev/null 2>&1 ||
      kill -KILL "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID}" >/dev/null 2>&1 || true
    wait "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID}" >/dev/null 2>&1 || true
    CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID=""
    rm -f -- "${status_file}" "${status_temporary}" "${hold_fifo}" "${process_file}"
    return 2
  fi

  while [[ ! -f "${status_file}" || -L "${status_file}" ]]; do
    if ! kill -0 "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_CALLER_PID}" >/dev/null 2>&1; then
      caller_lost=1
      break
    fi
    if ! kill -0 "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID}" >/dev/null 2>&1; then
      break
    fi
    sleep 0.05
  done

  if [[ -f "${status_file}" && ! -L "${status_file}" ]]; then
    if chmod 600 "${status_file}"; then
      IFS= read -r callback_status <"${status_file}" || callback_status=256
    else
      callback_status=256
    fi
    [[ "${callback_status}" =~ ^(0|[1-9][0-9]{0,2})$ ]] || callback_status=256
    (( callback_status <= 255 )) || callback_status=256
    if (umask 077; set -C; command ps -axo pid=,pgid= >"${process_file}" 2>/dev/null); then
      chmod 600 "${process_file}" || callback_status=256
      while read -r process_pid process_group; do
        if [[ "${process_pid}" == "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID}" &&
          "${process_group}" == "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID}" ]]; then
          anchor_found=1
        elif [[ "${process_group}" == "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID}" ]]; then
          descendant_found=1
        fi
      done <"${process_file}"
      if (( anchor_found == 0 )) ||
        ! kill -0 "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID}" >/dev/null 2>&1; then
        callback_status=256
      fi
    else
      callback_status=256
    fi
  else
    callback_status=256
  fi

  kill -KILL -- "-${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID}" >/dev/null 2>&1 ||
    kill -KILL "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID}" >/dev/null 2>&1 || true
  wait "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID}" >/dev/null 2>&1 || true
  CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID=""
  rm -f -- "${status_file}" "${status_temporary}" "${hold_fifo}" "${process_file}"
  (( caller_lost == 0 )) || return 143
  (( callback_status <= 255 )) || return 2
  if (( descendant_found != 0 && callback_status == 0 )); then
    return 70
  fi
  return "${callback_status}"
}

control_plane_release_cleanup_private_render_worker() {
  if [[ "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID:-}" =~ ^[1-9][0-9]*$ ]] &&
    kill -0 "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID}" >/dev/null 2>&1; then
    kill -KILL -- "-${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID}" >/dev/null 2>&1 ||
      kill -KILL "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID}" >/dev/null 2>&1 || true
    wait "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID}" >/dev/null 2>&1 || true
  fi
  CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID=""
  if [[ -n "${staging_prefix:-}" && -n "${staging_dir:-}" &&
    "${staging_dir}" != "${staging_dir#"${staging_prefix}"}" &&
    "${staging_dir#"${staging_prefix}"}" =~ ^[A-Za-z0-9]{6}$ ]]; then
    rm -rf -- "${staging_dir}"
  fi
}

# Internal callback used only inside
# control_plane_release_run_private_canonical_render_set's isolated subshell.
control_plane_release_capture_private_render_phase() {
  (( $# >= 2 )) || return 2
  local phase="$1"
  local expected_phase=""
  local raw_file=""
  local stderr_file=""
  local status=0
  shift

  [[ -n "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_RUNNER:-}" &&
    -n "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_RAW_DIR:-}" &&
    "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_PHASE_COUNT:-}" =~ ^[0-2]$ ]] || return 2
  expected_phase="${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_EXPECTED_PHASES[${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_PHASE_COUNT}]}"
  [[ "${phase}" == "${expected_phase}" ]] || return 2
  case "${phase}" in
    target)
      raw_file="${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_RAW_DIR}/target.raw"
      ;;
    base)
      raw_file="${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_RAW_DIR}/base.raw"
      ;;
    repeated-target)
      raw_file="${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_RAW_DIR}/repeated-target.raw"
      ;;
    *)
      return 2
      ;;
  esac
  stderr_file="${raw_file}.stderr"
  if control_plane_release_run_private_render_callback \
    "${raw_file}" "${stderr_file}" \
    "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_RUNNER}" "${phase}" "$@"; then
    :
  else
    status=$?
    printf '[fugue-release-render] private %s render failed (status=%s)\n' "${phase}" "${status}" >&2
    return "${status}"
  fi
  [[ -f "${raw_file}" && ! -L "${raw_file}" && -f "${stderr_file}" && ! -L "${stderr_file}" ]] || return 2
  chmod 600 "${raw_file}" "${stderr_file}" || return
  CONTROL_PLANE_RELEASE_PRIVATE_RENDER_PHASE_COUNT=$((CONTROL_PLANE_RELEASE_PRIVATE_RENDER_PHASE_COUNT + 1))
}

# Executes the dormant target -> pinned base -> repeated target render seam,
# canonicalizes all three private outputs, proves repeated-target equivalence,
# and lends the canonical files to a synchronous consumer. The entire staging
# tree is deleted on every return or signal. This function is intentionally not
# called by the default release path before the B3 atomic activation boundary.
control_plane_release_run_private_canonical_render_set() {
  (( $# >= 11 )) || return 2

  local live_revision="$1"
  local runner="$2"
  local canonicalizer="$3"
  local consumer="$4"
  shift 4
  local -a source_argv=("$@")
  local hook_policy=""

  control_plane_release_valid_live_revision "${live_revision}" || return 2
  [[ -n "${runner}" && -n "${canonicalizer}" && -n "${consumer}" ]] || return 2
  declare -F "${runner}" >/dev/null 2>&1 || return 2
  declare -F "${canonicalizer}" >/dev/null 2>&1 || return 2
  declare -F "${consumer}" >/dev/null 2>&1 || return 2
  (( ${#source_argv[@]} >= 7 )) || return 2
  hook_policy="$(control_plane_release_fixed_hook_policy_from_upgrade_argv \
    "${source_argv[@]}")" || return 2
  [[ "${hook_policy}" == "include-hooks" || "${hook_policy}" == "exclude-hooks" ]] || return 2
  readonly hook_policy

  local caller_pid=""
  local caller_probe_pid=""
  local worker_pid=""
  local worker_status=0
  local caller_int_trap=""
  local caller_int_trap_installed=0
  local caller_monitor_enabled=0
  local CONTROL_PLANE_RELEASE_PRIVATE_RENDER_CALLER_INTERRUPTED=0

  # Bash 3.2 keeps $$ fixed at the top-level shell even in a subshell. Query a
  # short-lived direct child's PPID so each callback poll binds to the actual
  # caller rather than a stale top-level PID.
  command sleep 5 >/dev/null 2>&1 &
  caller_probe_pid=$!
  caller_pid="$(command ps -o ppid= -p "${caller_probe_pid}")" || {
    kill -TERM "${caller_probe_pid}" >/dev/null 2>&1 || true
    wait "${caller_probe_pid}" >/dev/null 2>&1 || true
    return 2
  }
  kill -TERM "${caller_probe_pid}" >/dev/null 2>&1 || true
  wait "${caller_probe_pid}" >/dev/null 2>&1 || true
  caller_pid="${caller_pid//[[:space:]]/}"
  [[ "${caller_pid}" =~ ^[1-9][0-9]*$ ]] || return 2

  # Waiting for a background worker lets an existing caller trap run
  # immediately. A non-interactive Bash with no INT trap otherwise ignores a
  # direct-PID INT while waiting, so install and later remove only that missing
  # default handler; never replace a caller-owned trap.
  caller_int_trap="$(trap -p INT)"
  if [[ -z "${caller_int_trap}" ]]; then
    trap 'CONTROL_PLANE_RELEASE_PRIVATE_RENDER_CALLER_INTERRUPTED=1; if [[ "${worker_pid:-}" =~ ^[1-9][0-9]*$ ]]; then kill -TERM "${worker_pid}" >/dev/null 2>&1 || true; fi' INT
    caller_int_trap_installed=1
  fi

  [[ "$-" == *m* ]] && caller_monitor_enabled=1
  if ! set -m; then
    (( caller_int_trap_installed == 0 )) || trap - INT
    return 2
  fi

  (
    local private_parent staging_prefix staging_suffix staging_dir raw_dir canonical_dir || return 2
    local release_name release_namespace expected_version status canonical_file || return 2
    local CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID || return 2
    local CONTROL_PLANE_RELEASE_PRIVATE_RENDER_CALLER_PID || return 2
    private_parent=""
    staging_prefix=""
    staging_suffix=""
    staging_dir=""
    raw_dir=""
    canonical_dir=""
    release_name="${source_argv[2]}"
    release_namespace="${source_argv[5]}"
    expected_version=$((live_revision + 1))
    status=0
    canonical_file=""
    CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID=""
    CONTROL_PLANE_RELEASE_PRIVATE_RENDER_CALLER_PID="${caller_pid}"
    umask 077

    private_parent="$(cd "${TMPDIR:-/tmp}" 2>/dev/null && pwd -P)" || return 2
    [[ -d "${private_parent}" ]] || return 2
    staging_prefix="${private_parent%/}/fugue-release-render."
    trap control_plane_release_cleanup_private_render_worker EXIT
    trap 'trap "" HUP INT TERM; control_plane_release_cleanup_private_render_worker; trap - EXIT; exit 129' HUP
    trap 'trap "" HUP INT TERM; control_plane_release_cleanup_private_render_worker; trap - EXIT; exit 130' INT
    trap 'trap "" HUP INT TERM; control_plane_release_cleanup_private_render_worker; trap - EXIT; exit 143' TERM
    staging_dir="$(command mktemp -d "${staging_prefix}XXXXXX")" || return
    staging_suffix="${staging_dir#"${staging_prefix}"}"
    [[ "${staging_dir}" != "${staging_suffix}" && "${staging_suffix}" =~ ^[A-Za-z0-9]{6}$ ]] || return 2
    readonly staging_dir
    [[ -n "${staging_dir}" && -d "${staging_dir}" && ! -L "${staging_dir}" ]] || return 2
    chmod 700 "${staging_dir}" || return

    raw_dir="${staging_dir}/raw"
    canonical_dir="${staging_dir}/canonical"
    mkdir -m 700 "${raw_dir}" "${canonical_dir}" || return

    CONTROL_PLANE_RELEASE_PRIVATE_RENDER_RUNNER="${runner}"
    CONTROL_PLANE_RELEASE_PRIVATE_RENDER_RAW_DIR="${raw_dir}"
    CONTROL_PLANE_RELEASE_PRIVATE_RENDER_PHASE_COUNT=0
    CONTROL_PLANE_RELEASE_PRIVATE_RENDER_EXPECTED_PHASES=(target base repeated-target)

    control_plane_release_with_private_manifest_render_argv \
      "${live_revision}" control_plane_release_capture_private_render_phase \
      "${source_argv[@]}" || return
    [[ "${CONTROL_PLANE_RELEASE_PRIVATE_RENDER_PHASE_COUNT}" == "3" ]] || return 2

    if control_plane_release_run_private_render_callback \
      "${staging_dir}/canonicalizer.stdout" "${staging_dir}/canonicalizer.stderr" \
      "${canonicalizer}" \
      "${live_revision}" "${expected_version}" "${release_name}" "${release_namespace}" "${hook_policy}" \
      "${raw_dir}/base.raw" "${raw_dir}/target.raw" "${raw_dir}/repeated-target.raw" \
      "${canonical_dir}"; then
      :
    else
      status=$?
      printf '[fugue-release-render] private canonicalization failed (status=%s)\n' "${status}" >&2
      return "${status}"
    fi
    chmod 600 "${staging_dir}/canonicalizer.stdout" "${staging_dir}/canonicalizer.stderr" || return
    for canonical_file in \
      "${canonical_dir}/base.manifest" \
      "${canonical_dir}/target.manifest" \
      "${canonical_dir}/repeated-target.manifest"; do
      [[ -f "${canonical_file}" && ! -L "${canonical_file}" ]] || return 2
      chmod 600 "${canonical_file}" || return
    done
    if ! cmp -s "${canonical_dir}/target.manifest" "${canonical_dir}/repeated-target.manifest"; then
      printf '[fugue-release-render] repeated target canonical manifest drifted\n' >&2
      return 74
    fi

    if control_plane_release_run_private_render_callback \
      "${staging_dir}/consumer.stdout" "${staging_dir}/consumer.stderr" \
      "${consumer}" \
      "${live_revision}" "${release_name}" "${release_namespace}" \
      "${canonical_dir}/base.manifest" \
      "${canonical_dir}/target.manifest" \
      "${canonical_dir}/repeated-target.manifest"; then
      :
    else
      status=$?
      printf '[fugue-release-render] private canonical render consumer failed (status=%s)\n' "${status}" >&2
      return "${status}"
    fi
    chmod 600 "${staging_dir}/consumer.stdout" "${staging_dir}/consumer.stderr" || return
  ) &
  worker_pid=$!
  if (( caller_monitor_enabled == 0 )) && ! set +m; then
    kill -KILL -- "-${worker_pid}" >/dev/null 2>&1 || kill -KILL "${worker_pid}" >/dev/null 2>&1 || true
    wait "${worker_pid}" >/dev/null 2>&1 || true
    (( caller_int_trap_installed == 0 )) || trap - INT
    return 2
  fi

  if (( CONTROL_PLANE_RELEASE_PRIVATE_RENDER_CALLER_INTERRUPTED != 0 )); then
    kill -INT "${worker_pid}" >/dev/null 2>&1 || true
  fi
  wait "${worker_pid}" || worker_status=$?
  if kill -0 "${worker_pid}" >/dev/null 2>&1; then
    case "${worker_status}" in
      129) kill -HUP "${worker_pid}" >/dev/null 2>&1 || true ;;
      130) kill -TERM "${worker_pid}" >/dev/null 2>&1 || true ;;
      *) kill -TERM "${worker_pid}" >/dev/null 2>&1 || true ;;
    esac
    wait "${worker_pid}" >/dev/null 2>&1 || true
  fi
  if (( caller_int_trap_installed != 0 )); then
    trap - INT
  fi
  if (( CONTROL_PLANE_RELEASE_PRIVATE_RENDER_CALLER_INTERRUPTED != 0 )); then
    return 130
  fi
  return "${worker_status}"
}
