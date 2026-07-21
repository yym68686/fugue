#!/usr/bin/env bash

# Production activation for the fixed single-domain dispatcher.
#
# This file is sourced only after upgrade_fugue_control_plane.sh has defined
# all of its release helpers and after control_plane_release_domains.sh and
# control_plane_release_render.sh have been sourced. It does not run a release
# on import. The sole public entrypoint is:
#
#   control_plane_release_run_atomic_domain_release
#
# The entrypoint consumes only the exact environment names wired by the formal
# workflow:
#   FUGUE_RELEASE_DOMAIN_BASE_SHA
#   FUGUE_RELEASE_DOMAIN_TARGET_SHA
#   FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL
#   FUGUE_RELEASE_DOMAIN_DISPATCH_TOOL
#   FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE
#   FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE
#   FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR
#   FUGUE_RELEASE_DOMAIN_VERIFIED_IMAGE_ARTIFACTS_DIGEST
#   FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE
# Apply additionally consumes the immutable outputs of the immediately prior
# pinned upload-artifact step:
#   FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_ID
#   FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_DIGEST
#   FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_URL
#   FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_ID
#   FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_DIGEST
#   FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_URL
#
# Changed-file evidence is regenerated inside the private work directory. The
# production implementation of control_plane_release_verify_repository_snapshot
# is intentionally a named function so the fake harness can replace that one
# read-only boundary without teaching production about a bypass environment.

control_plane_release_domain_production_error() {
  printf 'control-plane atomic domain release: %s\n' "$*" >&2
}

control_plane_release_domain_require_function() {
  declare -F "$1" >/dev/null 2>&1 || {
    control_plane_release_domain_production_error "required function $1 is unavailable"
    return 2
  }
}

control_plane_release_domain_validate_sha() {
  [[ "$1" =~ ^[0-9a-f]{40}$ ]]
}

control_plane_release_domain_validate_digest() {
  [[ "$1" =~ ^sha256:[0-9a-f]{64}$ ]]
}

control_plane_release_domain_validate_operational_phase() {
  local phase="${FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE:-}"
  local artifact_id="${FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_ID:-}"
  local artifact_digest="${FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_DIGEST:-}"
  local artifact_url="${FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_URL:-}"
  local activation_artifact_id="${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_ID:-}"
  local activation_artifact_digest="${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_DIGEST:-}"
  local activation_artifact_url="${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_URL:-}"
  local expected_url=""
  local expected_activation_url=""

  case "${phase}" in
    prepare)
      [[ -z "${artifact_id}" && -z "${artifact_digest}" && -z "${artifact_url}" &&
        -z "${activation_artifact_id}" && -z "${activation_artifact_digest}" &&
        -z "${activation_artifact_url}" ]]
      ;;
    apply)
      [[ "${artifact_id}" =~ ^[1-9][0-9]*$ &&
        "${artifact_digest}" =~ ^[0-9a-f]{64}$ &&
        "${activation_artifact_id}" =~ ^[1-9][0-9]*$ &&
        "${activation_artifact_digest}" =~ ^[0-9a-f]{64}$ &&
        -n "${GITHUB_SERVER_URL:-}" && -n "${GITHUB_REPOSITORY:-}" ]] || return 2
      expected_url="${GITHUB_SERVER_URL%/}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/artifacts/${artifact_id}"
      expected_activation_url="${GITHUB_SERVER_URL%/}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/artifacts/${activation_artifact_id}"
      [[ "${artifact_url}" == "${expected_url}" &&
        "${activation_artifact_url}" == "${expected_activation_url}" ]]
      ;;
    *) return 2 ;;
  esac
}

control_plane_release_domain_compare_build_activation_reports() {
  (( $# == 2 )) || return 2
  python3 - "$1" "$2" <<'PY'
import os
import stat
import sys

expected_names = [
    "build-artifact-plan.json",
    "composite-decomposition-evidence.json",
    "image-activation-evidence.json",
    "image-activation-plan.json",
]
payloads = []
for directory in map(os.path.abspath, sys.argv[1:]):
    metadata = os.lstat(directory)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or metadata.st_uid != os.geteuid()
    ):
        raise SystemExit(1)
    if sorted(os.listdir(directory)) != expected_names:
        raise SystemExit(1)
    directory_payload = []
    for name in expected_names:
        path = os.path.join(directory, name)
        at_path = os.lstat(path)
        if (
            not stat.S_ISREG(at_path.st_mode)
            or stat.S_ISLNK(at_path.st_mode)
            or stat.S_IMODE(at_path.st_mode) != 0o600
            or at_path.st_uid != os.geteuid()
            or at_path.st_nlink != 1
        ):
            raise SystemExit(1)
        with open(path, "rb") as handle:
            data = handle.read()
            opened = os.fstat(handle.fileno())
        if (opened.st_dev, opened.st_ino, opened.st_size) != (
            at_path.st_dev, at_path.st_ino, at_path.st_size
        ):
            raise SystemExit(1)
        directory_payload.append(data)
    payloads.append(directory_payload)
if payloads[0] != payloads[1]:
    raise SystemExit(1)
PY
}

control_plane_release_domain_compare_uploaded_operational_report() {
  (( $# == 2 )) || return 2
  python3 - "$1" "$2" <<'PY'
import os
import stat
import sys

paths = [os.path.abspath(value) for value in sys.argv[1:]]
payloads = []
for path in paths:
    at_path = os.lstat(path)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        opened = os.fstat(fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or stat.S_ISLNK(at_path.st_mode)
            or (opened.st_dev, opened.st_ino) != (at_path.st_dev, at_path.st_ino)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
            or opened.st_size > 8 << 20
        ):
            raise SystemExit(1)
        data = bytearray()
        while True:
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            data.extend(chunk)
        if len(data) != opened.st_size:
            raise SystemExit(1)
        after = os.lstat(path)
        if (after.st_dev, after.st_ino, after.st_size) != (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
        ):
            raise SystemExit(1)
        payloads.append(bytes(data))
    finally:
        os.close(fd)
if payloads[0] != payloads[1]:
    raise SystemExit(1)
PY
}

control_plane_release_domain_private_parent() {
  local requested="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
  local resolved=""

  [[ "${requested}" == /* && -d "${requested}" && ! -L "${requested}" ]] || return 2
  resolved="$(cd "${requested}" 2>/dev/null && pwd -P)" || return 2
  [[ -n "${resolved}" && "${resolved}" == /* && -d "${resolved}" && ! -L "${resolved}" ]] || return 2
  printf '%s\n' "${resolved}"
}

control_plane_release_domain_setup_private_workdir() {
  local parent=""

  parent="$(control_plane_release_domain_private_parent)" || return
  umask 077
  CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR="$(command mktemp -d "${parent%/}/fugue-release-domain.XXXXXX")" || return
  [[ -d "${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}" && ! -L "${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}" ]] || return 2
  chmod 700 "${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}" || return
  CONTROL_PLANE_RELEASE_DOMAIN_TRACE_FILE="${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/transaction.trace"
  if ! (umask 077; set -C; : >"${CONTROL_PLANE_RELEASE_DOMAIN_TRACE_FILE}"); then
    return 2
  fi
  chmod 600 "${CONTROL_PLANE_RELEASE_DOMAIN_TRACE_FILE}" || return
  exec 17>>"${CONTROL_PLANE_RELEASE_DOMAIN_TRACE_FILE}" || return
  CONTROL_PLANE_RELEASE_DOMAIN_TRACE_SEQUENCE=0
  CONTROL_PLANE_RELEASE_DOMAIN_RECOVERY_ACTIVE="false"
  CONTROL_PLANE_RELEASE_DOMAIN_COMMITTED="false"
  CONTROL_PLANE_RELEASE_DOMAIN_BUNDLE_DIR="${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/authorization-bundle"
  CONTROL_PLANE_RELEASE_DOMAIN_AUTHORIZATION_RESULT="${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/authorize.result"
  CONTROL_PLANE_RELEASE_DOMAIN_VERIFY_RESULT="${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/verify.result"
  CONTROL_PLANE_RELEASE_DOMAIN_CHANGED_EVIDENCE="${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/changed-file-evidence.json"
  CONTROL_PLANE_RELEASE_DOMAIN_OPERATIONAL_IMAGE_PLAN="${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/operational-image-plan.json"
  CONTROL_PLANE_RELEASE_DOMAIN_SOURCE_ARGV_SNAPSHOT="${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/upgrade-argv.source.snapshot"
  CONTROL_PLANE_RELEASE_DOMAIN_ARGV_INPUT_IDENTITIES="${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/upgrade-argv-input-identities.json"
  CONTROL_PLANE_RELEASE_DOMAIN_RUNTIME_TMP_DIR="${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/runtime"
  mkdir "${CONTROL_PLANE_RELEASE_DOMAIN_RUNTIME_TMP_DIR}" || return
  chmod 700 "${CONTROL_PLANE_RELEASE_DOMAIN_RUNTIME_TMP_DIR}" || return
}

control_plane_release_domain_prepare_public_parent() {
  python3 - "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" <<'PY'
import os
import stat
import sys

output = sys.argv[1]
if not os.path.isabs(output) or os.path.basename(output) in {"", ".", ".."}:
    raise SystemExit(1)
parent = os.path.dirname(os.path.abspath(output))
grandparent = os.path.dirname(parent)
if os.path.realpath(grandparent) != grandparent:
    raise SystemExit(1)
try:
    metadata = os.lstat(parent)
except FileNotFoundError:
    os.mkdir(parent, 0o700)
    metadata = os.lstat(parent)
if (
    not stat.S_ISDIR(metadata.st_mode)
    or stat.S_ISLNK(metadata.st_mode)
    or stat.S_IMODE(metadata.st_mode) != 0o700
    or metadata.st_uid != os.geteuid()
):
    raise SystemExit(1)
if os.path.realpath(parent) != parent:
    raise SystemExit(1)
try:
    destination = os.lstat(output)
except FileNotFoundError:
    destination = None
# Every workflow attempt owns one fresh path and upload uses overwrite=false.
# Reject stale output up front so the emergency publisher can safely promise
# never to replace an artifact whose provenance it did not create.
if destination is not None:
    raise SystemExit(1)
PY
}

control_plane_release_domain_with_private_tmp() {
  (( $# >= 1 )) || return 2
  local TMPDIR="${CONTROL_PLANE_RELEASE_DOMAIN_RUNTIME_TMP_DIR}"
  local RUNNER_TEMP="${CONTROL_PLANE_RELEASE_DOMAIN_RUNTIME_TMP_DIR}"
  export TMPDIR RUNNER_TEMP
  "$@"
}

control_plane_release_domain_cleanup_private_fds() {
  { exec 16<&-; } 2>/dev/null || :
  { exec 17>&-; } 2>/dev/null || :
  CONTROL_PLANE_RELEASE_DOMAIN_ARGV_FD_READY="false"
}

control_plane_release_domain_cleanup_private_workdir() {
  local workdir="${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR:-}"
  local parent=""

  [[ -n "${workdir}" ]] || return 0
  parent="$(dirname "${workdir}")" || return 2
  python3 - "${workdir}" "${parent}" <<'PY' || return
import os
import stat
import sys

root = os.path.abspath(sys.argv[1])
parent = os.path.abspath(sys.argv[2])
if os.path.dirname(root) != parent or os.path.realpath(parent) != parent:
    raise SystemExit(1)
root_metadata = os.lstat(root)
if (
    not stat.S_ISDIR(root_metadata.st_mode)
    or stat.S_ISLNK(root_metadata.st_mode)
    or stat.S_IMODE(root_metadata.st_mode) != 0o700
    or root_metadata.st_uid != os.geteuid()
):
    raise SystemExit(1)

def validate_private_tree(path):
    with os.scandir(path) as entries:
        children = list(entries)
    for entry in children:
        metadata = entry.stat(follow_symlinks=False)
        if metadata.st_uid != os.geteuid() or stat.S_ISLNK(metadata.st_mode):
            raise SystemExit(1)
        if stat.S_ISDIR(metadata.st_mode):
            if stat.S_IMODE(metadata.st_mode) & 0o077:
                raise SystemExit(1)
            validate_private_tree(entry.path)
        elif stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1:
            pass
        else:
            raise SystemExit(1)

def remove_private_tree(path):
    with os.scandir(path) as entries:
        children = list(entries)
    for entry in children:
        metadata = entry.stat(follow_symlinks=False)
        if metadata.st_uid != os.geteuid() or stat.S_ISLNK(metadata.st_mode):
            raise SystemExit(1)
        if stat.S_ISDIR(metadata.st_mode):
            if stat.S_IMODE(metadata.st_mode) & 0o077:
                raise SystemExit(1)
            remove_private_tree(entry.path)
            os.rmdir(entry.path)
        elif stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1:
            os.unlink(entry.path)
        else:
            raise SystemExit(1)

validate_private_tree(root)
remove_private_tree(root)
os.rmdir(root)
flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
parent_fd = os.open(parent, flags)
try:
    os.fsync(parent_fd)
finally:
    os.close(parent_fd)
PY
  CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR=""
}

control_plane_release_domain_private_recovery_required() {
  [[ "${CONTROL_PLANE_RELEASE_DOMAIN_ROLLBACK_FAILED:-false}" == "true" ||
    "${CONTROL_PLANE_RELEASE_ROLLBACK_FAILED:-false}" == "true" ||
    "${CONTROL_PLANE_RELEASE_DOMAIN_PUBLICATION_FAILED:-false}" == "true" ||
    "${DNS_MANIFEST_SNAPSHOT_KEEP:-false}" == "true" ||
    "${CONTROL_PLANE_RELEASE_RECOVERY_FENCE_REQUIRED:-false}" == "true" ||
    ( "${CONTROL_PLANE_RELEASE_DOMAIN_USES_BACKUP_COORDINATION:-false}" == "true" &&
      "${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD:-false}" == "true" ) ]]
}

control_plane_release_domain_verify_trace_identity() {
  local barrier="${1:-false}"

  python3 - "${CONTROL_PLANE_RELEASE_DOMAIN_TRACE_FILE}" \
    "${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}" "${barrier}" 17 <<'PY'
import os
import stat
import sys

path, directory, barrier = sys.argv[1], sys.argv[2], sys.argv[3] == "true"
opened = os.fstat(17)
at_path = os.lstat(path)
parent = os.lstat(directory)
if (
    not stat.S_ISREG(opened.st_mode)
    or stat.S_ISLNK(at_path.st_mode)
    or (opened.st_dev, opened.st_ino) != (at_path.st_dev, at_path.st_ino)
    or stat.S_IMODE(opened.st_mode) != 0o600
    or opened.st_uid != os.geteuid()
    or opened.st_nlink != 1
    or not stat.S_ISDIR(parent.st_mode)
    or stat.S_ISLNK(parent.st_mode)
    or stat.S_IMODE(parent.st_mode) != 0o700
    or parent.st_uid != os.geteuid()
):
    raise SystemExit(1)
os.fsync(17)
if barrier:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_fd = os.open(directory, flags)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
PY
}

control_plane_release_domain_pending_signal() {
  [[ -n "${CONTROL_PLANE_RELEASE_DOMAIN_PENDING_SIGNAL:-${CONTROL_PLANE_RELEASE_PENDING_SIGNAL:-}}" ]]
}

control_plane_release_domain_pending_status() {
  case "${CONTROL_PLANE_RELEASE_DOMAIN_PENDING_STATUS:-}" in
    129|130|143) printf '%s\n' "${CONTROL_PLANE_RELEASE_DOMAIN_PENDING_STATUS}"; return 0 ;;
  esac
  case "${CONTROL_PLANE_RELEASE_PENDING_SIGNAL_STATUS:-}" in
    129|130|143) printf '%s\n' "${CONTROL_PLANE_RELEASE_PENDING_SIGNAL_STATUS}"; return 0 ;;
  esac
  printf '143\n'
}

control_plane_release_domain_capture_signal() {
  local signal_name="$1"
  local signal_status="$2"

  if [[ -z "${CONTROL_PLANE_RELEASE_DOMAIN_PENDING_SIGNAL:-}" ]]; then
    CONTROL_PLANE_RELEASE_DOMAIN_PENDING_SIGNAL="${signal_name}"
    CONTROL_PLANE_RELEASE_DOMAIN_PENDING_STATUS="${signal_status}"
  fi
  if [[ -z "${CONTROL_PLANE_RELEASE_PENDING_SIGNAL:-}" ]]; then
    CONTROL_PLANE_RELEASE_PENDING_SIGNAL="${signal_name}"
    CONTROL_PLANE_RELEASE_PENDING_SIGNAL_STATUS="${signal_status}"
  fi
}

control_plane_release_domain_capture_backup_abort() {
  local forced="${1:-false}"

  case "${forced}" in true|false) ;; *) return 2 ;; esac
  if [[ "${forced}" == "true" ]]; then
    control_plane_release_domain_capture_signal USR2 143
  else
    control_plane_release_domain_capture_signal USR1 143
  fi
  if [[ "${CONTROL_PLANE_RELEASE_DOMAIN_RECOVERY_ACTIVE:-false}" == "true" ]]; then
    return 0
  fi
  if [[ "${CONTROL_PLANE_RELEASE_DOMAIN_TRANSACTION_COMMITTED:-false}" == "true" ||
    "${CONTROL_PLANE_RELEASE_COMMITTED:-false}" == "true" ]]; then
    # Commit has won, so no signal-triggered rollback is legal. Persist any
    # forced/unsafe Lease-loss fence for the post-commit settlement path, then
    # defer process exit until evidence and private recovery state are durable.
    prepare_control_plane_backup_abort_state_for_dispatch "${forced}" || :
    return 0
  fi
  prepare_control_plane_backup_abort_state_for_dispatch "${forced}" || :
  # Unlike a pending-only trap, synchronously stop and reap the active guarded
  # process group before the dispatcher can enter selected rollback.
  if ! terminate_active_control_plane_release_command; then
    mark_control_plane_release_command_termination_unproven
  fi
}

control_plane_release_domain_handle_release_signal() {
  local signal_name="$1"
  local signal_status=0

  case "${signal_name}" in
    HUP) signal_status=129 ;;
    INT) signal_status=130 ;;
    TERM) signal_status=143 ;;
    *) return 2 ;;
  esac
  control_plane_release_domain_capture_signal "${signal_name}" "${signal_status}"
  if [[ "${CONTROL_PLANE_RELEASE_DOMAIN_TRANSACTION_COMMITTED:-false}" == "true" ||
    "${CONTROL_PLANE_RELEASE_COMMITTED:-false}" == "true" ||
    "${CONTROL_PLANE_RELEASE_DOMAIN_RECOVERY_ACTIVE:-false}" == "true" ]]; then
    # Commit has won. Defer process exit until post-commit owner cleanup,
    # durable public evidence publication, and private cleanup complete.
    return 0
  fi
  if [[ "${CONTROL_PLANE_RELEASE_DOMAIN_TRANSACTION_ACTIVE:-false}" == "true" ]]; then
    # During forward transaction work, retain the root handler's proven
    # process-group termination and return-to-dispatch behavior.
    handle_control_plane_release_signal "${signal_name}"
    return
  fi
  # Before the dispatcher is active, stop any bounded render/preflight child
  # but do not allow the root handler to exit nonlocally past private cleanup.
  if ! terminate_active_control_plane_release_command; then
    mark_control_plane_release_command_termination_unproven
  fi
}

control_plane_release_domain_install_signal_boundary() {
  control_plane_release_domain_require_function handle_control_plane_release_signal || return
  control_plane_release_domain_require_function handle_control_plane_backup_coordination_abort || return
  [[ "$(trap -p HUP)" == *"handle_control_plane_release_signal HUP"* ]] || return 2
  [[ "$(trap -p INT)" == *"handle_control_plane_release_signal INT"* ]] || return 2
  [[ "$(trap -p TERM)" == *"handle_control_plane_release_signal TERM"* ]] || return 2
  [[ "$(trap -p USR1)" == *"handle_control_plane_backup_coordination_abort"* ]] || return 2
  [[ "$(trap -p USR2)" == *"handle_control_plane_backup_coordination_abort true"* ]] || return 2

  CONTROL_PLANE_RELEASE_DOMAIN_PENDING_SIGNAL=""
  CONTROL_PLANE_RELEASE_DOMAIN_PENDING_STATUS="0"
  # Delegate pre-commit HUP/INT/TERM to the root domain-aware handler, while a
  # post-commit signal is captured until evidence/cleanup is durable.
  trap 'control_plane_release_domain_handle_release_signal HUP' HUP
  trap 'control_plane_release_domain_handle_release_signal INT' INT
  trap 'control_plane_release_domain_handle_release_signal TERM' TERM
  trap 'control_plane_release_domain_capture_backup_abort false' USR1
  trap 'control_plane_release_domain_capture_backup_abort true' USR2
  CONTROL_PLANE_RELEASE_DOMAIN_SIGNAL_BOUNDARY_ACTIVE="true"
}

control_plane_release_domain_restore_signal_boundary() {
  [[ "${CONTROL_PLANE_RELEASE_DOMAIN_SIGNAL_BOUNDARY_ACTIVE:-false}" == "true" ]] || return 0
  trap 'handle_control_plane_release_signal HUP' HUP
  trap 'handle_control_plane_release_signal INT' INT
  trap 'handle_control_plane_release_signal TERM' TERM
  trap handle_control_plane_backup_coordination_abort USR1
  trap 'handle_control_plane_backup_coordination_abort true' USR2
  CONTROL_PLANE_RELEASE_DOMAIN_SIGNAL_BOUNDARY_ACTIVE="false"
}

# Fixed durable trace callback consumed by control_plane_release_domains.sh.
control_plane_release_trace() {
  local phase="$1"
  local state="$2"
  local pending_status=0

  (( $# == 2 )) || return 2
  case "${phase}:${state}" in
    transaction:started|transaction:succeeded|transaction:failed|\
    prepare:started|prepare:succeeded|prepare:failed|\
    apply:started|apply:succeeded|apply:failed|\
    verify:started|verify:succeeded|verify:failed|\
    rollback:started|rollback:succeeded|rollback:failed)
      ;;
    *) return 2 ;;
  esac
  case "${phase}:${state}" in
    rollback:*|transaction:failed)
      CONTROL_PLANE_RELEASE_DOMAIN_RECOVERY_ACTIVE="true"
      ;;
    *)
      if control_plane_release_domain_pending_signal; then
        pending_status="$(control_plane_release_domain_pending_status)"
        case "${phase}:${state}" in
          prepare:succeeded|apply:succeeded|verify:succeeded)
            # A signal can win after the phase callback returns but before its
            # success record. Persist a terminal failure for the already-
            # started phase, then return the signal status so the dispatcher
            # follows its normal failure/rollback edge and public trace remains
            # structurally complete.
            state="failed"
            ;;
          prepare:failed|apply:failed|verify:failed)
            # The active bounded command may have observed the signal and
            # returned nonzero before the dispatcher records its terminal
            # phase. Persist that explicit failure before rollback so the
            # trace remains structurally complete.
            ;;
          *) return "${pending_status}" ;;
        esac
      fi
      ;;
  esac
  control_plane_release_domain_validate_digest "${CONTROL_PLANE_RELEASE_DOMAIN_PLAN_DIGEST:-}" || return 2
  CONTROL_PLANE_RELEASE_DOMAIN_TRACE_SEQUENCE=$((CONTROL_PLANE_RELEASE_DOMAIN_TRACE_SEQUENCE + 1))
  (( CONTROL_PLANE_RELEASE_DOMAIN_TRACE_SEQUENCE <= 64 )) || return 2
  # Values are either fixed enums or validated lowercase digest/domain data,
  # so this is the exact field order emitted by json.Marshal(TraceEvent).
  printf '{"apiVersion":"release-transaction-trace.fugue.dev/v1","kind":"ReleaseTransactionTraceEvent","sequence":%s,"domain":"%s","planDigest":"%s","phase":"%s","state":"%s"}\n' \
    "${CONTROL_PLANE_RELEASE_DOMAIN_TRACE_SEQUENCE}" \
    "${CONTROL_PLANE_RELEASE_SELECTED_DOMAIN}" \
    "${CONTROL_PLANE_RELEASE_DOMAIN_PLAN_DIGEST}" \
    "${phase}" "${state}" >&17 || return
  control_plane_release_domain_verify_trace_identity false || return
  CONTROL_PLANE_RELEASE_DOMAIN_LAST_TRACE_PHASE="${phase}"
  CONTROL_PLANE_RELEASE_DOMAIN_LAST_TRACE_STATE="${state}"
  case "${phase}:${state}" in
    apply:started)
      CONTROL_PLANE_RELEASE_DOMAIN_WRITE_BOUNDARY_CROSSED="true"
      CONTROL_PLANE_RELEASE_DOMAIN_APPLY_STARTED="true"
      CONTROL_PLANE_RELEASE_MUTATION_OCCURRED="true"
      CONTROL_PLANE_RELEASE_ROLLBACK_REQUIRED="true"
      CONTROL_PLANE_RELEASE_PHASE="single-domain-apply"
      ;;
    rollback:started)
      CONTROL_PLANE_RELEASE_DOMAIN_ROLLBACK_ATTEMPTED="true"
      CONTROL_PLANE_RELEASE_ROLLBACK_ATTEMPTED="true"
      CONTROL_PLANE_RELEASE_RECOVERY_IN_PROGRESS="true"
      CONTROL_PLANE_RELEASE_ROLLBACK_IN_PROGRESS="true"
      CONTROL_PLANE_RELEASE_PHASE="single-domain-rollback"
      ;;
    rollback:succeeded)
      CONTROL_PLANE_RELEASE_DOMAIN_ROLLBACK_COMPLETED="true"
      CONTROL_PLANE_RELEASE_ROLLBACK_COMPLETED="true"
      CONTROL_PLANE_RELEASE_ROLLBACK_FAILED="false"
      CONTROL_PLANE_RELEASE_ROLLBACK_REQUIRED="false"
      CONTROL_PLANE_RELEASE_HELM_MUTATION_STARTED="false"
      CONTROL_PLANE_RELEASE_RECOVERY_IN_PROGRESS="false"
      CONTROL_PLANE_RELEASE_ROLLBACK_IN_PROGRESS="false"
      ;;
    rollback:failed)
      CONTROL_PLANE_RELEASE_DOMAIN_ROLLBACK_FAILED="true"
      CONTROL_PLANE_RELEASE_ROLLBACK_FAILED="true"
      CONTROL_PLANE_RELEASE_ROLLBACK_REQUIRED="true"
      CONTROL_PLANE_RELEASE_RECOVERY_IN_PROGRESS="false"
      CONTROL_PLANE_RELEASE_ROLLBACK_IN_PROGRESS="false"
      ;;
  esac
  if [[ "${phase}:${state}" == "transaction:succeeded" ]]; then
    # This assignment follows the fsynced terminal record and cannot fail. A
    # later signal loses the commit race and must never request rollback.
    CONTROL_PLANE_RELEASE_DOMAIN_COMMITTED="true"
    CONTROL_PLANE_RELEASE_DOMAIN_TRANSACTION_COMMITTED="true"
    CONTROL_PLANE_RELEASE_DOMAIN_TRANSACTION_ACTIVE="false"
    CONTROL_PLANE_RELEASE_COMMITTED="true"
    CONTROL_PLANE_RELEASE_ROLLBACK_REQUIRED="false"
    CONTROL_PLANE_RELEASE_HELM_MUTATION_STARTED="false"
    CONTROL_PLANE_RELEASE_PHASE="single-domain-committed"
  fi
  if (( pending_status != 0 )); then
    return "${pending_status}"
  fi
}

control_plane_release_trace_barrier() {
  (( $# == 0 )) || return 2
  if [[ "${CONTROL_PLANE_RELEASE_DOMAIN_RECOVERY_ACTIVE:-false}" != "true" ]] &&
    control_plane_release_domain_pending_signal; then
    return "$(control_plane_release_domain_pending_status)"
  fi
  control_plane_release_domain_verify_trace_identity true
}

# Production repository identity verification. Unit tests replace this exact
# read-only function after sourcing the library; production has no environment
# switch that can select another implementation.
control_plane_release_verify_repository_snapshot() {
  local base_commit="$1"
  local target_commit="$2"
  local repository_root=""
  local resolved_base=""
  local resolved_target=""
  local resolved_head=""

  control_plane_release_domain_validate_sha "${base_commit}" || return 2
  control_plane_release_domain_validate_sha "${target_commit}" || return 2
  repository_root="$(git rev-parse --show-toplevel 2>/dev/null)" || return 2
  repository_root="$(cd "${repository_root}" && pwd -P)" || return 2
  [[ "${repository_root}" == "${REPO_ROOT}" ]] || return 2
  resolved_base="$(git rev-parse --verify "${base_commit}^{commit}" 2>/dev/null)" || return 2
  resolved_target="$(git rev-parse --verify "${target_commit}^{commit}" 2>/dev/null)" || return 2
  resolved_head="$(git rev-parse --verify HEAD 2>/dev/null)" || return 2
  [[ "${resolved_base}" == "${base_commit}" &&
    "${resolved_target}" == "${target_commit}" &&
    "${resolved_head}" == "${target_commit}" ]] || return 2
  git diff --quiet --ignore-submodules -- || return 2
  git diff --cached --quiet --ignore-submodules -- || return 2
  [[ -z "$(git status --porcelain=v1 --untracked-files=all)" ]] || return 2
  git diff --quiet "${target_commit}" -- "${FUGUE_HELM_CHART_PATH}" || return 2
}

control_plane_release_domain_regenerate_changed_evidence() {
  [[ -x "${FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL}" ]] || return 2
  [[ ! -e "${CONTROL_PLANE_RELEASE_DOMAIN_CHANGED_EVIDENCE}" ]] || return 2
  "${FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL}" \
    --repo "${REPO_ROOT}" \
    --base "${FUGUE_RELEASE_DOMAIN_BASE_SHA}" \
    --target "${FUGUE_RELEASE_DOMAIN_TARGET_SHA}" \
    --output "${CONTROL_PLANE_RELEASE_DOMAIN_CHANGED_EVIDENCE}" || return
  [[ -f "${CONTROL_PLANE_RELEASE_DOMAIN_CHANGED_EVIDENCE}" &&
    ! -L "${CONTROL_PLANE_RELEASE_DOMAIN_CHANGED_EVIDENCE}" ]] || return 2
  chmod 600 "${CONTROL_PLANE_RELEASE_DOMAIN_CHANGED_EVIDENCE}" || return
}

control_plane_release_domain_classify_files() {
  local output=""
  local status=0

  if output="$("${FUGUE_RELEASE_DOMAIN_DISPATCH_TOOL}" classify-files \
    --ownership "${CONTROL_PLANE_RELEASE_DOMAIN_OWNERSHIP_FILE}" \
    --changed-evidence "${CONTROL_PLANE_RELEASE_DOMAIN_CHANGED_EVIDENCE}" \
    --trusted-base-commit "${FUGUE_RELEASE_DOMAIN_BASE_SHA}" \
    --trusted-target-commit "${FUGUE_RELEASE_DOMAIN_TARGET_SHA}")"; then
    status=0
  else
    status=$?
  fi
  case "${output}" in
    zero)
      [[ "${status}" == "0" ]] || return 2
      CONTROL_PLANE_RELEASE_DOMAIN_PRELIMINARY_OUTCOME="zero"
      CONTROL_PLANE_RELEASE_DOMAIN_SELECTED=""
      ;;
    $'single\tnode-local')
      [[ "${status}" == "0" ]] || return 2
      CONTROL_PLANE_RELEASE_DOMAIN_PRELIMINARY_OUTCOME="single"
      CONTROL_PLANE_RELEASE_DOMAIN_SELECTED="node-local"
      ;;
    $'single\tauthoritative-dns')
      [[ "${status}" == "0" ]] || return 2
      CONTROL_PLANE_RELEASE_DOMAIN_PRELIMINARY_OUTCOME="single"
      CONTROL_PLANE_RELEASE_DOMAIN_SELECTED="authoritative-dns"
      ;;
    $'single\tcontrol-plane')
      [[ "${status}" == "0" ]] || return 2
      CONTROL_PLANE_RELEASE_DOMAIN_PRELIMINARY_OUTCOME="single"
      CONTROL_PLANE_RELEASE_DOMAIN_SELECTED="control-plane"
      ;;
    $'single\timage-cache')
      [[ "${status}" == "0" ]] || return 2
      CONTROL_PLANE_RELEASE_DOMAIN_PRELIMINARY_OUTCOME="single"
      CONTROL_PLANE_RELEASE_DOMAIN_SELECTED="image-cache"
      ;;
    $'single\tbackup')
      [[ "${status}" == "0" ]] || return 2
      CONTROL_PLANE_RELEASE_DOMAIN_PRELIMINARY_OUTCOME="single"
      CONTROL_PLANE_RELEASE_DOMAIN_SELECTED="backup"
      ;;
    multiple|unknown)
      [[ "${status}" == "2" ]] || return 2
      CONTROL_PLANE_RELEASE_DOMAIN_PRELIMINARY_OUTCOME="${output}"
      CONTROL_PLANE_RELEASE_DOMAIN_SELECTED=""
      ;;
    *) return 2 ;;
  esac
}

control_plane_release_domain_set_preservation_modes() {
  CONTROL_PLANE_RELEASE_DOMAIN_DNS_ONLY="false"
  case "${CONTROL_PLANE_RELEASE_DOMAIN_SELECTED}" in
    node-local)
      FUGUE_PUBLIC_DATA_PLANE_RELEASE_MODE=preserve
      FUGUE_NODE_LOCAL_BUILD_PLANE_RELEASE_MODE=preserve
      FUGUE_STATEFUL_DEPENDENCY_RELEASE_MODE=guard
      FUGUE_MAINTENANCE_AGENT_RELEASE_MODE=preserve
      ;;
    authoritative-dns)
      FUGUE_PUBLIC_DATA_PLANE_RELEASE_MODE=allow
      FUGUE_NODE_LOCAL_BUILD_PLANE_RELEASE_MODE=preserve
      FUGUE_STATEFUL_DEPENDENCY_RELEASE_MODE=guard
      FUGUE_MAINTENANCE_AGENT_RELEASE_MODE=preserve
      CONTROL_PLANE_RELEASE_DOMAIN_DNS_ONLY="true"
      ;;
    control-plane)
      FUGUE_PUBLIC_DATA_PLANE_RELEASE_MODE=preserve
      FUGUE_NODE_LOCAL_BUILD_PLANE_RELEASE_MODE=preserve
      FUGUE_STATEFUL_DEPENDENCY_RELEASE_MODE=allow
      FUGUE_MAINTENANCE_AGENT_RELEASE_MODE=preserve
      ;;
    image-cache)
      FUGUE_PUBLIC_DATA_PLANE_RELEASE_MODE=preserve
      FUGUE_NODE_LOCAL_BUILD_PLANE_RELEASE_MODE=allow
      FUGUE_STATEFUL_DEPENDENCY_RELEASE_MODE=guard
      FUGUE_MAINTENANCE_AGENT_RELEASE_MODE=preserve
      ;;
    backup)
      FUGUE_PUBLIC_DATA_PLANE_RELEASE_MODE=preserve
      FUGUE_NODE_LOCAL_BUILD_PLANE_RELEASE_MODE=preserve
      FUGUE_STATEFUL_DEPENDENCY_RELEASE_MODE=allow
      FUGUE_MAINTENANCE_AGENT_RELEASE_MODE=preserve
      ;;
    "")
      # A blocked or zero preliminary result gets an all-preserve, read-only
      # attempt to produce a complete authorization bundle/public artifact.
      FUGUE_PUBLIC_DATA_PLANE_RELEASE_MODE=preserve
      FUGUE_NODE_LOCAL_BUILD_PLANE_RELEASE_MODE=preserve
      FUGUE_STATEFUL_DEPENDENCY_RELEASE_MODE=guard
      FUGUE_MAINTENANCE_AGENT_RELEASE_MODE=preserve
      ;;
    *) return 2 ;;
  esac
}

control_plane_release_domain_build_binding_args() {
  local postgres_name=""
  local postgres_secret=""

  node_local_dns_configure_cohort_names || return
  postgres_name="$(control_plane_postgres_name)" || return
  postgres_secret="${FUGUE_CONTROL_PLANE_POSTGRES_EXISTING_SECRET_NAME:-${postgres_name}-app}"
  CONTROL_PLANE_RELEASE_DOMAIN_BINDING_ARGS=(
    --binding "releaseName=${FUGUE_RELEASE_NAME}"
    --binding "releaseNamespace=${FUGUE_NAMESPACE}"
    --binding "nodeLocalNamespace=${FUGUE_NODE_LOCAL_DNS_NAMESPACE}"
    --binding "nodeLocalName=${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME}"
    --binding "nodeLocalUpstreamServiceName=${NODE_LOCAL_DNS_UPSTREAM_SERVICE_NAME}"
    --binding "nodeLocalActiveName=${NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME}"
    --binding "dnsName=${FUGUE_RELEASE_FULLNAME}-dns"
    --binding "apiName=${FUGUE_API_DEPLOYMENT_NAME}"
    --binding "controllerName=${FUGUE_CONTROLLER_DEPLOYMENT_NAME}"
    --binding "serviceName=${FUGUE_RELEASE_FULLNAME}"
    --binding "ingressName=${FUGUE_RELEASE_FULLNAME}"
    --binding "imageCacheName=${FUGUE_RELEASE_FULLNAME}-image-cache"
    --binding "controlPlanePostgresName=${postgres_name}"
    --binding "controlPlanePostgresSecretName=${postgres_secret}"
    --binding "controlPlaneRestoreDrillName=${FUGUE_RELEASE_FULLNAME}-control-plane-restore-drill"
  )
}

control_plane_release_domain_prepare_fixed_preservation() {
  local selected="${CONTROL_PLANE_RELEASE_DOMAIN_SELECTED}"
  local strict_build_preservation="false"

  # The planner owns classification. Never re-run the legacy changed-file
  # attribution path here: it can call fail/exit and it can select behavior
  # independently of the immutable single-domain plan. Instead, preserve every
  # non-selected shared workload from one read-only live snapshot.
  FUGUE_EDGE_HELM_IMAGE_REPOSITORY="${FUGUE_EDGE_IMAGE_REPOSITORY}"
  FUGUE_EDGE_HELM_IMAGE_TAG="${FUGUE_EDGE_IMAGE_TAG}"
  PUBLIC_DATA_PLANE_HELM_SET_ARGS=()
  NODE_LOCAL_BUILD_PLANE_HELM_SET_ARGS=()
  MAINTENANCE_AGENT_HELM_SET_ARGS=()

  case "${selected}" in
    authoritative-dns)
      # Desired authoritative-DNS values must remain visible to the renderer.
      # Any edge drift then classifies as an additional rendered domain and
      # blocks authorization instead of being silently hidden.
      ;;
    node-local|control-plane|image-cache|backup|"")
      preserve_public_data_plane_from_live || return
      ;;
    *) return 2 ;;
  esac

  case "${selected}" in
    image-cache) ;;
    node-local|authoritative-dns|control-plane|backup|"")
      if node_local_dns_split_release_enabled; then
        strict_build_preservation="true"
      fi
      preserve_node_local_build_plane_from_live \
        true true "${strict_build_preservation}" || return
      ;;
    *) return 2 ;;
  esac

  case "${selected}" in
    node-local|authoritative-dns|control-plane|image-cache|backup|"")
      preserve_maintenance_agents_from_live || return
      ;;
    *) return 2 ;;
  esac

  case "${selected}" in
    control-plane) ;;
    node-local|authoritative-dns|image-cache|backup|"")
      preserve_strict_drain_agent_image_from_live || return
      ;;
    *) return 2 ;;
  esac
}

control_plane_release_domain_prepare_render_inputs() {
  CONTROL_PLANE_RELEASE_DOMAIN_GATE_ACTIVE="true"
  control_plane_release_domain_run_budget_preflight pin || return
  control_plane_release_domain_prepare_fixed_preservation || return
  control_plane_release_domain_pending_signal &&
    return "$(control_plane_release_domain_pending_status)"

  # This helper is read-only while DOMAIN_GATE_ACTIVE=true: probe-Pod and host
  # functional checks are deferred to the selected NodeLocal Apply callback.
  if ! run_node_local_dns_phase_with_state_handoff \
    "release-domain pre-render inventory" prepare_node_local_dns_helm_args; then
    return 1
  fi
  control_plane_release_domain_prepare_registry_zero_preservation || return
  CONTROL_PLANE_RELEASE_DOMAIN_PINNED_BUILD_OVERRIDE="${NODE_LOCAL_BUILD_PLANE_PREFLIGHT_OVERRIDE_USED:-false}"
  write_upgrade_override_values || return
  build_dns_helm_set_args || return
  prepare_helm_post_renderer || return

  control_plane_release_domain_build_binding_args
}

control_plane_release_domain_prepare_registry_zero_preservation() {
  local replicas=""

  NODE_LOCAL_BUILD_PLANE_PREFLIGHT_OVERRIDE_USED="false"
  replicas="$(live_deployment_replicas "${FUGUE_REGISTRY_DEPLOYMENT_NAME}")" || return
  replicas="$(printf '%s' "${replicas}" | awk '{$1=$1; print}')"
  [[ -z "${replicas}" || "${replicas}" =~ ^(0|[1-9][0-9]*)$ ]] || return 1
  case "${replicas}" in
    0)
      # Registry is not an executable release domain. Preserve an intentional
      # live zero-replica state in the authorized render without depending on
      # the later platform-health preflight's mutable override output.
      if ! stateful_dependency_changed; then
        NODE_LOCAL_BUILD_PLANE_PREFLIGHT_OVERRIDE_USED="true"
      fi
      ;;
    *) ;;
  esac
}

control_plane_release_domain_release_preflight_worker() {
  local targets_file="$1"
  local override_file="$2"

  (( $# == 2 )) || return 2
  run_release_preflight || return
  case "${NODE_LOCAL_BUILD_PLANE_PREFLIGHT_OVERRIDE_USED:-false}" in
    true|false) ;;
    *) return 1 ;;
  esac
  umask 077
  printf '%s' "${NODE_LOCAL_DNS_PREFLIGHT_TARGET_NODES:-}" >"${targets_file}" || return
  printf '%s\n' "${NODE_LOCAL_BUILD_PLANE_PREFLIGHT_OVERRIDE_USED:-false}" >"${override_file}" || return
  chmod 600 "${targets_file}" "${override_file}"
}

control_plane_release_domain_run_release_preflight_handoff() {
  local mode="$1"
  local handoff_dir="${CONTROL_PLANE_RELEASE_DOMAIN_RUNTIME_TMP_DIR}/release-preflight-${mode}"
  local targets_file="${handoff_dir}/node-local-targets"
  local override_file="${handoff_dir}/build-plane-override"
  local observed_targets=""
  local observed_override=""
  local timeout_seconds="${FUGUE_RELEASE_PREFLIGHT_OUTER_TIMEOUT_SECONDS:-300}"

  [[ "${mode}" == "verify" ]] || return 2
  [[ ! -e "${handoff_dir}" ]] || return 2
  mkdir "${handoff_dir}" || return
  chmod 700 "${handoff_dir}" || return
  [[ "${timeout_seconds}" =~ ^[1-9][0-9]*$ ]] || return 2
  if ! control_plane_release_domain_with_private_tmp run_release_long_command \
    "${timeout_seconds}" "release-domain ${mode} release preflight" \
    control_plane_release_domain_release_preflight_worker \
    "${targets_file}" "${override_file}"; then
    return 1
  fi
  python3 - "${handoff_dir}" "${targets_file}" "${override_file}" <<'PY' || return
import os
import re
import stat
import sys

directory, targets_path, override_path = sys.argv[1:]
metadata = os.lstat(directory)
if (
    not stat.S_ISDIR(metadata.st_mode)
    or stat.S_ISLNK(metadata.st_mode)
    or stat.S_IMODE(metadata.st_mode) != 0o700
    or metadata.st_uid != os.geteuid()
):
    raise SystemExit(1)
for path in (targets_path, override_path):
    item = os.lstat(path)
    if (
        not stat.S_ISREG(item.st_mode)
        or stat.S_ISLNK(item.st_mode)
        or stat.S_IMODE(item.st_mode) != 0o600
        or item.st_uid != os.geteuid()
        or item.st_nlink != 1
        or item.st_size > 1048576
    ):
        raise SystemExit(1)
targets = open(targets_path, encoding="utf-8").read()
for target in targets.splitlines():
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", target):
        raise SystemExit(1)
if open(override_path, encoding="ascii").read().strip() not in {"true", "false"}:
    raise SystemExit(1)
PY
  observed_targets="$(<"${targets_file}")" || return
  observed_override="$(<"${override_file}")" || return
  rm -f "${targets_file}" "${override_file}" || return
  rmdir "${handoff_dir}" || return
  CONTROL_PLANE_RELEASE_DOMAIN_OBSERVED_BUILD_OVERRIDE="${observed_override}"
  [[ "${NODE_LOCAL_DNS_PREFLIGHT_TARGET_NODES:-}" == "${observed_targets}" &&
    "${CONTROL_PLANE_RELEASE_DOMAIN_PINNED_BUILD_OVERRIDE:-}" == \
      "${observed_override}" ]]
}

# Records the exact mutable file inputs referenced by the frozen argv. Tests
# may replace this fixed implementation; production has no selector for it.
control_plane_release_record_argv_input_identities() {
  python3 - "${CONTROL_PLANE_RELEASE_DOMAIN_SOURCE_ARGV_SNAPSHOT}" \
    "${CONTROL_PLANE_RELEASE_DOMAIN_ARGV_INPUT_IDENTITIES}" "${REPO_ROOT}" <<'PY'
import hashlib
import json
import os
import stat
import sys

snapshot, output, repository = map(os.path.abspath, sys.argv[1:])
raw = open(snapshot, "rb").read()
if not raw or not raw.endswith(b"\0"):
    raise SystemExit(1)
argv = [item.decode("utf-8") for item in raw[:-1].split(b"\0")]
paths = set()
index = 0
while index < len(argv):
    argument = argv[index]
    if argument in {"-f", "--values", "--post-renderer", "--ca-file", "--cert-file", "--key-file"}:
        index += 1
        if index >= len(argv):
            raise SystemExit(1)
        paths.add(argv[index])
    elif argument.startswith(("--values=", "--post-renderer=", "--ca-file=", "--cert-file=", "--key-file=")):
        paths.add(argument.split("=", 1)[1])
    elif argument == "--set-file":
        index += 1
        if index >= len(argv) or "=" not in argv[index]:
            raise SystemExit(1)
        paths.add(argv[index].split("=", 1)[1])
    elif argument.startswith("--set-file=") and "=" in argument[len("--set-file="):]:
        paths.add(argument.split("=", 2)[2])
    index += 1

records = []
for original in sorted(paths):
    absolute = os.path.abspath(original)
    resolved = os.path.realpath(absolute)
    if resolved != absolute or not os.path.isfile(absolute):
        raise SystemExit(1)
    metadata = os.lstat(absolute)
    parent = os.lstat(os.path.dirname(absolute))
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_nlink != 1
        or not stat.S_ISDIR(parent.st_mode)
        or stat.S_ISLNK(parent.st_mode)
        or parent.st_uid != os.geteuid()
        or stat.S_IMODE(parent.st_mode) & 0o077
    ):
        raise SystemExit(1)
    with open(absolute, "rb") as handle:
        data = handle.read()
        opened = os.fstat(handle.fileno())
    if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
        raise SystemExit(1)
    records.append({
        "path": absolute,
        "dev": metadata.st_dev,
        "ino": metadata.st_ino,
        "mode": stat.S_IMODE(metadata.st_mode),
        "uid": metadata.st_uid,
        "nlink": metadata.st_nlink,
        "size": metadata.st_size,
        "sha256": hashlib.sha256(data).hexdigest(),
    })
payload = {"version": 1, "files": records}
flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
fd = os.open(output, flags, 0o600)
with os.fdopen(fd, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
    handle.write("\n")
    handle.flush()
    os.fsync(handle.fileno())
PY
}

control_plane_release_verify_argv_input_identities() {
  python3 - "${CONTROL_PLANE_RELEASE_DOMAIN_ARGV_INPUT_IDENTITIES}" <<'PY'
import hashlib
import json
import os
import stat
import sys

manifest = os.path.abspath(sys.argv[1])
metadata = os.lstat(manifest)
if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_uid != os.geteuid() or metadata.st_nlink != 1:
    raise SystemExit(1)
with open(manifest, encoding="utf-8") as handle:
    document = json.load(handle)
if set(document) != {"version", "files"} or document["version"] != 1 or not isinstance(document["files"], list):
    raise SystemExit(1)
seen = set()
for record in document["files"]:
    if set(record) != {"path", "dev", "ino", "mode", "uid", "nlink", "size", "sha256"}:
        raise SystemExit(1)
    path = record["path"]
    if path in seen or os.path.realpath(path) != path:
        raise SystemExit(1)
    seen.add(path)
    current = os.lstat(path)
    parent = os.lstat(os.path.dirname(path))
    if not stat.S_ISREG(current.st_mode) or stat.S_ISLNK(current.st_mode) or not stat.S_ISDIR(parent.st_mode) or stat.S_ISLNK(parent.st_mode) or parent.st_uid != os.geteuid() or stat.S_IMODE(parent.st_mode) & 0o077:
        raise SystemExit(1)
    with open(path, "rb") as handle:
        data = handle.read()
        opened = os.fstat(handle.fileno())
    observed = {
        "path": path, "dev": current.st_dev, "ino": current.st_ino,
        "mode": stat.S_IMODE(current.st_mode), "uid": current.st_uid,
        "nlink": current.st_nlink, "size": current.st_size,
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    if observed != record or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
        raise SystemExit(1)
PY
}

control_plane_release_domain_private_render_runner() {
  local phase="$1"
  shift
  run_release_long_command \
    "${FUGUE_RELEASE_KUBERNETES_OPERATION_OUTER_TIMEOUT_SECONDS:-900}" \
    "release-domain private ${phase} render" "$@"
}

control_plane_release_domain_private_canonicalizer() {
  local live_revision="$1"
  local expected_version="$2"
  local release_name="$3"
  local release_namespace="$4"
  local hook_policy="$5"
  local base_raw="$6"
  local target_raw="$7"
  local repeated_target_raw="$8"
  local output_dir="$9"
  local -a target_args=()
  local -a repeated_args=()

  [[ "${hook_policy}" == "exclude-hooks" ]] || return 2
  target_args=(canonicalize-manifest --ownership "${CONTROL_PLANE_RELEASE_DOMAIN_OWNERSHIP_FILE}" \
    --input "${target_raw}" --input-format helm-release-json --namespace "${release_namespace}" \
    --release-name "${release_name}" --release-version "${expected_version}" --exclude-hooks \
    --output "${output_dir}/target.manifest")
  repeated_args=(canonicalize-manifest --ownership "${CONTROL_PLANE_RELEASE_DOMAIN_OWNERSHIP_FILE}" \
    --input "${repeated_target_raw}" --input-format helm-release-json --namespace "${release_namespace}" \
    --release-name "${release_name}" --release-version "${expected_version}" --exclude-hooks \
    --output "${output_dir}/repeated-target.manifest")
  "${FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL}" canonicalize-manifest \
    --ownership "${CONTROL_PLANE_RELEASE_DOMAIN_OWNERSHIP_FILE}" \
    --input "${base_raw}" --namespace "${release_namespace}" \
    --output "${output_dir}/base.manifest" || return
  "${FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL}" "${target_args[@]}" || return
  "${FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL}" "${repeated_args[@]}"
}

control_plane_release_domain_private_authorize_consumer() {
  local live_revision="$1"
  local release_name="$2"
  local release_namespace="$3"
  local base_manifest="$4"
  local target_manifest="$5"
  local repeated_manifest="$6"
	local target_revision=$((live_revision + 1))
	local status=0
	local -a operational_args=()

	if [[ -n "${CONTROL_PLANE_RELEASE_DOMAIN_ACTIVATION_REPORT:-}" ||
	  -n "${CONTROL_PLANE_RELEASE_DOMAIN_ACTIVATION_REPORT_DIGEST:-}" ]]; then
	  [[ -n "${CONTROL_PLANE_RELEASE_DOMAIN_ACTIVATION_REPORT:-}" &&
	    -n "${CONTROL_PLANE_RELEASE_DOMAIN_ACTIVATION_REPORT_DIGEST:-}" ]] || return 2
	  operational_args=(
	    --operational-report "${CONTROL_PLANE_RELEASE_DOMAIN_ACTIVATION_REPORT}"
	    --operational-report-digest "${CONTROL_PLANE_RELEASE_DOMAIN_ACTIVATION_REPORT_DIGEST}"
	  )
	fi

  rm -f "${CONTROL_PLANE_RELEASE_DOMAIN_AUTHORIZATION_RESULT}"
  if "${FUGUE_RELEASE_DOMAIN_DISPATCH_TOOL}" authorize \
    --ownership "${CONTROL_PLANE_RELEASE_DOMAIN_OWNERSHIP_FILE}" \
    --changed-evidence "${CONTROL_PLANE_RELEASE_DOMAIN_CHANGED_EVIDENCE}" \
    --trusted-base-commit "${FUGUE_RELEASE_DOMAIN_BASE_SHA}" \
    --trusted-target-commit "${FUGUE_RELEASE_DOMAIN_TARGET_SHA}" \
    --base-canonical-manifest "${base_manifest}" \
    --target-canonical-manifest "${target_manifest}" \
    --repeated-target-canonical-manifest "${repeated_manifest}" \
    --argv-snapshot "${CONTROL_PLANE_RELEASE_DOMAIN_SOURCE_ARGV_SNAPSHOT}" \
    --bundle-dir "${CONTROL_PLANE_RELEASE_DOMAIN_BUNDLE_DIR}" \
    --release-name "${release_name}" \
    --release-namespace "${release_namespace}" \
    --base-revision "${live_revision}" \
		--target-revision "${target_revision}" \
		--ignore-helm-test-hooks=false \
		${operational_args[@]+"${operational_args[@]}"} \
		"${CONTROL_PLANE_RELEASE_DOMAIN_BINDING_ARGS[@]}" \
    >"${CONTROL_PLANE_RELEASE_DOMAIN_AUTHORIZATION_RESULT}"; then
    status=0
  else
    status=$?
  fi
  chmod 600 "${CONTROL_PLANE_RELEASE_DOMAIN_AUTHORIZATION_RESULT}" || return
  case "${status}" in
    0) return 0 ;;
    2)
      # Multiple/unknown is a verified safe block. Preserve the completed
      # bundle and return success to the private render parent so it can emit
      # the strict public no-write artifact before returning 2/freeze.
      : >"${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/authorization.blocked"
      chmod 600 "${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/authorization.blocked" || return
      return 0
      ;;
    *) return "${status}" ;;
  esac
}

control_plane_release_domain_operational_report_digest() {
	(( $# == 1 )) || return 2
	python3 - "$1" <<'PY'
import json
import re
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    document = json.load(handle)
digest = document.get("digest")
if not isinstance(digest, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
    raise SystemExit(1)
print(digest)
PY
}

control_plane_release_domain_try_operational_activation() {
	local conservative_bundle="${CONTROL_PLANE_RELEASE_DOMAIN_BUNDLE_DIR}"
	local activated_bundle="${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/authorization-bundle-operational"
	local report_digest=""
	local result=""
	local outcome=""
	local selected=""
	local plan_digest=""

	[[ "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE}" == "apply" &&
	  -f "${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/authorization.blocked" ]] || return 2
	[[ -d "${conservative_bundle}" && ! -e "${activated_bundle}" ]] || return 2
	report_digest="$(control_plane_release_domain_operational_report_digest \
	  "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}")" || return 2
	control_plane_release_domain_validate_digest "${report_digest}" || return 2

	CONTROL_PLANE_RELEASE_DOMAIN_ACTIVATION_REPORT="${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}"
	CONTROL_PLANE_RELEASE_DOMAIN_ACTIVATION_REPORT_DIGEST="${report_digest}"
	CONTROL_PLANE_RELEASE_DOMAIN_BUNDLE_DIR="${activated_bundle}"
	if ! control_plane_release_domain_private_authorize_consumer \
	  "${PREVIOUS_REVISION}" "${FUGUE_RELEASE_NAME}" "${FUGUE_NAMESPACE}" \
	  "${conservative_bundle}/base-manifest.yaml" \
	  "${conservative_bundle}/target-manifest.yaml" \
	  "${conservative_bundle}/repeated-target-manifest.yaml"; then
	  CONTROL_PLANE_RELEASE_DOMAIN_BUNDLE_DIR="${conservative_bundle}"
	  unset CONTROL_PLANE_RELEASE_DOMAIN_ACTIVATION_REPORT
	  unset CONTROL_PLANE_RELEASE_DOMAIN_ACTIVATION_REPORT_DIGEST
	  return 2
	fi
	unset CONTROL_PLANE_RELEASE_DOMAIN_ACTIVATION_REPORT
	unset CONTROL_PLANE_RELEASE_DOMAIN_ACTIVATION_REPORT_DIGEST
	result="$(control_plane_release_domain_read_exact_result \
	  "${CONTROL_PLANE_RELEASE_DOMAIN_AUTHORIZATION_RESULT}")" || return 2
	case "${result}" in
	  $'single\tnode-local\t'sha256:*|$'single\tauthoritative-dns\t'sha256:*|\
	  $'single\tcontrol-plane\t'sha256:*|$'single\timage-cache\t'sha256:*|\
	  $'single\tbackup\t'sha256:*) ;;
	  *)
	    CONTROL_PLANE_RELEASE_DOMAIN_BUNDLE_DIR="${conservative_bundle}"
	    return 2
	    ;;
	esac
	IFS=$'\t' read -r outcome selected plan_digest <<<"${result}"
	[[ "${outcome}" == "single" && -n "${selected}" ]] || return 2
	control_plane_release_domain_validate_digest "${plan_digest}" || return 2
	CONTROL_PLANE_RELEASE_DOMAIN_SELECTED="${selected}"
	CONTROL_PLANE_RELEASE_DOMAIN_PLAN_DIGEST="${plan_digest}"
	CONTROL_PLANE_RELEASE_DOMAIN_PRELIMINARY_OUTCOME="single"
	control_plane_release_domain_verify_bundle_command || return
	rm -f "${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/authorization.blocked"
}

control_plane_release_domain_capture_frozen_argv() {
  (( $# >= 7 )) || return 2
  [[ ! -e "${CONTROL_PLANE_RELEASE_DOMAIN_SOURCE_ARGV_SNAPSHOT}" ]] || return 2
  (umask 077; printf '%s\0' "$@" >"${CONTROL_PLANE_RELEASE_DOMAIN_SOURCE_ARGV_SNAPSHOT}") || return
  chmod 600 "${CONTROL_PLANE_RELEASE_DOMAIN_SOURCE_ARGV_SNAPSHOT}" || return
  CONTROL_PLANE_RELEASE_DOMAIN_SOURCE_ARGV_CONTENT_IDENTITY="$(
    control_plane_release_domain_file_content_identity \
      "${CONTROL_PLANE_RELEASE_DOMAIN_SOURCE_ARGV_SNAPSHOT}"
  )" || return
  [[ "${CONTROL_PLANE_RELEASE_DOMAIN_SOURCE_ARGV_CONTENT_IDENTITY}" =~ ^[0-9]+:[0-9a-f]{64}$ ]] || return 1
  control_plane_release_record_argv_input_identities || return
  control_plane_release_verify_repository_snapshot \
    "${FUGUE_RELEASE_DOMAIN_BASE_SHA}" "${FUGUE_RELEASE_DOMAIN_TARGET_SHA}" || return
  control_plane_release_run_private_canonical_render_set \
    "${PREVIOUS_REVISION}" \
    control_plane_release_domain_private_render_runner \
    control_plane_release_domain_private_canonicalizer \
    control_plane_release_domain_private_authorize_consumer \
    "$@"
}

control_plane_release_domain_render_and_authorize() {
  with_frozen_control_plane_helm_upgrade_argv \
    "${UPGRADE_OVERRIDE_VALUES_FILE}" \
    control_plane_release_domain_capture_frozen_argv
}

control_plane_release_domain_materialize_operational_report() {
  local plan_file="${CONTROL_PLANE_RELEASE_DOMAIN_BUNDLE_DIR}/release-domain-plan.json"
  local plan_digest=""
  local report_output=""
  local target=""
  local source_base=""
  local artifact_digest=""
  local activation_output=""
  local -a targets=()
  local -a target_args=()
  local -a activation_artifact_args=()
  local -a image_plan_command=()
  local -a activation_plan_command=()

  [[ -f "${plan_file}" && ! -L "${plan_file}" ]] || return 2
  [[ ! -e "${CONTROL_PLANE_RELEASE_DOMAIN_OPERATIONAL_IMAGE_PLAN}" &&
    ! -L "${CONTROL_PLANE_RELEASE_DOMAIN_OPERATIONAL_IMAGE_PLAN}" ]] || return 2
  case "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE}" in
    prepare)
      [[ ! -e "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}" &&
        ! -L "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}" ]] || return 2
      report_output="${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}"
      [[ ! -e "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}" &&
        ! -L "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}" ]] || return 2
      activation_output="${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}"
      ;;
    apply)
      [[ -f "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}" &&
        ! -L "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}" ]] || return 2
      report_output="${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/operational-domain-evidence.apply.json"
      [[ ! -e "${report_output}" && ! -L "${report_output}" ]] || return 2
      [[ -d "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}" &&
        ! -L "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}" ]] || return 2
      activation_output="${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/build-activation-evidence.apply"
      [[ ! -e "${activation_output}" && ! -L "${activation_output}" ]] || return 2
      ;;
    *) return 2 ;;
  esac

  if [[ -n "${FUGUE_RELEASE_DOMAIN_IMAGE_TARGETS:-}" ]]; then
    read -r -a targets <<<"${FUGUE_RELEASE_DOMAIN_IMAGE_TARGETS}"
  fi
  for target in "${targets[@]-}"; do
    [[ -n "${target}" ]] || continue
    case "${target}" in
      api)
        source_base="${FUGUE_RELEASE_DOMAIN_API_IMAGE_BASE_SHA:-}"
        artifact_digest="${FUGUE_RELEASE_DOMAIN_API_IMAGE_DIGEST:-}"
        ;;
      controller)
        source_base="${FUGUE_RELEASE_DOMAIN_CONTROLLER_IMAGE_BASE_SHA:-}"
        artifact_digest="${FUGUE_RELEASE_DOMAIN_CONTROLLER_IMAGE_DIGEST:-}"
        ;;
      drain_agent)
        source_base="${FUGUE_RELEASE_DOMAIN_DRAIN_AGENT_IMAGE_BASE_SHA:-}"
        artifact_digest="${FUGUE_RELEASE_DOMAIN_DRAIN_AGENT_IMAGE_DIGEST:-}"
        ;;
      telemetry_agent)
        source_base="${FUGUE_RELEASE_DOMAIN_TELEMETRY_AGENT_IMAGE_BASE_SHA:-}"
        artifact_digest="${FUGUE_RELEASE_DOMAIN_TELEMETRY_AGENT_IMAGE_DIGEST:-}"
        ;;
      image_cache)
        source_base="${FUGUE_RELEASE_DOMAIN_IMAGE_CACHE_IMAGE_BASE_SHA:-}"
        artifact_digest="${FUGUE_RELEASE_DOMAIN_IMAGE_CACHE_IMAGE_DIGEST:-}"
        ;;
      edge)
        source_base="${FUGUE_RELEASE_DOMAIN_EDGE_IMAGE_BASE_SHA:-}"
        artifact_digest="${FUGUE_RELEASE_DOMAIN_EDGE_IMAGE_DIGEST:-}"
        ;;
      app_ssh)
        source_base="${FUGUE_RELEASE_DOMAIN_BASE_SHA}"
        artifact_digest="${FUGUE_RELEASE_DOMAIN_APP_SSH_IMAGE_DIGEST:-}"
        ;;
      *) return 2 ;;
    esac
    control_plane_release_domain_validate_sha "${source_base}" || return 2
    control_plane_release_domain_validate_digest "${artifact_digest}" || return 2
    target_args+=(--target "${target}=${source_base}=${artifact_digest}")
    activation_artifact_args+=(--artifact "${target}=${source_base}=${artifact_digest}")
  done

  image_plan_command=("${FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL}" operational-image-plan \
    --changed-evidence "${CONTROL_PLANE_RELEASE_DOMAIN_CHANGED_EVIDENCE}" \
    --trusted-base "${FUGUE_RELEASE_DOMAIN_BASE_SHA}" \
    --trusted-target "${FUGUE_RELEASE_DOMAIN_TARGET_SHA}")
  if (( ${#target_args[@]} > 0 )); then
    image_plan_command+=("${target_args[@]}")
  fi
  image_plan_command+=(--output "${CONTROL_PLANE_RELEASE_DOMAIN_OPERATIONAL_IMAGE_PLAN}")
  "${image_plan_command[@]}" || return

  plan_digest="$(python3 - "${plan_file}" <<'PY'
import json
import re
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    document = json.load(handle)
digest = document.get("planDigest")
if not isinstance(digest, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
    raise SystemExit(1)
print(digest)
PY
)" || return 2
  control_plane_release_domain_validate_digest "${plan_digest}" || return 2

  activation_plan_command=("${FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL}" image-activation-plans \
    --changed-evidence "${CONTROL_PLANE_RELEASE_DOMAIN_CHANGED_EVIDENCE}" \
    --ownership "${CONTROL_PLANE_RELEASE_DOMAIN_OWNERSHIP_FILE}" \
    --plan "${plan_file}" \
    --plan-digest "${plan_digest}" \
    --base-manifest "${CONTROL_PLANE_RELEASE_DOMAIN_BUNDLE_DIR}/base-manifest.yaml" \
    --target-manifest "${CONTROL_PLANE_RELEASE_DOMAIN_BUNDLE_DIR}/target-manifest.yaml" \
    --trusted-base "${FUGUE_RELEASE_DOMAIN_BASE_SHA}" \
    --trusted-target "${FUGUE_RELEASE_DOMAIN_TARGET_SHA}" \
    --provenance-digest "${FUGUE_RELEASE_DOMAIN_VERIFIED_IMAGE_ARTIFACTS_DIGEST}" \
    --output-dir "${activation_output}")
  if (( ${#activation_artifact_args[@]} > 0 )); then
    activation_plan_command+=("${activation_artifact_args[@]}")
  fi
  "${activation_plan_command[@]}" || return

  "${FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL}" operational-report \
    --changed-evidence "${CONTROL_PLANE_RELEASE_DOMAIN_CHANGED_EVIDENCE}" \
    --image-plan "${CONTROL_PLANE_RELEASE_DOMAIN_OPERATIONAL_IMAGE_PLAN}" \
    --plan "${plan_file}" \
    --plan-digest "${plan_digest}" \
    --trusted-base "${FUGUE_RELEASE_DOMAIN_BASE_SHA}" \
    --trusted-target "${FUGUE_RELEASE_DOMAIN_TARGET_SHA}" \
    --output "${report_output}" || return

  if [[ "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE}" == "apply" ]]; then
    control_plane_release_domain_compare_build_activation_reports \
      "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}" "${activation_output}" || return
    control_plane_release_domain_compare_uploaded_operational_report \
      "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}" "${report_output}"
  fi
}

control_plane_release_domain_read_exact_result() {
  local path="$1"
  local output=""

  [[ -f "${path}" && ! -L "${path}" ]] || return 2
  output="$(<"${path}")" || return 2
  printf '%s' "${output}"
}

control_plane_release_domain_verify_bundle_command() {
  local output=""
  local status=0

  rm -f "${CONTROL_PLANE_RELEASE_DOMAIN_VERIFY_RESULT}"
  if "${FUGUE_RELEASE_DOMAIN_DISPATCH_TOOL}" verify \
    --bundle-dir "${CONTROL_PLANE_RELEASE_DOMAIN_BUNDLE_DIR}" \
    >"${CONTROL_PLANE_RELEASE_DOMAIN_VERIFY_RESULT}"; then
    status=0
  else
    status=$?
  fi
  chmod 600 "${CONTROL_PLANE_RELEASE_DOMAIN_VERIFY_RESULT}" || return
  output="$(control_plane_release_domain_read_exact_result "${CONTROL_PLANE_RELEASE_DOMAIN_VERIFY_RESULT}")" || output=""
  case "${status}" in
    0)
      [[ "${output}" == $'single\t'"${CONTROL_PLANE_RELEASE_DOMAIN_SELECTED}"$'\t'"${CONTROL_PLANE_RELEASE_DOMAIN_PLAN_DIGEST}" ]] || return 2
      ;;
    2)
      [[ -z "${output}" ]] || return 2
      return 2
      ;;
    *) return "${status}" ;;
  esac
}

control_plane_release_domain_verify_open_argv_identity() {
  python3 - "${CONTROL_PLANE_RELEASE_DOMAIN_BUNDLE_DIR}/upgrade-argv.snapshot" 16 <<'PY'
import os
import stat
import sys
path = sys.argv[1]
opened = os.fstat(16)
at_path = os.lstat(path)
parent = os.lstat(os.path.dirname(path))
if (
    not stat.S_ISREG(opened.st_mode)
    or stat.S_ISLNK(at_path.st_mode)
    or (opened.st_dev, opened.st_ino) != (at_path.st_dev, at_path.st_ino)
    or stat.S_IMODE(opened.st_mode) != 0o600
    or opened.st_uid != os.geteuid()
    or opened.st_nlink != 1
    or not stat.S_ISDIR(parent.st_mode)
    or stat.S_ISLNK(parent.st_mode)
    or stat.S_IMODE(parent.st_mode) != 0o700
    or parent.st_uid != os.geteuid()
):
    raise SystemExit(1)
PY
}

control_plane_release_domain_open_argv_content_identity() {
  python3 - 16 <<'PY'
import hashlib
import os

metadata = os.fstat(16)
digest = hashlib.sha256()
offset = 0
while offset < metadata.st_size:
    chunk = os.pread(16, min(65536, metadata.st_size - offset), offset)
    if not chunk:
        raise SystemExit(1)
    digest.update(chunk)
    offset += len(chunk)
if offset != metadata.st_size:
    raise SystemExit(1)
print(f"{metadata.st_size}:{digest.hexdigest()}")
PY
}

control_plane_release_domain_file_content_identity() {
  python3 - "$1" <<'PY'
import hashlib
import os
import stat
import sys

path = os.path.abspath(sys.argv[1])
at_path = os.lstat(path)
flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
fd = os.open(path, flags)
try:
    opened = os.fstat(fd)
    if (
        not stat.S_ISREG(opened.st_mode)
        or stat.S_ISLNK(at_path.st_mode)
        or (opened.st_dev, opened.st_ino) != (at_path.st_dev, at_path.st_ino)
        or opened.st_uid != os.geteuid()
        or opened.st_nlink != 1
    ):
        raise SystemExit(1)
    digest = hashlib.sha256()
    size = 0
    while True:
        chunk = os.read(fd, 65536)
        if not chunk:
            break
        digest.update(chunk)
        size += len(chunk)
    if size != opened.st_size:
        raise SystemExit(1)
    print(f"{size}:{digest.hexdigest()}")
finally:
    os.close(fd)
PY
}

control_plane_release_domain_argv_array_content_identity() {
  (( $# > 0 )) || return 2
  printf '%s\0' "$@" | python3 -c '
import hashlib
import sys
data = sys.stdin.buffer.read()
print(f"{len(data)}:{hashlib.sha256(data).hexdigest()}")
'
}

# Final authorization callback. It opens the bundle argv before the second
# verify and keeps that exact verified inode on FD 16 for Apply.
control_plane_release_reverify_execution_authorization() {
  local expected_digest="$1"

  (( $# == 1 )) || return 2
  [[ "${expected_digest}" == "${CONTROL_PLANE_RELEASE_DOMAIN_PLAN_DIGEST}" ]] || return 2
  control_plane_release_domain_pending_signal &&
    return "$(control_plane_release_domain_pending_status)"
  control_plane_release_verify_repository_snapshot \
    "${FUGUE_RELEASE_DOMAIN_BASE_SHA}" "${FUGUE_RELEASE_DOMAIN_TARGET_SHA}" || return
  control_plane_release_verify_argv_input_identities || return
  control_plane_release_domain_verify_bundle_command || return
  { exec 16<&-; } 2>/dev/null || :
  exec 16<"${CONTROL_PLANE_RELEASE_DOMAIN_BUNDLE_DIR}/upgrade-argv.snapshot" || return
  control_plane_release_domain_verify_bundle_command || {
    exec 16<&-
    return 1
  }
  control_plane_release_domain_verify_open_argv_identity || {
    exec 16<&-
    return 1
  }
  CONTROL_PLANE_RELEASE_DOMAIN_AUTHORIZED_ARGV_CONTENT_IDENTITY="$(
    control_plane_release_domain_open_argv_content_identity
  )" || {
    exec 16<&-
    return 1
  }
  [[ "${CONTROL_PLANE_RELEASE_DOMAIN_AUTHORIZED_ARGV_CONTENT_IDENTITY}" =~ ^[0-9]+:[0-9a-f]{64}$ ]] || {
    exec 16<&-
    return 1
  }
  [[ -n "${CONTROL_PLANE_RELEASE_DOMAIN_SOURCE_ARGV_CONTENT_IDENTITY:-}" &&
    "${CONTROL_PLANE_RELEASE_DOMAIN_AUTHORIZED_ARGV_CONTENT_IDENTITY}" == \
      "${CONTROL_PLANE_RELEASE_DOMAIN_SOURCE_ARGV_CONTENT_IDENTITY}" ]] || {
    exec 16<&-
    return 1
  }
  CONTROL_PLANE_RELEASE_DOMAIN_ARGV_FD_READY="true"
  if control_plane_release_domain_pending_signal; then
    return "$(control_plane_release_domain_pending_status)"
  fi
  return 0
}

control_plane_release_domain_execute_sealed_helm_upgrade() {
  local argument=""
  local no_hooks=0
  local observed_content_identity=""
  local -a sealed_argv=()

  [[ "${CONTROL_PLANE_RELEASE_DOMAIN_ARGV_FD_READY:-false}" == "true" ]] || return 2
  # Fixed production checks are intentionally repeated immediately before the
  # Helm exec. No original in-memory frozen argv is consulted.
  control_plane_release_verify_repository_snapshot \
    "${FUGUE_RELEASE_DOMAIN_BASE_SHA}" "${FUGUE_RELEASE_DOMAIN_TARGET_SHA}" || return
  control_plane_release_verify_argv_input_identities || return
  control_plane_release_domain_verify_open_argv_identity || return
  while IFS= read -r -d '' argument <&16; do
    [[ -n "${argument}" ]] || return 2
    sealed_argv+=("${argument}")
  done
  exec 16<&-
  CONTROL_PLANE_RELEASE_DOMAIN_ARGV_FD_READY="false"
  [[ -z "${argument}" ]] || return 2
  (( ${#sealed_argv[@]} >= 7 )) || return 2
  observed_content_identity="$(
    control_plane_release_domain_argv_array_content_identity "${sealed_argv[@]}"
  )" || return
  [[ -n "${CONTROL_PLANE_RELEASE_DOMAIN_AUTHORIZED_ARGV_CONTENT_IDENTITY:-}" &&
    "${observed_content_identity}" == \
      "${CONTROL_PLANE_RELEASE_DOMAIN_AUTHORIZED_ARGV_CONTENT_IDENTITY}" ]] || return 1
  [[ "${sealed_argv[0]}" == "helm" && "${sealed_argv[1]}" == "upgrade" &&
    "${sealed_argv[2]}" == "${FUGUE_RELEASE_NAME}" ]] || return 2
  for argument in "${sealed_argv[@]}"; do
    [[ "${argument}" == "--no-hooks" ]] && no_hooks=$((no_hooks + 1))
  done
  (( no_hooks == 1 )) || return 2
  CONTROL_PLANE_RELEASE_DOMAIN_MANIFEST_MUTATION_POSSIBLE="true"
  CONTROL_PLANE_RELEASE_DOMAIN_HELM_EXECUTION_STARTED="true"
  CONTROL_PLANE_RELEASE_HELM_MUTATION_STARTED="true"
  CONTROL_PLANE_RELEASE_ROLLBACK_REQUIRED="true"
  CONTROL_PLANE_RELEASE_PHASE="single-domain-helm-upgrade"
  run_release_long_command "$(( $(duration_to_seconds "${FUGUE_HELM_TIMEOUT}") + 30 ))" \
    "single-domain Helm upgrade" "${sealed_argv[@]}" || return
  CONTROL_PLANE_RELEASE_DOMAIN_HELM_EXECUTED="true"
}

control_plane_release_domain_capture_live_canonical_manifest() {
  local revision="$1"
  local output="$2"
  local raw="${output}.raw"

  [[ "${revision}" =~ ^[1-9][0-9]*$ && ! -e "${output}" && ! -e "${raw}" ]] || return 2
  (umask 077; run_release_long_command \
    "${FUGUE_RELEASE_KUBERNETES_OPERATION_OUTER_TIMEOUT_SECONDS:-900}" \
    "release-domain live manifest revision ${revision}" \
    helm get all "${FUGUE_RELEASE_NAME}" -n "${FUGUE_NAMESPACE}" \
    --revision "${revision}" --template '{{ .Release.Manifest }}' >"${raw}") || return
  chmod 600 "${raw}" || return
  "${FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL}" canonicalize-manifest \
    --ownership "${CONTROL_PLANE_RELEASE_DOMAIN_OWNERSHIP_FILE}" \
    --input "${raw}" --namespace "${FUGUE_NAMESPACE}" --output "${output}"
}

control_plane_release_domain_verify_live_manifest() {
  local expected_revision="$1"
  local expected_bundle_name="$2"
  local label="$3"
  local current_revision=""
  local canonical="${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/live-${label}.manifest"

  current_revision="$(helm_current_revision)" || return
  [[ "${current_revision}" == "${expected_revision}" ]] || return 1
  control_plane_release_domain_capture_live_canonical_manifest \
    "${expected_revision}" "${canonical}" || return
  cmp -s "${canonical}" "${CONTROL_PLANE_RELEASE_DOMAIN_BUNDLE_DIR}/${expected_bundle_name}"
}

control_plane_release_domain_verify_current_manifest() {
  local expected_current_revision="$1"
  local expected_bundle_name="$2"
  local label="$3"
  local current_revision=""
  local canonical="${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/live-${label}.manifest"

  current_revision="$(helm_current_revision)" || return
  [[ "${current_revision}" =~ ^[1-9][0-9]*$ ]] || return 1
  if [[ -n "${expected_current_revision}" &&
    "${current_revision}" != "${expected_current_revision}" ]]; then
    return 1
  fi
  control_plane_release_domain_capture_live_canonical_manifest \
    "${current_revision}" "${canonical}" || return
  cmp -s "${canonical}" "${CONTROL_PLANE_RELEASE_DOMAIN_BUNDLE_DIR}/${expected_bundle_name}"
}

control_plane_release_domain_selected_uses_lease() {
  case "${CONTROL_PLANE_RELEASE_SELECTED_DOMAIN}" in
    control-plane|backup) return 0 ;;
    node-local|authoritative-dns|image-cache) return 1 ;;
    *) return 2 ;;
  esac
}

control_plane_release_domain_acquire_lease_and_fence() {
  control_plane_release_domain_selected_uses_lease || return 2
  CONTROL_PLANE_RELEASE_DOMAIN_USES_BACKUP_COORDINATION="true"
  acquire_control_plane_backup_coordination_lease || return
  CONTROL_PLANE_RELEASE_DOMAIN_LEASE_ACQUIRED="true"
  # drain can call the legacy fail helper. Keep that nonlocal exit inside a
  # subshell so the selected transaction still owns cleanup/rollback.
  (drain_control_plane_backup_before_schema_rollout) || return
  arm_control_plane_release_recovery_fence "single-domain-${CONTROL_PLANE_RELEASE_SELECTED_DOMAIN}" || return
}

control_plane_release_domain_mark_recovery_fence_resolved() {
  local disposition="$1"
  case "${disposition}" in committed-lease-released|rollback-lease-released) ;; *) return 2 ;; esac
  CONTROL_PLANE_RELEASE_RECOVERY_FENCE_REQUIRED="false"
  CONTROL_PLANE_RELEASE_RECOVERY_FENCE_ARMED_PHASE=""
  CONTROL_PLANE_RELEASE_RECOVERY_FENCE_DISPOSITION="${disposition}"
}

control_plane_release_domain_apply_control_plane_probes() {
  control_plane_canary_readiness_gate || return
  rollout_status "${FUGUE_API_DEPLOYMENT_NAME}" || return
  rollout_status "${FUGUE_CONTROLLER_DEPLOYMENT_NAME}" || return
  retry "${FUGUE_SMOKE_RETRIES}" "${FUGUE_SMOKE_DELAY_SECONDS}" smoke_test
}

control_plane_release_domain_apply_backup_probe() {
  [[ "${FUGUE_CONTROL_PLANE_POSTGRES_ENABLED:-false}" == "true" ]] || return 0
  [[ -n "$(control_plane_postgres_primary_pod_name)" ]]
}

control_plane_release_domain_apply_node_local() {
  CONTROL_PLANE_RELEASE_DOMAIN_APPLY_COMMAND_ENTERED="true"
  node_local_dns_run_deferred_operational_validation || return
  if [[ "${FUGUE_NODE_LOCAL_DNS_ENABLED:-false}" == "true" &&
    "${FUGUE_NODE_LOCAL_DNS_MODE:-}" == "shadow" ]]; then
    node_local_dns_shadow_host_preflight "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}" || return
  elif [[ "${FUGUE_NODE_LOCAL_DNS_ENABLED:-false}" != "true" &&
    "${NODE_LOCAL_DNS_PREVIOUS_ENABLED:-false}" == "true" ]]; then
    CONTROL_PLANE_RELEASE_DOMAIN_MANIFEST_MUTATION_POSSIBLE="true"
    run_node_local_dns_whole_phase "single-domain pre-Helm safe removal" \
      node_local_dns_delete_daemonset_safely "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}" || return
  fi
  control_plane_release_domain_execute_sealed_helm_upgrade || return
  if [[ "${FUGUE_NODE_LOCAL_DNS_ENABLED:-false}" == "true" ]]; then
    run_node_local_dns_phase_with_state_handoff "single-domain post-Helm reconcile" \
      node_local_dns_reconcile_after_helm || return
  elif [[ "${NODE_LOCAL_DNS_PREVIOUS_ENABLED:-false}" == "true" ]]; then
    run_node_local_dns_whole_phase "single-domain teardown verification" \
      node_local_dns_verify_teardown "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}" || return
  fi
  node_local_dns_verify_central_coredns_ready || return
  node_local_dns_verify_target_before_commit || return
  node_local_dns_verify_target_snapshot_unchanged
}

control_plane_release_domain_apply_authoritative_dns() {
  CONTROL_PLANE_RELEASE_DOMAIN_APPLY_COMMAND_ENTERED="true"
  control_plane_release_domain_execute_sealed_helm_upgrade || return
  run_dns_manifest_transaction_after_helm || return
  verify_dns_manifest_transaction_snapshot_before_commit
}

control_plane_release_domain_apply_control_plane() {
  CONTROL_PLANE_RELEASE_DOMAIN_APPLY_COMMAND_ENTERED="true"
  control_plane_release_domain_acquire_lease_and_fence || return
  control_plane_release_domain_with_private_tmp \
    run_control_plane_rollback_image_preflight || return
  control_plane_release_domain_execute_sealed_helm_upgrade || return
  control_plane_release_domain_apply_control_plane_probes
}

control_plane_release_domain_apply_image_cache() {
  CONTROL_PLANE_RELEASE_DOMAIN_APPLY_COMMAND_ENTERED="true"
  control_plane_release_domain_execute_sealed_helm_upgrade || return
  require_daemonset_present "${FUGUE_RELEASE_FULLNAME}-image-cache" || return
  image_cache_rollout_status "${FUGUE_RELEASE_FULLNAME}-image-cache"
}

control_plane_release_domain_apply_backup() {
  CONTROL_PLANE_RELEASE_DOMAIN_APPLY_COMMAND_ENTERED="true"
  control_plane_release_domain_acquire_lease_and_fence || return
  validate_live_api_backup_coordination_ready || return
  control_plane_release_domain_execute_sealed_helm_upgrade || return
  control_plane_release_domain_apply_backup_probe
}

control_plane_release_domain_verify_selected_target() {
  local target_revision=$((PREVIOUS_REVISION + 1))
  control_plane_release_domain_verify_live_manifest \
    "${target_revision}" target-manifest.yaml target
}

control_plane_release_domain_exact_helm_rollback_if_needed() {
  local current_revision=""
  local rollback_revision=""
  local target_revision=$((PREVIOUS_REVISION + 1))

  CONTROL_PLANE_RELEASE_DOMAIN_EXPECTED_ROLLBACK_CURRENT_REVISION=""
  current_revision="$(helm_current_revision)" || return
  case "${current_revision}" in
    "${PREVIOUS_REVISION}")
      if [[ "${CONTROL_PLANE_RELEASE_DOMAIN_HELM_EXECUTION_STARTED:-false}" == "true" ]]; then
        # Helm may have partially written Kubernetes resources before its
        # release record advances. Re-apply the already-authorized base
        # revision and require the one exact new Helm revision; inspecting the
        # old release record alone cannot prove live-resource restoration.
        run_release_long_command "$(( $(duration_to_seconds "${FUGUE_HELM_TIMEOUT}") + 30 ))" \
          "single-domain Helm base reapply" \
          helm rollback "${FUGUE_RELEASE_NAME}" "${PREVIOUS_REVISION}" \
          -n "${FUGUE_NAMESPACE}" --no-hooks --timeout "${FUGUE_HELM_TIMEOUT}" || return
        rollback_revision="$(helm_current_revision)" || return
        [[ "${rollback_revision}" == "${target_revision}" ]] || return 1
        CONTROL_PLANE_RELEASE_DOMAIN_EXPECTED_ROLLBACK_CURRENT_REVISION="${rollback_revision}"
      else
        # A selected-domain pre-Helm operation can fail before Helm starts.
        # Domain restoration plus an exact current base proof is sufficient.
        CONTROL_PLANE_RELEASE_DOMAIN_HELM_ROLLBACK_SKIPPED="true"
        CONTROL_PLANE_RELEASE_DOMAIN_EXPECTED_ROLLBACK_CURRENT_REVISION="${PREVIOUS_REVISION}"
      fi
      ;;
    "${target_revision}")
      run_release_long_command "$(( $(duration_to_seconds "${FUGUE_HELM_TIMEOUT}") + 30 ))" \
        "single-domain Helm rollback" \
        helm rollback "${FUGUE_RELEASE_NAME}" "${PREVIOUS_REVISION}" \
        -n "${FUGUE_NAMESPACE}" --no-hooks --timeout "${FUGUE_HELM_TIMEOUT}" || return
      rollback_revision="$(helm_current_revision)" || return
      [[ "${rollback_revision}" == "$((target_revision + 1))" ]] || return 1
      CONTROL_PLANE_RELEASE_DOMAIN_EXPECTED_ROLLBACK_CURRENT_REVISION="${rollback_revision}"
      ;;
    *)
      # Unreadable, stale, or concurrent revisions are never guessed.
      return 1
      ;;
  esac
}

control_plane_release_domain_validate_rollback_start_revision() {
  local current_revision=""
  local target_revision=$((PREVIOUS_REVISION + 1))

  current_revision="$(helm_current_revision)" || return
  case "${CONTROL_PLANE_RELEASE_DOMAIN_MANIFEST_MUTATION_POSSIBLE:-false}:${current_revision}" in
    false:"${PREVIOUS_REVISION}"|true:"${PREVIOUS_REVISION}"|true:"${target_revision}")
      CONTROL_PLANE_RELEASE_DOMAIN_ROLLBACK_START_REVISION="${current_revision}"
      ;;
    *) return 1 ;;
  esac
}

control_plane_release_domain_rollback_selected() {
  local rollback_status=0

  CONTROL_PLANE_RELEASE_DOMAIN_RECOVERY_ACTIVE="true"
  if [[ "${CONTROL_PLANE_RELEASE_DOMAIN_APPLY_COMMAND_ENTERED:-false}" != "true" ]]; then
    # Durable apply-started can precede a failed final authorization recheck.
    # In that case no adapter command ran, so selected rollback is an exact
    # read-only base proof rather than an unnecessary domain write.
    control_plane_release_domain_verify_current_manifest \
      "${PREVIOUS_REVISION}" base-manifest.yaml rollback-base
    return
  fi
  CONTROL_PLANE_RELEASE_DOMAIN_EXPECTED_ROLLBACK_CURRENT_REVISION="${PREVIOUS_REVISION}"
  # No selected cleanup or restore write is legal until the live Helm revision
  # is proven to be exactly the pre-release revision, or its one authorized
  # target successor when Helm may have started.
  control_plane_release_domain_validate_rollback_start_revision || return 1
  if [[ "${CONTROL_PLANE_RELEASE_SELECTED_DOMAIN}" == "node-local" &&
    "${NODE_LOCAL_DNS_PREVIOUS_ENABLED:-false}" != "true" &&
    "${FUGUE_NODE_LOCAL_DNS_ENABLED:-false}" == "true" &&
    "${CONTROL_PLANE_RELEASE_DOMAIN_MANIFEST_MUTATION_POSSIBLE:-false}" == "true" ]]; then
    run_node_local_dns_whole_phase "single-domain rollback interception removal" \
      node_local_dns_delete_daemonset_safely "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}" || rollback_status=1
  fi

  if [[ "${CONTROL_PLANE_RELEASE_DOMAIN_MANIFEST_MUTATION_POSSIBLE:-false}" == "true" ]]; then
    control_plane_release_domain_exact_helm_rollback_if_needed || rollback_status=1
  fi

  case "${CONTROL_PLANE_RELEASE_SELECTED_DOMAIN}" in
    node-local)
      if [[ "${NODE_LOCAL_DNS_PREVIOUS_ENABLED:-false}" == "true" ]]; then
        run_node_local_dns_whole_phase "single-domain previous cohort restoration" \
          node_local_dns_restore_previous_after_helm_rollback || rollback_status=1
      elif [[ "${FUGUE_NODE_LOCAL_DNS_ENABLED:-false}" == "true" ]]; then
        run_node_local_dns_whole_phase "single-domain rollback teardown verification" \
          node_local_dns_verify_teardown "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}" || rollback_status=1
      fi
      node_local_dns_verify_preserved_snapshot_after_helm_rollback || rollback_status=1
      ;;
    authoritative-dns)
      restore_dns_manifest_transaction_after_helm_rollback || rollback_status=1
      ;;
    control-plane)
      control_plane_release_domain_apply_control_plane_probes || rollback_status=1
      ;;
    image-cache)
      image_cache_restore_ondelete_after_helm_rollback || rollback_status=1
      ;;
    backup)
      control_plane_release_domain_apply_backup_probe || rollback_status=1
      ;;
    *) rollback_status=1 ;;
  esac

  if ! control_plane_release_domain_verify_current_manifest \
    "${CONTROL_PLANE_RELEASE_DOMAIN_EXPECTED_ROLLBACK_CURRENT_REVISION}" \
    base-manifest.yaml rollback-base; then
    rollback_status=1
  fi

  if control_plane_release_domain_selected_uses_lease; then
    if (( rollback_status == 0 )); then
      if release_control_plane_backup_coordination_lease; then
        CONTROL_PLANE_RELEASE_DOMAIN_LEASE_ACQUIRED="false"
        CONTROL_PLANE_RELEASE_DOMAIN_USES_BACKUP_COORDINATION="false"
        control_plane_release_domain_mark_recovery_fence_resolved \
          rollback-lease-released || rollback_status=1
      else
        rollback_status=1
      fi
    fi
  elif [[ "${CONTROL_PLANE_RELEASE_DOMAIN_LEASE_ACQUIRED:-false}" == "true" ]]; then
    # A non-Lease adapter can never inherit or release a Lease.
    rollback_status=1
  fi
  return "${rollback_status}"
}

control_plane_release_domain_prepare_common() {
  # Recompute both bounded gates after authorization and require their state
  # outputs to match the values already frozen into the rendered argv.
  control_plane_release_domain_run_budget_preflight verify || return
  control_plane_release_domain_run_release_preflight_handoff verify || return
  control_plane_release_domain_pending_signal &&
    return "$(control_plane_release_domain_pending_status)"

  case "${CONTROL_PLANE_RELEASE_SELECTED_DOMAIN}" in
    node-local|authoritative-dns)
      # This helper intentionally exports a complete wire-attestation envelope
      # consumed by the authoritative DNS transaction subprocess. It is
      # return-safe, so keep it in the parent shell rather than discarding the
      # attestation in a subshell.
      control_plane_release_domain_with_private_tmp \
        authoritative_dns_dig_preflight || return
      ;;
    control-plane|image-cache|backup) ;;
    *) return 2 ;;
  esac
}

control_plane_release_domain_run_budget_preflight() {
  local mode="$1"
  local handoff_dir="${CONTROL_PLANE_RELEASE_DOMAIN_RUNTIME_TMP_DIR}/budget-handoff-${mode}"
  local deadline_file="${handoff_dir}/deadline"
  local target_nodes_file="${handoff_dir}/node-local-targets"
  local rollback_budget_file="${handoff_dir}/node-local-rollback-budget"
  local deadline=""
  local rollback_budget=""
  local target_nodes=""

  case "${mode}" in pin|verify) ;; *) return 2 ;; esac
  [[ ! -e "${handoff_dir}" ]] || return 2
  mkdir "${handoff_dir}" || return
  chmod 700 "${handoff_dir}" || return
  if ! (
    umask 077
    control_plane_release_domain_with_private_tmp validate_control_plane_release_job_budget || exit $?
    control_plane_release_domain_with_private_tmp \
      validate_node_local_dns_release_budget_pre_mutation || exit $?
    printf '%s\n' "${CONTROL_PLANE_RELEASE_JOB_DEADLINE_EPOCH:-0}" >"${deadline_file}" || exit 1
    printf '%s' "${NODE_LOCAL_DNS_BUDGET_TARGET_NODES:-}" >"${target_nodes_file}" || exit 1
    printf '%s\n' "${NODE_LOCAL_DNS_ROLLBACK_BUDGET_SECONDS:-0}" >"${rollback_budget_file}" || exit 1
    chmod 600 "${deadline_file}" "${target_nodes_file}" "${rollback_budget_file}" || exit 1
  ); then
    return 1
  fi
  python3 - "${handoff_dir}" "${deadline_file}" \
    "${target_nodes_file}" "${rollback_budget_file}" <<'PY' || return
import os
import stat
import sys

directory, *files = sys.argv[1:]
metadata = os.lstat(directory)
if (
    not stat.S_ISDIR(metadata.st_mode)
    or stat.S_ISLNK(metadata.st_mode)
    or stat.S_IMODE(metadata.st_mode) != 0o700
    or metadata.st_uid != os.geteuid()
):
    raise SystemExit(1)
for path in files:
    item = os.lstat(path)
    if (
        not stat.S_ISREG(item.st_mode)
        or stat.S_ISLNK(item.st_mode)
        or stat.S_IMODE(item.st_mode) != 0o600
        or item.st_uid != os.geteuid()
        or item.st_nlink != 1
        or item.st_size > 1048576
    ):
        raise SystemExit(1)
PY
  deadline="$(<"${deadline_file}")" || return
  target_nodes="$(<"${target_nodes_file}")" || return
  rollback_budget="$(<"${rollback_budget_file}")" || return
  [[ "${deadline}" =~ ^[1-9][0-9]*$ &&
    "${rollback_budget}" =~ ^[0-9]+$ ]] || return 1
  rm -f "${deadline_file}" "${target_nodes_file}" "${rollback_budget_file}" || return
  rmdir "${handoff_dir}" || return
  case "${mode}" in
    pin)
      CONTROL_PLANE_RELEASE_JOB_DEADLINE_EPOCH="${deadline}"
      NODE_LOCAL_DNS_BUDGET_TARGET_NODES="${target_nodes}"
      NODE_LOCAL_DNS_ROLLBACK_BUDGET_SECONDS="${rollback_budget}"
      CONTROL_PLANE_RELEASE_DOMAIN_PINNED_JOB_DEADLINE="${deadline}"
      CONTROL_PLANE_RELEASE_DOMAIN_PINNED_BUDGET_TARGETS="${target_nodes}"
      CONTROL_PLANE_RELEASE_DOMAIN_PINNED_ROLLBACK_BUDGET="${rollback_budget}"
      ;;
    verify)
      [[ "${deadline}" == "${CONTROL_PLANE_RELEASE_DOMAIN_PINNED_JOB_DEADLINE:-}" &&
        "${target_nodes}" == "${CONTROL_PLANE_RELEASE_DOMAIN_PINNED_BUDGET_TARGETS:-}" &&
        "${rollback_budget}" == "${CONTROL_PLANE_RELEASE_DOMAIN_PINNED_ROLLBACK_BUDGET:-}" &&
        "${CONTROL_PLANE_RELEASE_JOB_DEADLINE_EPOCH:-}" == "${deadline}" &&
        "${NODE_LOCAL_DNS_BUDGET_TARGET_NODES:-}" == "${target_nodes}" &&
        "${NODE_LOCAL_DNS_ROLLBACK_BUDGET_SECONDS:-}" == "${rollback_budget}" ]]
      ;;
  esac
}

# Fixed literal callbacks selected by control_plane_release_domains.sh.
control_plane_release_adapter_node_local_prepare() {
  [[ "${CONTROL_PLANE_RELEASE_SELECTED_DOMAIN}" == "node-local" &&
    "${CONTROL_PLANE_RELEASE_DOMAIN_LEASE_ACQUIRED:-false}" != "true" ]] || return 2
  control_plane_release_domain_prepare_common
}
control_plane_release_adapter_node_local_apply() { control_plane_release_domain_apply_node_local; }
control_plane_release_adapter_node_local_verify() { control_plane_release_domain_verify_selected_target; }
control_plane_release_adapter_node_local_rollback() { control_plane_release_domain_rollback_selected; }

control_plane_release_adapter_authoritative_dns_prepare() {
  [[ "${CONTROL_PLANE_RELEASE_SELECTED_DOMAIN}" == "authoritative-dns" &&
    "${CONTROL_PLANE_RELEASE_DOMAIN_LEASE_ACQUIRED:-false}" != "true" ]] || return 2
  control_plane_release_domain_prepare_common || return
  control_plane_release_domain_with_private_tmp prepare_dns_manifest_transaction
}
control_plane_release_adapter_authoritative_dns_apply() { control_plane_release_domain_apply_authoritative_dns; }
control_plane_release_adapter_authoritative_dns_verify() {
  control_plane_release_domain_verify_selected_target || return
  finalize_dns_manifest_transaction
}
control_plane_release_adapter_authoritative_dns_rollback() { control_plane_release_domain_rollback_selected; }

control_plane_release_adapter_control_plane_prepare() {
  [[ "${CONTROL_PLANE_RELEASE_SELECTED_DOMAIN}" == "control-plane" ]] || return 2
  control_plane_release_domain_prepare_common || return
  control_plane_release_pre_helm_revision_unchanged
}
control_plane_release_adapter_control_plane_apply() { control_plane_release_domain_apply_control_plane; }
control_plane_release_adapter_control_plane_verify() { control_plane_release_domain_verify_selected_target; }
control_plane_release_adapter_control_plane_rollback() { control_plane_release_domain_rollback_selected; }

control_plane_release_adapter_image_cache_prepare() {
  [[ "${CONTROL_PLANE_RELEASE_SELECTED_DOMAIN}" == "image-cache" &&
    "${CONTROL_PLANE_RELEASE_DOMAIN_LEASE_ACQUIRED:-false}" != "true" ]] || return 2
  control_plane_release_domain_prepare_common || return
  # A preserved offline NodeLocal cohort makes any image-cache Pod-template
  # rollout a cross-domain operation. Block before Apply; never patch the
  # DaemonSet strategy from this adapter.
  ! node_local_dns_split_release_enabled || return 1
  require_daemonset_present "${FUGUE_RELEASE_FULLNAME}-image-cache"
}
control_plane_release_adapter_image_cache_apply() { control_plane_release_domain_apply_image_cache; }
control_plane_release_adapter_image_cache_verify() { control_plane_release_domain_verify_selected_target; }
control_plane_release_adapter_image_cache_rollback() { control_plane_release_domain_rollback_selected; }

control_plane_release_adapter_backup_prepare() {
  [[ "${CONTROL_PLANE_RELEASE_SELECTED_DOMAIN}" == "backup" ]] || return 2
  control_plane_release_domain_prepare_common
}
control_plane_release_adapter_backup_apply() { control_plane_release_domain_apply_backup; }
control_plane_release_adapter_backup_verify() { control_plane_release_domain_verify_selected_target; }
control_plane_release_adapter_backup_rollback() { control_plane_release_domain_rollback_selected; }

# EXIT-trap fallback for a genuinely nonlocal exit after durable apply-started.
# The normal dispatcher path remains the primary owner. Mark the attempt before
# any trace or adapter call so the legacy cleanup trap can never retry it.
control_plane_release_domain_emergency_rollback_once() {
  local selected="${CONTROL_PLANE_RELEASE_DOMAIN_SELECTED:-}"
  local rollback_status=0
  local -r CONTROL_PLANE_RELEASE_SELECTED_DOMAIN="${selected}"

  [[ "${CONTROL_PLANE_RELEASE_DOMAIN_GATE_ACTIVE:-false}" == "true" &&
    "${CONTROL_PLANE_RELEASE_DOMAIN_APPLY_STARTED:-false}" == "true" &&
    "${CONTROL_PLANE_RELEASE_DOMAIN_TRANSACTION_COMMITTED:-false}" != "true" ]] || return 2
  [[ "${CONTROL_PLANE_RELEASE_DOMAIN_ROLLBACK_ATTEMPTED:-false}" != "true" ]] || return 2
  case "${selected}" in
    node-local|authoritative-dns|control-plane|image-cache|backup) ;;
    *) return 2 ;;
  esac

  CONTROL_PLANE_RELEASE_DOMAIN_ROLLBACK_ATTEMPTED="true"
  CONTROL_PLANE_RELEASE_ROLLBACK_ATTEMPTED="true"
  CONTROL_PLANE_RELEASE_DOMAIN_RECOVERY_ACTIVE="true"
  CONTROL_PLANE_RELEASE_RECOVERY_IN_PROGRESS="true"
  CONTROL_PLANE_RELEASE_ROLLBACK_IN_PROGRESS="true"
  case "${CONTROL_PLANE_RELEASE_DOMAIN_LAST_TRACE_PHASE:-}:${CONTROL_PLANE_RELEASE_DOMAIN_LAST_TRACE_STATE:-}" in
    apply:started) control_plane_release_trace apply failed || : ;;
    verify:started) control_plane_release_trace verify failed || : ;;
  esac
  control_plane_release_trace rollback started || :
  if ! control_plane_release_domain_rollback_selected; then
    rollback_status=1
  fi
  if (( rollback_status == 0 )); then
    control_plane_release_trace rollback succeeded || :
  else
    control_plane_release_trace rollback failed || :
  fi
  control_plane_release_trace transaction failed || :
  control_plane_release_trace_barrier || :
  if [[ -e "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE:-}" ]]; then
    # The entrypoint requires a fresh destination. An existing file here is
    # therefore an unexpected provenance conflict, never evidence to reuse.
    rollback_status=1
  elif ! control_plane_release_domain_publish_bundle_evidence; then
    rollback_status=1
  fi
  control_plane_release_domain_cleanup_private_fds
  if control_plane_release_domain_private_recovery_required; then
    rollback_status=1
  elif ! control_plane_release_domain_cleanup_private_workdir; then
    rollback_status=1
  fi
  CONTROL_PLANE_RELEASE_DOMAIN_TRANSACTION_ACTIVE="false"
  if (( rollback_status == 0 )); then
    return 1
  fi
  return 2
}

control_plane_release_domain_validate_dependencies() {
  local name=""
  for name in \
    control_plane_release_dispatch_single_domain_transaction \
    control_plane_release_run_private_canonical_render_set \
    with_frozen_control_plane_helm_upgrade_argv \
    node_local_dns_configure_cohort_names \
    run_node_local_dns_phase_with_state_handoff prepare_node_local_dns_helm_args \
    write_upgrade_override_values build_dns_helm_set_args prepare_helm_post_renderer \
    control_plane_postgres_name helm_current_revision run_release_long_command \
    duration_to_seconds run_release_preflight authoritative_dns_dig_preflight \
    validate_control_plane_release_job_budget validate_node_local_dns_release_budget_pre_mutation \
    run_control_plane_rollback_image_preflight \
    terminate_active_control_plane_release_command \
    mark_control_plane_release_command_termination_unproven \
    mark_control_plane_release_committed_lease_unsafe \
    prepare_control_plane_backup_abort_state_for_dispatch \
    preserve_public_data_plane_from_live preserve_node_local_build_plane_from_live \
    preserve_maintenance_agents_from_live preserve_strict_drain_agent_image_from_live \
    live_deployment_replicas stateful_dependency_changed \
    finalize_dns_manifest_transaction write_dns_manifest_release_record_after_commit \
    cleanup_finalized_dns_manifest_snapshot; do
    control_plane_release_domain_require_function "${name}" || return
  done
}

control_plane_release_domain_publish_blocked_evidence() {
  if "${FUGUE_RELEASE_DOMAIN_DISPATCH_TOOL}" write-blocked-public-evidence \
    --ownership "${CONTROL_PLANE_RELEASE_DOMAIN_OWNERSHIP_FILE}" \
    --changed-evidence "${CONTROL_PLANE_RELEASE_DOMAIN_CHANGED_EVIDENCE}" \
    --trusted-base-commit "${FUGUE_RELEASE_DOMAIN_BASE_SHA}" \
    --trusted-target-commit "${FUGUE_RELEASE_DOMAIN_TARGET_SHA}" \
    --helm-revision "${PREVIOUS_REVISION}" \
    --namespace "${FUGUE_NAMESPACE}" \
    "${CONTROL_PLANE_RELEASE_DOMAIN_BINDING_ARGS[@]}" \
    --run-id "${GITHUB_RUN_ID}" \
    --run-attempt "${GITHUB_RUN_ATTEMPT}" \
    --head-sha "${FUGUE_RELEASE_DOMAIN_TARGET_SHA}" \
    --output "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}"; then
    return 0
  fi
  CONTROL_PLANE_RELEASE_DOMAIN_PUBLICATION_FAILED="true"
  if [[ -z "${CONTROL_PLANE_RELEASE_FAILURE_PHASE:-}" ]]; then
    CONTROL_PLANE_RELEASE_FAILURE_PHASE="single-domain-blocked-public-evidence"
  fi
  return 2
}

control_plane_release_domain_publish_bundle_evidence() {
  if "${FUGUE_RELEASE_DOMAIN_DISPATCH_TOOL}" write-public-evidence \
    --bundle-dir "${CONTROL_PLANE_RELEASE_DOMAIN_BUNDLE_DIR}" \
    --trace-file "${CONTROL_PLANE_RELEASE_DOMAIN_TRACE_FILE}" \
    --output "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" \
    --run-id "${GITHUB_RUN_ID}" \
    --run-attempt "${GITHUB_RUN_ATTEMPT}" \
    --head-sha "${FUGUE_RELEASE_DOMAIN_TARGET_SHA}" \
    --write-boundary-crossed="${CONTROL_PLANE_RELEASE_DOMAIN_WRITE_BOUNDARY_CROSSED:-false}" \
    --rollback-attempted="${CONTROL_PLANE_RELEASE_DOMAIN_ROLLBACK_ATTEMPTED:-false}" \
    --rollback-completed="${CONTROL_PLANE_RELEASE_DOMAIN_ROLLBACK_COMPLETED:-false}" \
    --rollback-failed="${CONTROL_PLANE_RELEASE_DOMAIN_ROLLBACK_FAILED:-false}"; then
    return 0
  fi
  CONTROL_PLANE_RELEASE_DOMAIN_PUBLICATION_FAILED="true"
  if [[ -z "${CONTROL_PLANE_RELEASE_FAILURE_PHASE:-}" ]]; then
    CONTROL_PLANE_RELEASE_FAILURE_PHASE="single-domain-public-evidence"
  fi
  return 2
}

control_plane_release_domain_post_commit() {
  local status=0
  local dns_snapshot_file="${DNS_MANIFEST_SNAPSHOT_FILE:-}"
  local dns_target_file="${DNS_MANIFEST_TARGET_STATE_FILE:-}"
  local dns_identity_file="${DNS_MANIFEST_HANDOFF_IDENTITY_FILE:-}"
  local dns_transaction_dir="${DNS_MANIFEST_TRANSACTION_DIR:-}"

  case "${CONTROL_PLANE_RELEASE_DOMAIN_SELECTED}" in
    control-plane|backup)
      if release_control_plane_backup_coordination_lease; then
        CONTROL_PLANE_RELEASE_DOMAIN_LEASE_ACQUIRED="false"
        CONTROL_PLANE_RELEASE_DOMAIN_USES_BACKUP_COORDINATION="false"
        control_plane_release_domain_mark_recovery_fence_resolved \
          committed-lease-released || status=2
      else
        mark_control_plane_release_committed_lease_unsafe || :
        status=2
      fi
      ;;
    authoritative-dns)
      if ! write_dns_manifest_release_record_after_commit; then
        DNS_MANIFEST_SNAPSHOT_KEEP="true"
        [[ -n "${CONTROL_PLANE_RELEASE_FAILURE_PHASE:-}" ]] ||
          CONTROL_PLANE_RELEASE_FAILURE_PHASE="single-domain-committed-dns-record"
        status=2
      fi
      if (( status == 0 )); then
        cleanup_finalized_dns_manifest_snapshot || status=2
      fi
      if (( status == 0 )) && {
        [[ ( "${DNS_MANIFEST_TRANSACTION_REQUIRED:-false}" == "true" &&
            "${DNS_MANIFEST_TRANSACTION_FINALIZED:-false}" != "true" ) ||
          "${DNS_MANIFEST_SNAPSHOT_KEEP:-false}" == "true" ||
          -n "${DNS_MANIFEST_SNAPSHOT_FILE:-}" ||
          -n "${DNS_MANIFEST_TARGET_STATE_FILE:-}" ||
          -n "${DNS_MANIFEST_HANDOFF_IDENTITY_FILE:-}" ||
          -n "${DNS_MANIFEST_TRANSACTION_DIR:-}" ]] ||
          { [[ -n "${dns_snapshot_file}" &&
              ( -e "${dns_snapshot_file}" || -L "${dns_snapshot_file}" ) ]] ||
            [[ -n "${dns_target_file}" &&
              ( -e "${dns_target_file}" || -L "${dns_target_file}" ) ]] ||
            [[ -n "${dns_identity_file}" &&
              ( -e "${dns_identity_file}" || -L "${dns_identity_file}" ) ]] ||
            [[ -n "${dns_transaction_dir}" &&
              ( -e "${dns_transaction_dir}" || -L "${dns_transaction_dir}" ) ]]; }
      }; then
        DNS_MANIFEST_SNAPSHOT_KEEP="true"
        [[ -n "${CONTROL_PLANE_RELEASE_FAILURE_PHASE:-}" ]] ||
          CONTROL_PLANE_RELEASE_FAILURE_PHASE="single-domain-committed-dns-cleanup"
        status=2
      fi
      ;;
    node-local|image-cache) ;;
    *) status=1 ;;
  esac
  (( status == 0 )) || return 2
  return 0
}

control_plane_release_run_atomic_domain_release() {
  local authorize_result=""
  local dispatch_status=0
  local final_status=0
  local prepare_complete="false"

  (( $# == 0 )) || return 2
  CONTROL_PLANE_RELEASE_DOMAIN_OWNERSHIP_FILE="${REPO_ROOT}/deploy/release-domains/ownership-v1.yaml"
  for required in \
    FUGUE_RELEASE_DOMAIN_BASE_SHA FUGUE_RELEASE_DOMAIN_TARGET_SHA \
    FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL FUGUE_RELEASE_DOMAIN_DISPATCH_TOOL \
    FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE \
    FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE \
    FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR \
    FUGUE_RELEASE_DOMAIN_VERIFIED_IMAGE_ARTIFACTS_DIGEST \
    FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE GITHUB_RUN_ID GITHUB_RUN_ATTEMPT; do
    [[ -n "${!required:-}" ]] || {
      control_plane_release_domain_production_error "${required} is required"
      return 2
    }
  done
  control_plane_release_domain_validate_sha "${FUGUE_RELEASE_DOMAIN_BASE_SHA}" || return 2
  control_plane_release_domain_validate_sha "${FUGUE_RELEASE_DOMAIN_TARGET_SHA}" || return 2
  [[ -x "${FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL}" &&
    -x "${FUGUE_RELEASE_DOMAIN_DISPATCH_TOOL}" &&
    -f "${CONTROL_PLANE_RELEASE_DOMAIN_OWNERSHIP_FILE}" ]] || return 2
  [[ "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}" == /* &&
    "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}" != "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" ]] || return 2
  [[ "$(dirname "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}")" == "$(dirname "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}")" ]] || return 2
  [[ "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}" == /* &&
    "$(dirname "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}")" == "$(dirname "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}")" &&
    "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}" != "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" &&
    "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}" != "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}" ]] || return 2
  control_plane_release_domain_validate_digest \
    "${FUGUE_RELEASE_DOMAIN_VERIFIED_IMAGE_ARTIFACTS_DIGEST}" || return 2
  control_plane_release_domain_validate_operational_phase || return 2
  [[ "${PREVIOUS_REVISION:-}" =~ ^[1-9][0-9]*$ ]] || return 2
  control_plane_release_domain_validate_dependencies || return
  control_plane_release_verify_repository_snapshot \
    "${FUGUE_RELEASE_DOMAIN_BASE_SHA}" "${FUGUE_RELEASE_DOMAIN_TARGET_SHA}" || return
  control_plane_release_domain_prepare_public_parent || return
  if control_plane_release_domain_setup_private_workdir; then
    :
  else
    final_status=$?
    control_plane_release_domain_cleanup_private_fds
    control_plane_release_domain_cleanup_private_workdir || :
    return "${final_status}"
  fi
  if control_plane_release_domain_install_signal_boundary; then
    :
  else
    final_status=$?
    control_plane_release_domain_cleanup_private_fds
    control_plane_release_domain_cleanup_private_workdir || :
    return "${final_status}"
  fi
  CONTROL_PLANE_RELEASE_DOMAIN_GATE_ACTIVE="true"
  CONTROL_PLANE_RELEASE_DOMAIN_TRANSACTION_ACTIVE="false"
  CONTROL_PLANE_RELEASE_DOMAIN_TRANSACTION_COMMITTED="false"
  CONTROL_PLANE_RELEASE_DOMAIN_APPLY_STARTED="false"
  CONTROL_PLANE_RELEASE_DOMAIN_USES_BACKUP_COORDINATION="false"
  CONTROL_PLANE_RELEASE_DOMAIN_WRITE_BOUNDARY_CROSSED="false"
  CONTROL_PLANE_RELEASE_DOMAIN_ROLLBACK_ATTEMPTED="false"
  CONTROL_PLANE_RELEASE_DOMAIN_ROLLBACK_COMPLETED="false"
  CONTROL_PLANE_RELEASE_DOMAIN_ROLLBACK_FAILED="false"
  CONTROL_PLANE_RELEASE_DOMAIN_PUBLICATION_FAILED="false"
  CONTROL_PLANE_RELEASE_ROLLBACK_ATTEMPTED="false"
  CONTROL_PLANE_RELEASE_ROLLBACK_COMPLETED="false"
  CONTROL_PLANE_RELEASE_ROLLBACK_FAILED="false"
  CONTROL_PLANE_RELEASE_COMMITTED="false"
  CONTROL_PLANE_RELEASE_DOMAIN_COMMITTED="false"
  CONTROL_PLANE_RELEASE_DOMAIN_LAST_TRACE_PHASE=""
  CONTROL_PLANE_RELEASE_DOMAIN_LAST_TRACE_STATE=""

  if control_plane_release_domain_regenerate_changed_evidence; then
    if control_plane_release_domain_classify_files; then
      if control_plane_release_domain_build_binding_args; then
        case "${CONTROL_PLANE_RELEASE_DOMAIN_PRELIMINARY_OUTCOME}" in
          multiple|unknown|zero|single)
            if control_plane_release_domain_set_preservation_modes; then
              if control_plane_release_domain_with_private_tmp \
                control_plane_release_domain_prepare_render_inputs; then
                control_plane_release_domain_render_and_authorize || final_status=$?
              else
                final_status=$?
              fi
            else
              final_status=$?
            fi
            ;;
          *) final_status=2 ;;
        esac
      else
        final_status=$?
      fi
    else
      final_status=$?
    fi
  else
    final_status=$?
  fi

	if (( final_status == 0 )); then
		if ! control_plane_release_domain_materialize_operational_report; then
			final_status=2
		fi
	fi

	if (( final_status == 0 )) &&
	  [[ "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE}" == "apply" ]] &&
	  [[ -f "${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/authorization.blocked" ]]; then
		# Activation is best-effort within the fail-closed boundary. Any invalid,
		# incomplete, or non-single report leaves the conservative block intact;
		# the normal no-write evidence path below then returns status 2.
		control_plane_release_domain_try_operational_activation || :
	fi

  if (( final_status == 0 )) &&
    [[ "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE}" == "prepare" ]] &&
    [[ ! -f "${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/authorization.blocked" ]]; then
    authorize_result="$(control_plane_release_domain_read_exact_result \
      "${CONTROL_PLANE_RELEASE_DOMAIN_AUTHORIZATION_RESULT}")" || final_status=2
    if (( final_status == 0 )); then
      case "${authorize_result}" in
        $'zero\t'sha256:*)
          IFS=$'\t' read -r outcome CONTROL_PLANE_RELEASE_DOMAIN_PLAN_DIGEST <<<"${authorize_result}"
          control_plane_release_domain_validate_digest \
            "${CONTROL_PLANE_RELEASE_DOMAIN_PLAN_DIGEST}" || final_status=2
          [[ "${CONTROL_PLANE_RELEASE_DOMAIN_PRELIMINARY_OUTCOME}" == "zero" ]] || final_status=2
          ;;
        $'single\tnode-local\t'sha256:*|$'single\tauthoritative-dns\t'sha256:*|\
        $'single\tcontrol-plane\t'sha256:*|$'single\timage-cache\t'sha256:*|\
        $'single\tbackup\t'sha256:*)
          IFS=$'\t' read -r outcome CONTROL_PLANE_RELEASE_DOMAIN_SELECTED \
            CONTROL_PLANE_RELEASE_DOMAIN_PLAN_DIGEST <<<"${authorize_result}"
          control_plane_release_domain_validate_digest \
            "${CONTROL_PLANE_RELEASE_DOMAIN_PLAN_DIGEST}" || final_status=2
          [[ "${CONTROL_PLANE_RELEASE_DOMAIN_PRELIMINARY_OUTCOME}" == "single" &&
            -n "${CONTROL_PLANE_RELEASE_DOMAIN_SELECTED}" ]] || final_status=2
          if (( final_status == 0 )); then
            control_plane_release_domain_verify_bundle_command || final_status=$?
          fi
          ;;
        *) final_status=2 ;;
      esac
    fi
    if (( final_status == 0 )); then
      prepare_complete="true"
    fi
  fi

	if (( final_status == 0 )) && [[ "${prepare_complete}" != "true" ]]; then
		if [[ -f "${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/authorization.blocked" ]]; then
			if [[ "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE}" == "prepare" ]]; then
				# Prepare is read-only. Returning success after the report is
				# materialized lets the pinned upload finish and apply rederive the
				# evidence. Final public evidence belongs to apply, so prepare must
				# not leave a stale publication at the shared path.
				final_status=0
			elif control_plane_release_domain_publish_bundle_evidence; then
				final_status=2
			else
				final_status=2
      fi
    else
      authorize_result="$(control_plane_release_domain_read_exact_result \
        "${CONTROL_PLANE_RELEASE_DOMAIN_AUTHORIZATION_RESULT}")" || final_status=2
      if (( final_status == 0 )); then
        case "${authorize_result}" in
          $'zero\t'sha256:*)
            IFS=$'\t' read -r outcome CONTROL_PLANE_RELEASE_DOMAIN_PLAN_DIGEST <<<"${authorize_result}"
            control_plane_release_domain_validate_digest \
              "${CONTROL_PLANE_RELEASE_DOMAIN_PLAN_DIGEST}" || final_status=2
            [[ "${CONTROL_PLANE_RELEASE_DOMAIN_PRELIMINARY_OUTCOME}" == "zero" ]] || final_status=2
            if (( final_status == 0 )); then
              # A zero bundle has no transaction, so its private trace remains
              # the canonical empty file and every write/rollback flag is false.
              control_plane_release_domain_publish_bundle_evidence || final_status=2
            fi
            ;;
          $'single\tnode-local\t'sha256:*|$'single\tauthoritative-dns\t'sha256:*|\
          $'single\tcontrol-plane\t'sha256:*|$'single\timage-cache\t'sha256:*|\
          $'single\tbackup\t'sha256:*) ;;
          *) final_status=2 ;;
        esac
      fi
      if (( final_status == 0 )) && [[ "${authorize_result}" == single$'\t'* ]]; then
        IFS=$'\t' read -r outcome CONTROL_PLANE_RELEASE_DOMAIN_SELECTED \
          CONTROL_PLANE_RELEASE_DOMAIN_PLAN_DIGEST <<<"${authorize_result}"
        [[ "${outcome}" == "single" ]] || final_status=2
        control_plane_release_domain_validate_digest \
          "${CONTROL_PLANE_RELEASE_DOMAIN_PLAN_DIGEST}" || final_status=2
        [[ "${CONTROL_PLANE_RELEASE_DOMAIN_PRELIMINARY_OUTCOME}" == "single" &&
          -n "${CONTROL_PLANE_RELEASE_DOMAIN_SELECTED}" ]] || final_status=2
      fi
      if (( final_status == 0 )) && [[ "${authorize_result}" == single$'\t'* ]]; then
        control_plane_release_domain_verify_bundle_command || final_status=$?
      fi
      if (( final_status == 0 )) && [[ "${authorize_result}" == single$'\t'* ]]; then
        CONTROL_PLANE_RELEASE_DOMAIN_HELM_EXECUTED="false"
        CONTROL_PLANE_RELEASE_DOMAIN_HELM_EXECUTION_STARTED="false"
        CONTROL_PLANE_RELEASE_DOMAIN_MANIFEST_MUTATION_POSSIBLE="false"
        CONTROL_PLANE_RELEASE_DOMAIN_APPLY_COMMAND_ENTERED="false"
        CONTROL_PLANE_RELEASE_DOMAIN_LEASE_ACQUIRED="false"
        CONTROL_PLANE_RELEASE_DOMAIN_AUTHORIZED_ARGV_CONTENT_IDENTITY=""
        case "${CONTROL_PLANE_RELEASE_DOMAIN_SELECTED}" in
          control-plane|backup)
            CONTROL_PLANE_RELEASE_DOMAIN_USES_BACKUP_COORDINATION="true"
            ;;
          node-local|authoritative-dns|image-cache)
            CONTROL_PLANE_RELEASE_DOMAIN_USES_BACKUP_COORDINATION="false"
            ;;
          *) final_status=2 ;;
        esac
        if (( final_status == 0 )); then
          CONTROL_PLANE_RELEASE_DOMAIN_TRANSACTION_ACTIVE="true"
          if control_plane_release_domain_with_private_tmp \
            control_plane_release_dispatch_single_domain_transaction \
            single "${CONTROL_PLANE_RELEASE_DOMAIN_SELECTED}" \
            "${CONTROL_PLANE_RELEASE_DOMAIN_PLAN_DIGEST}"; then
            dispatch_status=0
          else
            dispatch_status=$?
          fi
          CONTROL_PLANE_RELEASE_DOMAIN_TRANSACTION_ACTIVE="false"
        else
          dispatch_status="${final_status}"
        fi
        if (( dispatch_status == 0 )); then
          control_plane_release_domain_post_commit || final_status=$?
        else
          final_status="${dispatch_status}"
        fi
        if control_plane_release_domain_publish_bundle_evidence; then
          :
        else
          final_status=2
        fi
      fi
    fi
  fi

  # Never let the legacy EXIT/signal path invoke its cross-domain rollback.
  CONTROL_PLANE_RELEASE_ROLLBACK_REQUIRED="false"
  CONTROL_PLANE_RELEASE_HELM_MUTATION_STARTED="false"
  control_plane_release_domain_cleanup_private_fds
  if control_plane_release_domain_private_recovery_required; then
    :
  elif ! control_plane_release_domain_cleanup_private_workdir; then
    final_status=2
  fi
  control_plane_release_domain_restore_signal_boundary || final_status=2
  if [[ -n "${CONTROL_PLANE_RELEASE_DOMAIN_PENDING_SIGNAL:-${CONTROL_PLANE_RELEASE_PENDING_SIGNAL:-}}" &&
    ( "${final_status}" == "0" || "${final_status}" == "1" ) ]]; then
    final_status="$(control_plane_release_domain_pending_status)"
  fi
  return "${final_status}"
}
