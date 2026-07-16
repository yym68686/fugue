#!/usr/bin/env bash

# Single-domain control-plane release transaction seam.
#
# This file is source-only and deliberately registers no production adapter.
# The activation boundary must provide the fixed callbacks described below and
# call control_plane_release_dispatch_single_domain_transaction only after the
# release-domain plan and rollback ownership proof have been verified.
#
# Required fixed callbacks:
#   control_plane_release_trace PHASE STATE
#   control_plane_release_trace_barrier
#   control_plane_release_reverify_execution_authorization AUTHORIZATION_REF
#   control_plane_release_adapter_<literal-domain>_{prepare,apply,verify,rollback}
#
# Adapter phase callbacks receive no arguments. They can read the dynamically
# scoped CONTROL_PLANE_RELEASE_SELECTED_DOMAIN and
# CONTROL_PLANE_RELEASE_TRANSACTION_STATE bindings. In particular, the opaque
# authorization reference is passed only to the reverify callback and is never
# passed to an adapter phase, trace callback, or barrier callback.

_control_plane_release_domain_error() {
  printf 'control-plane release domain transaction: %s\n' "$*" >&2
}

_control_plane_release_require_callback() {
  if ! declare -F "$1" >/dev/null 2>&1; then
    _control_plane_release_domain_error "required callback $1 is not defined"
    return 2
  fi
}

_control_plane_release_validate_selected_adapter() {
  _control_plane_release_require_callback "$1" || return
  _control_plane_release_require_callback "$2" || return
  _control_plane_release_require_callback "$3" || return
  _control_plane_release_require_callback "$4"
}

_control_plane_release_record() {
  CONTROL_PLANE_RELEASE_TRANSACTION_STATE="$1-$2"
  control_plane_release_trace "$1" "$2"
}

_control_plane_release_barrier_best_effort() {
  CONTROL_PLANE_RELEASE_TRANSACTION_STATE="failure-barrier"
  control_plane_release_trace_barrier || :
}

_control_plane_release_record_transaction_failure() {
  _control_plane_release_record transaction failed || :
  _control_plane_release_barrier_best_effort
  CONTROL_PLANE_RELEASE_TRANSACTION_STATE="failed"
}

_control_plane_release_fail_before_apply() {
  # An empty phase means the primary error was not a phase command failure
  # (for example, execution-authorization reverification failed).
  if [[ -n "$1" ]]; then
    _control_plane_release_record "$1" failed || :
  fi
  _control_plane_release_record_transaction_failure
  return 1
}

_control_plane_release_rollback_once() {
  local rollback_failed=0

  # Mark the attempt in this helper's positional parameters before trace I/O.
  # Called functions receive their own positional parameter set, so no phase,
  # trace, or reverify callback can rewrite this marker through Bash's dynamic
  # variable scope. Every forward failure has one structural call site, each
  # call site returns immediately, and this helper contains no retry path.
  set -- "$1" rollback-attempted
  if [[ "$2" != "rollback-attempted" ]]; then
    _control_plane_release_domain_error "failed to mark rollback attempt"
    return 2
  fi

  _control_plane_release_record rollback started || :
  CONTROL_PLANE_RELEASE_TRANSACTION_STATE="rollback-running"
  if ! "$1"; then
    rollback_failed=1
  fi
  if (( rollback_failed != 0 )); then
    _control_plane_release_record rollback failed || :
  else
    _control_plane_release_record rollback succeeded || :
  fi
  _control_plane_release_record_transaction_failure

  if (( rollback_failed != 0 )); then
    return 2
  fi
  return 1
}

_control_plane_release_run_selected_domain_transaction() {
  # Positional parameter 2 is intentionally never copied into a local or
  # global variable. A phase callback has its own empty positional parameter
  # set, while the reverify callback is the only callback that receives $2.
  local -r CONTROL_PLANE_RELEASE_SELECTED_DOMAIN="$1"
  local CONTROL_PLANE_RELEASE_TRANSACTION_STATE="validating-adapter"

  _control_plane_release_validate_selected_adapter "$3" "$4" "$5" "$6" || return

  if ! _control_plane_release_record transaction started; then
    # Match the durable Go transaction contract: if even the first trace
    # record is rejected, try only the trace barrier and perform no phase.
    _control_plane_release_barrier_best_effort
    CONTROL_PLANE_RELEASE_TRANSACTION_STATE="failed"
    return 1
  fi
  if ! _control_plane_release_record prepare started; then
    _control_plane_release_record_transaction_failure
    return 1
  fi

  CONTROL_PLANE_RELEASE_TRANSACTION_STATE="prepare-running"
  if ! "$3"; then
    _control_plane_release_fail_before_apply prepare
    return 1
  fi
  if ! _control_plane_release_record prepare succeeded; then
    _control_plane_release_record_transaction_failure
    return 1
  fi

  if ! _control_plane_release_record apply started; then
    _control_plane_release_record_transaction_failure
    return 1
  fi

  # The durable apply-started record is the conservative write boundary. Put
  # the final execution-authorization recheck after that record and directly
  # adjacent to Apply: there is no trace callback, assignment, or other write
  # between a successful recheck and the selected adapter command. A failed
  # recheck therefore follows the same exactly-once selected rollback path,
  # even though that rollback may prove there was no domain write to undo.
  CONTROL_PLANE_RELEASE_TRANSACTION_STATE="apply-running"
  if ! control_plane_release_reverify_execution_authorization "$2"; then
    _control_plane_release_record apply failed || :
    _control_plane_release_rollback_once "$6"
    return
  fi
  if ! "$4"; then
    _control_plane_release_record apply failed || :
    _control_plane_release_rollback_once "$6"
    return
  fi
  if ! _control_plane_release_record apply succeeded; then
    _control_plane_release_rollback_once "$6"
    return
  fi
  if ! _control_plane_release_record verify started; then
    _control_plane_release_rollback_once "$6"
    return
  fi

  CONTROL_PLANE_RELEASE_TRANSACTION_STATE="verify-running"
  if ! "$5"; then
    _control_plane_release_record verify failed || :
    _control_plane_release_rollback_once "$6"
    return
  fi
  if ! _control_plane_release_record verify succeeded; then
    _control_plane_release_rollback_once "$6"
    return
  fi

  CONTROL_PLANE_RELEASE_TRANSACTION_STATE="precommit-barrier"
  if ! control_plane_release_trace_barrier; then
    _control_plane_release_rollback_once "$6"
    return
  fi

  # The durable transaction-succeeded trace record is the commit linearization
  # point. There is deliberately no fallible operation or cancellation check
  # after it: once the callback returns success, rollback is no longer legal.
  if ! _control_plane_release_record transaction succeeded; then
    _control_plane_release_rollback_once "$6"
    return
  fi
  return 0
}

control_plane_release_dispatch_single_domain_transaction() {
  if (( $# != 3 )); then
    _control_plane_release_domain_error "expected RESULT DOMAIN AUTHORIZATION_REF"
    return 2
  fi
  # extdebug publishes caller arguments through BASH_ARGV/BASH_ARGC. Refuse
  # the transaction before any external callback so an adapter phase cannot
  # observe the opaque authorization reference through the dynamic call stack.
  if shopt -q extdebug; then
    _control_plane_release_domain_error "extdebug must be disabled before dispatch"
    return 2
  fi

  # Non-single planner outcomes are terminal and cannot select an adapter,
  # trace a transaction, reverify authorization, or execute a phase.
  case "$1" in
    single) ;;
    zero|multiple|unknown)
      _control_plane_release_domain_error "planner result $1 cannot dispatch a transaction"
      return 2
      ;;
    *)
      _control_plane_release_domain_error "unsupported planner result $1"
      return 2
      ;;
  esac
  if [[ -z "$3" ]]; then
    _control_plane_release_domain_error "execution authorization reference is empty"
    return 2
  fi

  _control_plane_release_require_callback control_plane_release_trace || return
  _control_plane_release_require_callback control_plane_release_trace_barrier || return
  _control_plane_release_require_callback control_plane_release_reverify_execution_authorization || return

  # This is the sole adapter-selection case. Every callback name is a literal;
  # no domain or phase text is normalized, concatenated, evaluated, or looked
  # up through a generated function name. Only the selected branch constructs
  # and validates an adapter snapshot.
  case "$2" in
    node-local)
      _control_plane_release_run_selected_domain_transaction \
        node-local "$3" \
        control_plane_release_adapter_node_local_prepare \
        control_plane_release_adapter_node_local_apply \
        control_plane_release_adapter_node_local_verify \
        control_plane_release_adapter_node_local_rollback
      ;;
    authoritative-dns)
      _control_plane_release_run_selected_domain_transaction \
        authoritative-dns "$3" \
        control_plane_release_adapter_authoritative_dns_prepare \
        control_plane_release_adapter_authoritative_dns_apply \
        control_plane_release_adapter_authoritative_dns_verify \
        control_plane_release_adapter_authoritative_dns_rollback
      ;;
    control-plane)
      _control_plane_release_run_selected_domain_transaction \
        control-plane "$3" \
        control_plane_release_adapter_control_plane_prepare \
        control_plane_release_adapter_control_plane_apply \
        control_plane_release_adapter_control_plane_verify \
        control_plane_release_adapter_control_plane_rollback
      ;;
    image-cache)
      _control_plane_release_run_selected_domain_transaction \
        image-cache "$3" \
        control_plane_release_adapter_image_cache_prepare \
        control_plane_release_adapter_image_cache_apply \
        control_plane_release_adapter_image_cache_verify \
        control_plane_release_adapter_image_cache_rollback
      ;;
    backup)
      _control_plane_release_run_selected_domain_transaction \
        backup "$3" \
        control_plane_release_adapter_backup_prepare \
        control_plane_release_adapter_backup_apply \
        control_plane_release_adapter_backup_verify \
        control_plane_release_adapter_backup_rollback
      ;;
    *)
      _control_plane_release_domain_error "unsupported selected domain $2"
      return 2
      ;;
  esac
}
