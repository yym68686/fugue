#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/control_plane_release_domains.sh
source "${REPO_ROOT}/scripts/lib/control_plane_release_domains.sh"

fail() {
  printf '[test_single_domain_release] ERROR: %s\n' "$*" >&2
  exit 1
}

assert_eq() {
  local got="$1"
  local want="$2"
  local label="$3"
  [[ "${got}" == "${want}" ]] || fail "${label}: got <${got}>, want <${want}>"
}

TEMPORARY_DIR="$(mktemp -d)"
trap 'rm -rf "${TEMPORARY_DIR}"' EXIT
FAKE_EVENT_LOG="${TEMPORARY_DIR}/events.log"
FAKE_EXPECTED_AUTHORIZATION_REF="sealed-authorization-ref"
FAKE_FAIL_PHASE=""
FAKE_FAIL_TRACE_EVENT=""
FAKE_TRACE_FAILURE_USED=0
FAKE_FAIL_REVERIFY=0
FAKE_FAIL_BARRIER_AT=0
FAKE_BARRIER_COUNT=0
FAKE_REVERIFY_COUNT=0
FAKE_NODE_LOCAL_WRITES=0
FAKE_AUTHORITATIVE_DNS_WRITES=0
FAKE_CONTROL_PLANE_WRITES=0
FAKE_IMAGE_CACHE_WRITES=0
FAKE_BACKUP_WRITES=0

fake_reset() {
  : >"${FAKE_EVENT_LOG}"
  FAKE_FAIL_PHASE=""
  FAKE_FAIL_TRACE_EVENT=""
  FAKE_TRACE_FAILURE_USED=0
  FAKE_FAIL_REVERIFY=0
  FAKE_FAIL_BARRIER_AT=0
  FAKE_BARRIER_COUNT=0
  FAKE_REVERIFY_COUNT=0
  FAKE_NODE_LOCAL_WRITES=0
  FAKE_AUTHORITATIVE_DNS_WRITES=0
  FAKE_CONTROL_PLANE_WRITES=0
  FAKE_IMAGE_CACHE_WRITES=0
  FAKE_BACKUP_WRITES=0
}

fake_increment_write() {
  case "$1" in
    node-local) FAKE_NODE_LOCAL_WRITES=$((FAKE_NODE_LOCAL_WRITES + 1)) ;;
    authoritative-dns) FAKE_AUTHORITATIVE_DNS_WRITES=$((FAKE_AUTHORITATIVE_DNS_WRITES + 1)) ;;
    control-plane) FAKE_CONTROL_PLANE_WRITES=$((FAKE_CONTROL_PLANE_WRITES + 1)) ;;
    image-cache) FAKE_IMAGE_CACHE_WRITES=$((FAKE_IMAGE_CACHE_WRITES + 1)) ;;
    backup) FAKE_BACKUP_WRITES=$((FAKE_BACKUP_WRITES + 1)) ;;
    *) fail "fake write used unknown domain $1" ;;
  esac
}

fake_total_writes() {
  printf '%s\n' "$((
    FAKE_NODE_LOCAL_WRITES +
    FAKE_AUTHORITATIVE_DNS_WRITES +
    FAKE_CONTROL_PLANE_WRITES +
    FAKE_IMAGE_CACHE_WRITES +
    FAKE_BACKUP_WRITES
  ))"
}

fake_event_count() {
  awk -v wanted="$1" '$0 == wanted { count++ } END { print count + 0 }' "${FAKE_EVENT_LOG}"
}

fake_adapter_phase() {
  local domain="$1"
  local phase="$2"
  shift 2

  (( $# == 0 )) || fail "${domain} ${phase} received adapter arguments"
  if [[ " ${BASH_ARGV[*]-} " == *" ${FAKE_EXPECTED_AUTHORIZATION_REF} "* ]]; then
    fail "${domain} ${phase} observed the authorization reference through BASH_ARGV"
  fi
  assert_eq "${CONTROL_PLANE_RELEASE_SELECTED_DOMAIN}" "${domain}" "${domain} ${phase} selected domain"
  assert_eq "${CONTROL_PLANE_RELEASE_TRANSACTION_STATE}" "${phase}-running" "${domain} ${phase} state"
  printf 'phase:%s:%s\n' "${domain}" "${phase}" >>"${FAKE_EVENT_LOG}"
  case "${phase}" in
    apply|rollback) fake_increment_write "${domain}" ;;
    prepare|verify) ;;
    *) fail "fake adapter received unknown phase ${phase}" ;;
  esac
  [[ "${FAKE_FAIL_PHASE}" != "${domain}:${phase}" ]]
}

control_plane_release_trace() {
  local event=""

  (( $# == 2 )) || fail "trace callback received $# arguments"
  assert_eq "${CONTROL_PLANE_RELEASE_TRANSACTION_STATE}" "$1-$2" "trace state binding"
  event="$1:$2"
  printf 'trace:%s\n' "${event}" >>"${FAKE_EVENT_LOG}"
  if [[ "${FAKE_FAIL_TRACE_EVENT}" == "${event}" && "${FAKE_TRACE_FAILURE_USED}" == "0" ]]; then
    FAKE_TRACE_FAILURE_USED=1
    return 1
  fi
  return 0
}

control_plane_release_trace_barrier() {
  (( $# == 0 )) || fail "trace barrier received arguments"
  FAKE_BARRIER_COUNT=$((FAKE_BARRIER_COUNT + 1))
  printf 'barrier:%s\n' "${FAKE_BARRIER_COUNT}" >>"${FAKE_EVENT_LOG}"
  if (( FAKE_FAIL_BARRIER_AT == FAKE_BARRIER_COUNT )); then
    return 1
  fi
  return 0
}

control_plane_release_reverify_execution_authorization() {
  (( $# == 1 )) || fail "reverify callback received $# arguments"
  assert_eq "$1" "${FAKE_EXPECTED_AUTHORIZATION_REF}" "reverify authorization reference"
  assert_eq "${CONTROL_PLANE_RELEASE_TRANSACTION_STATE}" "apply-running" "reverify state"
  FAKE_REVERIFY_COUNT=$((FAKE_REVERIFY_COUNT + 1))
  printf 'reverify:%s\n' "$1" >>"${FAKE_EVENT_LOG}"
  (( FAKE_FAIL_REVERIFY == 0 ))
}

control_plane_release_adapter_node_local_prepare() { fake_adapter_phase node-local prepare "$@"; }
control_plane_release_adapter_node_local_apply() { fake_adapter_phase node-local apply "$@"; }
control_plane_release_adapter_node_local_verify() { fake_adapter_phase node-local verify "$@"; }
control_plane_release_adapter_node_local_rollback() { fake_adapter_phase node-local rollback "$@"; }

control_plane_release_adapter_authoritative_dns_prepare() { fake_adapter_phase authoritative-dns prepare "$@"; }
control_plane_release_adapter_authoritative_dns_apply() { fake_adapter_phase authoritative-dns apply "$@"; }
control_plane_release_adapter_authoritative_dns_verify() { fake_adapter_phase authoritative-dns verify "$@"; }
control_plane_release_adapter_authoritative_dns_rollback() { fake_adapter_phase authoritative-dns rollback "$@"; }

control_plane_release_adapter_control_plane_prepare() { fake_adapter_phase control-plane prepare "$@"; }
control_plane_release_adapter_control_plane_apply() { fake_adapter_phase control-plane apply "$@"; }
control_plane_release_adapter_control_plane_verify() { fake_adapter_phase control-plane verify "$@"; }
control_plane_release_adapter_control_plane_rollback() { fake_adapter_phase control-plane rollback "$@"; }

control_plane_release_adapter_image_cache_prepare() { fake_adapter_phase image-cache prepare "$@"; }
control_plane_release_adapter_image_cache_apply() { fake_adapter_phase image-cache apply "$@"; }
control_plane_release_adapter_image_cache_verify() { fake_adapter_phase image-cache verify "$@"; }
control_plane_release_adapter_image_cache_rollback() { fake_adapter_phase image-cache rollback "$@"; }

control_plane_release_adapter_backup_prepare() { fake_adapter_phase backup prepare "$@"; }
control_plane_release_adapter_backup_apply() { fake_adapter_phase backup apply "$@"; }
control_plane_release_adapter_backup_verify() { fake_adapter_phase backup verify "$@"; }
control_plane_release_adapter_backup_rollback() { fake_adapter_phase backup rollback "$@"; }

dispatch_must_fail() {
  if control_plane_release_dispatch_single_domain_transaction "$@" 2>/dev/null; then
    fail "transaction unexpectedly succeeded: $*"
  fi
}

assert_no_activity() {
  assert_eq "$(wc -l <"${FAKE_EVENT_LOG}" | tr -d '[:space:]')" "0" "$1 event count"
  assert_eq "$(fake_total_writes)" "0" "$1 write count"
  assert_eq "${FAKE_REVERIFY_COUNT}" "0" "$1 reverify count"
  assert_eq "${FAKE_BARRIER_COUNT}" "0" "$1 barrier count"
}

assert_only_domain_wrote() {
  local selected="$1"
  local expected_selected_writes="$2"

  case "${selected}" in
    node-local)
      assert_eq "${FAKE_NODE_LOCAL_WRITES}" "${expected_selected_writes}" "node-local selected writes"
      assert_eq "${FAKE_AUTHORITATIVE_DNS_WRITES}" "0" "node-local authoritative DNS writes"
      assert_eq "${FAKE_CONTROL_PLANE_WRITES}" "0" "node-local control-plane writes"
      assert_eq "${FAKE_IMAGE_CACHE_WRITES}" "0" "node-local image-cache writes"
      assert_eq "${FAKE_BACKUP_WRITES}" "0" "node-local backup writes"
      ;;
    authoritative-dns)
      assert_eq "${FAKE_NODE_LOCAL_WRITES}" "0" "authoritative DNS node-local writes"
      assert_eq "${FAKE_AUTHORITATIVE_DNS_WRITES}" "${expected_selected_writes}" "authoritative DNS selected writes"
      assert_eq "${FAKE_CONTROL_PLANE_WRITES}" "0" "authoritative DNS control-plane writes"
      assert_eq "${FAKE_IMAGE_CACHE_WRITES}" "0" "authoritative DNS image-cache writes"
      assert_eq "${FAKE_BACKUP_WRITES}" "0" "authoritative DNS backup writes"
      ;;
    control-plane)
      assert_eq "${FAKE_NODE_LOCAL_WRITES}" "0" "control-plane node-local writes"
      assert_eq "${FAKE_AUTHORITATIVE_DNS_WRITES}" "0" "control-plane authoritative DNS writes"
      assert_eq "${FAKE_CONTROL_PLANE_WRITES}" "${expected_selected_writes}" "control-plane selected writes"
      assert_eq "${FAKE_IMAGE_CACHE_WRITES}" "0" "control-plane image-cache writes"
      assert_eq "${FAKE_BACKUP_WRITES}" "0" "control-plane backup writes"
      ;;
    image-cache)
      assert_eq "${FAKE_NODE_LOCAL_WRITES}" "0" "image-cache node-local writes"
      assert_eq "${FAKE_AUTHORITATIVE_DNS_WRITES}" "0" "image-cache authoritative DNS writes"
      assert_eq "${FAKE_CONTROL_PLANE_WRITES}" "0" "image-cache control-plane writes"
      assert_eq "${FAKE_IMAGE_CACHE_WRITES}" "${expected_selected_writes}" "image-cache selected writes"
      assert_eq "${FAKE_BACKUP_WRITES}" "0" "image-cache backup writes"
      ;;
    backup)
      assert_eq "${FAKE_NODE_LOCAL_WRITES}" "0" "backup node-local writes"
      assert_eq "${FAKE_AUTHORITATIVE_DNS_WRITES}" "0" "backup authoritative DNS writes"
      assert_eq "${FAKE_CONTROL_PLANE_WRITES}" "0" "backup control-plane writes"
      assert_eq "${FAKE_IMAGE_CACHE_WRITES}" "0" "backup image-cache writes"
      assert_eq "${FAKE_BACKUP_WRITES}" "${expected_selected_writes}" "backup selected writes"
      ;;
    *) fail "assert_only_domain_wrote received unknown domain ${selected}" ;;
  esac
}

test_non_single_and_invalid_inputs_are_inert() {
  local result=""

  for result in zero multiple unknown; do
    fake_reset
    dispatch_must_fail "${result}" "" ""
    assert_no_activity "${result} outcome"
  done

  fake_reset
  dispatch_must_fail single "not-a-domain" "${FAKE_EXPECTED_AUTHORIZATION_REF}"
  assert_no_activity "unknown selected domain"

  fake_reset
  dispatch_must_fail single 'node-local;control-plane' "${FAKE_EXPECTED_AUTHORIZATION_REF}"
  assert_no_activity "injected selected domain"

  fake_reset
  dispatch_must_fail single node-local ""
  assert_no_activity "empty authorization reference"

  fake_reset
  dispatch_must_fail unsupported node-local "${FAKE_EXPECTED_AUTHORIZATION_REF}"
  assert_no_activity "unsupported planner result"

  (
    shopt -s extdebug
    fake_reset
    dispatch_must_fail single node-local "${FAKE_EXPECTED_AUTHORIZATION_REF}"
    assert_no_activity "extdebug authorization exposure"
  )
}

test_five_literal_domains_succeed() {
  local domain=""
  local foreign_phases=""

  for domain in node-local authoritative-dns control-plane image-cache backup; do
    fake_reset
    control_plane_release_dispatch_single_domain_transaction \
      single "${domain}" "${FAKE_EXPECTED_AUTHORIZATION_REF}"
    assert_only_domain_wrote "${domain}" 1
    assert_eq "${FAKE_REVERIFY_COUNT}" "1" "${domain} reverify count"
    assert_eq "${FAKE_BARRIER_COUNT}" "1" "${domain} success barrier count"
    foreign_phases="$(awk -F: -v selected="${domain}" '$1 == "phase" && $2 != selected { count++ } END { print count + 0 }' "${FAKE_EVENT_LOG}")"
    assert_eq "${foreign_phases}" "0" "${domain} foreign adapter phase count"
    assert_eq "$(fake_event_count "phase:${domain}:prepare")" "1" "${domain} prepare count"
    assert_eq "$(fake_event_count "phase:${domain}:apply")" "1" "${domain} apply count"
    assert_eq "$(fake_event_count "phase:${domain}:verify")" "1" "${domain} verify count"
    assert_eq "$(fake_event_count "phase:${domain}:rollback")" "0" "${domain} rollback count"
    if grep -E '^phase:|^trace:|^barrier:' "${FAKE_EVENT_LOG}" | grep -F "${FAKE_EXPECTED_AUTHORIZATION_REF}" >/dev/null 2>&1; then
      fail "${domain} authorization reference escaped the reverify callback"
    fi
  done
}

test_success_trace_order_and_commit_point() {
  local expected=""
  local got=""

  fake_reset
  control_plane_release_dispatch_single_domain_transaction \
    single node-local "${FAKE_EXPECTED_AUTHORIZATION_REF}"
  expected=$'trace:transaction:started\ntrace:prepare:started\nphase:node-local:prepare\ntrace:prepare:succeeded\ntrace:apply:started\nreverify:sealed-authorization-ref\nphase:node-local:apply\ntrace:apply:succeeded\ntrace:verify:started\nphase:node-local:verify\ntrace:verify:succeeded\nbarrier:1\ntrace:transaction:succeeded'
  got="$(cat "${FAKE_EVENT_LOG}")"
  assert_eq "${got}" "${expected}" "successful transaction trace order"
  assert_eq "$(tail -n 1 "${FAKE_EVENT_LOG}")" "trace:transaction:succeeded" "commit linearization point"
}

test_only_selected_adapter_is_constructed() {
  (
    unset -f \
      control_plane_release_adapter_authoritative_dns_prepare \
      control_plane_release_adapter_authoritative_dns_apply \
      control_plane_release_adapter_authoritative_dns_verify \
      control_plane_release_adapter_authoritative_dns_rollback \
      control_plane_release_adapter_control_plane_prepare \
      control_plane_release_adapter_control_plane_apply \
      control_plane_release_adapter_control_plane_verify \
      control_plane_release_adapter_control_plane_rollback \
      control_plane_release_adapter_image_cache_prepare \
      control_plane_release_adapter_image_cache_apply \
      control_plane_release_adapter_image_cache_verify \
      control_plane_release_adapter_image_cache_rollback \
      control_plane_release_adapter_backup_prepare \
      control_plane_release_adapter_backup_apply \
      control_plane_release_adapter_backup_verify \
      control_plane_release_adapter_backup_rollback
    fake_reset
    control_plane_release_dispatch_single_domain_transaction \
      single node-local "${FAKE_EXPECTED_AUTHORIZATION_REF}"
    assert_only_domain_wrote node-local 1
  )

  (
    unset -f control_plane_release_adapter_node_local_rollback
    fake_reset
    dispatch_must_fail single node-local "${FAKE_EXPECTED_AUTHORIZATION_REF}"
    assert_no_activity "incomplete selected adapter"
  )
}

test_forward_phase_failure_matrix() {
  local domain=""
  local phase=""

  for domain in node-local authoritative-dns control-plane image-cache backup; do
    for phase in prepare apply verify; do
      fake_reset
      FAKE_FAIL_PHASE="${domain}:${phase}"
      dispatch_must_fail single "${domain}" "${FAKE_EXPECTED_AUTHORIZATION_REF}"
      case "${phase}" in
        prepare)
          assert_only_domain_wrote "${domain}" 0
          assert_eq "$(fake_event_count "phase:${domain}:rollback")" "0" "${domain} prepare failure rollback count"
          ;;
        apply|verify)
          assert_only_domain_wrote "${domain}" 2
          assert_eq "$(fake_event_count "phase:${domain}:rollback")" "1" "${domain} ${phase} failure rollback count"
          ;;
      esac
    done
  done

  fake_reset
  FAKE_FAIL_PHASE="node-local:apply"
  # The rollback callback also fails, but it is still attempted exactly once.
  control_plane_release_adapter_node_local_rollback() {
    fake_adapter_phase node-local rollback "$@" || return
    return 1
  }
  dispatch_must_fail single node-local "${FAKE_EXPECTED_AUTHORIZATION_REF}"
  assert_only_domain_wrote node-local 2
  assert_eq "$(fake_event_count phase:node-local:rollback)" "1" "failed rollback is not retried"
  control_plane_release_adapter_node_local_rollback() { fake_adapter_phase node-local rollback "$@"; }
}

test_callback_cannot_suppress_rollback_marker() {
  fake_reset
  FAKE_FAIL_PHASE="node-local:apply"
  control_plane_release_adapter_node_local_apply() {
    # This is the name used by the rejected mutable-local implementation. A
    # callback may create or rewrite it, but rollback control no longer reads
    # callback-visible variables.
    _CONTROL_PLANE_RELEASE_ROLLBACK_ATTEMPTED=1
    fake_adapter_phase node-local apply "$@"
  }
  dispatch_must_fail single node-local "${FAKE_EXPECTED_AUTHORIZATION_REF}"
  assert_only_domain_wrote node-local 2
  assert_eq "$(fake_event_count phase:node-local:rollback)" "1" "callback-mutated rollback marker count"
  control_plane_release_adapter_node_local_apply() { fake_adapter_phase node-local apply "$@"; }
  unset _CONTROL_PLANE_RELEASE_ROLLBACK_ATTEMPTED
}

test_reverify_failure_crosses_conservative_apply_boundary() {
  fake_reset
  FAKE_FAIL_REVERIFY=1
  dispatch_must_fail single node-local "${FAKE_EXPECTED_AUTHORIZATION_REF}"
  # Apply itself was never called; the one selected-domain write is the
  # conservative exactly-once rollback after durable apply-started evidence.
  assert_only_domain_wrote node-local 1
  assert_eq "${FAKE_REVERIFY_COUNT}" "1" "failed reverify count"
  assert_eq "$(fake_event_count phase:node-local:prepare)" "1" "failed reverify prepare count"
  assert_eq "$(fake_event_count phase:node-local:apply)" "0" "failed reverify apply count"
  assert_eq "$(fake_event_count phase:node-local:rollback)" "1" "failed reverify rollback count"
  assert_eq "$(fake_event_count trace:apply:started)" "1" "failed reverify apply-start trace count"
  assert_eq "$(fake_event_count trace:apply:failed)" "1" "failed reverify apply-failed trace count"
}

test_forward_trace_failure_matrix() {
  local event=""
  local expected_writes=0
  local expected_rollbacks=0

  for event in \
    transaction:started \
    prepare:started \
    prepare:succeeded \
    apply:started \
    apply:succeeded \
    verify:started \
    verify:succeeded \
    transaction:succeeded; do
    fake_reset
    FAKE_FAIL_TRACE_EVENT="${event}"
    dispatch_must_fail single node-local "${FAKE_EXPECTED_AUTHORIZATION_REF}"
    assert_eq "${FAKE_TRACE_FAILURE_USED}" "1" "${event} trace failure exercised"
    case "${event}" in
      transaction:started|prepare:started|prepare:succeeded|apply:started)
        expected_writes=0
        expected_rollbacks=0
        ;;
      *)
        expected_writes=2
        expected_rollbacks=1
        ;;
    esac
    assert_only_domain_wrote node-local "${expected_writes}"
    assert_eq "$(fake_event_count phase:node-local:rollback)" "${expected_rollbacks}" "${event} rollback count"
  done
}

test_failure_trace_matrix_never_retries_rollback() {
  local event=""

  fake_reset
  FAKE_FAIL_PHASE="node-local:prepare"
  FAKE_FAIL_TRACE_EVENT="prepare:failed"
  dispatch_must_fail single node-local "${FAKE_EXPECTED_AUTHORIZATION_REF}"
  assert_only_domain_wrote node-local 0
  assert_eq "${FAKE_TRACE_FAILURE_USED}" "1" "prepare failure trace exercised"

  for event in apply:failed verify:failed rollback:started rollback:succeeded transaction:failed; do
    fake_reset
    case "${event}" in
      verify:failed) FAKE_FAIL_PHASE="node-local:verify" ;;
      *) FAKE_FAIL_PHASE="node-local:apply" ;;
    esac
    FAKE_FAIL_TRACE_EVENT="${event}"
    dispatch_must_fail single node-local "${FAKE_EXPECTED_AUTHORIZATION_REF}"
    assert_eq "${FAKE_TRACE_FAILURE_USED}" "1" "${event} failure trace exercised"
    assert_only_domain_wrote node-local 2
    assert_eq "$(fake_event_count phase:node-local:rollback)" "1" "${event} rollback count"
  done

  fake_reset
  FAKE_FAIL_PHASE="node-local:apply"
  FAKE_FAIL_TRACE_EVENT="rollback:failed"
  control_plane_release_adapter_node_local_rollback() {
    fake_adapter_phase node-local rollback "$@" || return
    return 1
  }
  dispatch_must_fail single node-local "${FAKE_EXPECTED_AUTHORIZATION_REF}"
  assert_eq "${FAKE_TRACE_FAILURE_USED}" "1" "rollback failed trace exercised"
  assert_only_domain_wrote node-local 2
  assert_eq "$(fake_event_count phase:node-local:rollback)" "1" "rollback failed trace retry count"
  control_plane_release_adapter_node_local_rollback() { fake_adapter_phase node-local rollback "$@"; }
}

test_barrier_failure_matrix() {
  fake_reset
  FAKE_FAIL_BARRIER_AT=1
  dispatch_must_fail single node-local "${FAKE_EXPECTED_AUTHORIZATION_REF}"
  assert_only_domain_wrote node-local 2
  assert_eq "$(fake_event_count phase:node-local:rollback)" "1" "precommit barrier rollback count"
  assert_eq "${FAKE_BARRIER_COUNT}" "2" "precommit and failure barriers"

  fake_reset
  FAKE_FAIL_PHASE="node-local:prepare"
  FAKE_FAIL_BARRIER_AT=1
  dispatch_must_fail single node-local "${FAKE_EXPECTED_AUTHORIZATION_REF}"
  assert_only_domain_wrote node-local 0
  assert_eq "$(fake_event_count phase:node-local:rollback)" "0" "prepare failure barrier rollback count"
  assert_eq "${FAKE_BARRIER_COUNT}" "1" "prepare failure barrier count"

  fake_reset
  FAKE_FAIL_PHASE="node-local:apply"
  FAKE_FAIL_BARRIER_AT=1
  dispatch_must_fail single node-local "${FAKE_EXPECTED_AUTHORIZATION_REF}"
  assert_only_domain_wrote node-local 2
  assert_eq "$(fake_event_count phase:node-local:rollback)" "1" "failed evidence barrier rollback count"
  assert_eq "${FAKE_BARRIER_COUNT}" "1" "failed evidence barrier is not retried"
}

test_node_local_never_invokes_cross_domain_writes() {
  fake_reset
  FAKE_FAIL_PHASE="node-local:apply"
  dispatch_must_fail single node-local "${FAKE_EXPECTED_AUTHORIZATION_REF}"
  assert_only_domain_wrote node-local 2
  assert_eq "${FAKE_AUTHORITATIVE_DNS_WRITES}" "0" "node-local authoritative DNS isolation"
  assert_eq "${FAKE_CONTROL_PLANE_WRITES}" "0" "node-local control-plane isolation"
  assert_eq "${FAKE_IMAGE_CACHE_WRITES}" "0" "node-local image-cache isolation"
  assert_eq "${FAKE_BACKUP_WRITES}" "0" "node-local backup isolation"
}

/bin/bash -n "${REPO_ROOT}/scripts/lib/control_plane_release_domains.sh"
/bin/bash -n "${REPO_ROOT}/scripts/test_single_domain_release.sh"
test_non_single_and_invalid_inputs_are_inert
test_five_literal_domains_succeed
test_success_trace_order_and_commit_point
test_only_selected_adapter_is_constructed
test_forward_phase_failure_matrix
test_callback_cannot_suppress_rollback_marker
test_reverify_failure_crosses_conservative_apply_boundary
test_forward_trace_failure_matrix
test_failure_trace_matrix_never_retries_rollback
test_barrier_failure_matrix
test_node_local_never_invokes_cross_domain_writes

printf '[test_single_domain_release] fixed five-domain dispatcher and exactly-once transaction passed\n'
