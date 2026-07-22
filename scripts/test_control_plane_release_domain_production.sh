#!/usr/bin/env bash

set -euo pipefail

fake_flag_value() {
  local wanted="$1"
  shift
  while (( $# > 0 )); do
    case "$1" in
      "${wanted}")
        (( $# >= 2 )) || return 2
        printf '%s' "$2"
        return 0
        ;;
      "${wanted}="*)
        printf '%s' "${1#*=}"
        return 0
        ;;
    esac
    shift
  done
  return 2
}

fake_tool_log() {
  printf '%s\n' "$*" >>"${FAKE_LOG:?}"
}

case "$(basename "$0")" in
  fake-release-evidence)
    fake_tool_log "evidence:$*"
    if [[ "${1:-}" == "image-activation-plans" ]]; then
      output_dir="$(fake_flag_value --output-dir "$@")" || exit 2
      mkdir "${output_dir}" || exit 1
      chmod 700 "${output_dir}" || exit 1
      printf '{}\n' >"${output_dir}/build-artifact-plan.json" || exit 1
      printf '{}\n' >"${output_dir}/composite-decomposition-evidence.json" || exit 1
      printf '{}\n' >"${output_dir}/image-activation-evidence.json" || exit 1
      printf '{}\n' >"${output_dir}/image-activation-plan.json" || exit 1
      printf '%s\n' 'immutable-target-manifest' >"${output_dir}/immutable-target-manifest.yaml" || exit 1
      chmod 600 "${output_dir}"/* || exit 1
      exit 0
    fi
    output="$(fake_flag_value --output "$@")" || exit 2
    if [[ "${1:-}" == "canonicalize-manifest" ]]; then
      input="$(fake_flag_value --input "$@")" || exit 2
      cp "${input}" "${output}" || exit 1
	elif [[ "${1:-}" == "operational-report" &&
	  "${FAKE_OPERATIONAL_REPORT_ELIGIBLE:-false}" == "true" ]]; then
	  printf '{"digest":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}\n' >"${output}" || exit 1
	else
      printf '{}\n' >"${output}" || exit 1
    fi
    chmod 600 "${output}" || exit 1
    exit 0
    ;;
  fake-release-dispatch)
    command_name="${1:-}"
    shift || :
    fake_tool_log "dispatch:${command_name}:$*"
    case "${command_name}" in
      classify-files)
        case "${FAKE_OUTCOME:?}" in
          zero) printf 'zero\n'; exit 0 ;;
          single) printf 'single\t%s\n' "${FAKE_DOMAIN:?}"; exit 0 ;;
          multiple|unknown) printf '%s\n' "${FAKE_OUTCOME}"; exit 2 ;;
          *) exit 2 ;;
        esac
        ;;
      authorize)
        bundle_dir="$(fake_flag_value --bundle-dir "$@")" || exit 2
        base_manifest="$(fake_flag_value --base-canonical-manifest "$@")" || exit 2
        target_manifest="$(fake_flag_value --target-canonical-manifest "$@")" || exit 2
        repeated_manifest="$(fake_flag_value --repeated-target-canonical-manifest "$@")" || exit 2
        argv_snapshot="$(fake_flag_value --argv-snapshot "$@")" || exit 2
        mkdir "${bundle_dir}" || exit 1
        chmod 700 "${bundle_dir}" || exit 1
        cp "${base_manifest}" "${bundle_dir}/base-manifest.yaml" || exit 1
        cp "${target_manifest}" "${bundle_dir}/target-manifest.yaml" || exit 1
        cp "${repeated_manifest}" "${bundle_dir}/repeated-target-manifest.yaml" || exit 1
        cp "${argv_snapshot}" "${bundle_dir}/upgrade-argv.snapshot" || exit 1
        printf '{"planDigest":"%s"}\n' "${FAKE_PLAN_DIGEST}" >"${bundle_dir}/release-domain-plan.json" || exit 1
        chmod 600 "${bundle_dir}"/* || exit 1
		operational_report=""
		if operational_report="$(fake_flag_value --operational-report "$@" 2>/dev/null)"; then
		  [[ "${FAKE_OPERATIONAL_REPORT_ELIGIBLE:-false}" == "true" ]] || exit 2
		  printf 'single\t%s\t%s\n' "${FAKE_DOMAIN}" "${FAKE_PLAN_DIGEST}"
		elif [[ "${FAKE_OUTCOME}" == "zero" ]]; then
          printf 'zero\t%s\n' "${FAKE_PLAN_DIGEST}"
        elif [[ "${FAKE_OUTCOME}" == "multiple" || "${FAKE_OUTCOME}" == "unknown" ]]; then
          exit 2
        else
          printf 'single\t%s\t%s\n' "${FAKE_DOMAIN}" "${FAKE_PLAN_DIGEST}"
        fi
        ;;
      verify)
        verify_count=0
        if [[ -f "${FAKE_VERIFY_COUNT_FILE}" ]]; then
          verify_count="$(<"${FAKE_VERIFY_COUNT_FILE}")"
        fi
        verify_count=$((verify_count + 1))
        printf '%s\n' "${verify_count}" >"${FAKE_VERIFY_COUNT_FILE}"
        if [[ "${FAKE_VERIFY_FAIL_AT:-0}" == "${verify_count}" ]]; then
          exit 1
        fi
        if [[ "${FAKE_OUTCOME}" == "zero" ]]; then
          printf 'zero\t%s\n' "${FAKE_PLAN_DIGEST}"
        else
          printf 'single\t%s\t%s\n' "${FAKE_DOMAIN}" "${FAKE_PLAN_DIGEST}"
        fi
        ;;
      write-public-evidence)
        [[ "${FAKE_PUBLIC_FAIL:-false}" != "true" ]] || exit 9
        output="$(fake_flag_value --output "$@")" || exit 2
        trace_file="$(fake_flag_value --trace-file "$@")" || exit 2
        python3 - "${trace_file}" <<'PY' || exit 2
import json
import sys

events = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
if events:
    if (events[0].get("phase"), events[0].get("state")) != ("transaction", "started"):
        raise SystemExit(1)
    if (events[-1].get("phase"), events[-1].get("state")) not in {
        ("transaction", "succeeded"), ("transaction", "failed")
    }:
        raise SystemExit(1)
    phases = {}
    for event in events:
        phase = event.get("phase")
        state = event.get("state")
        progress = phases.setdefault(phase, {"started": False, "terminal": False})
        if state == "started":
            if progress["started"]:
                raise SystemExit(1)
            progress["started"] = True
        elif state in {"succeeded", "failed"}:
            if not progress["started"] or progress["terminal"]:
                raise SystemExit(1)
            progress["terminal"] = True
        else:
            raise SystemExit(1)
    if any(progress["started"] and not progress["terminal"] for progress in phases.values()):
        raise SystemExit(1)
PY
        printf '%s\n' "$*" >"${output}" || exit 1
        cp "${trace_file}" "${output}.trace" || exit 1
        chmod 600 "${output}" "${output}.trace" || exit 1
        if [[ "${FAKE_TAMPER_CLEANUP:-false}" == "true" ]]; then
          bundle_dir="$(fake_flag_value --bundle-dir "$@")" || exit 2
          ln -s /dev/null "$(dirname "${bundle_dir}")/cleanup-tamper" || exit 1
        fi
        ;;
      write-blocked-public-evidence)
        [[ "${FAKE_PUBLIC_FAIL:-false}" != "true" ]] || exit 9
        output="$(fake_flag_value --output "$@")" || exit 2
        printf '%s\n' "$*" >"${output}" || exit 1
        chmod 600 "${output}" || exit 1
        ;;
      *) exit 2 ;;
    esac
    exit 0
    ;;
esac

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
TEST_SCRIPT="${REPO_ROOT}/scripts/test_control_plane_release_domain_production.sh"

# shellcheck source=scripts/lib/control_plane_release_domains.sh
source "${REPO_ROOT}/scripts/lib/control_plane_release_domains.sh"
# shellcheck source=scripts/lib/control_plane_release_domain_production.sh
source "${REPO_ROOT}/scripts/lib/control_plane_release_domain_production.sh"

fail_test() {
  printf '[test_control_plane_release_domain_production] FAIL: %s\n' "$*" >&2
  return 1
}

assert_file_contains() {
  local file="$1"
  local text="$2"
  grep -Fq -- "${text}" "${file}" || fail_test "${file} does not contain ${text}"
}

assert_file_not_contains() {
  local file="$1"
  local text="$2"
  if grep -Fq -- "${text}" "${file}"; then
    fail_test "${file} unexpectedly contains ${text}"
  fi
}

assert_log_count() {
  local expected="$1"
  local text="$2"
  local actual=0
  actual="$(grep -Fc -- "${text}" "${FAKE_LOG}" || true)"
  [[ "${actual}" == "${expected}" ]] ||
    fail_test "log count for ${text}: got ${actual}, want ${expected}"
}

assert_log_order() {
  local first="$1"
  local second="$2"
  local first_line=""
  local second_line=""
  first_line="$(grep -nF -- "${first}" "${FAKE_LOG}" | head -1 | cut -d: -f1)"
  second_line="$(grep -nF -- "${second}" "${FAKE_LOG}" | head -1 | cut -d: -f1)"
  [[ -n "${first_line}" && -n "${second_line}" && "${first_line}" -lt "${second_line}" ]] ||
    fail_test "expected log order ${first} before ${second}"
}

assert_file_order() {
  local file="$1"
  local first="$2"
  local second="$3"
  local first_line=""
  local second_line=""
  first_line="$(grep -nF -- "${first}" "${file}" | head -1 | cut -d: -f1)"
  second_line="$(grep -nF -- "${second}" "${file}" | head -1 | cut -d: -f1)"
  [[ -n "${first_line}" && -n "${second_line}" && "${first_line}" -lt "${second_line}" ]] ||
    fail_test "expected file order ${first} before ${second}"
}

fake_log() {
  printf '%s\n' "$*" >>"${FAKE_LOG}"
}

fake_signal_current_shell() {
  local signal_name="$1"
  python3 - "${signal_name}" <<'PY'
import os
import signal
import sys

signals = {
    "TERM": signal.SIGTERM,
    "USR2": signal.SIGUSR2,
}
os.kill(os.getppid(), signals[sys.argv[1]])
PY
}

handle_control_plane_release_signal() {
  local signal_name="$1"
  local signal_status=143
  case "${signal_name}" in
    HUP) signal_status=129 ;;
    INT) signal_status=130 ;;
    TERM) signal_status=143 ;;
    *) return 2 ;;
  esac
  CONTROL_PLANE_RELEASE_PENDING_SIGNAL="${signal_name}"
  CONTROL_PLANE_RELEASE_PENDING_SIGNAL_STATUS="${signal_status}"
  fake_log "root-signal:${signal_name}"
  terminate_active_control_plane_release_command
}
handle_control_plane_backup_coordination_abort() { :; }
terminate_active_control_plane_release_command() { fake_log "active-command:terminated"; }
mark_control_plane_release_command_termination_unproven() { fake_log "active-command:unproven"; }
mark_control_plane_release_committed_lease_unsafe() {
  CONTROL_PLANE_RELEASE_RECOVERY_FENCE_REQUIRED="true"
  CONTROL_PLANE_RELEASE_RECOVERY_FENCE_DISPOSITION="committed-lease-unsafe"
  fake_log "lease:committed-unsafe"
}
prepare_control_plane_backup_abort_state_for_dispatch() {
  fake_log "backup-abort-state:$*"
  if [[ "${1:-false}" == "true" && "${CONTROL_PLANE_RELEASE_COMMITTED:-false}" == "true" ]]; then
    mark_control_plane_release_committed_lease_unsafe
  fi
}

control_plane_release_verify_repository_snapshot() {
  FAKE_REPOSITORY_VERIFY_COUNT=$((FAKE_REPOSITORY_VERIFY_COUNT + 1))
  fake_log "repository-verify:${FAKE_REPOSITORY_VERIFY_COUNT}"
  if [[ "${FAKE_TAMPER_ARGV_INPUT_AT:-0}" == "${FAKE_REPOSITORY_VERIFY_COUNT}" ]]; then
    printf 'tamper\n' >>"${UPGRADE_OVERRIDE_VALUES_FILE}"
    fake_log "argv-input-tampered"
  fi
  [[ "${FAKE_REPOSITORY_VERIFY_FAIL_AT:-0}" != "${FAKE_REPOSITORY_VERIFY_COUNT}" ]]
}

control_plane_release_run_private_canonical_render_set() {
  local live_revision="$1"
  local consumer="$4"
  local render_dir="${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/fake-render"
  shift 4
  mkdir "${render_dir}" || return
  chmod 700 "${render_dir}" || return
  printf 'base\n' >"${render_dir}/base.manifest"
  if [[ "${FAKE_OUTCOME}" == "zero" ]]; then
    printf 'base\n' >"${render_dir}/target.manifest"
    printf 'base\n' >"${render_dir}/repeated.manifest"
  else
    printf 'target\n' >"${render_dir}/target.manifest"
    printf 'target\n' >"${render_dir}/repeated.manifest"
  fi
  chmod 600 "${render_dir}"/*
  "${consumer}" "${live_revision}" "${FUGUE_RELEASE_NAME}" "${FUGUE_NAMESPACE}" \
    "${render_dir}/base.manifest" "${render_dir}/target.manifest" \
    "${render_dir}/repeated.manifest"
}

with_frozen_control_plane_helm_upgrade_argv() {
  local override_file="$1"
  local consumer="$2"
  "${consumer}" helm upgrade "${FUGUE_RELEASE_NAME}" "${FUGUE_HELM_CHART_PATH}" \
    -n "${FUGUE_NAMESPACE}" --reset-then-reuse-values --no-hooks -f "${override_file}"
}

node_local_dns_configure_cohort_names() {
  NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME="fugue-node-local-dns"
  NODE_LOCAL_DNS_UPSTREAM_SERVICE_NAME="fugue-node-local-upstream"
  NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME="fugue-node-local-dns-active"
}

control_plane_postgres_name() { printf 'custom-control-plane-postgres\n'; }

preserve_public_data_plane_from_live() {
  fake_log "preserve:public"
  if [[ "${FAKE_SIGNAL_PRETRANSACTION:-false}" == "true" ]]; then
    fake_signal_current_shell TERM
  fi
  PUBLIC_DATA_PLANE_HELM_SET_ARGS=(--set-string fake.public=preserved)
}
preserve_node_local_build_plane_from_live() {
  fake_log "preserve:image-cache:$*"
  NODE_LOCAL_BUILD_PLANE_HELM_SET_ARGS=(--set-string fake.imageCache=preserved)
}
preserve_maintenance_agents_from_live() {
  fake_log "preserve:maintenance"
  MAINTENANCE_AGENT_HELM_SET_ARGS=(--set-string fake.maintenance=preserved)
}
preserve_strict_drain_agent_image_from_live() { fake_log "preserve:strict-drain"; }
live_deployment_replicas() { printf '1\n'; }
stateful_dependency_changed() { return 1; }

run_node_local_dns_phase_with_state_handoff() {
  shift
  "$@"
}
run_node_local_dns_whole_phase() {
  shift
  "$@"
}
prepare_node_local_dns_helm_args() {
  NODE_LOCAL_DNS_HELM_SET_ARGS=(--set-string fake.nodeLocal=target)
  NODE_LOCAL_DNS_PREVIOUS_ENABLED="false"
  NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP="10.96.0.10"
}
write_upgrade_override_values() {
  UPGRADE_OVERRIDE_VALUES_FILE="${TMPDIR}/upgrade-values.yaml"
  printf 'fake: true\n' >"${UPGRADE_OVERRIDE_VALUES_FILE}"
  chmod 600 "${UPGRADE_OVERRIDE_VALUES_FILE}"
  fake_log "override:${UPGRADE_OVERRIDE_VALUES_FILE}"
}
build_dns_helm_set_args() { DNS_HELM_SET_ARGS=(--set-string fake.dns=target); }
prepare_helm_post_renderer() { HELM_POST_RENDERER_ARGS=(); }

node_local_dns_split_release_enabled() { [[ "${FAKE_SPLIT:-false}" == "true" ]]; }
run_release_preflight() {
  fake_log "preflight:${CONTROL_PLANE_RELEASE_SELECTED_DOMAIN}"
  if [[ "${FAKE_PREFLIGHT_EXIT:-false}" == "true" ]]; then
    exit 41
  fi
  NODE_LOCAL_DNS_PREFLIGHT_TARGET_NODES="${NODE_LOCAL_DNS_PREFLIGHT_TARGET_NODES:-}"
  NODE_LOCAL_BUILD_PLANE_PREFLIGHT_OVERRIDE_USED="false"
}
authoritative_dns_dig_preflight() {
  AUTHORITATIVE_DNS_DIG_ATTESTED="fake-wire-policy"
  AUTHORITATIVE_DNS_DIG_ATTESTATION_SHA256="sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  export AUTHORITATIVE_DNS_DIG_ATTESTED AUTHORITATIVE_DNS_DIG_ATTESTATION_SHA256
  fake_log "dig:${CONTROL_PLANE_RELEASE_SELECTED_DOMAIN}"
}
validate_control_plane_release_job_budget() {
  CONTROL_PLANE_RELEASE_JOB_DEADLINE_EPOCH=4102444800
  fake_log "budget:job"
}
validate_node_local_dns_release_budget_pre_mutation() {
  NODE_LOCAL_DNS_BUDGET_TARGET_NODES=""
  NODE_LOCAL_DNS_ROLLBACK_BUDGET_SECONDS=0
  fake_log "budget:node-local"
}
run_control_plane_rollback_image_preflight() {
  fake_log "rollback-image:preflight"
  [[ "${FAKE_ROLLBACK_IMAGE_FAIL:-false}" != "true" ]]
}

duration_to_seconds() { printf '10\n'; }
helm_current_revision() { printf '%s\n' "${FAKE_CURRENT_REVISION}"; }

run_release_long_command() {
  local timeout="$1"
  local label="$2"
  shift 2
  : "${timeout}"
  case "${1:-}:${2:-}" in
    helm:upgrade)
      fake_log "helm-upgrade:${CONTROL_PLANE_RELEASE_SELECTED_DOMAIN}"
      case "${FAKE_HELM_BEHAVIOR:-success}" in
        success)
          FAKE_CURRENT_REVISION=$((PREVIOUS_REVISION + 1))
          FAKE_LIVE_MANIFEST="target"
          return 0
          ;;
        fail-before-record)
          FAKE_LIVE_MANIFEST="partial"
          return 1
          ;;
        fail-after-record)
          FAKE_CURRENT_REVISION=$((PREVIOUS_REVISION + 1))
          FAKE_LIVE_MANIFEST="target"
          return 1
          ;;
        fail-weird)
          FAKE_CURRENT_REVISION=99
          FAKE_LIVE_MANIFEST="unknown"
          return 1
          ;;
        *) return 2 ;;
      esac
      ;;
    helm:rollback)
      [[ "$*" == "helm rollback ${FUGUE_RELEASE_NAME} ${PREVIOUS_REVISION} -n ${FUGUE_NAMESPACE} --no-hooks --timeout ${FUGUE_HELM_TIMEOUT}" ]] || return 2
      fake_log "helm-rollback:${FAKE_CURRENT_REVISION}:$*"
      FAKE_CURRENT_REVISION=$((FAKE_CURRENT_REVISION + 1))
      FAKE_LIVE_MANIFEST="base"
      return 0
      ;;
    helm:get)
      fake_log "helm-get:${FAKE_CURRENT_REVISION}:${FAKE_LIVE_MANIFEST}"
      if [[ "${FAKE_LIVE_MANIFEST}" == "target" ]]; then
        printf 'target\n'
      else
        printf 'base\n'
      fi
      return 0
      ;;
    *)
      fake_log "long-command:${label}:$*"
      ("$@")
      ;;
  esac
}

node_local_dns_run_deferred_operational_validation() {
  fake_log "node-local:deferred-validation"
  if [[ "${FAKE_TAMPER_SEALED_ARGV:-false}" == "true" ]]; then
    printf 'injected\0' >>"${CONTROL_PLANE_RELEASE_DOMAIN_BUNDLE_DIR}/upgrade-argv.snapshot"
    fake_log "sealed-argv:in-place-tamper"
  fi
}
node_local_dns_shadow_host_preflight() { fake_log "node-local:shadow-preflight"; }
node_local_dns_delete_daemonset_safely() { fake_log "node-local:delete"; }
node_local_dns_reconcile_after_helm() { fake_log "node-local:reconcile"; }
node_local_dns_verify_teardown() { fake_log "node-local:teardown"; }
node_local_dns_verify_central_coredns_ready() { fake_log "node-local:central-coredns"; }
node_local_dns_verify_target_before_commit() { fake_log "node-local:target"; }
node_local_dns_verify_target_snapshot_unchanged() { fake_log "node-local:snapshot"; }
node_local_dns_restore_previous_after_helm_rollback() { fake_log "node-local:restore"; }
node_local_dns_verify_preserved_snapshot_after_helm_rollback() { fake_log "node-local:rollback-proof"; }

prepare_dns_manifest_transaction() {
  [[ "${AUTHORITATIVE_DNS_DIG_ATTESTED:-}" == "fake-wire-policy" &&
    "${AUTHORITATIVE_DNS_DIG_ATTESTATION_SHA256:-}" == sha256:* ]] || return 1
  fake_log "dns:prepare:attested"
  DNS_MANIFEST_TRANSACTION_REQUIRED="true"
  if [[ "${FAKE_DNS_CLEANUP_BROKEN_SYMLINK:-false}" == "true" ]]; then
    DNS_MANIFEST_SNAPSHOT_FILE="${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/dns-broken-snapshot.json"
    ln -s "${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/missing-snapshot-target" \
      "${DNS_MANIFEST_SNAPSHOT_FILE}" || return
  fi
}
run_dns_manifest_transaction_after_helm() {
  [[ "${CONTROL_PLANE_RELEASE_DOMAIN_DNS_ONLY:-false}" == "true" ]] || return 1
  fake_log "dns:apply"
}
verify_dns_manifest_transaction_snapshot_before_commit() { fake_log "dns:snapshot-verify"; }
finalize_dns_manifest_transaction() {
  if grep -Fq '"phase":"transaction","state":"succeeded"' \
    "${CONTROL_PLANE_RELEASE_DOMAIN_TRACE_FILE}"; then
    return 1
  fi
  DNS_MANIFEST_TRANSACTION_FINALIZED="true"
  fake_log "dns:finalize-before-commit"
}
write_dns_manifest_release_record_after_commit() {
  fake_log "dns:write-record"
  [[ "${FAKE_DNS_RECORD_FAIL:-false}" != "true" ]]
}
cleanup_finalized_dns_manifest_snapshot() {
  fake_log "dns:cleanup"
  if [[ "${FAKE_DNS_CLEANUP_RESIDUAL:-false}" == "true" ]]; then
    DNS_MANIFEST_SNAPSHOT_KEEP="true"
    DNS_MANIFEST_SNAPSHOT_FILE="${CONTROL_PLANE_RELEASE_DOMAIN_WORK_DIR}/dns-snapshot.json"
    printf '{}\n' >"${DNS_MANIFEST_SNAPSHOT_FILE}" || return
    chmod 600 "${DNS_MANIFEST_SNAPSHOT_FILE}" || return
    return 0
  fi
  if [[ "${FAKE_DNS_CLEANUP_BROKEN_SYMLINK:-false}" == "true" ]]; then
    DNS_MANIFEST_SNAPSHOT_KEEP="false"
    DNS_MANIFEST_SNAPSHOT_FILE=""
    DNS_MANIFEST_TARGET_STATE_FILE=""
    DNS_MANIFEST_HANDOFF_IDENTITY_FILE=""
    DNS_MANIFEST_TRANSACTION_DIR=""
    return 0
  fi
  DNS_MANIFEST_SNAPSHOT_KEEP="false"
  DNS_MANIFEST_SNAPSHOT_FILE=""
  DNS_MANIFEST_TARGET_STATE_FILE=""
  DNS_MANIFEST_HANDOFF_IDENTITY_FILE=""
  DNS_MANIFEST_TRANSACTION_DIR=""
}
restore_dns_manifest_transaction_after_helm_rollback() {
  [[ "${CONTROL_PLANE_RELEASE_DOMAIN_DNS_ONLY:-false}" == "true" ]] || return 1
  fake_log "dns:restore"
}

acquire_control_plane_backup_coordination_lease() {
  fake_log "lease:acquire"
  CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD="true"
}
drain_control_plane_backup_before_schema_rollout() { fake_log "lease:drain"; }
arm_control_plane_release_recovery_fence() {
  fake_log "lease:fence:$*"
  CONTROL_PLANE_RELEASE_RECOVERY_FENCE_REQUIRED="true"
  CONTROL_PLANE_RELEASE_RECOVERY_FENCE_DISPOSITION="armed-pre-helm"
}
release_control_plane_backup_coordination_lease() {
  fake_log "lease:release"
  if [[ "${FAKE_SIGNAL_POSTCOMMIT:-false}" == "true" &&
    "${CONTROL_PLANE_RELEASE_DOMAIN_TRANSACTION_COMMITTED:-false}" == "true" ]]; then
    fake_signal_current_shell TERM
  fi
  if [[ "${FAKE_LEASE_RELEASE_FAIL:-false}" == "true" ]]; then
    return 1
  fi
  CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD="false"
  CONTROL_PLANE_RELEASE_RECOVERY_FENCE_REQUIRED="false"
  CONTROL_PLANE_RELEASE_RECOVERY_FENCE_DISPOSITION="not-armed"
}
validate_live_api_backup_coordination_ready() {
  [[ "${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD:-false}" == "true" ]] || return 1
  fake_log "backup:live-api-validation"
}
control_plane_release_pre_helm_revision_unchanged() { fake_log "control-plane:revision-proof"; }
control_plane_canary_readiness_gate() { fake_log "control-plane:canary"; }
rollout_status() { fake_log "control-plane:rollout:$*"; }
retry() { shift 2; "$@"; }
smoke_test() { fake_log "control-plane:smoke"; }
control_plane_postgres_primary_pod_name() { printf 'postgres-primary\n'; }

require_daemonset_present() { fake_log "image-cache:present:$*"; }
image_cache_rollout_status() {
  fake_log "image-cache:rollout"
  if [[ "${FAKE_SIGNAL_DURING_APPLY:-false}" == "true" ]]; then
    fake_signal_current_shell TERM
    return 143
  fi
  if [[ "${FAKE_ABNORMAL_EXIT:-false}" == "true" ]]; then
    exit 77
  fi
  [[ "${FAKE_POST_HELM_PROBE_FAIL:-false}" != "true" ]]
}
image_cache_restore_ondelete_after_helm_rollback() {
  fake_log "image-cache:rollback-cleanup"
  if [[ "${FAKE_SIGNAL_DURING_ROLLBACK:-false}" == "true" ]]; then
    fake_signal_current_shell TERM
  fi
}
image_cache_prepare_offline_safe_rollout() { fake_log "FORBIDDEN:image-cache-offline-patch"; return 1; }

setup_case() {
  local raw_case_dir=""
  raw_case_dir="$(mktemp -d "${TMPDIR:-/tmp}/fugue-domain-production-test.XXXXXX")"
  CASE_DIR="$(cd "${raw_case_dir}" && pwd -P)"
  chmod 700 "${CASE_DIR}"
  mkdir "${CASE_DIR}/runner"
  chmod 700 "${CASE_DIR}/runner"
  ln -s "${TEST_SCRIPT}" "${CASE_DIR}/fake-release-evidence"
  ln -s "${TEST_SCRIPT}" "${CASE_DIR}/fake-release-dispatch"

  FAKE_LOG="${CASE_DIR}/operations.log"
  FAKE_VERIFY_COUNT_FILE="${CASE_DIR}/verify.count"
  : >"${FAKE_LOG}"
  chmod 600 "${FAKE_LOG}"
  export FAKE_LOG FAKE_VERIFY_COUNT_FILE

  RUNNER_TEMP="${CASE_DIR}/runner"
  FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE="${CASE_DIR}/public/evidence.json"
  FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE="${CASE_DIR}/public/operational-domain-evidence.json"
  FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR="${CASE_DIR}/public/build-activation-evidence"
  mkdir "${CASE_DIR}/public"
  chmod 700 "${CASE_DIR}/public"
  printf '{}\n' >"${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}"
  chmod 600 "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}"
  mkdir "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}"
  chmod 700 "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}"
  printf '{}\n' >"${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}/build-artifact-plan.json"
  printf '{}\n' >"${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}/composite-decomposition-evidence.json"
  printf '{}\n' >"${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}/image-activation-evidence.json"
  printf '{}\n' >"${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}/image-activation-plan.json"
  printf '%s\n' 'immutable-target-manifest' >"${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}/immutable-target-manifest.yaml"
  chmod 600 "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}"/*
  FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE="apply"
  FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_ID="1234"
  FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_DIGEST="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_URL="https://github.com/example/fugue/actions/runs/123/artifacts/1234"
  FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_ID="5678"
  FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_DIGEST="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_URL="https://github.com/example/fugue/actions/runs/123/artifacts/5678"
  FUGUE_RELEASE_DOMAIN_VERIFIED_IMAGE_ARTIFACTS_DIGEST="sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
  FUGUE_RELEASE_DOMAIN_IMAGE_TARGETS=""
  FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL="${CASE_DIR}/fake-release-evidence"
  FUGUE_RELEASE_DOMAIN_DISPATCH_TOOL="${CASE_DIR}/fake-release-dispatch"
  FUGUE_RELEASE_DOMAIN_BASE_SHA="1111111111111111111111111111111111111111"
  FUGUE_RELEASE_DOMAIN_TARGET_SHA="2222222222222222222222222222222222222222"
  GITHUB_RUN_ID="123"
  GITHUB_RUN_ATTEMPT="1"
  GITHUB_SERVER_URL="https://github.com"
  GITHUB_REPOSITORY="example/fugue"
  PREVIOUS_REVISION=7
  FAKE_CURRENT_REVISION=7
  FAKE_LIVE_MANIFEST="base"
  FAKE_PLAN_DIGEST="sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  FAKE_OUTCOME="single"
  FAKE_DOMAIN="node-local"
  FAKE_SPLIT="false"
  FAKE_PREFLIGHT_EXIT="false"
  FAKE_HELM_BEHAVIOR="success"
  FAKE_POST_HELM_PROBE_FAIL="false"
  FAKE_ABNORMAL_EXIT="false"
  FAKE_PUBLIC_FAIL="false"
  FAKE_TAMPER_CLEANUP="false"
  FAKE_TAMPER_SEALED_ARGV="false"
	FAKE_OPERATIONAL_REPORT_ELIGIBLE="false"
  FAKE_SIGNAL_PRETRANSACTION="false"
  FAKE_SIGNAL_DURING_APPLY="false"
  FAKE_SIGNAL_DURING_ROLLBACK="false"
  FAKE_SIGNAL_POSTCOMMIT="false"
  FAKE_ROLLBACK_IMAGE_FAIL="false"
  FAKE_LEASE_RELEASE_FAIL="false"
  FAKE_DNS_RECORD_FAIL="false"
  FAKE_DNS_CLEANUP_RESIDUAL="false"
  FAKE_DNS_CLEANUP_BROKEN_SYMLINK="false"
  FAKE_VERIFY_FAIL_AT=0
  FAKE_REPOSITORY_VERIFY_FAIL_AT=0
  FAKE_TAMPER_ARGV_INPUT_AT=0
  FAKE_REPOSITORY_VERIFY_COUNT=0
  export FAKE_OUTCOME FAKE_DOMAIN FAKE_PLAN_DIGEST FAKE_VERIFY_FAIL_AT FAKE_PUBLIC_FAIL
	export FAKE_TAMPER_CLEANUP FAKE_OPERATIONAL_REPORT_ELIGIBLE

  FUGUE_RELEASE_NAME="fugue"
  FUGUE_RELEASE_FULLNAME="fugue"
  FUGUE_NAMESPACE="fugue-system"
  FUGUE_HELM_CHART_PATH="${REPO_ROOT}/deploy/helm/fugue"
  FUGUE_HELM_TIMEOUT="10m"
  FUGUE_NODE_LOCAL_DNS_NAMESPACE="kube-system"
  FUGUE_API_DEPLOYMENT_NAME="fugue-api"
  FUGUE_CONTROLLER_DEPLOYMENT_NAME="fugue-controller"
  FUGUE_REGISTRY_DEPLOYMENT_NAME="fugue-registry"
  FUGUE_EDGE_IMAGE_REPOSITORY="example/edge"
  FUGUE_EDGE_IMAGE_TAG="test"
  FUGUE_NODE_LOCAL_DNS_ENABLED="false"
  FUGUE_NODE_LOCAL_DNS_MODE="shadow"
  FUGUE_CONTROL_PLANE_POSTGRES_ENABLED="false"
  FUGUE_CONTROL_PLANE_POSTGRES_EXISTING_SECRET_NAME=""
  FUGUE_SMOKE_RETRIES=1
  FUGUE_SMOKE_DELAY_SECONDS=0
  FUGUE_RELEASE_KUBERNETES_OPERATION_OUTER_TIMEOUT_SECONDS=30
  CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD="false"
  CONTROL_PLANE_RELEASE_RECOVERY_FENCE_REQUIRED="false"
  CONTROL_PLANE_RELEASE_RECOVERY_FENCE_DISPOSITION="not-armed"
  DNS_MANIFEST_TRANSACTION_REQUIRED="false"
  DNS_MANIFEST_TRANSACTION_FINALIZED="false"
  DNS_MANIFEST_SNAPSHOT_KEEP="false"
  DNS_MANIFEST_SNAPSHOT_FILE=""
  DNS_MANIFEST_TARGET_STATE_FILE=""
  DNS_MANIFEST_HANDOFF_IDENTITY_FILE=""
  DNS_MANIFEST_TRANSACTION_DIR=""
  CONTROL_PLANE_RELEASE_PENDING_SIGNAL=""
  CONTROL_PLANE_RELEASE_PENDING_SIGNAL_STATUS="0"
  UPGRADE_OVERRIDE_VALUES_FILE=""
  PUBLIC_DATA_PLANE_HELM_SET_ARGS=()
  NODE_LOCAL_BUILD_PLANE_HELM_SET_ARGS=()
  MAINTENANCE_AGENT_HELM_SET_ARGS=()
  NODE_LOCAL_DNS_HELM_SET_ARGS=()
  DNS_HELM_SET_ARGS=()
  HELM_POST_RENDERER_ARGS=()

  trap 'handle_control_plane_release_signal HUP' HUP
  trap 'handle_control_plane_release_signal INT' INT
  trap 'handle_control_plane_release_signal TERM' TERM
  trap handle_control_plane_backup_coordination_abort USR1
  trap 'handle_control_plane_backup_coordination_abort true' USR2
}

cleanup_case() {
  rm -rf "${CASE_DIR}"
}

run_release_status() {
  local status=0
  if control_plane_release_run_atomic_domain_release; then
    status=0
  else
    status=$?
  fi
  printf '%s\n' "${status}"
}

assert_public_parent_and_cleanup() {
  [[ -f "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" ]] || fail_test "public evidence is missing"
  [[ -f "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}" ]] || fail_test "operational report is missing"
  [[ -d "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}" ]] || fail_test "build-activation report is missing"
  python3 - "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" \
    "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}" \
    "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}" "${RUNNER_TEMP}" <<'PY'
import os
import stat
import sys
evidence, operational, activation, runner = sys.argv[1:]
parent = os.lstat(os.path.dirname(evidence))
artifact = os.lstat(evidence)
operational_artifact = os.lstat(operational)
activation_directory = os.lstat(activation)
if stat.S_IMODE(parent.st_mode) != 0o700 or parent.st_uid != os.geteuid():
    raise SystemExit(1)
if stat.S_IMODE(artifact.st_mode) != 0o600 or artifact.st_uid != os.geteuid():
    raise SystemExit(1)
if stat.S_IMODE(operational_artifact.st_mode) != 0o600 or operational_artifact.st_uid != os.geteuid():
    raise SystemExit(1)
if stat.S_IMODE(activation_directory.st_mode) != 0o700 or activation_directory.st_uid != os.geteuid():
    raise SystemExit(1)
if sorted(os.listdir(activation)) != [
    "build-artifact-plan.json",
    "composite-decomposition-evidence.json",
    "image-activation-evidence.json",
    "image-activation-plan.json",
    "immutable-target-manifest.yaml",
]:
    raise SystemExit(1)
for name in os.listdir(activation):
    item = os.lstat(os.path.join(activation, name))
    if stat.S_IMODE(item.st_mode) != 0o600 or item.st_uid != os.geteuid():
        raise SystemExit(1)
if os.listdir(runner):
    raise SystemExit(1)
PY
}

assert_public_parent_and_preserved_recovery() {
  [[ -f "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" ]] || fail_test "public evidence is missing"
  python3 - "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" "${RUNNER_TEMP}" <<'PY'
import os
import stat
import sys

evidence, runner = sys.argv[1:]
parent = os.lstat(os.path.dirname(evidence))
artifact = os.lstat(evidence)
if stat.S_IMODE(parent.st_mode) != 0o700 or parent.st_uid != os.geteuid():
    raise SystemExit(1)
if stat.S_IMODE(artifact.st_mode) != 0o600 or artifact.st_uid != os.geteuid():
    raise SystemExit(1)
entries = os.listdir(runner)
if len(entries) != 1:
    raise SystemExit(1)
workdir = os.path.join(runner, entries[0])
metadata = os.lstat(workdir)
if (
    not stat.S_ISDIR(metadata.st_mode)
    or stat.S_ISLNK(metadata.st_mode)
    or stat.S_IMODE(metadata.st_mode) != 0o700
    or metadata.st_uid != os.geteuid()
):
    raise SystemExit(1)
trace = os.lstat(os.path.join(workdir, "transaction.trace"))
if (
    not stat.S_ISREG(trace.st_mode)
    or stat.S_ISLNK(trace.st_mode)
    or stat.S_IMODE(trace.st_mode) != 0o600
    or trace.st_uid != os.geteuid()
    or trace.st_nlink != 1
):
    raise SystemExit(1)
PY
}

assert_private_recovery_workdir() {
  python3 - "${RUNNER_TEMP}" <<'PY'
import os
import stat
import sys

runner = sys.argv[1]
entries = os.listdir(runner)
if len(entries) != 1:
    raise SystemExit(1)
workdir = os.path.join(runner, entries[0])
metadata = os.lstat(workdir)
if (
    not stat.S_ISDIR(metadata.st_mode)
    or stat.S_ISLNK(metadata.st_mode)
    or stat.S_IMODE(metadata.st_mode) != 0o700
    or metadata.st_uid != os.geteuid()
):
    raise SystemExit(1)
trace = os.lstat(os.path.join(workdir, "transaction.trace"))
if (
    not stat.S_ISREG(trace.st_mode)
    or stat.S_ISLNK(trace.st_mode)
    or stat.S_IMODE(trace.st_mode) != 0o600
    or trace.st_uid != os.geteuid()
    or trace.st_nlink != 1
):
    raise SystemExit(1)
PY
}

case_zero() {
  setup_case
  trap cleanup_case EXIT
  FAKE_OUTCOME="zero"
  FAKE_DOMAIN=""
  [[ "$(run_release_status)" == "0" ]] || fail_test "zero release failed"
  assert_log_count 1 "preserve:public"
  assert_log_count 1 "preserve:image-cache:"
  assert_log_count 1 "preserve:maintenance"
  assert_log_count 1 "preserve:strict-drain"
  assert_log_count 0 "helm-upgrade:"
  assert_log_count 0 "lease:acquire"
  assert_public_parent_and_cleanup
  [[ ! -s "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}.trace" ]] || fail_test "zero trace is not empty"
}

case_blocked() {
  local outcome="$1"
  setup_case
  trap cleanup_case EXIT
  FAKE_OUTCOME="${outcome}"
  FAKE_DOMAIN=""
  [[ "$(run_release_status)" == "2" ]] || fail_test "${outcome} did not block"
  assert_log_count 1 "preserve:public"
  assert_log_count 1 "preserve:image-cache:"
  assert_log_count 1 "preserve:maintenance"
  assert_log_count 1 "preserve:strict-drain"
  assert_log_count 0 "helm-upgrade:"
  assert_file_contains "${FAKE_LOG}" "dispatch:write-public-evidence:"
  assert_public_parent_and_cleanup
}

case_blocked_public_failure() {
  setup_case
  trap cleanup_case EXIT
  FAKE_OUTCOME="multiple"
  FAKE_DOMAIN=""
  FAKE_PUBLIC_FAIL="true"
  [[ "$(run_release_status)" == "2" ]] || fail_test "blocked publication failure did not freeze"
  [[ ! -e "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" ]] ||
    fail_test "failed blocked public evidence exists"
  assert_private_recovery_workdir
}

case_domain_success() {
  local domain="$1"
  local release_status=""
  setup_case
  trap cleanup_case EXIT
  FAKE_DOMAIN="${domain}"
  release_status="$(run_release_status)"
  if [[ "${release_status}" != "0" ]]; then
    printf '%s\n' '--- fake operations ---' >&2
    sed -n '1,240p' "${FAKE_LOG}" >&2 || :
    if [[ -f "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}.trace" ]]; then
      printf '%s\n' '--- transaction trace ---' >&2
      sed -n '1,120p' "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}.trace" >&2 || :
    fi
    fail_test "${domain} success path failed with status ${release_status}"
  fi
  assert_log_count 1 "helm-upgrade:${domain}"
  assert_log_count 0 "FORBIDDEN:"
  case "${domain}" in
    node-local)
      assert_log_count 1 "dig:node-local"
      assert_log_count 1 "node-local:central-coredns"
      assert_log_count 0 "lease:acquire"
      assert_log_count 0 "dns:apply"
      ;;
    authoritative-dns)
      assert_log_count 1 "dig:authoritative-dns"
      assert_log_count 1 "dns:apply"
      assert_log_count 1 "dns:finalize-before-commit"
      assert_log_count 0 "lease:acquire"
      ;;
    control-plane)
      assert_log_count 1 "rollback-image:preflight"
      assert_log_count 1 "lease:acquire"
      assert_log_count 1 "lease:release"
      assert_log_count 1 "control-plane:smoke"
      assert_log_count 0 "dns:apply"
      assert_log_order "lease:acquire" "lease:fence:"
      assert_log_order "lease:fence:" "rollback-image:preflight"
      assert_log_order "rollback-image:preflight" "helm-upgrade:control-plane"
      ;;
    image-cache)
      assert_log_count 1 "image-cache:rollout"
      assert_log_count 0 "lease:acquire"
      assert_log_count 0 "dns:apply"
      ;;
    backup)
      assert_log_count 1 "lease:acquire"
      assert_log_count 1 "lease:release"
      assert_log_count 1 "backup:live-api-validation"
      assert_log_order "lease:acquire" "backup:live-api-validation"
      assert_log_order "backup:live-api-validation" "helm-upgrade:backup"
      ;;
  esac
  if [[ "${domain}" != "control-plane" ]]; then
    assert_log_count 0 "rollback-image:preflight"
  fi
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}.trace" '"phase":"transaction","state":"succeeded"'
  assert_file_contains "${FAKE_LOG}" "controlPlanePostgresName=custom-control-plane-postgres"
  assert_file_contains "${FAKE_LOG}" "controlPlanePostgresSecretName=custom-control-plane-postgres-app"
  assert_file_contains "${FAKE_LOG}" "telemetryAgentName=fugue-telemetry-agent"
  assert_public_parent_and_cleanup
}

case_prepare_failure_evidence() {
  local mode="$1"
  setup_case
  trap cleanup_case EXIT
  if [[ "${mode}" == "preflight" ]]; then
    FAKE_DOMAIN="control-plane"
    FAKE_PREFLIGHT_EXIT="true"
  else
    FAKE_DOMAIN="image-cache"
    FAKE_SPLIT="true"
  fi
  [[ "$(run_release_status)" == "1" ]] || fail_test "${mode} failure status is wrong"
  assert_log_count 0 "helm-upgrade:"
  assert_log_count 0 "FORBIDDEN:image-cache-offline-patch"
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}.trace" '"phase":"prepare","state":"failed"'
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" "--write-boundary-crossed=false"
  assert_public_parent_and_cleanup
}

case_reverify_failure() {
  local mode="$1"
  setup_case
  trap cleanup_case EXIT
  FAKE_DOMAIN="image-cache"
  case "${mode}" in
    gate) FAKE_VERIFY_FAIL_AT=2 ;;
    repository) FAKE_REPOSITORY_VERIFY_FAIL_AT=3 ;;
    argv) FAKE_TAMPER_ARGV_INPUT_AT=3 ;;
  esac
  [[ "$(run_release_status)" == "1" ]] || fail_test "${mode} reverify failure status is wrong"
  assert_log_count 0 "helm-upgrade:"
  assert_log_count 0 "image-cache:rollout"
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" "--write-boundary-crossed=true"
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" "--rollback-attempted=true"
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" "--rollback-completed=true"
  assert_public_parent_and_cleanup
}

case_helm_failure() {
  local behavior="$1"
  local expected_status="$2"
  local expected_rollback_count="$3"
  setup_case
  trap cleanup_case EXIT
  FAKE_DOMAIN="image-cache"
  FAKE_HELM_BEHAVIOR="${behavior}"
  [[ "$(run_release_status)" == "${expected_status}" ]] || fail_test "${behavior} status is wrong"
  assert_log_count "${expected_rollback_count}" "helm-rollback:"
  if [[ "${expected_status}" == "1" ]]; then
    assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" "--rollback-completed=true"
    assert_file_not_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" "--rollback-failed=true"
  else
    assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" "--rollback-failed=true"
    assert_log_count 0 "image-cache:rollback-cleanup"
    assert_log_count 0 "dns:restore"
    assert_log_count 0 "node-local:restore"
    assert_log_count 0 "lease:release"
    assert_public_parent_and_preserved_recovery
    return
  fi
  assert_public_parent_and_cleanup
}

case_post_helm_probe_failure() {
  setup_case
  trap cleanup_case EXIT
  FAKE_DOMAIN="image-cache"
  FAKE_POST_HELM_PROBE_FAIL="true"
  [[ "$(run_release_status)" == "1" ]] || fail_test "post-Helm probe failure status is wrong"
  assert_log_count 1 "helm-rollback:8:"
  assert_log_count 1 "image-cache:rollback-cleanup"
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" "--rollback-completed=true"
  assert_public_parent_and_cleanup
}

case_sealed_argv_same_inode_tamper() {
  setup_case
  trap cleanup_case EXIT
  FAKE_DOMAIN="node-local"
  FAKE_TAMPER_SEALED_ARGV="true"
  [[ "$(run_release_status)" == "1" ]] || fail_test "same-inode sealed argv tamper status is wrong"
  assert_log_count 1 "sealed-argv:in-place-tamper"
  assert_log_count 0 "helm-upgrade:"
  assert_log_count 0 "helm-rollback:"
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" "--rollback-attempted=true"
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" "--rollback-completed=true"
  assert_public_parent_and_cleanup
}

case_rollback_image_preflight_failure() {
  setup_case
  trap cleanup_case EXIT
  FAKE_DOMAIN="control-plane"
  FAKE_ROLLBACK_IMAGE_FAIL="true"
  [[ "$(run_release_status)" == "1" ]] || fail_test "rollback image preflight failure status is wrong"
  assert_log_count 1 "lease:acquire"
  assert_log_count 1 "lease:fence:"
  assert_log_count 1 "rollback-image:preflight"
  assert_log_count 0 "helm-upgrade:"
  assert_log_count 0 "helm-rollback:"
  assert_log_count 1 "lease:release"
  assert_log_order "lease:fence:" "rollback-image:preflight"
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" "--rollback-completed=true"
  assert_public_parent_and_cleanup
}

case_signal_pretransaction() {
  setup_case
  trap cleanup_case EXIT
  FAKE_DOMAIN="image-cache"
  FAKE_SIGNAL_PRETRANSACTION="true"
  [[ "$(run_release_status)" == "143" ]] || fail_test "pretransaction TERM status is wrong"
  assert_log_count 1 "active-command:terminated"
  assert_log_count 0 "root-signal:TERM"
  assert_log_count 0 "helm-upgrade:"
  [[ ! -e "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" ]] ||
    fail_test "pretransaction signal unexpectedly published a transaction artifact"
  [[ -z "$(find "${RUNNER_TEMP}" -mindepth 1 -print -quit)" ]] ||
    fail_test "pretransaction signal leaked private workdir"
}

case_signal_transaction_active() {
  setup_case
  trap cleanup_case EXIT
  FAKE_DOMAIN="image-cache"
  FAKE_SIGNAL_DURING_APPLY="true"
  [[ "$(run_release_status)" == "143" ]] || fail_test "active transaction TERM status is wrong"
  assert_log_count 1 "root-signal:TERM"
  assert_log_count 1 "active-command:terminated"
  assert_log_count 1 "helm-rollback:8:"
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}.trace" '"phase":"apply","state":"failed"'
  assert_file_order "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}.trace" \
    '"phase":"apply","state":"failed"' '"phase":"rollback","state":"started"'
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" "--rollback-completed=true"
  assert_public_parent_and_cleanup
}

case_postcommit_lease_release_failure() {
  setup_case
  trap cleanup_case EXIT
  FAKE_DOMAIN="backup"
  FAKE_LEASE_RELEASE_FAIL="true"
  [[ "$(run_release_status)" == "2" ]] || fail_test "postcommit Lease release failure did not freeze"
  assert_log_count 1 "lease:release"
  assert_log_count 1 "lease:committed-unsafe"
  assert_log_count 0 "helm-rollback:"
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}.trace" '"phase":"transaction","state":"succeeded"'
  assert_file_not_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}.trace" '"phase":"transaction","state":"failed"'
  assert_public_parent_and_preserved_recovery
}

case_postcommit_dns_failure() {
  local mode="$1"
  setup_case
  trap cleanup_case EXIT
  FAKE_DOMAIN="authoritative-dns"
  if [[ "${mode}" == "record" ]]; then
    FAKE_DNS_RECORD_FAIL="true"
  else
    FAKE_DNS_CLEANUP_RESIDUAL="true"
  fi
  [[ "$(run_release_status)" == "2" ]] || fail_test "postcommit DNS ${mode} failure did not freeze"
  assert_log_count 0 "helm-rollback:"
  assert_log_count 1 "dns:write-record"
  if [[ "${mode}" == "record" ]]; then
    assert_log_count 0 "dns:cleanup"
  else
    assert_log_count 1 "dns:cleanup"
  fi
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}.trace" '"phase":"transaction","state":"succeeded"'
  assert_file_not_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}.trace" '"phase":"transaction","state":"failed"'
  assert_public_parent_and_preserved_recovery
}

case_postcommit_dns_broken_symlink_residual() {
  local status=0
  setup_case
  trap cleanup_case EXIT
  FAKE_DOMAIN="authoritative-dns"
  FAKE_DNS_CLEANUP_BROKEN_SYMLINK="true"
  if control_plane_release_run_atomic_domain_release; then
    status=0
  else
    status=$?
  fi
  [[ "${status}" == "2" ]] || fail_test "postcommit DNS broken-symlink residual did not freeze"
  [[ "${CONTROL_PLANE_RELEASE_FAILURE_PHASE:-}" == "single-domain-committed-dns-cleanup" ]] ||
    fail_test "broken-symlink residual did not fail in committed DNS cleanup"
  [[ "${DNS_MANIFEST_SNAPSHOT_KEEP:-false}" == "true" ]] ||
    fail_test "broken-symlink residual did not preserve DNS recovery state"
  assert_log_count 0 "helm-rollback:"
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}.trace" '"phase":"transaction","state":"succeeded"'
  assert_public_parent_and_preserved_recovery
}

case_signal_during_rollback() {
  setup_case
  trap cleanup_case EXIT
  FAKE_DOMAIN="image-cache"
  FAKE_POST_HELM_PROBE_FAIL="true"
  FAKE_SIGNAL_DURING_ROLLBACK="true"
  [[ "$(run_release_status)" == "143" ]] || fail_test "rollback TERM status is wrong"
  assert_log_count 0 "root-signal:TERM"
  assert_log_count 0 "active-command:terminated"
  assert_log_count 1 "helm-rollback:8:"
  assert_log_count 1 "image-cache:rollback-cleanup"
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" "--rollback-completed=true"
  assert_public_parent_and_cleanup
}

case_signal_postcommit() {
  setup_case
  trap cleanup_case EXIT
  FAKE_DOMAIN="backup"
  FAKE_SIGNAL_POSTCOMMIT="true"
  [[ "$(run_release_status)" == "143" ]] || fail_test "postcommit TERM status is wrong"
  assert_log_count 0 "root-signal:TERM"
  assert_log_count 0 "active-command:terminated"
  assert_log_count 0 "helm-rollback:"
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}.trace" '"phase":"transaction","state":"succeeded"'
  assert_public_parent_and_cleanup
}

case_cleanup_identity_tamper() {
  setup_case
  trap cleanup_case EXIT
  FAKE_DOMAIN="image-cache"
  FAKE_TAMPER_CLEANUP="true"
  [[ "$(run_release_status)" == "2" ]] || fail_test "cleanup identity tamper did not freeze the lane"
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}.trace" '"phase":"transaction","state":"succeeded"'
  assert_public_parent_and_preserved_recovery
  [[ -n "$(find "${RUNNER_TEMP}" -type l -print -quit)" ]] ||
    fail_test "cleanup identity tamper symlink was not retained for evidence"
}

case_public_failure_status() {
  setup_case
  trap cleanup_case EXIT
  FAKE_DOMAIN="image-cache"
  FAKE_PUBLIC_FAIL="true"
  [[ "$(run_release_status)" == "2" ]] || fail_test "public evidence failure was not normalized to a frozen lane"
  [[ ! -e "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" ]] || fail_test "failed public evidence exists"
  assert_private_recovery_workdir
}

case_emergency_nonlocal_exit() {
  local status=0
  setup_case
  trap cleanup_case EXIT
  FAKE_DOMAIN="image-cache"
  FAKE_ABNORMAL_EXIT="true"
  if (
    trap 'handle_control_plane_release_signal HUP' HUP
    trap 'handle_control_plane_release_signal INT' INT
    trap 'handle_control_plane_release_signal TERM' TERM
    trap handle_control_plane_backup_coordination_abort USR1
    trap 'handle_control_plane_backup_coordination_abort true' USR2
    trap 'control_plane_release_domain_emergency_rollback_once >/dev/null 2>&1 || :' EXIT
    control_plane_release_run_atomic_domain_release
  ); then
    status=0
  else
    status=$?
  fi
  [[ "${status}" == "77" ]] || fail_test "abnormal exit status = ${status}, want 77"
  assert_log_count 1 "helm-rollback:8:"
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" "--rollback-attempted=true"
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" "--rollback-completed=true"
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}.trace" '"phase":"apply","state":"failed"'
  assert_file_contains "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}.trace" '"phase":"transaction","state":"failed"'
  assert_public_parent_and_cleanup
}

case_operational_report_binds_build_target_before_dispatch() {
  setup_case
  trap cleanup_case EXIT
  FAKE_DOMAIN="control-plane"
  FUGUE_RELEASE_DOMAIN_IMAGE_TARGETS="controller"
  FUGUE_RELEASE_DOMAIN_CONTROLLER_IMAGE_BASE_SHA="3333333333333333333333333333333333333333"
  FUGUE_RELEASE_DOMAIN_CONTROLLER_IMAGE_DIGEST="sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  FUGUE_CONTROLLER_IMAGE_REPOSITORY="ghcr.io/acme/fugue-controller"
  [[ "$(run_release_status)" == "0" ]] || fail_test "operational report target binding release failed"
  assert_file_contains "${FAKE_LOG}" \
    "evidence:operational-image-plan --changed-evidence"
  assert_file_contains "${FAKE_LOG}" \
    "--target controller=3333333333333333333333333333333333333333=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  assert_file_contains "${FAKE_LOG}" \
    "evidence:image-activation-plans --changed-evidence"
  assert_file_contains "${FAKE_LOG}" \
    "--artifact controller=3333333333333333333333333333333333333333=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb=ghcr.io/acme/fugue-controller@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  assert_file_contains "${FAKE_LOG}" \
    "--provenance-digest sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	assert_file_contains "${FAKE_LOG}" \
	  "evidence:operational-report --changed-evidence"
	assert_file_contains "${FAKE_LOG}" \
	  "--build-artifact-plan"
	assert_file_contains "${FAKE_LOG}" \
	  "--image-activation-plan"
	assert_file_contains "${FAKE_LOG}" \
	  "--image-activation-evidence"
	assert_file_contains "${FAKE_LOG}" \
	  "--immutable-target-manifest"
  assert_log_order "evidence:operational-report" "dispatch:verify:"
  assert_public_parent_and_cleanup
}

case_operational_prepare_stops_before_dispatch() {
  setup_case
  trap cleanup_case EXIT
  rm -f "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}"
  rm -rf "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}"
  FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE="prepare"
  FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_ID=""
  FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_DIGEST=""
  FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_URL=""
  FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_ID=""
  FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_DIGEST=""
  FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_URL=""
  [[ "$(run_release_status)" == "0" ]] || fail_test "operational prepare phase failed"
  [[ -f "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}" ]] ||
    fail_test "operational prepare phase did not materialize its report"
  [[ -f "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}/build-artifact-plan.json" &&
    -f "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}/composite-decomposition-evidence.json" &&
    -f "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}/image-activation-evidence.json" &&
    -f "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}/image-activation-plan.json" &&
    -f "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}/immutable-target-manifest.yaml" ]] ||
    fail_test "operational prepare phase did not materialize build-activation evidence"
  [[ ! -e "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" ]] ||
    fail_test "operational prepare phase unexpectedly published final evidence"
  assert_log_count 0 "helm-upgrade:"
  assert_log_count 0 "node-local:apply"
  assert_log_count 0 "authoritative-dns:apply"
  assert_log_count 0 "control-plane:apply"
  assert_log_count 0 "image-cache:apply"
  assert_log_count 0 "backup:apply"
  assert_log_order "evidence:operational-report" "dispatch:verify:"
  [[ -z "$(find "${RUNNER_TEMP}" -mindepth 1 -print -quit)" ]] ||
    fail_test "operational prepare phase leaked its private workdir"
}

case_operational_apply_requires_upload_proof() {
  setup_case
  trap cleanup_case EXIT
  FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_ID=""
  [[ "$(run_release_status)" == "2" ]] ||
    fail_test "operational apply accepted missing artifact proof"
  assert_log_count 0 "helm-upgrade:"
  [[ ! -e "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" ]] ||
    fail_test "missing artifact proof unexpectedly published evidence"
}

case_build_activation_apply_requires_upload_proof() {
  setup_case
  trap cleanup_case EXIT
  FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_ID=""
  [[ "$(run_release_status)" == "2" ]] ||
    fail_test "operational apply accepted missing build-activation artifact proof"
  assert_log_count 0 "helm-upgrade:"
  [[ ! -e "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" ]] ||
    fail_test "missing build-activation artifact proof unexpectedly published evidence"
}

case_operational_apply_rejects_report_drift() {
  setup_case
  trap cleanup_case EXIT
  printf '{"drift":true}\n' >"${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}"
  chmod 600 "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}"
  [[ "$(run_release_status)" == "2" ]] ||
    fail_test "operational apply accepted report drift after upload"
  assert_log_count 0 "helm-upgrade:"
  [[ ! -e "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" ]] ||
    fail_test "report drift unexpectedly published final evidence"
}

case_operational_apply_rejects_build_activation_drift() {
  setup_case
  trap cleanup_case EXIT
  printf '{"drift":true}\n' >"${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}/image-activation-plan.json"
  chmod 600 "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}/image-activation-plan.json"
  [[ "$(run_release_status)" == "2" ]] ||
    fail_test "operational apply accepted build-activation report drift after upload"
  assert_log_count 0 "helm-upgrade:"
  [[ ! -e "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" ]] ||
    fail_test "build-activation drift unexpectedly published final evidence"
}

case_operational_apply_activates_complete_single_domain() {
	setup_case
	trap cleanup_case EXIT
	FAKE_OUTCOME="unknown"
	FAKE_DOMAIN="control-plane"
	FAKE_OPERATIONAL_REPORT_ELIGIBLE="true"
	export FAKE_OUTCOME FAKE_DOMAIN FAKE_OPERATIONAL_REPORT_ELIGIBLE
	rm -f "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}"
	rm -rf "${FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR}"
	FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE="prepare"
	FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_ID=""
	FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_DIGEST=""
	FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_URL=""
	FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_ID=""
	FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_DIGEST=""
	FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_URL=""
	[[ "$(run_release_status)" == "0" ]] ||
	  fail_test "blocked prepare did not reach the durable upload boundary"
	[[ -f "${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}" ]] ||
	  fail_test "blocked prepare did not materialize operational evidence"
	[[ ! -e "${FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE}" ]] ||
	  fail_test "blocked prepare published stale final evidence"

	FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE="apply"
	FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_ID="1234"
	FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_DIGEST="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_URL="https://github.com/example/fugue/actions/runs/123/artifacts/1234"
	FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_ID="5678"
	FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_DIGEST="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_URL="https://github.com/example/fugue/actions/runs/123/artifacts/5678"
	[[ "$(run_release_status)" == "0" ]] ||
	  fail_test "complete operational single-domain report did not activate"
	assert_log_count 3 "dispatch:authorize:"
	assert_log_count 1 "helm-upgrade:control-plane"
	assert_log_order "evidence:operational-report" "helm-upgrade:control-plane"
	assert_file_contains "${FAKE_LOG}" "--operational-report ${FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE}"
	assert_public_parent_and_cleanup
}

run_case() {
  local label="$1"
  local status=0
  shift
  set +e
  (
    set -e
    "$@"
  )
  status=$?
  set -e
  (( status == 0 )) || fail_test "case ${label} failed"
}

run_case zero case_zero
run_case multiple case_blocked multiple
run_case unknown case_blocked unknown
run_case blocked-public-failure case_blocked_public_failure
run_case operational-prepare-before-dispatch case_operational_prepare_stops_before_dispatch
run_case operational-apply-upload-proof case_operational_apply_requires_upload_proof
run_case build-activation-apply-upload-proof case_build_activation_apply_requires_upload_proof
run_case operational-apply-report-drift case_operational_apply_rejects_report_drift
run_case build-activation-apply-report-drift case_operational_apply_rejects_build_activation_drift
run_case operational-apply-activation case_operational_apply_activates_complete_single_domain
run_case operational-report-build-binding case_operational_report_binds_build_target_before_dispatch
for domain in node-local authoritative-dns control-plane image-cache backup; do
  run_case "success-${domain}" case_domain_success "${domain}"
done
run_case preflight-failure case_prepare_failure_evidence preflight
run_case split-image-cache case_prepare_failure_evidence split
run_case gate-reverify case_reverify_failure gate
run_case repository-reverify case_reverify_failure repository
run_case argv-reverify case_reverify_failure argv
run_case helm-fail-before-record case_helm_failure fail-before-record 1 1
run_case helm-fail-after-record case_helm_failure fail-after-record 1 1
run_case helm-weird-revision case_helm_failure fail-weird 2 0
run_case post-helm-probe case_post_helm_probe_failure
run_case sealed-argv-same-inode-tamper case_sealed_argv_same_inode_tamper
run_case rollback-image-preflight-failure case_rollback_image_preflight_failure
run_case signal-pretransaction case_signal_pretransaction
run_case signal-transaction-active case_signal_transaction_active
run_case signal-during-rollback case_signal_during_rollback
run_case signal-postcommit case_signal_postcommit
run_case postcommit-lease-release-failure case_postcommit_lease_release_failure
run_case postcommit-dns-record-failure case_postcommit_dns_failure record
run_case postcommit-dns-cleanup-residual case_postcommit_dns_failure cleanup
run_case postcommit-dns-broken-symlink-residual case_postcommit_dns_broken_symlink_residual
run_case cleanup-identity-tamper case_cleanup_identity_tamper
run_case public-failure case_public_failure_status
run_case emergency-nonlocal-exit case_emergency_nonlocal_exit

printf '[test_control_plane_release_domain_production] production five-domain activation and rollback matrix passed\n'
