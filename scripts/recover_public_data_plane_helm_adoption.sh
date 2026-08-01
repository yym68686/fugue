#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
ADOPTION_TOOL="${FUGUE_PUBLIC_DATA_PLANE_ADOPTION_TOOL:-${REPO_ROOT}/bin/fugue-public-data-plane-adoption}"
EVIDENCE_TOOL="${FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL:-${REPO_ROOT}/bin/fugue-release-domain-evidence}"
OWNERSHIP_FILE="${FUGUE_RELEASE_DOMAIN_OWNERSHIP_FILE:-${REPO_ROOT}/deploy/release-domains/ownership-v1.yaml}"
EVIDENCE_DIR="${FUGUE_PUBLIC_DATA_PLANE_ADOPTION_EVIDENCE_DIR:-${RUNNER_TEMP:-/tmp}/fugue-public-data-plane-adoption-recovery-${GITHUB_RUN_ID:-local}}"
RELEASE_NAME="${FUGUE_RELEASE_NAME:-fugue}"
RELEASE_NAMESPACE="${FUGUE_NAMESPACE:-fugue-system}"
RELEASE_FULLNAME="${FUGUE_RELEASE_FULLNAME:-fugue-fugue}"
EXPECTED_SOURCE_SHA="${FUGUE_EXPECTED_SOURCE_SHA:-}"
RECOVERY_SHA="${FUGUE_RECOVERY_SHA:-}"
EXPECTED_WAL_DIGEST="${FUGUE_EXPECTED_WAL_DIGEST:-}"
EXPECTED_ORIGIN_RUN_ID="${FUGUE_EXPECTED_ORIGIN_RUN_ID:-}"
KUBECTL="${KUBECTL:-kubectl}"
HELM="${HELM:-helm}"
FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAME="${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAME:-${RELEASE_FULLNAME}-control-plane-db-backup}"
FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAMESPACE="${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAMESPACE:-${RELEASE_NAMESPACE}}"
FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_COMMAND_TIMEOUT_SECONDS="${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_COMMAND_TIMEOUT_SECONDS:-15}"

log() { printf '[fugue-public-data-plane-adoption-recovery] %s\n' "$*" >&2; }
fail() { log "ERROR: $*"; return 1; }

capture_snapshot() {
  local output="$1"
  local temporary="${output}.tmp"
  rm -f "${temporary}"
  ${KUBECTL} -n "${RELEASE_NAMESPACE}" get daemonsets.apps,configmaps \
    --selector fugue.io/rollout-subsystem=public-data-plane -o json >"${temporary}"
  chmod 0600 "${temporary}"
  mv -f "${temporary}" "${output}"
}

snapshot_workloads() {
  SNAPSHOT="$1" OUTPUT="$2" python3 - <<'PY'
import json, os, pathlib
value=json.load(open(os.environ["SNAPSHOT"], encoding="utf-8"))
assert value.get("apiVersion")=="v1" and value.get("kind")=="List" and isinstance(value.get("items"),list)
items=[x for x in value["items"] if x.get("apiVersion")=="apps/v1" and x.get("kind")=="DaemonSet"]
path=pathlib.Path(os.environ["OUTPUT"]); path.write_text(json.dumps({"apiVersion":"v1","kind":"List","items":items},separators=(",",":"))+"\n"); path.chmod(0o600)
PY
}

helm_revision() {
  "${HELM}" status "${RELEASE_NAME}" -n "${RELEASE_NAMESPACE}" -o json |
    python3 -c 'import json,sys; value=json.load(sys.stdin).get("version"); assert isinstance(value,int) and value>0; print(value)'
}

canonical_helm_manifest() {
  "${HELM}" get manifest "${RELEASE_NAME}" -n "${RELEASE_NAMESPACE}" --revision "$1" |
    "${ADOPTION_TOOL}" canonicalize-secret-free --ownership "${OWNERSHIP_FILE}" --namespace "${RELEASE_NAMESPACE}" >"$2"
  chmod 0600 "$2"
}

capture_observed() {
  local workloads="${3}.workloads.json"
  rm -f "${workloads}" "$3"
  snapshot_workloads "$2" "${workloads}"
  "${EVIDENCE_TOOL}" observed-live-manifest --base-manifest "$1" --live-workloads "${workloads}" \
    --ownership "${OWNERSHIP_FILE}" --namespace "${RELEASE_NAMESPACE}" --output "$3"
  rm -f "${workloads}"
}

restore_from_wal() {
  local phase="$1" patches="${EVIDENCE_DIR}/restore-patches.json" lines="${EVIDENCE_DIR}/restore-patches.tsv"
  local name encoded patch attempt
  capture_snapshot "${EVIDENCE_DIR}/recovery-candidate-snapshot.json"
  "${ADOPTION_TOOL}" verify-recovery-candidate \
    --transaction "${EVIDENCE_DIR}/transaction.json" --restore "${EVIDENCE_DIR}/restore.json" \
    --snapshot "${EVIDENCE_DIR}/recovery-candidate-snapshot.json" --namespace "${RELEASE_NAMESPACE}"
  if [[ "${phase}" != restore-started ]]; then
    public_data_plane_adoption_advance_recovery_wal restore-started
  fi
  "${ADOPTION_TOOL}" restore-patches --restore "${EVIDENCE_DIR}/restore.json" >"${patches}"
  PATCHES="${patches}" python3 - <<'PY' >"${lines}"
import base64,json,os
for item in json.load(open(os.environ["PATCHES"],encoding="utf-8")):
    print(item["name"]+"\t"+base64.b64encode(json.dumps(item["patch"],separators=(",",":")).encode()).decode())
PY
  while IFS=$'\t' read -r name encoded; do
    patch="$(printf '%s' "${encoded}" | base64 --decode)"
    ${KUBECTL} -n "${RELEASE_NAMESPACE}" patch daemonset "${name}" --type=json -p "${patch}" >/dev/null
  done <"${lines}"
  for attempt in $(seq 1 30); do
    if capture_snapshot "${EVIDENCE_DIR}/restore-verification-snapshot.json" &&
      "${ADOPTION_TOOL}" verify-restore --restore "${EVIDENCE_DIR}/restore.json" \
        --snapshot "${EVIDENCE_DIR}/restore-verification-snapshot.json" \
        --release "${RELEASE_NAME}" --namespace "${RELEASE_NAMESPACE}" --fullname "${RELEASE_FULLNAME}"; then
      local current_revision recovery_phase="restore-succeeded-awaiting-helm-compensation"
      current_revision="$(helm_revision 2>/dev/null || true)"
      if [[ -n "${current_revision}" ]] &&
        canonical_helm_manifest "${current_revision}" "${EVIDENCE_DIR}/restore-helm-manifest.yaml" 2>/dev/null &&
        "${ADOPTION_TOOL}" verify-recovery-base \
          --transaction "${EVIDENCE_DIR}/transaction.json" \
          --current-revision "${current_revision}" \
          --current-manifest "${EVIDENCE_DIR}/restore-helm-manifest.yaml"; then
        recovery_phase="restore-succeeded"
      fi
      public_data_plane_adoption_advance_recovery_wal "${recovery_phase}"
      [[ "${recovery_phase}" == restore-succeeded ]] && return 0
      log "live images were restored but Helm metadata requires a separate typed authoritative-only compensation"
      return 2
    fi
    sleep 2
  done
  public_data_plane_adoption_advance_recovery_wal restore-failed || true
  return 1
}

finalize_from_wal() {
  local target_revision="$1" baseline_digest="$2" revision observed_digest
  revision="$(helm_revision)" || return 1
  [[ "${revision}" == "${target_revision}" ]] || { fail "live Helm revision is not the WAL target"; return 1; }
  canonical_helm_manifest "${revision}" "${EVIDENCE_DIR}/final-manifest.yaml" || return 1
  capture_snapshot "${EVIDENCE_DIR}/final-snapshot.json" || return 1
  capture_observed "${EVIDENCE_DIR}/final-manifest.yaml" "${EVIDENCE_DIR}/final-snapshot.json" "${EVIDENCE_DIR}/final-observed.yaml" || return 1
  "${ADOPTION_TOOL}" finalize --evidence-dir "${EVIDENCE_DIR}" --revision "${revision}" >/dev/null || return 1
  observed_digest="$(BASELINE="${EVIDENCE_DIR}/stage1-baseline.json" python3 -c 'import json,os; print(json.load(open(os.environ["BASELINE"],encoding="utf-8"))["digest"])')"
  if [[ -n "${baseline_digest}" ]]; then
    [[ "${observed_digest}" == "${baseline_digest}" ]] || { fail "reconstructed baseline does not match the durable WAL"; return 1; }
  fi
}

verify_aborted_before_apply_state() {
  local revision
  capture_snapshot "${EVIDENCE_DIR}/abort-verification-snapshot.json" || return 1
  "${ADOPTION_TOOL}" verify-restore --restore "${EVIDENCE_DIR}/restore.json" \
    --snapshot "${EVIDENCE_DIR}/abort-verification-snapshot.json" \
    --release "${RELEASE_NAME}" --namespace "${RELEASE_NAMESPACE}" --fullname "${RELEASE_FULLNAME}" || return 1
  revision="$(helm_revision)" || return 1
  canonical_helm_manifest "${revision}" "${EVIDENCE_DIR}/abort-helm-manifest.yaml" || return 1
  "${ADOPTION_TOOL}" verify-recovery-base --transaction "${EVIDENCE_DIR}/transaction.json" \
    --current-revision "${revision}" --current-manifest "${EVIDENCE_DIR}/abort-helm-manifest.yaml"
}

main() {
  [[ "${RECOVERY_SHA}" =~ ^[0-9a-f]{40}$ && "$(git -C "${REPO_ROOT}" rev-parse HEAD)" == "${RECOVERY_SHA}" ]] || fail "exact recovery implementation SHA mismatch"
  [[ "${EXPECTED_SOURCE_SHA}" =~ ^[0-9a-f]{40}$ ]] || fail "exact Stage1 source SHA is required"
  [[ "${EXPECTED_WAL_DIGEST}" =~ ^sha256:[0-9a-f]{64}$ ]] || fail "exact WAL digest is required"
  [[ "${EXPECTED_ORIGIN_RUN_ID}" =~ ^[1-9][0-9]*$ ]] || fail "exact origin run ID is required"
  [[ -x "${ADOPTION_TOOL}" && -x "${EVIDENCE_TOOL}" ]] || fail "recovery tools are unavailable"
  mkdir -p "${EVIDENCE_DIR}"; chmod 0700 "${EVIDENCE_DIR}"
  cp "${OWNERSHIP_FILE}" "${EVIDENCE_DIR}/ownership.yaml"; chmod 0600 "${EVIDENCE_DIR}/ownership.yaml"

  if [[ -n "${FUGUE_PUBLIC_DATA_PLANE_ADOPTION_COORDINATION_LIBRARY:-}" ]]; then
    # Test-only injection; the production recovery workflow forbids it.
    # shellcheck source=/dev/null
    source "${FUGUE_PUBLIC_DATA_PLANE_ADOPTION_COORDINATION_LIBRARY}"
  else
    FUGUE_UPGRADE_LIB_ONLY=true source "${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh"
  fi
  if [[ -n "${FUGUE_PUBLIC_DATA_PLANE_ADOPTION_RECOVERY_LIBRARY:-}" ]]; then
    # shellcheck source=/dev/null
    source "${FUGUE_PUBLIC_DATA_PLANE_ADOPTION_RECOVERY_LIBRARY}"
  else
    # shellcheck source=scripts/lib/public_data_plane_adoption_recovery.sh
    source "${REPO_ROOT}/scripts/lib/public_data_plane_adoption_recovery.sh"
  fi

  local cm_name current lease_json owner token fields phase source target_revision baseline_digest wal_digest origin_run_id recovery_required
  local apply_attempts restore_attempts terminal_phase=""
  cm_name="$(public_data_plane_adoption_recovery_cm_name)"
  current="$(${KUBECTL} -n "${RELEASE_NAMESPACE}" get "configmap/${cm_name}" -o json)"
  RELEASE_NAME="${RELEASE_NAME}" public_data_plane_adoption_extract_recovery_configmap "${current}" "${EVIDENCE_DIR}"
  "${ADOPTION_TOOL}" wal-verify --transaction "${EVIDENCE_DIR}/transaction.json" \
    --restore "${EVIDENCE_DIR}/restore.json" --wal "${EVIDENCE_DIR}/wal.json" >/dev/null
  read -r wal_digest origin_run_id < <(WAL="${EVIDENCE_DIR}/wal.json" python3 -c 'import json,os; v=json.load(open(os.environ["WAL"],encoding="utf-8")); print(v["digest"],v["originRunId"])')
  [[ "${wal_digest}" == "${EXPECTED_WAL_DIGEST}" && "${origin_run_id}" == "${EXPECTED_ORIGIN_RUN_ID}" ]] || fail "recovery WAL dispatch identity mismatch"
  control_plane_stale_release_old_process_absent "${origin_run_id}" || fail "origin Stage1 process is still present on the recovery runner"
  lease_json="$(${KUBECTL} -n "${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAMESPACE}" \
    get "lease/${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAME}" -o json)"
  fields="$(LEASE="${lease_json}" WAL="${EVIDENCE_DIR}/wal.json" python3 - <<'PY'
import hashlib,json,os
lease=json.loads(os.environ["LEASE"]); wal=json.load(open(os.environ["WAL"],encoding="utf-8"))
metadata=lease.get("metadata") or {}; annotations=metadata.get("annotations") or {}; spec=lease.get("spec") or {}
owner=spec.get("holderIdentity"); token=annotations.get("fugue.pro/coordination-token")
assert owner==wal["leaseOwner"] and token
recovery = annotations.get("fugue.pro/recovery-required")
if wal["phase"] == "lease-acquired":
    assert recovery in (None, "true")
else:
    assert annotations.get("fugue.pro/recovery-required")=="true"
assert "sha256:"+hashlib.sha256(token.encode()).hexdigest()==wal["leaseTokenDigest"]
assert not metadata.get("deletionTimestamp")
print("\t".join([owner,token,wal["phase"],wal["sourceCommit"],wal["targetRevision"],wal.get("baselineDigest") or "-",recovery or "false",str(wal["applyAttempts"]),str(wal["restoreAttempts"])]))
PY
)"
  IFS=$'\t' read -r owner token phase source target_revision baseline_digest recovery_required apply_attempts restore_attempts <<<"${fields}"
  [[ "${baseline_digest}" != - ]] || baseline_digest=""
  [[ "${source}" == "${EXPECTED_SOURCE_SHA}" ]] || fail "WAL source commit does not match the exact Stage1 source"
  CONTROL_PLANE_BACKUP_COORDINATION_LEASE_OWNER="${owner}"
  CONTROL_PLANE_BACKUP_COORDINATION_LEASE_TOKEN="${token}"
  CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD=true
  cp "${EVIDENCE_DIR}/wal.json" "${EVIDENCE_DIR}/recovery-wal.json"

  case "${phase}" in
    lease-acquired)
      public_data_plane_adoption_delete_unarmed_wal "${recovery_required}"
      release_control_plane_backup_coordination_lease
      log "unarmed recovery WAL cleared; no Helm apply was attempted"
      return 0
      ;;
    baseline-finalized)
      finalize_from_wal "${target_revision}" "${baseline_digest}"
      terminal_phase=baseline-finalized
      ;;
    restore-succeeded)
      capture_snapshot "${EVIDENCE_DIR}/restore-verification-snapshot.json"
      "${ADOPTION_TOOL}" verify-restore --restore "${EVIDENCE_DIR}/restore.json" \
        --snapshot "${EVIDENCE_DIR}/restore-verification-snapshot.json" \
        --release "${RELEASE_NAME}" --namespace "${RELEASE_NAMESPACE}" --fullname "${RELEASE_FULLNAME}"
      revision="$(helm_revision)"
      canonical_helm_manifest "${revision}" "${EVIDENCE_DIR}/restore-helm-manifest.yaml"
      "${ADOPTION_TOOL}" verify-recovery-base --transaction "${EVIDENCE_DIR}/transaction.json" \
        --current-revision "${revision}" --current-manifest "${EVIDENCE_DIR}/restore-helm-manifest.yaml"
      terminal_phase=restore-succeeded
      ;;
    restore-failed|restore-succeeded-awaiting-helm-compensation)
      fail "durable WAL records a failed restore; operator repair is required"
      ;;
    fence-armed)
      [[ "${apply_attempts}" == 0 && "${restore_attempts}" == 0 ]] || fail "fence-armed WAL attempt counters are not zero"
      verify_aborted_before_apply_state || fail "pre-apply abort state is not exact; preserving the recovery fence"
      public_data_plane_adoption_advance_recovery_wal aborted-before-apply || fail "could not durably record the zero-write abort"
      terminal_phase=aborted-before-apply
      ;;
    aborted-before-apply)
      [[ "${apply_attempts}" == 0 && "${restore_attempts}" == 0 ]] || fail "aborted WAL attempt counters are not zero"
      verify_aborted_before_apply_state || fail "zero-write abort state drifted; preserving the recovery fence"
      terminal_phase=aborted-before-apply
      ;;
    apply-succeeded)
      if finalize_from_wal "${target_revision}" ""; then
        baseline_digest="$(BASELINE="${EVIDENCE_DIR}/stage1-baseline.json" python3 -c 'import json,os; print(json.load(open(os.environ["BASELINE"],encoding="utf-8"))["digest"])')"
        public_data_plane_adoption_advance_recovery_wal baseline-finalized "${baseline_digest}"
        terminal_phase=baseline-finalized
      else
        rm -f "${EVIDENCE_DIR}/stage1-baseline.json"
        restore_from_wal "${phase}"
        terminal_phase=restore-succeeded
      fi
      ;;
    apply-started|apply-failed|apply-verification-failed|restore-started)
      restore_from_wal "${phase}"
      terminal_phase=restore-succeeded
      ;;
    *) fail "durable WAL phase is unsupported" ;;
  esac
  [[ -n "${terminal_phase}" ]] || fail "recovery did not reach a terminal WAL phase"
  public_data_plane_adoption_seal_terminal_wal "${terminal_phase}" || fail "terminal recovery WAL could not be sealed"
  release_control_plane_backup_coordination_lease
  public_data_plane_adoption_delete_terminal_wal
  log "recovery completed without a second Helm apply"
}

main "$@"
