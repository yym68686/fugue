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
KUBECONFIG="${KUBECONFIG:-${HOME:?}/.kube/config}"
export KUBECONFIG
FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAME="${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAME:-${RELEASE_FULLNAME}-control-plane-db-backup}"
FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAMESPACE="${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAMESPACE:-${RELEASE_NAMESPACE}}"
FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_COMMAND_TIMEOUT_SECONDS="${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_COMMAND_TIMEOUT_SECONDS:-15}"
CALLER_LEASE_DURATION_SET="${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_DURATION_SECONDS+x}"
CALLER_LEASE_RENEW_SET="${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_RENEW_SECONDS+x}"

log() { printf '[fugue-public-data-plane-adoption-recovery] %s\n' "$*" >&2; }
fail() { log "ERROR: $*"; return 1; }

bind_recovery_lease_duration() {
  local lease_json="$1" owner="$2" token="$3" expected_recovery="$4" duration
  [[ "${expected_recovery}" == true || "${expected_recovery}" == false ]] || return 1
  duration="$(LEASE_JSON="${lease_json}" EXPECTED_OWNER="${owner}" EXPECTED_TOKEN="${token}" \
    EXPECTED_RECOVERY="${expected_recovery}" python3 - <<'PY'
import json, os
value=json.loads(os.environ["LEASE_JSON"])
metadata=value.get("metadata") or {}; annotations=metadata.get("annotations") or {}; spec=value.get("spec") or {}
duration=spec.get("leaseDurationSeconds")
assert metadata.get("uid") and metadata.get("resourceVersion") and not metadata.get("deletionTimestamp")
assert spec.get("holderIdentity") == os.environ["EXPECTED_OWNER"]
assert annotations.get("fugue.pro/coordination-token") == os.environ["EXPECTED_TOKEN"]
if os.environ["EXPECTED_RECOVERY"] == "true":
    assert annotations.get("fugue.pro/recovery-required") == "true"
else:
    assert "fugue.pro/recovery-required" not in annotations
assert type(duration) is int and 1 <= duration <= 2147483647
print(duration)
PY
)" || return 1
  [[ "${duration}" =~ ^[1-9][0-9]*$ ]] || return 1
  FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_DURATION_SECONDS="${duration}"
  PUBLIC_DATA_PLANE_ADOPTION_BOUND_LEASE_DURATION_SECONDS="${duration}"
  readonly FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_DURATION_SECONDS
  readonly PUBLIC_DATA_PLANE_ADOPTION_BOUND_LEASE_DURATION_SECONDS
}

release_bound_recovery_lease() {
  local expected_recovery="${1:-true}" lease_json now patch released_json
  [[ "${expected_recovery}" == true || "${expected_recovery}" == false ]] || return 1
  if ! stop_control_plane_backup_coordination_lease_renewer; then
    log "refusing to release the shared Lease before the renewer and its command group are reaped"
    return 1
  fi
  [[ "${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD}" == true ]] || return 0
  lease_json="$(control_plane_backup_coordination_lease_json)" || return 1
  [[ -n "$(trim_field "${lease_json}")" ]] || return 1
  now="$(control_plane_backup_coordination_now)" || return 1
  patch="$(LEASE_JSON="${lease_json}" LEASE_OWNER="${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_OWNER}" \
    LEASE_TOKEN="${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_TOKEN}" \
    LEASE_DURATION="${PUBLIC_DATA_PLANE_ADOPTION_BOUND_LEASE_DURATION_SECONDS}" LEASE_NOW="${now}" \
    EXPECTED_RECOVERY="${expected_recovery}" python3 - <<'PY'
import json, os
value=json.loads(os.environ["LEASE_JSON"])
metadata=value.get("metadata") or {}; annotations=dict(metadata.get("annotations") or {}); spec=value.get("spec") or {}
duration=spec.get("leaseDurationSeconds"); expected_duration=int(os.environ["LEASE_DURATION"])
resource_version=metadata.get("resourceVersion"); owner=os.environ["LEASE_OWNER"]; token=os.environ["LEASE_TOKEN"]
assert metadata.get("uid") and isinstance(resource_version, str) and resource_version
assert not metadata.get("deletionTimestamp")
assert spec.get("holderIdentity") == owner
assert annotations.get("fugue.pro/coordination-token") == token
if os.environ["EXPECTED_RECOVERY"] == "true":
    assert annotations.get("fugue.pro/recovery-required") == "true"
else:
    assert "fugue.pro/recovery-required" not in annotations
assert type(duration) is int and duration == expected_duration
annotations.pop("fugue.pro/coordination-token")
annotations.pop("fugue.pro/recovery-required", None)
print(json.dumps([
  {"op":"test","path":"/metadata/resourceVersion","value":resource_version},
  {"op":"test","path":"/spec/holderIdentity","value":owner},
  {"op":"test","path":"/metadata/annotations/fugue.pro~1coordination-token","value":token},
  {"op":"test","path":"/spec/leaseDurationSeconds","value":expected_duration},
  {"op":"add","path":"/metadata/annotations","value":annotations},
  {"op":"add","path":"/spec/holderIdentity","value":""},
  {"op":"add","path":"/spec/leaseDurationSeconds","value":expected_duration},
  {"op":"add","path":"/spec/renewTime","value":os.environ["LEASE_NOW"]},
],separators=(",",":")))
PY
)" || return 1
  released_json="$(bounded_kubectl "${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_COMMAND_TIMEOUT_SECONDS}" \
    -n "${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAMESPACE}" \
    patch "lease/${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAME}" \
    --type=json -p "${patch}" -o json)" || return 1
  RELEASED_LEASE_JSON="${released_json}" PREWRITE_LEASE_JSON="${lease_json}" \
    EXPECTED_DURATION="${PUBLIC_DATA_PLANE_ADOPTION_BOUND_LEASE_DURATION_SECONDS}" python3 - <<'PY'
import json, os
value=json.loads(os.environ["RELEASED_LEASE_JSON"])
prewrite=json.loads(os.environ["PREWRITE_LEASE_JSON"])
metadata=value.get("metadata") or {}; annotations=metadata.get("annotations") or {}; spec=value.get("spec") or {}
assert metadata.get("uid") == (prewrite.get("metadata") or {}).get("uid")
assert metadata.get("resourceVersion") and not metadata.get("deletionTimestamp")
assert not str(spec.get("holderIdentity") or "")
assert "fugue.pro/coordination-token" not in annotations
assert "fugue.pro/recovery-required" not in annotations
assert type(spec.get("leaseDurationSeconds")) is int
assert spec["leaseDurationSeconds"] == int(os.environ["EXPECTED_DURATION"])
PY
  bind_released_recovery_lease_identity "${released_json}" \
    "${PUBLIC_DATA_PLANE_ADOPTION_BOUND_LEASE_DURATION_SECONDS}" || return 1
  CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD=false
}

bind_released_recovery_lease_identity() {
  local lease_json="$1" expected_duration="${2:-}" fields
  fields="$(LEASE_JSON="${lease_json}" EXPECTED_DURATION="${expected_duration}" python3 - <<'PY'
import json, os
value=json.loads(os.environ["LEASE_JSON"])
metadata=value.get("metadata") or {}; annotations=metadata.get("annotations") or {}; spec=value.get("spec") or {}
uid=metadata.get("uid"); rv=metadata.get("resourceVersion"); duration=spec.get("leaseDurationSeconds")
assert isinstance(uid,str) and uid and isinstance(rv,str) and rv and not metadata.get("deletionTimestamp")
assert spec.get("holderIdentity") == ""
assert "fugue.pro/coordination-token" not in annotations and "fugue.pro/recovery-required" not in annotations
assert type(duration) is int and 1 <= duration <= 2147483647
if os.environ["EXPECTED_DURATION"]:
    assert duration == int(os.environ["EXPECTED_DURATION"])
print(uid+"\t"+rv+"\t"+str(duration))
PY
)" || return 1
  IFS=$'\t' read -r PUBLIC_DATA_PLANE_ADOPTION_RELEASED_LEASE_UID \
    PUBLIC_DATA_PLANE_ADOPTION_RELEASED_LEASE_RESOURCE_VERSION \
    PUBLIC_DATA_PLANE_ADOPTION_RELEASED_LEASE_DURATION_SECONDS <<<"${fields}"
  [[ -n "${PUBLIC_DATA_PLANE_ADOPTION_RELEASED_LEASE_UID}" && \
    -n "${PUBLIC_DATA_PLANE_ADOPTION_RELEASED_LEASE_RESOURCE_VERSION}" && \
    "${PUBLIC_DATA_PLANE_ADOPTION_RELEASED_LEASE_DURATION_SECONDS}" =~ ^[1-9][0-9]*$ ]]
}

verify_released_recovery_lease() {
  local lease_json
  lease_json="$(${KUBECTL} -n "${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAMESPACE}" \
    get "lease/${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAME}" -o json)" || return 1
  LEASE_JSON="${lease_json}" EXPECTED_UID="${PUBLIC_DATA_PLANE_ADOPTION_RELEASED_LEASE_UID}" \
    EXPECTED_RV="${PUBLIC_DATA_PLANE_ADOPTION_RELEASED_LEASE_RESOURCE_VERSION}" \
    EXPECTED_DURATION="${PUBLIC_DATA_PLANE_ADOPTION_RELEASED_LEASE_DURATION_SECONDS}" python3 - <<'PY'
import json, os
value=json.loads(os.environ["LEASE_JSON"])
metadata=value.get("metadata") or {}; annotations=metadata.get("annotations") or {}; spec=value.get("spec") or {}
assert metadata.get("uid") == os.environ["EXPECTED_UID"]
assert metadata.get("resourceVersion") == os.environ["EXPECTED_RV"]
assert not metadata.get("deletionTimestamp") and spec.get("holderIdentity") == ""
assert "fugue.pro/coordination-token" not in annotations and "fugue.pro/recovery-required" not in annotations
assert type(spec.get("leaseDurationSeconds")) is int
assert spec["leaseDurationSeconds"] == int(os.environ["EXPECTED_DURATION"])
PY
}

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
  [[ -z "${CALLER_LEASE_DURATION_SET}" ]] || fail "recovery Lease duration must come from the durable live Lease"
  [[ -z "${CALLER_LEASE_RENEW_SET}" ]] || fail "recovery does not accept a caller-supplied Lease renew interval"
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

  local cm_name current lease_json lease_state owner token fields phase source target_revision baseline_digest wal_digest origin_run_id recovery_required
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
duration=spec.get("leaseDurationSeconds"); recovery=annotations.get("fugue.pro/recovery-required")
assert metadata.get("uid") and metadata.get("resourceVersion") and not metadata.get("deletionTimestamp")
assert type(duration) is int and 1 <= duration <= 2147483647
terminal={"baseline-finalized","restore-succeeded","aborted-before-apply"}
if owner==wal["leaseOwner"] and isinstance(token,str) and token:
    state="held"
    if wal["phase"] == "lease-acquired": assert recovery in (None,"true")
    else: assert recovery=="true"
    assert "sha256:"+hashlib.sha256(token.encode()).hexdigest()==wal["leaseTokenDigest"]
else:
    state="released"
    assert owner=="" and "fugue.pro/coordination-token" not in annotations and "fugue.pro/recovery-required" not in annotations
    assert wal["phase"] in terminal
    owner=token="-"; recovery="false"
print("\t".join([state,owner,token,wal["phase"],wal["sourceCommit"],wal["targetRevision"],wal.get("baselineDigest") or "-",recovery or "false",str(wal["applyAttempts"]),str(wal["restoreAttempts"])]))
PY
)"
  IFS=$'\t' read -r lease_state owner token phase source target_revision baseline_digest recovery_required apply_attempts restore_attempts <<<"${fields}"
  [[ "${baseline_digest}" != - ]] || baseline_digest=""
  [[ "${source}" == "${EXPECTED_SOURCE_SHA}" ]] || fail "WAL source commit does not match the exact Stage1 source"
  case "${lease_state}" in
    held)
      CONTROL_PLANE_BACKUP_COORDINATION_LEASE_OWNER="${owner}"
      CONTROL_PLANE_BACKUP_COORDINATION_LEASE_TOKEN="${token}"
      CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD=true
      bind_recovery_lease_duration "${lease_json}" "${owner}" "${token}" "${recovery_required}" || fail "durable recovery Lease duration is invalid"
      ;;
    released)
      CONTROL_PLANE_BACKUP_COORDINATION_LEASE_OWNER=""
      CONTROL_PLANE_BACKUP_COORDINATION_LEASE_TOKEN=""
      CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD=false
      bind_released_recovery_lease_identity "${lease_json}" || fail "released recovery Lease identity is invalid"
      ;;
    *) fail "recovery Lease state is invalid" ;;
  esac
  cp "${EVIDENCE_DIR}/wal.json" "${EVIDENCE_DIR}/recovery-wal.json"

  case "${phase}" in
    lease-acquired)
      [[ "${lease_state}" == held ]] || fail "unarmed WAL requires the exact held recovery Lease"
      public_data_plane_adoption_delete_unarmed_wal "${recovery_required}"
      release_bound_recovery_lease "${recovery_required}"
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
  public_data_plane_adoption_seal_terminal_wal "${terminal_phase}" "${lease_state}" || fail "terminal recovery WAL could not be sealed"
  if [[ "${lease_state}" == held ]]; then
    release_bound_recovery_lease
  fi
  verify_released_recovery_lease || fail "released recovery Lease drifted before terminal residue cleanup"
  public_data_plane_adoption_delete_terminal_wal
  log "recovery completed without a second Helm apply"
}

main "$@"
