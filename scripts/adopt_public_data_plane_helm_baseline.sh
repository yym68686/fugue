#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
ADOPTION_TOOL="${FUGUE_PUBLIC_DATA_PLANE_ADOPTION_TOOL:-${REPO_ROOT}/bin/fugue-public-data-plane-adoption}"
EVIDENCE_TOOL="${FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL:-${REPO_ROOT}/bin/fugue-release-domain-evidence}"
OWNERSHIP_FILE="${FUGUE_RELEASE_DOMAIN_OWNERSHIP_FILE:-${REPO_ROOT}/deploy/release-domains/ownership-v1.yaml}"
CHART_PATH="${FUGUE_HELM_CHART_PATH:-${REPO_ROOT}/deploy/helm/fugue}"
EVIDENCE_DIR="${FUGUE_PUBLIC_DATA_PLANE_ADOPTION_EVIDENCE_DIR:-${RUNNER_TEMP:-/tmp}/fugue-public-data-plane-adoption-${GITHUB_RUN_ID:-local}}"
RELEASE_NAME="${FUGUE_RELEASE_NAME:-fugue}"
RELEASE_NAMESPACE="${FUGUE_NAMESPACE:-fugue-system}"
RELEASE_FULLNAME="${FUGUE_RELEASE_FULLNAME:-fugue-fugue}"
EXPECTED_SHA="${FUGUE_EXPECTED_SHA:-}"
DRY_RUN="${FUGUE_PUBLIC_DATA_PLANE_ADOPTION_DRY_RUN:-false}"
KUBECTL="${KUBECTL:-kubectl}"
HELM="${HELM:-helm}"
SECRET_HMAC_KEY_FILE="${EVIDENCE_DIR}/.secret-render-hmac.key"

lease_acquired=false
wal_persisted=false
fence_armed=false
apply_started=false
restore_attempted=false
baseline_finalized=false
lease_released=false
cleanup_running=false

log() { printf '[fugue-public-data-plane-adoption] %s\n' "$*" >&2; }
fail() { log "ERROR: $*"; return 1; }
adoption_now() { python3 -c 'import datetime; print(datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"))'; }

require_private_directory() {
  local path="$1"
  mkdir -p -- "${path}"
  chmod 0700 "${path}"
  [[ -d "${path}" && ! -L "${path}" ]] || fail "private evidence directory is invalid"
}

trace_phase() {
  local phase="$1"
  "${ADOPTION_TOOL}" trace \
    --transaction "${EVIDENCE_DIR}/transaction.json" \
    --trace "${EVIDENCE_DIR}/execution-trace.json" \
    --phase "${phase}" \
    --at "$(adoption_now)" >/dev/null
}

helm_revision() {
  "${HELM}" status "${RELEASE_NAME}" -n "${RELEASE_NAMESPACE}" -o json |
    python3 -c 'import json,sys; value=json.load(sys.stdin).get("version"); assert isinstance(value,int) and value>0; print(value)'
}

capture_snapshot() {
  local output="$1"
  local temporary="${output}.tmp"
  rm -f -- "${temporary}"
  ${KUBECTL} -n "${RELEASE_NAMESPACE}" get daemonsets.apps,configmaps \
    --selector fugue.io/rollout-subsystem=public-data-plane -o json >"${temporary}"
  chmod 0600 "${temporary}"
  mv -f -- "${temporary}" "${output}"
}

snapshot_workloads() {
  local snapshot="$1"
  local output="$2"
  SNAPSHOT="${snapshot}" OUTPUT="${output}" python3 - <<'PY'
import json, os
with open(os.environ["SNAPSHOT"], "r", encoding="utf-8") as source:
    document = json.load(source)
assert document.get("apiVersion") == "v1" and document.get("kind") == "List"
items = document.get("items")
assert isinstance(items, list)
workloads = [item for item in items if item.get("apiVersion") == "apps/v1" and item.get("kind") == "DaemonSet"]
with open(os.environ["OUTPUT"], "x", encoding="utf-8") as target:
    json.dump({"apiVersion": "v1", "kind": "List", "items": workloads}, target, separators=(",", ":"))
    target.write("\n")
os.chmod(os.environ["OUTPUT"], 0o600)
PY
}

canonical_helm_manifest() {
  local revision="$1"
  local output="$2"
  local witness="$3"
  local output_temporary="${output}.tmp"
  local witness_temporary="${witness}.tmp"
  rm -f -- "${output_temporary}" "${witness_temporary}"
  "${HELM}" get manifest "${RELEASE_NAME}" -n "${RELEASE_NAMESPACE}" --revision "${revision}" |
    "${ADOPTION_TOOL}" canonicalize-secret-free \
      --ownership "${OWNERSHIP_FILE}" --namespace "${RELEASE_NAMESPACE}" \
      --secret-hmac-key-file "${SECRET_HMAC_KEY_FILE}" \
      --secret-witness-output "${witness_temporary}" >"${output_temporary}"
  chmod 0600 "${output_temporary}" "${witness_temporary}"
  mv -f -- "${output_temporary}" "${output}"
  mv -f -- "${witness_temporary}" "${witness}"
}

render_target() {
  local values="$1"
  local output="$2"
  local witness="$3"
  local expected_lookup="$4"
  local output_temporary="${output}.tmp"
  local witness_temporary="${witness}.tmp"
  local post_renderer="${output}.post-renderer.sh"
  local lookup_before="${output}.lookup-before.json"
  local lookup_after="${output}.lookup-after.json"
  rm -f -- "${output_temporary}" "${witness_temporary}" "${post_renderer}" "${lookup_before}" "${lookup_after}"
  capture_secret_lookup_witness "${lookup_before}"
  cmp -s "${expected_lookup}" "${lookup_before}" || {
    rm -f -- "${lookup_before}"
    fail "Secret lookup identity drifted before server render"
    return 1
  }
  cat >"${post_renderer}" <<EOF
#!/usr/bin/env bash
set -euo pipefail
exec $(printf '%q' "${ADOPTION_TOOL}") post-render \
  --ownership $(printf '%q' "${OWNERSHIP_FILE}") \
  --intent $(printf '%q' "${EVIDENCE_DIR}/intent.json") \
  --namespace $(printf '%q' "${RELEASE_NAMESPACE}") \
  --secret-hmac-key-file $(printf '%q' "${SECRET_HMAC_KEY_FILE}") \
  --secret-witness-output $(printf '%q' "${witness_temporary}")
EOF
  chmod 0700 "${post_renderer}"
  if ! "${HELM}" template "${RELEASE_NAME}" "${CHART_PATH}" \
    --namespace "${RELEASE_NAMESPACE}" --is-upgrade --no-hooks \
    --dry-run=server --post-renderer "${post_renderer}" \
    -f "${values}" >"${output_temporary}"; then
    rm -f -- "${output_temporary}" "${witness_temporary}" "${post_renderer}" "${lookup_before}" "${lookup_after}"
    return 1
  fi
  capture_secret_lookup_witness "${lookup_after}"
  if ! cmp -s "${expected_lookup}" "${lookup_after}"; then
    rm -f -- "${output_temporary}" "${witness_temporary}" "${post_renderer}" "${lookup_before}" "${lookup_after}"
    fail "Secret lookup identity drifted during server render"
    return 1
  fi
  rm -f -- "${post_renderer}" "${lookup_before}" "${lookup_after}"
  chmod 0600 "${output_temporary}" "${witness_temporary}"
  mv -f -- "${output_temporary}" "${output}"
  mv -f -- "${witness_temporary}" "${witness}"
}

capture_secret_lookup_witness() {
  local output="$1"
  local temporary="${output}.tmp"
  local config_name="${RELEASE_FULLNAME}-config"
  local postgres_name="${RELEASE_FULLNAME}-control-plane-postgres-app"
  local platform_name="${RELEASE_FULLNAME}-platform-component-identity"
  rm -f -- "${temporary}"
  "${KUBECTL}" -n "${RELEASE_NAMESPACE}" get secrets \
    "${config_name}" "${postgres_name}" "${platform_name}" -o json |
    "${ADOPTION_TOOL}" secret-lookup-witness \
      --release "${RELEASE_NAME}" --namespace "${RELEASE_NAMESPACE}" \
      --config-secret "${config_name}" \
      --control-plane-postgres-secret "${postgres_name}" \
      --platform-identity-secret "${platform_name}" >"${temporary}"
  chmod 0600 "${temporary}"
  mv -f -- "${temporary}" "${output}"
}

capture_observed() {
  local base="$1"
  local snapshot="$2"
  local output="$3"
  local workloads="${output}.workloads.json"
  rm -f -- "${workloads}" "${output}"
  snapshot_workloads "${snapshot}" "${workloads}"
  "${EVIDENCE_TOOL}" observed-live-manifest \
    --base-manifest "${base}" --live-workloads "${workloads}" \
    --ownership "${OWNERSHIP_FILE}" --namespace "${RELEASE_NAMESPACE}" \
    --output "${output}"
  rm -f -- "${workloads}"
}

restore_public_snapshot() {
  local patches="${EVIDENCE_DIR}/restore-patches.json"
  local lines="${EVIDENCE_DIR}/restore-patches.tsv"
  local name encoded patch rc=0
  [[ "${restore_attempted}" == false ]] || return 1
  restore_attempted=true
  capture_snapshot "${EVIDENCE_DIR}/restore-candidate-snapshot.json" || return 1
  "${ADOPTION_TOOL}" verify-recovery-candidate \
    --transaction "${EVIDENCE_DIR}/transaction.json" \
    --restore "${EVIDENCE_DIR}/restore.json" \
    --snapshot "${EVIDENCE_DIR}/restore-candidate-snapshot.json" \
    --namespace "${RELEASE_NAMESPACE}" || return 1
  public_data_plane_adoption_advance_recovery_wal restore-started || return 1
  trace_phase restore-started || return 1
  "${ADOPTION_TOOL}" restore-patches --restore "${EVIDENCE_DIR}/restore.json" >"${patches}" || rc=1
  if (( rc == 0 )); then
    PATCHES="${patches}" python3 - <<'PY' >"${lines}"
import base64, json, os
with open(os.environ["PATCHES"], "r", encoding="utf-8") as source:
    patches = json.load(source)
for item in patches:
    patch = json.dumps(item["patch"], separators=(",", ":")).encode()
    print(item["name"] + "\t" + base64.b64encode(patch).decode())
PY
    while IFS=$'\t' read -r name encoded; do
      patch="$(printf '%s' "${encoded}" | base64 --decode)" || { rc=1; break; }
      ${KUBECTL} -n "${RELEASE_NAMESPACE}" patch daemonset "${name}" --type=json -p "${patch}" >/dev/null || { rc=1; break; }
    done <"${lines}"
  fi
  if (( rc == 0 )); then
    local attempt verified=false
    for attempt in $(seq 1 30); do
      if capture_snapshot "${EVIDENCE_DIR}/restore-verification-snapshot.json" &&
        "${ADOPTION_TOOL}" verify-restore \
          --restore "${EVIDENCE_DIR}/restore.json" \
          --snapshot "${EVIDENCE_DIR}/restore-verification-snapshot.json" \
          --release "${RELEASE_NAME}" --namespace "${RELEASE_NAMESPACE}" --fullname "${RELEASE_FULLNAME}"; then
        verified=true
        break
      fi
      sleep 2
    done
    [[ "${verified}" == true ]] || rc=1
  fi
  if (( rc == 0 )); then
    local recovery_phase="restore-succeeded-awaiting-helm-compensation"
    local recovery_revision=""
    recovery_revision="$(helm_revision 2>/dev/null || true)"
    if [[ -n "${recovery_revision}" ]] &&
      canonical_helm_manifest "${recovery_revision}" "${EVIDENCE_DIR}/restore-helm-manifest.yaml" "${EVIDENCE_DIR}/restore-secret-render-witness.json" 2>/dev/null &&
      "${ADOPTION_TOOL}" verify-recovery-base \
        --transaction "${EVIDENCE_DIR}/transaction.json" \
        --current-revision "${recovery_revision}" \
        --current-manifest "${EVIDENCE_DIR}/restore-helm-manifest.yaml"; then
      recovery_phase="restore-succeeded"
    fi
    if ! public_data_plane_adoption_advance_recovery_wal "${recovery_phase}" || ! trace_phase restore-succeeded; then
      rc=1
    fi
  else
    public_data_plane_adoption_advance_recovery_wal restore-failed || true
    trace_phase restore-failed || true
  fi
  return "${rc}"
}

cleanup() {
  local status=$?
  [[ "${cleanup_running}" == false ]] || exit "${status}"
  cleanup_running=true
  set +e
  if [[ "${fence_armed}" == true && "${baseline_finalized}" == false ]]; then
    if [[ "${apply_started}" == true && "${restore_attempted}" == false ]]; then
      restore_public_snapshot
    fi
    trace_phase recovery-fenced
    if declare -F stop_control_plane_backup_coordination_lease_renewer >/dev/null; then
      stop_control_plane_backup_coordination_lease_renewer
    fi
    log "Stage1 failed after the durable fence; coordination Lease remains recovery-required"
  elif [[ "${lease_acquired}" == true && "${lease_released}" == false ]]; then
    if [[ "${wal_persisted}" == true ]]; then
      if ! public_data_plane_adoption_delete_unarmed_wal; then
        if declare -F stop_control_plane_backup_coordination_lease_renewer >/dev/null; then
          stop_control_plane_backup_coordination_lease_renewer
        fi
        log "unarmed WAL cleanup failed; preserving the owned Lease for explicit recovery"
        rm -f -- "${SECRET_HMAC_KEY_FILE}"
        exit 1
      fi
    fi
    release_control_plane_backup_coordination_lease
    trace_phase lease-released
    lease_released=true
  fi
  rm -f -- "${SECRET_HMAC_KEY_FILE}"
  exit "${status}"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

main() {
  [[ "${DRY_RUN}" == true || "${DRY_RUN}" == false ]] || fail "dry-run flag is invalid"
  [[ "${EXPECTED_SHA}" =~ ^[0-9a-f]{40}$ ]] || fail "exact expected SHA is required"
  [[ "$(git -C "${REPO_ROOT}" rev-parse HEAD)" == "${EXPECTED_SHA}" ]] || fail "checkout SHA drifted"
  [[ -x "${ADOPTION_TOOL}" && -x "${EVIDENCE_TOOL}" && -f "${OWNERSHIP_FILE}" && ! -L "${OWNERSHIP_FILE}" ]] || fail "tools or ownership are unavailable"
  command -v "${HELM}" >/dev/null
  command -v "${KUBECTL%% *}" >/dev/null
  require_private_directory "${EVIDENCE_DIR}"
  rm -f -- "${SECRET_HMAC_KEY_FILE}"
  (umask 077; head -c 32 /dev/urandom >"${SECRET_HMAC_KEY_FILE}")
  [[ -f "${SECRET_HMAC_KEY_FILE}" && ! -L "${SECRET_HMAC_KEY_FILE}" && "$(wc -c <"${SECRET_HMAC_KEY_FILE}" | tr -d ' ')" == 32 ]] ||
    fail "ephemeral secret render HMAC key is invalid"
  cp -- "${OWNERSHIP_FILE}" "${EVIDENCE_DIR}/ownership.yaml"
  chmod 0600 "${EVIDENCE_DIR}/ownership.yaml"

  BASE_REVISION="$(helm_revision)"
  [[ "${BASE_REVISION}" =~ ^[1-9][0-9]*$ ]] || fail "live Helm revision is invalid"
  TARGET_REVISION="$((BASE_REVISION + 1))"
  export BASE_REVISION TARGET_REVISION

  capture_snapshot "${EVIDENCE_DIR}/snapshot.json"
  capture_secret_lookup_witness "${EVIDENCE_DIR}/secret-lookup-witness.json"
  canonical_helm_manifest "${BASE_REVISION}" "${EVIDENCE_DIR}/base.yaml" "${EVIDENCE_DIR}/base-secret-render-witness.json"
  "${HELM}" get values "${RELEASE_NAME}" -n "${RELEASE_NAMESPACE}" --all -o yaml >"${EVIDENCE_DIR}/values.yaml"
  chmod 0600 "${EVIDENCE_DIR}/values.yaml"

  local -a common=(
    --evidence-dir "${EVIDENCE_DIR}"
    --release "${RELEASE_NAME}"
    --namespace "${RELEASE_NAMESPACE}"
    --fullname "${RELEASE_FULLNAME}"
    --source-commit "${EXPECTED_SHA}"
    --base-revision "${BASE_REVISION}"
    --target-revision "${TARGET_REVISION}"
    --binding "releaseName=${RELEASE_NAME}"
    --binding "releaseNamespace=${RELEASE_NAMESPACE}"
    --binding "nodeLocalNamespace=${FUGUE_NODE_LOCAL_DNS_NAMESPACE:-kube-system}"
    --binding "nodeLocalName=${FUGUE_NODE_LOCAL_DNS_PRESERVED_NAME:-${RELEASE_FULLNAME}-node-local-dns}"
    --binding "nodeLocalUpstreamServiceName=${FUGUE_NODE_LOCAL_DNS_UPSTREAM_NAME:-${RELEASE_FULLNAME}-dns-upstream}"
    --binding "nodeLocalActiveName=${FUGUE_NODE_LOCAL_DNS_ACTIVE_NAME:-${RELEASE_FULLNAME}-node-local-dns}"
    --binding "dnsName=${FUGUE_DNS_NAME:-${RELEASE_FULLNAME}-dns}"
    --binding "apiName=${FUGUE_API_DEPLOYMENT_NAME:-${RELEASE_FULLNAME}-api}"
    --binding "controllerName=${FUGUE_CONTROLLER_DEPLOYMENT_NAME:-${RELEASE_FULLNAME}-controller}"
    --binding "telemetryAgentName=${FUGUE_TELEMETRY_AGENT_NAME:-${RELEASE_FULLNAME}-telemetry-agent}"
    --binding "serviceName=${FUGUE_SERVICE_NAME:-${RELEASE_FULLNAME}}"
    --binding "ingressName=${FUGUE_INGRESS_NAME:-${RELEASE_FULLNAME}}"
    --binding "imageCacheName=${FUGUE_IMAGE_CACHE_NAME:-${RELEASE_FULLNAME}-image-cache}"
    --binding "controlPlanePostgresName=${FUGUE_CONTROL_PLANE_POSTGRES_NAME:-${RELEASE_FULLNAME}-control-plane-postgres}"
    --binding "controlPlanePostgresSecretName=${FUGUE_CONTROL_PLANE_POSTGRES_SECRET_NAME:-${RELEASE_FULLNAME}-control-plane-postgres-app}"
    --binding "controlPlaneRestoreDrillName=${FUGUE_CONTROL_PLANE_RESTORE_DRILL_NAME:-${RELEASE_FULLNAME}-control-plane-restore-drill}"
  )
  "${ADOPTION_TOOL}" intent "${common[@]}" >"${EVIDENCE_DIR}/intent.json"
  chmod 0600 "${EVIDENCE_DIR}/intent.json"
  render_target "${EVIDENCE_DIR}/values.yaml" "${EVIDENCE_DIR}/target.yaml" "${EVIDENCE_DIR}/target-secret-render-witness.json" "${EVIDENCE_DIR}/secret-lookup-witness.json"
  render_target "${EVIDENCE_DIR}/values.yaml" "${EVIDENCE_DIR}/repeated-target.yaml" "${EVIDENCE_DIR}/repeated-target-secret-render-witness.json" "${EVIDENCE_DIR}/secret-lookup-witness.json"
  cmp -s "${EVIDENCE_DIR}/target.yaml" "${EVIDENCE_DIR}/repeated-target.yaml" || fail "target render is not deterministic"
  capture_observed "${EVIDENCE_DIR}/base.yaml" "${EVIDENCE_DIR}/snapshot.json" "${EVIDENCE_DIR}/observed.yaml"
  "${ADOPTION_TOOL}" authorize "${common[@]}" >/dev/null
  trace_phase prepared

  if [[ "${DRY_RUN}" == true ]]; then
    log "dry-run complete: typed Stage1 transaction authorized; no Lease or Helm write"
    return 0
  fi

  if [[ -n "${FUGUE_PUBLIC_DATA_PLANE_ADOPTION_COORDINATION_LIBRARY:-}" ]]; then
    # Tests inject a coordination mock. The production workflow forbids this variable.
    # shellcheck source=/dev/null
    source "${FUGUE_PUBLIC_DATA_PLANE_ADOPTION_COORDINATION_LIBRARY}"
  else
    FUGUE_UPGRADE_LIB_ONLY=true source "${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh"
  fi
  FUGUE_CONTROL_PLANE_BACKUP_DRAIN_WAIT_SECONDS="${FUGUE_CONTROL_PLANE_BACKUP_DRAIN_WAIT_SECONDS:-120}"
  FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAME="${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAME:-${RELEASE_FULLNAME}-control-plane-db-backup}"
  FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAMESPACE="${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAMESPACE:-${RELEASE_NAMESPACE}}"
  FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_DURATION_SECONDS="${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_DURATION_SECONDS:-120}"
  FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_RENEW_SECONDS="${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_RENEW_SECONDS:-30}"
  FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_COMMAND_TIMEOUT_SECONDS="${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_COMMAND_TIMEOUT_SECONDS:-15}"
  FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_DB_QUERY_TIMEOUT_SECONDS="${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_DB_QUERY_TIMEOUT_SECONDS:-20}"
  FUGUE_CONTROL_PLANE_STALE_RELEASE_RECOVERY_MODE="${FUGUE_CONTROL_PLANE_STALE_RELEASE_RECOVERY_MODE:-}"
  FUGUE_CONTROL_PLANE_STALE_RELEASE_RECOVERY_PROOF_FILE="${FUGUE_CONTROL_PLANE_STALE_RELEASE_RECOVERY_PROOF_FILE:-}"
  if [[ -n "${FUGUE_PUBLIC_DATA_PLANE_ADOPTION_RECOVERY_LIBRARY:-}" ]]; then
    # shellcheck source=/dev/null
    source "${FUGUE_PUBLIC_DATA_PLANE_ADOPTION_RECOVERY_LIBRARY}"
  else
    # shellcheck source=scripts/lib/public_data_plane_adoption_recovery.sh
    source "${REPO_ROOT}/scripts/lib/public_data_plane_adoption_recovery.sh"
  fi
  acquire_control_plane_backup_coordination_lease
  lease_acquired=true
  trace_phase lease-acquired

  [[ "$(helm_revision)" == "${BASE_REVISION}" ]] || fail "live Helm revision drifted before prewrite"
  capture_secret_lookup_witness "${EVIDENCE_DIR}/prewrite-secret-lookup-witness.json"
  canonical_helm_manifest "${BASE_REVISION}" "${EVIDENCE_DIR}/prewrite-base.yaml" "${EVIDENCE_DIR}/prewrite-base-secret-render-witness.json"
  "${HELM}" get values "${RELEASE_NAME}" -n "${RELEASE_NAMESPACE}" --all -o yaml >"${EVIDENCE_DIR}/prewrite-values.yaml"
  chmod 0600 "${EVIDENCE_DIR}/prewrite-values.yaml"
  capture_snapshot "${EVIDENCE_DIR}/prewrite-snapshot.json"
  render_target "${EVIDENCE_DIR}/prewrite-values.yaml" "${EVIDENCE_DIR}/prewrite-target.yaml" "${EVIDENCE_DIR}/prewrite-target-secret-render-witness.json" "${EVIDENCE_DIR}/prewrite-secret-lookup-witness.json"
  render_target "${EVIDENCE_DIR}/prewrite-values.yaml" "${EVIDENCE_DIR}/prewrite-repeated-target.yaml" "${EVIDENCE_DIR}/prewrite-repeated-target-secret-render-witness.json" "${EVIDENCE_DIR}/prewrite-secret-lookup-witness.json"
  capture_observed \
    "${EVIDENCE_DIR}/prewrite-base.yaml" \
    "${EVIDENCE_DIR}/prewrite-snapshot.json" \
    "${EVIDENCE_DIR}/prewrite-observed.yaml"
  if ! "${ADOPTION_TOOL}" verify-prewrite "${common[@]}"; then
    fail "fresh prewrite evidence drifted"
  fi
  trace_phase prewrite-verified
  public_data_plane_adoption_persist_recovery_wal
  wal_persisted=true
  arm_control_plane_release_recovery_fence public-data-plane-adoption-stage1
  fence_armed=true
  public_data_plane_adoption_advance_recovery_wal fence-armed
  trace_phase fence-armed

  cat >"${EVIDENCE_DIR}/post-renderer.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
exec $(printf '%q' "${ADOPTION_TOOL}") transaction-post-render \\
  --ownership $(printf '%q' "${OWNERSHIP_FILE}") \\
  --transaction $(printf '%q' "${EVIDENCE_DIR}/transaction.json") \\
  --namespace $(printf '%q' "${RELEASE_NAMESPACE}") \\
  --secret-hmac-key-file $(printf '%q' "${SECRET_HMAC_KEY_FILE}")
EOF
  chmod 0700 "${EVIDENCE_DIR}/post-renderer.sh"

  public_data_plane_adoption_advance_recovery_wal apply-started
  apply_started=true
  trace_phase apply-started
  if ! "${HELM}" upgrade "${RELEASE_NAME}" "${CHART_PATH}" \
    -n "${RELEASE_NAMESPACE}" --reset-values -f "${EVIDENCE_DIR}/prewrite-values.yaml" \
    --no-hooks --wait --timeout "${FUGUE_PUBLIC_DATA_PLANE_ADOPTION_TIMEOUT:-10m}" \
    --post-renderer "${EVIDENCE_DIR}/post-renderer.sh"; then
    public_data_plane_adoption_advance_recovery_wal apply-failed
    trace_phase apply-failed
    fail "the single Stage1 Helm apply failed"
  fi
  public_data_plane_adoption_advance_recovery_wal apply-succeeded
  trace_phase apply-succeeded

  local final_revision
  final_revision="$(helm_revision)"
  canonical_helm_manifest "${final_revision}" "${EVIDENCE_DIR}/final-manifest.yaml" "${EVIDENCE_DIR}/final-secret-render-witness.json"
  local finalize_attempt finalized=false
  for finalize_attempt in $(seq 1 "${FUGUE_PUBLIC_DATA_PLANE_ADOPTION_FINALIZE_ATTEMPTS:-30}"); do
    if capture_snapshot "${EVIDENCE_DIR}/final-snapshot.json" &&
      capture_observed "${EVIDENCE_DIR}/final-manifest.yaml" "${EVIDENCE_DIR}/final-snapshot.json" "${EVIDENCE_DIR}/final-observed.yaml" &&
      "${ADOPTION_TOOL}" finalize --evidence-dir "${EVIDENCE_DIR}" --revision "${final_revision}" >/dev/null; then
      finalized=true
      break
    fi
    sleep "${FUGUE_PUBLIC_DATA_PLANE_ADOPTION_FINALIZE_DELAY_SECONDS:-2}"
  done
  if [[ "${finalized}" != true ]]; then
    public_data_plane_adoption_advance_recovery_wal apply-verification-failed
    trace_phase apply-verification-failed
    fail "Stage1 postwrite verification failed"
  fi
  local baseline_digest
  baseline_digest="$(BASELINE="${EVIDENCE_DIR}/stage1-baseline.json" python3 -c 'import json,os; print(json.load(open(os.environ["BASELINE"], encoding="utf-8"))["digest"])')"
  public_data_plane_adoption_advance_recovery_wal baseline-finalized "${baseline_digest}"
  if ! trace_phase baseline-finalized; then
    rm -f "${EVIDENCE_DIR}/stage1-baseline.json"
    fail "could not seal the local completed trace"
  fi
  baseline_finalized=true
  release_control_plane_backup_coordination_lease
  lease_released=true
  trace_phase lease-released
  public_data_plane_adoption_delete_terminal_wal
  log "Stage1 adoption complete; immutable Stage2 baseline: ${EVIDENCE_DIR}/stage1-baseline.json"
}

main "$@"
