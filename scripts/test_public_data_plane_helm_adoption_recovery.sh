#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SUBJECT="${ROOT}/scripts/recover_public_data_plane_helm_adoption.sh"
HEAD_SHA="$(git -C "${ROOT}" rev-parse HEAD)"
WAL_DIGEST="sha256:$(printf '1%.0s' {1..64})"
BASELINE_DIGEST="sha256:$(printf '2%.0s' {1..64})"
TOKEN="0123456789abcdef0123456789abcdef"
TOKEN_DIGEST="sha256:$(printf '%s' "${TOKEN}" | shasum -a 256 | awk '{print $1}')"
TMP="$(mktemp -d)"; trap 'rm -rf "${TMP}"' EXIT
fail(){ echo "recovery-test: $*" >&2; exit 1; }
count(){ local want="$1" value="$2" file="$3"; local got; got="$(grep -c -- "${value}" "${file}" 2>/dev/null || true)"; [[ "${got}" == "${want}" ]] || fail "${value}: got ${got}, want ${want}"; }

fixture(){
  local dir="$1" phase="$2"
  mkdir -p "${dir}/bin" "${dir}/evidence"
  : >"${dir}/log"; printf 'ownership\n' >"${dir}/ownership.yaml"
  cat >"${dir}/bin/adoption" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
sub="$1"; shift; echo "tool:${sub}" >>"${TEST_LOG}"
arg(){ local key="$1"; shift; while (( $# )); do [[ "$1" == "${key}" ]] && { echo "$2"; return; }; shift; done; }
case "${sub}" in
 wal-verify|verify-recovery-candidate|verify-restore) : ;;
 canonicalize-secret-free) cat ;;
 verify-recovery-base) [[ "${TEST_HELM_BASE}" == true ]] ;;
 restore-patches) printf '[{"name":"a","patch":[]},{"name":"b","patch":[]},{"name":"c","patch":[]}]\n' ;;
 finalize) dir="$(arg --evidence-dir "$@")"; printf '{"digest":"%s"}\n' "${TEST_BASELINE_DIGEST}" >"${dir}/stage1-baseline.json" ;;
 *) exit 91 ;;
esac
MOCK
  chmod +x "${dir}/bin/adoption"
  cat >"${dir}/bin/evidence" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
while (( $# )); do [[ "$1" == --output ]] && { printf 'observed\n' >"$2"; exit; }; shift; done
exit 1
MOCK
  chmod +x "${dir}/bin/evidence"
  cat >"${dir}/bin/helm" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
 status) if [[ "${TEST_HELM_BASE}" == true ]]; then echo '{"version":806}'; else echo '{"version":807}'; fi ;;
 get) if [[ "$2" == manifest ]]; then if [[ "${TEST_HELM_BASE}" == true ]]; then echo base; else echo target; fi; else exit 1; fi ;;
 *) echo 'second Helm apply forbidden' >&2; exit 92 ;;
esac
MOCK
  chmod +x "${dir}/bin/helm"
  cat >"${dir}/bin/kubectl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
echo "kubectl:$*" >>"${TEST_LOG}"
if [[ " $* " == *" get daemonsets.apps,configmaps "* ]]; then
  echo '{"apiVersion":"v1","kind":"List","items":[]}'
elif [[ " $* " == *" get lease/"* ]]; then
  recovery=''
  [[ "${TEST_PHASE}" == lease-acquired ]] || recovery=',"fugue.pro/recovery-required":"true"'
  printf '{"metadata":{"uid":"lease-uid","resourceVersion":"9","annotations":{"fugue.pro/coordination-token":"%s"%s}},"spec":{"holderIdentity":"release/123-1"}}\n' "${TEST_TOKEN}" "${recovery}"
elif [[ " $* " == *" get configmap/"* ]]; then
  echo '{}'
elif [[ " $* " == *" patch daemonset "* ]]; then
  :
else
  exit 93
fi
MOCK
  chmod +x "${dir}/bin/kubectl"
  cat >"${dir}/coord.sh" <<'MOCK'
release_control_plane_backup_coordination_lease(){ echo coord:release >>"${TEST_LOG}"; CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD=false; }
control_plane_stale_release_old_process_absent(){ echo coord:origin-process-absent >>"${TEST_LOG}"; }
MOCK
  cat >"${dir}/recovery.sh" <<'MOCK'
public_data_plane_adoption_recovery_cm_name(){ echo recovery-cm; }
public_data_plane_adoption_extract_recovery_configmap(){
  local output="$2"; mkdir -p "${output}"
  cat >"${output}/wal.json" <<EOF
{"digest":"${TEST_WAL_DIGEST}","originRunId":"123","phase":"${TEST_PHASE}","sourceCommit":"${TEST_HEAD_SHA}","targetRevision":"807","baselineDigest":"${TEST_WAL_BASELINE_DIGEST}","leaseOwner":"release/123-1","leaseTokenDigest":"${TEST_TOKEN_DIGEST}"}
EOF
  echo '{}' >"${output}/transaction.json"; echo '{}' >"${output}/restore.json"; echo 7 >"${output}/configmap-resource-version"
}
public_data_plane_adoption_advance_recovery_wal(){ echo "wal:advance:$1" >>"${TEST_LOG}"; }
public_data_plane_adoption_delete_terminal_wal(){ echo wal:delete >>"${TEST_LOG}"; }
public_data_plane_adoption_delete_unarmed_wal(){ echo wal:delete-unarmed >>"${TEST_LOG}"; }
MOCK
}

run(){
 local scenario="$1" phase="$2" helm_base="$3" want="$4" wal_baseline="${5:-}"
 local dir="${TMP}/${scenario}"; fixture "${dir}" "${phase}"
 set +e
 PATH="${dir}/bin:${PATH}" TEST_LOG="${dir}/log" TEST_PHASE="${phase}" TEST_HELM_BASE="${helm_base}" \
 TEST_WAL_DIGEST="${WAL_DIGEST}" TEST_WAL_BASELINE_DIGEST="${wal_baseline}" TEST_BASELINE_DIGEST="${BASELINE_DIGEST}" \
 TEST_TOKEN="${TOKEN}" TEST_TOKEN_DIGEST="${TOKEN_DIGEST}" TEST_HEAD_SHA="${HEAD_SHA}" \
 REPO_ROOT="${ROOT}" FUGUE_EXPECTED_SHA="${HEAD_SHA}" FUGUE_EXPECTED_WAL_DIGEST="${WAL_DIGEST}" FUGUE_EXPECTED_ORIGIN_RUN_ID=123 \
 FUGUE_PUBLIC_DATA_PLANE_ADOPTION_TOOL="${dir}/bin/adoption" FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL="${dir}/bin/evidence" \
 FUGUE_RELEASE_DOMAIN_OWNERSHIP_FILE="${dir}/ownership.yaml" FUGUE_PUBLIC_DATA_PLANE_ADOPTION_EVIDENCE_DIR="${dir}/evidence" \
 FUGUE_PUBLIC_DATA_PLANE_ADOPTION_COORDINATION_LIBRARY="${dir}/coord.sh" FUGUE_PUBLIC_DATA_PLANE_ADOPTION_RECOVERY_LIBRARY="${dir}/recovery.sh" \
 HELM="${dir}/bin/helm" KUBECTL="${dir}/bin/kubectl" bash "${SUBJECT}" >"${dir}/out" 2>"${dir}/err"
 status=$?; set -e
 [[ "${status}" == "${want}" ]] || { cat "${dir}/err" >&2; fail "${scenario}: ${status} != ${want}"; }
 echo "${dir}"
}

dir="$(run death-after-fence fence-armed true 0)"
count 3 'patch daemonset' "${dir}/log"; count 1 'wal:advance:restore-started' "${dir}/log"; count 1 'wal:advance:restore-succeeded' "${dir}/log"; count 1 'coord:release' "${dir}/log"; count 1 '^wal:delete$' "${dir}/log"

dir="$(run death-before-fence lease-acquired true 0)"
count 0 'patch daemonset' "${dir}/log"; count 1 'wal:delete-unarmed' "${dir}/log"; count 1 'coord:release' "${dir}/log"; count 0 '^wal:delete$' "${dir}/log"

dir="$(run death-mid-apply apply-started false 2)"
count 3 'patch daemonset' "${dir}/log"; count 1 'wal:advance:restore-succeeded-awaiting-helm-compensation' "${dir}/log"; count 0 'coord:release' "${dir}/log"; count 0 '^wal:delete$' "${dir}/log"

dir="$(run failed-target-revision apply-failed false 2)"
count 3 'patch daemonset' "${dir}/log"; count 1 'wal:advance:restore-succeeded-awaiting-helm-compensation' "${dir}/log"; count 0 'coord:release' "${dir}/log"

dir="$(run after-apply-before-finalize apply-succeeded false 0)"
count 0 'patch daemonset' "${dir}/log"; count 1 'wal:advance:baseline-finalized' "${dir}/log"; count 1 'coord:release' "${dir}/log"; count 1 '^wal:delete$' "${dir}/log"

dir="$(run after-baseline baseline-finalized false 0 "${BASELINE_DIGEST}")"
count 0 'patch daemonset' "${dir}/log"; count 1 'coord:release' "${dir}/log"; count 1 '^wal:delete$' "${dir}/log"

for file in "${TMP}"/*/log; do count 0 'helm:upgrade' "${file}"; done
printf 'public data-plane adoption cross-process recovery matrix passed\n'
