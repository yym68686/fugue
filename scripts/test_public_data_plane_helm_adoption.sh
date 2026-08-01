#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT_UNDER_TEST="${ROOT}/scripts/adopt_public_data_plane_helm_baseline.sh"
HEAD_SHA="$(git -C "${ROOT}" rev-parse HEAD)"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

fail() { printf 'test_public_data_plane_helm_adoption: %s\n' "$*" >&2; exit 1; }
assert_count() {
  local expected="$1" pattern="$2" file="$3" actual
  actual="$(grep -c -- "${pattern}" "${file}" 2>/dev/null || true)"
  [[ "${actual}" == "${expected}" ]] || fail "${file}: ${pattern} count=${actual}, want ${expected}"
}

make_fixture() {
  local dir="$1"
  mkdir -p "${dir}/bin" "${dir}/chart"
  printf 'ownership\n' >"${dir}/ownership.yaml"
  cat >"${dir}/bin/adoption" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
sub="$1"; shift
printf 'tool:%s %s\n' "${sub}" "$*" >>"${TEST_LOG}"
arg() { local want="$1"; shift; while (( $# )); do if [[ "$1" == "${want}" ]]; then printf '%s' "$2"; return; fi; shift; done; return 1; }
case "${sub}" in
  canonicalize-secret-free)
    witness="$(arg --secret-witness-output "$@" || true)"
    [[ -z "${witness}" ]] || printf '{}\n' >"${witness}"
    sed 's/super-secret-bytes/[secret-redacted]/g'
    ;;
  secret-lookup-witness)
    cat >/dev/null
    count_file="${TEST_STATE}/lookup-count"; count=0; [[ -f "${count_file}" ]] && count="$(cat "${count_file}")"; count=$((count+1)); printf '%s' "${count}" >"${count_file}"
    if [[ "${TEST_SCENARIO}" == lookup-uid-drift && "${count}" -ge 2 ]]; then printf '{"drift":true}\n'; else printf '{}\n'; fi
    ;;
  post-render)
    witness="$(arg --secret-witness-output "$@")"
    printf '{}\n' >"${witness}"
    sed 's/super-secret-bytes/[secret-redacted]/g'
    ;;
  transaction-post-render)
    if [[ "${TEST_SCENARIO}" == apply-render-drift || "${TEST_SCENARIO}" == target-output-drift || "${TEST_SCENARIO}" == transaction-swap ]]; then exit 1; fi
    cat
    ;;
  intent) printf '{}\n' ;;
  authorize)
    dir="$(arg --evidence-dir "$@")"
    printf '{}\n' >"${dir}/transaction.json"
    printf '{}\n' >"${dir}/restore.json"
    printf '{}\n'
    ;;
  trace)
    phase="$(arg --phase "$@")"; trace="$(arg --trace "$@")"
    printf 'trace:%s\n' "${phase}" >>"${TEST_LOG}"
    printf '{}\n' >"${trace}"
    printf '{}\n'
    ;;
  verify-prewrite)
    [[ "${TEST_SCENARIO}" != prewrite-fail && "${TEST_SCENARIO}" != manifest-drift && "${TEST_SCENARIO}" != values-drift && "${TEST_SCENARIO}" != observed-drift ]]
    ;;
  restore-patches)
    printf '[{"name":"edge-a","patch":[]},{"name":"edge-b","patch":[]},{"name":"edge-dynamic-b","patch":[]}]\n'
    ;;
  verify-recovery-candidate) : ;;
  verify-recovery-base) [[ ! -f "${TEST_STATE}/upgraded" ]] ;;
  verify-restore) [[ "${TEST_SCENARIO}" != restore-verify-fail ]] ;;
  finalize)
    dir="$(arg --evidence-dir "$@")"
    if [[ "${TEST_SCENARIO}" == finalize-fail ]]; then exit 1; fi
    printf '{"digest":"sha256:1111111111111111111111111111111111111111111111111111111111111111"}\n' >"${dir}/stage1-baseline.json"
    printf '{}\n'
    ;;
  *) exit 97 ;;
esac
MOCK
  chmod +x "${dir}/bin/adoption"

  cat >"${dir}/bin/evidence" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf 'evidence:%s\n' "$*" >>"${TEST_LOG}"
output=""
while (( $# )); do [[ "$1" == --output ]] && { output="$2"; break; }; shift; done
[[ -n "${output}" ]]
printf '%s\n' "${TEST_SCENARIO}-observed-${output##*/}" >"${output}"
MOCK
  chmod +x "${dir}/bin/evidence"

  cat >"${dir}/bin/helm" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf 'helm:%s\n' "$*" >>"${TEST_LOG}"
case "$1" in
  status)
    count_file="${TEST_STATE}/status-count"; count=0; [[ -f "${count_file}" ]] && count="$(cat "${count_file}")"; count=$((count+1)); printf '%s' "${count}" >"${count_file}"
    version=806
    if [[ "${TEST_SCENARIO}" == revision-drift && "${count}" -ge 2 ]]; then version=807; fi
    if [[ -f "${TEST_STATE}/upgraded" ]]; then version=807; fi
    printf '{"version":%s}\n' "${version}"
    ;;
  get)
    case "$2" in
      manifest)
        count_file="${TEST_STATE}/manifest-count"; count=0; [[ -f "${count_file}" ]] && count="$(cat "${count_file}")"; count=$((count+1)); printf '%s' "${count}" >"${count_file}"
        value=base-manifest; [[ "${TEST_SCENARIO}" == manifest-drift && "${count}" -ge 2 ]] && value=drifted-manifest
        [[ -f "${TEST_STATE}/upgraded" ]] && value=target-manifest
        printf '%s\n' "${value}"
        ;;
      values)
        count_file="${TEST_STATE}/values-count"; count=0; [[ -f "${count_file}" ]] && count="$(cat "${count_file}")"; count=$((count+1)); printf '%s' "${count}" >"${count_file}"
        value=values; [[ "${TEST_SCENARIO}" == values-drift && "${count}" -ge 2 ]] && value=drifted-values
        printf '%s\n' "${value}"
        ;;
      *) exit 98 ;;
    esac
    ;;
  template)
    [[ " $* " == *" --dry-run=server "* ]] || { printf 'server-render-mutation-attempt\n' >>"${TEST_LOG}"; exit 1; }
    post_renderer=""
    while (( $# )); do [[ "$1" == --post-renderer ]] && { post_renderer="$2"; break; }; shift; done
    [[ -n "${post_renderer}" ]]
    if [[ "${TEST_SCENARIO}" == server-render-fail ]]; then exit 1; fi
    printf 'rendered-manifest super-secret-bytes\n' | "${post_renderer}"
    ;;
  upgrade)
    printf 'upgrade\n' >>"${TEST_LOG}"
    if [[ "${TEST_SCENARIO}" == apply-fail || "${TEST_SCENARIO}" == restore-patch-fail || "${TEST_SCENARIO}" == restore-verify-fail ]]; then exit 1; fi
    post_renderer=""
    while (( $# )); do [[ "$1" == --post-renderer ]] && { post_renderer="$2"; break; }; shift; done
    [[ -n "${post_renderer}" ]]
    printf 'base-manifest\n' | "${post_renderer}" >/dev/null
    : >"${TEST_STATE}/upgraded"
    ;;
  *) exit 99 ;;
esac
MOCK
  chmod +x "${dir}/bin/helm"

  cat >"${dir}/bin/kubectl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf 'kubectl:%s\n' "$*" >>"${TEST_LOG}"
if [[ " $* " == *" get daemonsets.apps,configmaps "* ]]; then
  printf '{"apiVersion":"v1","kind":"List","items":[]}\n'
elif [[ " $* " == *" get secrets "* ]]; then
  printf '{"apiVersion":"v1","kind":"List","items":[],"raw":"super-secret-bytes"}\n'
elif [[ " $* " == *" patch daemonset "* ]]; then
  count_file="${TEST_STATE}/patch-count"; count=0; [[ -f "${count_file}" ]] && count="$(cat "${count_file}")"; count=$((count+1)); printf '%s' "${count}" >"${count_file}"
  [[ "${TEST_SCENARIO}" != restore-patch-fail || "${count}" -lt 2 ]]
else
  exit 96
fi
MOCK
  chmod +x "${dir}/bin/kubectl"

  cat >"${dir}/coord.sh" <<'MOCK'
acquire_control_plane_backup_coordination_lease() { CONTROL_PLANE_BACKUP_COORDINATION_LEASE_OWNER=release/test-1; CONTROL_PLANE_BACKUP_COORDINATION_LEASE_TOKEN=0123456789abcdef0123456789abcdef; printf 'coord:acquire\n' >>"${TEST_LOG}"; }
arm_control_plane_release_recovery_fence() { printf 'coord:arm:%s\n' "$1" >>"${TEST_LOG}"; }
release_control_plane_backup_coordination_lease() { printf 'coord:release\n' >>"${TEST_LOG}"; }
stop_control_plane_backup_coordination_lease_renewer() { printf 'coord:stop\n' >>"${TEST_LOG}"; }
MOCK
  cat >"${dir}/recovery.sh" <<'MOCK'
public_data_plane_adoption_persist_recovery_wal() { printf 'wal:persist\n' >>"${TEST_LOG}"; }
public_data_plane_adoption_advance_recovery_wal() {
  printf 'wal:advance:%s\n' "$1" >>"${TEST_LOG}"
  [[ "${TEST_SCENARIO}" != wal-fence-readback-fail || "$1" != fence-armed ]]
}
public_data_plane_adoption_seal_terminal_wal() { printf 'wal:seal:%s\n' "$1" >>"${TEST_LOG}"; }
public_data_plane_adoption_delete_terminal_wal() { printf 'wal:delete\n' >>"${TEST_LOG}"; }
public_data_plane_adoption_delete_unarmed_wal() { printf 'wal:delete-unarmed\n' >>"${TEST_LOG}"; }
MOCK
}

run_case() {
  local scenario="$1" expected_status="$2" dry_run="${3:-false}"
  local dir="${TMP}/${scenario}"
  mkdir -p "${dir}/state" "${dir}/evidence"
  make_fixture "${dir}"
  : >"${dir}/log"
  set +e
  PATH="${dir}/bin:${PATH}" \
    TEST_SCENARIO="${scenario}" TEST_LOG="${dir}/log" TEST_STATE="${dir}/state" \
    REPO_ROOT="${ROOT}" FUGUE_EXPECTED_SHA="${HEAD_SHA}" \
    FUGUE_PUBLIC_DATA_PLANE_ADOPTION_TOOL="${dir}/bin/adoption" \
    FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL="${dir}/bin/evidence" \
    FUGUE_RELEASE_DOMAIN_OWNERSHIP_FILE="${dir}/ownership.yaml" \
    FUGUE_HELM_CHART_PATH="${dir}/chart" \
    FUGUE_PUBLIC_DATA_PLANE_ADOPTION_EVIDENCE_DIR="${dir}/evidence" \
    FUGUE_PUBLIC_DATA_PLANE_ADOPTION_COORDINATION_LIBRARY="${dir}/coord.sh" \
    FUGUE_PUBLIC_DATA_PLANE_ADOPTION_RECOVERY_LIBRARY="${dir}/recovery.sh" \
    FUGUE_PUBLIC_DATA_PLANE_ADOPTION_DRY_RUN="${dry_run}" \
    FUGUE_PUBLIC_DATA_PLANE_ADOPTION_FINALIZE_ATTEMPTS=1 FUGUE_PUBLIC_DATA_PLANE_ADOPTION_FINALIZE_DELAY_SECONDS=0 \
    RUNNER_TEMP="${dir}" \
    HELM="${dir}/bin/helm" KUBECTL="${dir}/bin/kubectl" \
    bash "${SCRIPT_UNDER_TEST}" >"${dir}/stdout" 2>"${dir}/stderr"
  status=$?
  set -e
  [[ "${status}" == "${expected_status}" ]] || { cat "${dir}/stderr" >&2; fail "${scenario}: status=${status}, want ${expected_status}"; }
  if find "${dir}" -name '.secret-render-hmac.key' -print -quit | grep -q .; then
    fail "${scenario}: ephemeral secret render HMAC key survived cleanup"
  fi
  printf '%s' "${dir}"
}

dir="$(run_case dry-run 0 true)"
assert_count 0 'coord:acquire' "${dir}/log"
assert_count 0 '^upgrade$' "${dir}/log"
assert_count 0 'patch daemonset' "${dir}/log"
assert_count 2 'helm:template .*--dry-run=server' "${dir}/log"
assert_count 0 'server-render-mutation-attempt' "${dir}/log"
if grep -R -q -- 'super-secret-bytes' "${dir}/evidence" "${dir}/log" "${dir}/stdout" "${dir}/stderr"; then
  fail "dry-run evidence/log output leaked Secret payload bytes"
fi

dir="$(run_case server-render-fail 1 true)"
assert_count 0 'coord:acquire' "${dir}/log"
assert_count 0 '^upgrade$' "${dir}/log"

dir="$(run_case lookup-uid-drift 1 true)"
assert_count 0 'coord:acquire' "${dir}/log"
assert_count 0 '^upgrade$' "${dir}/log"

dir="$(run_case prewrite-fail 1)"
assert_count 1 'coord:acquire' "${dir}/log"
assert_count 1 'coord:release' "${dir}/log"
assert_count 0 'coord:arm' "${dir}/log"
assert_count 0 '^upgrade$' "${dir}/log"

for scenario in manifest-drift values-drift observed-drift; do
  dir="$(run_case "${scenario}" 1)"
  assert_count 1 'coord:release' "${dir}/log"
  assert_count 0 'coord:arm' "${dir}/log"
  assert_count 0 '^upgrade$' "${dir}/log"
done

dir="$(run_case revision-drift 1)"
assert_count 1 'coord:release' "${dir}/log"
assert_count 0 'coord:arm' "${dir}/log"
assert_count 0 '^upgrade$' "${dir}/log"

dir="$(run_case wal-fence-readback-fail 1)"
assert_count 1 'coord:arm' "${dir}/log"
assert_count 1 'wal:advance:fence-armed' "${dir}/log"
assert_count 0 'wal:advance:apply-started' "${dir}/log"
assert_count 0 '^upgrade$' "${dir}/log"
assert_count 0 'patch daemonset' "${dir}/log"
assert_count 0 'trace:fence-armed' "${dir}/log"
assert_count 0 'trace:recovery-fenced' "${dir}/log"
assert_count 0 'coord:release' "${dir}/log"
assert_count 1 'coord:stop' "${dir}/log"
assert_count 1 'primary Stage1 failure: durable WAL fence-armed CAS/readback failed' "${dir}/stderr"
assert_count 0 'trace transition is invalid' "${dir}/stderr"

dir="$(run_case apply-fail 1)"
assert_count 1 '^upgrade$' "${dir}/log"
assert_count 3 'patch daemonset' "${dir}/log"
assert_count 0 'coord:release' "${dir}/log"
assert_count 1 'coord:stop' "${dir}/log"
assert_count 1 'trace:restore-started' "${dir}/log"
assert_count 1 'trace:recovery-fenced' "${dir}/log"

for scenario in apply-render-drift target-output-drift transaction-swap; do
  dir="$(run_case "${scenario}" 1)"
  assert_count 1 '^upgrade$' "${dir}/log"
  assert_count 1 'tool:transaction-post-render' "${dir}/log"
  assert_count 3 'patch daemonset' "${dir}/log"
  assert_count 0 'coord:release' "${dir}/log"
done

dir="$(run_case restore-patch-fail 1)"
assert_count 1 '^upgrade$' "${dir}/log"
assert_count 2 'patch daemonset' "${dir}/log"
assert_count 0 'coord:release' "${dir}/log"
assert_count 1 'trace:restore-failed' "${dir}/log"
assert_count 1 'trace:recovery-fenced' "${dir}/log"

dir="$(run_case finalize-fail 1)"
assert_count 1 '^upgrade$' "${dir}/log"
assert_count 3 'patch daemonset' "${dir}/log"
assert_count 0 'coord:release' "${dir}/log"
assert_count 1 'trace:apply-verification-failed' "${dir}/log"

dir="$(run_case success 0)"
assert_count 1 '^upgrade$' "${dir}/log"
assert_count 0 'patch daemonset' "${dir}/log"
assert_count 1 'coord:release' "${dir}/log"
assert_count 1 'trace:baseline-finalized' "${dir}/log"
assert_count 1 'trace:lease-released' "${dir}/log"
assert_count 1 'wal:seal:baseline-finalized' "${dir}/log"
[[ -f "${dir}/evidence/stage1-baseline.json" ]] || fail "success baseline missing"

printf 'public data-plane Helm adoption fault matrix passed\n'
