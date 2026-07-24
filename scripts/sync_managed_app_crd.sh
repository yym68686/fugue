#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="${FUGUE_MANAGED_APP_CRD_MANIFEST:-${ROOT_DIR}/deploy/helm/fugue/crds/managedapps.fugue.pro.yaml}"
VALIDATOR="${ROOT_DIR}/scripts/validate_managed_app_crd_transition.py"
KUBECTL_BIN="${KUBECTL_BIN:-kubectl}"
CRD_NAME="managedapps.fugue.pro"

[[ -f "${MANIFEST}" && ! -L "${MANIFEST}" ]] || {
  printf 'managed app CRD manifest is missing or not a regular file: %s\n' "${MANIFEST}" >&2
  exit 1
}
[[ -f "${VALIDATOR}" && ! -L "${VALIDATOR}" ]] || {
  printf 'managed app CRD transition validator is missing: %s\n' "${VALIDATOR}" >&2
  exit 1
}
command -v "${KUBECTL_BIN}" >/dev/null 2>&1 || {
  printf 'kubectl binary is unavailable: %s\n' "${KUBECTL_BIN}" >&2
  exit 1
}
command -v timeout >/dev/null 2>&1 || {
  printf 'timeout is required for managed app CRD synchronization\n' >&2
  exit 1
}

work_dir="$(mktemp -d)"
cleanup() {
  rm -rf -- "${work_dir}"
}
trap cleanup EXIT

live_json="${work_dir}/live.json"
target_json="${work_dir}/target.json"
patch_json="${work_dir}/patch.json"
patch_dry_run_json="${work_dir}/patch-dry-run.json"
after_json="${work_dir}/after.json"
after_target_json="${work_dir}/after-target.json"

timeout --kill-after=2s 30s "${KUBECTL_BIN}" get "crd/${CRD_NAME}" -o json >"${live_json}"
timeout --kill-after=2s 30s "${KUBECTL_BIN}" apply --dry-run=server -f "${MANIFEST}" -o json >"${target_json}"
transition="$(python3 "${VALIDATOR}" "${live_json}" "${target_json}")"

case "${transition}" in
  noop)
    printf 'managed app CRD schema already matches the additive target\n'
    ;;
  additive:*)
    python3 "${VALIDATOR}" "${live_json}" "${target_json}" \
      --patch-output "${patch_json}" >/dev/null
    timeout --kill-after=2s 30s "${KUBECTL_BIN}" patch "crd/${CRD_NAME}" \
      --type=json --patch-file "${patch_json}" --dry-run=server -o json \
      >"${patch_dry_run_json}"
    patch_transition="$(python3 "${VALIDATOR}" "${live_json}" "${patch_dry_run_json}")"
    [[ "${patch_transition}" == "${transition}" ]] || {
      printf 'managed app CRD patch dry-run drifted: got=%s want=%s\n' \
        "${patch_transition}" "${transition}" >&2
      exit 1
    }
    patch_target_transition="$(python3 "${VALIDATOR}" "${patch_dry_run_json}" "${target_json}")"
    [[ "${patch_target_transition}" == "noop" ]] || {
      printf 'managed app CRD patch dry-run does not exactly match target: %s\n' \
        "${patch_target_transition}" >&2
      exit 1
    }
    printf 'patching verified additive managed app CRD status schema: %s\n' "${transition#additive:}"
    timeout --kill-after=2s 60s "${KUBECTL_BIN}" patch "crd/${CRD_NAME}" \
      --type=json --patch-file "${patch_json}"
    timeout --kill-after=2s 70s "${KUBECTL_BIN}" wait \
      --for=condition=Established --timeout=60s "crd/${CRD_NAME}"
    ;;
  *)
    printf 'unexpected managed app CRD transition result: %s\n' "${transition}" >&2
    exit 1
    ;;
esac

timeout --kill-after=2s 30s "${KUBECTL_BIN}" get "crd/${CRD_NAME}" -o json >"${after_json}"
timeout --kill-after=2s 30s "${KUBECTL_BIN}" apply --dry-run=server -f "${MANIFEST}" -o json >"${after_target_json}"
after_transition="$(python3 "${VALIDATOR}" "${after_json}" "${after_target_json}")"
[[ "${after_transition}" == "noop" ]] || {
  printf 'managed app CRD schema did not converge: %s\n' "${after_transition}" >&2
  exit 1
}

diff_status=0
set +e
timeout --kill-after=2s 30s "${KUBECTL_BIN}" diff -f "${MANIFEST}"
diff_status=$?
set -e
[[ "${diff_status}" == "0" ]] || {
  printf 'managed app CRD still differs from the authoritative manifest (status=%s)\n' "${diff_status}" >&2
  exit 1
}
printf 'managed app CRD schema synchronization verified\n'
