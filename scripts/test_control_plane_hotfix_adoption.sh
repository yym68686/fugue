#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT
LOG="${TMP}/transaction.log"
PLAN="${TMP}/plan.json"
HEAD_SHA="$(git -C "${ROOT}" rev-parse HEAD)"
CURRENT_SOURCE="$(printf '2%.0s' {1..40})"
ADOPTED_SOURCE="$(printf '3%.0s' {1..40})"
IMAGE_DIGEST="sha256:$(printf 'a%.0s' {1..64})"

PLAN="${PLAN}" HEAD_SHA="${HEAD_SHA}" CURRENT_SOURCE="${CURRENT_SOURCE}" \
  ADOPTED_SOURCE="${ADOPTED_SOURCE}" IMAGE_DIGEST="${IMAGE_DIGEST}" python3 - <<'PY'
import json
import os

plan = {
    "expectedSha": os.environ["HEAD_SHA"],
    "runId": "30733955954",
    "runAttempt": 1,
    "namespace": "fugue-system",
    "releaseName": "fugue",
    "releaseFullname": "fugue-fugue",
    "baseRevision": 806,
    "targetRevision": 807,
    "currentSource": os.environ["CURRENT_SOURCE"],
    "adoptedSource": os.environ["ADOPTED_SOURCE"],
    "liveImageRef": "ghcr.io/example/fugue-api@" + os.environ["IMAGE_DIGEST"],
}
with open(os.environ["PLAN"], "w", encoding="utf-8") as stream:
    json.dump(plan, stream, separators=(",", ":"))
PY

(
  cd "${ROOT}"
  export FUGUE_UPGRADE_LIB_ONLY=true
  # shellcheck source=scripts/upgrade_fugue_control_plane.sh
  source "${ROOT}/scripts/upgrade_fugue_control_plane.sh"

  record() { printf '%s|%s\n' "$$" "$1" >>"${LOG}"; }
  initialize_control_plane_release_job_deadline() { record deadline; }
  detect_kubectl() { printf kubectl; }
  control_plane_hotfix_capture_render_set() { mkdir -p "$1"; record capture; }
  control_plane_hotfix_verify_prewrite_bindings() { record "bindings:$2"; }
  control_plane_hotfix_verify_kubernetes() { record "kubernetes:$1"; }
  control_plane_hotfix_write_wal() { record "wal:$1"; }
  acquire_control_plane_backup_coordination_lease() {
    CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD=true
    CONTROL_PLANE_BACKUP_COORDINATION_LEASE_OWNER="release/${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
    CONTROL_PLANE_BACKUP_COORDINATION_LEASE_TOKEN=0123456789abcdef0123456789abcdef
    record acquire
  }
  require_control_plane_backup_coordination_or_abort() { record require; }
  arm_control_plane_release_recovery_fence() { record arm; }
  control_plane_release_verify_repository_snapshot() { :; }
  control_plane_release_domain_execute_sealed_helm_upgrade() {
    [[ "${CONTROL_PLANE_RELEASE_DOMAIN_ARGV_FD_READY}" == true ]]
    ARGV_OUTPUT="${TMP}/argv" python3 - 16 <<'PY'
import os

raw = os.read(16, 65536)
if not raw.endswith(b"\0"):
    raise SystemExit(1)
argv = [item.decode() for item in raw[:-1].split(b"\0")]
expected = [
    "helm", "upgrade", "fugue", "deploy/helm/fugue", "-n", "fugue-system",
    "--reset-then-reuse-values", "--no-hooks", "--history-max", "20",
    "--timeout", "10m0s", "--wait", "--set-string",
    "api.image.tag=" + os.environ["ADOPTED_SOURCE"], "--set-string",
    "api.image.digest=" + os.environ["IMAGE_DIGEST"],
]
if argv != expected:
    raise SystemExit(1)
with open(os.environ["ARGV_OUTPUT"], "w", encoding="utf-8") as stream:
    stream.write("\n".join(argv) + "\n")
PY
    exec 16<&-
    CONTROL_PLANE_RELEASE_DOMAIN_ARGV_FD_READY=false
    record execute
  }
  control_plane_hotfix_verify_live_target() { record "verify:$1:$2:$3"; }
  release_control_plane_backup_coordination_lease() {
    CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD=false
    record release
  }

  export RUNNER_TEMP="${TMP}" LOG TMP ADOPTED_SOURCE IMAGE_DIGEST
  run_control_plane_hotfix_baseline_adoption <"${PLAN}"
)

EXPECTED=$'deadline\ncapture\nbindings:released\nkubernetes:base\nwal:prepared\nacquire\nrequire\ncapture\nbindings:owned\nkubernetes:base\nwal:prewrite-verified\narm\nwal:forward-started\nexecute\nverify:target:807:target.yaml\nwal:forward-committed\nwal:verified\nrelease'
ACTUAL="$(cut -d'|' -f2- "${LOG}")"
[[ "${ACTUAL}" == "${EXPECTED}" ]]
[[ "$(cut -d'|' -f1 "${LOG}" | sort -u | wc -l | tr -d ' ')" == 1 ]]
[[ "$(grep -c '^api.image.tag=' "${TMP}/argv")" == 1 ]]
[[ "$(grep -c '^api.image.digest=' "${TMP}/argv")" == 1 ]]

(
  cd "${ROOT}"
  export FUGUE_UPGRADE_LIB_ONLY=true
  # shellcheck source=scripts/upgrade_fugue_control_plane.sh
  source "${ROOT}/scripts/upgrade_fugue_control_plane.sh"

  mkdir -p "${TMP}/authorized"
  for manifest in base.yaml target.yaml repeated-target.yaml hybrid.yaml; do
    printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: fixture\n' >"${TMP}/authorized/${manifest}"
  done
  printf '{"targetValuesDigest":"sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"}\n' >"${TMP}/values-plan.json"
  mkdir -p "${TMP}/bin"
  printf '#!/usr/bin/env bash\nexit 0\n' >"${TMP}/bin/go"
  chmod 700 "${TMP}/bin/go"

  CONTROL_PLANE_HOTFIX_WORK_DIR="${TMP}/values-readback"
  CONTROL_PLANE_HOTFIX_AUTHORIZED_DIR="${TMP}/authorized"
  CONTROL_PLANE_HOTFIX_PLAN_FILE="${TMP}/values-plan.json"
  mkdir -p "${CONTROL_PLANE_HOTFIX_WORK_DIR}"
  PATH="${TMP}/bin:${PATH}"
  helm_current_revision() { printf '807\n'; }
  control_plane_hotfix_verify_kubernetes() { :; }
  run_release_long_command() {
    shift 2
    printf '%s\n' "$*" >>"${TMP}/readback-argv"
    case "$*" in
      'helm get manifest fugue -n fugue-system --revision 807') printf '%s\n' 'apiVersion: v1' ;;
      'helm get values fugue -n fugue-system --revision 807 -o json') printf '{}\n' ;;
      *) return 1 ;;
    esac
  }

  control_plane_hotfix_verify_live_target target 807 target.yaml
  grep -Fxq 'helm get values fugue -n fugue-system --revision 807 -o json' "${TMP}/readback-argv"
  ! grep -q -- '--all' "${TMP}/readback-argv"
)

printf '[test_control_plane_hotfix_adoption] fixed single-shell Lease/FD16/Helm transaction passed\n'
