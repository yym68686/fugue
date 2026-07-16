#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export FUGUE_UPGRADE_LIB_ONLY=true
# shellcheck source=scripts/upgrade_fugue_control_plane.sh
source "${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh"

fail() {
  printf '[test_control_plane_release_render] ERROR: %s\n' "$*" >&2
  exit 1
}

assert_eq() {
  local got="$1"
  local want="$2"
  local label="$3"
  [[ "${got}" == "${want}" ]] || fail "${label}: got ${got}, want ${want}"
}

TEMP_DIR="$(mktemp -d)"
TARGET_ARGV_FILE="${TEMP_DIR}/target.argv"
BASE_ARGV_FILE="${TEMP_DIR}/base.argv"
REPEATED_TARGET_ARGV_FILE="${TEMP_DIR}/repeated-target.argv"
SOURCE_BEFORE_FILE="${TEMP_DIR}/source-before.argv"
SOURCE_AFTER_FILE="${TEMP_DIR}/source-after.argv"
trap 'rm -rf "${TEMP_DIR}"' EXIT

while IFS= read -r name; do
  [[ -n "${name}" ]] || continue
  printf -v "${name}" '%s' "fixture-${name}"
done < <(
  declare -f with_frozen_control_plane_helm_upgrade_argv |
    python3 -c 'import re,sys; print("\n".join(sorted(set(re.findall(r"\$\{(FUGUE_[A-Z0-9_]+)", sys.stdin.read())))))'
)

FUGUE_RELEASE_NAME=release-fixture
FUGUE_NAMESPACE=namespace-fixture
FUGUE_HELM_CHART_PATH='/private/target chart'
HELM_POST_RENDERER_ARGS=(--post-renderer '/private/post renderer')
HEADSCALE_HELM_SET_ARGS=(--set-string 'headscale.fixture=value with space')
DNS_HELM_SET_ARGS=(--set-string 'dns.fixture=value')
NODE_LOCAL_DNS_HELM_SET_ARGS=(--set-string 'nodeLocalDNS.fixture=value')
PUBLIC_DATA_PLANE_HELM_SET_ARGS=(--set-string 'edge.fixture=value')
NODE_LOCAL_BUILD_PLANE_HELM_SET_ARGS=(--set-string 'imageCache.fixture=value')
MAINTENANCE_AGENT_HELM_SET_ARGS=(--set-string 'maintenance.fixture=value')
CORE_IMAGE_DIGEST_HELM_SET_ARGS=(--set-string 'api.image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')

RENDER_PHASES=()

capture_private_render_argv() {
  local phase="$1"
  shift
  RENDER_PHASES+=("${phase}")
  case "${phase}" in
    target)
      if (CONTROL_PLANE_RELEASE_TARGET_RENDER_ARGV+=(tampered)) 2>/dev/null; then
        return 70
      fi
      if (CONTROL_PLANE_RELEASE_RENDER_SOURCE_ARGV+=(tampered)) 2>/dev/null; then
        return 71
      fi
      printf '%s\0' "$@" >"${TARGET_ARGV_FILE}"
      ;;
    base)
      if (CONTROL_PLANE_RELEASE_BASE_RENDER_ARGV+=(tampered)) 2>/dev/null; then
        return 72
      fi
      printf '%s\0' "$@" >"${BASE_ARGV_FILE}"
      ;;
    repeated-target)
      if (CONTROL_PLANE_RELEASE_REPEATED_TARGET_RENDER_ARGV+=(tampered)) 2>/dev/null; then
        return 73
      fi
      printf '%s\0' "$@" >"${REPEATED_TARGET_ARGV_FILE}"
      ;;
    *)
      return 74
      ;;
  esac
}

render_from_frozen_upgrade_argv() {
  [[ "$1" == "synthetic-timeout" && "$2" == "Helm upgrade" ]] || return 75
  shift 2
  printf '%s\0' "$@" >"${SOURCE_BEFORE_FILE}"
  control_plane_release_with_private_manifest_render_argv 17 capture_private_render_argv "$@" || return
  printf '%s\0' "$@" >"${SOURCE_AFTER_FILE}"
}

with_frozen_control_plane_helm_upgrade_argv \
  '/private/override values.yaml' \
  render_from_frozen_upgrade_argv \
  synthetic-timeout \
  'Helm upgrade' \
  >"${TEMP_DIR}/stdout" 2>"${TEMP_DIR}/stderr"

[[ ! -s "${TEMP_DIR}/stdout" && ! -s "${TEMP_DIR}/stderr" ]] ||
  fail "render argv wrapper emitted private command data"
assert_eq "${RENDER_PHASES[*]}" "target base repeated-target" "private render phase order"
cmp -s "${SOURCE_BEFORE_FILE}" "${SOURCE_AFTER_FILE}" ||
  fail "render argv wrapper mutated the frozen source arguments"
cmp -s "${TARGET_ARGV_FILE}" "${REPEATED_TARGET_ARGV_FILE}" ||
  fail "target and repeated-target render arguments differ"

python3 - \
  "${SOURCE_BEFORE_FILE}" "${TARGET_ARGV_FILE}" "${BASE_ARGV_FILE}" <<'PY'
import sys

def items(path):
    payload = open(path, "rb").read()
    if not payload.endswith(b"\0"):
        raise SystemExit(f"{path} is not NUL terminated")
    return payload.split(b"\0")[:-1]

source = items(sys.argv[1])
target = items(sys.argv[2])
base = items(sys.argv[3])
if target[:-3] != source or target[-3:] != [b"--dry-run=server", b"--output", b"json"]:
    raise SystemExit("target render is not the exact upgrade argv plus fixed server dry-run flags")
if target.count(b"--dry-run=server") != 1 or target.count(b"--output") != 1:
    raise SystemExit("target render controls are not unique")
if b"--hide-secret" in target:
    raise SystemExit("private ownership render must not drop Secret objects")
expected_base_prefix = [
    b"helm", b"get", b"all", b"release-fixture",
    b"-n", b"namespace-fixture", b"--revision", b"17", b"--template",
]
if base[:9] != expected_base_prefix or len(base) != 10:
    raise SystemExit("base render is not bound to the exact stored Helm revision")
template = base[9]
for required in (b".Release.Manifest", b".Release.Hooks", b".Manifest"):
    if required not in template:
        raise SystemExit("base release template omits stored manifest or hooks")
PY

VALID_RENDER_SOURCE=(
  helm upgrade release-fixture '/private/target chart'
  -n namespace-fixture
  --reset-then-reuse-values
  --timeout 10m0s
  -f '/private/override values.yaml'
)
FAIL_PHASE=""
FAIL_TRACE=()

failing_render_consumer() {
  local phase="$1"
  FAIL_TRACE+=("${phase}")
  case "${phase}" in
    target) [[ "${FAIL_PHASE}" != "target" ]] || return 81 ;;
    base) [[ "${FAIL_PHASE}" != "base" ]] || return 82 ;;
    repeated-target) [[ "${FAIL_PHASE}" != "repeated-target" ]] || return 83 ;;
  esac
}

for failure in target base repeated-target; do
  FAIL_PHASE="${failure}"
  FAIL_TRACE=()
  rc=0
  control_plane_release_with_private_manifest_render_argv \
    23 failing_render_consumer "${VALID_RENDER_SOURCE[@]}" || rc=$?
  case "${failure}" in
    target)
      assert_eq "${rc}" "81" "target failure status"
      assert_eq "${FAIL_TRACE[*]}" "target" "target failure trace"
      ;;
    base)
      assert_eq "${rc}" "82" "base failure status"
      assert_eq "${FAIL_TRACE[*]}" "target base" "base failure trace"
      ;;
    repeated-target)
      assert_eq "${rc}" "83" "repeated-target failure status"
      assert_eq "${FAIL_TRACE[*]}" "target base repeated-target" "repeated-target failure trace"
      ;;
  esac
done

counting_render_consumer() {
  FAIL_TRACE+=("$1")
}

assert_invalid_render_source() {
  local label="$1"
  shift
  local rc=0
  FAIL_TRACE=()
  control_plane_release_with_private_manifest_render_argv \
    23 counting_render_consumer "$@" || rc=$?
  assert_eq "${rc}" "2" "${label} status"
  assert_eq "${#FAIL_TRACE[@]}" "0" "${label} callback count"
}

assert_invalid_render_source "invalid Helm verb" \
  helm install release-fixture chart -n namespace-fixture --reset-then-reuse-values
assert_invalid_render_source "missing namespace prefix" \
  helm upgrade release-fixture chart --namespace namespace-fixture --reset-then-reuse-values
assert_invalid_render_source "duplicate reuse boundary" \
  "${VALID_RENDER_SOURCE[@]}" --reset-then-reuse-values
assert_invalid_render_source "preexisting dry-run" \
  "${VALID_RENDER_SOURCE[@]}" --dry-run=server
assert_invalid_render_source "preexisting bare dry-run" \
  "${VALID_RENDER_SOURCE[@]}" --dry-run
assert_invalid_render_source "preexisting output" \
  "${VALID_RENDER_SOURCE[@]}" --output json
assert_invalid_render_source "preexisting long output assignment" \
  "${VALID_RENDER_SOURCE[@]}" --output=json
assert_invalid_render_source "preexisting short output" \
  "${VALID_RENDER_SOURCE[@]}" -o json
assert_invalid_render_source "preexisting short output assignment" \
  "${VALID_RENDER_SOURCE[@]}" -o=json
assert_invalid_render_source "preexisting attached short output" \
  "${VALID_RENDER_SOURCE[@]}" -ojson
assert_invalid_render_source "hidden Secret render" \
  "${VALID_RENDER_SOURCE[@]}" --hide-secret
assert_invalid_render_source "hidden Secret render assignment" \
  "${VALID_RENDER_SOURCE[@]}" --hide-secret=true
assert_invalid_render_source "preexisting install" \
  "${VALID_RENDER_SOURCE[@]}" --install
assert_invalid_render_source "preexisting install assignment" \
  "${VALID_RENDER_SOURCE[@]}" --install=true
assert_invalid_render_source "preexisting short install" \
  "${VALID_RENDER_SOURCE[@]}" -i
assert_invalid_render_source "preexisting short install assignment" \
  "${VALID_RENDER_SOURCE[@]}" -i=true
assert_invalid_render_source "preexisting debug" \
  "${VALID_RENDER_SOURCE[@]}" --debug
assert_invalid_render_source "preexisting debug assignment" \
  "${VALID_RENDER_SOURCE[@]}" --debug=true

rc=0
control_plane_release_with_private_manifest_render_argv \
  0 counting_render_consumer "${VALID_RENDER_SOURCE[@]}" || rc=$?
assert_eq "${rc}" "2" "invalid live revision status"
rc=0
control_plane_release_with_private_manifest_render_argv \
  23 /bin/true "${VALID_RENDER_SOURCE[@]}" || rc=$?
assert_eq "${rc}" "2" "external render consumer status"

python3 - \
  "${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh" \
  "${REPO_ROOT}/scripts/lib/control_plane_release_render.sh" <<'PY'
from pathlib import Path
import sys

upgrade = Path(sys.argv[1]).read_text(encoding="utf-8")
library = Path(sys.argv[2]).read_text(encoding="utf-8")
main = upgrade[upgrade.index("\nmain() {"):]
source_line = 'source "${REPO_ROOT}/scripts/lib/control_plane_release_render.sh"'
if upgrade.count(source_line) != 1:
    raise SystemExit("upgrade entrypoint must source the render library exactly once")
for forbidden in (
    "control_plane_release_with_private_manifest_render_argv",
    "fugue-release-domain-plan",
    "control_plane_release_run_domain_adapter_transaction",
):
    if forbidden in main:
        raise SystemExit(f"default release path unexpectedly activates {forbidden}")
if main.count("with_frozen_control_plane_helm_upgrade_argv") != 1:
    raise SystemExit("default release path no longer has one real Helm argv consumer")
for forbidden in ("eval ", "printf '%q'"):
    if forbidden in library:
        raise SystemExit(f"private render library contains unsafe construct {forbidden!r}")
phase_order = [
    library.index('"${consumer}" target'),
    library.index('"${consumer}" base'),
    library.index('"${consumer}" repeated-target'),
]
if phase_order != sorted(phase_order):
    raise SystemExit("private render phases no longer bracket the pinned base read")
PY

bash -n \
  "${REPO_ROOT}/scripts/lib/control_plane_release_render.sh" \
  "${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh"
printf '[test_control_plane_release_render] private render argv wrapper remains inactive and deterministic\n'
