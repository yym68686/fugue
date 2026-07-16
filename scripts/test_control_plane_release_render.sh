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
if source.count(b"--no-hooks") != 1:
    raise SystemExit("production frozen Helm argv must contain exactly one bare --no-hooks")
if any(item.startswith(b"--no-hooks=") for item in source):
    raise SystemExit("production frozen Helm argv contains assigned --no-hooks")
if b"--hide-secret" in target:
    raise SystemExit("private ownership render must not drop Secret objects")
expected_base_prefix = [
    b"helm", b"get", b"all", b"release-fixture",
    b"-n", b"namespace-fixture", b"--revision", b"17", b"--template",
]
if base[:9] != expected_base_prefix or len(base) != 10:
    raise SystemExit("base render is not bound to the exact stored Helm revision")
template = base[9]
if b".Release.Manifest" not in template:
    raise SystemExit("base release template omits stored manifest")
for forbidden in (b".Release.Hooks", b".Manifest }}{{ end"):
    if forbidden in template:
        raise SystemExit("no-hooks base release template retained stored hooks")
PY

LEGACY_RENDER_SOURCE=(
  helm upgrade release-fixture '/private/target chart'
  -n namespace-fixture
  --reset-then-reuse-values
  --timeout 10m0s
  -f '/private/override values.yaml'
)
VALID_RENDER_SOURCE=(
  "${LEGACY_RENDER_SOURCE[@]}"
  --no-hooks
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

LEGACY_BASE_ARGV_FILE="${TEMP_DIR}/legacy-base.argv"
capture_legacy_render_argv() {
  local phase="$1"
  shift
  if [[ "${phase}" == "base" ]]; then
    printf '%s\0' "$@" >"${LEGACY_BASE_ARGV_FILE}"
  fi
}
control_plane_release_with_private_manifest_render_argv \
  23 capture_legacy_render_argv "${LEGACY_RENDER_SOURCE[@]}"
python3 - "${LEGACY_BASE_ARGV_FILE}" <<'PY'
import sys

payload = open(sys.argv[1], "rb").read()
if not payload.endswith(b"\0"):
    raise SystemExit("legacy base argv is not NUL terminated")
argv = payload.split(b"\0")[:-1]
if len(argv) != 10 or argv[-2] != b"--template":
    raise SystemExit("legacy base render is not pinned to a Helm template")
template = argv[-1]
for required in (b".Release.Manifest", b".Release.Hooks", b".Manifest"):
    if required not in template:
        raise SystemExit("legacy base release template no longer includes stored hooks")
PY

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
assert_invalid_render_source "duplicate no-hooks" \
  "${VALID_RENDER_SOURCE[@]}" --no-hooks
assert_invalid_render_source "assigned no-hooks" \
  "${LEGACY_RENDER_SOURCE[@]}" --no-hooks=true
assert_invalid_render_source "false no-hooks" \
  "${LEGACY_RENDER_SOURCE[@]}" --no-hooks=false
assert_invalid_render_source "split false no-hooks" \
  "${LEGACY_RENDER_SOURCE[@]}" --no-hooks false

rc=0
control_plane_release_with_private_manifest_render_argv \
  0 counting_render_consumer "${VALID_RENDER_SOURCE[@]}" || rc=$?
assert_eq "${rc}" "2" "invalid live revision status"
for invalid_revision in 2147483647 9223372036854775808 18446744073709551616; do
  rc=0
  control_plane_release_with_private_manifest_render_argv \
    "${invalid_revision}" counting_render_consumer "${VALID_RENDER_SOURCE[@]}" || rc=$?
  assert_eq "${rc}" "2" "overflowing live revision ${invalid_revision} status"
done
rc=0
control_plane_release_with_private_manifest_render_argv \
  23 /bin/true "${VALID_RENDER_SOURCE[@]}" || rc=$?
assert_eq "${rc}" "2" "external render consumer status"

CANONICALIZER_BINARY="${TEMP_DIR}/fugue-release-domain-evidence"
CANONICAL_OWNERSHIP="${REPO_ROOT}/deploy/release-domains/ownership-v1.yaml"
BASE_RAW_FIXTURE="${TEMP_DIR}/base.raw.fixture"
LEGACY_BASE_RAW_FIXTURE="${TEMP_DIR}/legacy-base.raw.fixture"
TARGET_JSON_FIXTURE="${TEMP_DIR}/target.json.fixture"
REPEATED_TARGET_JSON_FIXTURE="${TEMP_DIR}/repeated-target.json.fixture"
DRIFT_TARGET_JSON_FIXTURE="${TEMP_DIR}/drift-target.json.fixture"

go build -o "${CANONICALIZER_BINARY}" "${REPO_ROOT}/cmd/fugue-release-domain-evidence"
chmod 700 "${CANONICALIZER_BINARY}"
python3 - \
  "${BASE_RAW_FIXTURE}" \
  "${LEGACY_BASE_RAW_FIXTURE}" \
  "${TARGET_JSON_FIXTURE}" \
  "${REPEATED_TARGET_JSON_FIXTURE}" \
  "${DRIFT_TARGET_JSON_FIXTURE}" <<'PY'
import json
import os
import sys

base, legacy_base, target, repeated, drift = sys.argv[1:]
base_manifest = """apiVersion: v1
kind: ConfigMap
metadata:
  name: release-fixture-config
  labels:
    app.kubernetes.io/managed-by: Helm
data:
  value: old
"""
hook_manifest = """apiVersion: v1
kind: Secret
metadata:
  name: release-fixture-hook
  annotations:
    helm.sh/hook: pre-upgrade
stringData:
  token: sentinel-private-hook-secret
"""
target_main = """kind: ConfigMap
metadata: {name: release-fixture-config, labels: {app.kubernetes.io/managed-by: Helm}}
apiVersion: v1
data:
  ordered: [first, second]
  value: new
---
apiVersion: v1
kind: Secret
metadata: {name: release-fixture-main-secret}
stringData: {token: sentinel-private-main-secret}
"""
repeated_main = """apiVersion: v1
data:
  value: new
  ordered:
    - first
    - second
metadata:
  labels: {app.kubernetes.io/managed-by: Helm}
  name: release-fixture-config
kind: ConfigMap
---
kind: Secret
apiVersion: v1
stringData:
  token: sentinel-private-main-secret
metadata:
  name: release-fixture-main-secret
"""
drift_main = repeated_main.replace("    - first\n    - second", "    - second\n    - first")
target_hook = """apiVersion: v1
kind: Secret
metadata:
  name: release-fixture-hook
  annotations:
    helm.sh/hook: pre-upgrade
stringData:
  token: sentinel-private-hook-secret
"""

def release(manifest, hook, volatile):
    return {
        "apply_method": "server_side",
        "chart": {},
        "hooks": [{
            "events": ["pre-upgrade"],
            "kind": "Secret",
            "last_run": {},
            "manifest": hook,
            "name": "release-fixture-hook",
            "path": "templates/hook.yaml",
        }],
        "info": {"last_deployed": volatile, "status": "pending-upgrade"},
        "manifest": manifest,
        "name": "release-fixture",
        "namespace": "namespace-fixture",
        "version": 24,
    }

with open(base, "w", encoding="utf-8") as handle:
    handle.write(base_manifest)
with open(legacy_base, "w", encoding="utf-8") as handle:
    handle.write(base_manifest + "\n---\n" + hook_manifest)
with open(target, "w", encoding="utf-8") as handle:
    json.dump(release(target_main, target_hook, "first-volatile-value"), handle, indent=2)
with open(repeated, "w", encoding="utf-8") as handle:
    json.dump(release(repeated_main, target_hook, "second-volatile-value"), handle, separators=(",", ":"), sort_keys=True)
with open(drift, "w", encoding="utf-8") as handle:
    json.dump(release(drift_main, target_hook, "third-volatile-value"), handle, sort_keys=True)
for path in (base, legacy_base, target, repeated, drift):
    os.chmod(path, 0o600)
PY

PRIVATE_RENDER_TRACE=""
PRIVATE_RENDER_CAPTURE_PREFIX=""
PRIVATE_RENDER_REPEAT_FIXTURE="${REPEATED_TARGET_JSON_FIXTURE}"
PRIVATE_RENDER_CONSUMER_CALLS_FILE="${TEMP_DIR}/private-render-consumer.calls"

private_render_fixture_runner() {
  local phase="$1"
  shift
  local -a argv=("$@")
  local argv_count="${#argv[@]}"
  printf '%s\n' "${phase}" >>"${PRIVATE_RENDER_TRACE}"
  case "${phase}" in
    target)
      (( argv_count >= 3 )) || return 77
      [[ "${argv[argv_count - 3]}" == "--dry-run=server" &&
        "${argv[argv_count - 2]}" == "--output" &&
        "${argv[argv_count - 1]}" == "json" ]] || return 77
      /bin/cat "${TARGET_JSON_FIXTURE}"
      ;;
    base)
      [[ "$1" == "helm" && "$2" == "get" && "$3" == "all" && "$4" == "release-fixture" ]] || return 78
      if [[ "${argv[argv_count - 1]}" == '{{ .Release.Manifest }}' ]]; then
        /bin/cat "${BASE_RAW_FIXTURE}"
      else
        [[ "${argv[argv_count - 1]}" == *'.Release.Hooks'* ]] || return 78
        /bin/cat "${LEGACY_BASE_RAW_FIXTURE}"
      fi
      ;;
    repeated-target)
      (( argv_count >= 3 )) || return 79
      [[ "${argv[argv_count - 3]}" == "--dry-run=server" &&
        "${argv[argv_count - 2]}" == "--output" &&
        "${argv[argv_count - 1]}" == "json" ]] || return 79
      /bin/cat "${PRIVATE_RENDER_REPEAT_FIXTURE}"
      ;;
    *)
      return 80
      ;;
  esac
}

private_render_real_canonicalizer() {
  local live_revision="$1"
  local expected_version="$2"
  local release_name="$3"
  local release_namespace="$4"
  local hook_policy="$5"
  local base_raw="$6"
  local target_raw="$7"
  local repeated_target_raw="$8"
  local output_dir="$9"
  local -a target_canonicalizer_args=(
    --ownership "${CANONICAL_OWNERSHIP}"
    --input "${target_raw}"
    --input-format helm-release-json
    --namespace "${release_namespace}"
    --release-name "${release_name}"
    --release-version "${expected_version}"
  )
  local -a repeated_target_canonicalizer_args=(
    --ownership "${CANONICAL_OWNERSHIP}"
    --input "${repeated_target_raw}"
    --input-format helm-release-json
    --namespace "${release_namespace}"
    --release-name "${release_name}"
    --release-version "${expected_version}"
  )
  [[ "${live_revision}" == "23" && "${expected_version}" == "24" ]] || return 90
  case "${hook_policy}" in
    exclude-hooks)
      target_canonicalizer_args+=(--exclude-hooks)
      repeated_target_canonicalizer_args+=(--exclude-hooks)
      ;;
    include-hooks) ;;
    *) return 90 ;;
  esac
  python3 - "${base_raw}" "${target_raw}" "${repeated_target_raw}" <<'PY'
import os
import stat
import sys
for path in sys.argv[1:]:
    if stat.S_IMODE(os.stat(path).st_mode) != 0o600:
        raise SystemExit(f"private raw mode drifted: {path}")
PY
  "${CANONICALIZER_BINARY}" canonicalize-manifest \
    --ownership "${CANONICAL_OWNERSHIP}" \
    --input "${base_raw}" \
    --namespace "${release_namespace}" \
    --output "${output_dir}/base.manifest" || return 101
  target_canonicalizer_args+=(--output "${output_dir}/target.manifest")
  "${CANONICALIZER_BINARY}" canonicalize-manifest \
    "${target_canonicalizer_args[@]}" || return 102
  repeated_target_canonicalizer_args+=(--output "${output_dir}/repeated-target.manifest")
  "${CANONICALIZER_BINARY}" canonicalize-manifest \
    "${repeated_target_canonicalizer_args[@]}" || return 103
}

private_render_capture_consumer() {
  local live_revision="$1"
  local release_name="$2"
  local release_namespace="$3"
  local base_manifest="$4"
  local target_manifest="$5"
  local repeated_target_manifest="$6"
  [[ "${live_revision}|${release_name}|${release_namespace}" == "23|release-fixture|namespace-fixture" ]] || return 93
  cp "${base_manifest}" "${PRIVATE_RENDER_CAPTURE_PREFIX}.base"
  cp "${target_manifest}" "${PRIVATE_RENDER_CAPTURE_PREFIX}.target"
  cp "${repeated_target_manifest}" "${PRIVATE_RENDER_CAPTURE_PREFIX}.repeated"
  chmod 600 \
    "${PRIVATE_RENDER_CAPTURE_PREFIX}.base" \
    "${PRIVATE_RENDER_CAPTURE_PREFIX}.target" \
    "${PRIVATE_RENDER_CAPTURE_PREFIX}.repeated"
  printf 'called\n' >>"${PRIVATE_RENDER_CONSUMER_CALLS_FILE}"
}

cmp -s "${TARGET_JSON_FIXTURE}" "${REPEATED_TARGET_JSON_FIXTURE}" &&
  fail "target Helm JSON fixtures must differ before canonicalization"
for caller_umask in 000 022 077; do
  PRIVATE_RENDER_TRACE="${TEMP_DIR}/private-render-${caller_umask}.trace"
  PRIVATE_RENDER_CAPTURE_PREFIX="${TEMP_DIR}/private-render-${caller_umask}"
  : >"${PRIVATE_RENDER_TRACE}"
  : >"${PRIVATE_RENDER_CONSUMER_CALLS_FILE}"
  (
    umask "${caller_umask}"
    control_plane_release_run_private_canonical_render_set \
      23 private_render_fixture_runner private_render_real_canonicalizer private_render_capture_consumer \
      "${VALID_RENDER_SOURCE[@]}"
  ) >"${TEMP_DIR}/private-render-${caller_umask}.stdout" \
    2>"${TEMP_DIR}/private-render-${caller_umask}.stderr" ||
    {
      command sed 's/^/[private-render-diagnostic] /' \
        "${TEMP_DIR}/private-render-${caller_umask}.stderr" >&2
      fail "private canonical render failed under umask ${caller_umask}"
    }
  [[ ! -s "${TEMP_DIR}/private-render-${caller_umask}.stdout" &&
    ! -s "${TEMP_DIR}/private-render-${caller_umask}.stderr" ]] ||
    fail "private canonical render emitted output under umask ${caller_umask}"
  assert_eq "$(tr '\n' ' ' <"${PRIVATE_RENDER_TRACE}" | sed 's/ $//')" \
    "target base repeated-target" "private canonical phase order under umask ${caller_umask}"
  cmp -s "${PRIVATE_RENDER_CAPTURE_PREFIX}.target" "${PRIVATE_RENDER_CAPTURE_PREFIX}.repeated" ||
    fail "canonical target drifted under umask ${caller_umask}"
  cmp -s "${PRIVATE_RENDER_CAPTURE_PREFIX}.base" "${PRIVATE_RENDER_CAPTURE_PREFIX}.target" &&
    fail "base and changed target unexpectedly match under umask ${caller_umask}"
  grep -q 'sentinel-private-main-secret' "${PRIVATE_RENDER_CAPTURE_PREFIX}.target" ||
    fail "canonical target dropped a non-hook Secret under umask ${caller_umask}"
  grep -q 'release-fixture-hook\|sentinel-private-hook-secret' \
    "${PRIVATE_RENDER_CAPTURE_PREFIX}.target" &&
    fail "no-hooks canonical target retained a Helm release hook under umask ${caller_umask}"
  grep -q 'release-fixture-hook\|sentinel-private-hook-secret' \
    "${PRIVATE_RENDER_CAPTURE_PREFIX}.base" &&
    fail "no-hooks canonical base retained a Helm release hook under umask ${caller_umask}"
  assert_eq "$(wc -l <"${PRIVATE_RENDER_CONSUMER_CALLS_FILE}" | tr -d ' ')" "1" \
    "private canonical consumer calls under umask ${caller_umask}"
  python3 - \
    "${PRIVATE_RENDER_CAPTURE_PREFIX}.base" \
    "${PRIVATE_RENDER_CAPTURE_PREFIX}.target" \
    "${PRIVATE_RENDER_CAPTURE_PREFIX}.repeated" <<'PY'
import os
import stat
import sys
for path in sys.argv[1:]:
    mode = stat.S_IMODE(os.stat(path).st_mode)
    if mode != 0o600:
        raise SystemExit(f"canonical capture mode is {mode:o}, want 600: {path}")
PY
done

PRIVATE_RENDER_TRACE="${TEMP_DIR}/private-render-legacy.trace"
PRIVATE_RENDER_CAPTURE_PREFIX="${TEMP_DIR}/private-render-legacy"
: >"${PRIVATE_RENDER_TRACE}"
: >"${PRIVATE_RENDER_CONSUMER_CALLS_FILE}"
control_plane_release_run_private_canonical_render_set \
  23 private_render_fixture_runner private_render_real_canonicalizer private_render_capture_consumer \
  "${LEGACY_RENDER_SOURCE[@]}" \
  >"${TEMP_DIR}/private-render-legacy.stdout" \
  2>"${TEMP_DIR}/private-render-legacy.stderr" ||
  {
    command sed 's/^/[private-render-legacy-diagnostic] /' \
      "${TEMP_DIR}/private-render-legacy.stderr" >&2
    fail "legacy private canonical render failed"
  }
[[ ! -s "${TEMP_DIR}/private-render-legacy.stdout" &&
  ! -s "${TEMP_DIR}/private-render-legacy.stderr" ]] ||
  fail "legacy private canonical render emitted output"
for legacy_manifest in base target repeated; do
  grep -q 'release-fixture-hook' "${PRIVATE_RENDER_CAPTURE_PREFIX}.${legacy_manifest}" ||
    fail "legacy canonical ${legacy_manifest} dropped Helm release hooks"
  grep -q 'sentinel-private-hook-secret' "${PRIVATE_RENDER_CAPTURE_PREFIX}.${legacy_manifest}" ||
    fail "legacy canonical ${legacy_manifest} dropped hook data"
done
cmp -s "${PRIVATE_RENDER_CAPTURE_PREFIX}.target" "${PRIVATE_RENDER_CAPTURE_PREFIX}.repeated" ||
  fail "legacy canonical target drifted"

PRIVATE_RENDER_CONCURRENT_PIDS=()
for concurrent_index in 1 2 3 4; do
  (
    PRIVATE_RENDER_TRACE="${TEMP_DIR}/private-render-concurrent-${concurrent_index}.trace"
    PRIVATE_RENDER_CAPTURE_PREFIX="${TEMP_DIR}/private-render-concurrent-${concurrent_index}"
    PRIVATE_RENDER_CONSUMER_CALLS_FILE="${TEMP_DIR}/private-render-concurrent-${concurrent_index}.calls"
    : >"${PRIVATE_RENDER_TRACE}"
    : >"${PRIVATE_RENDER_CONSUMER_CALLS_FILE}"
    control_plane_release_run_private_canonical_render_set \
      23 private_render_fixture_runner private_render_real_canonicalizer private_render_capture_consumer \
      "${VALID_RENDER_SOURCE[@]}"
  ) >"${TEMP_DIR}/private-render-concurrent-${concurrent_index}.stdout" \
    2>"${TEMP_DIR}/private-render-concurrent-${concurrent_index}.stderr" &
  PRIVATE_RENDER_CONCURRENT_PIDS+=("$!")
done
for concurrent_index in 0 1 2 3; do
  wait "${PRIVATE_RENDER_CONCURRENT_PIDS[concurrent_index]}" ||
    fail "concurrent private canonical render $((concurrent_index + 1)) failed"
  [[ ! -s "${TEMP_DIR}/private-render-concurrent-$((concurrent_index + 1)).stdout" &&
    ! -s "${TEMP_DIR}/private-render-concurrent-$((concurrent_index + 1)).stderr" ]] ||
    fail "concurrent private canonical render $((concurrent_index + 1)) emitted output"
  cmp -s \
    "${TEMP_DIR}/private-render-concurrent-$((concurrent_index + 1)).target" \
    "${TEMP_DIR}/private-render-concurrent-$((concurrent_index + 1)).repeated" ||
    fail "concurrent private canonical render $((concurrent_index + 1)) drifted"
done

PRIVATE_RENDER_FAIL_PHASE=""
private_render_failing_runner() {
  local phase="$1"
  if [[ "${phase}" == "${PRIVATE_RENDER_FAIL_PHASE}" ]]; then
    case "${phase}" in
      target) return 81 ;;
      base) return 82 ;;
      repeated-target) return 83 ;;
    esac
  fi
  private_render_fixture_runner "$@"
}

for failure in target base repeated-target; do
  PRIVATE_RENDER_FAIL_PHASE="${failure}"
  PRIVATE_RENDER_TRACE="${TEMP_DIR}/private-render-fail-${failure}.trace"
  PRIVATE_RENDER_CAPTURE_PREFIX="${TEMP_DIR}/private-render-fail-${failure}"
  : >"${PRIVATE_RENDER_TRACE}"
  : >"${PRIVATE_RENDER_CONSUMER_CALLS_FILE}"
  rc=0
  control_plane_release_run_private_canonical_render_set \
    23 private_render_failing_runner private_render_real_canonicalizer private_render_capture_consumer \
    "${VALID_RENDER_SOURCE[@]}" \
    >"${TEMP_DIR}/private-render-fail-${failure}.stdout" \
    2>"${TEMP_DIR}/private-render-fail-${failure}.stderr" || rc=$?
  case "${failure}" in
    target) assert_eq "${rc}" "81" "private target failure status" ;;
    base) assert_eq "${rc}" "82" "private base failure status" ;;
    repeated-target) assert_eq "${rc}" "83" "private repeated-target failure status" ;;
  esac
  [[ ! -s "${TEMP_DIR}/private-render-fail-${failure}.stdout" ]] ||
    fail "private ${failure} failure leaked stdout"
  grep -q 'sentinel-private-render-secret' "${TEMP_DIR}/private-render-fail-${failure}.stderr" &&
    fail "private ${failure} failure leaked Secret"
  [[ ! -s "${PRIVATE_RENDER_CONSUMER_CALLS_FILE}" ]] ||
    fail "private ${failure} failure reached consumer"
done

private_render_failing_canonicalizer() { return 91; }
PRIVATE_RENDER_TRACE="${TEMP_DIR}/private-render-canonicalizer-fail.trace"
PRIVATE_RENDER_CAPTURE_PREFIX="${TEMP_DIR}/private-render-canonicalizer-fail"
: >"${PRIVATE_RENDER_TRACE}"
: >"${PRIVATE_RENDER_CONSUMER_CALLS_FILE}"
rc=0
control_plane_release_run_private_canonical_render_set \
  23 private_render_fixture_runner private_render_failing_canonicalizer private_render_capture_consumer \
  "${VALID_RENDER_SOURCE[@]}" >/dev/null 2>"${TEMP_DIR}/private-render-canonicalizer-fail.stderr" || rc=$?
assert_eq "${rc}" "91" "private canonicalizer failure status"
[[ ! -s "${PRIVATE_RENDER_CONSUMER_CALLS_FILE}" ]] || fail "canonicalizer failure reached consumer"

PRIVATE_RENDER_REPEAT_FIXTURE="${DRIFT_TARGET_JSON_FIXTURE}"
PRIVATE_RENDER_TRACE="${TEMP_DIR}/private-render-drift.trace"
PRIVATE_RENDER_CAPTURE_PREFIX="${TEMP_DIR}/private-render-drift"
: >"${PRIVATE_RENDER_TRACE}"
: >"${PRIVATE_RENDER_CONSUMER_CALLS_FILE}"
rc=0
control_plane_release_run_private_canonical_render_set \
  23 private_render_fixture_runner private_render_real_canonicalizer private_render_capture_consumer \
  "${VALID_RENDER_SOURCE[@]}" >/dev/null 2>"${TEMP_DIR}/private-render-drift.stderr" || rc=$?
assert_eq "${rc}" "74" "private canonical drift status"
[[ ! -s "${PRIVATE_RENDER_CONSUMER_CALLS_FILE}" ]] || fail "canonical drift reached consumer"
PRIVATE_RENDER_REPEAT_FIXTURE="${REPEATED_TARGET_JSON_FIXTURE}"

private_render_failing_consumer() { return 92; }
PRIVATE_RENDER_TRACE="${TEMP_DIR}/private-render-consumer-fail.trace"
: >"${PRIVATE_RENDER_TRACE}"
rc=0
control_plane_release_run_private_canonical_render_set \
  23 private_render_fixture_runner private_render_real_canonicalizer private_render_failing_consumer \
  "${VALID_RENDER_SOURCE[@]}" >/dev/null 2>"${TEMP_DIR}/private-render-consumer-fail.stderr" || rc=$?
assert_eq "${rc}" "92" "private canonical consumer failure status"

PRIVATE_RENDER_ADVERSARIAL_TMP="${TEMP_DIR}/private-render-adversarial-tmp"
PRIVATE_RENDER_ADVERSARIAL_VICTIM="${TEMP_DIR}/private-render-adversarial-victim"
mkdir -m 700 "${PRIVATE_RENDER_ADVERSARIAL_TMP}" "${PRIVATE_RENDER_ADVERSARIAL_VICTIM}"
printf 'must-survive\n' >"${PRIVATE_RENDER_ADVERSARIAL_VICTIM}/marker"
private_render_tampering_runner() {
  CONTROL_PLANE_RELEASE_PRIVATE_RENDER_STAGING_DIR="${PRIVATE_RENDER_ADVERSARIAL_VICTIM}"
  trap - EXIT
  printf 'sentinel-private-render-secret\n'
  return 81
}
rc=0
(
  TMPDIR="${PRIVATE_RENDER_ADVERSARIAL_TMP}"
  control_plane_release_run_private_canonical_render_set \
    23 private_render_tampering_runner private_render_real_canonicalizer private_render_capture_consumer \
    "${VALID_RENDER_SOURCE[@]}"
) >"${TEMP_DIR}/private-render-adversarial.stdout" \
  2>"${TEMP_DIR}/private-render-adversarial.stderr" || rc=$?
assert_eq "${rc}" "81" "adversarial callback status"
[[ -f "${PRIVATE_RENDER_ADVERSARIAL_VICTIM}/marker" ]] || fail "callback redirected cleanup to victim"
find "${PRIVATE_RENDER_ADVERSARIAL_TMP}" -mindepth 1 -maxdepth 1 -name 'fugue-release-render.*' -print -quit |
  grep -q . && fail "adversarial callback left private staging"
grep -q 'sentinel-private-render-secret' "${TEMP_DIR}/private-render-adversarial.stderr" &&
  fail "adversarial callback leaked Secret"

PRIVATE_RENDER_OVERSIZE_TMP="${TEMP_DIR}/private-render-oversize-tmp"
mkdir -m 700 "${PRIVATE_RENDER_OVERSIZE_TMP}"
private_render_oversize_runner() {
  local phase="$1"
  if [[ "${phase}" == "target" ]]; then
    dd if=/dev/zero bs=1048576 count=20 2>/dev/null
    return
  fi
  private_render_fixture_runner "$@"
}
: >"${PRIVATE_RENDER_CONSUMER_CALLS_FILE}"
rc=0
(
  TMPDIR="${PRIVATE_RENDER_OVERSIZE_TMP}"
  control_plane_release_run_private_canonical_render_set \
    23 private_render_oversize_runner private_render_real_canonicalizer private_render_capture_consumer \
    "${VALID_RENDER_SOURCE[@]}"
) >"${TEMP_DIR}/private-render-oversize.stdout" \
  2>"${TEMP_DIR}/private-render-oversize.stderr" || rc=$?
[[ "${rc}" != "0" ]] || fail "oversize private render unexpectedly succeeded"
[[ ! -s "${PRIVATE_RENDER_CONSUMER_CALLS_FILE}" ]] || fail "oversize private render reached consumer"
find "${PRIVATE_RENDER_OVERSIZE_TMP}" -mindepth 1 -maxdepth 1 -name 'fugue-release-render.*' -print -quit |
  grep -q . && fail "oversize private render left private staging"

PRIVATE_RENDER_DESCENDANT_TMP="${TEMP_DIR}/private-render-descendant-tmp"
PRIVATE_RENDER_DESCENDANT_PID_FILE="${TEMP_DIR}/private-render-descendant.pid"
mkdir -m 700 "${PRIVATE_RENDER_DESCENDANT_TMP}"
private_render_descendant_runner() {
  local phase="$1"
  if [[ "${phase}" == "target" ]]; then
    sleep 30 &
    printf '%s\n' "$!" >"${PRIVATE_RENDER_DESCENDANT_PID_FILE}"
    /bin/cat "${TARGET_JSON_FIXTURE}"
    return 0
  fi
  private_render_fixture_runner "$@"
}
: >"${PRIVATE_RENDER_CONSUMER_CALLS_FILE}"
rc=0
(
  TMPDIR="${PRIVATE_RENDER_DESCENDANT_TMP}"
  control_plane_release_run_private_canonical_render_set \
    23 private_render_descendant_runner private_render_real_canonicalizer private_render_capture_consumer \
    "${VALID_RENDER_SOURCE[@]}"
) >"${TEMP_DIR}/private-render-descendant.stdout" \
  2>"${TEMP_DIR}/private-render-descendant.stderr" || rc=$?
assert_eq "${rc}" "70" "background callback descendant status"
[[ -s "${PRIVATE_RENDER_DESCENDANT_PID_FILE}" ]] || fail "background callback descendant PID was not recorded"
PRIVATE_RENDER_DESCENDANT_PID="$(tr -d '[:space:]' <"${PRIVATE_RENDER_DESCENDANT_PID_FILE}")"
kill -0 "${PRIVATE_RENDER_DESCENDANT_PID}" >/dev/null 2>&1 && fail "background callback descendant survived"
[[ ! -s "${PRIVATE_RENDER_CONSUMER_CALLS_FILE}" ]] || fail "background callback descendant reached consumer"
find "${PRIVATE_RENDER_DESCENDANT_TMP}" -mindepth 1 -maxdepth 1 -name 'fugue-release-render.*' -print -quit |
  grep -q . && fail "background callback descendant left private staging"
grep -q 'sentinel-private-render-secret' "${TEMP_DIR}/private-render-descendant.stderr" &&
  fail "background callback descendant leaked Secret"

private_render_ps_failure_callback() { printf 'bounded-output\n'; }
PRIVATE_RENDER_PS_FAILURE_PREFIX="${TEMP_DIR}/private-render-ps-failure"
rc=0
(
  hash -p /usr/bin/false ps
  CONTROL_PLANE_RELEASE_PRIVATE_RENDER_CALLER_PID="$$"
  CONTROL_PLANE_RELEASE_PRIVATE_RENDER_ACTIVE_PID=""
  control_plane_release_run_private_render_callback \
    "${PRIVATE_RENDER_PS_FAILURE_PREFIX}.stdout" \
    "${PRIVATE_RENDER_PS_FAILURE_PREFIX}.stderr" \
    private_render_ps_failure_callback
) || rc=$?
assert_eq "${rc}" "2" "process inventory failure status"
grep -q 'bounded-output' "${PRIVATE_RENDER_PS_FAILURE_PREFIX}.stdout" ||
  fail "process inventory failure lost bounded callback output"

for signal_name in HUP TERM; do
  case "${signal_name}" in
    HUP) signal_status=129 ;;
    INT) signal_status=130 ;;
    TERM) signal_status=143 ;;
  esac
  for signal_iteration in 1 2 3; do
  PRIVATE_RENDER_SIGNAL_TMP="${TEMP_DIR}/private-render-signal-tmp-${signal_name}-${signal_iteration}"
  PRIVATE_RENDER_SIGNAL_PID_FILE="${TEMP_DIR}/private-render-signal-callback-${signal_name}-${signal_iteration}.pid"
  mkdir -m 700 "${PRIVATE_RENDER_SIGNAL_TMP}"
  TMPDIR="${PRIVATE_RENDER_SIGNAL_TMP}" \
    PRIVATE_RENDER_SIGNAL_PID_FILE="${PRIVATE_RENDER_SIGNAL_PID_FILE}" \
    /bin/bash -c '
  source "$1"
  private_render_top_level_slow_runner() {
    /bin/sh -c "printf \"%s\\n\" \"\$PPID\"" >"${PRIVATE_RENDER_SIGNAL_PID_FILE}"
    printf "sentinel-private-render-secret\n"
    sleep 30
  }
  private_render_top_level_noop() { return 0; }
  control_plane_release_run_private_canonical_render_set \
    23 private_render_top_level_slow_runner private_render_top_level_noop private_render_top_level_noop \
    helm upgrade release-fixture chart -n namespace-fixture --reset-then-reuse-values
  ' _ "${REPO_ROOT}/scripts/lib/control_plane_release_render.sh" \
    >"${TEMP_DIR}/private-render-signal-${signal_name}-${signal_iteration}.stdout" \
    2>"${TEMP_DIR}/private-render-signal-${signal_name}-${signal_iteration}.stderr" &
  PRIVATE_RENDER_SIGNAL_CALLER_PID=$!
  for _ in $(seq 1 100); do
    [[ -s "${PRIVATE_RENDER_SIGNAL_PID_FILE}" ]] && break
    sleep 0.05
  done
  [[ -s "${PRIVATE_RENDER_SIGNAL_PID_FILE}" ]] || fail "${signal_name} callback ${signal_iteration} did not start"
  PRIVATE_RENDER_SIGNAL_CALLBACK_PID="$(tr -d '[:space:]' <"${PRIVATE_RENDER_SIGNAL_PID_FILE}")"
  kill -"${signal_name}" "${PRIVATE_RENDER_SIGNAL_CALLER_PID}"
  rc=0
  wait "${PRIVATE_RENDER_SIGNAL_CALLER_PID}" 2>/dev/null || rc=$?
  assert_eq "${rc}" "${signal_status}" "direct caller ${signal_name} status iteration ${signal_iteration}"
  for _ in $(seq 1 100); do
    if ! kill -0 "${PRIVATE_RENDER_SIGNAL_CALLBACK_PID}" >/dev/null 2>&1 &&
      ! find "${PRIVATE_RENDER_SIGNAL_TMP}" -mindepth 1 -maxdepth 1 -name 'fugue-release-render.*' -print -quit | grep -q .; then
      break
    fi
    sleep 0.05
  done
  kill -0 "${PRIVATE_RENDER_SIGNAL_CALLBACK_PID}" >/dev/null 2>&1 &&
    fail "direct caller ${signal_name} iteration ${signal_iteration} left callback alive"
  find "${PRIVATE_RENDER_SIGNAL_TMP}" -mindepth 1 -maxdepth 1 -name 'fugue-release-render.*' -print -quit |
    grep -q . && fail "direct caller ${signal_name} iteration ${signal_iteration} left private staging"
  grep -q 'sentinel-private-render-secret' "${TEMP_DIR}/private-render-signal-${signal_name}-${signal_iteration}.stderr" &&
    fail "direct caller ${signal_name} iteration ${signal_iteration} leaked Secret"
  done
done

python3 - \
  "${REPO_ROOT}/scripts/lib/control_plane_release_render.sh" \
  "${TEMP_DIR}" <<'PY'
import glob
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

library, parent = sys.argv[1:]
script = r'''
source "$1"
private_render_top_level_slow_runner() {
  /bin/sh -c 'printf "%s\n" "$PPID"' >"${PRIVATE_RENDER_SIGNAL_PID_FILE}"
  printf 'sentinel-private-render-secret\n'
  sleep 30
}
private_render_top_level_noop() { return 0; }
control_plane_release_run_private_canonical_render_set \
  23 private_render_top_level_slow_runner private_render_top_level_noop private_render_top_level_noop \
  helm upgrade release-fixture chart -n namespace-fixture --reset-then-reuse-values
'''

for iteration in range(1, 4):
    root = os.path.join(parent, f"private-render-direct-int-{iteration}")
    tmp = os.path.join(root, "tmp")
    pid_file = os.path.join(root, "callback.pid")
    stdout_file = os.path.join(root, "stdout")
    stderr_file = os.path.join(root, "stderr")
    os.makedirs(tmp, mode=0o700)
    env = os.environ.copy()
    env.update(TMPDIR=tmp, PRIVATE_RENDER_SIGNAL_PID_FILE=pid_file)
    callback_pid = None
    with open(stdout_file, "wb") as stdout, open(stderr_file, "wb") as stderr:
        process = subprocess.Popen(
            ["/bin/bash", "-c", script, "_", library],
            env=env,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
        try:
            for _ in range(200):
                if os.path.isfile(pid_file) and os.path.getsize(pid_file):
                    callback_pid = int(Path(pid_file).read_text(encoding="utf-8").strip())
                    break
                if process.poll() is not None:
                    raise SystemExit(f"direct INT caller exited before callback: {process.returncode}")
                time.sleep(0.025)
            if callback_pid is None:
                raise SystemExit("direct INT callback did not start")
            os.kill(process.pid, signal.SIGINT)
            if process.wait(timeout=5) != 130:
                raise SystemExit(f"direct INT caller status is {process.returncode}, want 130")
            for _ in range(200):
                try:
                    os.kill(callback_pid, 0)
                    callback_alive = True
                except ProcessLookupError:
                    callback_alive = False
                staging = glob.glob(os.path.join(tmp, "fugue-release-render.*"))
                if not callback_alive and not staging:
                    break
                time.sleep(0.025)
            if callback_alive:
                raise SystemExit("direct INT left callback alive")
            if staging:
                raise SystemExit(f"direct INT left private staging: {staging}")
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            if callback_pid is not None:
                try:
                    os.killpg(os.getpgid(callback_pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
    if Path(stdout_file).read_bytes():
        raise SystemExit("direct INT leaked stdout")
    if b"sentinel-private-render-secret" in Path(stderr_file).read_bytes():
        raise SystemExit("direct INT leaked Secret")
    shutil.rmtree(root)
PY

python3 - \
  "${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh" \
  "${REPO_ROOT}/scripts/lib/control_plane_release_render.sh" \
  "${REPO_ROOT}/scripts/lib/control_plane_release_domain_production.sh" <<'PY'
from pathlib import Path
import sys

upgrade = Path(sys.argv[1]).read_text(encoding="utf-8")
library = Path(sys.argv[2]).read_text(encoding="utf-8")
production = Path(sys.argv[3]).read_text(encoding="utf-8")
main = upgrade[
    upgrade.index("\nmain() {") : upgrade.index(
        "\n# The production activation is source-only"
    )
]
source_line = 'source "${REPO_ROOT}/scripts/lib/control_plane_release_render.sh"'
if upgrade.count(source_line) != 1:
    raise SystemExit("upgrade entrypoint must source the render library exactly once")
for forbidden in (
    "control_plane_release_with_private_manifest_render_argv",
    "control_plane_release_run_private_canonical_render_set",
    "fugue-release-domain-plan",
    "control_plane_release_run_domain_adapter_transaction",
    "with_frozen_control_plane_helm_upgrade_argv",
):
    if forbidden in main:
        raise SystemExit(f"main bypasses the atomic domain activation with {forbidden}")
if main.count("run_control_plane_atomic_domain_gate") != 1:
    raise SystemExit("main must enter the atomic domain activation exactly once")
frozen_builder = production[
    production.index("\ncontrol_plane_release_domain_render_and_authorize() {") :
    production.index("\ncontrol_plane_release_domain_read_exact_result() {")
]
if frozen_builder.count("with_frozen_control_plane_helm_upgrade_argv") != 1:
    raise SystemExit("production domain activation must freeze one Helm argv")
sealed_consumer = production[
    production.index("\ncontrol_plane_release_domain_execute_sealed_helm_upgrade() {") :
    production.index("\ncontrol_plane_release_domain_capture_live_canonical_manifest() {")
]
if sealed_consumer.count('"single-domain Helm upgrade" "${sealed_argv[@]}"') != 1:
    raise SystemExit("production domain activation must execute one verified sealed Helm argv")
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
printf '[test_control_plane_release_render] private render argv remains deterministic and production execution is sealed\n'
