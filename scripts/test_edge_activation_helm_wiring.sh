#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export FUGUE_UPGRADE_LIB_ONLY=true
# shellcheck source=scripts/upgrade_fugue_control_plane.sh
source "${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh"

test_fail() {
  printf '[test_edge_activation_helm_wiring] ERROR: %s\n' "$*" >&2
  exit 1
}

TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TEMP_DIR}"' EXIT

render_values() {
  local enabled="$1"
  local secret_name="$2"
  local output="$3"
  (
    unset FUGUE_EDGE_ACTIVATION_ENABLED FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_NAME
    if [[ "${enabled}" != __unset__ ]]; then
      FUGUE_EDGE_ACTIVATION_ENABLED="${enabled}"
    fi
    if [[ "${secret_name}" != __unset__ ]]; then
      FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_NAME="${secret_name}"
    fi
    FUGUE_PUBLIC_DATA_PLANE_RELEASE_MODE=preserve
    UPGRADE_OVERRIDE_VALUES_FILE="${output}"
    : >"${UPGRADE_OVERRIDE_VALUES_FILE}"
    append_edge_activation_upgrade_values
    [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_MODE}" == preserve ]]
  )
}

disabled_missing="${TEMP_DIR}/disabled-missing.yaml"
disabled_explicit="${TEMP_DIR}/disabled-explicit.yaml"
enabled="${TEMP_DIR}/enabled.yaml"
render_values __unset__ __unset__ "${disabled_missing}"
render_values false __unset__ "${disabled_explicit}"
cmp -s "${disabled_missing}" "${disabled_explicit}" ||
  test_fail "missing and explicit false activation values differ"

expected_disabled=$'\nedgeActivation:\n  enabled: false\n  signingSecretName: ""'
[[ "$(cat "${disabled_missing}")" == "${expected_disabled}" ]] ||
  test_fail "default-off values are not canonical"

render_values true fugue-fugue-edge-activation-signing-v1 "${enabled}"
expected_enabled=$'\nedgeActivation:\n  enabled: true\n  signingSecretName: "fugue-fugue-edge-activation-signing-v1"'
[[ "$(cat "${enabled}")" == "${expected_enabled}" ]] ||
  test_fail "enabled values are not canonical"

if grep -Eq 'PLAN_SIGNING_KEY|KEY_GENERATION|KEY_ID|--set' "${enabled}"; then
  test_fail "Helm values contain key material or an argv override"
fi

assert_rejected() {
  local enabled_value="$1"
  local secret_value="$2"
  local label="$3"
  if render_values "${enabled_value}" "${secret_value}" "${TEMP_DIR}/rejected.yaml" >/dev/null 2>&1; then
    test_fail "${label} was accepted"
  fi
}

for near_miss in TRUE True 1 yes ' true' 'true '; do
  assert_rejected "${near_miss}" fugue-fugue-edge-activation-signing-v1 "near-miss enabled=${near_miss}"
done
assert_rejected true __unset__ "enabled without secret name"
assert_rejected false fugue-fugue-edge-activation-signing-v1 "disabled with secret name"
for invalid_name in \
  Fugue-Activation \
  fugue_activation \
  .fugue-activation \
  fugue-activation. \
  fugue..activation \
  ' fugue-activation' \
  'fugue-activation '; do
  assert_rejected true "${invalid_name}" "invalid secret name=${invalid_name}"
done
long_label="$(printf 'a%.0s' {1..64})"
assert_rejected true "${long_label}.example" "overlong DNS label"
long_name="$(printf 'a%.0s' {1..63}).$(printf 'b%.0s' {1..63}).$(printf 'c%.0s' {1..63}).$(printf 'd%.0s' {1..63})"
assert_rejected true "${long_name}" "overlong DNS subdomain"

if (
  FUGUE_EDGE_ACTIVATION_ENABLED=true
  FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_NAME=fugue-fugue-edge-activation-signing-v1
  configure_edge_activation_helm_values
  FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_NAME=fugue-fugue-edge-activation-signing-v2
  UPGRADE_OVERRIDE_VALUES_FILE="${TEMP_DIR}/drifted.yaml"
  : >"${UPGRADE_OVERRIDE_VALUES_FILE}"
  append_edge_activation_upgrade_values
) >/dev/null 2>&1; then
  test_fail "Secret name drift after configuration sealing was accepted"
fi

python3 - "${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh" \
  "${REPO_ROOT}/.github/workflows/deploy-control-plane.yml" <<'PY'
from pathlib import Path
import sys

script = Path(sys.argv[1]).read_text(encoding="utf-8")
workflow = Path(sys.argv[2]).read_text(encoding="utf-8")
if script.count("\n  append_edge_activation_upgrade_values\n") != 1:
    raise SystemExit("edge activation values must enter exactly one controlled override path")
if script.count("\nedgeActivation:\n  enabled: ${CONTROL_PLANE_EDGE_ACTIVATION_ENABLED}\n") != 1:
    raise SystemExit("edge activation Helm values block is not unique")
if "--set edgeActivation" in script or "--set-string edgeActivation" in script:
    raise SystemExit("edge activation wiring escaped into Helm argv")
main = script[script.index("\nmain() {\n"):script.index("\n# The production activation is source-only")]
if main.index("configure_edge_activation_helm_values") > main.index("FUGUE_OBSERVABILITY_ENABLED="):
    raise SystemExit("edge activation variables are not parsed before release configuration")
for secret_key in (
    "FUGUE_EDGE_ACTIVATION_PLAN_SIGNING_KEY",
    "FUGUE_EDGE_ACTIVATION_PLAN_SIGNING_KEY_ID",
    "FUGUE_EDGE_ACTIVATION_PLAN_SIGNING_KEY_GENERATION",
):
    if secret_key in workflow:
        raise SystemExit(f"workflow receives forbidden key material: {secret_key}")
if workflow.count("FUGUE_EDGE_ACTIVATION_ENABLED: ${{ vars.FUGUE_EDGE_ACTIVATION_ENABLED || 'false' }}") != 1:
    raise SystemExit("workflow activation enable binding is missing or duplicated")
if workflow.count("FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_NAME: ${{ vars.FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_NAME || '' }}") != 1:
    raise SystemExit("workflow activation Secret name binding is missing or duplicated")
PY

printf '[test_edge_activation_helm_wiring] ok\n'
