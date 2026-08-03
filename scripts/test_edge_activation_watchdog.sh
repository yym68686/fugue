#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT
mkdir -p "${TMP}/bin"

cat >"${TMP}/bin/curl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
output=""
url=""
while (($#)); do
  case "$1" in
    --output) output="$2"; shift 2 ;;
    --config|--data-urlencode|--write-out) shift 2 ;;
    --get) shift ;;
    https://*) url="$1"; shift ;;
    *) shift ;;
  esac
done
case "${url}" in
  */v1/admin/edge/activation)
    cat >"${output}" <<JSON
{"activation":{"schema":"edge-activation/v1","phase":"active-epoch-enforced","route_authority":"active-epoch","generation":5,"release_id":"release-b","plan_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","soak_started_at":"2026-08-01T00:00:00Z","expected_instances":[{"edge_id":"edge-de","edge_group_id":"edge-group-de","slot":"b","instance_uid":"pod-b","release_epoch":"release-b"}]},"instances":[{"edge_id":"edge-de","edge_group_id":"edge-group-de","slot":"b","instance_uid":"pod-b","release_epoch":"release-b","effective_healthy":${MOCK_HEALTHY:-true},"failure_class":"","node":{"draining":false,"tls_status":"ready"}}]}
JSON
    ;;
  */v1/admin/edge/release-evidence)
    cat >"${output}" <<JSON
{"schema":"platform-release-evidence/v1","status":"${MOCK_PLATFORM_STATUS:-passed}","reason":"${MOCK_PLATFORM_REASON:-active cohort and platform requests passed}","release_epoch":"release-b","evidence_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","metrics":{"request_count":12,"hard_failure_count":${MOCK_HARD_FAILURES:-0},"origin_connected_application_5xx_count":${MOCK_APPLICATION_5XX:-4},"platform_error_classes":["origin_connected_application_5xx"]}}
JSON
    ;;
  *) echo "unexpected URL ${url}" >&2; exit 97 ;;
esac
printf 200
MOCK
chmod +x "${TMP}/bin/curl"
export PATH="${TMP}/bin:${PATH}"
export FUGUE_EDGE_ACTIVATION_API_URL=https://api.example.test
export FUGUE_EDGE_ACTIVATION_API_KEY=bootstrap_abcdefghijklmnopqrstuvwxyz
export FUGUE_EDGE_WATCHDOG_EVIDENCE_DIR="${TMP}/due"
export FUGUE_EDGE_WATCHDOG_NOW=2026-08-02T00:00:01Z
export MOCK_HEALTHY=true MOCK_PLATFORM_STATUS=passed MOCK_HARD_FAILURES=0 MOCK_APPLICATION_5XX=4
bash "${ROOT}/scripts/observe_edge_activation_watchdog.sh" | grep -Fx '[edge_activation_watchdog] ok'
python3 - "${TMP}/due/evidence.json" <<'PY'
import json,sys
value=json.load(open(sys.argv[1]))
assert value["schema"]=="edge-activation-watchdog/v1"
assert value["status"]=="passed"
assert value["platform_http"]==200
assert value["platform_evidence_digest"].startswith("sha256:")
assert "responses_http" not in value
PY

cat >"${TMP}/bin/curl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
output=""
while (($#)); do
  case "$1" in
    --output) output="$2"; shift 2 ;;
    --config|--write-out) shift 2 ;;
    https://*) shift ;;
    *) shift ;;
  esac
done
cat >"${output}" <<'JSON'
{"activation":{"schema":"edge-activation/v1","phase":"legacy-authoritative","route_authority":"legacy","generation":19,"expected_instances":[]},"instances":[],"active_epochs":[]}
JSON
printf 200
MOCK
chmod +x "${TMP}/bin/curl"
export FUGUE_EDGE_ACTIVATION_API_KEY=bootstrap_abcdefghijklmnopqrstuvwxyz
export FUGUE_EDGE_WATCHDOG_EVIDENCE_DIR="${TMP}/legacy"
bash "${ROOT}/scripts/observe_edge_activation_watchdog.sh" | grep -Fx '[edge_activation_watchdog] legacy-authoritative'
python3 - "${TMP}/legacy/evidence.json" <<'PY'
import json,sys
value=json.load(open(sys.argv[1]))
assert value["schema"]=="edge-activation-watchdog/v1"
assert value["status"]=="legacy-authoritative"
assert value["activation_generation"]==19
assert value["digest"].startswith("sha256:")
PY

sed -i.bak 's/"active_epochs":\[\]/"active_epochs":[{"edge_group_id":"edge-group-country-us"}]/' "${TMP}/bin/curl"
rm -f "${TMP}/bin/curl.bak"
export FUGUE_EDGE_ACTIVATION_API_KEY=bootstrap_abcdefghijklmnopqrstuvwxyz
export FUGUE_EDGE_WATCHDOG_EVIDENCE_DIR="${TMP}/legacy-inconsistent"
if bash "${ROOT}/scripts/observe_edge_activation_watchdog.sh" >/dev/null 2>&1; then
  echo "inconsistent legacy authority must fail closed" >&2; exit 1
fi

cat >"${TMP}/bin/curl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
output=""
url=""
while (($#)); do
  case "$1" in
    --output) output="$2"; shift 2 ;;
    --config|--data-urlencode|--write-out) shift 2 ;;
    --get) shift ;;
    https://*) url="$1"; shift ;;
    *) shift ;;
  esac
done
case "${url}" in
  */v1/admin/edge/activation)
    cat >"${output}" <<JSON
{"activation":{"schema":"edge-activation/v1","phase":"active-epoch-enforced","route_authority":"active-epoch","generation":5,"release_id":"release-b","plan_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","soak_started_at":"2026-08-01T00:00:00Z","expected_instances":[{"edge_id":"edge-de","edge_group_id":"edge-group-de","slot":"b","instance_uid":"pod-b","release_epoch":"release-b"}]},"instances":[{"edge_id":"edge-de","edge_group_id":"edge-group-de","slot":"b","instance_uid":"pod-b","release_epoch":"release-b","effective_healthy":${MOCK_HEALTHY:-true},"failure_class":"","node":{"draining":false,"tls_status":"ready"}}]}
JSON
    ;;
  */v1/admin/edge/release-evidence)
    cat >"${output}" <<JSON
{"schema":"platform-release-evidence/v1","status":"${MOCK_PLATFORM_STATUS:-passed}","reason":"${MOCK_PLATFORM_REASON:-active cohort and platform requests passed}","release_epoch":"release-b","evidence_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","metrics":{"request_count":12,"hard_failure_count":${MOCK_HARD_FAILURES:-0},"origin_connected_application_5xx_count":${MOCK_APPLICATION_5XX:-4},"platform_error_classes":["origin_connected_application_5xx"]}}
JSON
    ;;
  *) echo "unexpected URL ${url}" >&2; exit 97 ;;
esac
printf 200
MOCK
chmod +x "${TMP}/bin/curl"

# The API has already classified connected application 5xx as non-platform;
# the watchdog consumes the resulting passed evidence without business replay.
export FUGUE_EDGE_ACTIVATION_API_KEY=bootstrap_abcdefghijklmnopqrstuvwxyz
export FUGUE_EDGE_WATCHDOG_EVIDENCE_DIR="${TMP}/not-due"
export FUGUE_EDGE_WATCHDOG_NOW=2026-08-01T12:00:00Z
bash "${ROOT}/scripts/observe_edge_activation_watchdog.sh"
grep -q '"status":"not-due"' "${TMP}/not-due/evidence.json"

export FUGUE_EDGE_ACTIVATION_API_KEY=bootstrap_abcdefghijklmnopqrstuvwxyz
export FUGUE_EDGE_WATCHDOG_EVIDENCE_DIR="${TMP}/unknown"
export FUGUE_EDGE_WATCHDOG_NOW=2026-08-02T00:00:01Z
export MOCK_PLATFORM_STATUS=unknown
if bash "${ROOT}/scripts/observe_edge_activation_watchdog.sh" >/dev/null 2>&1; then
  echo "unknown platform evidence must fail closed" >&2; exit 1
fi

export FUGUE_EDGE_ACTIVATION_API_KEY=bootstrap_abcdefghijklmnopqrstuvwxyz
export FUGUE_EDGE_WATCHDOG_EVIDENCE_DIR="${TMP}/unhealthy"
export MOCK_PLATFORM_STATUS=passed MOCK_HEALTHY=false
if bash "${ROOT}/scripts/observe_edge_activation_watchdog.sh" >/dev/null 2>&1; then
  echo "unhealthy 24-hour instance must fail closed" >&2; exit 1
fi

legacy_response_path='/v1/'"responses"
legacy_synthetic='RESPONSES_'"SYNTHETIC"
if rg -n "${legacy_response_path}|${legacy_synthetic}|PLATFORM_EVIDENCE_(URL|TOKEN|MODEL)" \
  "${ROOT}/scripts/observe_edge_activation_watchdog.sh" \
  "${ROOT}/.github/workflows/observe-edge-activation-watchdog.yml"; then
  echo "watchdog retained business-specific evidence coupling" >&2; exit 1
fi
printf '[test_edge_activation_watchdog] ok\n'
