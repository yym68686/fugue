#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"; trap 'rm -rf "${TMP}"' EXIT
mkdir -p "${TMP}/bin"
cat >"${TMP}/bin/curl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
output=""; config=""
while (($#)); do
  case "$1" in
    --output) output="$2"; shift 2;;
    --config) config="$2"; shift 2;;
    --data-binary|--request|--write-out|--header) shift 2;;
    *) shift;;
  esac
done
if [[ "${config}" == *activation-curl.conf ]]; then
  cat >"${output}" <<JSON
{"activation":{"schema":"edge-activation/v1","phase":"active-epoch-enforced","route_authority":"active-epoch","generation":5,"release_id":"release-b","plan_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","soak_started_at":"2026-08-01T00:00:00Z","expected_instances":[{"edge_id":"edge-de","edge_group_id":"edge-group-de","slot":"b","instance_uid":"pod-b","release_epoch":"release-b"}]},"instances":[{"edge_id":"edge-de","edge_group_id":"edge-group-de","slot":"b","instance_uid":"pod-b","release_epoch":"release-b","effective_healthy":${MOCK_HEALTHY:-true},"failure_class":"","node":{"draining":false,"tls_status":"ready"}}]}
JSON
else
  printf '{"status":"completed","output":[{"type":"message"}]}\n' >"${output}"
fi
printf 200
MOCK
chmod +x "${TMP}/bin/curl"
export PATH="${TMP}/bin:${PATH}"
export FUGUE_EDGE_ACTIVATION_API_URL=https://api.example.test
export FUGUE_EDGE_ACTIVATION_API_KEY=bootstrap_abcdefghijklmnopqrstuvwxyz
export FUGUE_RESPONSES_SYNTHETIC_URL=https://api.example.test/v1/responses
export FUGUE_RESPONSES_SYNTHETIC_TOKEN=synthetic_abcdefghijklmnopqrstuvwxyz
export FUGUE_EDGE_WATCHDOG_EVIDENCE_DIR="${TMP}/due"
export FUGUE_EDGE_WATCHDOG_NOW=2026-08-02T00:00:01Z
export MOCK_HEALTHY=true
bash "${ROOT}/scripts/observe_edge_activation_watchdog.sh" | grep -Fx '[edge_activation_watchdog] ok'
python3 - "${TMP}/due/evidence.json" <<'PY'
import json,sys
value=json.load(open(sys.argv[1]))
assert value["schema"]=="edge-activation-watchdog/v1" and value["status"]=="passed" and value["responses_http"]==200
PY

export FUGUE_EDGE_ACTIVATION_API_KEY=bootstrap_abcdefghijklmnopqrstuvwxyz
export FUGUE_RESPONSES_SYNTHETIC_TOKEN=synthetic_abcdefghijklmnopqrstuvwxyz
export FUGUE_EDGE_WATCHDOG_EVIDENCE_DIR="${TMP}/not-due"
export FUGUE_EDGE_WATCHDOG_NOW=2026-08-01T12:00:00Z
bash "${ROOT}/scripts/observe_edge_activation_watchdog.sh"
grep -q '"status":"not-due"' "${TMP}/not-due/evidence.json"

export FUGUE_EDGE_ACTIVATION_API_KEY=bootstrap_abcdefghijklmnopqrstuvwxyz
export FUGUE_RESPONSES_SYNTHETIC_TOKEN=synthetic_abcdefghijklmnopqrstuvwxyz
export FUGUE_EDGE_WATCHDOG_EVIDENCE_DIR="${TMP}/unhealthy"
export FUGUE_EDGE_WATCHDOG_NOW=2026-08-02T00:00:01Z
export MOCK_HEALTHY=false
if bash "${ROOT}/scripts/observe_edge_activation_watchdog.sh" >/dev/null 2>&1; then
  echo "unhealthy 24-hour instance must fail closed" >&2; exit 1
fi
printf '[test_edge_activation_watchdog] ok\n'
