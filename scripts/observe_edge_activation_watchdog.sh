#!/usr/bin/env bash

set -euo pipefail

evidence_dir="${FUGUE_EDGE_WATCHDOG_EVIDENCE_DIR:-${RUNNER_TEMP:-/tmp}/fugue-edge-activation-watchdog}"
install -d -m 700 "${evidence_dir}"
config="${evidence_dir}/curl.conf"
activation_config="${evidence_dir}/activation-curl.conf"
responses_config="${evidence_dir}/responses-curl.conf"
activation_json="${evidence_dir}/activation.json"
response_json="${evidence_dir}/responses.json"
evidence_json="${evidence_dir}/evidence.json"
watchdog_cleanup() {
  rm -f -- "${activation_json:-}" "${response_json:-}" "${body:-}" "${config:-}" "${activation_config:-}" "${responses_config:-}"
}
trap watchdog_cleanup EXIT

: "${FUGUE_EDGE_ACTIVATION_API_URL:?FUGUE_EDGE_ACTIVATION_API_URL is required}"
: "${FUGUE_EDGE_ACTIVATION_API_KEY:?FUGUE_EDGE_ACTIVATION_API_KEY is required}"
: "${FUGUE_RESPONSES_SYNTHETIC_URL:?FUGUE_RESPONSES_SYNTHETIC_URL is required}"
: "${FUGUE_RESPONSES_SYNTHETIC_TOKEN:?FUGUE_RESPONSES_SYNTHETIC_TOKEN is required}"
[[ "${FUGUE_EDGE_ACTIVATION_API_URL}" =~ ^https://[^/]+(/.*)?$ ]]
[[ "${FUGUE_RESPONSES_SYNTHETIC_URL}" =~ ^https://[^/]+/v1/responses$ ]]
[[ "${FUGUE_EDGE_ACTIVATION_API_KEY}" =~ ^[A-Za-z0-9._-]+$ && ${#FUGUE_EDGE_ACTIVATION_API_KEY} -ge 20 && ${#FUGUE_EDGE_ACTIVATION_API_KEY} -le 512 ]]
[[ "${FUGUE_RESPONSES_SYNTHETIC_TOKEN}" =~ ^[A-Za-z0-9._-]+$ && ${#FUGUE_RESPONSES_SYNTHETIC_TOKEN} -ge 20 && ${#FUGUE_RESPONSES_SYNTHETIC_TOKEN} -le 512 ]]

(
  umask 077
  printf 'silent\nshow-error\nconnect-timeout = 5\nmax-time = 90\nproto = "=https"\nheader = "Content-Type: application/json"\n' >"${config}"
  cat "${config}" >"${activation_config}"
  printf 'header = "Authorization: Bearer %s"\n' "${FUGUE_EDGE_ACTIVATION_API_KEY}" >>"${activation_config}"
  cat "${config}" >"${responses_config}"
  printf 'header = "Authorization: Bearer %s"\n' "${FUGUE_RESPONSES_SYNTHETIC_TOKEN}" >>"${responses_config}"
)
chmod 600 "${config}" "${activation_config}" "${responses_config}"
unset FUGUE_EDGE_ACTIVATION_API_KEY FUGUE_RESPONSES_SYNTHETIC_TOKEN
for file in "${activation_json}" "${response_json}" "${evidence_json}"; do
  install -m 600 /dev/null "${file}"
done

activation_status="$(curl --config "${activation_config}" \
  --output "${activation_json}" --write-out '%{http_code}' "${FUGUE_EDGE_ACTIVATION_API_URL%/}/v1/admin/edge/activation")"
[[ "${activation_status}" == "200" ]]

eligibility="$(FUGUE_EDGE_WATCHDOG_NOW="${FUGUE_EDGE_WATCHDOG_NOW:-}" python3 - "${activation_json}" <<'PY'
import datetime,json,os,sys
with open(sys.argv[1],encoding="utf-8") as h: value=json.load(h)
activation=value.get("activation") or {}
if activation.get("schema")!="edge-activation/v1" or activation.get("phase")!="active-epoch-enforced" or activation.get("route_authority")!="active-epoch":
    raise SystemExit("edge activation is not in the enforced terminal phase")
raw=activation.get("soak_started_at")
if not isinstance(raw,str): raise SystemExit("edge activation has no soak timestamp")
started=datetime.datetime.fromisoformat(raw.replace("Z","+00:00"))
now_raw=os.environ.get("FUGUE_EDGE_WATCHDOG_NOW")
now=datetime.datetime.fromisoformat(now_raw.replace("Z","+00:00")) if now_raw else datetime.datetime.now(datetime.timezone.utc)
age=(now-started).total_seconds()
if age < 24*3600:
    print("not-due")
    raise SystemExit(0)
expected=activation.get("expected_instances") or []
instances=value.get("instances") or []
by_key={(v.get("edge_id"),v.get("edge_group_id"),v.get("slot"),v.get("instance_uid"),v.get("release_epoch")):v for v in instances}
for item in expected:
    key=tuple(item.get(name) for name in ("edge_id","edge_group_id","slot","instance_uid","release_epoch"))
    observed=by_key.get(key)
    node=(observed or {}).get("node") or {}
    if not observed or not observed.get("effective_healthy") or observed.get("failure_class") or node.get("draining") or node.get("tls_status")!="ready":
        raise SystemExit("an expected active edge instance is not healthy after 24 hours")
print("due")
PY
)"
if [[ "${eligibility}" == "not-due" ]]; then
  printf '{"schema":"edge-activation-watchdog/v1","status":"not-due"}\n' >"${evidence_json}"
  exit 0
fi
[[ "${eligibility}" == "due" ]]

body="${evidence_dir}/responses-body.json"
SYNTHETIC_MODEL="${FUGUE_RESPONSES_SYNTHETIC_MODEL:-gpt-5.6-sol}" python3 - "${body}" <<'PY'
import json,os,sys
with open(sys.argv[1],"w",encoding="utf-8") as h:
    json.dump({"model":os.environ["SYNTHETIC_MODEL"],"input":"Reply with exactly ok.","stream":False,"max_output_tokens":8},h,separators=(",",":"),sort_keys=True); h.write("\n")
PY
chmod 600 "${body}"
response_status="$(curl --config "${responses_config}" --header 'Connection: close' \
  --request POST --data-binary "@${body}" --output "${response_json}" --write-out '%{http_code}' "${FUGUE_RESPONSES_SYNTHETIC_URL}")"
[[ "${response_status}" == "200" ]]

ACTIVATION_STATUS="${activation_status}" RESPONSE_STATUS="${response_status}" python3 - "${activation_json}" "${response_json}" "${evidence_json}" <<'PY'
import hashlib,json,os,sys
with open(sys.argv[1],encoding="utf-8") as h: activation=json.load(h)["activation"]
with open(sys.argv[2],encoding="utf-8") as h: response=json.load(h)
if not isinstance(response,dict) or response.get("status") not in ("completed",None) or not (response.get("output") or response.get("output_text")):
    raise SystemExit("24-hour Responses API synthetic did not complete")
material={"schema":"edge-activation-watchdog/v1","status":"passed","activation_generation":activation["generation"],"release_id":activation["release_id"],"plan_digest":activation["plan_digest"],"activation_http":int(os.environ["ACTIVATION_STATUS"]),"responses_http":int(os.environ["RESPONSE_STATUS"])}
material["digest"]="sha256:"+hashlib.sha256(json.dumps(material,separators=(",",":"),sort_keys=True).encode()).hexdigest()
with open(sys.argv[3],"w",encoding="utf-8") as h: json.dump(material,h,separators=(",",":"),sort_keys=True); h.write("\n")
PY

rm -f "${activation_json}" "${response_json}" "${body}" "${config}" "${activation_config}" "${responses_config}"
printf '[edge_activation_watchdog] ok\n'
