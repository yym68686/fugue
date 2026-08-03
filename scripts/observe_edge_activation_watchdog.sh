#!/usr/bin/env bash

set -euo pipefail

evidence_dir="${FUGUE_EDGE_WATCHDOG_EVIDENCE_DIR:-${RUNNER_TEMP:-/tmp}/fugue-edge-activation-watchdog}"
install -d -m 700 "${evidence_dir}"
api_config="${evidence_dir}/api-curl.conf"
activation_json="${evidence_dir}/activation.json"
platform_json="${evidence_dir}/platform-evidence.json"
evidence_json="${evidence_dir}/evidence.json"
watchdog_cleanup() {
  rm -f -- "${activation_json:-}" "${platform_json:-}" "${api_config:-}"
}
trap watchdog_cleanup EXIT

: "${FUGUE_EDGE_ACTIVATION_API_URL:?FUGUE_EDGE_ACTIVATION_API_URL is required}"
: "${FUGUE_EDGE_ACTIVATION_API_KEY:?FUGUE_EDGE_ACTIVATION_API_KEY is required}"
[[ "${FUGUE_EDGE_ACTIVATION_API_URL}" =~ ^https://[^/]+(/.*)?$ ]]
[[ "${FUGUE_EDGE_ACTIVATION_API_KEY}" =~ ^[A-Za-z0-9._-]+$ && ${#FUGUE_EDGE_ACTIVATION_API_KEY} -ge 20 && ${#FUGUE_EDGE_ACTIVATION_API_KEY} -le 512 ]]

(
  umask 077
  printf 'silent\nshow-error\nconnect-timeout = 5\nmax-time = 30\nproto = "=https"\nheader = "Authorization: Bearer %s"\nheader = "Content-Type: application/json"\n' \
    "${FUGUE_EDGE_ACTIVATION_API_KEY}" >"${api_config}"
)
chmod 600 "${api_config}"
unset FUGUE_EDGE_ACTIVATION_API_KEY
for file in "${activation_json}" "${platform_json}" "${evidence_json}"; do
  install -m 600 /dev/null "${file}"
done

activation_status="$(curl --config "${api_config}" \
  --output "${activation_json}" --write-out '%{http_code}' "${FUGUE_EDGE_ACTIVATION_API_URL%/}/v1/admin/edge/activation")"
[[ "${activation_status}" == "200" ]]

eligibility_output="$(FUGUE_EDGE_WATCHDOG_NOW="${FUGUE_EDGE_WATCHDOG_NOW:-}" python3 - "${activation_json}" <<'PY'
import datetime,json,os,sys
with open(sys.argv[1],encoding="utf-8") as h: value=json.load(h)
activation=value.get("activation") or {}
if activation.get("schema")!="edge-activation/v1":
    raise SystemExit("edge activation schema is invalid")
phase=activation.get("phase")
authority=activation.get("route_authority")
if phase=="legacy-authoritative":
    if authority!="legacy" or (value.get("active_epochs") or []) or (activation.get("expected_instances") or []):
        raise SystemExit("legacy edge activation terminal state is inconsistent")
    generation=activation.get("generation")
    if not isinstance(generation,int) or generation<=0:
        raise SystemExit("legacy edge activation generation is invalid")
    print("legacy-authoritative\t"+str(generation)); raise SystemExit(0)
if phase!="active-epoch-enforced" or authority!="active-epoch":
    raise SystemExit("edge activation is not in a terminal authoritative phase")
release=str(activation.get("release_id") or "")
if not release: raise SystemExit("edge activation release identity is missing")
raw=activation.get("soak_started_at")
if not isinstance(raw,str): raise SystemExit("edge activation has no soak timestamp")
started=datetime.datetime.fromisoformat(raw.replace("Z","+00:00"))
now_raw=os.environ.get("FUGUE_EDGE_WATCHDOG_NOW")
now=datetime.datetime.fromisoformat(now_raw.replace("Z","+00:00")) if now_raw else datetime.datetime.now(datetime.timezone.utc)
age=(now-started).total_seconds()
if age < 24*3600:
    print("not-due\t"+release); raise SystemExit(0)
expected=activation.get("expected_instances") or []
instances=value.get("instances") or []
by_key={(v.get("edge_id"),v.get("edge_group_id"),v.get("slot"),v.get("instance_uid"),v.get("release_epoch")):v for v in instances}
for item in expected:
    key=tuple(item.get(name) for name in ("edge_id","edge_group_id","slot","instance_uid","release_epoch"))
    observed=by_key.get(key); node=(observed or {}).get("node") or {}
    if not observed or not observed.get("effective_healthy") or observed.get("failure_class") or node.get("draining") or node.get("tls_status")!="ready":
        raise SystemExit("an expected active edge instance is not healthy after 24 hours")
print("due\t"+release)
PY
)"
IFS=$'\t' read -r eligibility release_epoch <<<"${eligibility_output}"
if [[ "${eligibility}" == "legacy-authoritative" ]]; then
  ACTIVATION_GENERATION="${release_epoch}" python3 - "${evidence_json}" <<'PY'
import hashlib,json,os,sys
material={"schema":"edge-activation-watchdog/v1","status":"legacy-authoritative","activation_generation":int(os.environ["ACTIVATION_GENERATION"])}
material["digest"]="sha256:"+hashlib.sha256(json.dumps(material,separators=(",",":"),sort_keys=True).encode()).hexdigest()
with open(sys.argv[1],"w",encoding="utf-8") as h: json.dump(material,h,separators=(",",":"),sort_keys=True); h.write("\n")
PY
  printf '[edge_activation_watchdog] legacy-authoritative\n'
  exit 0
fi
if [[ "${eligibility}" == "not-due" ]]; then
  printf '{"schema":"edge-activation-watchdog/v1","status":"not-due"}\n' >"${evidence_json}"
  exit 0
fi
[[ "${eligibility}" == "due" && -n "${release_epoch}" ]]

platform_status="$(curl --config "${api_config}" --get \
  --data-urlencode "release_epoch=${release_epoch}" --data-urlencode 'window=30m' \
  --output "${platform_json}" --write-out '%{http_code}' \
  "${FUGUE_EDGE_ACTIVATION_API_URL%/}/v1/admin/edge/release-evidence")"
[[ "${platform_status}" == "200" ]]

ACTIVATION_STATUS="${activation_status}" PLATFORM_STATUS="${platform_status}" EXPECTED_RELEASE="${release_epoch}" python3 - "${activation_json}" "${platform_json}" "${evidence_json}" <<'PY'
import hashlib,json,os,re,sys
with open(sys.argv[1],encoding="utf-8") as h: activation=json.load(h)["activation"]
with open(sys.argv[2],encoding="utf-8") as h: platform=json.load(h)
if platform.get("schema")!="platform-release-evidence/v1" or platform.get("status")!="passed" or platform.get("release_epoch")!=os.environ["EXPECTED_RELEASE"]:
    raise SystemExit("24-hour platform release evidence did not pass")
digest=str(platform.get("evidence_digest") or "")
if not re.fullmatch(r"sha256:[0-9a-f]{64}",digest): raise SystemExit("platform release evidence digest is invalid")
metrics=platform.get("metrics") or {}
if int(metrics.get("request_count") or 0)<=0 or int(metrics.get("hard_failure_count") or 0)!=0:
    raise SystemExit("24-hour request evidence is incomplete or failed")
material={"schema":"edge-activation-watchdog/v1","status":"passed","activation_generation":activation["generation"],"release_id":activation["release_id"],"plan_digest":activation["plan_digest"],"activation_http":int(os.environ["ACTIVATION_STATUS"]),"platform_http":int(os.environ["PLATFORM_STATUS"]),"platform_evidence_digest":digest}
material["digest"]="sha256:"+hashlib.sha256(json.dumps(material,separators=(",",":"),sort_keys=True).encode()).hexdigest()
with open(sys.argv[3],"w",encoding="utf-8") as h: json.dump(material,h,separators=(",",":"),sort_keys=True); h.write("\n")
PY

printf '[edge_activation_watchdog] ok\n'
