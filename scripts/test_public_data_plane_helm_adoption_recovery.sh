#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SUBJECT="${ROOT}/scripts/recover_public_data_plane_helm_adoption.sh"
HEAD_SHA="$(git -C "${ROOT}" rev-parse HEAD)"
WAL_DIGEST="sha256:$(printf '1%.0s' {1..64})"
BASELINE_DIGEST="sha256:$(printf '2%.0s' {1..64})"
TOKEN="0123456789abcdef0123456789abcdef"
TOKEN_DIGEST="sha256:$(printf '%s' "${TOKEN}" | shasum -a 256 | awk '{print $1}')"
TMP="$(mktemp -d)"; trap '[[ "${KEEP_RECOVERY_TEST_TMP:-false}" == true ]] || rm -rf "${TMP}" "${CAS_SOCKET_ROOT:-}"' EXIT
fail(){ echo "recovery-test: $*" >&2; exit 1; }
count(){ local want="$1" value="$2" file="$3"; local got; got="$(grep -c -- "${value}" "${file}" 2>/dev/null || true)"; [[ "${got}" == "${want}" ]] || fail "${value}: got ${got}, want ${want}"; }

fixture(){
  local dir="$1" phase="$2"
  mkdir -p "${dir}/bin" "${dir}/evidence"
  : >"${dir}/log"; printf 'ownership\n' >"${dir}/ownership.yaml"
  cat >"${dir}/bin/adoption" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
sub="$1"; shift; echo "tool:${sub}" >>"${TEST_LOG}"
arg(){ local key="$1"; shift; while (( $# )); do [[ "$1" == "${key}" ]] && { echo "$2"; return; }; shift; done; }
case "${sub}" in
 wal-verify|verify-recovery-candidate) : ;;
 verify-restore) [[ "${TEST_LIVE_EXACT}" == true ]] ;;
 canonicalize-secret-free) cat ;;
 verify-recovery-base) [[ "${TEST_HELM_BASE}" == true ]] ;;
 restore-patches) printf '[{"name":"a","patch":[]},{"name":"b","patch":[]},{"name":"c","patch":[]}]\n' ;;
 finalize) dir="$(arg --evidence-dir "$@")"; printf '{"digest":"%s"}\n' "${TEST_BASELINE_DIGEST}" >"${dir}/stage1-baseline.json" ;;
 *) exit 91 ;;
esac
MOCK
  chmod +x "${dir}/bin/adoption"
  cat >"${dir}/bin/evidence" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
while (( $# )); do [[ "$1" == --output ]] && { printf 'observed\n' >"$2"; exit; }; shift; done
exit 1
MOCK
  chmod +x "${dir}/bin/evidence"
  cat >"${dir}/bin/helm" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
 status) if [[ "${TEST_HELM_BASE}" == true ]]; then echo '{"version":806}'; else echo '{"version":807}'; fi ;;
 get) if [[ "$2" == manifest ]]; then if [[ "${TEST_HELM_BASE}" == true ]]; then echo base; else echo target; fi; else exit 1; fi ;;
 *) echo 'second Helm apply forbidden' >&2; exit 92 ;;
esac
MOCK
  chmod +x "${dir}/bin/helm"
  cat >"${dir}/bin/kubectl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
echo "kubectl:$*" >>"${TEST_LOG}"
if [[ " $* " == *" get daemonsets.apps,configmaps "* ]]; then
  echo '{"apiVersion":"v1","kind":"List","items":[]}'
elif [[ " $* " == *" get lease/"* ]]; then
  recovery=''
  [[ "${TEST_PHASE}" == lease-acquired ]] || recovery=',"fugue.pro/recovery-required":"true"'
  printf '{"metadata":{"uid":"lease-uid","resourceVersion":"9","annotations":{"fugue.pro/coordination-token":"%s"%s}},"spec":{"holderIdentity":"release/123-1","leaseDurationSeconds":120}}\n' "${TEST_TOKEN}" "${recovery}"
elif [[ " $* " == *" get configmap/"* ]]; then
  echo '{}'
elif [[ " $* " == *" patch daemonset "* ]]; then
  :
elif [[ " $* " == *" patch lease/"* ]]; then
  printf '{"metadata":{"uid":"lease-uid","resourceVersion":"10","annotations":{}},"spec":{"holderIdentity":"","leaseDurationSeconds":120}}\n'
else
  exit 93
fi
MOCK
  chmod +x "${dir}/bin/kubectl"
  cat >"${dir}/coord.sh" <<'MOCK'
    stop_control_plane_backup_coordination_lease_renewer(){ :; }
    control_plane_backup_coordination_lease_json(){ "${KUBECTL}" -n "${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAMESPACE}" get "lease/${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAME}" -o json; }
    control_plane_backup_coordination_now(){ printf '2026-08-01T00:00:02Z\n'; }
    trim_field(){ printf '%s' "$1"; }
    bounded_kubectl(){ shift; echo coord:release >>"${TEST_LOG}"; "${KUBECTL}" "$@"; }
    verify_released_recovery_lease(){ :; }
    control_plane_stale_release_old_process_absent(){ echo coord:origin-process-absent >>"${TEST_LOG}"; }
MOCK
  cat >"${dir}/recovery.sh" <<'MOCK'
public_data_plane_adoption_recovery_cm_name(){ echo recovery-cm; }
public_data_plane_adoption_extract_recovery_configmap(){
  local output="$2"; mkdir -p "${output}"
  cat >"${output}/wal.json" <<EOF
{"digest":"${TEST_WAL_DIGEST}","originRunId":"123","phase":"${TEST_PHASE}","sourceCommit":"${TEST_HEAD_SHA}","targetRevision":"807","baselineDigest":"${TEST_WAL_BASELINE_DIGEST}","leaseOwner":"release/123-1","leaseTokenDigest":"${TEST_TOKEN_DIGEST}","applyAttempts":${TEST_APPLY_ATTEMPTS},"restoreAttempts":${TEST_RESTORE_ATTEMPTS}}
EOF
  echo '{}' >"${output}/transaction.json"; echo '{}' >"${output}/restore.json"; echo 7 >"${output}/configmap-resource-version"
}
public_data_plane_adoption_advance_recovery_wal(){ echo "wal:advance:$1" >>"${TEST_LOG}"; }
public_data_plane_adoption_seal_terminal_wal(){ echo "wal:seal:$1" >>"${TEST_LOG}"; printf '{}\n' >"${FUGUE_PUBLIC_DATA_PLANE_ADOPTION_EVIDENCE_DIR}/terminal-wal.json"; }
public_data_plane_adoption_delete_terminal_wal(){ echo wal:delete >>"${TEST_LOG}"; }
public_data_plane_adoption_delete_unarmed_wal(){ echo wal:delete-unarmed >>"${TEST_LOG}"; }
MOCK
}

run(){
 local scenario="$1" phase="$2" helm_base="$3" want="$4" wal_baseline="${5:-}" live_exact="${6:-true}"
 local apply_attempts=1 restore_attempts=0
 case "${phase}" in
   lease-acquired|fence-armed|aborted-before-apply) apply_attempts=0 ;;
   restore-started|restore-failed|restore-succeeded|restore-succeeded-awaiting-helm-compensation) restore_attempts=1 ;;
 esac
 local dir="${TMP}/${scenario}"; fixture "${dir}" "${phase}"
 set +e
 PATH="${dir}/bin:${PATH}" TEST_LOG="${dir}/log" TEST_PHASE="${phase}" TEST_HELM_BASE="${helm_base}" TEST_LIVE_EXACT="${live_exact}" \
 TEST_WAL_DIGEST="${WAL_DIGEST}" TEST_WAL_BASELINE_DIGEST="${wal_baseline}" TEST_BASELINE_DIGEST="${BASELINE_DIGEST}" \
 TEST_APPLY_ATTEMPTS="${apply_attempts}" TEST_RESTORE_ATTEMPTS="${restore_attempts}" \
 TEST_TOKEN="${TOKEN}" TEST_TOKEN_DIGEST="${TOKEN_DIGEST}" TEST_HEAD_SHA="${HEAD_SHA}" \
 REPO_ROOT="${ROOT}" FUGUE_RECOVERY_SHA="${HEAD_SHA}" FUGUE_EXPECTED_SOURCE_SHA="${HEAD_SHA}" FUGUE_EXPECTED_WAL_DIGEST="${WAL_DIGEST}" FUGUE_EXPECTED_ORIGIN_RUN_ID=123 \
 FUGUE_PUBLIC_DATA_PLANE_ADOPTION_TOOL="${dir}/bin/adoption" FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL="${dir}/bin/evidence" \
 FUGUE_RELEASE_DOMAIN_OWNERSHIP_FILE="${dir}/ownership.yaml" FUGUE_PUBLIC_DATA_PLANE_ADOPTION_EVIDENCE_DIR="${dir}/evidence" \
 FUGUE_PUBLIC_DATA_PLANE_ADOPTION_COORDINATION_LIBRARY="${dir}/coord.sh" FUGUE_PUBLIC_DATA_PLANE_ADOPTION_RECOVERY_LIBRARY="${dir}/recovery.sh" \
 HELM="${dir}/bin/helm" KUBECTL="${dir}/bin/kubectl" bash "${SUBJECT}" >"${dir}/out" 2>"${dir}/err"
 status=$?; set -e
 [[ "${status}" == "${want}" ]] || { cat "${dir}/err" >&2; fail "${scenario}: ${status} != ${want}"; }
 echo "${dir}"
}

dir="$(run death-after-fence fence-armed true 0)"
count 0 'patch daemonset' "${dir}/log"; count 1 'wal:advance:aborted-before-apply' "${dir}/log"; count 1 'tool:verify-restore' "${dir}/log"; count 1 'tool:verify-recovery-base' "${dir}/log"; count 1 'wal:seal:aborted-before-apply' "${dir}/log"; count 1 'coord:release' "${dir}/log"; count 1 '^wal:delete$' "${dir}/log"

dir="$(run resume-aborted-before-apply aborted-before-apply true 0)"
count 0 'patch daemonset' "${dir}/log"; count 0 'wal:advance:' "${dir}/log"; count 1 'tool:verify-restore' "${dir}/log"; count 1 'tool:verify-recovery-base' "${dir}/log"; count 1 'wal:seal:aborted-before-apply' "${dir}/log"; count 1 'coord:release' "${dir}/log"; count 1 '^wal:delete$' "${dir}/log"

dir="$(run fence-armed-helm-drift fence-armed false 1)"
count 0 'patch daemonset' "${dir}/log"; count 0 'wal:advance:aborted-before-apply' "${dir}/log"; count 0 'coord:release' "${dir}/log"; count 0 '^wal:delete$' "${dir}/log"

dir="$(run fence-armed-live-drift fence-armed true 1 '' false)"
count 0 'patch daemonset' "${dir}/log"; count 0 'wal:advance:aborted-before-apply' "${dir}/log"; count 0 'coord:release' "${dir}/log"; count 0 '^wal:delete$' "${dir}/log"

dir="$(run death-before-fence lease-acquired true 0)"
count 0 'patch daemonset' "${dir}/log"; count 1 'wal:delete-unarmed' "${dir}/log"; count 1 'coord:release' "${dir}/log"; count 0 '^wal:delete$' "${dir}/log"

dir="$(run death-mid-apply apply-started false 2)"
count 3 'patch daemonset' "${dir}/log"; count 1 'wal:advance:restore-succeeded-awaiting-helm-compensation' "${dir}/log"; count 0 'coord:release' "${dir}/log"; count 0 '^wal:delete$' "${dir}/log"

dir="$(run failed-target-revision apply-failed false 2)"
count 3 'patch daemonset' "${dir}/log"; count 1 'wal:advance:restore-succeeded-awaiting-helm-compensation' "${dir}/log"; count 0 'coord:release' "${dir}/log"

dir="$(run after-apply-before-finalize apply-succeeded false 0)"
count 0 'patch daemonset' "${dir}/log"; count 1 'wal:advance:baseline-finalized' "${dir}/log"; count 1 'coord:release' "${dir}/log"; count 1 '^wal:delete$' "${dir}/log"

dir="$(run after-baseline baseline-finalized false 0 "${BASELINE_DIGEST}")"
count 0 'patch daemonset' "${dir}/log"; count 1 'coord:release' "${dir}/log"; count 1 '^wal:delete$' "${dir}/log"

for file in "${TMP}"/*/log; do count 0 'helm:upgrade' "${file}"; done

roundtrip="${TMP}/wal-newline-roundtrip"
mkdir -p "${roundtrip}/bin" "${roundtrip}/evidence"
: >"${roundtrip}/log"
cat >"${roundtrip}/configmap.json" <<'JSON'
{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"fugue-public-data-plane-adoption-recovery","namespace":"fugue-system","uid":"cm-uid","resourceVersion":"7","labels":{"app.kubernetes.io/instance":"fugue","app.kubernetes.io/component":"public-data-plane-adoption-recovery","fugue.io/recovery-policy":"public-data-plane-helm-adoption-v1"}},"data":{"wal.json":"{\"phase\":\"lease-acquired\"}\n","transaction.json":"{}\n","restore.json":"{}\n"}}
JSON
cat >"${roundtrip}/bin/adoption" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
sub="$1"; shift
case "${sub}" in
  wal-advance)
    wal=''; phase=''
    while (( $# )); do
      case "$1" in --wal) wal="$2"; shift 2;; --phase) phase="$2"; shift 2;; *) shift;; esac
    done
    printf '{"phase":"%s"}\n' "${phase}" >"${wal}"
    ;;
  wal-verify) ;;
  *) exit 90 ;;
esac
MOCK
chmod +x "${roundtrip}/bin/adoption"
cat >"${roundtrip}/bin/kubectl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
if [[ " $* " == *" get lease/"* ]]; then
  printf '{"metadata":{"uid":"lease-uid","resourceVersion":"9","annotations":{"fugue.pro/coordination-token":"%s","fugue.pro/recovery-required":"true"}},"spec":{"holderIdentity":"release/123-1"}}\n' "${TEST_TOKEN}"
elif [[ " $* " == *" get configmap/"* ]]; then
  cat "${TEST_CONFIGMAP}"
elif [[ " $* " == *" patch configmap/"* ]]; then
  patch=''
  while (( $# )); do [[ "$1" == -p ]] && { patch="$2"; break; }; shift; done
  PATCH="${patch}" STATE="${TEST_CONFIGMAP}" python3 - <<'PY'
import json, os, pathlib
path=pathlib.Path(os.environ["STATE"]); value=json.loads(path.read_text()); patch=json.loads(os.environ["PATCH"])
assert patch[0] == {"op":"test","path":"/metadata/resourceVersion","value":value["metadata"]["resourceVersion"]}
assert patch[1]["op"] == "replace" and patch[1]["path"] == "/data/wal.json"
value["data"]["wal.json"] = patch[1]["value"]
value["metadata"]["resourceVersion"] = str(int(value["metadata"]["resourceVersion"])+1)
path.write_text(json.dumps(value,separators=(",",":"))+"\n")
PY
elif [[ " $* " == *" delete configmap/"* ]]; then
  [[ " $* " != *" --resource-version="* ]] || { echo 'unknown flag: --resource-version' >&2; exit 1; }
  : >"${TEST_DELETED}"
else
  exit 91
fi
MOCK
chmod +x "${roundtrip}/bin/kubectl"
(
  set -euo pipefail
  RELEASE_FULLNAME=fugue RELEASE_NAME=fugue RELEASE_NAMESPACE=fugue-system
  FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAMESPACE=fugue-system
  FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAME=fugue-lock
  CONTROL_PLANE_BACKUP_COORDINATION_LEASE_OWNER=release/123-1
  CONTROL_PLANE_BACKUP_COORDINATION_LEASE_TOKEN="${TOKEN}"
  EVIDENCE_DIR="${roundtrip}/evidence"
  ADOPTION_TOOL="${roundtrip}/bin/adoption"
  KUBECTL="${roundtrip}/bin/kubectl"
  TEST_TOKEN="${TOKEN}" TEST_CONFIGMAP="${roundtrip}/configmap.json" TEST_DELETED="${roundtrip}/deleted" TEST_LOG="${roundtrip}/log"
  export RELEASE_FULLNAME RELEASE_NAME RELEASE_NAMESPACE FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAMESPACE
  export FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAME CONTROL_PLANE_BACKUP_COORDINATION_LEASE_OWNER
  export CONTROL_PLANE_BACKUP_COORDINATION_LEASE_TOKEN EVIDENCE_DIR ADOPTION_TOOL KUBECTL TEST_TOKEN TEST_CONFIGMAP TEST_DELETED TEST_LOG
  # shellcheck source=scripts/lib/public_data_plane_adoption_recovery.sh
  source "${ROOT}/scripts/lib/public_data_plane_adoption_recovery.sh"
  public_data_plane_adoption_delete_configmap_cas(){
    [[ "$1" == fugue-public-data-plane-adoption-recovery && "$2" == cm-uid && "$3" =~ ^[0-9]+$ ]]
    printf 'cas-delete:%s:%s:%s\n' "$1" "$2" "$3" >>"${TEST_LOG:-/dev/null}"
    : >"${TEST_DELETED}"
  }
  public_data_plane_adoption_delete_unarmed_wal true
  public_data_plane_adoption_advance_recovery_wal fence-armed
  cp "${EVIDENCE_DIR}/recovery-wal.json" "${EVIDENCE_DIR}/fence-armed-wal.json"
  public_data_plane_adoption_advance_recovery_wal aborted-before-apply
  public_data_plane_adoption_seal_terminal_wal aborted-before-apply
  public_data_plane_adoption_delete_terminal_wal
)
count 1 'cas-delete:fugue-public-data-plane-adoption-recovery:cm-uid:7' "${roundtrip}/log"
count 1 'cas-delete:fugue-public-data-plane-adoption-recovery:cm-uid:9' "${roundtrip}/log"
STATE="${roundtrip}/configmap.json" FIRST="${roundtrip}/evidence/fence-armed-wal.json" WAL="${roundtrip}/evidence/recovery-wal.json" TERMINAL="${roundtrip}/evidence/terminal-wal.json" DELETED="${roundtrip}/deleted" python3 - <<'PY'
import json, os, pathlib
stored=json.loads(pathlib.Path(os.environ["STATE"]).read_text())["data"]["wal.json"]
local=pathlib.Path(os.environ["WAL"]).read_text()
first=pathlib.Path(os.environ["FIRST"]).read_text(); terminal=pathlib.Path(os.environ["TERMINAL"]).read_text()
assert first.endswith("\n") and json.loads(first)["phase"] == "fence-armed"
assert stored == local == terminal and stored.endswith("\n") and json.loads(stored)["phase"] == "aborted-before-apply"
assert pathlib.Path(os.environ["DELETED"]).exists()
PY

cas_real="${TMP}/real-unix-proxy-cas"
CAS_SOCKET_ROOT="$(mktemp -d /tmp/fugue-pdp-cas-test.XXXXXX)"
mkdir -p "${cas_real}"
chmod 0700 "${cas_real}" "${CAS_SOCKET_ROOT}"
REAL_KUBECTL="$(command -v kubectl)"; REAL_OPENSSL="$(command -v openssl)"
[[ "${REAL_KUBECTL}" == /* && "${REAL_OPENSSL}" == /* ]] || fail "real kubectl/openssl are required for the CAS proxy harness"
"${REAL_OPENSSL}" req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 -subj /CN=localhost \
  -keyout "${cas_real}/server.key" -out "${cas_real}/server.crt" >/dev/null 2>&1
chmod 0600 "${cas_real}/server.key" "${cas_real}/server.crt"
cat >"${cas_real}/server.py" <<'PY'
import http.server,json,os,pathlib,ssl,sys
mode,output,portfile,cert,key=sys.argv[1:]
requests=[]; state={"exists":True,"uid":"cm-uid","rv":"7"}
if mode=="uid-drift": state["uid"]="actual-uid"
if mode=="rv-drift": state["rv"]="8"
def write_capture(): pathlib.Path(output).write_text(json.dumps(requests,separators=(",",":"))+"\n")
class H(http.server.BaseHTTPRequestHandler):
 def record(self,body):
  try: parsed=json.loads(body) if body else None
  except Exception: parsed="invalid"
  requests.append({"method":self.command,"path":self.path,"contentType":self.headers.get("Content-Type"),"bodyLength":len(body),"bodyText":body.decode(errors="replace"),"body":parsed}); write_capture()
 def send_json(self,code,value):
  body=json.dumps(value,separators=(",",":")).encode(); self.send_response(code); self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
 def status(self,code,reason,status="Failure",include_code=True):
  value={"apiVersion":"v1","kind":"Status","status":status,"reason":reason}
  if include_code: value["code"]=code
  return value
 def do_DELETE(self):
  body=self.rfile.read(int(self.headers.get("Content-Length","0"))); self.record(body)
  if mode=="raw-drop": state["exists"]=False; self.send_json(200,self.status(200,"","Success")); return
  try: value=json.loads(body); pre=value["preconditions"]
  except Exception: self.send_json(400,self.status(400,"BadRequest")); return
  if value.get("apiVersion")!="v1" or value.get("kind")!="DeleteOptions" or pre.get("uid")!=state["uid"] or pre.get("resourceVersion")!=state["rv"] or mode=="conflict":
   self.send_json(409,self.status(409,"Conflict")); return
  if mode=="notfound" or mode=="notfound-exists":
   if mode=="notfound": state["exists"]=False
   self.send_json(404,self.status(404,"NotFound")); return
  if mode=="new-uid": state.update(exists=True,uid="replacement-uid",rv="9")
  elif mode not in {"exists","five-hundred-exists","timeout-exists"}: state["exists"]=False
  if mode=="ambiguous": self.connection.shutdown(2); self.connection.close(); return
  if mode=="invalid-json":
   payload=b'{invalid'; self.send_response(200); self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(payload))); self.end_headers(); self.wfile.write(payload); return
  if mode=="success-no-code-200": self.send_json(200,self.status(200,"","Success",False)); return
  if mode=="success-no-code-202": self.send_json(202,self.status(202,"","Success",False)); return
  if mode=="success-no-code-202-exists": state["exists"]=True; self.send_json(202,self.status(202,"","Success",False)); return
  if mode=="success-null-code":
   value=self.status(200,"","Success",False); value["code"]=None; self.send_json(200,value); return
  if mode=="success-wrong-code": self.send_json(200,self.status(202,"","Success")); return
  if mode=="non-status": self.send_json(200,{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"recovery-cm"}}); return
  if mode=="notfound-no-code": self.send_json(404,self.status(404,"NotFound","Failure",False)); return
  if mode in {"five-hundred-absent","five-hundred-exists"}: self.send_json(500,self.status(500,"InternalError")); return
  self.send_json(200,self.status(200,"","Success"))
 def do_GET(self):
  self.record(b"")
  if not state["exists"]: self.send_json(404,self.status(404,"NotFound")); return
  self.send_json(200,{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"recovery-cm","namespace":"fugue-system","uid":state["uid"],"resourceVersion":state["rv"]}})
 def log_message(self,*args): pass
server=http.server.ThreadingHTTPServer(("127.0.0.1",0),H); context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); context.load_cert_chain(cert,key); server.socket=context.wrap_socket(server.socket,server_side=True)
pathlib.Path(portfile).write_text(str(server.server_address[1])); server.serve_forever()
PY

start_cas_server(){
  local mode="$1" dir="$2"
  python3 "${cas_real}/server.py" "${mode}" "${dir}/capture.json" "${dir}/port" "${cas_real}/server.crt" "${cas_real}/server.key" &
  CAS_SERVER_PID=$!
  for _ in $(seq 1 100); do [[ -s "${dir}/port" ]] && break; sleep 0.02; done
  [[ -s "${dir}/port" ]]
  cat >"${dir}/kubeconfig" <<EOF
apiVersion: v1
kind: Config
clusters:
- name: fake
  cluster:
    server: https://127.0.0.1:$(cat "${dir}/port")
    insecure-skip-tls-verify: true
contexts:
- name: fake
  context: {cluster: fake, user: fake}
current-context: fake
users:
- name: fake
  user: {token: fake-token}
EOF
  chmod 0600 "${dir}/kubeconfig"
}
stop_cas_server(){
  kill "${CAS_SERVER_PID}" >/dev/null 2>&1 || true
  wait "${CAS_SERVER_PID}" >/dev/null 2>&1 || true
  CAS_SERVER_PID=""
}
REAL_CURL_BIN="$(command -v curl)"
run_cas_case(){
  local mode="$1" want="$2" uid="${3:-cm-uid}" rv="${4:-7}" curl_mode="${5:-real}" dir status
  dir="${cas_real}/${mode}"
  mkdir -p "${dir}/evidence"; chmod 0700 "${dir}" "${dir}/evidence"
  if [[ "${curl_mode}" == lost-response || "${curl_mode}" == timeout-response ]]; then
    mkdir -p "${dir}/bin"
    cat >"${dir}/bin/curl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
method=''; output=''; args=()
while (( $# )); do
  case "$1" in
    --request) method="$2"; args+=("$1" "$2"); shift 2 ;;
    --output) output="$2"; args+=("$1" "$2"); shift 2 ;;
    --write-out)
      if [[ "${method}" == DELETE ]]; then args+=("$1" ''); else args+=("$1" "$2"); fi
      shift 2
      ;;
    *) args+=("$1"); shift ;;
  esac
done
if [[ "${method}" == DELETE ]]; then
  "${REAL_CURL}" "${args[@]}" >/dev/null
  : >"${output}"
  printf '000'
  exit "${MOCK_CURL_EXIT}"
fi
exec "${REAL_CURL}" "${args[@]}"
MOCK
    chmod +x "${dir}/bin/curl"
  fi
  start_cas_server "${mode}" "${dir}"
  set +e
  (
    set -euo pipefail
    PATH="${dir}/bin:${PATH}" REAL_CURL="${REAL_CURL_BIN}" \
      MOCK_CURL_EXIT="$([[ "${curl_mode}" == timeout-response ]] && printf 28 || printf 52)" \
      RELEASE_NAMESPACE=fugue-system EVIDENCE_DIR="${dir}/evidence" KUBECTL="${REAL_KUBECTL}" \
      KUBECONFIG="${dir}/kubeconfig" RUNNER_TEMP="${CAS_SOCKET_ROOT}"
    export PATH REAL_CURL MOCK_CURL_EXIT RELEASE_NAMESPACE EVIDENCE_DIR KUBECTL KUBECONFIG RUNNER_TEMP
    # shellcheck source=scripts/lib/public_data_plane_adoption_recovery.sh
    source "${ROOT}/scripts/lib/public_data_plane_adoption_recovery.sh"
    public_data_plane_adoption_delete_configmap_cas recovery-cm "${uid}" "${rv}"
  ) >"${dir}/out" 2>"${dir}/err"
  status=$?
  set -e
  stop_cas_server
  [[ ( "${want}" == success && "${status}" == 0 ) || ( "${want}" == failure && "${status}" != 0 ) ]] || { cat "${dir}/err" >&2; fail "CAS case ${mode}: status=${status} want=${want}"; }
  MODE="${mode}" CAPTURE="${dir}/capture.json" WANT="${want}" python3 - <<'PY'
import json,os
rows=json.load(open(os.environ["CAPTURE"])); deletes=[x for x in rows if x["method"]=="DELETE"]
assert len(deletes)==1
body=deletes[0]; assert body["path"]=="/api/v1/namespaces/fugue-system/configmaps/recovery-cm"
assert body["contentType"]=="application/json" and body["bodyLength"]>0
assert body["bodyText"]=='{"apiVersion":"v1","kind":"DeleteOptions","preconditions":{"resourceVersion":"7","uid":"cm-uid"}}\n'
assert body["body"]=={"apiVersion":"v1","kind":"DeleteOptions","preconditions":{"resourceVersion":"7","uid":"cm-uid"}}
PY
  [[ -z "$(find "${CAS_SOCKET_ROOT}" -mindepth 1 -maxdepth 1 -print -quit)" ]] || fail "CAS case ${mode} leaked proxy state"
}

run_cas_case success success
run_cas_case success-no-code-200 success
run_cas_case success-no-code-202 success
run_cas_case success-no-code-202-exists failure
run_cas_case success-null-code failure
run_cas_case success-wrong-code failure
run_cas_case non-status failure
run_cas_case notfound-no-code failure
run_cas_case ambiguous failure
run_cas_case lost-response success cm-uid 7 lost-response
run_cas_case timeout-exists failure cm-uid 7 timeout-response
run_cas_case five-hundred-absent success
run_cas_case notfound success
run_cas_case conflict failure
run_cas_case uid-drift failure
run_cas_case rv-drift failure
run_cas_case new-uid failure
run_cas_case exists failure
run_cas_case five-hundred-exists failure
run_cas_case notfound-exists failure
run_cas_case invalid-json failure

classifier="${cas_real}/classifier"
mkdir -p "${classifier}"
printf '%s\n' '{"apiVersion":"v1","kind":"Status","status":"Success","code":null}' >"${classifier}/response.json"
set +e
(
  # shellcheck source=scripts/lib/public_data_plane_adoption_recovery.sh
  source "${ROOT}/scripts/lib/public_data_plane_adoption_recovery.sh"
  RESPONSE="${classifier}/response.json" HTTP_CODE=200 CURL_RC=0 OUTPUT="${classifier}/classification" \
    public_data_plane_adoption_classify_delete_response
)
classifier_status=$?
set -e
[[ "${classifier_status}" != 0 ]] || fail "invalid Status.code was classified as a successful DELETE"
[[ "$(cat "${classifier}/classification")" == invalid ]] || fail "failed DELETE classification did not leave a deterministic result"

proxy_failure="${cas_real}/proxy-failure"
mkdir -p "${proxy_failure}/evidence" "${proxy_failure}/bin"
cp "${cas_real}/success/kubeconfig" "${proxy_failure}/kubeconfig"
cat >"${proxy_failure}/bin/kubectl" <<'MOCK'
#!/usr/bin/env bash
exit 42
MOCK
chmod +x "${proxy_failure}/bin/kubectl"
set +e
(
  RELEASE_NAMESPACE=fugue-system EVIDENCE_DIR="${proxy_failure}/evidence" \
    KUBECTL="${proxy_failure}/bin/kubectl" KUBECONFIG="${proxy_failure}/kubeconfig" \
    RUNNER_TEMP="${CAS_SOCKET_ROOT}" \
    bash -c 'source "$1"; public_data_plane_adoption_delete_configmap_cas recovery-cm cm-uid 7' \
    bash "${ROOT}/scripts/lib/public_data_plane_adoption_recovery.sh"
) >"${proxy_failure}/out" 2>"${proxy_failure}/err"
proxy_failure_status=$?
set -e
[[ "${proxy_failure_status}" != 0 ]] || fail "CAS proxy startup failure was accepted"
[[ -z "$(find "${CAS_SOCKET_ROOT}" -mindepth 1 -maxdepth 1 -print -quit)" ]] || fail "CAS proxy startup failure leaked proxy state"

raw_drop="${cas_real}/raw-drop"; mkdir -p "${raw_drop}"; start_cas_server raw-drop "${raw_drop}"
printf '%s\n' '{"apiVersion":"v1","kind":"DeleteOptions","preconditions":{"uid":"cm-uid","resourceVersion":"7"}}' | \
  KUBECONFIG="${raw_drop}/kubeconfig" "${REAL_KUBECTL}" delete \
    --raw=/api/v1/namespaces/fugue-system/configmaps/recovery-cm -f - >/dev/null
stop_cas_server
CAPTURE="${raw_drop}/capture.json" python3 - <<'PY'
import json,os
rows=json.load(open(os.environ["CAPTURE"])); assert len(rows)==1 and rows[0]["method"]=="DELETE"
assert rows[0]["bodyLength"]==0 and rows[0]["body"] is None
PY

production_shape="${TMP}/production-shape-aborted"
mkdir -p "${production_shape}/bin" "${production_shape}/evidence"
: >"${production_shape}/log"
printf 'ownership\n' >"${production_shape}/ownership.yaml"
REAL_PYTHON3="$(command -v python3)"
cp "${TMP}/death-after-fence/bin/adoption" "${production_shape}/bin/adoption"
cp "${TMP}/death-after-fence/bin/evidence" "${production_shape}/bin/evidence"
cp "${TMP}/death-after-fence/bin/helm" "${production_shape}/bin/helm"
cat >"${production_shape}/bin/adoption" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
sub="$1"; shift; echo "tool:${sub}" >>"${TEST_LOG}"
arg(){ local key="$1"; shift; while (( $# )); do [[ "$1" == "${key}" ]] && { echo "$2"; return; }; shift; done; }
case "${sub}" in
  wal-verify|verify-recovery-candidate) : ;;
  wal-advance)
    wal="$(arg --wal "$@")"; phase="$(arg --phase "$@")"
    WAL="${wal}" PHASE="${phase}" python3 - <<'PY'
import json, os, pathlib
path=pathlib.Path(os.environ["WAL"]); value=json.loads(path.read_text()); value["phase"]=os.environ["PHASE"]
path.write_text(json.dumps(value,separators=(",",":"))+"\n")
PY
    ;;
  verify-restore) [[ "${TEST_LIVE_EXACT}" == true ]] ;;
  canonicalize-secret-free) cat ;;
  verify-recovery-base) [[ "${TEST_HELM_BASE}" == true ]] ;;
  restore-patches) printf '[]\n' ;;
  finalize) dir="$(arg --evidence-dir "$@")"; printf '{"digest":"%s"}\n' "${TEST_BASELINE_DIGEST}" >"${dir}/stage1-baseline.json" ;;
  *) exit 91 ;;
esac
MOCK
chmod +x "${production_shape}/bin/adoption"
cat >"${production_shape}/lease.json" <<EOF
{"apiVersion":"coordination.k8s.io/v1","kind":"Lease","metadata":{"name":"fugue-lock","namespace":"fugue-system","uid":"lease-uid","resourceVersion":"9","annotations":{"fugue.pro/coordination-token":"${TOKEN}","fugue.pro/recovery-required":"true"}},"spec":{"holderIdentity":"release/123-1","leaseDurationSeconds":120,"acquireTime":"2026-08-01T00:00:00Z","renewTime":"2026-08-01T00:00:01Z","leaseTransitions":1}}
EOF
cat >"${production_shape}/configmap.json" <<EOF
{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"fugue-fugue-public-data-plane-adoption-recovery","namespace":"fugue-system","uid":"cm-uid","resourceVersion":"7","labels":{"app.kubernetes.io/instance":"fugue","app.kubernetes.io/component":"public-data-plane-adoption-recovery","fugue.io/recovery-policy":"public-data-plane-helm-adoption-v1"}},"data":{"wal.json":"{\"digest\":\"${WAL_DIGEST}\",\"originRunId\":\"123\",\"phase\":\"aborted-before-apply\",\"sourceCommit\":\"${HEAD_SHA}\",\"targetRevision\":\"807\",\"leaseOwner\":\"release/123-1\",\"leaseTokenDigest\":\"${TOKEN_DIGEST}\",\"applyAttempts\":0,\"restoreAttempts\":0}\n","transaction.json":"{}\n","restore.json":"{}\n"}}
EOF
chmod 0600 "${production_shape}/lease.json" "${production_shape}/configmap.json"
cp "${production_shape}/lease.json" "${production_shape}/lease.initial.json"
cp "${production_shape}/configmap.json" "${production_shape}/configmap.initial.json"
chmod 0600 "${production_shape}/lease.initial.json" "${production_shape}/configmap.initial.json"
cat >"${production_shape}/bin/kubectl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
echo "kubectl:$*" >>"${TEST_LOG}"
if [[ " $* " == *" get daemonsets.apps,configmaps "* ]]; then
  echo '{"apiVersion":"v1","kind":"List","items":[]}'
elif [[ " $* " == *" get lease/"* ]]; then
  count_value=0
  [[ ! -f "${TEST_LEASE_READ_COUNT}" ]] || count_value="$(cat "${TEST_LEASE_READ_COUNT}")"
  count_value=$((count_value + 1)); printf '%s\n' "${count_value}" >"${TEST_LEASE_READ_COUNT}"
  if [[ "${count_value}" == 3 && "${TEST_LEASE_DRIFT_MODE:-none}" != none ]]; then
    MODE="${TEST_LEASE_DRIFT_MODE}" STATE="${TEST_LEASE_STATE}" python3 - <<'PY'
import json, os, pathlib
path=pathlib.Path(os.environ["STATE"]); value=json.loads(path.read_text()); mode=os.environ["MODE"]
if mode == "duration": value["spec"]["leaseDurationSeconds"] += 1
elif mode == "owner": value["spec"]["holderIdentity"] = "release/other-1"
elif mode == "token": value["metadata"]["annotations"]["fugue.pro/coordination-token"] = "different-token"
else: raise AssertionError(mode)
value["metadata"]["resourceVersion"] = str(int(value["metadata"]["resourceVersion"])+1)
path.write_text(json.dumps(value,separators=(",",":"))+"\n")
PY
  fi
  cat "${TEST_LEASE_STATE}"
elif [[ " $* " == *" get configmap/"* ]]; then
  cat "${TEST_CONFIGMAP_STATE}"
elif [[ " $* " == *" patch configmap/"* ]]; then
  patch=''
  while (( $# )); do [[ "$1" == -p ]] && { patch="$2"; break; }; shift; done
  PATCH="${patch}" STATE="${TEST_CONFIGMAP_STATE}" python3 - <<'PY'
import json, os, pathlib
path=pathlib.Path(os.environ["STATE"]); value=json.loads(path.read_text()); patch=json.loads(os.environ["PATCH"])
assert patch[0] == {"op":"test","path":"/metadata/resourceVersion","value":value["metadata"]["resourceVersion"]}
assert patch[1]["op"] == "replace" and patch[1]["path"] == "/data/wal.json"
value["data"]["wal.json"] = patch[1]["value"]
value["metadata"]["resourceVersion"] = str(int(value["metadata"]["resourceVersion"])+1)
path.write_text(json.dumps(value,separators=(",",":"))+"\n")
PY
elif [[ " $* " == *" patch lease/"* ]]; then
  patch=''; output=false
  while (( $# )); do
    case "$1" in
      -p) patch="$2"; shift 2 ;;
      -o) [[ "$2" == json ]] && output=true; shift 2 ;;
      *) shift ;;
    esac
  done
  if [[ "${TEST_PATCH_RV_DRIFT:-false}" == true ]]; then
    STATE="${TEST_LEASE_STATE}" python3 - <<'PY'
import json, os, pathlib
path=pathlib.Path(os.environ["STATE"]); value=json.loads(path.read_text())
value["metadata"]["resourceVersion"] = str(int(value["metadata"]["resourceVersion"])+1)
path.write_text(json.dumps(value,separators=(",",":"))+"\n")
PY
  fi
  PATCH="${patch}" STATE="${TEST_LEASE_STATE}" python3 - <<'PY'
import json, os, pathlib
path=pathlib.Path(os.environ["STATE"]); value=json.loads(path.read_text()); patch=json.loads(os.environ["PATCH"])
for item in patch:
    if item["op"] == "test":
        if item["path"] == "/metadata/resourceVersion": assert value["metadata"]["resourceVersion"] == item["value"], item
        elif item["path"] == "/spec/holderIdentity": assert value["spec"]["holderIdentity"] == item["value"], item
        elif item["path"] == "/metadata/annotations/fugue.pro~1coordination-token": assert value["metadata"]["annotations"]["fugue.pro/coordination-token"] == item["value"], item
        elif item["path"] == "/spec/leaseDurationSeconds": assert value["spec"]["leaseDurationSeconds"] == item["value"], item
        else: raise AssertionError(item)
    elif item["path"] == "/metadata/annotations": value["metadata"]["annotations"] = item["value"]
    elif item["path"] == "/spec/holderIdentity": value["spec"]["holderIdentity"] = item["value"]
    elif item["path"] == "/spec/leaseDurationSeconds": value["spec"]["leaseDurationSeconds"] = item["value"]
    elif item["path"] == "/spec/renewTime": value["spec"]["renewTime"] = item["value"]
    else: raise AssertionError(item)
value["metadata"]["resourceVersion"] = str(int(value["metadata"]["resourceVersion"])+1)
path.write_text(json.dumps(value,separators=(",",":"))+"\n")
print(json.dumps(value,separators=(",",":")))
PY
elif [[ " $* " == *" delete configmap/"* ]]; then
  : >"${TEST_DELETED}"
else
  exit 93
fi
MOCK
chmod +x "${production_shape}/bin/kubectl"
cat >"${production_shape}/bin/python3" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == */scripts/verify_stale_release_recovery.py &&
      "${2:-}" == classify-origin-process &&
      "${3:-}" == --old-run-id &&
      "${4:-}" =~ ^[1-9][0-9]*$ && $# == 4 ]]; then
  if [[ "${GITHUB_RUN_ID:-}" == "$4" ]]; then
    printf 'found_origin_process\t%s\n' "$$"
  else
    printf 'no_match\t-\n'
  fi
  exit 0
fi
exec "${TEST_REAL_PYTHON3:?}" "$@"
MOCK
chmod +x "${production_shape}/bin/python3"
cat >"${production_shape}/recovery.sh" <<EOF
# shellcheck source=scripts/lib/public_data_plane_adoption_recovery.sh
source "${ROOT}/scripts/lib/public_data_plane_adoption_recovery.sh"
public_data_plane_adoption_delete_configmap_cas(){
  [[ "\$1" == fugue-fugue-public-data-plane-adoption-recovery && "\$2" == cm-uid && "\$3" =~ ^[0-9]+\$ ]]
  printf 'cas-delete:%s:%s:%s\\n' "\$1" "\$2" "\$3" >>"\${TEST_LOG}"
  : >"\${TEST_DELETED}"
}
EOF
chmod 0600 "${production_shape}/recovery.sh"

same_origin_result="$(
  PATH="${production_shape}/bin:${PATH}" TEST_REAL_PYTHON3="${REAL_PYTHON3}" \
    GITHUB_RUN_ID=123 REPO_ROOT="${ROOT}" FUGUE_UPGRADE_LIB_ONLY=true \
    bash -c '
set -euo pipefail
source "$1"
if control_plane_stale_release_old_process_absent 123; then
  exit 90
fi
printf "reason=%s pid=%s\n" \
  "${CONTROL_PLANE_STALE_RELEASE_ORIGIN_PROCESS_REASON}" \
  "${CONTROL_PLANE_STALE_RELEASE_ORIGIN_PROCESS_PID:--}"
' _ "${ROOT}/scripts/upgrade_fugue_control_plane.sh"
)" || fail "same-origin controlled process did not fail closed"
[[ "${same_origin_result}" =~ ^reason=found_origin_process\ pid=[1-9][0-9]*$ ]] ||
  fail "same-origin controlled process did not report found_origin_process"

set +e
env -u FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_DURATION_SECONDS \
  -u FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_RENEW_SECONDS \
  GITHUB_RUN_ID=124 GITHUB_RUN_ATTEMPT=1 TEST_REAL_PYTHON3="${REAL_PYTHON3}" \
  PATH="${production_shape}/bin:${PATH}" TEST_LOG="${production_shape}/log" \
  TEST_PHASE=aborted-before-apply TEST_HELM_BASE=true TEST_LIVE_EXACT=true \
  TEST_WAL_DIGEST="${WAL_DIGEST}" TEST_WAL_BASELINE_DIGEST='' TEST_BASELINE_DIGEST="${BASELINE_DIGEST}" \
  TEST_APPLY_ATTEMPTS=0 TEST_RESTORE_ATTEMPTS=0 TEST_TOKEN="${TOKEN}" TEST_TOKEN_DIGEST="${TOKEN_DIGEST}" \
  TEST_HEAD_SHA="${HEAD_SHA}" TEST_LEASE_STATE="${production_shape}/lease.json" \
  TEST_LEASE_READ_COUNT="${production_shape}/lease-read-count" TEST_LEASE_DRIFT_MODE=none TEST_PATCH_RV_DRIFT=false \
  TEST_CONFIGMAP_STATE="${production_shape}/configmap.json" TEST_DELETED="${production_shape}/deleted" \
  REPO_ROOT="${ROOT}" FUGUE_RECOVERY_SHA="${HEAD_SHA}" FUGUE_EXPECTED_SOURCE_SHA="${HEAD_SHA}" \
  FUGUE_EXPECTED_WAL_DIGEST="${WAL_DIGEST}" FUGUE_EXPECTED_ORIGIN_RUN_ID=123 \
  FUGUE_PUBLIC_DATA_PLANE_ADOPTION_TOOL="${production_shape}/bin/adoption" \
  FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL="${production_shape}/bin/evidence" \
  FUGUE_RELEASE_DOMAIN_OWNERSHIP_FILE="${production_shape}/ownership.yaml" \
  FUGUE_PUBLIC_DATA_PLANE_ADOPTION_EVIDENCE_DIR="${production_shape}/evidence" \
  FUGUE_PUBLIC_DATA_PLANE_ADOPTION_RECOVERY_LIBRARY="${production_shape}/recovery.sh" \
  HELM="${production_shape}/bin/helm" KUBECTL="${production_shape}/bin/kubectl" \
  bash "${SUBJECT}" >"${production_shape}/out" 2>"${production_shape}/err"
production_status=$?
set -e
[[ "${production_status}" == 0 ]] || { cat "${production_shape}/err" >&2; fail "production-shape aborted cleanup failed"; }
count 0 'patch daemonset' "${production_shape}/log"
count 1 'patch lease/' "${production_shape}/log"
[[ -f "${production_shape}/deleted" ]] || fail "production-shape recovery WAL was not deleted"
STATE="${production_shape}/lease.json" CONFIGMAP="${production_shape}/configmap.json" \
  TERMINAL="${production_shape}/evidence/terminal-wal.json" python3 - <<'PY'
import json, os
value=json.load(open(os.environ["STATE"])); metadata=value["metadata"]; spec=value["spec"]
assert spec["holderIdentity"] == "" and spec["leaseDurationSeconds"] == 120
assert "fugue.pro/coordination-token" not in metadata.get("annotations", {})
assert "fugue.pro/recovery-required" not in metadata.get("annotations", {})
stored=json.load(open(os.environ["CONFIGMAP"]))["data"]["wal.json"]
terminal=open(os.environ["TERMINAL"]).read()
assert stored == terminal and json.loads(terminal)["phase"] == "aborted-before-apply"
PY

production_fence="${TMP}/production-shape-fence"
mkdir -p "${production_fence}/bin" "${production_fence}/evidence"
cp "${production_shape}/bin/"* "${production_fence}/bin/"
cp "${production_shape}/ownership.yaml" "${production_fence}/ownership.yaml"
cp "${production_shape}/lease.initial.json" "${production_fence}/lease.json"
cp "${production_shape}/configmap.initial.json" "${production_fence}/configmap.json"
: >"${production_fence}/log"
STATE="${production_fence}/configmap.json" python3 - <<'PY'
import json, pathlib, os
path=pathlib.Path(os.environ["STATE"]); value=json.loads(path.read_text()); wal=json.loads(value["data"]["wal.json"])
wal["phase"]="fence-armed"; value["data"]["wal.json"]=json.dumps(wal,separators=(",",":"))+"\n"
path.write_text(json.dumps(value,separators=(",",":"))+"\n")
PY
(
  unset FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_DURATION_SECONDS
  unset FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_RENEW_SECONDS
  export GITHUB_RUN_ID=124 GITHUB_RUN_ATTEMPT=1 TEST_REAL_PYTHON3="${REAL_PYTHON3}"
  PATH="${production_fence}/bin:${PATH}" TEST_LOG="${production_fence}/log" \
  TEST_PHASE=fence-armed TEST_HELM_BASE=true TEST_LIVE_EXACT=true \
  TEST_WAL_DIGEST="${WAL_DIGEST}" TEST_WAL_BASELINE_DIGEST='' TEST_BASELINE_DIGEST="${BASELINE_DIGEST}" \
  TEST_APPLY_ATTEMPTS=0 TEST_RESTORE_ATTEMPTS=0 TEST_TOKEN="${TOKEN}" TEST_TOKEN_DIGEST="${TOKEN_DIGEST}" \
  TEST_HEAD_SHA="${HEAD_SHA}" TEST_LEASE_STATE="${production_fence}/lease.json" \
  TEST_LEASE_READ_COUNT="${production_fence}/lease-read-count" TEST_LEASE_DRIFT_MODE=none TEST_PATCH_RV_DRIFT=false \
  TEST_CONFIGMAP_STATE="${production_fence}/configmap.json" TEST_DELETED="${production_fence}/deleted" \
  REPO_ROOT="${ROOT}" FUGUE_RECOVERY_SHA="${HEAD_SHA}" FUGUE_EXPECTED_SOURCE_SHA="${HEAD_SHA}" \
  FUGUE_EXPECTED_WAL_DIGEST="${WAL_DIGEST}" FUGUE_EXPECTED_ORIGIN_RUN_ID=123 \
  FUGUE_PUBLIC_DATA_PLANE_ADOPTION_TOOL="${production_fence}/bin/adoption" \
  FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL="${production_fence}/bin/evidence" \
  FUGUE_RELEASE_DOMAIN_OWNERSHIP_FILE="${production_fence}/ownership.yaml" \
  FUGUE_PUBLIC_DATA_PLANE_ADOPTION_EVIDENCE_DIR="${production_fence}/evidence" \
  FUGUE_PUBLIC_DATA_PLANE_ADOPTION_RECOVERY_LIBRARY="${production_shape}/recovery.sh" \
  HELM="${production_fence}/bin/helm" KUBECTL="${production_fence}/bin/kubectl" \
  bash "${SUBJECT}" >"${production_fence}/out" 2>"${production_fence}/err"
)
count 0 'patch daemonset' "${production_fence}/log"
count 1 'patch configmap/' "${production_fence}/log"
count 1 'patch lease/' "${production_fence}/log"
[[ -f "${production_fence}/deleted" ]] || fail "production-shape fence recovery WAL was not deleted"
STATE="${production_fence}/configmap.json" TERMINAL="${production_fence}/evidence/terminal-wal.json" python3 - <<'PY'
import json, pathlib, os
value=json.load(open(os.environ["STATE"])); stored=value["data"]["wal.json"]; terminal=pathlib.Path(os.environ["TERMINAL"]).read_text()
assert stored == terminal and json.loads(terminal)["phase"] == "aborted-before-apply"
PY

production_residue="${TMP}/production-shape-released-residue"
mkdir -p "${production_residue}/bin" "${production_residue}/evidence"
cp "${production_shape}/bin/"* "${production_residue}/bin/"
cp "${production_shape}/ownership.yaml" "${production_residue}/ownership.yaml"
cp "${production_shape}/lease.initial.json" "${production_residue}/lease.json"
cp "${production_shape}/configmap.initial.json" "${production_residue}/configmap.json"
: >"${production_residue}/log"
STATE="${production_residue}/lease.json" python3 - <<'PY'
import json, pathlib, os
path=pathlib.Path(os.environ["STATE"]); value=json.loads(path.read_text())
value["metadata"]["resourceVersion"]="10"; value["metadata"]["annotations"]={}
value["spec"]["holderIdentity"]=""; value["spec"]["renewTime"]="2026-08-01T00:00:03Z"
path.write_text(json.dumps(value,separators=(",",":"))+"\n")
PY
(
  unset FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_DURATION_SECONDS
  unset FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_RENEW_SECONDS
  export GITHUB_RUN_ID=124 GITHUB_RUN_ATTEMPT=1 TEST_REAL_PYTHON3="${REAL_PYTHON3}"
  PATH="${production_residue}/bin:${PATH}" TEST_LOG="${production_residue}/log" \
  TEST_PHASE=aborted-before-apply TEST_HELM_BASE=true TEST_LIVE_EXACT=true \
  TEST_WAL_DIGEST="${WAL_DIGEST}" TEST_WAL_BASELINE_DIGEST='' TEST_BASELINE_DIGEST="${BASELINE_DIGEST}" \
  TEST_APPLY_ATTEMPTS=0 TEST_RESTORE_ATTEMPTS=0 TEST_TOKEN="${TOKEN}" TEST_TOKEN_DIGEST="${TOKEN_DIGEST}" \
  TEST_HEAD_SHA="${HEAD_SHA}" TEST_LEASE_STATE="${production_residue}/lease.json" \
  TEST_LEASE_READ_COUNT="${production_residue}/lease-read-count" TEST_LEASE_DRIFT_MODE=none TEST_PATCH_RV_DRIFT=false \
  TEST_CONFIGMAP_STATE="${production_residue}/configmap.json" TEST_DELETED="${production_residue}/deleted" \
  REPO_ROOT="${ROOT}" FUGUE_RECOVERY_SHA="${HEAD_SHA}" FUGUE_EXPECTED_SOURCE_SHA="${HEAD_SHA}" \
  FUGUE_EXPECTED_WAL_DIGEST="${WAL_DIGEST}" FUGUE_EXPECTED_ORIGIN_RUN_ID=123 \
  FUGUE_PUBLIC_DATA_PLANE_ADOPTION_TOOL="${production_residue}/bin/adoption" \
  FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL="${production_residue}/bin/evidence" \
  FUGUE_RELEASE_DOMAIN_OWNERSHIP_FILE="${production_residue}/ownership.yaml" \
  FUGUE_PUBLIC_DATA_PLANE_ADOPTION_EVIDENCE_DIR="${production_residue}/evidence" \
  FUGUE_PUBLIC_DATA_PLANE_ADOPTION_RECOVERY_LIBRARY="${production_shape}/recovery.sh" \
  HELM="${production_residue}/bin/helm" KUBECTL="${production_residue}/bin/kubectl" \
  bash "${SUBJECT}" >"${production_residue}/out" 2>"${production_residue}/err"
)
count 0 'patch daemonset' "${production_residue}/log"
count 0 'patch lease/' "${production_residue}/log"
count 0 'tool:wal-advance' "${production_residue}/log"
count 1 'cas-delete:fugue-fugue-public-data-plane-adoption-recovery:cm-uid:7' "${production_residue}/log"
[[ -f "${production_residue}/deleted" ]] || fail "released-Lease terminal residue was not CAS deleted"
STATE="${production_residue}/configmap.json" TERMINAL="${production_residue}/evidence/terminal-wal.json" python3 - <<'PY'
import json, pathlib, os
stored=json.load(open(os.environ["STATE"]))["data"]["wal.json"]; terminal=pathlib.Path(os.environ["TERMINAL"]).read_text()
assert stored == terminal and json.loads(terminal)["phase"] == "aborted-before-apply"
PY

production_residue_nonterminal="${TMP}/production-shape-released-nonterminal"
cp -R "${production_residue}" "${production_residue_nonterminal}"
rm -rf "${production_residue_nonterminal}/evidence"; mkdir "${production_residue_nonterminal}/evidence"
rm -f "${production_residue_nonterminal}/deleted" "${production_residue_nonterminal}/lease-read-count"
: >"${production_residue_nonterminal}/log"
STATE="${production_residue_nonterminal}/configmap.json" python3 - <<'PY'
import json,pathlib,os
path=pathlib.Path(os.environ["STATE"]); value=json.loads(path.read_text()); wal=json.loads(value["data"]["wal.json"])
wal["phase"]="fence-armed"; value["data"]["wal.json"]=json.dumps(wal,separators=(",",":"))+"\n"
path.write_text(json.dumps(value,separators=(",",":"))+"\n")
PY
set +e
GITHUB_RUN_ID=124 GITHUB_RUN_ATTEMPT=1 TEST_REAL_PYTHON3="${REAL_PYTHON3}" \
PATH="${production_residue_nonterminal}/bin:${PATH}" TEST_LOG="${production_residue_nonterminal}/log" \
TEST_PHASE=fence-armed TEST_HELM_BASE=true TEST_LIVE_EXACT=true TEST_WAL_DIGEST="${WAL_DIGEST}" \
TEST_WAL_BASELINE_DIGEST='' TEST_BASELINE_DIGEST="${BASELINE_DIGEST}" TEST_APPLY_ATTEMPTS=0 TEST_RESTORE_ATTEMPTS=0 \
TEST_TOKEN="${TOKEN}" TEST_TOKEN_DIGEST="${TOKEN_DIGEST}" TEST_HEAD_SHA="${HEAD_SHA}" \
TEST_LEASE_STATE="${production_residue_nonterminal}/lease.json" TEST_LEASE_READ_COUNT="${production_residue_nonterminal}/lease-read-count" \
TEST_LEASE_DRIFT_MODE=none TEST_PATCH_RV_DRIFT=false TEST_CONFIGMAP_STATE="${production_residue_nonterminal}/configmap.json" \
TEST_DELETED="${production_residue_nonterminal}/deleted" REPO_ROOT="${ROOT}" FUGUE_RECOVERY_SHA="${HEAD_SHA}" \
FUGUE_EXPECTED_SOURCE_SHA="${HEAD_SHA}" FUGUE_EXPECTED_WAL_DIGEST="${WAL_DIGEST}" FUGUE_EXPECTED_ORIGIN_RUN_ID=123 \
FUGUE_PUBLIC_DATA_PLANE_ADOPTION_TOOL="${production_residue_nonterminal}/bin/adoption" \
FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL="${production_residue_nonterminal}/bin/evidence" \
FUGUE_RELEASE_DOMAIN_OWNERSHIP_FILE="${production_residue_nonterminal}/ownership.yaml" \
FUGUE_PUBLIC_DATA_PLANE_ADOPTION_EVIDENCE_DIR="${production_residue_nonterminal}/evidence" \
FUGUE_PUBLIC_DATA_PLANE_ADOPTION_RECOVERY_LIBRARY="${production_shape}/recovery.sh" \
HELM="${production_residue_nonterminal}/bin/helm" KUBECTL="${production_residue_nonterminal}/bin/kubectl" \
bash "${SUBJECT}" >"${production_residue_nonterminal}/out" 2>"${production_residue_nonterminal}/err"
residue_nonterminal_status=$?
set -e
[[ "${residue_nonterminal_status}" != 0 && ! -f "${production_residue_nonterminal}/deleted" ]] || fail "released Lease accepted a nonterminal WAL"
count 0 'patch lease/' "${production_residue_nonterminal}/log"
count 0 'cas-delete:' "${production_residue_nonterminal}/log"

production_residue_drift="${TMP}/production-shape-released-drift"
cp -R "${production_residue}" "${production_residue_drift}"
rm -rf "${production_residue_drift}/evidence"; mkdir "${production_residue_drift}/evidence"
rm -f "${production_residue_drift}/deleted" "${production_residue_drift}/lease-read-count"
: >"${production_residue_drift}/log"
set +e
GITHUB_RUN_ID=124 GITHUB_RUN_ATTEMPT=1 TEST_REAL_PYTHON3="${REAL_PYTHON3}" \
PATH="${production_residue_drift}/bin:${PATH}" TEST_LOG="${production_residue_drift}/log" \
TEST_PHASE=aborted-before-apply TEST_HELM_BASE=true TEST_LIVE_EXACT=true TEST_WAL_DIGEST="${WAL_DIGEST}" \
TEST_WAL_BASELINE_DIGEST='' TEST_BASELINE_DIGEST="${BASELINE_DIGEST}" TEST_APPLY_ATTEMPTS=0 TEST_RESTORE_ATTEMPTS=0 \
TEST_TOKEN="${TOKEN}" TEST_TOKEN_DIGEST="${TOKEN_DIGEST}" TEST_HEAD_SHA="${HEAD_SHA}" \
TEST_LEASE_STATE="${production_residue_drift}/lease.json" TEST_LEASE_READ_COUNT="${production_residue_drift}/lease-read-count" \
TEST_LEASE_DRIFT_MODE=duration TEST_PATCH_RV_DRIFT=false TEST_CONFIGMAP_STATE="${production_residue_drift}/configmap.json" \
TEST_DELETED="${production_residue_drift}/deleted" REPO_ROOT="${ROOT}" FUGUE_RECOVERY_SHA="${HEAD_SHA}" \
FUGUE_EXPECTED_SOURCE_SHA="${HEAD_SHA}" FUGUE_EXPECTED_WAL_DIGEST="${WAL_DIGEST}" FUGUE_EXPECTED_ORIGIN_RUN_ID=123 \
FUGUE_PUBLIC_DATA_PLANE_ADOPTION_TOOL="${production_residue_drift}/bin/adoption" \
FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL="${production_residue_drift}/bin/evidence" \
FUGUE_RELEASE_DOMAIN_OWNERSHIP_FILE="${production_residue_drift}/ownership.yaml" \
FUGUE_PUBLIC_DATA_PLANE_ADOPTION_EVIDENCE_DIR="${production_residue_drift}/evidence" \
FUGUE_PUBLIC_DATA_PLANE_ADOPTION_RECOVERY_LIBRARY="${production_shape}/recovery.sh" \
HELM="${production_residue_drift}/bin/helm" KUBECTL="${production_residue_drift}/bin/kubectl" \
bash "${SUBJECT}" >"${production_residue_drift}/out" 2>"${production_residue_drift}/err"
residue_drift_status=$?
set -e
[[ "${residue_drift_status}" != 0 && ! -f "${production_residue_drift}/deleted" ]] || fail "released Lease drift was not fail-closed"
count 0 'patch lease/' "${production_residue_drift}/log"
count 0 'cas-delete:' "${production_residue_drift}/log"

production_failure_case() {
  local scenario="$1" duration_mode="$2" drift_mode="${3:-none}" patch_rv_drift="${4:-false}" caller_mode="${5:-none}"
  local dir="${TMP}/production-${scenario}" status
  mkdir -p "${dir}/bin" "${dir}/evidence"
  cp "${production_shape}/bin/"* "${dir}/bin/"
  cp "${production_shape}/ownership.yaml" "${dir}/ownership.yaml"
  cp "${production_shape}/lease.initial.json" "${dir}/lease.json"
  cp "${production_shape}/configmap.initial.json" "${dir}/configmap.json"
  : >"${dir}/log"
  MODE="${duration_mode}" STATE="${dir}/lease.json" python3 - <<'PY'
import json, os, pathlib
path=pathlib.Path(os.environ["STATE"]); value=json.loads(path.read_text()); mode=os.environ["MODE"]
if mode == "valid": pass
elif mode == "missing": value["spec"].pop("leaseDurationSeconds")
elif mode == "zero": value["spec"]["leaseDurationSeconds"] = 0
elif mode == "negative": value["spec"]["leaseDurationSeconds"] = -1
elif mode == "string": value["spec"]["leaseDurationSeconds"] = "120"
elif mode == "overlarge": value["spec"]["leaseDurationSeconds"] = 2147483648
else: raise AssertionError(mode)
path.write_text(json.dumps(value,separators=(",",":"))+"\n")
PY
  set +e
  (
    unset FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_DURATION_SECONDS
    unset FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_RENEW_SECONDS
    export GITHUB_RUN_ID=124 GITHUB_RUN_ATTEMPT=1 TEST_REAL_PYTHON3="${REAL_PYTHON3}"
    case "${caller_mode}" in
      duration) export FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_DURATION_SECONDS=120 ;;
      renew) export FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_RENEW_SECONDS=30 ;;
      none) ;;
      *) exit 97 ;;
    esac
    PATH="${dir}/bin:${PATH}" TEST_LOG="${dir}/log" \
    TEST_PHASE=aborted-before-apply TEST_HELM_BASE=true TEST_LIVE_EXACT=true \
    TEST_WAL_DIGEST="${WAL_DIGEST}" TEST_WAL_BASELINE_DIGEST='' TEST_BASELINE_DIGEST="${BASELINE_DIGEST}" \
    TEST_APPLY_ATTEMPTS=0 TEST_RESTORE_ATTEMPTS=0 TEST_TOKEN="${TOKEN}" TEST_TOKEN_DIGEST="${TOKEN_DIGEST}" \
    TEST_HEAD_SHA="${HEAD_SHA}" TEST_LEASE_STATE="${dir}/lease.json" TEST_LEASE_READ_COUNT="${dir}/lease-read-count" \
    TEST_LEASE_DRIFT_MODE="${drift_mode}" TEST_PATCH_RV_DRIFT="${patch_rv_drift}" \
    TEST_CONFIGMAP_STATE="${dir}/configmap.json" TEST_DELETED="${dir}/deleted" \
    REPO_ROOT="${ROOT}" FUGUE_RECOVERY_SHA="${HEAD_SHA}" FUGUE_EXPECTED_SOURCE_SHA="${HEAD_SHA}" \
    FUGUE_EXPECTED_WAL_DIGEST="${WAL_DIGEST}" FUGUE_EXPECTED_ORIGIN_RUN_ID=123 \
    FUGUE_PUBLIC_DATA_PLANE_ADOPTION_TOOL="${dir}/bin/adoption" FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL="${dir}/bin/evidence" \
    FUGUE_RELEASE_DOMAIN_OWNERSHIP_FILE="${dir}/ownership.yaml" FUGUE_PUBLIC_DATA_PLANE_ADOPTION_EVIDENCE_DIR="${dir}/evidence" \
    FUGUE_PUBLIC_DATA_PLANE_ADOPTION_RECOVERY_LIBRARY="${production_shape}/recovery.sh" \
    HELM="${dir}/bin/helm" KUBECTL="${dir}/bin/kubectl" bash "${SUBJECT}" >"${dir}/out" 2>"${dir}/err"
  )
  status=$?; set -e
  [[ "${status}" != 0 ]] || fail "${scenario}: unsafe recovery cleanup succeeded"
  [[ ! -f "${dir}/deleted" ]] || fail "${scenario}: recovery WAL was deleted"
  count 0 'patch daemonset' "${dir}/log"
  if [[ "${patch_rv_drift}" == true ]]; then
    count 1 'patch lease/' "${dir}/log"
  else
    count 0 'patch lease/' "${dir}/log"
  fi
  [[ "$(shasum -a 256 "${dir}/configmap.json" | awk '{print $1}')" == \
    "$(shasum -a 256 "${production_shape}/configmap.initial.json" | awk '{print $1}')" ]] || fail "${scenario}: recovery WAL drifted"
  STATE="${dir}/lease.json" python3 - <<'PY'
import json, os
value=json.load(open(os.environ["STATE"])); metadata=value["metadata"]; spec=value["spec"]
assert spec.get("holderIdentity")
assert metadata.get("annotations", {}).get("fugue.pro/coordination-token")
assert metadata.get("annotations", {}).get("fugue.pro/recovery-required") == "true"
PY
}

production_failure_case duration-missing missing
production_failure_case duration-zero zero
production_failure_case duration-negative negative
production_failure_case duration-string string
production_failure_case duration-overlarge overlarge
production_failure_case duration-two-read-drift valid duration
production_failure_case owner-two-read-drift valid owner
production_failure_case token-two-read-drift valid token
production_failure_case rv-apply-drift valid none true
production_failure_case caller-duration-rejected valid none false duration
production_failure_case caller-renew-rejected valid none false renew

printf 'public data-plane adoption cross-process recovery matrix passed\n'
