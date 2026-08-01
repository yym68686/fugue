#!/usr/bin/env bash

# Durable, bounded recovery WAL for the dedicated public data-plane Helm
# adoption. Callers must already hold the shared coordination Lease.

public_data_plane_adoption_recovery_now() {
  python3 -c 'import datetime; print(datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"))'
}

public_data_plane_adoption_recovery_cm_name() {
  printf '%s-public-data-plane-adoption-recovery' "${RELEASE_FULLNAME}"
}

public_data_plane_adoption_verify_owned_lease() {
  local expected_recovery="${1:-true}" lease_json
  lease_json="$(${KUBECTL} -n "${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAMESPACE}" \
    get "lease/${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAME}" -o json)" || return 1
  LEASE_JSON="${lease_json}" EXPECTED_OWNER="${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_OWNER}" \
    EXPECTED_TOKEN="${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_TOKEN}" EXPECTED_RECOVERY="${expected_recovery}" python3 - <<'PY'
import json, os
value = json.loads(os.environ["LEASE_JSON"])
metadata = value.get("metadata") or {}
annotations = metadata.get("annotations") or {}
spec = value.get("spec") or {}
assert spec.get("holderIdentity") == os.environ["EXPECTED_OWNER"]
assert annotations.get("fugue.pro/coordination-token") == os.environ["EXPECTED_TOKEN"]
if os.environ["EXPECTED_RECOVERY"] == "true":
    assert annotations.get("fugue.pro/recovery-required") == "true"
else:
    assert "fugue.pro/recovery-required" not in annotations
assert not metadata.get("deletionTimestamp")
assert metadata.get("uid") and metadata.get("resourceVersion")
PY
}

public_data_plane_adoption_extract_recovery_configmap() {
  local configmap_json="$1" output_dir="$2"
  CONFIGMAP_JSON="${configmap_json}" OUTPUT_DIR="${output_dir}" \
    EXPECTED_NAME="$(public_data_plane_adoption_recovery_cm_name)" \
    EXPECTED_NAMESPACE="${RELEASE_NAMESPACE}" python3 - <<'PY'
import json, os, pathlib
value = json.loads(os.environ["CONFIGMAP_JSON"])
metadata = value.get("metadata") or {}
labels = metadata.get("labels") or {}
assert value.get("apiVersion") == "v1" and value.get("kind") == "ConfigMap"
assert metadata.get("name") == os.environ["EXPECTED_NAME"]
assert metadata.get("namespace") == os.environ["EXPECTED_NAMESPACE"]
assert not metadata.get("deletionTimestamp")
assert metadata.get("uid") and metadata.get("resourceVersion")
assert labels == {
    "app.kubernetes.io/instance": os.environ.get("RELEASE_NAME", "fugue"),
    "app.kubernetes.io/component": "public-data-plane-adoption-recovery",
    "fugue.io/recovery-policy": "public-data-plane-helm-adoption-v1",
}
data = value.get("data") or {}
assert set(data) == {"wal.json", "transaction.json", "restore.json"}
root = pathlib.Path(os.environ["OUTPUT_DIR"])
root.mkdir(mode=0o700, parents=True, exist_ok=True)
for name, content in data.items():
    assert isinstance(content, str) and 0 < len(content.encode()) <= 98304
    path = root / name
    path.write_text(content, encoding="utf-8")
    path.chmod(0o600)
(root / "configmap-resource-version").write_text(metadata["resourceVersion"], encoding="utf-8")
(root / "configmap-resource-version").chmod(0o600)
(root / "configmap-uid").write_text(metadata["uid"], encoding="utf-8")
(root / "configmap-uid").chmod(0o600)
PY
}

public_data_plane_adoption_delete_configmap_cas() (
  set -euo pipefail
  local cm_name="$1" uid="$2" resource_version="$3"
  local kubeconfig="${KUBECONFIG:?explicit KUBECONFIG is required for ConfigMap CAS deletion}"
  local api_path proxy_root proxy_dir proxy_socket proxy_log body response reconcile classification curl_path
  local proxy_pid="" curl_rc=0 http_code="" reconcile_rc=0 reconcile_http="" attempt

  [[ "${RELEASE_NAMESPACE}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ && ${#RELEASE_NAMESPACE} -le 63 ]]
  [[ "${cm_name}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ && ${#cm_name} -le 253 ]]
  [[ "${uid}" =~ ^[A-Za-z0-9_-]+$ && ${#uid} -le 253 ]]
  [[ "${resource_version}" =~ ^[A-Za-z0-9._:-]+$ && ${#resource_version} -le 253 ]]
  [[ "${kubeconfig}" == /* && -f "${kubeconfig}" && ! -L "${kubeconfig}" && -r "${kubeconfig}" ]]
  KUBECONFIG_PATH="${kubeconfig}" python3 - <<'PY'
import os, pathlib, stat
path=pathlib.Path(os.environ["KUBECONFIG_PATH"]); info=path.stat()
assert stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) & 0o077 == 0
assert 0 < info.st_size <= 1024 * 1024
PY
  curl_path="$(command -v curl)"
  [[ "${curl_path}" == /* && -x "${curl_path}" ]]

  api_path="/api/v1/namespaces/${RELEASE_NAMESPACE}/configmaps/${cm_name}"
  proxy_root="${RUNNER_TEMP:-/tmp}"
  [[ "${proxy_root}" == /* && -d "${proxy_root}" ]]
  proxy_dir="$(mktemp -d "${proxy_root}/fugue-pdp-cas.XXXXXX")"
  chmod 0700 "${proxy_dir}"
  proxy_socket="${proxy_dir}/proxy.sock"
  SOCKET_PATH="${proxy_socket}" python3 -c 'import os; assert len(os.fsencode(os.environ["SOCKET_PATH"])) <= 100'
  proxy_log="${proxy_dir}/proxy.log"
  body="${proxy_dir}/delete-options.json"
  response="${proxy_dir}/delete-response.json"
  reconcile="${proxy_dir}/reconcile.json"
  classification="${proxy_dir}/classification"
  : >"${proxy_log}"; : >"${response}"; : >"${reconcile}"
  chmod 0600 "${proxy_log}" "${response}" "${reconcile}"

  cleanup_proxy() {
    local stop_attempt
    if [[ -n "${proxy_pid}" ]]; then
      kill -TERM "${proxy_pid}" >/dev/null 2>&1 || true
      for stop_attempt in $(seq 1 50); do
        kill -0 "${proxy_pid}" >/dev/null 2>&1 || break
        sleep 0.1
      done
      if kill -0 "${proxy_pid}" >/dev/null 2>&1; then
        kill -KILL "${proxy_pid}" >/dev/null 2>&1 || true
      fi
      wait "${proxy_pid}" >/dev/null 2>&1 || true
      proxy_pid=""
    fi
    rm -rf -- "${proxy_dir}"
  }
  trap cleanup_proxy EXIT
  trap 'exit 129' HUP
  trap 'exit 130' INT
  trap 'exit 143' TERM

  UID_VALUE="${uid}" RV_VALUE="${resource_version}" OUTPUT="${body}" python3 - <<'PY'
import json, os, pathlib
value={"apiVersion":"v1","kind":"DeleteOptions","preconditions":{"uid":os.environ["UID_VALUE"],"resourceVersion":os.environ["RV_VALUE"]}}
path=pathlib.Path(os.environ["OUTPUT"]); path.write_text(json.dumps(value,sort_keys=True,separators=(",",":"))+"\n",encoding="utf-8"); path.chmod(0o600)
PY

  HTTP_PROXY= HTTPS_PROXY= ALL_PROXY= http_proxy= https_proxy= all_proxy= \
    NO_PROXY='*' no_proxy='*' KUBECONFIG="${kubeconfig}" ${KUBECTL} proxy \
    --unix-socket="${proxy_socket}" --api-prefix=/ \
    --accept-hosts='^localhost$' --accept-paths="^${api_path}$" \
    --reject-methods='^(HEAD|POST|PUT|PATCH|OPTIONS|CONNECT|TRACE)$' \
    --www='' >"${proxy_log}" 2>&1 &
  proxy_pid=$!
  for attempt in $(seq 1 50); do
    [[ -S "${proxy_socket}" ]] && break
    kill -0 "${proxy_pid}" >/dev/null 2>&1 || return 1
    sleep 0.1
  done
  [[ -S "${proxy_socket}" && ! -L "${proxy_socket}" ]]
  chmod 0700 "${proxy_socket}"
  SOCKET_PATH="${proxy_socket}" python3 - <<'PY'
import os, pathlib, stat
info=pathlib.Path(os.environ["SOCKET_PATH"]).stat()
assert stat.S_ISSOCK(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o700
PY

  if http_code="$(HTTP_PROXY= HTTPS_PROXY= ALL_PROXY= http_proxy= https_proxy= all_proxy= \
    NO_PROXY='*' no_proxy='*' "${curl_path}" --disable --noproxy '*' --silent --show-error \
    --connect-timeout 5 --max-time 15 --max-filesize 1048576 \
    --unix-socket "${proxy_socket}" --request DELETE \
    --header 'Content-Type: application/json' --data-binary "@${body}" \
    --output "${response}" --write-out '%{http_code}' "http://localhost${api_path}")"; then
    curl_rc=0
  else
    curl_rc=$?
  fi
  [[ "${http_code}" =~ ^[0-9]{3}$ ]]

  RESPONSE="${response}" HTTP_CODE="${http_code}" CURL_RC="${curl_rc}" OUTPUT="${classification}" python3 - <<'PY'
import json, os, pathlib
def unique(pairs):
    value={}
    for key,item in pairs:
        if key in value: raise ValueError("duplicate JSON key")
        value[key]=item
    return value
path=pathlib.Path(os.environ["RESPONSE"]); raw=path.read_bytes(); assert len(raw) <= 1024*1024
code=int(os.environ["HTTP_CODE"]); curl_rc=int(os.environ["CURL_RC"]); classification=""
if not raw:
    assert curl_rc != 0 and code == 0
    classification="ambiguous"
else:
    value=json.loads(raw, object_pairs_hook=unique)
    assert isinstance(value,dict) and value.get("apiVersion")=="v1" and value.get("kind")=="Status"
    assert type(value.get("code")) is int and value["code"]==code
    if 200 <= code <= 299:
        assert curl_rc==0 and value.get("status")=="Success"
        classification="success"
    elif code==404:
        assert curl_rc==0 and value.get("status")=="Failure" and value.get("reason")=="NotFound"
        classification="not-found"
    elif code==409:
        raise SystemExit(1)
    elif 500 <= code <= 599:
        assert curl_rc==0 and value.get("status")=="Failure"
        classification="ambiguous"
    else:
        raise SystemExit(1)
out=pathlib.Path(os.environ["OUTPUT"]); out.write_text(classification+"\n",encoding="utf-8"); out.chmod(0o600)
PY

  if reconcile_http="$(HTTP_PROXY= HTTPS_PROXY= ALL_PROXY= http_proxy= https_proxy= all_proxy= \
    NO_PROXY='*' no_proxy='*' "${curl_path}" --disable --noproxy '*' --silent --show-error \
    --connect-timeout 5 --max-time 15 --max-filesize 1048576 \
    --unix-socket "${proxy_socket}" --request GET --header 'Accept: application/json' \
    --output "${reconcile}" --write-out '%{http_code}' "http://localhost${api_path}")"; then
    reconcile_rc=0
  else
    reconcile_rc=$?
  fi
  RECONCILE="${reconcile}" HTTP_CODE="${reconcile_http}" CURL_RC="${reconcile_rc}" python3 - <<'PY'
import json, os, pathlib
def unique(pairs):
    value={}
    for key,item in pairs:
        if key in value: raise ValueError("duplicate JSON key")
        value[key]=item
    return value
assert int(os.environ["CURL_RC"])==0 and os.environ["HTTP_CODE"]=="404"
path=pathlib.Path(os.environ["RECONCILE"]); raw=path.read_bytes(); assert 0 < len(raw) <= 1024*1024
value=json.loads(raw,object_pairs_hook=unique)
assert isinstance(value,dict) and value.get("apiVersion")=="v1" and value.get("kind")=="Status"
assert value.get("status")=="Failure" and value.get("reason")=="NotFound" and value.get("code")==404
PY
  [[ "$(cat "${classification}")" =~ ^(success|not-found|ambiguous)$ ]]
)

public_data_plane_adoption_persist_recovery_wal() {
  local cm_name transaction restore wal payload created readback
  cm_name="$(public_data_plane_adoption_recovery_cm_name)"
  transaction="${EVIDENCE_DIR}/transaction.json"
  restore="${EVIDENCE_DIR}/restore.json"
  wal="${EVIDENCE_DIR}/recovery-wal.json"
  public_data_plane_adoption_verify_owned_lease false || return 1
  if ${KUBECTL} -n "${RELEASE_NAMESPACE}" get "configmap/${cm_name}" -o name >/dev/null 2>&1; then
    return 1
  fi
  "${ADOPTION_TOOL}" wal-init \
    --transaction "${transaction}" --restore "${restore}" --wal "${wal}" \
    --lease-namespace "${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAMESPACE}" \
    --lease-name "${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAME}" \
    --lease-owner "${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_OWNER}" \
    --lease-token "${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_TOKEN}" \
    --origin-run-id "${GITHUB_RUN_ID}" --origin-run-attempt "${GITHUB_RUN_ATTEMPT:-1}" \
    --at "$(public_data_plane_adoption_recovery_now)" >/dev/null || return 1
  payload="${EVIDENCE_DIR}/recovery-configmap.json"
  TRANSACTION="${transaction}" RESTORE="${restore}" WAL="${wal}" OUTPUT="${payload}" \
    NAME="${cm_name}" NAMESPACE="${RELEASE_NAMESPACE}" RELEASE_NAME="${RELEASE_NAME}" python3 - <<'PY'
import json, os, pathlib
read = lambda key: pathlib.Path(os.environ[key]).read_text(encoding="utf-8")
value = {
  "apiVersion": "v1", "kind": "ConfigMap",
  "metadata": {
    "name": os.environ["NAME"], "namespace": os.environ["NAMESPACE"],
    "labels": {
      "app.kubernetes.io/instance": os.environ["RELEASE_NAME"],
      "app.kubernetes.io/component": "public-data-plane-adoption-recovery",
      "fugue.io/recovery-policy": "public-data-plane-helm-adoption-v1",
    },
  },
  "immutable": False,
  "data": {"wal.json": read("WAL"), "transaction.json": read("TRANSACTION"), "restore.json": read("RESTORE")},
}
encoded = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
assert len(encoded) <= 128 * 1024
pathlib.Path(os.environ["OUTPUT"]).write_bytes(encoded)
pathlib.Path(os.environ["OUTPUT"]).chmod(0o600)
PY
  created="$(${KUBECTL} create -f "${payload}" -o json)" || return 1
  readback="$(${KUBECTL} -n "${RELEASE_NAMESPACE}" get "configmap/${cm_name}" -o json)" || return 1
  rm -rf "${EVIDENCE_DIR}/wal-readback"
  RELEASE_NAME="${RELEASE_NAME}" public_data_plane_adoption_extract_recovery_configmap "${readback}" "${EVIDENCE_DIR}/wal-readback" || return 1
  cmp -s "${transaction}" "${EVIDENCE_DIR}/wal-readback/transaction.json" || return 1
  cmp -s "${restore}" "${EVIDENCE_DIR}/wal-readback/restore.json" || return 1
  cmp -s "${wal}" "${EVIDENCE_DIR}/wal-readback/wal.json" || return 1
  "${ADOPTION_TOOL}" wal-verify --transaction "${transaction}" --restore "${restore}" --wal "${wal}" >/dev/null || return 1
  public_data_plane_adoption_verify_owned_lease false
}

public_data_plane_adoption_advance_recovery_wal() {
  local phase="$1" baseline_digest="${2:-}" cm_name current rv patch readback
  local -a command
  cm_name="$(public_data_plane_adoption_recovery_cm_name)"
  public_data_plane_adoption_verify_owned_lease || return 1
  current="$(${KUBECTL} -n "${RELEASE_NAMESPACE}" get "configmap/${cm_name}" -o json)" || return 1
  rm -rf "${EVIDENCE_DIR}/wal-current"
  RELEASE_NAME="${RELEASE_NAME}" public_data_plane_adoption_extract_recovery_configmap "${current}" "${EVIDENCE_DIR}/wal-current" || return 1
  rv="$(cat "${EVIDENCE_DIR}/wal-current/configmap-resource-version")"
  command=("${ADOPTION_TOOL}" wal-advance \
    --transaction "${EVIDENCE_DIR}/wal-current/transaction.json" \
    --restore "${EVIDENCE_DIR}/wal-current/restore.json" \
    --wal "${EVIDENCE_DIR}/wal-current/wal.json" \
    --lease-owner "${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_OWNER}" \
    --lease-token "${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_TOKEN}" \
    --phase "${phase}" --at "$(public_data_plane_adoption_recovery_now)")
  if [[ -n "${baseline_digest}" ]]; then
    command+=(--baseline-digest "${baseline_digest}")
  fi
  "${command[@]}" >/dev/null || return 1
  cp "${EVIDENCE_DIR}/wal-current/wal.json" "${EVIDENCE_DIR}/recovery-wal.json"
  patch="$(RV="${rv}" WAL_FILE="${EVIDENCE_DIR}/recovery-wal.json" python3 - <<'PY'
import json, os, pathlib
print(json.dumps([
  {"op":"test","path":"/metadata/resourceVersion","value":os.environ["RV"]},
  {"op":"replace","path":"/data/wal.json","value":pathlib.Path(os.environ["WAL_FILE"]).read_text(encoding="utf-8")},
], separators=(",", ":")))
PY
)" || return 1
  ${KUBECTL} -n "${RELEASE_NAMESPACE}" patch "configmap/${cm_name}" --type=json -p "${patch}" >/dev/null || return 1
  readback="$(${KUBECTL} -n "${RELEASE_NAMESPACE}" get "configmap/${cm_name}" -o json)" || return 1
  rm -rf "${EVIDENCE_DIR}/wal-readback"
  RELEASE_NAME="${RELEASE_NAME}" public_data_plane_adoption_extract_recovery_configmap "${readback}" "${EVIDENCE_DIR}/wal-readback" || return 1
  cmp -s "${EVIDENCE_DIR}/recovery-wal.json" "${EVIDENCE_DIR}/wal-readback/wal.json" || return 1
  "${ADOPTION_TOOL}" wal-verify \
    --transaction "${EVIDENCE_DIR}/wal-readback/transaction.json" \
    --restore "${EVIDENCE_DIR}/wal-readback/restore.json" \
    --wal "${EVIDENCE_DIR}/wal-readback/wal.json" >/dev/null || return 1
  public_data_plane_adoption_verify_owned_lease
}

public_data_plane_adoption_delete_terminal_wal() {
  local cm_name current uid rv sealed_digest final_digest
  cm_name="$(public_data_plane_adoption_recovery_cm_name)"
  [[ -f "${EVIDENCE_DIR}/terminal-wal.json" && ! -L "${EVIDENCE_DIR}/terminal-wal.json" ]] || return 1
  sealed_digest="$(shasum -a 256 "${EVIDENCE_DIR}/terminal-wal.json" | awk '{print $1}')" || return 1
  current="$(${KUBECTL} -n "${RELEASE_NAMESPACE}" get "configmap/${cm_name}" -o json)" || return 1
  rm -rf "${EVIDENCE_DIR}/wal-terminal"
  RELEASE_NAME="${RELEASE_NAME}" public_data_plane_adoption_extract_recovery_configmap "${current}" "${EVIDENCE_DIR}/wal-terminal" || return 1
  "${ADOPTION_TOOL}" wal-verify \
    --transaction "${EVIDENCE_DIR}/wal-terminal/transaction.json" \
    --restore "${EVIDENCE_DIR}/wal-terminal/restore.json" \
    --wal "${EVIDENCE_DIR}/wal-terminal/wal.json" >/dev/null || return 1
  cmp -s "${EVIDENCE_DIR}/terminal-wal.json" "${EVIDENCE_DIR}/wal-terminal/wal.json" || return 1
  WAL="${EVIDENCE_DIR}/wal-terminal/wal.json" python3 - <<'PY'
import json, os
with open(os.environ["WAL"], encoding="utf-8") as source: value=json.load(source)
assert value["phase"] in {"baseline-finalized", "restore-succeeded", "aborted-before-apply"}
PY
  rv="$(cat "${EVIDENCE_DIR}/wal-terminal/configmap-resource-version")"
  uid="$(cat "${EVIDENCE_DIR}/wal-terminal/configmap-uid")"
  public_data_plane_adoption_delete_configmap_cas "${cm_name}" "${uid}" "${rv}" || return 1
  "${ADOPTION_TOOL}" wal-verify \
    --transaction "${EVIDENCE_DIR}/transaction.json" \
    --restore "${EVIDENCE_DIR}/restore.json" \
    --wal "${EVIDENCE_DIR}/terminal-wal.json" >/dev/null || return 1
  final_digest="$(shasum -a 256 "${EVIDENCE_DIR}/terminal-wal.json" | awk '{print $1}')" || return 1
  [[ "${final_digest}" == "${sealed_digest}" ]]
}

public_data_plane_adoption_seal_terminal_wal() {
  local expected_phase="$1" lease_state="${2:-held}" cm_name current
  cm_name="$(public_data_plane_adoption_recovery_cm_name)"
  case "${lease_state}" in
    held) public_data_plane_adoption_verify_owned_lease || return 1 ;;
    released) verify_released_recovery_lease || return 1 ;;
    *) return 1 ;;
  esac
  current="$(${KUBECTL} -n "${RELEASE_NAMESPACE}" get "configmap/${cm_name}" -o json)" || return 1
  rm -rf "${EVIDENCE_DIR}/wal-terminal-seal"
  RELEASE_NAME="${RELEASE_NAME}" public_data_plane_adoption_extract_recovery_configmap \
    "${current}" "${EVIDENCE_DIR}/wal-terminal-seal" || return 1
  "${ADOPTION_TOOL}" wal-verify \
    --transaction "${EVIDENCE_DIR}/wal-terminal-seal/transaction.json" \
    --restore "${EVIDENCE_DIR}/wal-terminal-seal/restore.json" \
    --wal "${EVIDENCE_DIR}/wal-terminal-seal/wal.json" >/dev/null || return 1
  WAL="${EVIDENCE_DIR}/wal-terminal-seal/wal.json" EXPECTED_PHASE="${expected_phase}" python3 - <<'PY'
import json, os
with open(os.environ["WAL"], encoding="utf-8") as source: value=json.load(source)
assert value["phase"] == os.environ["EXPECTED_PHASE"]
assert value["phase"] in {"baseline-finalized", "restore-succeeded", "aborted-before-apply"}
PY
  cp "${EVIDENCE_DIR}/wal-terminal-seal/wal.json" "${EVIDENCE_DIR}/terminal-wal.json"
  chmod 0600 "${EVIDENCE_DIR}/terminal-wal.json"
}

public_data_plane_adoption_delete_unarmed_wal() {
  local expected_recovery="${1:-false}" cm_name current uid rv
  cm_name="$(public_data_plane_adoption_recovery_cm_name)"
  public_data_plane_adoption_verify_owned_lease "${expected_recovery}" || return 1
  current="$(${KUBECTL} -n "${RELEASE_NAMESPACE}" get "configmap/${cm_name}" -o json)" || return 1
  rm -rf "${EVIDENCE_DIR}/wal-unarmed"
  RELEASE_NAME="${RELEASE_NAME}" public_data_plane_adoption_extract_recovery_configmap "${current}" "${EVIDENCE_DIR}/wal-unarmed" || return 1
  "${ADOPTION_TOOL}" wal-verify \
    --transaction "${EVIDENCE_DIR}/wal-unarmed/transaction.json" \
    --restore "${EVIDENCE_DIR}/wal-unarmed/restore.json" \
    --wal "${EVIDENCE_DIR}/wal-unarmed/wal.json" >/dev/null || return 1
  WAL="${EVIDENCE_DIR}/wal-unarmed/wal.json" python3 - <<'PY'
import json, os
with open(os.environ["WAL"], encoding="utf-8") as source: value=json.load(source)
assert value["phase"] == "lease-acquired"
PY
  rv="$(cat "${EVIDENCE_DIR}/wal-unarmed/configmap-resource-version")"
  uid="$(cat "${EVIDENCE_DIR}/wal-unarmed/configmap-uid")"
  public_data_plane_adoption_delete_configmap_cas "${cm_name}" "${uid}" "${rv}"
}
