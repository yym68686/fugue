#!/usr/bin/env bash

# shellcheck shell=bash

edge_activation_log() { printf '[fugue-edge-activation] %s\n' "$*"; }
edge_activation_error() { printf '[fugue-edge-activation] ERROR: %s\n' "$*" >&2; }

edge_activation_init() {
  [[ "${FUGUE_EDGE_ACTIVATION_ENABLED:-false}" == "true" ]] || return 0
  : "${FUGUE_EDGE_ACTIVATION_API_URL:?FUGUE_EDGE_ACTIVATION_API_URL is required}"
  : "${FUGUE_EDGE_ACTIVATION_API_KEY:?FUGUE_EDGE_ACTIVATION_API_KEY is required}"
  : "${FUGUE_EDGE_ACTIVATION_SIGNER_POD:?FUGUE_EDGE_ACTIVATION_SIGNER_POD is required}"
  : "${FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_UID:?FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_UID is required}"
  : "${FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_VERSION:?FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_VERSION is required}"
  [[ "${FUGUE_EDGE_ACTIVATION_API_URL}" =~ ^https://[^/]+(/.*)?$ ]] || {
    edge_activation_error "activation API URL must use HTTPS"
    return 1
  }
  : "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
  : "${GITHUB_RUN_ID:?GITHUB_RUN_ID is required}"
  : "${GITHUB_RUN_ATTEMPT:?GITHUB_RUN_ATTEMPT is required}"
  : "${GITHUB_SHA:?GITHUB_SHA is required}"
  FUGUE_EDGE_ACTIVATION_RELEASE_FENCE="github:${GITHUB_REPOSITORY}:${GITHUB_RUN_ID}:${GITHUB_RUN_ATTEMPT}:${GITHUB_SHA}"
  [[ "${FUGUE_EDGE_ACTIVATION_RELEASE_FENCE}" =~ ^github:[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+:[1-9][0-9]*:[1-9][0-9]*:[0-9a-f]{40}$ ]] || {
    edge_activation_error "activation release fence is invalid"
    return 1
  }
  [[ "${FUGUE_EDGE_ACTIVATION_API_KEY}" =~ ^[A-Za-z0-9._-]+$ && ${#FUGUE_EDGE_ACTIVATION_API_KEY} -ge 20 && ${#FUGUE_EDGE_ACTIVATION_API_KEY} -le 512 ]] || {
    edge_activation_error "activation API key format is invalid"
    return 1
  }
  FUGUE_EDGE_ACTIVATION_DIR="${FUGUE_EDGE_ACTIVATION_DIR:-$(mktemp -d)}"
  install -d -m 700 "${FUGUE_EDGE_ACTIVATION_DIR}"
  FUGUE_EDGE_ACTIVATION_CURL_CONFIG="${FUGUE_EDGE_ACTIVATION_DIR}/curl.conf"
  (
    umask 077
    printf 'silent\nshow-error\nconnect-timeout = 5\nmax-time = 20\nproto = "=https"\nheader = "Authorization: Bearer %s"\nheader = "Content-Type: application/json"\n' \
      "${FUGUE_EDGE_ACTIVATION_API_KEY}" >"${FUGUE_EDGE_ACTIVATION_CURL_CONFIG}"
  )
  chmod 600 "${FUGUE_EDGE_ACTIVATION_CURL_CONFIG}"
  unset FUGUE_EDGE_ACTIVATION_API_KEY
  export FUGUE_EDGE_ACTIVATION_DIR FUGUE_EDGE_ACTIVATION_CURL_CONFIG FUGUE_EDGE_ACTIVATION_RELEASE_FENCE
}

edge_activation_cleanup() {
  if [[ -n "${FUGUE_EDGE_ACTIVATION_DIR:-}" && -d "${FUGUE_EDGE_ACTIVATION_DIR}" ]]; then
    rm -rf -- "${FUGUE_EDGE_ACTIVATION_DIR}"
  fi
  FUGUE_EDGE_ACTIVATION_DIR=""
  FUGUE_EDGE_ACTIVATION_CURL_CONFIG=""
}

edge_activation_sign_request() {
  local mode="$1"
  local request="$2"
  local signed="${request}.signed"
  local command
  case "${mode}" in
    activation) command=sign-edge-activation ;;
    remediation) command=sign-edge-remediation ;;
    *) edge_activation_error "unknown signing mode"; return 1 ;;
  esac
  install -m 600 /dev/null "${signed}"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" exec "pod/${FUGUE_EDGE_ACTIVATION_SIGNER_POD}" -c api -- /usr/local/bin/fugue-api "${command}" <"${request}" >"${signed}" || return 1
  [[ -s "${signed}" ]] || return 1
  mv -f -- "${signed}" "${request}"
  chmod 600 "${request}"
}

edge_activation_endpoint() {
  printf '%s/v1/admin/edge/activation' "${FUGUE_EDGE_ACTIVATION_API_URL%/}"
}

edge_activation_get() {
  local output="$1"
  local status
  install -m 600 /dev/null "${output}"
  status="$(curl --config "${FUGUE_EDGE_ACTIVATION_CURL_CONFIG}" --request GET \
    --output "${output}" --write-out '%{http_code}' "$(edge_activation_endpoint)")" || return 1
  [[ "${status}" == "200" ]] || {
    edge_activation_error "activation inventory returned HTTP ${status}"
    return 1
  }
  python3 - "${output}" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as handle:
    value = json.load(handle)
if not isinstance(value, dict) or not isinstance(value.get("activation"), dict):
    raise SystemExit("activation inventory is invalid")
PY
}

edge_activation_state_field() {
  local inventory="$1"
  local field="$2"
  python3 - "${inventory}" "${field}" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as handle:
    value = json.load(handle)["activation"].get(sys.argv[2])
if isinstance(value, bool) or not isinstance(value, (str, int)):
    raise SystemExit("activation field is invalid")
print(value)
PY
}

edge_activation_state_matches_transaction() {
  local inventory="$1"
  local phase="$2"
  EXPECTED_PHASE="${phase}" EXPECTED_PLAN="${FUGUE_EDGE_ACTIVATION_PLAN_DIGEST}" EXPECTED_RELEASE="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID}" EXPECTED_RECORD_UID="${FUGUE_EDGE_ACTIVATION_RECORD_UID}" EXPECTED_RECORD_VERSION="${FUGUE_EDGE_ACTIVATION_RECORD_VERSION}" EXPECTED_RECORD_DIGEST="${FUGUE_EDGE_ACTIVATION_RECORD_DIGEST}" python3 - "${inventory}" <<'PY'
import json,os,sys
with open(sys.argv[1],encoding="utf-8") as handle: value=json.load(handle)["activation"]
expected={"phase":os.environ["EXPECTED_PHASE"],"plan_digest":os.environ["EXPECTED_PLAN"],"release_id":os.environ["EXPECTED_RELEASE"],"release_record_uid":os.environ["EXPECTED_RECORD_UID"],"release_record_version":os.environ["EXPECTED_RECORD_VERSION"],"release_record_digest":os.environ["EXPECTED_RECORD_DIGEST"]}
if any(str(value.get(key) or "") != wanted for key,wanted in expected.items()):
    raise SystemExit("activation state belongs to another transaction")
PY
}

edge_activation_advance() {
  local phase="$1"
  local expected_file="${2:-}"
  local epochs_file="${3:-}"
  local api_generation="${4:-}"
  local before="${FUGUE_EDGE_ACTIVATION_DIR}/before-${phase}.json"
  local request="${FUGUE_EDGE_ACTIVATION_DIR}/request-${phase}.json"
  local response="${FUGUE_EDGE_ACTIVATION_DIR}/response-${phase}.json"
  local status=""
  local reconcile_phase="${phase}"
  edge_activation_get "${before}" || return 1
  if [[ "${phase}" == "rollback" ]]; then
    reconcile_phase="$(python3 - "${before}" <<'PY'
import json,sys
with open(sys.argv[1],encoding="utf-8") as h: value=json.load(h)["activation"]
rollback=value.get("rollback") or {}; phase=rollback.get("phase")
if not isinstance(phase,str) or not phase: raise SystemExit("activation has no rollback snapshot")
print(phase)
PY
)" || return 1
  fi
  if [[ "$(edge_activation_state_field "${before}" phase)" == "${phase}" ]] && edge_activation_state_matches_transaction "${before}" "${phase}"; then
    edge_activation_log "phase ${phase} is already durably recorded"
    return 0
  fi
  ACTIVATION_BEFORE="${before}" ACTIVATION_PHASE="${phase}" ACTIVATION_EXPECTED="${expected_file}" \
    ACTIVATION_EPOCHS="${epochs_file}" ACTIVATION_API_GENERATION="${api_generation}" \
    ACTIVATION_PLAN_DIGEST="${FUGUE_EDGE_ACTIVATION_PLAN_DIGEST}" \
    ACTIVATION_EVIDENCE_DIGEST="${FUGUE_EDGE_ACTIVATION_EVIDENCE_DIGEST}" \
    ACTIVATION_RELEASE_ID="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID}" \
    ACTIVATION_RECORD_UID="${FUGUE_EDGE_ACTIVATION_RECORD_UID}" \
    ACTIVATION_RECORD_VERSION="${FUGUE_EDGE_ACTIVATION_RECORD_VERSION}" \
    ACTIVATION_RECORD_DIGEST="${FUGUE_EDGE_ACTIVATION_RECORD_DIGEST}" \
    ACTIVATION_LEGACY_DIGEST="${FUGUE_EDGE_ACTIVATION_LEGACY_DIGEST}" \
    ACTIVATION_RELEASE_FENCE="${FUGUE_EDGE_ACTIVATION_RELEASE_FENCE}" \
    python3 - "${request}" <<'PY'
import datetime, hashlib, json, os, sys
with open(os.environ["ACTIVATION_BEFORE"], encoding="utf-8") as handle:
    before = json.load(handle)["activation"]
def load_optional(path):
    if not path:
        return []
    with open(path, encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, list):
        raise SystemExit("activation material must be a JSON array")
    return value
request = {
    "expected_generation": before["generation"],
    "to_phase": os.environ["ACTIVATION_PHASE"],
    "plan_digest": os.environ["ACTIVATION_PLAN_DIGEST"],
    "evidence_digest": os.environ["ACTIVATION_EVIDENCE_DIGEST"],
    "release_id": os.environ["ACTIVATION_RELEASE_ID"],
    "release_record_uid": os.environ["ACTIVATION_RECORD_UID"],
    "release_record_version": os.environ["ACTIVATION_RECORD_VERSION"],
    "release_record_digest": os.environ["ACTIVATION_RECORD_DIGEST"],
    "legacy_snapshot_digest": os.environ["ACTIVATION_LEGACY_DIGEST"],
    "expected_instances": load_optional(os.environ["ACTIVATION_EXPECTED"]),
    "active_epochs": load_optional(os.environ["ACTIVATION_EPOCHS"]),
}
if os.environ["ACTIVATION_API_GENERATION"]:
    request["api_replica_generation"] = os.environ["ACTIVATION_API_GENERATION"]
expected_material=[{key:item[key] for key in ("edge_id","edge_group_id","slot","instance_uid","release_epoch")} for item in request["expected_instances"]]
expected_material.sort(key=lambda item: json.dumps(item,separators=(",",":")))
epoch_material=[{key:item[key] for key in ("edge_group_id","slot","release_epoch","fence_sequence","min_healthy_instances")} for item in request["active_epochs"]]
epoch_material.sort(key=lambda item:item["edge_group_id"])
digest=lambda value:"sha256:"+hashlib.sha256(json.dumps(value,separators=(",",":")).encode()).hexdigest()
expected_digest=digest(expected_material); epoch_digest=digest(epoch_material)
phase_nonce="sha256:"+hashlib.sha256((os.environ["ACTIVATION_RELEASE_FENCE"]+"\n"+str(request["expected_generation"])+"\n"+request["to_phase"]+"\n"+request["plan_digest"]+"\n"+request["release_record_uid"]+"\n"+request["release_record_version"]).encode()).hexdigest()
valid_until=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=5)).replace(microsecond=0).isoformat().replace("+00:00","Z")
authorization={"release_fence":os.environ["ACTIVATION_RELEASE_FENCE"],"phase_nonce":phase_nonce,"valid_until":valid_until,"runner_observed_secret_uid":os.environ["FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_UID"],"runner_observed_secret_version":os.environ["FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_VERSION"]}
request["authorization"]=authorization
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump(request, handle, separators=(",", ":"), sort_keys=True)
    handle.write("\n")
PY
  chmod 600 "${request}"
  edge_activation_sign_request activation "${request}" || return 1
  install -m 600 /dev/null "${response}"
  set +e
  status="$(curl --config "${FUGUE_EDGE_ACTIVATION_CURL_CONFIG}" --request POST \
    --data-binary "@${request}" --output "${response}" --write-out '%{http_code}' "$(edge_activation_endpoint)")"
  local curl_rc=$?
  set -e
  if (( curl_rc == 0 )) && [[ "${status}" == "200" ]]; then
    edge_activation_log "advanced activation to ${phase}"
    return 0
  fi
  # A lost response is reconciled once with a GET; the mutation is never retried.
  local reconcile="${FUGUE_EDGE_ACTIVATION_DIR}/reconcile-${phase}.json"
  if edge_activation_get "${reconcile}" && [[ "$(edge_activation_state_field "${reconcile}" phase)" == "${reconcile_phase}" ]] && { [[ "${phase}" == "rollback" ]] || edge_activation_state_matches_transaction "${reconcile}" "${phase}"; }; then
    edge_activation_log "activation ${phase} committed despite an ambiguous client response"
    return 0
  fi
  edge_activation_error "activation ${phase} failed or was not committed (curl=${curl_rc} http=${status:-none}); refusing retry"
  return 1
}
