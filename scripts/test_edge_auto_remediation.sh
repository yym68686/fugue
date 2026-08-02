#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf -- "${TMP}"' EXIT
export REPO_ROOT="${ROOT}" FUGUE_EDGE_REMEDIATION_LIB_ONLY=true FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
# shellcheck source=scripts/remediate_edge_inactive_slot.sh
source "${ROOT}/scripts/remediate_edge_inactive_slot.sh"

fail() { printf '[test_edge_auto_remediation] ERROR: %s\n' "$*" >&2; exit 1; }

export FUGUE_EDGE_ACTIVATION_DIR="${TMP}/evidence" FUGUE_NAMESPACE=fugue-system
export FUGUE_EDGE_ACTIVATION_RELEASE_FENCE="github:test/repo:77:1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
export FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_UID="runner-observed-secret-uid"
export FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_VERSION="17"
mkdir -m 700 "${FUGUE_EDGE_ACTIVATION_DIR}"

candidate="${TMP}/candidate.json"
cat >"${candidate}" <<'JSON'
{"actionable":true,"activation_generation":10,"action_sequence":0,"nonce":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","active_evidence_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","platform_evidence_digest":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","kubernetes_digest":"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","target":{"edge_id":"node-de","edge_group_id":"edge-group-country-de","slot":"a","instance_uid":"pod-a","release_epoch":"release-old","daemonset_name":"fugue-edge-worker-a","daemonset_uid":"ds-a","daemonset_version":"100","failure_class":"bundle_signature_invalid"}}
JSON

state="${TMP}/state.json"
cat >"${state}" <<'JSON'
{"activation":{"generation":10,"phase":"active-epoch-authoritative","route_authority":"active-epoch","remediation":null}}
JSON

edge_activation_get() { cp "${state}" "$1"; }
edge_activation_sign_request() { :; }
edge_activation_endpoint() { printf 'https://api.example/v1/admin/edge/activation'; }
export FUGUE_EDGE_ACTIVATION_CURL_CONFIG="${TMP}/curl.conf"
: >"${FUGUE_EDGE_ACTIVATION_CURL_CONFIG}"
curl() {
  local request output arg
  while (($#)); do
    arg="$1"; shift
    case "${arg}" in
      --data-binary) request="${1#@}"; shift ;;
      --output) output="$1"; shift ;;
    esac
  done
  REQUEST="${request}" STATE="${state}" python3 - <<'PY'
import json,os
request=json.load(open(os.environ["REQUEST"])); state=json.load(open(os.environ["STATE"])); activation=state["activation"]
action=activation.get("remediation") or {}
if request["to_phase"]=="prepared":
 action={"sequence":1,"phase":"prepared","nonce":request["authorization"]["action_nonce"],"target":request["target"]}
else:
 action.update({"phase":request["to_phase"]})
activation["generation"]+=1; activation["remediation"]=action
json.dump(state,open(os.environ["STATE"],"w"),separators=(",",":"),sort_keys=True)
PY
  printf '{}\n' >"${output}"
  printf '200'
}

edge_remediation_action_advance prepared "${candidate}" || fail "prepared action did not reconcile"
[[ "$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["activation"]["remediation"]["phase"])' "${state}")" == prepared ]] || fail "prepared action state missing"

ds_state="${TMP}/ds.json"; patch_log="${TMP}/patch.log"
cat >"${ds_state}" <<'JSON'
{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"name":"fugue-edge-worker-a","uid":"ds-a","resourceVersion":"100"},"spec":{"template":{"metadata":{"labels":{"fugue.io/edge-slot":"a"},"annotations":{"fugue.io/edge-release-epoch":"release-old","fugue.io/edge-heartbeat-fenced":"false"}},"spec":{"nodeSelector":{"kubernetes.io/os":"linux"}}}},"status":{"desiredNumberScheduled":1,"currentNumberScheduled":1,"numberReady":0,"numberAvailable":0,"numberUnavailable":1,"updatedNumberScheduled":1}}
JSON
patch_calls=0
kubectl_cmd() {
  local joined="$*"
  if [[ "${joined}" == *" get daemonset/fugue-edge-worker-a -o json"* ]]; then
    cat "${ds_state}"; return 0
  fi
  if [[ "${joined}" == *" patch daemonset/fugue-edge-worker-a "* ]]; then
    patch_calls=$((patch_calls+1)); printf '%s\n' "${joined}" >>"${patch_log}"
    local payload=""
    while (($#)); do [[ "$1" == -p ]] && { payload="$2"; break; }; shift; done
    PAYLOAD="${payload}" STATE="${ds_state}" CALL="${patch_calls}" python3 - <<'PY'
import json,os
patch=json.loads(os.environ["PAYLOAD"]); state=json.load(open(os.environ["STATE"])); call=int(os.environ["CALL"])
tests={x["path"]:x.get("value") for x in patch if x["op"]=="test"}
assert tests["/metadata/uid"]=="ds-a" and tests["/spec/template/metadata/labels/fugue.io~1edge-slot"]=="a" and tests["/spec/template/metadata/annotations/fugue.io~1edge-release-epoch"]=="release-old"
if call==1:
 assert tests["/metadata/resourceVersion"]=="100"; state["metadata"]["resourceVersion"]="101"; state["spec"]["template"]["metadata"]["annotations"]["fugue.io/edge-heartbeat-fenced"]="true"
else:
 assert tests["/metadata/resourceVersion"]=="101"; state["metadata"]["resourceVersion"]="102"; state["spec"]["template"]["spec"]["nodeSelector"]["fugue.io/edge-retired"]=patch[-1]["value"]
 for key in ("desiredNumberScheduled","currentNumberScheduled","numberReady","numberAvailable","numberUnavailable","updatedNumberScheduled"): state["status"][key]=0
json.dump(state,open(os.environ["STATE"],"w"),separators=(",",":"),sort_keys=True)
PY
    # Simulate lost patch responses. The implementation may only GET-reconcile,
    # never send either patch a second time.
    return 1
  fi
  fail "unexpected kubectl call: ${joined}"
}

fence_and_scale_inactive_target_once "${candidate}" || fail "ambiguous exact patches were not GET-reconciled"
[[ "${patch_calls}" == 2 ]] || fail "patch was retried: ${patch_calls} calls"
grep -q '/metadata/resourceVersion' "${patch_log}" || fail "resourceVersion CAS is absent"
grep -q 'fugue.io~1edge-retired' "${patch_log}" || fail "exact scale-to-zero fence is absent"

edge_remediation_action_advance committed "${candidate}" || fail "committed action did not reconcile"
edge_remediation_action_advance verified "${candidate}" || fail "verified action did not reconcile"
[[ "$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["activation"]["remediation"]["phase"])' "${state}")" == verified ]] || fail "verified terminal action missing"

source_text="$(cat "${ROOT}/scripts/remediate_edge_inactive_slot.sh")"
[[ "${source_text}" != *write_front_active_slot* && "${source_text}" != *FUGUE_BUNDLE_SIGNING_KEY* ]] || fail "remediator contains forbidden front/signer mutation"
printf '[test_edge_auto_remediation] ok\n'
