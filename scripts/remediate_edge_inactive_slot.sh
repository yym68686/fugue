#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY=true
# shellcheck source=scripts/release_fugue_public_data_plane.sh
source "${REPO_ROOT}/scripts/release_fugue_public_data_plane.sh"

remediation_log() { printf '[fugue-edge-remediation] %s\n' "$*"; }
remediation_error() { printf '[fugue-edge-remediation] ERROR: %s\n' "$*" >&2; }

select_edge_activation_signer() {
  local snapshot="${FUGUE_EDGE_ACTIVATION_DIR}/signer-pods.json"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get pods -l "app.kubernetes.io/instance=${FUGUE_RELEASE_INSTANCE},app.kubernetes.io/component=api" -o json >"${snapshot}" || return 1
  FUGUE_EDGE_ACTIVATION_SIGNER_POD="$(EXPECTED_SHA="${GITHUB_SHA}" python3 - "${snapshot}" <<'PY'
import json,os,sys
with open(sys.argv[1],encoding="utf-8") as h: value=json.load(h)
eligible=[]
for pod in value.get("items") or []:
    meta=pod.get("metadata") or {}; status=pod.get("status") or {}
    conditions={item.get("type"):item.get("status") for item in status.get("conditions") or []}
    if not meta.get("deletionTimestamp") and status.get("phase")=="Running" and conditions.get("Ready")=="True" and (meta.get("annotations") or {}).get("fugue.pro/source-commit")==os.environ["EXPECTED_SHA"]:
        eligible.append(str(meta.get("name") or ""))
if len(eligible)<2 or any(not name for name in eligible): raise SystemExit("exact API signer cohort is unavailable")
print(sorted(eligible)[0])
PY
)" || return 1
  export FUGUE_EDGE_ACTIVATION_SIGNER_POD
}

capture_edge_remediation_candidate() {
  local output="$1"
  local inventory="${FUGUE_EDGE_ACTIVATION_DIR}/remediation-inventory.json"
  local workloads="${FUGUE_EDGE_ACTIVATION_DIR}/remediation-workloads.json"
  edge_activation_get "${inventory}" || return 1
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get daemonsets,pods -l "fugue.io/rollout-mode=node-local-blue-green-worker" -o json >"${workloads}" || return 1
  chmod 600 "${inventory}" "${workloads}"
  INVENTORY="${inventory}" WORKLOADS="${workloads}" OUT="${output}" RELEASE_FENCE="${FUGUE_EDGE_ACTIVATION_RELEASE_FENCE}" PLATFORM_EVIDENCE_DIGEST="${FUGUE_EDGE_REMEDIATION_PLATFORM_EVIDENCE_DIGEST}" python3 - <<'PY'
import datetime,hashlib,json,os
def digest(value): return "sha256:"+hashlib.sha256(json.dumps(value,separators=(",",":"),sort_keys=True).encode()).hexdigest()
with open(os.environ["INVENTORY"],encoding="utf-8") as h: inventory=json.load(h)
with open(os.environ["WORKLOADS"],encoding="utf-8") as h: workloads=json.load(h)
activation=inventory.get("activation") or {}
if activation.get("route_authority")!="active-epoch" or activation.get("phase") not in ("active-epoch-authoritative","active-epoch-enforced"):
    raise SystemExit("route authority is not eligible for automatic remediation")
receipts=activation.get("receipts") or []
if not receipts:
    raise SystemExit("activation has no signed release receipt")
receipt=receipts[-1]
if receipt.get("to_phase")!=activation.get("phase") or receipt.get("plan_digest")!=activation.get("plan_digest") or not receipt.get("authorization_digest") or not receipt.get("key_id") or not receipt.get("key_generation"):
    raise SystemExit("activation signed receipt is incomplete or drifted")
remediation=activation.get("remediation") or {}
if remediation.get("phase") in ("prepared","committed","rollback_pending"):
    with open(os.environ["OUT"],"w",encoding="utf-8") as h: json.dump({"actionable":False,"blocked_by":"existing_action"},h,separators=(",",":"),sort_keys=True); h.write("\n")
    raise SystemExit(0)
epochs=inventory.get("active_epochs") or []
instances=inventory.get("instances") or []
if not epochs: raise SystemExit("active epoch set is empty")
now=datetime.datetime.now(datetime.timezone.utc)
def parse_time(raw):
    if not isinstance(raw,str): return None
    try: return datetime.datetime.fromisoformat(raw.replace("Z","+00:00"))
    except ValueError: return None
active_material=[]
active_by_group={}
for epoch in epochs:
    group=epoch.get("edge_group_id"); key=(group,epoch.get("slot"),epoch.get("release_epoch"))
    if not all(isinstance(v,str) and v for v in key) or group in active_by_group: raise SystemExit("active epoch set is ambiguous")
    matches=[]
    for item in instances:
        node=item.get("node") or {}; seen=parse_time(item.get("last_heartbeat_at"))
        if (item.get("edge_group_id"),item.get("slot"),item.get("release_epoch")) != key: continue
        fresh=seen is not None and seen <= now+datetime.timedelta(seconds=5) and now-seen <= datetime.timedelta(seconds=45)
        if fresh and item.get("effective_healthy") is True and int(item.get("consecutive_healthy") or 0)>=2 and not item.get("failure_class") and node.get("tls_status")=="ready" and not node.get("draining"):
            matches.append({k:item.get(k) for k in ("edge_id","edge_group_id","slot","instance_uid","release_epoch","last_heartbeat_at")})
    if int(epoch.get("min_healthy_instances") or 0)<=0 or len(matches)<int(epoch["min_healthy_instances"]): raise SystemExit("active epoch is not continuously fresh and healthy")
    active_by_group[group]=epoch; active_material.extend(matches)
items=workloads.get("items") if isinstance(workloads,dict) else None
if not isinstance(items,list): raise SystemExit("workload snapshot is invalid")
daemonsets={}; pods=[]
for item in items:
    kind=item.get("kind"); meta=item.get("metadata") or {}
    if kind=="DaemonSet":
        template=((item.get("spec") or {}).get("template") or {}); labels=(template.get("metadata") or {}).get("labels") or {}; annotations=(template.get("metadata") or {}).get("annotations") or {}
        key=(labels.get("fugue.io/edge-group-id"),labels.get("fugue.io/edge-slot"),annotations.get("fugue.io/edge-release-epoch"))
        if not all(isinstance(v,str) and v for v in key) or key in daemonsets or meta.get("deletionTimestamp"): raise SystemExit("worker DaemonSet identity is invalid or duplicated")
        daemonsets[key]=item
    elif kind=="Pod": pods.append(item)
for group,epoch in active_by_group.items():
    key=(group,epoch["slot"],epoch["release_epoch"]); ds=daemonsets.get(key)
    if ds is None: raise SystemExit("active DaemonSet identity is missing")
    spec=ds.get("spec") or {}; status=ds.get("status") or {}; desired=int(status.get("desiredNumberScheduled") or 0)
    if desired<=0 or any(int(status.get(field) or 0)!=desired for field in ("currentNumberScheduled","numberReady","numberAvailable","updatedNumberScheduled")): raise SystemExit("active DaemonSet is not fully healthy")
hard={"bundle_signature_invalid","max_stale_exceeded","identity_drift"}
candidates=[]
for item in instances:
    failure=item.get("failure_class")
    if failure not in hard: continue
    group=item.get("edge_group_id"); epoch=active_by_group.get(group)
    if epoch is None or (item.get("slot"),item.get("release_epoch"))==(epoch.get("slot"),epoch.get("release_epoch")): continue
    key=(group,item.get("slot"),item.get("release_epoch")); ds=daemonsets.get(key)
    if ds is None: continue
    pod_match=[p for p in pods if (p.get("metadata") or {}).get("uid")==item.get("instance_uid") and (p.get("spec") or {}).get("nodeName")==item.get("edge_id")]
    if len(pod_match)!=1: continue
    candidates.append((item,ds,failure))
for pod in pods:
    meta=pod.get("metadata") or {}; labels=meta.get("labels") or {}; annotations=meta.get("annotations") or {}; group=labels.get("fugue.io/edge-group-id"); slot=labels.get("fugue.io/edge-slot"); release=annotations.get("fugue.io/edge-release-epoch")
    epoch=active_by_group.get(group)
    if epoch is None or (slot,release)==(epoch.get("slot"),epoch.get("release_epoch")): continue
    waiting=[((entry.get("state") or {}).get("waiting") or {}).get("reason") for entry in (pod.get("status") or {}).get("containerStatuses") or [] if entry.get("name")=="edge"]
    ds=daemonsets.get((group,slot,release))
    if ds is not None and waiting==["CrashLoopBackOff"]:
        synthetic={"edge_id":(pod.get("spec") or {}).get("nodeName"),"edge_group_id":group,"slot":slot,"instance_uid":meta.get("uid"),"release_epoch":release}
        candidates.append((synthetic,ds,"crash_loop"))
dedup={}
for item,ds,failure in candidates:
    key=((ds.get("metadata") or {}).get("uid"),item.get("instance_uid"),failure); dedup[key]=(item,ds,failure)
ordered=[dedup[key] for key in sorted(dedup)]
if not ordered:
    with open(os.environ["OUT"],"w",encoding="utf-8") as h: json.dump({"actionable":False},h,separators=(",",":"),sort_keys=True); h.write("\n")
    raise SystemExit(0)
item,ds,failure=ordered[0]; meta=ds.get("metadata") or {}; spec=ds.get("spec") or {}; status=ds.get("status") or {}; template=(spec.get("template") or {}); tmeta=template.get("metadata") or {}; tspec=template.get("spec") or {}
if int(status.get("desiredNumberScheduled") or 0)<=0 or (tmeta.get("annotations") or {}).get("fugue.io/edge-heartbeat-fenced")!="false" or "fugue.io/edge-retired" in (tspec.get("nodeSelector") or {}): raise SystemExit("inactive target is already fenced or retired")
target={"edge_id":item.get("edge_id"),"edge_group_id":item.get("edge_group_id"),"slot":item.get("slot"),"instance_uid":item.get("instance_uid"),"release_epoch":item.get("release_epoch"),"daemonset_name":meta.get("name"),"daemonset_uid":meta.get("uid"),"daemonset_version":str(meta.get("resourceVersion") or ""),"failure_class":failure}
if not all(isinstance(v,str) and v for v in target.values()): raise SystemExit("remediation target identity is incomplete")
active_material.sort(key=lambda v:(v["edge_group_id"],v["edge_id"],v["instance_uid"]))
kube_material={"active":[{"uid":(daemonsets[(g,e["slot"],e["release_epoch"])].get("metadata") or {}).get("uid"),"rv":(daemonsets[(g,e["slot"],e["release_epoch"])].get("metadata") or {}).get("resourceVersion"),"status":daemonsets[(g,e["slot"],e["release_epoch"])].get("status") or {}} for g,e in sorted(active_by_group.items())],"target":{"uid":meta.get("uid"),"rv":meta.get("resourceVersion"),"generation":meta.get("generation"),"status":status,"template":template}}
sequence=int(remediation.get("sequence") or 0)
nonce=digest({"fence":os.environ["RELEASE_FENCE"],"activation_generation":activation.get("generation"),"action_sequence":sequence,"target":target})
result={"actionable":True,"activation_generation":activation.get("generation"),"action_sequence":sequence,"nonce":nonce,"target":target,"active_evidence_digest":digest({"epochs":epochs,"instances":active_material}),"kubernetes_digest":digest(kube_material),"platform_evidence_digest":os.environ["PLATFORM_EVIDENCE_DIGEST"],"candidate_count":len(ordered)}
with open(os.environ["OUT"],"w",encoding="utf-8") as h: json.dump(result,h,separators=(",",":"),sort_keys=True); h.write("\n")
PY
  chmod 600 "${output}"
}

edge_remediation_action_advance() {
  local phase="$1" candidate="$2"
  local inventory="${FUGUE_EDGE_ACTIVATION_DIR}/action-${phase}-before.json"
  local request="${FUGUE_EDGE_ACTIVATION_DIR}/action-${phase}-request.json"
  local response="${FUGUE_EDGE_ACTIVATION_DIR}/action-${phase}-response.json"
  edge_activation_get "${inventory}" || return 1
  PHASE="${phase}" CANDIDATE="${candidate}" INVENTORY="${inventory}" REQUEST="${request}" RELEASE_FENCE="${FUGUE_EDGE_ACTIVATION_RELEASE_FENCE}" python3 - <<'PY'
import datetime,json,os
with open(os.environ["CANDIDATE"],encoding="utf-8") as h: candidate=json.load(h)
with open(os.environ["INVENTORY"],encoding="utf-8") as h: activation=json.load(h)["activation"]
current=activation.get("remediation") or {}
phase=os.environ["PHASE"]
if phase=="prepared": expected_sequence=int(candidate["action_sequence"])
else:
    expected_sequence=int(current.get("sequence") or 0)
    if current.get("nonce")!=candidate["nonce"]: raise SystemExit("action nonce changed")
request={"expected_activation_generation":activation["generation"],"expected_action_sequence":expected_sequence,"to_phase":phase,"active_evidence_digest":candidate["active_evidence_digest"],"platform_evidence_digest":candidate["platform_evidence_digest"],"kubernetes_digest":candidate["kubernetes_digest"],"target":candidate["target"],"authorization":{"release_fence":os.environ["RELEASE_FENCE"],"action_nonce":candidate["nonce"],"valid_until":(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=5)).replace(microsecond=0).isoformat().replace("+00:00","Z"),"runner_observed_secret_uid":os.environ["FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_UID"],"runner_observed_secret_version":os.environ["FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_VERSION"]}}
with open(os.environ["REQUEST"],"w",encoding="utf-8") as h: json.dump(request,h,separators=(",",":"),sort_keys=True); h.write("\n")
PY
  chmod 600 "${request}"
  edge_activation_sign_request remediation "${request}" || return 1
  install -m 600 /dev/null "${response}"
  local status="" rc
  set +e
  status="$(curl --config "${FUGUE_EDGE_ACTIVATION_CURL_CONFIG}" --request POST --data-binary "@${request}" --output "${response}" --write-out '%{http_code}' "$(edge_activation_endpoint)/remediation")"
  rc=$?
  set -e
  local reconcile="${FUGUE_EDGE_ACTIVATION_DIR}/action-${phase}-reconcile.json"
  edge_activation_get "${reconcile}" || {
    remediation_error "action ${phase} response was not safely reconcilable (curl=${rc} http=${status:-none})"
    return 1
  }
  PHASE="${phase}" CANDIDATE="${candidate}" RECONCILE="${reconcile}" python3 - <<'PY'
import json,os
with open(os.environ["CANDIDATE"],encoding="utf-8") as h: candidate=json.load(h)
with open(os.environ["RECONCILE"],encoding="utf-8") as h: action=(json.load(h)["activation"].get("remediation") or {})
if action.get("phase")!=os.environ["PHASE"] or action.get("nonce")!=candidate["nonce"] or action.get("target")!=candidate["target"]: raise SystemExit("action reconciliation does not match exact fence")
PY
}

reconcile_target_fenced() {
  local candidate="$1" output="$2"
  local name
  name="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["target"]["daemonset_name"])' "${candidate}")"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get "daemonset/${name}" -o json >"${output}" || return 1
  CANDIDATE="${candidate}" SNAPSHOT="${output}" python3 - <<'PY'
import json,os
with open(os.environ["CANDIDATE"],encoding="utf-8") as h: target=json.load(h)["target"]
with open(os.environ["SNAPSHOT"],encoding="utf-8") as h: ds=json.load(h)
meta=ds.get("metadata") or {}; template=((ds.get("spec") or {}).get("template") or {}); tmeta=template.get("metadata") or {}
if meta.get("uid")!=target["daemonset_uid"] or (tmeta.get("annotations") or {}).get("fugue.io/edge-heartbeat-fenced")!="true": raise SystemExit("inactive heartbeat fence was not reconciled")
PY
}

fence_and_scale_inactive_target_once() {
  local candidate="$1" current="${FUGUE_EDGE_ACTIVATION_DIR}/target-current.json" fenced="${FUGUE_EDGE_ACTIVATION_DIR}/target-fenced.json"
  local name uid rv slot epoch
  IFS=$'\t' read -r name uid rv slot epoch < <(python3 - "${candidate}" <<'PY'
import json,sys
with open(sys.argv[1],encoding="utf-8") as h: t=json.load(h)["target"]
print("\t".join(t[k] for k in ("daemonset_name","daemonset_uid","daemonset_version","slot","release_epoch")))
PY
) || return 1
  local fence_patch
  fence_patch="$(python3 - "${uid}" "${rv}" "${slot}" "${epoch}" <<'PY'
import json,sys
print(json.dumps([{"op":"test","path":"/metadata/uid","value":sys.argv[1]},{"op":"test","path":"/metadata/resourceVersion","value":sys.argv[2]},{"op":"test","path":"/spec/template/metadata/labels/fugue.io~1edge-slot","value":sys.argv[3]},{"op":"test","path":"/spec/template/metadata/annotations/fugue.io~1edge-release-epoch","value":sys.argv[4]},{"op":"test","path":"/spec/template/metadata/annotations/fugue.io~1edge-heartbeat-fenced","value":"false"},{"op":"replace","path":"/spec/template/metadata/annotations/fugue.io~1edge-heartbeat-fenced","value":"true"}],separators=(",",":")))
PY
)" || return 1
  if ! kubectl_cmd -n "${FUGUE_NAMESPACE}" patch "daemonset/${name}" --type=json -p "${fence_patch}" >/dev/null; then
    reconcile_target_fenced "${candidate}" "${current}" || return 1
  fi
  reconcile_target_fenced "${candidate}" "${fenced}" || return 1
  local fenced_rv scale_patch
  fenced_rv="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["metadata"]["resourceVersion"])' "${fenced}")"
  scale_patch="$(python3 - "${uid}" "${fenced_rv}" "${slot}" "${epoch}" "${FUGUE_EDGE_ACTIVATION_RELEASE_FENCE}" <<'PY'
import json,sys
print(json.dumps([{"op":"test","path":"/metadata/uid","value":sys.argv[1]},{"op":"test","path":"/metadata/resourceVersion","value":sys.argv[2]},{"op":"test","path":"/spec/template/metadata/labels/fugue.io~1edge-slot","value":sys.argv[3]},{"op":"test","path":"/spec/template/metadata/annotations/fugue.io~1edge-release-epoch","value":sys.argv[4]},{"op":"test","path":"/spec/template/metadata/annotations/fugue.io~1edge-heartbeat-fenced","value":"true"},{"op":"add","path":"/spec/template/spec/nodeSelector/fugue.io~1edge-retired","value":sys.argv[5]}],separators=(",",":")))
PY
)" || return 1
  if ! kubectl_cmd -n "${FUGUE_NAMESPACE}" patch "daemonset/${name}" --type=json -p "${scale_patch}" >/dev/null; then
    remediation_error "scale-to-zero patch was ambiguous; reconciling once without retry"
  fi
  local deadline=$((SECONDS+30)) snapshot="${FUGUE_EDGE_ACTIVATION_DIR}/target-zero.json"
  while (( SECONDS < deadline )); do
    kubectl_cmd -n "${FUGUE_NAMESPACE}" get "daemonset/${name}" -o json >"${snapshot}" || return 1
    if CANDIDATE="${candidate}" SNAPSHOT="${snapshot}" python3 - <<'PY'
import json,os
with open(os.environ["CANDIDATE"],encoding="utf-8") as h:t=json.load(h)["target"]
with open(os.environ["SNAPSHOT"],encoding="utf-8") as h:ds=json.load(h)
meta=ds.get("metadata") or {}; status=ds.get("status") or {}; template=((ds.get("spec") or {}).get("template") or {}); annotations=(template.get("metadata") or {}).get("annotations") or {}; selector=(template.get("spec") or {}).get("nodeSelector") or {}
if meta.get("uid")!=t["daemonset_uid"] or annotations.get("fugue.io/edge-release-epoch")!=t["release_epoch"] or annotations.get("fugue.io/edge-heartbeat-fenced")!="true" or "fugue.io/edge-retired" not in selector: raise SystemExit(1)
if any(int(status.get(field) or 0)!=0 for field in ("desiredNumberScheduled","currentNumberScheduled","numberReady","numberAvailable","numberUnavailable","updatedNumberScheduled")): raise SystemExit(1)
PY
    then return 0; fi
    sleep 2
  done
  return 1
}

fresh_platform_release_evidence() {
  local inventory="${FUGUE_EDGE_ACTIVATION_DIR}/platform-evidence-activation.json"
  edge_activation_get "${inventory}" || return 1
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID="$(edge_activation_state_field "${inventory}" release_id)" || return 1
  export FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID
  run_smoke_urls && run_platform_release_evidence
}

compute_remediation_platform_evidence_digest() {
  python3 - "${FUGUE_EDGE_ACTIVATION_DIR}/platform-release-evidence.json" <<'PY'
import hashlib,sys
h=hashlib.sha256(); h.update(open(sys.argv[1],"rb").read()); print("sha256:"+h.hexdigest())
PY
}

observe_once() {
  fresh_platform_release_evidence || return 1
  FUGUE_EDGE_REMEDIATION_PLATFORM_EVIDENCE_DIGEST="$(compute_remediation_platform_evidence_digest)"; export FUGUE_EDGE_REMEDIATION_PLATFORM_EVIDENCE_DIGEST
  local candidate="${FUGUE_EDGE_ACTIVATION_DIR}/candidate.json"
  capture_edge_remediation_candidate "${candidate}" || return 1
  python3 -c 'import json,sys;raise SystemExit(0 if json.load(open(sys.argv[1])).get("actionable") else 1)' "${candidate}"
}

execute_once() {
  fresh_platform_release_evidence || return 1
  FUGUE_EDGE_REMEDIATION_PLATFORM_EVIDENCE_DIGEST="$(compute_remediation_platform_evidence_digest)"; export FUGUE_EDGE_REMEDIATION_PLATFORM_EVIDENCE_DIGEST
  local candidate="${FUGUE_EDGE_ACTIVATION_DIR}/candidate.json"
  capture_edge_remediation_candidate "${candidate}" || return 1
  python3 -c 'import json,sys;raise SystemExit(0 if json.load(open(sys.argv[1])).get("actionable") else 1)' "${candidate}" || return 0
  edge_remediation_action_advance prepared "${candidate}" || return 1
  fence_and_scale_inactive_target_once "${candidate}" || return 1
  edge_remediation_action_advance committed "${candidate}" || return 1
  if ! fresh_platform_release_evidence; then
    edge_remediation_action_advance rollback_pending "${candidate}" || true
    return 1
  fi
  edge_remediation_action_advance verified "${candidate}" || return 1
  remediation_log "exact inactive Edge action verified"
}

main() {
  local mode="${1:-observe}"
  [[ "${FUGUE_EDGE_AUTO_REMEDIATION_ENABLED:-false}" == "true" ]] || { remediation_log "disabled"; return 0; }
  [[ "${mode}" == "observe" || "${mode}" == "execute" ]] || return 1
  local interval="${FUGUE_EDGE_AUTO_REMEDIATION_INTERVAL_SECONDS:-20}" deadline="${FUGUE_EDGE_AUTO_REMEDIATION_DEADLINE_SECONDS:-55}"
  [[ "${interval}" =~ ^[0-9]+$ && "${deadline}" =~ ^[0-9]+$ ]] || return 1
  (( interval>=15 && interval<=30 && deadline>=30 && deadline<=55 )) || return 1
  FUGUE_NAMESPACE="${FUGUE_NAMESPACE:-fugue-system}"; FUGUE_RELEASE_NAME="${FUGUE_RELEASE_NAME:-fugue}"; FUGUE_RELEASE_FULLNAME="${FUGUE_RELEASE_FULLNAME:-fugue-fugue}"; FUGUE_RELEASE_INSTANCE="${FUGUE_RELEASE_INSTANCE:-${FUGUE_RELEASE_NAME}}"
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN=false
  detect_kubectl
  FUGUE_EDGE_ACTIVATION_DIR="$(mktemp -d)"; export FUGUE_EDGE_ACTIVATION_DIR
  chmod 700 "${FUGUE_EDGE_ACTIVATION_DIR}"
  select_edge_activation_signer || return 1
  : "${FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_NAME:?FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_NAME is required}"
  IFS=$'\t' read -r FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_UID FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_VERSION < <(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "secret/${FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_NAME}" -o 'jsonpath={.metadata.uid}{"\t"}{.metadata.resourceVersion}') || return 1
  export FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_UID FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_VERSION
  edge_activation_init
  trap edge_activation_cleanup EXIT
  if [[ "${mode}" == "execute" ]]; then execute_once; return; fi
  local stop=$((SECONDS+deadline))
  while (( SECONDS < stop )); do
    if observe_once; then
      [[ -n "${GITHUB_OUTPUT:-}" ]] && printf 'actionable=true\n' >>"${GITHUB_OUTPUT}"
      return 0
    fi
    sleep "${interval}"
  done
  [[ -n "${GITHUB_OUTPUT:-}" ]] && printf 'actionable=false\n' >>"${GITHUB_OUTPUT}"
}

if [[ "${FUGUE_EDGE_REMEDIATION_LIB_ONLY:-false}" != "true" ]]; then
  main "$@"
fi
