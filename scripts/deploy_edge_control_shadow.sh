#!/usr/bin/env bash
set -euo pipefail

fail() { printf '[edge-control-shadow] ERROR: %s\n' "$*" >&2; exit 1; }
log() { printf '[edge-control-shadow] %s\n' "$*"; }

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
EXPECTED_SOURCE="${FUGUE_EDGE_CONTROL_EXPECTED_SOURCE:?expected source is required}"
IMAGE="${FUGUE_EDGE_CONTROL_IMAGE:?image repository is required}"
IMAGE_DIGEST="${FUGUE_EDGE_CONTROL_IMAGE_DIGEST:?image digest is required}"
IMAGE_RECEIPT_DIGEST="${FUGUE_EDGE_CONTROL_IMAGE_RECEIPT_DIGEST:?image receipt digest is required}"
SOURCE_RUN_ID="${FUGUE_EDGE_CONTROL_SOURCE_RUN_ID:?source run id is required}"
SOURCE_ARTIFACT_ID="${FUGUE_EDGE_CONTROL_SOURCE_ARTIFACT_ID:?source artifact id is required}"
SOURCE_ARTIFACT_DIGEST="${FUGUE_EDGE_CONTROL_SOURCE_ARTIFACT_DIGEST:?source artifact digest is required}"
NAMESPACE="${FUGUE_EDGE_CONTROL_NAMESPACE:-fugue-system}"
RELEASE="${FUGUE_EDGE_CONTROL_RELEASE:-edge-control}"
LEGACY_RELEASE="${FUGUE_LEGACY_RELEASE:-fugue}"
LEGACY_FULLNAME="${FUGUE_LEGACY_RELEASE_FULLNAME:-fugue-fugue}"
PRODUCT_HEALTH_URL="${FUGUE_PRODUCT_HEALTH_URL:?product health URL is required}"
RECEIPT_PATH="${FUGUE_EDGE_CONTROL_RECEIPT_PATH:?receipt path is required}"
CHART="${REPO_ROOT}/deploy/helm/fugue-edge-control"
WORK_DIR="$(dirname "${RECEIPT_PATH}")"

[[ "${EXPECTED_SOURCE}" =~ ^[0-9a-f]{40}$ ]] || fail 'expected source must be exact lowercase 40-hex'
[[ "${IMAGE}" =~ ^[a-z0-9._-]+(:[0-9]+)?(/[a-z0-9._-]+)+$ ]] || fail 'image repository is not canonical'
[[ "${IMAGE_DIGEST}" =~ ^sha256:[0-9a-f]{64}$ ]] || fail 'image digest is not canonical'
[[ "${IMAGE_RECEIPT_DIGEST}" =~ ^sha256:[0-9a-f]{64}$ ]] || fail 'image receipt digest is not canonical'
[[ "${SOURCE_RUN_ID}" =~ ^[1-9][0-9]*$ && "${SOURCE_ARTIFACT_ID}" =~ ^[1-9][0-9]*$ ]] || fail 'source IDs are invalid'
[[ "${SOURCE_ARTIFACT_DIGEST}" =~ ^sha256:[0-9a-f]{64}$ ]] || fail 'source artifact digest is invalid'
[[ "${NAMESPACE}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || fail 'namespace is invalid'
[[ "${RELEASE}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || fail 'release is invalid'
[[ "${LEGACY_RELEASE}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || fail 'legacy release is invalid'
[[ "${LEGACY_FULLNAME}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || fail 'legacy fullname is invalid'
[[ "${PRODUCT_HEALTH_URL}" =~ ^https://[^/?#]+/healthz$ ]] || fail 'product health URL must be an HTTPS /healthz endpoint'
[[ -d "${CHART}" && ! -L "${CHART}" ]] || fail 'chart path is missing or symlinked'
[[ ! -e "${WORK_DIR}" && ! -L "${WORK_DIR}" ]] || fail 'receipt directory already exists'
install -d -m 0700 "${WORK_DIR}"
umask 077

command -v helm >/dev/null || fail 'helm is required'
command -v python3 >/dev/null || fail 'python3 is required'
command -v curl >/dev/null || fail 'curl is required'
if [[ -n "${KUBECTL:-}" ]]; then
  read -r -a KUBE <<<"${KUBECTL}"
elif command -v kubectl >/dev/null; then
  KUBE=(kubectl)
elif command -v k3s >/dev/null; then
  KUBE=(k3s kubectl)
else
  fail 'kubectl or k3s kubectl is required'
fi
kube() { "${KUBE[@]}" "$@"; }

github_api_read() {
  local path="$1"
  curl --fail --silent --show-error --location --max-time 10 \
    --header 'Accept: application/vnd.github+json' \
    --header "Authorization: Bearer ${GITHUB_TOKEN}" \
    --header 'X-GitHub-Api-Version: 2022-11-28' \
    "https://api.github.com/${path}"
}

verify_github_prewrite() {
  local remote_main runs_file state
  [[ "${GITHUB_ACTIONS:-}" == 'true' ]] || fail 'production shadow release must run in GitHub Actions'
  [[ "${GITHUB_REPOSITORY:-}" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || fail 'GitHub repository identity is invalid'
  [[ "${GITHUB_RUN_ID:-}" =~ ^[1-9][0-9]*$ && "${GITHUB_RUN_ATTEMPT:-}" == '1' ]] || fail 'GitHub run identity is invalid'
  [[ -n "${GITHUB_TOKEN:-}" ]] || fail 'GitHub token is required for the final prewrite CAS'
  github_api_read "repos/${GITHUB_REPOSITORY}/git/ref/heads/main" >"${WORK_DIR}/github-main.json" || fail 'could not read main before Helm write'
  remote_main="$(python3 -c \
    'import json,sys; print(json.load(open(sys.argv[1],encoding="utf-8"))["object"]["sha"])' \
    "${WORK_DIR}/github-main.json")" || fail 'could not decode main before Helm write'
  [[ "${remote_main}" == "${EXPECTED_SOURCE}" ]] || fail 'main moved before Helm write'
  for state in in_progress queued; do
    runs_file="${WORK_DIR}/github-runs-${state}.json"
    github_api_read \
      "repos/${GITHUB_REPOSITORY}/actions/runs?status=${state}&per_page=100" >"${runs_file}" || fail 'could not read production workflow state'
    python3 - "${runs_file}" "${GITHUB_RUN_ID}" <<'PY'
import json,sys
blocked={
 ".github/workflows/deploy-control-plane.yml",
 ".github/workflows/release-public-data-plane.yml",
 ".github/workflows/adopt-public-data-plane-helm-baseline.yml",
 ".github/workflows/recover-public-data-plane-helm-adoption.yml",
 ".github/workflows/remediate-edge-inactive-slot.yml",
}
conflicts=[r for r in json.load(open(sys.argv[1],encoding="utf-8")).get("workflow_runs",[])
           if str(r.get("id"))!=sys.argv[2] and r.get("path") in blocked]
if conflicts: raise SystemExit("another production cluster mutation lane became active")
PY
  done
}

snapshot_legacy() {
  local output="$1" resources record status front_plan front_observed base pod file expected actual
  resources="${output}.resources.json"
  record="${output}.record.json"
  status="${output}.helm.json"
  front_plan="${output}.front-plan.tsv"
  front_observed="${output}.front-observed.tsv"
  kube -n "${NAMESPACE}" get deployment,daemonset,pod \
    -l "app.kubernetes.io/instance=${LEGACY_RELEASE}" -o json >"${resources}"
  kube -n "${NAMESPACE}" get "configmap/${LEGACY_FULLNAME}-public-data-plane-release" -o json >"${record}"
  helm status "${LEGACY_RELEASE}" -n "${NAMESPACE}" -o json >"${status}"
  python3 - "${resources}" "${record}" >"${front_plan}" <<'PY'
import json,re,sys
resources=json.load(open(sys.argv[1],encoding="utf-8")); record=json.load(open(sys.argv[2],encoding="utf-8"))
try: active_slots=json.loads((record.get("data") or {}).get("active_slots",""))
except Exception as exc: raise SystemExit(f"active slot record is invalid: {exc}")
if not isinstance(active_slots,dict) or not active_slots: raise SystemExit("active slot record is empty")
items=resources.get("items",[]); daemonsets={x.get("metadata",{}).get("name"):x for x in items if x.get("kind")=="DaemonSet"}; pods=[x for x in items if x.get("kind")=="Pod"]
for base,expected in sorted(active_slots.items()):
    if re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?",str(base)) is None or expected not in ("a","b"):
        raise SystemExit("active slot identity is invalid")
    ds=daemonsets.get(f"{base}-front")
    if ds is None: raise SystemExit(f"front daemonset for {base} is missing")
    selector=(ds.get("spec",{}).get("selector") or {}).get("matchLabels") or {}
    if not selector: raise SystemExit(f"front daemonset for {base} has no selector")
    matches=[]
    for pod in pods:
        meta=pod.get("metadata",{}); labels=meta.get("labels") or {}; state=pod.get("status",{}); statuses=state.get("containerStatuses") or []
        if all(labels.get(k)==v for k,v in selector.items()) and not meta.get("deletionTimestamp") and state.get("phase")=="Running" and statuses and all(x.get("ready") for x in statuses):
            matches.append(meta.get("name"))
    desired=(ds.get("status") or {}).get("desiredNumberScheduled")
    if not isinstance(desired,int) or desired<1 or len(matches)!=desired: raise SystemExit(f"front pod cohort for {base} is not exact and ready")
    containers=ds.get("spec",{}).get("template",{}).get("spec",{}).get("containers") or []
    edge_front=[c for c in containers if c.get("name")=="edge-front"]
    if len(edge_front)!=1: raise SystemExit(f"front container for {base} is ambiguous")
    env={item.get("name"):item.get("value") for item in edge_front[0].get("env") or []}
    path=env.get("FUGUE_EDGE_FRONT_ACTIVE_SLOT_FILE")
    if not isinstance(path,str) or not path.startswith("/") or "\t" in path or "\n" in path: raise SystemExit(f"front slot path for {base} is invalid")
    for pod in sorted(matches): print(f"{base}\t{pod}\t{path}\t{expected}")
PY
  : >"${front_observed}"
  while IFS=$'\t' read -r base pod file expected extra; do
    [[ -n "${base}" && -n "${pod}" && -n "${file}" && "${expected}" =~ ^[ab]$ && -z "${extra:-}" ]] || fail 'front probe plan is malformed'
    actual="$(kube -n "${NAMESPACE}" exec "pod/${pod}" -c edge-front -- /bin/sh -ec 'cat "$1"' sh "${file}")" || fail "could not read active slot from front ${pod}"
    actual="${actual#"${actual%%[![:space:]]*}"}"; actual="${actual%"${actual##*[![:space:]]}"}"
    [[ "${actual}" == "${expected}" ]] || fail "front ${pod} serves slot ${actual:-empty}, record expects ${expected}"
    printf '%s\t%s\t%s\n' "${base}" "${pod}" "${actual}" >>"${front_observed}"
  done <"${front_plan}"
  [[ -s "${front_observed}" ]] || fail 'front active-slot observation is empty'
  python3 - "${resources}" "${record}" "${status}" "${front_observed}" "${output}" <<'PY'
import hashlib,json,re,sys
with open(sys.argv[1],encoding="utf-8") as handle: resources=json.load(handle)
with open(sys.argv[2],encoding="utf-8") as handle: record=json.load(handle)
with open(sys.argv[3],encoding="utf-8") as handle: status=json.load(handle)
front_observed=[]
with open(sys.argv[4],encoding="utf-8") as handle:
    for line in handle:
        group,pod,slot=line.rstrip("\n").split("\t")
        front_observed.append({"base":group,"pod":pod,"slot":slot})
output=sys.argv[5]
items=resources.get("items",[])
deployments=[x for x in items if x.get("kind")=="Deployment"]
daemonsets=[x for x in items if x.get("kind")=="DaemonSet" and (x.get("metadata",{}).get("labels") or {}).get("fugue.io/rollout-subsystem")=="public-data-plane"]
pods=[x for x in items if x.get("kind")=="Pod"]
by_component={}
for item in deployments:
    labels=item.get("metadata",{}).get("labels") or {}
    by_component.setdefault(labels.get("app.kubernetes.io/component"),[]).append(item)
def declared_digest(image):
    match=re.fullmatch(r"[^@\s]+@(sha256:[0-9a-f]{64})",str(image or ""))
    return match.group(1) if match else None
def runtime_digest(image_id):
    value=str(image_id or "")
    for prefix in ("docker-pullable://","containerd://"):
        if value.startswith(prefix): value=value[len(prefix):]; break
    match=re.fullmatch(r"(?:[^@\s]+@)?(sha256:[0-9a-f]{64})",value)
    return match.group(1) if match else None
for component in ("api","controller"):
    selected=by_component.get(component,[])
    if len(selected)!=1: raise SystemExit(f"legacy {component} deployment cardinality drifted")
    item=selected[0]; meta=item.get("metadata",{}); spec=item.get("spec",{}); state=item.get("status",{})
    replicas=spec.get("replicas")
    if (not isinstance(replicas,int) or replicas<1 or state.get("observedGeneration")!=meta.get("generation") or
        any(state.get(key)!=replicas for key in ("updatedReplicas","readyReplicas","availableReplicas"))):
        raise SystemExit(f"legacy {component} deployment is not converged")
    selected_pods=[p for p in pods if (p.get("metadata",{}).get("labels") or {}).get("app.kubernetes.io/component")==component]
    if len(selected_pods)!=replicas: raise SystemExit(f"legacy {component} pod cardinality drifted")
    template_containers=spec.get("template",{}).get("spec",{}).get("containers") or []
    expected=[container for container in template_containers if container.get("name")==component]
    if len(template_containers)!=1 or len(expected)!=1: raise SystemExit(f"legacy {component} deployment container identity drifted")
    expected_image=expected[0].get("image"); expected_digest=declared_digest(expected_image)
    if expected_digest is None: raise SystemExit(f"legacy {component} deployment image is not digest-pinned")
    for pod in selected_pods:
        pm=pod.get("metadata",{}); pod_spec=pod.get("spec",{}); ps=pod.get("status",{})
        pod_containers=pod_spec.get("containers") or []; statuses=ps.get("containerStatuses") or []
        pod_declared=[container for container in pod_containers if container.get("name")==component]
        pod_status=[container for container in statuses if container.get("name")==component]
        if (pm.get("deletionTimestamp") or ps.get("phase")!="Running" or len(pod_containers)!=1 or len(pod_declared)!=1 or
            pod_declared[0].get("image")!=expected_image or len(statuses)!=1 or len(pod_status)!=1 or
            not pod_status[0].get("ready") or runtime_digest(pod_status[0].get("imageID"))!=expected_digest):
            raise SystemExit(f"legacy {component} pod is not ready on the deployment image")
if not daemonsets: raise SystemExit("public data-plane daemonset inventory is empty")
daemonsets_by_name={x.get("metadata",{}).get("name"):x for x in daemonsets}
for item in daemonsets:
    meta=item.get("metadata",{}); state=item.get("status",{}); desired=state.get("desiredNumberScheduled",0)
    if state.get("observedGeneration")!=meta.get("generation") or state.get("numberUnavailable",0)!=0:
        raise SystemExit(f"public daemonset {meta.get('name')} is not observed/available")
    if desired>0 and (state.get("numberReady")!=desired or state.get("numberAvailable")!=desired):
        raise SystemExit(f"public daemonset {meta.get('name')} is not ready")
public_pods=[p for p in pods if (p.get("metadata",{}).get("labels") or {}).get("fugue.io/rollout-subsystem")=="public-data-plane"]
for pod in public_pods:
    meta=pod.get("metadata",{}); state=pod.get("status",{}); statuses=state.get("containerStatuses") or []
    if meta.get("deletionTimestamp") or state.get("phase")!="Running" or not statuses or any(not x.get("ready") for x in statuses):
        raise SystemExit(f"public data-plane pod {meta.get('name')} is not ready")
data=record.get("data") or {}
try: active_slots=json.loads(data.get("active_slots",""))
except Exception as exc: raise SystemExit(f"active slot record is invalid: {exc}")
if not isinstance(active_slots,dict) or not active_slots or any(v not in ("a","b") for v in active_slots.values()):
    raise SystemExit("active slot record is empty or ambiguous")
for base,slot in active_slots.items():
    for name in (f"{base}-front",f"{base}-worker-{slot}"):
        ds=daemonsets_by_name.get(name)
        if ds is None: raise SystemExit(f"active data-plane daemonset {name} is missing")
        state=ds.get("status",{}); desired=state.get("desiredNumberScheduled",0)
        if desired<1 or state.get("numberReady")!=desired or state.get("numberAvailable")!=desired:
            raise SystemExit(f"active data-plane daemonset {name} is not ready")
helm_status=(status.get("info") or {}).get("status") or status.get("status")
revision=status.get("version") or status.get("revision")
if helm_status!="deployed" or not isinstance(revision,int) or revision<1:
    raise SystemExit("legacy Helm release is not deployed")
record_meta=record.get("metadata",{})
record_uid=record_meta.get("uid"); record_version=record_meta.get("resourceVersion")
if not isinstance(record_uid,str) or not record_uid or not isinstance(record_version,str) or not record_version:
    raise SystemExit("public data-plane release record identity is incomplete")
material={"helm":{"revision":revision,"status":helm_status},"record":{"uid":record_uid,"resource_version":record_version,"data":data},"front_slots":front_observed,"workloads":[],"pods":[]}
for item in deployments+daemonsets:
    meta=item.get("metadata",{})
    material["workloads"].append({"kind":item.get("kind"),"name":meta.get("name"),"uid":meta.get("uid"),"generation":meta.get("generation"),"spec":item.get("spec")})
material["workloads"].sort(key=lambda x:(x["kind"],x["name"]))
for pod in pods:
    meta=pod.get("metadata",{}); labels=meta.get("labels") or {}
    component=labels.get("app.kubernetes.io/component")
    if component not in ("api","controller") and labels.get("fugue.io/rollout-subsystem")!="public-data-plane": continue
    statuses=[]
    spec_images={container.get("name"):container.get("image") for container in pod.get("spec",{}).get("containers") or []}
    for container in pod.get("status",{}).get("containerStatuses") or []:
        statuses.append({"name":container.get("name"),"spec_image":spec_images.get(container.get("name")),"status_image":container.get("image"),"image_id":container.get("imageID"),"ready":container.get("ready"),"restart_count":container.get("restartCount")})
    statuses.sort(key=lambda x:str(x["name"]))
    material["pods"].append({"name":meta.get("name"),"uid":meta.get("uid"),"component":component,"statuses":statuses})
material["pods"].sort(key=lambda x:str(x["name"]))
encoded=json.dumps(material,separators=(",",":"),sort_keys=True).encode()
value={"schema":"edge-control-shadow-legacy-snapshot/v1","status":"healthy","digest":"sha256:"+hashlib.sha256(encoded).hexdigest(),"helm_revision":revision,"active_slots":active_slots,"front_slots":front_observed,"public_pods":len(public_pods)}
with open(output,"x",encoding="utf-8") as handle: json.dump(value,handle,separators=(",",":"),sort_keys=True);handle.write("\n")
PY
}

verify_edge_control() {
  local output="$1"
  local expected_source="${2:-${EXPECTED_SOURCE}}"
  local expected_repository="${3:-${IMAGE}}"
  local expected_digest="${4:-${IMAGE_DIGEST}}"
  local objects pods values status pod_name status_body metrics_body
  objects="${output}.objects.json"; pods="${output}.pods.json"; values="${output}.values.json"; status="${output}.helm.json"
  kube -n "${NAMESPACE}" get serviceaccount,deployment,service,poddisruptionbudget,networkpolicy \
    -l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=edge-control" -o json >"${objects}"
  kube -n "${NAMESPACE}" get pods \
    -l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=edge-control" -o json >"${pods}"
  helm get values "${RELEASE}" -n "${NAMESPACE}" -o json >"${values}"
  helm status "${RELEASE}" -n "${NAMESPACE}" -o json >"${status}"
  pod_name="$(python3 - "${pods}" <<'PY'
import json,sys
items=[x for x in json.load(open(sys.argv[1],encoding="utf-8")).get("items",[]) if not x.get("metadata",{}).get("deletionTimestamp")]
if len(items)!=1: raise SystemExit("edge-control live pod cardinality drifted")
print(items[0]["metadata"]["name"])
PY
  )"
  status_body="$(kube -n "${NAMESPACE}" exec "pod/${pod_name}" -c edge-control -- wget -qO- http://127.0.0.1:8092/v1/status)"
  metrics_body="$(kube -n "${NAMESPACE}" exec "pod/${pod_name}" -c edge-control -- wget -qO- http://127.0.0.1:8092/metrics)"
  STATUS_BODY="${status_body}" METRICS_BODY="${metrics_body}" EXPECTED_SOURCE="${expected_source}" \
    EXPECTED_REPOSITORY="${expected_repository}" EXPECTED_IMAGE="${expected_repository}@${expected_digest}" \
    EXPECTED_DIGEST="${expected_digest}" python3 - \
    "${objects}" "${pods}" "${values}" "${status}" "${output}" <<'PY'
import hashlib,json,os,re,sys
with open(sys.argv[1],encoding="utf-8") as handle: objects=json.load(handle)
with open(sys.argv[2],encoding="utf-8") as handle: pods=json.load(handle)
with open(sys.argv[3],encoding="utf-8") as handle: values=json.load(handle)
with open(sys.argv[4],encoding="utf-8") as handle: status=json.load(handle)
output=sys.argv[5]
items=objects.get("items",[]); by_kind={}
for item in items: by_kind.setdefault(item.get("kind"),[]).append(item)
expected={"ServiceAccount":1,"Deployment":1,"Service":1,"PodDisruptionBudget":1,"NetworkPolicy":1}
if {k:len(v) for k,v in by_kind.items()}!=expected: raise SystemExit("edge-control object inventory drifted")
deployment=by_kind["Deployment"][0]; meta=deployment.get("metadata",{}); spec=deployment.get("spec",{}); state=deployment.get("status",{})
template=spec.get("template",{}); annotations=template.get("metadata",{}).get("annotations") or {}; podspec=template.get("spec",{})
want_annotations={"fugue.pro/source-commit":os.environ["EXPECTED_SOURCE"],"fugue.pro/image-digest":os.environ["EXPECTED_DIGEST"],"fugue.pro/edge-control-authority":"none","fugue.pro/edge-control-mode":"boundary-only","fugue.pro/edge-control-publication":"disabled"}
if any(annotations.get(k)!=v for k,v in want_annotations.items()): raise SystemExit("edge-control authority/source annotations drifted")
if (spec.get("replicas")!=1 or state.get("observedGeneration")!=meta.get("generation") or
    any(state.get(key)!=1 for key in ("updatedReplicas","readyReplicas","availableReplicas"))):
    raise SystemExit("edge-control deployment is not converged")
containers=podspec.get("containers") or []
if len(containers)!=1 or containers[0].get("name")!="edge-control" or containers[0].get("image")!=os.environ["EXPECTED_IMAGE"]:
    raise SystemExit("edge-control container identity drifted")
container=containers[0]
env={x.get("name"):x.get("value") for x in container.get("env") or []}
if env!={"FUGUE_EDGE_CONTROL_ENABLED":"true","FUGUE_EDGE_CONTROL_BIND_ADDR":"0.0.0.0:8092","FUGUE_EDGE_CONTROL_SHUTDOWN_TIMEOUT":"10s"}:
    raise SystemExit("edge-control environment gained capability")
if (container.get("envFrom") or container.get("volumeMounts") or podspec.get("volumes") or podspec.get("automountServiceAccountToken") is not False):
    raise SystemExit("edge-control gained credential or volume capability")
sa=by_kind["ServiceAccount"][0]
if sa.get("automountServiceAccountToken") is not False: raise SystemExit("edge-control service account token is enabled")
service=by_kind["Service"][0].get("spec",{})
if service.get("type")!="ClusterIP" or service.get("externalIPs") or service.get("externalName"):
    raise SystemExit("edge-control service escaped cluster-local boundary")
policy=by_kind["NetworkPolicy"][0].get("spec",{})
if policy.get("egress")!=[] or policy.get("policyTypes")!=["Ingress","Egress"]:
    raise SystemExit("edge-control egress boundary drifted")
live=[x for x in pods.get("items",[]) if not x.get("metadata",{}).get("deletionTimestamp")]
if len(live)!=1: raise SystemExit("edge-control live pod cardinality drifted")
pod=live[0]; pm=pod.get("metadata",{}); live_spec=pod.get("spec",{}); ps=pod.get("status",{})
live_containers=live_spec.get("containers") or []; statuses=ps.get("containerStatuses") or []
live_declared=[item for item in live_containers if item.get("name")=="edge-control"]
live_status=[item for item in statuses if item.get("name")=="edge-control"]
def runtime_digest(image_id):
    value=str(image_id or "")
    for prefix in ("docker-pullable://","containerd://"):
        if value.startswith(prefix): value=value[len(prefix):]; break
    match=re.fullmatch(r"(?:[^@\s]+@)?(sha256:[0-9a-f]{64})",value)
    return match.group(1) if match else None
if (ps.get("phase")!="Running" or len(live_containers)!=1 or len(live_declared)!=1 or
    live_declared[0].get("image")!=os.environ["EXPECTED_IMAGE"] or len(statuses)!=1 or len(live_status)!=1 or
    not live_status[0].get("ready") or live_status[0].get("restartCount")!=0 or
    runtime_digest(live_status[0].get("imageID"))!=os.environ["EXPECTED_DIGEST"] or
    any((pm.get("annotations") or {}).get(k)!=v for k,v in want_annotations.items())):
    raise SystemExit("edge-control pod is not pristine and ready")
expected_values={"enabled":True,"image":{"digest":os.environ["EXPECTED_DIGEST"],"repository":os.environ["EXPECTED_REPOSITORY"],"sourceCommit":os.environ["EXPECTED_SOURCE"]}}
if values!=expected_values: raise SystemExit("edge-control Helm values drifted")
helm_status=(status.get("info") or {}).get("status") or status.get("status"); revision=status.get("version") or status.get("revision")
if helm_status!="deployed" or not isinstance(revision,int) or revision<1: raise SystemExit("edge-control Helm release is not deployed")
runtime=json.loads(os.environ["STATUS_BODY"])
if runtime!={"schema":"edge-control-boundary/v1","status":"ok","mode":"boundary-only","authority":"none","enabled":True,"publication_enabled":False,"data_plane_dependency":False,"database_capability":False,"kubernetes_capability":False,"bundle_signer_capability":False}:
    raise SystemExit("edge-control runtime boundary drifted")
if 'authority="none",mode="boundary-only"' not in os.environ["METRICS_BODY"]:
    raise SystemExit("edge-control boundary metric drifted")
material={"deployment":{"uid":meta.get("uid"),"generation":meta.get("generation"),"spec":spec},"pod":{"uid":pm.get("uid"),"spec_image":live_declared[0].get("image"),"status_image":live_status[0].get("image"),"image_id":live_status[0].get("imageID"),"restart_count":live_status[0].get("restartCount")},"helm_revision":revision,"runtime":runtime}
digest="sha256:"+hashlib.sha256(json.dumps(material,separators=(",",":"),sort_keys=True).encode()).hexdigest()
value={"schema":"edge-control-shadow-runtime-snapshot/v1","status":"healthy","authority":"none","mode":"boundary-only","publication_enabled":False,"data_plane_dependency":False,"digest":digest,"helm_revision":revision,"deployment_uid":meta.get("uid"),"deployment_generation":meta.get("generation"),"pod_uid":pm.get("uid"),"pod_name":pm.get("name"),"pod_restart_count":0}
with open(output,"x",encoding="utf-8") as handle:json.dump(value,handle,separators=(",",":"),sort_keys=True);handle.write("\n")
PY
}

legacy_before="${WORK_DIR}/legacy-before.json"
legacy_after="${WORK_DIR}/legacy-after.json"
runtime_final="${WORK_DIR}/runtime-final.json"
candidate="${WORK_DIR}/candidate.yaml"
snapshot_legacy "${legacy_before}"
curl -fsS --max-time 10 "${PRODUCT_HEALTH_URL}" >/dev/null || fail 'product health preflight failed'

prestate='absent'
previous_revision=0
previous_source=''
previous_image=''
previous_image_digest=''
previous_runtime_digest=''
if helm status "${RELEASE}" -n "${NAMESPACE}" -o json >"${WORK_DIR}/prestate.json" 2>"${WORK_DIR}/prestate.err"; then
  prestate='deployed'
  helm get values "${RELEASE}" -n "${NAMESPACE}" -o json >"${WORK_DIR}/prestate-values.json"
  previous_fields="$(python3 - "${WORK_DIR}/prestate-values.json" <<'PY'
import json,re,sys
value=json.load(open(sys.argv[1],encoding="utf-8")); image=value.get("image") or {}
source=image.get("sourceCommit"); repository=image.get("repository"); digest=image.get("digest")
if (value.get("enabled") is not True or re.fullmatch(r"[0-9a-f]{40}",str(source)) is None or
    re.fullmatch(r"[a-z0-9._-]+(?::[0-9]+)?(?:/[a-z0-9._-]+)+",str(repository)) is None or
    re.fullmatch(r"sha256:[0-9a-f]{64}",str(digest)) is None):
    raise SystemExit("existing edge-control Helm values are not canonical")
print(f"{source}\t{repository}\t{digest}")
PY
  )"
  IFS=$'\t' read -r previous_source previous_image previous_image_digest extra <<<"${previous_fields}"
  [[ -z "${extra:-}" ]]
  verify_edge_control "${WORK_DIR}/runtime-before.json" "${previous_source}" "${previous_image}" "${previous_image_digest}"
  previous_revision="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["helm_revision"])' "${WORK_DIR}/runtime-before.json")"
  previous_runtime_digest="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["digest"])' "${WORK_DIR}/runtime-before.json")"
else
  grep -Eqi 'release(: | ")?not found|not found' "${WORK_DIR}/prestate.err" || fail 'could not classify edge-control Helm prestate'
  count="$(kube -n "${NAMESPACE}" get serviceaccount,deployment,service,poddisruptionbudget,networkpolicy \
    -l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=edge-control" -o name | wc -l | tr -d ' ')"
  [[ "${count}" == '0' ]] || fail 'unowned edge-control objects exist without a Helm release'
fi

helm lint "${CHART}" >/dev/null
helm template "${RELEASE}" "${CHART}" -n "${NAMESPACE}" \
  --set enabled=true --set-string "image.repository=${IMAGE}" --set-string "image.digest=${IMAGE_DIGEST}" \
  --set-string "image.sourceCommit=${EXPECTED_SOURCE}" >"${candidate}"
for forbidden in '^kind: (Secret|Role|RoleBinding|ClusterRole|ClusterRoleBinding|Ingress|PersistentVolume|PersistentVolumeClaim)$' 'hostPath:' 'secretKeyRef:' 'configMapKeyRef:'; do
  if grep -Eq "${forbidden}" "${candidate}"; then fail "rendered candidate gained forbidden capability: ${forbidden}"; fi
done
kube apply --dry-run=server --validate=strict -f "${candidate}" >/dev/null

if [[ "${prestate}" == 'absent' ]]; then
  if helm status "${RELEASE}" -n "${NAMESPACE}" >/dev/null 2>"${WORK_DIR}/prewrite-status.err"; then
    fail 'edge-control Helm release appeared after the absent preflight'
  fi
  grep -Eqi 'release(: | ")?not found|not found' "${WORK_DIR}/prewrite-status.err" || fail 'could not reconcile absent prestate before write'
  count="$(kube -n "${NAMESPACE}" get serviceaccount,deployment,service,poddisruptionbudget,networkpolicy \
    -l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=edge-control" -o name | wc -l | tr -d ' ')"
  [[ "${count}" == '0' ]] || fail 'edge-control objects appeared after the absent preflight'
else
  verify_edge_control "${WORK_DIR}/runtime-prewrite.json" "${previous_source}" "${previous_image}" "${previous_image_digest}"
  prewrite_revision="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["helm_revision"])' "${WORK_DIR}/runtime-prewrite.json")"
  [[ "${prewrite_revision}" == "${previous_revision}" ]] || fail 'edge-control Helm revision changed before write'
  prewrite_digest="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["digest"])' "${WORK_DIR}/runtime-prewrite.json")"
  [[ "${prewrite_digest}" == "${previous_runtime_digest}" ]] || fail 'edge-control runtime spec changed before write'
fi
verify_github_prewrite

log "deploying immutable boundary image ${IMAGE}@${IMAGE_DIGEST}"
helm_failure_guard=(--atomic)
if helm upgrade --help | grep -q -- '--rollback-on-failure'; then
  helm_failure_guard=(--rollback-on-failure)
fi
set +e
helm upgrade --install "${RELEASE}" "${CHART}" -n "${NAMESPACE}" \
  --reset-values --history-max 5 "${helm_failure_guard[@]}" --wait --timeout 5m \
  --set enabled=true --set-string "image.repository=${IMAGE}" --set-string "image.digest=${IMAGE_DIGEST}" \
  --set-string "image.sourceCommit=${EXPECTED_SOURCE}"
helm_status=$?
set -e

if ! verify_edge_control "${WORK_DIR}/runtime-initial.json"; then
  (( helm_status == 0 )) || fail "Helm failed with status ${helm_status} and exact readback did not converge"
  fail 'Helm returned success but exact edge-control readback failed'
fi
if (( helm_status != 0 )); then
  log "Helm response status ${helm_status} was reconciled by exact converged readback; no retry was attempted"
fi

samples=0
for sample in 1 2 3 4 5 6; do
  verify_edge_control "${WORK_DIR}/runtime-soak-${sample}.json"
  snapshot_legacy "${WORK_DIR}/legacy-soak-${sample}.json"
  before_digest="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["digest"])' "${legacy_before}")"
  sample_digest="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["digest"])' "${WORK_DIR}/legacy-soak-${sample}.json")"
  [[ "${sample_digest}" == "${before_digest}" ]] || fail 'legacy Core/Edge spec changed during shadow soak'
  curl -fsS --max-time 10 "${PRODUCT_HEALTH_URL}" >/dev/null || fail 'product health failed during shadow soak'
  samples=$sample
  (( sample == 6 )) || sleep 20
done
cp "${WORK_DIR}/runtime-soak-6.json" "${runtime_final}"
cp "${WORK_DIR}/legacy-soak-6.json" "${legacy_after}"

EXPECTED_SOURCE="${EXPECTED_SOURCE}" IMAGE="${IMAGE}" IMAGE_DIGEST="${IMAGE_DIGEST}" \
IMAGE_RECEIPT_DIGEST="${IMAGE_RECEIPT_DIGEST}" SOURCE_RUN_ID="${SOURCE_RUN_ID}" \
SOURCE_ARTIFACT_ID="${SOURCE_ARTIFACT_ID}" SOURCE_ARTIFACT_DIGEST="${SOURCE_ARTIFACT_DIGEST}" \
GITHUB_RUN_ID_VALUE="${GITHUB_RUN_ID:-0}" GITHUB_RUN_ATTEMPT_VALUE="${GITHUB_RUN_ATTEMPT:-1}" \
PRESTATE="${prestate}" PREVIOUS_REVISION="${previous_revision}" HELM_STATUS_VALUE="${helm_status}" \
PREVIOUS_SOURCE="${previous_source}" PREVIOUS_IMAGE="${previous_image}" \
PREVIOUS_IMAGE_DIGEST="${previous_image_digest}" PREVIOUS_RUNTIME_DIGEST="${previous_runtime_digest}" \
SAMPLES="${samples}" NAMESPACE="${NAMESPACE}" RELEASE="${RELEASE}" python3 - \
  "${legacy_before}" "${legacy_after}" "${runtime_final}" "${RECEIPT_PATH}" <<'PY'
import datetime,hashlib,json,os,sys
with open(sys.argv[1],encoding="utf-8") as handle: before=json.load(handle)
with open(sys.argv[2],encoding="utf-8") as handle: after=json.load(handle)
with open(sys.argv[3],encoding="utf-8") as handle: runtime=json.load(handle)
output=sys.argv[4]
if before["digest"]!=after["digest"]: raise SystemExit("legacy snapshot digest changed")
value={
 "schema":"edge-control-shadow-release-receipt/v1","recorded_at":datetime.datetime.now(datetime.timezone.utc).isoformat(),
 "source_commit":os.environ["EXPECTED_SOURCE"],"image":os.environ["IMAGE"],"image_digest":os.environ["IMAGE_DIGEST"],
 "image_receipt_digest":os.environ["IMAGE_RECEIPT_DIGEST"],"source_run_id":int(os.environ["SOURCE_RUN_ID"]),
 "source_artifact_id":int(os.environ["SOURCE_ARTIFACT_ID"]),"source_artifact_digest":os.environ["SOURCE_ARTIFACT_DIGEST"],
 "deploy_run_id":int(os.environ["GITHUB_RUN_ID_VALUE"]),"deploy_run_attempt":int(os.environ["GITHUB_RUN_ATTEMPT_VALUE"]),
 "namespace":os.environ["NAMESPACE"],"release":os.environ["RELEASE"],"prestate":os.environ["PRESTATE"],
 "previous_revision":int(os.environ["PREVIOUS_REVISION"]),"previous_source_commit":os.environ["PREVIOUS_SOURCE"] or None,
 "previous_image":os.environ["PREVIOUS_IMAGE"] or None,"previous_image_digest":os.environ["PREVIOUS_IMAGE_DIGEST"] or None,
 "previous_runtime_digest":os.environ["PREVIOUS_RUNTIME_DIGEST"] or None,"helm_response_status":int(os.environ["HELM_STATUS_VALUE"]),
 "helm_revision":runtime["helm_revision"],"authority":"none","mode":"boundary-only","publication_enabled":False,
 "data_plane_dependency":False,"database_capability":False,"kubernetes_capability":False,"bundle_signer_capability":False,
 "legacy_spec_digest_before":before["digest"],"legacy_spec_digest_after":after["digest"],
 "runtime_digest":runtime["digest"],"deployment_uid":runtime["deployment_uid"],"deployment_generation":runtime["deployment_generation"],
 "pod_uid":runtime["pod_uid"],"pod_restart_count":runtime["pod_restart_count"],"soak_samples":int(os.environ["SAMPLES"]),
 "receipt_digest":"",
}
encoded=dict(value);encoded["receipt_digest"]=""
value["receipt_digest"]="sha256:"+hashlib.sha256(json.dumps(encoded,separators=(",",":"),sort_keys=True).encode()).hexdigest()
with open(output,"x",encoding="utf-8") as handle:json.dump(value,handle,separators=(",",":"),sort_keys=True);handle.write("\n")
PY
chmod 0600 "${RECEIPT_PATH}"
log "shadow release converged with authority=none and ${samples} soak samples"
