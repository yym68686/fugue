#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT
LOG="${TMP}/transaction.log"
PLAN="${TMP}/plan.json"
HEAD_SHA="$(git -C "${ROOT}" rev-parse HEAD)"
CURRENT_SOURCE="$(printf '2%.0s' {1..40})"
ADOPTED_SOURCE="$(printf '3%.0s' {1..40})"
IMAGE_DIGEST="sha256:$(printf 'a%.0s' {1..64})"

PLAN="${PLAN}" HEAD_SHA="${HEAD_SHA}" CURRENT_SOURCE="${CURRENT_SOURCE}" \
  ADOPTED_SOURCE="${ADOPTED_SOURCE}" IMAGE_DIGEST="${IMAGE_DIGEST}" python3 - <<'PY'
import json
import os

plan = {
    "policy": "control-plane-hotfix-baseline-adoption-v1",
    "expectedSha": os.environ["HEAD_SHA"],
    "runId": "30733955954",
    "runAttempt": 1,
    "namespace": "fugue-system",
    "releaseName": "fugue",
    "releaseFullname": "fugue-fugue",
    "baseRevision": 806,
    "targetRevision": 807,
    "currentSource": os.environ["CURRENT_SOURCE"],
    "adoptedSource": os.environ["ADOPTED_SOURCE"],
    "liveImageRef": "ghcr.io/example/fugue-api@" + os.environ["IMAGE_DIGEST"],
}
with open(os.environ["PLAN"], "w", encoding="utf-8") as stream:
    json.dump(plan, stream, separators=(",", ":"))
PY

(
  cd "${ROOT}"
  export FUGUE_UPGRADE_LIB_ONLY=true
  # shellcheck source=scripts/upgrade_fugue_control_plane.sh
  source "${ROOT}/scripts/upgrade_fugue_control_plane.sh"

  record() { printf '%s|%s\n' "$$" "$1" >>"${LOG}"; }
  initialize_control_plane_release_job_deadline() { record deadline; }
  detect_kubectl() { printf kubectl; }
  control_plane_hotfix_capture_render_set() { mkdir -p "$1"; record capture; }
  control_plane_hotfix_verify_prewrite_bindings() { record "bindings:$2"; }
  control_plane_hotfix_verify_kubernetes() { record "kubernetes:$1"; }
  control_plane_hotfix_write_wal() { record "wal:$1"; }
  control_plane_hotfix_publish_terminal_evidence() { record "publish:$1"; }
  acquire_control_plane_backup_coordination_lease() {
    CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD=true
    CONTROL_PLANE_BACKUP_COORDINATION_LEASE_OWNER="release/${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
    CONTROL_PLANE_BACKUP_COORDINATION_LEASE_TOKEN=0123456789abcdef0123456789abcdef
    record acquire
  }
  require_control_plane_backup_coordination_or_abort() { record require; }
  arm_control_plane_release_recovery_fence() { record arm; }
  control_plane_release_verify_repository_snapshot() { :; }
  control_plane_release_domain_execute_sealed_helm_upgrade() {
    [[ "${CONTROL_PLANE_RELEASE_DOMAIN_ARGV_FD_READY}" == true ]]
    ARGV_OUTPUT="${TMP}/argv" python3 - 16 <<'PY'
import os

raw = os.read(16, 65536)
if not raw.endswith(b"\0"):
    raise SystemExit(1)
argv = [item.decode() for item in raw[:-1].split(b"\0")]
expected = [
    "helm", "upgrade", "fugue", "deploy/helm/fugue", "-n", "fugue-system",
    "--reset-then-reuse-values", "--no-hooks", "--history-max", "20",
    "--timeout", "10m0s", "--wait", "--set-string",
    "api.image.tag=" + os.environ["ADOPTED_SOURCE"], "--set-string",
    "api.image.digest=" + os.environ["IMAGE_DIGEST"],
]
if argv != expected:
    raise SystemExit(1)
with open(os.environ["ARGV_OUTPUT"], "w", encoding="utf-8") as stream:
    stream.write("\n".join(argv) + "\n")
PY
    exec 16<&-
    CONTROL_PLANE_RELEASE_DOMAIN_ARGV_FD_READY=false
    record execute
  }
  control_plane_hotfix_verify_live_target() { record "verify:$1:$2:$3"; }
  release_control_plane_backup_coordination_lease() {
    CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD=false
    record release
  }

  export RUNNER_TEMP="${TMP}" LOG TMP ADOPTED_SOURCE IMAGE_DIGEST
  run_control_plane_hotfix_baseline_adoption <"${PLAN}"
)

EXPECTED=$'deadline\ncapture\nbindings:released\nkubernetes:base\nwal:prepared\nacquire\nrequire\ncapture\nbindings:owned\nkubernetes:base\nwal:prewrite-verified\narm\nwal:forward-started\nexecute\nverify:target:807:target.yaml\nwal:forward-committed\nwal:verified\nrelease'
ACTUAL="$(cut -d'|' -f2- "${LOG}")"
[[ "${ACTUAL}" == "${EXPECTED}" ]]
[[ "$(cut -d'|' -f1 "${LOG}" | sort -u | wc -l | tr -d ' ')" == 1 ]]
[[ "$(grep -c '^api.image.tag=' "${TMP}/argv")" == 1 ]]
[[ "$(grep -c '^api.image.digest=' "${TMP}/argv")" == 1 ]]
grep -Fq 'base:released|base:owned|target:target|hybrid:hybrid' \
  "${ROOT}/scripts/upgrade_fugue_control_plane.sh"
! grep -Fq 'local directory="${CONTROL_PLANE_HOTFIX_WORK_DIR}/kubernetes-${phase}"' \
  "${ROOT}/scripts/upgrade_fugue_control_plane.sh"

(
  cd "${ROOT}"
  export FUGUE_UPGRADE_LIB_ONLY=true
  # shellcheck source=scripts/upgrade_fugue_control_plane.sh
  source "${ROOT}/scripts/upgrade_fugue_control_plane.sh"
  live_source=a0f5bc0ac36b4e29c4c7928dda1923c2c4727759
  target_source=57dc767999741cea25fe4820a6c9603984dfa0b9
  live_image=ghcr.io/yym68686/fugue-api@sha256:7eb7e7682d44c3f283cd347e032de6fac2f6304221fbf72dfa788845950ccfd9
  target_image=ghcr.io/yym68686/fugue-api@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  CONTROL_PLANE_HOTFIX_LIVE_API_TEMPLATE_JSON="$(LIVE_SOURCE="${live_source}" LIVE_IMAGE="${live_image}" python3 - <<'PY'
import json, os
print(json.dumps({
    "metadata":{"annotations":{"fugue.io/emergency-hotfix":"true","fugue.pro/source-commit":os.environ["LIVE_SOURCE"]}},
    "spec":{"containers":[{"image":os.environ["LIVE_IMAGE"],"imagePullPolicy":"IfNotPresent","name":"api"}],"dnsPolicy":"ClusterFirst","restartPolicy":"Always"},
},sort_keys=True,separators=(",",":")))
PY
)"
  PUBLIC_DATA_PLANE_CHECKSUMS_JSON='{}'
  NODE_LOCAL_BUILD_PLANE_PREFLIGHT_OVERRIDE_USED=false
  prepare_helm_post_renderer
  raw="${TMP}/api-template-raw.yaml"
  rendered="${TMP}/api-template-rendered.yaml"
  cat >"${raw}" <<'YAML'
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fugue-fugue-api
  namespace: fugue-system
spec:
  replicas: 2
  template:
    metadata:
      annotations:
        fugue.pro/source-commit: 57dc767999741cea25fe4820a6c9603984dfa0b9
    spec:
      containers:
      - image: ghcr.io/yym68686/fugue-api@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
        name: api
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: untouched
  namespace: fugue-system
data:
  value: exact
YAML
  "${HELM_POST_RENDERER_FILE}" <"${raw}" >"${rendered}"
  RENDERED="${rendered}" TARGET_SOURCE="${target_source}" TARGET_IMAGE="${target_image}" python3 - <<'PY'
import json, os, re
raw=open(os.environ["RENDERED"],encoding="utf-8").read()
match=re.search(r'^  template: (\{.*\})$',raw,re.MULTILINE)
assert match
template=json.loads(match.group(1))
assert template["metadata"]["annotations"] == {"fugue.io/emergency-hotfix":"true","fugue.pro/source-commit":os.environ["TARGET_SOURCE"]}
assert template["spec"] == {"containers":[{"image":os.environ["TARGET_IMAGE"],"imagePullPolicy":"IfNotPresent","name":"api"}],"dnsPolicy":"ClusterFirst","restartPolicy":"Always"}
assert raw.endswith('data:\n  value: exact\n')
PY
  rm -f -- "${HELM_POST_RENDERER_FILE}"
)

(
  cd "${ROOT}"
  export FUGUE_UPGRADE_LIB_ONLY=true
  # shellcheck source=scripts/upgrade_fugue_control_plane.sh
  source "${ROOT}/scripts/upgrade_fugue_control_plane.sh"
  fixture="${TMP}/kubernetes-observation-fixture"
  mkdir -m 700 "${fixture}"
  cat >"${fixture}/deployment.json" <<'JSON'
{"metadata":{"generation":7,"name":"fugue-fugue-api","resourceVersion":"11","uid":"api-uid"},"spec":{"replicas":2,"template":{"metadata":{"annotations":{"fugue.pro/source-commit":"a0f5bc0ac36b4e29c4c7928dda1923c2c4727759"}},"spec":{"containers":[{"image":"ghcr.io/yym68686/fugue-api@sha256:7eb7e7682d44c3f283cd347e032de6fac2f6304221fbf72dfa788845950ccfd9","name":"api"}]}}},"status":{"availableReplicas":2,"observedGeneration":7,"readyReplicas":2,"replicas":2,"updatedReplicas":2}}
JSON
  cat >"${fixture}/service.json" <<'JSON'
{"metadata":{"name":"fugue-fugue","resourceVersion":"21","uid":"service-uid"},"spec":{"selector":{"app":"api"}}}
JSON
  cat >"${fixture}/slices.json" <<'JSON'
{"items":[{"addressType":"IPv4","endpoints":[{"addresses":["10.0.0.1"],"conditions":{"ready":true,"serving":true},"targetRef":{"name":"api-1"}},{"addresses":["10.0.0.2"],"conditions":{"ready":true,"serving":true},"targetRef":{"name":"api-2"}}],"metadata":{"name":"fugue-fugue-a","resourceVersion":"31","uid":"slice-uid"},"ports":[{"port":8080}]}]}
JSON
  cat >"${fixture}/pods.json" <<'JSON'
{"items":[{"status":{"conditions":[{"status":"True","type":"Ready"}],"containerStatuses":[{"imageID":"docker-pullable://ghcr.io/yym68686/fugue-api@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","name":"api"}],"phase":"Running"}},{"status":{"conditions":[{"status":"True","type":"Ready"}],"containerStatuses":[{"imageID":"docker-pullable://ghcr.io/yym68686/fugue-api@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","name":"api"}],"phase":"Running"}}]}
JSON
  printf 'ok\n' >"${fixture}/health"
  FIXTURE="${fixture}" python3 - <<'PY'
import hashlib,json,os,pathlib
r=pathlib.Path(os.environ["FIXTURE"]); load=lambda n:json.load(open(r/n))
digest=lambda v:"sha256:"+hashlib.sha256(json.dumps(v,sort_keys=True,separators=(",",":")).encode()).hexdigest()
d=load("deployment.json");s=load("service.json");sl=load("slices.json")["items"][0]
records=[]
for e in sl["endpoints"]:
 records.append({"addresses":sorted(e.get("addresses") or []),"conditions":e.get("conditions") or {},"nodeName":e.get("nodeName"),"targetRef":e.get("targetRef"),"zone":e.get("zone")})
records.sort(key=lambda v:json.dumps(v,sort_keys=True,separators=(",",":")))
p={"planVersion":2,"currentSource":"a0f5bc0ac36b4e29c4c7928dda1923c2c4727759","adoptedSource":"57dc767999741cea25fe4820a6c9603984dfa0b9","targetApiImageRef":"ghcr.io/yym68686/fugue-api@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","liveHybridApiImageRef":"ghcr.io/yym68686/fugue-api@sha256:7eb7e7682d44c3f283cd347e032de6fac2f6304221fbf72dfa788845950ccfd9","targetApiTemplateDigest":"sha256:"+"b"*64,"hybridApiTemplateDigest":digest(d["spec"]["template"]),"kubernetes":{"apiName":"fugue-fugue-api","apiUid":"api-uid","apiResourceVersion":"11","apiGeneration":7,"apiTemplateDigest":digest(d["spec"]["template"]),"apiImageId":"docker-pullable://ghcr.io/yym68686/fugue-api@sha256:"+"e"*64,"apiHealthDigest":"sha256:"+hashlib.sha256((r/"health").read_bytes()).hexdigest(),"serviceName":"fugue-fugue","serviceUid":"service-uid","serviceResourceVersion":"21","serviceSelectorDigest":digest(s["spec"]["selector"]),"endpointSliceName":"fugue-fugue-a","endpointSliceUid":"slice-uid","endpointSliceResourceVersion":"31","endpointBindingDigest":digest({"addressType":sl["addressType"],"endpoints":records,"ports":sl["ports"]})}}
(r/"plan.json").write_text(json.dumps(p,separators=(",",":")),encoding="utf-8")
PY
  calls="${fixture}/calls"
  : >"${calls}"
  bounded_kubectl() {
    printf '%s\n' "$*" >>"${calls}"
    case "$*" in
      *'get deployment/fugue-fugue-api -o json') cat "${fixture}/deployment.json" ;;
      *'get service/fugue-fugue -o json') cat "${fixture}/service.json" ;;
      *'get endpointslice -l kubernetes.io/service-name=fugue-fugue -o json') cat "${fixture}/slices.json" ;;
      *'get pods -l app.kubernetes.io/instance=fugue,app.kubernetes.io/component=api -o json') cat "${fixture}/pods.json" ;;
      *) return 1 ;;
    esac
  }
  run_with_wall_timeout() { cat "${fixture}/health"; }
  CONTROL_PLANE_HOTFIX_PLAN_FILE="${fixture}/plan.json"
  CONTROL_PLANE_HOTFIX_WORK_DIR="${fixture}/positive"
  FUGUE_SMOKE_URL=https://api.example.test/healthz
  mkdir -m 700 "${CONTROL_PLANE_HOTFIX_WORK_DIR}"
  control_plane_hotfix_verify_kubernetes base released
  control_plane_hotfix_verify_kubernetes base owned
  [[ -d "${CONTROL_PLANE_HOTFIX_WORK_DIR}/kubernetes-base-released" && -d "${CONTROL_PLANE_HOTFIX_WORK_DIR}/kubernetes-base-owned" ]]
  [[ "$(wc -l <"${calls}" | tr -d ' ')" == 8 ]]
  ! control_plane_hotfix_verify_kubernetes base released
  CONTROL_PLANE_HOTFIX_WORK_DIR="${fixture}/poison"
  mkdir -m 700 "${CONTROL_PLANE_HOTFIX_WORK_DIR}"
  ln -s "${fixture}" "${CONTROL_PLANE_HOTFIX_WORK_DIR}/kubernetes-base-owned"
  ! control_plane_hotfix_verify_kubernetes base owned
)

(
  cd "${ROOT}"
  export FUGUE_UPGRADE_LIB_ONLY=true
  # shellcheck source=scripts/upgrade_fugue_control_plane.sh
  source "${ROOT}/scripts/upgrade_fugue_control_plane.sh"
  contract="${TMP}/post-render-contract"
  mkdir -m 700 "${contract}"
  printf 'raw-target\n' >"${contract}/raw-target"
  printf 'raw-hybrid\n' >"${contract}/raw-hybrid"
  printf 'effective-target\n' >"${contract}/effective-target"
  printf 'effective-hybrid\n' >"${contract}/effective-hybrid"
  inner="${contract}/restore-nine-edge-objects"
  write_inner() { cat >"${inner}" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
value="$(cat)"
case "${value}" in
  raw-target) printf 'effective-target\n' ;;
  raw-hybrid) printf 'effective-hybrid\n' ;;
  *) exit 1 ;;
esac
SH
    chmod 700 "${inner}"
  }
  write_inner
  digest_file() { printf 'sha256:%s' "$(shasum -a 256 "$1" | awk '{print $1}')"; }
  restore_digest="$(digest_file "${inner}")"
  plan="${contract}/plan.json"
  PLAN="${plan}" RAW_TARGET="$(digest_file "${contract}/raw-target")" \
    RAW_HYBRID="$(digest_file "${contract}/raw-hybrid")" \
    TARGET="$(digest_file "${contract}/effective-target")" \
    HYBRID="$(digest_file "${contract}/effective-hybrid")" python3 - <<'PY'
import json,os
json.dump({"rawTargetManifestDigest":os.environ["RAW_TARGET"],"rawHybridManifestDigest":os.environ["RAW_HYBRID"],"targetPostRenderDigest":os.environ["TARGET"],"hybridPostRenderDigest":os.environ["HYBRID"]},open(os.environ["PLAN"],"w"),separators=(",",":"))
PY
  CONTROL_PLANE_HOTFIX_PLAN_VERSION=2
  CONTROL_PLANE_HOTFIX_PLAN_FILE="${plan}"
  CONTROL_PLANE_HOTFIX_EDGE_RESTORE_PLAN_DIGEST="${restore_digest}"
  control_plane_hotfix_prepare_post_renderer() {
    write_inner
    HELM_POST_RENDERER_FILE="${inner}"
    HELM_POST_RENDERER_ARGS=(--post-renderer "${inner}")
  }
  mkdir -m 700 "${contract}/forward" "${contract}/compensation"
  control_plane_hotfix_prepare_transaction_post_renderer forward "${contract}/forward"
  cmp -s "${contract}/effective-target" <("${HELM_POST_RENDERER_FILE}" <"${contract}/raw-target")
  ! printf 'raw-target-drift\n' | "${HELM_POST_RENDERER_FILE}" >/dev/null
  control_plane_hotfix_prepare_transaction_post_renderer compensation "${contract}/compensation"
  cmp -s "${contract}/effective-hybrid" <("${HELM_POST_RENDERER_FILE}" <"${contract}/raw-hybrid")
  printf '# drift\n' >>"${CONTROL_PLANE_HOTFIX_INNER_RENDERER_FILE}"
  ! "${HELM_POST_RENDERER_FILE}" <"${contract}/raw-hybrid" >/dev/null
)

V2_PLAN="${TMP}/plan-v2.json"
TARGET_DIGEST="sha256:$(printf 'b%.0s' {1..64})"
HYBRID_DIGEST="sha256:7eb7e7682d44c3f283cd347e032de6fac2f6304221fbf72dfa788845950ccfd9"
CURRENT_SOURCE="a0f5bc0ac36b4e29c4c7928dda1923c2c4727759"
ADOPTED_SOURCE="57dc767999741cea25fe4820a6c9603984dfa0b9"
V2_PLAN="${V2_PLAN}" HEAD_SHA="${HEAD_SHA}" CURRENT_SOURCE="${CURRENT_SOURCE}" \
  ADOPTED_SOURCE="${ADOPTED_SOURCE}" TARGET_DIGEST="${TARGET_DIGEST}" \
  HYBRID_DIGEST="${HYBRID_DIGEST}" python3 - <<'PY'
import json
import os

plan = {
    "planVersion": 2,
    "policy": "control-plane-api-hotfix-rollout-v2",
    "expectedSha": os.environ["HEAD_SHA"],
    "runId": "30788130816",
    "runAttempt": 1,
    "namespace": "fugue-system",
    "releaseName": "fugue",
    "releaseFullname": "fugue-fugue",
    "baseRevision": 819,
    "targetRevision": 820,
    "currentSource": os.environ["CURRENT_SOURCE"],
    "adoptedSource": os.environ["ADOPTED_SOURCE"],
    "targetApiImageRef": "ghcr.io/yym68686/fugue-api@" + os.environ["TARGET_DIGEST"],
    "liveHybridApiImageRef": "ghcr.io/yym68686/fugue-api@" + os.environ["HYBRID_DIGEST"],
}
with open(os.environ["V2_PLAN"], "w", encoding="utf-8") as stream:
    json.dump(plan, stream, separators=(",", ":"))
PY

: >"${LOG}"
(
  cd "${ROOT}"
  export FUGUE_UPGRADE_LIB_ONLY=true
  # shellcheck source=scripts/upgrade_fugue_control_plane.sh
  source "${ROOT}/scripts/upgrade_fugue_control_plane.sh"

  record() { printf '%s|%s\n' "$$" "$1" >>"${LOG}"; }
  initialize_control_plane_release_job_deadline() { record deadline; }
  detect_kubectl() { printf kubectl; }
  control_plane_hotfix_capture_render_set() { mkdir -p "$1"; record capture; }
  control_plane_hotfix_verify_prewrite_bindings() { record "bindings:$2"; }
  control_plane_hotfix_verify_kubernetes() { record "kubernetes:$1"; }
  control_plane_hotfix_write_wal() { record "wal:$1"; }
  control_plane_hotfix_publish_terminal_evidence() { record "publish:$1"; }
  control_plane_hotfix_prepare_transaction_post_renderer() {
    HELM_POST_RENDERER_FILE="${TMP}/hotfix-preserve-sealed.sh"
    printf '#!/usr/bin/env bash\ncat\n' >"${HELM_POST_RENDERER_FILE}"
    chmod 700 "${HELM_POST_RENDERER_FILE}"
    HELM_POST_RENDERER_ARGS=(--post-renderer "${HELM_POST_RENDERER_FILE}")
    record preserve
  }
  acquire_control_plane_backup_coordination_lease() {
    CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD=true
    CONTROL_PLANE_BACKUP_COORDINATION_LEASE_OWNER="release/${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
    CONTROL_PLANE_BACKUP_COORDINATION_LEASE_TOKEN=0123456789abcdef0123456789abcdef
    record acquire
  }
  require_control_plane_backup_coordination_or_abort() { record require; }
  arm_control_plane_release_recovery_fence() { [[ "$1" == control-plane-api-hotfix-rollout-v2 ]]; record arm; }
  control_plane_release_verify_repository_snapshot() { :; }
  control_plane_release_record_argv_input_identities() { : >"${CONTROL_PLANE_RELEASE_DOMAIN_ARGV_INPUT_IDENTITIES}"; }
  control_plane_release_domain_verify_open_argv_identity() { :; }
  control_plane_release_domain_execute_sealed_helm_upgrade() {
    [[ "${CONTROL_PLANE_RELEASE_DOMAIN_ARGV_FD_READY}" == true ]]
    local call
    call="$(grep -c '|execute$' "${LOG}" || :)"
    ARGV_OUTPUT="${TMP}/argv-v2-${call}" EXPECTED_SOURCE="$([[ "${call}" == 0 ]] && printf %s "${ADOPTED_SOURCE}" || printf %s "${CURRENT_SOURCE}")" \
      EXPECTED_DIGEST="$([[ "${call}" == 0 ]] && printf %s "${TARGET_DIGEST}" || printf %s "${HYBRID_DIGEST}")" python3 - 16 <<'PY'
import os

raw = os.read(16, 65536)
argv = [item.decode() for item in raw[:-1].split(b"\0")]
if not raw.endswith(b"\0") or argv[-6:-2] != [
    "--set-string", "api.image.tag=" + os.environ["EXPECTED_SOURCE"],
    "--set-string", "api.image.digest=" + os.environ["EXPECTED_DIGEST"],
] or argv[-2:] != ["--post-renderer", os.environ["TMP"] + "/hotfix-preserve-sealed.sh"]:
    raise SystemExit(1)
with open(os.environ["ARGV_OUTPUT"], "w", encoding="utf-8") as stream:
    stream.write("\n".join(argv) + "\n")
PY
    exec 16<&-
    CONTROL_PLANE_RELEASE_DOMAIN_ARGV_FD_READY=false
    record execute
  }
  control_plane_hotfix_verify_live_target() {
    record "verify:$1:$2:$3"
    [[ "$1" == hybrid ]]
  }
  release_control_plane_backup_coordination_lease() {
    CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD=false
    record release
  }

  export RUNNER_TEMP="${TMP}" LOG TMP ADOPTED_SOURCE CURRENT_SOURCE TARGET_DIGEST HYBRID_DIGEST
  if run_control_plane_hotfix_baseline_adoption <"${V2_PLAN}"; then
    exit 1
  fi
)

V2_EXPECTED=$'deadline\ncapture\nbindings:released\nkubernetes:base\nwal:prepared\nacquire\nrequire\ncapture\nbindings:owned\nkubernetes:base\nwal:prewrite-verified\narm\nwal:forward-started\npreserve\nexecute\nverify:target:820:target.yaml\nrequire\nwal:compensation-started\npreserve\nexecute\nverify:hybrid:821:hybrid.yaml\nwal:compensated\npublish:compensated\nrelease'
[[ "$(cut -d'|' -f2- "${LOG}")" == "${V2_EXPECTED}" ]]
grep -Fxq "api.image.tag=${ADOPTED_SOURCE}" "${TMP}/argv-v2-0"
grep -Fxq "api.image.digest=${TARGET_DIGEST}" "${TMP}/argv-v2-0"
grep -Fxq "api.image.tag=${CURRENT_SOURCE}" "${TMP}/argv-v2-1"
grep -Fxq "api.image.digest=${HYBRID_DIGEST}" "${TMP}/argv-v2-1"

(
  cd "${ROOT}"
  export FUGUE_UPGRADE_LIB_ONLY=true
  # shellcheck source=scripts/upgrade_fugue_control_plane.sh
  source "${ROOT}/scripts/upgrade_fugue_control_plane.sh"

  mkdir -p "${TMP}/authorized"
  for manifest in base.yaml target.yaml repeated-target.yaml hybrid.yaml; do
    printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: fixture\n' >"${TMP}/authorized/${manifest}"
  done
  printf '{"targetValuesDigest":"sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"}\n' >"${TMP}/values-plan.json"
  mkdir -p "${TMP}/bin"
  printf '#!/usr/bin/env bash\nexit 0\n' >"${TMP}/bin/go"
  chmod 700 "${TMP}/bin/go"

  CONTROL_PLANE_HOTFIX_WORK_DIR="${TMP}/values-readback"
  CONTROL_PLANE_HOTFIX_AUTHORIZED_DIR="${TMP}/authorized"
  CONTROL_PLANE_HOTFIX_PLAN_FILE="${TMP}/values-plan.json"
  mkdir -p "${CONTROL_PLANE_HOTFIX_WORK_DIR}"
  PATH="${TMP}/bin:${PATH}"
  helm_current_revision() { printf '807\n'; }
  control_plane_hotfix_verify_kubernetes() { :; }
  run_release_long_command() {
    require_release_forward_budget 30 "builder read-only fixture" || return
    shift 2
    printf '%s\n' "$*" >>"${TMP}/readback-argv"
    case "$*" in
      'helm get manifest fugue -n fugue-system --revision 807') printf '%s\n' 'apiVersion: v1' ;;
      'helm get values fugue -n fugue-system --revision 807 -o json') printf '{}\n' ;;
      *) return 1 ;;
    esac
  }

  control_plane_hotfix_verify_live_target target 807 target.yaml
  grep -Fxq 'helm get values fugue -n fugue-system --revision 807 -o json' "${TMP}/readback-argv"
  ! grep -q -- '--all' "${TMP}/readback-argv"
)

BUILDER_TMP="${TMP}/builder"
mkdir -m 700 "${BUILDER_TMP}"
BUILDER_ARTIFACT="${BUILDER_TMP}/artifact.json"
BUILDER_TARGET_DIGEST="sha256:$(printf 'b%.0s' {1..64})"
BUILDER_PLATFORM_DIGEST="sha256:$(printf 'c%.0s' {1..64})"
BUILDER_CONFIG_DIGEST="sha256:$(printf 'd%.0s' {1..64})"
BUILDER_ARTIFACT="${BUILDER_ARTIFACT}" BUILDER_TARGET_DIGEST="${BUILDER_TARGET_DIGEST}" \
  BUILDER_PLATFORM_DIGEST="${BUILDER_PLATFORM_DIGEST}" BUILDER_CONFIG_DIGEST="${BUILDER_CONFIG_DIGEST}" python3 - <<'PY'
import json, os, pathlib
artifact = [{
    "component": "api", "config_digest": os.environ["BUILDER_CONFIG_DIGEST"],
    "immutable_ref": "ghcr.io/yym68686/fugue-api@" + os.environ["BUILDER_TARGET_DIGEST"],
    "oci_revision": "57dc767999741cea25fe4820a6c9603984dfa0b9",
    "platform_manifest_digest": os.environ["BUILDER_PLATFORM_DIGEST"],
    "repository": "ghcr.io/yym68686/fugue-api",
    "source_tag": "57dc767999741cea25fe4820a6c9603984dfa0b9",
    "top_digest": os.environ["BUILDER_TARGET_DIGEST"],
    "verification": "registry_manifest_config_and_layer_get",
}]
pathlib.Path(os.environ["BUILDER_ARTIFACT"]).write_bytes(json.dumps(artifact, sort_keys=True, separators=(",", ":")).encode())
PY
chmod 600 "${BUILDER_ARTIFACT}"
BUILDER_ARTIFACT_DIGEST="sha256:$(shasum -a 256 "${BUILDER_ARTIFACT}" | awk '{print $1}')"

(
  cd "${ROOT}"
  export FUGUE_UPGRADE_LIB_ONLY=true
  # shellcheck source=scripts/upgrade_fugue_control_plane.sh
  source "${ROOT}/scripts/upgrade_fugue_control_plane.sh"
  builder_head="$(printf '4%.0s' {1..40})"
  stat() {
    if [[ "$*" == "-c %a ${BUILDER_ARTIFACT}" ]]; then
      printf '600\n'
    else
      command stat "$@"
    fi
  }
  git() {
    case "$*" in
      'rev-parse --verify HEAD') printf '%s\n' "${builder_head}" ;;
      'rev-parse --verify HEAD^') printf '%s\n' '9bf7e478af8d7b9dacedaa20f4f6c31ccc97e184' ;;
      'rev-list --parents -n 1 HEAD') printf '%s %s\n' "${builder_head}" '9bf7e478af8d7b9dacedaa20f4f6c31ccc97e184' ;;
      'merge-base --is-ancestor 57dc767999741cea25fe4820a6c9603984dfa0b9 HEAD') : ;;
      'diff --name-only 5a3b09c571601993367c50561b257dd6b9e743ca HEAD') printf '%s\n' \
        '.github/workflows/deploy-control-plane.yml' \
        'internal/platformsafety/release_workflow_test.go' \
        'internal/releasedomain/control_plane_hotfix_adoption.go' \
        'internal/releasedomain/control_plane_hotfix_adoption_test.go' \
        'scripts/test_control_plane_hotfix_adoption.sh' \
        'scripts/upgrade_fugue_control_plane.sh' ;;
      'diff --name-only 57dc767999741cea25fe4820a6c9603984dfa0b9 HEAD -- deploy/helm/fugue go.mod go.sum') : ;;
      'ls-tree -r HEAD -- deploy/helm/fugue') printf '100644 blob %040d\tdeploy/helm/fugue/values.yaml\n' 1 ;;
      *) command git "$@" ;;
    esac
  }
  control_plane_release_sha256_stream() { printf '%064d\n' 1; }
  detect_kubectl() { printf kubectl; }
  run_release_long_command() {
    shift 2
    case "$*" in
      'helm status fugue -n fugue-system -o json') printf '%s\n' '{"info":{"status":"deployed"},"version":819}' ;;
      'helm get values fugue -n fugue-system --all --revision 819 -o json') printf '%s\n' '{}' ;;
      'helm get manifest fugue -n fugue-system --revision 819') printf '%s\n' 'apiVersion: v1' ;;
      *) return 1 ;;
    esac
  }
  control_plane_hotfix_render() {
    local source="$1" output="$2" digest="$3"
    printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: fixture\n' >"${output}"
    cp "${output}" "${output}.raw"
    CONTROL_PLANE_HOTFIX_EDGE_RESTORE_PLAN_DIGEST="sha256:$(printf 'f%.0s' {1..64})"
    SOURCE="${source}" DIGEST="${digest}" OUTPUT="${output}.json" python3 - <<'PY'
import json, os
json.dump({"config":{"api":{"image":{"digest":os.environ["DIGEST"],"tag":os.environ["SOURCE"]}}},"manifest":"fixture"},open(os.environ["OUTPUT"],"w"),sort_keys=True,separators=(",", ":"))
PY
  }
  CONTROL_PLANE_RELEASE_JOB_DEADLINE_EPOCH="$(( $(date +%s) + 3600 ))"
  bounded_kubectl() {
    [[ "${KUBECTL:-}" == kubectl && "${FUGUE_NAMESPACE:-}" == fugue-system &&
      "${FUGUE_RELEASE_NAME:-}" == fugue && "${FUGUE_RELEASE_FULLNAME:-}" == fugue-fugue ]] || return 1
    case "$*" in
      *'get deployment/fugue-fugue-api -o json') cat <<'JSON'
{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"generation":7,"name":"fugue-fugue-api","namespace":"fugue-system","resourceVersion":"11","uid":"api-uid"},"spec":{"replicas":2,"template":{"metadata":{"annotations":{"fugue.pro/source-commit":"a0f5bc0ac36b4e29c4c7928dda1923c2c4727759"}},"spec":{"containers":[{"image":"ghcr.io/yym68686/fugue-api@sha256:7eb7e7682d44c3f283cd347e032de6fac2f6304221fbf72dfa788845950ccfd9","name":"api"}]}}},"status":{"availableReplicas":2,"observedGeneration":7,"readyReplicas":2,"replicas":2,"updatedReplicas":2}}
JSON
        ;;
      *'get service/fugue-fugue -o json') printf '%s\n' '{"metadata":{"name":"fugue-fugue","namespace":"fugue-system","resourceVersion":"21","uid":"service-uid"},"spec":{"selector":{"app":"api"}}}' ;;
      *'get endpointslice -l kubernetes.io/service-name=fugue-fugue -o json') printf '%s\n' '{"items":[{"addressType":"IPv4","endpoints":[{"addresses":["10.0.0.1"],"conditions":{"ready":true,"serving":true},"targetRef":{"name":"api-1"}},{"addresses":["10.0.0.2"],"conditions":{"ready":true,"serving":true},"targetRef":{"name":"api-2"}}],"metadata":{"name":"fugue-fugue-a","resourceVersion":"31","uid":"slice-uid"},"ports":[{"port":8080}]}]}' ;;
      *'get pods -l app.kubernetes.io/instance=fugue,app.kubernetes.io/component=api -o json') printf '%s\n' '{"items":[{"status":{"conditions":[{"status":"True","type":"Ready"}],"containerStatuses":[{"imageID":"ghcr.io/yym68686/fugue-api@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","name":"api"}],"phase":"Running"}},{"status":{"conditions":[{"status":"True","type":"Ready"}],"containerStatuses":[{"imageID":"ghcr.io/yym68686/fugue-api@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","name":"api"}],"phase":"Running"}}]}' ;;
      *'get lease/fugue-fugue-control-plane-db-backup -o json') printf '%s\n' '{"metadata":{"annotations":{},"name":"fugue-fugue-control-plane-db-backup","namespace":"fugue-system","resourceVersion":"41","uid":"lease-uid"},"spec":{"holderIdentity":""}}' ;;
      *'get deployments,daemonsets,statefulsets,pods -o json') printf '%s\n' '{"items":[]}' ;;
      *) return 1 ;;
    esac
  }
  run_with_wall_timeout() { printf 'ok\n'; }
  go() {
    [[ "$*" == 'run ./cmd/fugue-control-plane-hotfix-adoption' ]]
    INPUT="${FUGUE_CONTROL_PLANE_HOTFIX_BUILD_DIR}/input.json" EXPECTED_ARTIFACT="${BUILDER_ARTIFACT_DIGEST}" python3 - <<'PY'
import json, os
value=json.load(open(os.environ["INPUT"],encoding="utf-8"))
assert value["planVersion"] == 2 and value["helmRevision"] == 819
assert value["currentSource"] == "a0f5bc0ac36b4e29c4c7928dda1923c2c4727759"
assert value["adoptedSource"] == "57dc767999741cea25fe4820a6c9603984dfa0b9"
assert value["provenance"]["artifactDigest"] == os.environ["EXPECTED_ARTIFACT"]
assert value["kubernetes"]["apiReady"] == 2 and value["kubernetes"]["readyServingEndpoints"] == 2
assert value["kubernetes"]["frozenNonApiWorkloadDigest"].startswith("sha256:")
assert value["rawTargetManifestDigest"] == value["targetPostRenderDigest"]
assert value["rawHybridManifestDigest"] == value["hybridPostRenderDigest"]
assert value["nonApiEdgeRestorePlanDigest"].startswith("sha256:")
print(json.dumps({"planVersion":2,"policy":"control-plane-api-hotfix-rollout-v2"},separators=(",", ":")))
PY
  }
  run_control_plane_hotfix_baseline_adoption() {
    python3 -c 'import json,sys; value=json.load(sys.stdin); assert value=={"planVersion":2,"policy":"control-plane-api-hotfix-rollout-v2"}'
    printf 'builder-executed\n' >"${BUILDER_TMP}/result"
  }
  export GITHUB_SHA="${builder_head}" GITHUB_RUN_ID=30788130816 GITHUB_RUN_ATTEMPT=1
  export RUNNER_TEMP="${BUILDER_TMP}" FUGUE_SMOKE_URL=https://api.example.test/healthz
  export FUGUE_CONTROL_PLANE_API_HOTFIX_ARTIFACT_FILE="${BUILDER_ARTIFACT}"
  export FUGUE_CONTROL_PLANE_API_HOTFIX_ARTIFACT_DIGEST="${BUILDER_ARTIFACT_DIGEST}"
  export BUILDER_ARTIFACT_DIGEST BUILDER_TMP
  run_control_plane_api_hotfix_rollout_v2
)
[[ "$(cat "${BUILDER_TMP}/result")" == builder-executed ]]
[[ -z "$(find "${BUILDER_TMP}" -maxdepth 1 -type d -name 'fugue-api-hotfix-v2-plan.*' -print -quit)" ]]

(
  cd "${ROOT}"
  export FUGUE_UPGRADE_LIB_ONLY=true
  # shellcheck source=scripts/upgrade_fugue_control_plane.sh
  source "${ROOT}/scripts/upgrade_fugue_control_plane.sh"

  fixture_variant=base
  bounded_kubectl() {
    [[ "$*" == *'get deployments,daemonsets,statefulsets,pods -o json' ]] || return 1
    VARIANT="${fixture_variant}" python3 - <<'PY'
import json, os
variant=os.environ["VARIANT"]
deployment={"apiVersion":"apps/v1","kind":"Deployment","metadata":{"annotations":{"fugue.pro/source-commit":"a"*40},"generation":9,"labels":{"app":"controller"},"name":"controller","namespace":"fugue-system","resourceVersion":"100","uid":"controller-uid"},"spec":{"replicas":1,"template":{"spec":{"containers":[{"image":"example/controller@sha256:"+"a"*64,"name":"controller"}]}}},"status":{"availableReplicas":1,"observedGeneration":9,"readyReplicas":1,"updatedReplicas":1}}
pod={"apiVersion":"v1","kind":"Pod","metadata":{"labels":{"app.kubernetes.io/component":"controller"},"ownerReferences":[{"kind":"ReplicaSet","name":"controller-rs","uid":"rs-uid"}],"resourceVersion":"200","uid":"pod-uid"},"status":{"phase":"Running","containerStatuses":[{"imageID":"example/controller@sha256:"+"a"*64,"name":"controller","ready":True}]}}
daemonset={"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"generation":4,"labels":{"app":"cache"},"name":"cache","namespace":"fugue-system","resourceVersion":"300","uid":"cache-uid"},"spec":{"selector":{"matchLabels":{"app":"cache"}},"template":{"metadata":{"labels":{"app":"cache"}},"spec":{"containers":[{"image":"example/cache@sha256:"+"c"*64,"name":"cache"}]}}},"status":{"currentNumberScheduled":1,"desiredNumberScheduled":1,"numberAvailable":1,"numberMisscheduled":0,"numberReady":1,"numberUnavailable":0,"observedGeneration":4,"updatedNumberScheduled":1}}
if variant == "volatile":
    deployment["metadata"]["resourceVersion"]="101"
    pod["metadata"]["resourceVersion"]="201"
    daemonset["metadata"]["resourceVersion"]="301"
elif variant == "spec-drift":
    deployment["spec"]["replicas"]=2
elif variant == "uid-drift":
    deployment["metadata"]["uid"]="replacement-controller-uid"
elif variant == "ready-drift":
    deployment["status"]["readyReplicas"]=0
elif variant == "available-drift":
    deployment["status"]["availableReplicas"]=0
elif variant == "unavailable-drift":
    deployment["status"]["unavailableReplicas"]=1
elif variant == "observed-drift":
    deployment["status"]["observedGeneration"]=8
elif variant == "desired-drift":
    daemonset["status"]["desiredNumberScheduled"]=2
elif variant == "pod-drift":
    pod["status"]["containerStatuses"][0]["imageID"]="example/controller@sha256:"+"b"*64
print(json.dumps({"items":[deployment,daemonset,pod]},separators=(",",":")))
PY
  }

  base="$(control_plane_hotfix_non_api_workload_digest)"
  fixture_variant=volatile
  [[ "$(control_plane_hotfix_non_api_workload_digest)" == "${base}" ]]
  fixture_variant=spec-drift
  [[ "$(control_plane_hotfix_non_api_workload_digest)" != "${base}" ]]
  fixture_variant=uid-drift
  [[ "$(control_plane_hotfix_non_api_workload_digest)" != "${base}" ]]
  for fixture_variant in ready-drift available-drift unavailable-drift observed-drift desired-drift; do
    [[ "$(control_plane_hotfix_non_api_workload_digest)" != "${base}" ]]
  done
  fixture_variant=pod-drift
  [[ "$(control_plane_hotfix_non_api_workload_digest)" != "${base}" ]]
)

(
  cd "${ROOT}"
  export FUGUE_UPGRADE_LIB_ONLY=true
  # shellcheck source=scripts/upgrade_fugue_control_plane.sh
  source "${ROOT}/scripts/upgrade_fugue_control_plane.sh"
  recovery_root="${TMP}/recovery-only"
  install -d -m 700 "${recovery_root}"
  install -d -m 700 "${recovery_root}/evidence"
  recovery_log="${recovery_root}/calls"
  : >"${recovery_log}"
  git() {
    case "$*" in
      'rev-parse --verify HEAD') printf '%040d\n' 7 ;;
      'rev-parse --verify HEAD^') printf '%s\n' 9bf7e478af8d7b9dacedaa20f4f6c31ccc97e184 ;;
      'diff --name-only HEAD^ HEAD') printf '%s\n' \
        .github/workflows/deploy-control-plane.yml \
        internal/platformsafety/release_workflow_test.go \
        internal/releasedomain/control_plane_hotfix_adoption.go \
        internal/releasedomain/control_plane_hotfix_adoption_test.go \
        scripts/test_control_plane_hotfix_adoption.sh \
        scripts/upgrade_fugue_control_plane.sh ;;
      'status --short') : ;;
      *) return 1 ;;
    esac
  }
  stat() {
    if [[ "$1" == -c && "$2" == %a && "$3" == "${recovery_root}/evidence" ]]; then
      printf '700\n'
    else
      command stat "$@"
    fi
  }
  detect_kubectl() { printf kubectl; }
  control_plane_api_hotfix_recovery_capture() {
    local directory="$1" mode="$2"
    install -d -m 700 "${directory}"
    printf '%s\n' '{"lkg":"exact"}' >"${directory}/lkg.json"
    printf '{"lease":"%s"}\n' "${mode}" >"${directory}/lease-safe.json"
    CONTROL_PLANE_HOTFIX_RECOVERY_CAPTURED_TOKEN="$([[ "${mode}" == held ]] && printf '%s' 0123456789abcdef0123456789abcdef || printf '%s' -)"
    printf 'capture:%s\n' "${mode}" >>"${recovery_log}"
  }
  release_control_plane_backup_coordination_lease() {
    [[ "${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD}" == true ]]
    [[ "${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_OWNER}" == release/30804033592-1 ]]
    [[ "${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_TOKEN}" == 0123456789abcdef0123456789abcdef ]]
    CONTROL_PLANE_BACKUP_COORDINATION_LEASE_HELD=false
    printf 'release\n' >>"${recovery_log}"
  }
  export GITHUB_SHA="$(printf '%040d' 7)" RUNNER_TEMP="${recovery_root}"
  export FUGUE_SMOKE_URL=https://api.fugue.pro/healthz
  export FUGUE_CONTROL_PLANE_API_HOTFIX_RECOVERY_CONFIRM=CONFIRM_API_HOTFIX_RECOVERY_30804033592
  export FUGUE_CONTROL_PLANE_API_HOTFIX_RECOVERY_EVIDENCE_DIR="${recovery_root}/evidence"
  run_control_plane_api_hotfix_recovery_only
  [[ "$(cat "${recovery_log}")" == $'capture:held\ncapture:held\nrelease\ncapture:released' ]]
  recovery_source="$(declare -f run_control_plane_api_hotfix_recovery_only)"
  [[ "${recovery_source}" != *'helm upgrade'* && "${recovery_source}" != *'kubectl patch deployment'* ]]
  [[ "$(find "${recovery_root}/evidence" -maxdepth 1 -type f | wc -l | tr -d ' ')" == 7 ]]
  ! grep -R -Fq 0123456789abcdef0123456789abcdef "${recovery_root}/evidence"
)

printf '[test_control_plane_hotfix_adoption] fixed single-shell Lease/FD16/Helm transaction passed\n'
