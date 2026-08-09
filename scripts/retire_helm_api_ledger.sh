#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
intent="${repo_root}/deploy/releases/helm-api-retirement/intent.json"
result="${FUGUE_HELM_API_RETIREMENT_RESULT:-${RUNNER_TEMP:-/tmp}/fugue-helm-api-retirement-result.json}"
execute="${FUGUE_HELM_API_RETIREMENT_EXECUTE:-false}"
cd "${repo_root}"

if [[ $# -ne 0 || ("${execute}" != true && "${execute}" != false) ]]; then
  echo "helm-api-retirement: invalid invocation" >&2
  exit 2
fi
for command in git helm kubectl jq sha256sum python3; do
  command -v "${command}" >/dev/null || { echo "helm-api-retirement: missing ${command}" >&2; exit 2; }
done

work="$(mktemp -d "${RUNNER_TEMP:-/tmp}/fugue-api-reconcile.XXXXXX")"
chmod 700 "${work}"
stage=prewrite
cleanup() { find "${work}" -depth -delete 2>/dev/null || true; }
sha256_file() { printf 'sha256:%s' "$(sha256sum "$1" | awk '{print $1}')"; }
require_equal() {
  local label="$1" actual="$2" expected="$3"
  [[ "${actual}" == "${expected}" ]] || { echo "helm-api-retirement: ${label} mismatch" >&2; exit 1; }
}
component_leases() {
  kubectl -n fugue-system get lease -o json | jq -c '[.items[] | select(.metadata.name | startswith("fugue-production-")) | select((.spec.holderIdentity // "") != "") | .metadata.name]'
}
api_snapshot() { kubectl -n fugue-system get deployment fugue-fugue-api -o json; }
verify_old_api() {
  jq -e --argjson expected "$(jq -c '.retiredResources[0]' "${intent}")" \
    --arg target "$(jq -r .targetSource "${intent}")" --arg artifact "$(jq -r .targetArtifactReceipt "${intent}")" '
      .metadata.uid == $expected.uid and .metadata.resourceVersion == $expected.resourceVersion and
      .metadata.generation == $expected.generation and .metadata.annotations["helm.sh/resource-policy"] == "keep" and
      .metadata.annotations["fugue.pro/production-config-sha"] == $target and
      .metadata.annotations["fugue.pro/artifact-receipt-digest"] == $artifact and
      .spec.template.metadata.annotations["fugue.pro/production-config-sha"] == $target and
      .spec.template.metadata.annotations["fugue.pro/oci-revision"] == $target and
      .spec.template.metadata.annotations["fugue.pro/source-commit"] == $expected.source and
      (.spec.template.spec.containers | length) == 1 and .spec.template.spec.containers[0].name == "api" and
      .spec.template.spec.containers[0].image == $expected.image and
      .status.observedGeneration == .metadata.generation and .status.replicas == $expected.desired and
      .status.updatedReplicas == $expected.updated and .status.readyReplicas == $expected.ready and
      .status.availableReplicas == $expected.available and (.status.unavailableReplicas // 0) == $expected.unavailable
    ' "$1" >/dev/null
}
verify_target_api() {
  local target_source target_image pods
  target_source="$(jq -r .targetSource "${intent}")"
  target_image="$(jq -r .targetImage "${intent}")"
  jq -e --arg uid "$(jq -r '.retiredResources[0].uid' "${intent}")" --arg source "${target_source}" --arg image "${target_image}" '
      .metadata.uid == $uid and .metadata.annotations["helm.sh/resource-policy"] == "keep" and
      .metadata.annotations["fugue.pro/production-config-sha"] == $source and
      .spec.template.metadata.annotations["fugue.pro/source-commit"] == $source and
      .spec.template.metadata.annotations["fugue.pro/production-config-sha"] == $source and
      .spec.template.metadata.annotations["fugue.pro/oci-revision"] == $source and
      .spec.template.spec.containers[0].name == "api" and .spec.template.spec.containers[0].image == $image and
      .status.observedGeneration == .metadata.generation and .status.replicas == 2 and
      .status.updatedReplicas == 2 and .status.readyReplicas == 2 and .status.availableReplicas == 2 and
      (.status.unavailableReplicas // 0) == 0
    ' "$1" >/dev/null
  pods="${work}/api-pods.json"
  kubectl -n fugue-system get pods -l app.kubernetes.io/component=api,app.kubernetes.io/instance=fugue -o json >"${pods}"
  jq -e --arg image "${target_image}" '
    (.items | length) == 2 and all(.items[];
      .status.phase == "Running" and any(.status.conditions[]; .type == "Ready" and .status == "True") and
      (.status.containerStatuses | length) == 1 and .status.containerStatuses[0].ready == true and
      .status.containerStatuses[0].restartCount == 0 and .status.containerStatuses[0].imageID == $image)
  ' "${pods}" >/dev/null
}

rollback_api() {
  local rc=$? live rv source image old_source old_image target_source target_image rollback_patch
  trap - ERR
  if [[ "${stage}" == forward* ]]; then
    live="${work}/rollback-live.json"
    api_snapshot >"${live}" || true
    rv="$(jq -r .metadata.resourceVersion "${live}")"
    source="$(jq -r '.spec.template.metadata.annotations["fugue.pro/source-commit"]' "${live}")"
    image="$(jq -r '.spec.template.spec.containers[0].image' "${live}")"
    old_source="$(jq -r '.retiredResources[0].source' "${intent}")"
    old_image="$(jq -r '.retiredResources[0].image' "${intent}")"
    target_source="$(jq -r .targetSource "${intent}")"
    target_image="$(jq -r .targetImage "${intent}")"
    if [[ "${source}" == "${target_source}" && "${image}" == "${target_image}" ]]; then
      rollback_patch="${work}/rollback.json"
      jq -nc --arg uid "$(jq -r '.retiredResources[0].uid' "${intent}")" --arg rv "${rv}" \
        --arg source "${target_source}" --arg image "${target_image}" --arg oldSource "${old_source}" --arg oldImage "${old_image}" '
        [{op:"test",path:"/metadata/uid",value:$uid},{op:"test",path:"/metadata/resourceVersion",value:$rv},
         {op:"test",path:"/spec/template/metadata/annotations/fugue.pro~1source-commit",value:$source},
         {op:"test",path:"/spec/template/spec/containers/0/image",value:$image},
         {op:"replace",path:"/spec/template/metadata/annotations/fugue.pro~1source-commit",value:$oldSource},
         {op:"replace",path:"/spec/template/spec/containers/0/image",value:$oldImage}]' >"${rollback_patch}"
      kubectl -n fugue-system patch deployment fugue-fugue-api --type=json --patch-file "${rollback_patch}" >/dev/null || true
      kubectl -n fugue-system rollout status deployment/fugue-fugue-api --timeout=5m >/dev/null || true
    fi
  fi
  jq -nc --arg source "$(git rev-parse HEAD)" --arg stage "${stage}" --argjson rc "${rc}" \
    '{apiVersion:"release.fugue.dev/v1",kind:"HelmAPILedgerRetirementResult",status:"recovery-required",reason:"api-runtime-reconcile-failed",sourceCommit:$source,stage:$stage,exitCode:$rc,productionMutationAttempted:($stage != "prewrite")}' >"${result}" || true
  exit "${rc}"
}
trap rollback_api ERR
trap cleanup EXIT

jq -e '.apiVersion == "release.fugue.dev/v1" and .kind == "HelmLedgerRetirementIntent" and
  .expectedHelmRevision == 831 and (.retiredResources | length) == 1 and
  .retiredResources[0].name == "fugue-fugue-api" and (.targetSource | test("^[0-9a-f]{40}$")) and
  (.targetImage | test("@sha256:[0-9a-f]{64}$"))' "${intent}" >/dev/null
head_sha="$(git rev-parse HEAD)"
parent_sha="$(git rev-parse HEAD^)"
require_equal "source parent" "${parent_sha}" "$(jq -r .expectedSourceParent "${intent}")"
require_equal "GitHub source" "${GITHUB_SHA:-${head_sha}}" "${head_sha}"
remote_expected="${head_sha}"; [[ "${execute}" == true ]] || remote_expected="${parent_sha}"
require_equal "remote main" "$(git ls-remote --heads origin main | awk '{print $1}')" "${remote_expected}"
[[ -z "$(git status --porcelain)" ]] || { echo "helm-api-retirement: checkout is not clean" >&2; exit 1; }
require_equal "active component leases" "$(component_leases)" "[]"

status_json="${work}/helm-status.json"
helm -n fugue-system status fugue -o json >"${status_json}"
require_equal "Helm revision" "$(jq -r .version "${status_json}")" "831"
require_equal "Helm status" "$(jq -r .info.status "${status_json}")" "deployed"
manifest="${work}/manifest.yaml"
helm -n fugue-system get manifest fugue >"${manifest}"
require_equal "Helm manifest digest" "$(sha256_file "${manifest}")" "$(jq -r .expectedManifestDigest "${intent}")"
if awk 'BEGIN{RS="---"} /kind: Deployment/ && /name: fugue-fugue-api/{found=1} END{exit found?0:1}' "${manifest}"; then
  echo "helm-api-retirement: API remains in Helm manifest" >&2
  exit 1
fi
live_before="${work}/live-before.json"
api_snapshot >"${live_before}"
verify_old_api "${live_before}"
python3 scripts/verify_registry_image.py --metadata-only --platform linux/amd64 \
  --image "$(jq -r .targetImage "${intent}")" --expected-revision "$(jq -r .targetSource "${intent}")" >"${work}/registry.json"
if [[ "${execute}" == false ]]; then
  jq -nc --arg source "${head_sha}" '{apiVersion:"release.fugue.dev/v1",kind:"HelmAPILedgerRetirementResult",status:"prepared",reason:"retired-ledger-runtime-reconcile-verified",sourceCommit:$source,productionMutationAttempted:false}' >"${result}"
  exit 0
fi

stage=forward-patch
forward_patch="${work}/forward.json"
jq -nc --arg uid "$(jq -r '.retiredResources[0].uid' "${intent}")" --arg rv "$(jq -r .metadata.resourceVersion "${live_before}")" \
  --arg oldSource "$(jq -r '.retiredResources[0].source' "${intent}")" --arg oldImage "$(jq -r '.retiredResources[0].image' "${intent}")" \
  --arg source "$(jq -r .targetSource "${intent}")" --arg image "$(jq -r .targetImage "${intent}")" '
  [{op:"test",path:"/metadata/uid",value:$uid},{op:"test",path:"/metadata/resourceVersion",value:$rv},
   {op:"test",path:"/spec/template/metadata/annotations/fugue.pro~1source-commit",value:$oldSource},
   {op:"test",path:"/spec/template/spec/containers/0/image",value:$oldImage},
   {op:"replace",path:"/spec/template/metadata/annotations/fugue.pro~1source-commit",value:$source},
   {op:"replace",path:"/spec/template/spec/containers/0/image",value:$image}]' >"${forward_patch}"
kubectl -n fugue-system patch deployment fugue-fugue-api --type=json --patch-file "${forward_patch}" >/dev/null
stage=forward-health
kubectl -n fugue-system rollout status deployment/fugue-fugue-api --timeout=5m >/dev/null
live_final="${work}/live-final.json"
api_snapshot >"${live_final}"
verify_target_api "${live_final}"
require_equal "API health" "$(kubectl get --raw '/api/v1/namespaces/fugue-system/services/http:fugue-fugue:http/proxy/healthz')" "ok"
require_equal "final component leases" "$(component_leases)" "[]"
stage=verified
jq -nc --arg source "${head_sha}" --arg target "$(jq -r .targetSource "${intent}")" --arg image "$(jq -r .targetImage "${intent}")" \
  '{apiVersion:"release.fugue.dev/v1",kind:"HelmAPILedgerRetirementResult",status:"verified",reason:"api-ledger-retired-and-runtime-restored",sourceCommit:$source,targetSource:$target,targetImage:$image,productionMutationAttempted:true,helmRevision:831}' >"${result}"
