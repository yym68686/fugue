#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
intent="${repo_root}/deploy/releases/helm-api-retirement/intent.json"
result="${FUGUE_HELM_API_RETIREMENT_RESULT:-${RUNNER_TEMP:-/tmp}/fugue-helm-api-retirement-result.json}"
cd "${repo_root}"

if [[ $# -ne 0 ]]; then
  echo "helm-api-retirement-readback: positional arguments are forbidden" >&2
  exit 2
fi
for command in git helm kubectl jq sha256sum python3 curl; do
  command -v "${command}" >/dev/null || { echo "helm-api-retirement-readback: missing ${command}" >&2; exit 2; }
done
sha256_file() { printf 'sha256:%s' "$(sha256sum "$1" | awk '{print $1}')"; }
require_equal() {
  local label="$1" actual="$2" expected="$3"
  [[ "${actual}" == "${expected}" ]] || { echo "helm-api-retirement-readback: ${label} mismatch" >&2; exit 1; }
}

jq -e '.apiVersion == "release.fugue.dev/v1" and .kind == "HelmLedgerRetirementIntent" and
  .expectedHelmRevision == 831 and (.retiredResources | length) == 1 and
  .retiredResources[0].source == .targetSource and .retiredResources[0].image == .targetImage' "${intent}" >/dev/null
head_sha="$(git rev-parse HEAD)"
parent_sha="$(git rev-parse HEAD^)"
require_equal "source parent" "${parent_sha}" "$(jq -r .expectedSourceParent "${intent}")"
require_equal "GitHub source" "${GITHUB_SHA:-${head_sha}}" "${head_sha}"
require_equal "remote main" "$(git ls-remote --heads origin main | awk '{print $1}')" "${head_sha}"
[[ -z "$(git status --porcelain)" ]] || { echo "helm-api-retirement-readback: checkout is not clean" >&2; exit 1; }

status_json="$(helm -n fugue-system status fugue -o json)"
require_equal "Helm revision" "$(jq -r .version <<<"${status_json}")" "831"
require_equal "Helm status" "$(jq -r .info.status <<<"${status_json}")" "deployed"
manifest="$(mktemp "${RUNNER_TEMP:-/tmp}/fugue-helm831.XXXXXX")"
trap 'rm -f "${manifest}"' EXIT
helm -n fugue-system get manifest fugue >"${manifest}"
require_equal "Helm manifest digest" "$(sha256_file "${manifest}")" "$(jq -r .expectedManifestDigest "${intent}")"
if awk 'BEGIN{RS="---"} /kind: Deployment/ && /name: fugue-fugue-api/{found=1} END{exit found?0:1}' "${manifest}"; then
  echo "helm-api-retirement-readback: API remains in Helm manifest" >&2
  exit 1
fi

expected="$(jq -c '.retiredResources[0]' "${intent}")"
live="$(kubectl -n fugue-system get deployment fugue-fugue-api -o json)"
jq -e --argjson expected "${expected}" --arg target "$(jq -r .targetSource "${intent}")" --arg artifact "$(jq -r .targetArtifactReceipt "${intent}")" '
  .metadata.uid == $expected.uid and .metadata.resourceVersion == $expected.resourceVersion and
  .metadata.generation == $expected.generation and .metadata.annotations["helm.sh/resource-policy"] == "keep" and
  .metadata.annotations["fugue.pro/production-config-sha"] == $target and
  .metadata.annotations["fugue.pro/artifact-receipt-digest"] == $artifact and
  .spec.template.metadata.annotations["fugue.pro/source-commit"] == $expected.source and
  .spec.template.metadata.annotations["fugue.pro/production-config-sha"] == $target and
  .spec.template.metadata.annotations["fugue.pro/oci-revision"] == $target and
  .spec.template.spec.containers[0].name == "api" and .spec.template.spec.containers[0].image == $expected.image and
  .status.observedGeneration == .metadata.generation and .status.replicas == 2 and
  .status.updatedReplicas == 2 and .status.readyReplicas == 2 and .status.availableReplicas == 2 and
  (.status.unavailableReplicas // 0) == 0
' <<<"${live}" >/dev/null

pods="$(kubectl -n fugue-system get pods -l app.kubernetes.io/component=api,app.kubernetes.io/instance=fugue -o json)"
jq -e --arg image "$(jq -r .targetImage "${intent}")" '
  (.items | length) == 2 and all(.items[];
    .metadata.deletionTimestamp == null and .status.phase == "Running" and
    any(.status.conditions[]; .type == "Ready" and .status == "True") and
    (.status.containerStatuses | length) == 1 and .status.containerStatuses[0].ready == true and
    .status.containerStatuses[0].restartCount == 0 and .status.containerStatuses[0].imageID == $image)
' <<<"${pods}" >/dev/null
python3 scripts/verify_registry_image.py --metadata-only --platform linux/amd64 \
  --image "$(jq -r .targetImage "${intent}")" --expected-revision "$(jq -r .targetSource "${intent}")" >/dev/null
require_equal "API health" "$(kubectl get --raw '/api/v1/namespaces/fugue-system/services/http:fugue-fugue:http/proxy/healthz' | jq -r .status)" "ok"
require_equal "component leases" "$(kubectl -n fugue-system get lease -o json | jq -c '[.items[] | select(.metadata.name | startswith("fugue-production-")) | select((.spec.holderIdentity // "") != "") | .metadata.name]')" "[]"
for url in https://fugue.pro/ https://fugue.pro/healthz https://api.fugue.pro/healthz; do
  require_equal "external health ${url}" "$(curl --compressed -fsS -o /dev/null -w '%{http_code}' --connect-timeout 5 --max-time 15 "${url}")" "200"
done

jq -nc --arg source "${head_sha}" --arg target "$(jq -r .targetSource "${intent}")" --arg image "$(jq -r .targetImage "${intent}")" \
  '{apiVersion:"release.fugue.dev/v1",kind:"HelmAPILedgerRetirementResult",status:"verified",reason:"api-ledger-retired-runtime-readback",sourceCommit:$source,targetSource:$target,targetImage:$image,productionMutationAttempted:false,productionMutationObserved:true,helmRevision:831}' >"${result}"
