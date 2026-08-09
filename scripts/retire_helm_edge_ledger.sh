#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
intent="${repo_root}/deploy/releases/helm-ledger-retirement/intent.json"
patch_file="${repo_root}/deploy/releases/helm-ledger-retirement/helm828-declarative-ownership.patch"
result="${FUGUE_HELM_LEDGER_RESULT:-${RUNNER_TEMP:-/tmp}/fugue-helm-ledger-retirement-result.json}"
execute="${FUGUE_HELM_LEDGER_EXECUTE:-false}"
cd "${repo_root}"

if [[ $# -ne 0 ]]; then
  echo "helm-ledger-retirement: positional arguments are forbidden" >&2
  exit 2
fi
if [[ "${execute}" != "true" && "${execute}" != "false" ]]; then
  echo "helm-ledger-retirement: FUGUE_HELM_LEDGER_EXECUTE must be true or false" >&2
  exit 2
fi
for command in git helm kubectl jq sha256sum base64 gzip tar; do
  command -v "${command}" >/dev/null || { echo "helm-ledger-retirement: missing ${command}" >&2; exit 2; }
done

work="$(mktemp -d "${RUNNER_TEMP:-/tmp}/fugue-helm-ledger-retirement.XXXXXX")"
chmod 700 "${work}"
cleanup() {
  find "${work}" -type f -exec chmod 600 {} + 2>/dev/null || true
  find "${work}" -depth -delete 2>/dev/null || true
}
trap cleanup EXIT

sha256_file() {
  printf 'sha256:%s' "$(sha256sum "$1" | awk '{print $1}')"
}

require_equal() {
  local label="$1" actual="$2" expected="$3"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "helm-ledger-retirement: ${label} mismatch" >&2
    exit 1
  fi
}

assert_manifest_retired() {
  local manifest="$1" names="${work}/manifest-resource-names.txt"
  kubectl create --dry-run=client -f "${manifest}" -o name >"${names}"
  while IFS= read -r name; do
    if grep -Fxq "daemonset.apps/${name}" "${names}"; then
      echo "helm-ledger-retirement: ${name} remains in the Helm manifest" >&2
      exit 1
    fi
  done < <(jq -r '.retiredResources[].name' "${intent}")
}

jq -e '
  .apiVersion == "release.fugue.dev/v1" and
  .kind == "HelmLedgerRetirementIntent" and
  .release == "fugue" and .namespace == "fugue-system" and
  .expectedHelmRevision == 828 and .expectedHelmStatus == "deployed" and
  (.expectedFinalManifestDigest | test("^sha256:[0-9a-f]{64}$")) and
  (.retiredResources | length == 5) and
  ([.retiredResources[].name] | unique | length == 5) and
  (all(.retiredResources[]; .apiVersion == "apps/v1" and .kind == "DaemonSet" and
    .namespace == "fugue-system" and .uid != "" and .resourceVersion != "" and
    .generation > 0 and .source != "" and (.image | test("@sha256:[0-9a-f]{64}$"))))
' "${intent}" >/dev/null

head_sha="$(git -C "${repo_root}" rev-parse HEAD)"
parent_sha="$(git -C "${repo_root}" rev-parse HEAD^)"
expected_parent="$(jq -r .expectedSourceParent "${intent}")"
require_equal "source parent" "${parent_sha}" "${expected_parent}"
require_equal "GitHub source" "${GITHUB_SHA:-${head_sha}}" "${head_sha}"
remote_expected="${head_sha}"
if [[ "${execute}" == "false" ]]; then
  remote_expected="${expected_parent}"
fi
require_equal "remote main" "$(git -C "${repo_root}" ls-remote --heads origin main | awk '{print $1}')" "${remote_expected}"
if [[ -n "$(git -C "${repo_root}" status --porcelain)" ]]; then
  echo "helm-ledger-retirement: checkout is not clean" >&2
  exit 1
fi

helm_version="$(helm version --short)"
if [[ "${helm_version}" != v3.20.2+g8fb76d6 ]]; then
  echo "helm-ledger-retirement: Helm version is not v3.20.2" >&2
  exit 1
fi

release="$(jq -r .release "${intent}")"
namespace="$(jq -r .namespace "${intent}")"
status_json="${work}/status-before.json"
helm status "${release}" --namespace "${namespace}" --output json >"${status_json}"
require_equal "Helm status" "$(jq -r .info.status "${status_json}")" "$(jq -r .expectedHelmStatus "${intent}")"
expected_revision="$(jq -r .expectedHelmRevision "${intent}")"
observed_revision="$(jq -r .version "${status_json}")"
reconcile_existing=false
if [[ "${observed_revision}" == "$(( expected_revision + 1 ))" ]]; then
  reconcile_existing=true
elif [[ "${observed_revision}" != "${expected_revision}" ]]; then
  echo "helm-ledger-retirement: Helm revision mismatch" >&2
  exit 1
fi

current_manifest="${work}/manifest-before.yaml"
helm get manifest "${release}" --namespace "${namespace}" >"${current_manifest}"
chmod 600 "${current_manifest}"
if [[ "${reconcile_existing}" == "true" ]]; then
  require_equal "final Helm manifest digest" "$(sha256_file "${current_manifest}")" "$(jq -r .expectedFinalManifestDigest "${intent}")"
else
  require_equal "Helm manifest digest" "$(sha256_file "${current_manifest}")" "$(jq -r .expectedManifestDigest "${intent}")"
fi

if [[ "${reconcile_existing}" == "false" ]]; then
  release_json="${work}/release.json"
  kubectl --namespace "${namespace}" get secret "sh.helm.release.v1.${release}.v${expected_revision}" -o jsonpath='{.data.release}' |
    base64 -d | base64 -d | gzip -d >"${release_json}"
  chmod 600 "${release_json}"
  template_list="${work}/chart-template-sha256.tsv"
  jq -r '.chart.templates[] | [.name,.data] | @tsv' "${release_json}" |
    while IFS=$'\t' read -r name data; do
      printf '%s\t%s\n' "${name}" "$(printf %s "${data}" | base64 -d | sha256sum | awk '{print $1}')"
    done | sort >"${template_list}"
  require_equal "stored chart template digest" "$(sha256_file "${template_list}")" "$(jq -r .sourceChartTemplateDigest "${intent}")"

  source_chart_commit="$(jq -r .sourceChartCommit "${intent}")"
  require_equal "source chart tree" "$(git -C "${repo_root}" rev-parse "${source_chart_commit}:deploy/helm/fugue")" "$(jq -r .sourceChartTree "${intent}")"
  require_equal "ownership patch digest" "$(sha256_file "${patch_file}")" "$(jq -r .patchDigest "${intent}")"
  chart="${work}/chart"
  mkdir -m 700 "${chart}"
  git -C "${repo_root}" archive "${source_chart_commit}:deploy/helm/fugue" | tar -x -C "${chart}"
  git -C "${chart}" apply --check "${patch_file}"
  git -C "${chart}" apply "${patch_file}"
fi

lease_holders="$(kubectl --namespace "${namespace}" get lease -o json | jq -c '[.items[] | select(.metadata.name | startswith("fugue-production-")) | select((.spec.holderIdentity // "") != "") | .metadata.name]')"
require_equal "active component leases" "${lease_holders}" "[]"

while IFS= read -r encoded; do
  resource="$(printf %s "${encoded}" | base64 -d)"
  name="$(jq -r .name <<<"${resource}")"
  live="${work}/live-${name}.json"
  kubectl --namespace "${namespace}" get daemonset "${name}" -o json >"${live}"
  jq -e --argjson expected "${resource}" '
    .metadata.uid == $expected.uid and
    .metadata.resourceVersion == $expected.resourceVersion and
    .metadata.generation == $expected.generation and
    .metadata.annotations["helm.sh/resource-policy"] == "keep" and
    .spec.template.metadata.annotations["fugue.pro/source-commit"] == $expected.source and
    .spec.template.spec.containers[0].image == $expected.image and
    .status.desiredNumberScheduled == $expected.desired and
    .status.updatedNumberScheduled == $expected.updated and
    .status.numberReady == $expected.ready and
    .status.numberAvailable == $expected.available and
    (.status.numberUnavailable // 0) == $expected.unavailable
  ' "${live}" >/dev/null || { echo "helm-ledger-retirement: live ${name} CAS/health mismatch" >&2; exit 1; }
done < <(jq -r '.retiredResources[] | @base64' "${intent}")

if [[ "${reconcile_existing}" == "true" ]]; then
  assert_manifest_retired "${current_manifest}"
  require_equal "reconciled active component leases" "$(kubectl --namespace "${namespace}" get lease -o json | jq -c '[.items[] | select(.metadata.name | startswith("fugue-production-")) | select((.spec.holderIdentity // "") != "") | .metadata.name]')" "[]"
  jq -nc --arg source "${head_sha}" --arg origin "${expected_parent}" --arg final "$(sha256_file "${current_manifest}")" \
    --argjson revision "${observed_revision}" \
    '{apiVersion:"release.fugue.dev/v1",kind:"HelmLedgerRetirementResult",status:"verified",reason:"helm-ledger-retired-readback",sourceCommit:$source,originSourceCommit:$origin,productionMutationAttempted:false,productionMutationObserved:true,helmRevision:$revision,finalManifestDigest:$final}' >"${result}"
  exit 0
fi

filter="${FUGUE_HELM_LEDGER_FILTER_BINARY:-${work}/helm-ledger-retirement-filter}"
if [[ -z "${FUGUE_HELM_LEDGER_FILTER_BINARY:-}" ]]; then
  go build -trimpath -o "${filter}" ./cmd/fugue-helm-ledger-retirement-filter
elif [[ ! -x "${filter}" ]]; then
  echo "helm-ledger-retirement: injected filter is not executable" >&2
  exit 1
fi
export FUGUE_HELM_CURRENT_MANIFEST="${current_manifest}"
export FUGUE_HELM_RETIREMENT_INTENT="${intent}"
dry_run="${work}/dry-run.txt"
helm upgrade "${release}" "${chart}" --namespace "${namespace}" --reuse-values \
  --set-string dns.ownership=declarative \
  --set-string edge.sshFront.ownership=declarative \
  --set-string imageCache.ownership=declarative \
  --post-renderer "${filter}" --dry-run=server >"${dry_run}"
chmod 600 "${dry_run}"
require_equal "post-dry-run Helm revision" "$(helm status "${release}" --namespace "${namespace}" --output json | jq -r .version)" "$(jq -r .expectedHelmRevision "${intent}")"

if [[ "${execute}" == "false" ]]; then
  jq -nc --arg source "${head_sha}" --arg digest "$(sha256_file "${current_manifest}")" \
    '{apiVersion:"release.fugue.dev/v1",kind:"HelmLedgerRetirementResult",status:"prepared",reason:"server-dry-run-verified",sourceCommit:$source,productionMutationAttempted:false,currentManifestDigest:$digest}' >"${result}"
  exit 0
fi

helm upgrade "${release}" "${chart}" --namespace "${namespace}" --reuse-values \
  --set-string dns.ownership=declarative \
  --set-string edge.sshFront.ownership=declarative \
  --set-string imageCache.ownership=declarative \
  --post-renderer "${filter}" --history-max 30 --timeout 5m

final_status="${work}/status-after.json"
helm status "${release}" --namespace "${namespace}" --output json >"${final_status}"
require_equal "final Helm revision" "$(jq -r .version "${final_status}")" "$(( $(jq -r .expectedHelmRevision "${intent}") + 1 ))"
require_equal "final Helm status" "$(jq -r .info.status "${final_status}")" "deployed"
final_manifest="${work}/manifest-after.yaml"
helm get manifest "${release}" --namespace "${namespace}" >"${final_manifest}"
chmod 600 "${final_manifest}"
require_equal "final Helm manifest digest" "$(sha256_file "${final_manifest}")" "$(jq -r .expectedFinalManifestDigest "${intent}")"
assert_manifest_retired "${final_manifest}"

while IFS= read -r encoded; do
  resource="$(printf %s "${encoded}" | base64 -d)"
  name="$(jq -r .name <<<"${resource}")"
  kubectl --namespace "${namespace}" get daemonset "${name}" -o json |
    jq -e --argjson expected "${resource}" '
      .metadata.uid == $expected.uid and .metadata.generation == $expected.generation and
      .metadata.annotations["helm.sh/resource-policy"] == "keep" and
      .spec.template.metadata.annotations["fugue.pro/source-commit"] == $expected.source and
      .spec.template.spec.containers[0].image == $expected.image and
      .status.desiredNumberScheduled == $expected.desired and
      .status.updatedNumberScheduled == $expected.updated and
      .status.numberReady == $expected.ready and .status.numberAvailable == $expected.available and
      (.status.numberUnavailable // 0) == $expected.unavailable
    ' >/dev/null || { echo "helm-ledger-retirement: final ${name} identity/health mismatch" >&2; exit 1; }
done < <(jq -r '.retiredResources[] | @base64' "${intent}")

require_equal "final active component leases" "$(kubectl --namespace "${namespace}" get lease -o json | jq -c '[.items[] | select(.metadata.name | startswith("fugue-production-")) | select((.spec.holderIdentity // "") != "") | .metadata.name]')" "[]"
jq -nc --arg source "${head_sha}" --arg current "$(sha256_file "${current_manifest}")" --arg final "$(sha256_file "${final_manifest}")" \
  --argjson revision "$(jq -r .version "${final_status}")" \
  '{apiVersion:"release.fugue.dev/v1",kind:"HelmLedgerRetirementResult",status:"verified",reason:"helm-ledger-retired",sourceCommit:$source,productionMutationAttempted:true,helmRevision:$revision,currentManifestDigest:$current,finalManifestDigest:$final}' >"${result}"
