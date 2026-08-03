#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
cleanup() { rm -rf "${TMP}"; }
trap cleanup EXIT

BIN="${TMP}/bin"
STATE="${TMP}/state"
mkdir -p "${BIN}" "${STATE}"
printf '0\n' >"${STATE}/installed"
printf '0\n' >"${STATE}/writes"
printf '0\n' >"${STATE}/revision"
printf '\n' >"${STATE}/source"
printf '\n' >"${STATE}/image"
printf '\n' >"${STATE}/digest"

cat >"${BIN}/curl" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
args="$*"
if [[ "${args}" == *'/git/ref/heads/main'* ]]; then
  printf '{"object":{"sha":"%s"}}\n' "${FUGUE_EDGE_CONTROL_EXPECTED_SOURCE}"
elif [[ "${args}" == *'/actions/runs?status='* ]]; then
  printf '%s\n' '{"workflow_runs":[]}'
elif [[ "${args}" == *'api.example.test/healthz'* ]]; then
  :
else
  printf 'unexpected curl command: %s\n' "${args}" >&2
  exit 1
fi
SH
cat >"${BIN}/sleep" <<'SH'
#!/usr/bin/env bash
exit 0
SH
cat >"${BIN}/timeout" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == --kill-after=* ]]; then shift; fi
[[ "${1:-}" =~ ^[1-9][0-9]*s$ ]] && shift
exec "$@"
SH
cat >"${BIN}/helm" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
state="${FAKE_STATE}"
installed="$(<"${state}/installed")"
revision="$(<"${state}/revision")"
case "${1:-}" in
  lint) exit 0 ;;
  template)
    cat <<EOF
apiVersion: v1
kind: ServiceAccount
metadata: {name: edge-control-fugue-edge-control}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: edge-control-fugue-edge-control}
EOF
    ;;
  status)
    release="${2:-}"
    if [[ "${release}" == "fugue" ]]; then
      printf '%s\n' '{"version":817,"info":{"status":"deployed"}}'
    elif [[ "${installed}" == "1" ]]; then
      printf '{"version":%s,"info":{"status":"deployed"}}\n' "${revision}"
    else
      printf '%s\n' 'Error: release: not found' >&2
      exit 1
    fi
    ;;
  get)
    [[ "${2:-}" == "values" && "${installed}" == "1" ]]
    printf '{"enabled":true,"image":{"digest":"%s","repository":"%s","sourceCommit":"%s"}}\n' \
      "$(<"${state}/digest")" "$(<"${state}/image")" "$(<"${state}/source")"
    ;;
  upgrade)
    if [[ "${2:-}" == '--help' ]]; then printf '%s\n' '      --rollback-on-failure'; exit 0; fi
    count="$(<"${state}/writes")"; printf '%s\n' "$((count+1))" >"${state}/writes"
    printf '1\n' >"${state}/installed"
    printf '%s\n' "$((revision+1))" >"${state}/revision"
    printf '%s\n' "${FUGUE_EDGE_CONTROL_EXPECTED_SOURCE}" >"${state}/source"
    printf '%s\n' "${FUGUE_EDGE_CONTROL_IMAGE}" >"${state}/image"
    printf '%s\n' "${FUGUE_EDGE_CONTROL_IMAGE_DIGEST}" >"${state}/digest"
    ;;
  *) printf 'unexpected helm command: %s\n' "$*" >&2; exit 1 ;;
esac
SH
cat >"${BIN}/kubectl" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
state="${FAKE_STATE}"; installed="$(<"${state}/installed")"
live_source="$(<"${state}/source")"; live_image="$(<"${state}/image")"; live_digest="$(<"${state}/digest")"
if [[ "$*" == *'apply --dry-run=server'* ]]; then exit 0; fi
if [[ "$*" == *' get configmap/fugue-fugue-public-data-plane-release '* ]]; then
  printf '%s\n' '{"metadata":{"uid":"record-uid","resourceVersion":"42"},"data":{"active_slots":"{\"fugue-fugue-edge-country-us\":\"a\",\"fugue-fugue-edge-country-de\":\"a\"}"}}'
  exit 0
fi
if [[ "$*" == *' get deployment,daemonset,pod '* && "$*" == *'app.kubernetes.io/instance=fugue'* ]]; then
  legacy_api_image_id_digest="${FAKE_LEGACY_API_IMAGE_ID_DIGEST:-sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}"
  cat <<JSON
{"items":[
{"kind":"Deployment","metadata":{"name":"fugue-fugue-api","uid":"api-uid","generation":1,"labels":{"app.kubernetes.io/component":"api"}},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"api","image":"registry.example.test/fugue/api@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]}}},"status":{"observedGeneration":1,"updatedReplicas":1,"readyReplicas":1,"availableReplicas":1}},
{"kind":"Deployment","metadata":{"name":"fugue-fugue-controller","uid":"controller-uid","generation":1,"labels":{"app.kubernetes.io/component":"controller"}},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"controller","image":"registry.example.test/fugue/controller@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}]}}},"status":{"observedGeneration":1,"updatedReplicas":1,"readyReplicas":1,"availableReplicas":1}},
{"kind":"DaemonSet","metadata":{"name":"fugue-fugue-edge-country-us-front","uid":"us-front","generation":1,"labels":{"fugue.io/rollout-subsystem":"public-data-plane"}},"spec":{"selector":{"matchLabels":{"test.front":"us"}},"template":{"spec":{"containers":[{"name":"edge-front","env":[{"name":"FUGUE_EDGE_FRONT_ACTIVE_SLOT_FILE","value":"/state/active-slot"}]}]}}},"status":{"observedGeneration":1,"desiredNumberScheduled":1,"numberReady":1,"numberAvailable":1,"numberUnavailable":0}},
{"kind":"DaemonSet","metadata":{"name":"fugue-fugue-edge-country-us-worker-a","uid":"us-a","generation":1,"labels":{"fugue.io/rollout-subsystem":"public-data-plane"}},"spec":{},"status":{"observedGeneration":1,"desiredNumberScheduled":1,"numberReady":1,"numberAvailable":1,"numberUnavailable":0}},
{"kind":"DaemonSet","metadata":{"name":"fugue-fugue-edge-country-de-front","uid":"de-front","generation":1,"labels":{"fugue.io/rollout-subsystem":"public-data-plane"}},"spec":{"selector":{"matchLabels":{"test.front":"de"}},"template":{"spec":{"containers":[{"name":"edge-front","env":[{"name":"FUGUE_EDGE_FRONT_ACTIVE_SLOT_FILE","value":"/state/active-slot"}]}]}}},"status":{"observedGeneration":1,"desiredNumberScheduled":1,"numberReady":1,"numberAvailable":1,"numberUnavailable":0}},
{"kind":"DaemonSet","metadata":{"name":"fugue-fugue-edge-country-de-worker-a","uid":"de-a","generation":1,"labels":{"fugue.io/rollout-subsystem":"public-data-plane"}},"spec":{},"status":{"observedGeneration":1,"desiredNumberScheduled":1,"numberReady":1,"numberAvailable":1,"numberUnavailable":0}},
{"kind":"Pod","metadata":{"name":"api-1","uid":"api-pod","labels":{"app.kubernetes.io/component":"api"}},"spec":{"containers":[{"name":"api","image":"registry.example.test/fugue/api@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]},"status":{"phase":"Running","containerStatuses":[{"name":"api","ready":true,"image":"sha256:1111111111111111111111111111111111111111111111111111111111111111","imageID":"registry.example.test/fugue/api@${legacy_api_image_id_digest}"}]}},
{"kind":"Pod","metadata":{"name":"controller-1","uid":"controller-pod","labels":{"app.kubernetes.io/component":"controller"}},"spec":{"containers":[{"name":"controller","image":"registry.example.test/fugue/controller@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}]},"status":{"phase":"Running","containerStatuses":[{"name":"controller","ready":true,"image":"sha256:2222222222222222222222222222222222222222222222222222222222222222","imageID":"docker-pullable://registry.example.test/fugue/controller@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}]}},
{"kind":"Pod","metadata":{"name":"us-front-1","uid":"us-front-pod","labels":{"fugue.io/rollout-subsystem":"public-data-plane","test.front":"us"}},"status":{"phase":"Running","containerStatuses":[{"name":"edge-front","ready":true,"restartCount":0,"image":"front@sha256:ccc"}]}},
{"kind":"Pod","metadata":{"name":"us-a-1","labels":{"fugue.io/rollout-subsystem":"public-data-plane"}},"status":{"phase":"Running","containerStatuses":[{"ready":true}]}},
{"kind":"Pod","metadata":{"name":"de-front-1","uid":"de-front-pod","labels":{"fugue.io/rollout-subsystem":"public-data-plane","test.front":"de"}},"status":{"phase":"Running","containerStatuses":[{"name":"edge-front","ready":true,"restartCount":0,"image":"front@sha256:ccc"}]}},
{"kind":"Pod","metadata":{"name":"de-a-1","labels":{"fugue.io/rollout-subsystem":"public-data-plane"}},"status":{"phase":"Running","containerStatuses":[{"ready":true}]}}
]}
JSON
  exit 0
fi
if [[ "$*" == *' get serviceaccount,deployment,service,poddisruptionbudget,networkpolicy '* ]]; then
  if [[ "${installed}" == "0" ]]; then
    [[ "$*" == *' -o name'* ]] || printf '%s\n' '{"items":[]}'
    exit 0
  fi
  cat <<JSON
{"items":[
{"kind":"ServiceAccount","metadata":{"name":"edge-control-fugue-edge-control"},"automountServiceAccountToken":false},
{"kind":"Deployment","metadata":{"name":"edge-control-fugue-edge-control","uid":"deploy-uid","generation":1},"spec":{"replicas":1,"template":{"metadata":{"annotations":{"fugue.pro/source-commit":"${live_source}","fugue.pro/image-digest":"${live_digest}","fugue.pro/edge-control-authority":"none","fugue.pro/edge-control-mode":"boundary-only","fugue.pro/edge-control-publication":"disabled"}},"spec":{"automountServiceAccountToken":false,"containers":[{"name":"edge-control","image":"${live_image}@${live_digest}","env":[{"name":"FUGUE_EDGE_CONTROL_ENABLED","value":"true"},{"name":"FUGUE_EDGE_CONTROL_BIND_ADDR","value":"0.0.0.0:8092"},{"name":"FUGUE_EDGE_CONTROL_SHUTDOWN_TIMEOUT","value":"10s"}]}]}}},"status":{"observedGeneration":1,"updatedReplicas":1,"readyReplicas":1,"availableReplicas":1}},
{"kind":"Service","metadata":{"name":"edge-control-fugue-edge-control"},"spec":{"type":"ClusterIP"}},
{"kind":"PodDisruptionBudget","metadata":{"name":"edge-control-fugue-edge-control"},"spec":{"minAvailable":1}},
{"kind":"NetworkPolicy","metadata":{"name":"edge-control-fugue-edge-control"},"spec":{"policyTypes":["Ingress","Egress"],"egress":[]}}
]}
JSON
  exit 0
fi
if [[ "$*" == *' get pods '* && "$*" == *'app.kubernetes.io/instance=edge-control'* ]]; then
  [[ "${installed}" == "1" ]] || { printf '%s\n' '{"items":[]}'; exit 0; }
  runtime_digest="${FAKE_EDGE_CONTROL_IMAGE_ID_DIGEST:-${live_digest}}"
  cat <<JSON
{"items":[{"kind":"Pod","metadata":{"name":"edge-control-1","uid":"pod-uid","annotations":{"fugue.pro/source-commit":"${live_source}","fugue.pro/image-digest":"${live_digest}","fugue.pro/edge-control-authority":"none","fugue.pro/edge-control-mode":"boundary-only","fugue.pro/edge-control-publication":"disabled"}},"spec":{"containers":[{"name":"edge-control","image":"${live_image}@${live_digest}"}]},"status":{"phase":"Running","containerStatuses":[{"name":"edge-control","ready":true,"restartCount":0,"image":"sha256:3333333333333333333333333333333333333333333333333333333333333333","imageID":"containerd://${live_image}@${runtime_digest}"}]}}]}
JSON
  exit 0
fi
if [[ "$*" == *' exec pod/edge-control-1 '* ]]; then
  if [[ "$*" == *'/v1/status'* ]]; then
    printf '%s\n' '{"schema":"edge-control-boundary/v1","status":"ok","mode":"boundary-only","authority":"none","enabled":true,"publication_enabled":false,"data_plane_dependency":false,"database_capability":false,"kubernetes_capability":false,"bundle_signer_capability":false}'
  else
    printf '%s\n' 'fugue_edge_control_info{authority="none",mode="boundary-only"} 1'
  fi
  exit 0
fi
if [[ "$*" == *' exec pod/us-front-1 '* || "$*" == *' exec pod/de-front-1 '* ]]; then
  printf 'a\n'
  exit 0
fi
printf 'unexpected kubectl command: %s\n' "$*" >&2; exit 1
SH
chmod +x "${BIN}"/*

source_commit='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
image_digest='sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
artifact_digest='sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'
receipt_digest='sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd'
output="${TMP}/receipt/receipt.json"
PATH="${BIN}:${PATH}" FAKE_STATE="${STATE}" KUBECTL="${BIN}/kubectl" GITHUB_ACTIONS=true \
GITHUB_REPOSITORY=example/fugue GITHUB_RUN_ID=123 GITHUB_RUN_ATTEMPT=1 GITHUB_TOKEN=test-token \
FUGUE_EDGE_CONTROL_EXPECTED_SOURCE="${source_commit}" FUGUE_EDGE_CONTROL_IMAGE=registry.example.test/fugue/edge-control \
FUGUE_EDGE_CONTROL_IMAGE_DIGEST="${image_digest}" FUGUE_EDGE_CONTROL_IMAGE_RECEIPT_DIGEST="${receipt_digest}" \
FUGUE_EDGE_CONTROL_SOURCE_RUN_ID=99 FUGUE_EDGE_CONTROL_SOURCE_ARTIFACT_ID=88 \
FUGUE_EDGE_CONTROL_SOURCE_ARTIFACT_DIGEST="${artifact_digest}" FUGUE_EDGE_CONTROL_NAMESPACE=fugue-system \
FUGUE_EDGE_CONTROL_RELEASE=edge-control FUGUE_LEGACY_RELEASE=fugue FUGUE_LEGACY_RELEASE_FULLNAME=fugue-fugue \
FUGUE_PRODUCT_HEALTH_URL=https://api.example.test/healthz FUGUE_EDGE_CONTROL_RECEIPT_PATH="${output}" \
bash "${ROOT}/scripts/deploy_edge_control_shadow.sh"

[[ "$(<"${STATE}/writes")" == '1' ]]
python3 - "${output}" <<'PY'
import json,sys
value=json.load(open(sys.argv[1],encoding="utf-8"))
assert value["schema"]=="edge-control-shadow-release-receipt/v1"
assert value["authority"]=="none" and value["mode"]=="boundary-only"
assert value["publication_enabled"] is False and value["data_plane_dependency"] is False
assert value["legacy_spec_digest_before"]==value["legacy_spec_digest_after"]
assert value["pod_restart_count"]==0 and value["soak_samples"]==6
assert value["prestate"]=="absent" and value["previous_revision"]==0
assert value["receipt_digest"].startswith("sha256:")
PY

second_source='eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
second_digest='sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'
second_output="${TMP}/receipt-upgrade/receipt.json"
PATH="${BIN}:${PATH}" FAKE_STATE="${STATE}" KUBECTL="${BIN}/kubectl" GITHUB_ACTIONS=true \
GITHUB_REPOSITORY=example/fugue GITHUB_RUN_ID=124 GITHUB_RUN_ATTEMPT=1 GITHUB_TOKEN=test-token \
FUGUE_EDGE_CONTROL_EXPECTED_SOURCE="${second_source}" FUGUE_EDGE_CONTROL_IMAGE=registry.example.test/fugue/edge-control \
FUGUE_EDGE_CONTROL_IMAGE_DIGEST="${second_digest}" FUGUE_EDGE_CONTROL_IMAGE_RECEIPT_DIGEST="${receipt_digest}" \
FUGUE_EDGE_CONTROL_SOURCE_RUN_ID=100 FUGUE_EDGE_CONTROL_SOURCE_ARTIFACT_ID=89 \
FUGUE_EDGE_CONTROL_SOURCE_ARTIFACT_DIGEST="${artifact_digest}" FUGUE_EDGE_CONTROL_NAMESPACE=fugue-system \
FUGUE_EDGE_CONTROL_RELEASE=edge-control FUGUE_LEGACY_RELEASE=fugue FUGUE_LEGACY_RELEASE_FULLNAME=fugue-fugue \
FUGUE_PRODUCT_HEALTH_URL=https://api.example.test/healthz FUGUE_EDGE_CONTROL_RECEIPT_PATH="${second_output}" \
bash "${ROOT}/scripts/deploy_edge_control_shadow.sh"

[[ "$(<"${STATE}/writes")" == '2' ]]
python3 - "${second_output}" "${source_commit}" "${image_digest}" <<'PY'
import json,sys
value=json.load(open(sys.argv[1],encoding="utf-8"))
assert value["prestate"]=="deployed" and value["previous_revision"]==1
assert value["previous_source_commit"]==sys.argv[2]
assert value["previous_image"]=="registry.example.test/fugue/edge-control"
assert value["previous_image_digest"]==sys.argv[3]
assert value["previous_runtime_digest"].startswith("sha256:")
assert value["helm_revision"]==2 and value["authority"]=="none"
PY

bad_output="${TMP}/receipt-bad-image-id/receipt.json"
bad_log="${TMP}/bad-image-id.log"
if PATH="${BIN}:${PATH}" FAKE_STATE="${STATE}" KUBECTL="${BIN}/kubectl" GITHUB_ACTIONS=true \
GITHUB_REPOSITORY=example/fugue GITHUB_RUN_ID=125 GITHUB_RUN_ATTEMPT=1 GITHUB_TOKEN=test-token \
FAKE_LEGACY_API_IMAGE_ID_DIGEST=sha256:9999999999999999999999999999999999999999999999999999999999999999 \
FUGUE_EDGE_CONTROL_EXPECTED_SOURCE="${second_source}" FUGUE_EDGE_CONTROL_IMAGE=registry.example.test/fugue/edge-control \
FUGUE_EDGE_CONTROL_IMAGE_DIGEST="${second_digest}" FUGUE_EDGE_CONTROL_IMAGE_RECEIPT_DIGEST="${receipt_digest}" \
FUGUE_EDGE_CONTROL_SOURCE_RUN_ID=100 FUGUE_EDGE_CONTROL_SOURCE_ARTIFACT_ID=89 \
FUGUE_EDGE_CONTROL_SOURCE_ARTIFACT_DIGEST="${artifact_digest}" FUGUE_EDGE_CONTROL_NAMESPACE=fugue-system \
FUGUE_EDGE_CONTROL_RELEASE=edge-control FUGUE_LEGACY_RELEASE=fugue FUGUE_LEGACY_RELEASE_FULLNAME=fugue-fugue \
FUGUE_PRODUCT_HEALTH_URL=https://api.example.test/healthz FUGUE_EDGE_CONTROL_RECEIPT_PATH="${bad_output}" \
bash "${ROOT}/scripts/deploy_edge_control_shadow.sh" >"${bad_log}" 2>&1; then
  printf 'mismatched legacy imageID unexpectedly passed\n' >&2
  exit 1
fi
grep -q 'legacy api pod is not ready on the deployment image' "${bad_log}"
[[ "$(<"${STATE}/writes")" == '2' ]]

bad_runtime_output="${TMP}/receipt-bad-runtime-image-id/receipt.json"
bad_runtime_log="${TMP}/bad-runtime-image-id.log"
if PATH="${BIN}:${PATH}" FAKE_STATE="${STATE}" KUBECTL="${BIN}/kubectl" GITHUB_ACTIONS=true \
GITHUB_REPOSITORY=example/fugue GITHUB_RUN_ID=126 GITHUB_RUN_ATTEMPT=1 GITHUB_TOKEN=test-token \
FAKE_EDGE_CONTROL_IMAGE_ID_DIGEST=sha256:8888888888888888888888888888888888888888888888888888888888888888 \
FUGUE_EDGE_CONTROL_EXPECTED_SOURCE="${second_source}" FUGUE_EDGE_CONTROL_IMAGE=registry.example.test/fugue/edge-control \
FUGUE_EDGE_CONTROL_IMAGE_DIGEST="${second_digest}" FUGUE_EDGE_CONTROL_IMAGE_RECEIPT_DIGEST="${receipt_digest}" \
FUGUE_EDGE_CONTROL_SOURCE_RUN_ID=100 FUGUE_EDGE_CONTROL_SOURCE_ARTIFACT_ID=89 \
FUGUE_EDGE_CONTROL_SOURCE_ARTIFACT_DIGEST="${artifact_digest}" FUGUE_EDGE_CONTROL_NAMESPACE=fugue-system \
FUGUE_EDGE_CONTROL_RELEASE=edge-control FUGUE_LEGACY_RELEASE=fugue FUGUE_LEGACY_RELEASE_FULLNAME=fugue-fugue \
FUGUE_PRODUCT_HEALTH_URL=https://api.example.test/healthz FUGUE_EDGE_CONTROL_RECEIPT_PATH="${bad_runtime_output}" \
bash "${ROOT}/scripts/deploy_edge_control_shadow.sh" >"${bad_runtime_log}" 2>&1; then
  printf 'mismatched edge-control imageID unexpectedly passed\n' >&2
  exit 1
fi
grep -q 'edge-control pod is not pristine and ready' "${bad_runtime_log}"
[[ "$(<"${STATE}/writes")" == '2' ]]
printf '[test_deploy_edge_control_shadow] ok\n'
