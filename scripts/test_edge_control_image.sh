#!/usr/bin/env bash
set -euo pipefail

image_ref="${1:?edge-control image reference is required}"
expected_source="${2:-${GITHUB_SHA:-}}"
[[ -n "${image_ref}" ]]
[[ "${expected_source}" =~ ^[0-9a-f]{40}$ ]]

entrypoint="$(docker image inspect "${image_ref}" --format '{{json .Config.Entrypoint}}')"
user="$(docker image inspect "${image_ref}" --format '{{.Config.User}}')"
source_commit="$(docker image inspect "${image_ref}" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')"
[[ "${entrypoint}" == '["/usr/local/bin/fugue-edge-control"]' ]]
[[ "${user}" == '65532:65532' ]]
[[ "${source_commit}" == "${expected_source}" ]]

container="fugue-edge-control-probe-${RANDOM}-$$"
cleanup() {
  docker stop "${container}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --rm --detach --name "${container}" \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  --env FUGUE_EDGE_CONTROL_ENABLED=true \
  --env FUGUE_EDGE_CONTROL_BIND_ADDR=0.0.0.0:8092 \
  --publish 127.0.0.1::8092 \
  "${image_ref}" >/dev/null

port="$(docker port "${container}" 8092/tcp | sed -n '1s/.*://p')"
[[ "${port}" =~ ^[1-9][0-9]*$ ]]
for _ in {1..30}; do
  if curl -fsS --max-time 2 "http://127.0.0.1:${port}/readyz" >/dev/null; then
    break
  fi
  sleep 1
done

curl -fsS --max-time 2 "http://127.0.0.1:${port}/v1/status" |
  python3 -c '
import json,sys
x=json.load(sys.stdin)
assert x=={
  "schema":"edge-control-boundary/v1",
  "status":"ok",
  "mode":"boundary-only",
  "authority":"none",
  "enabled":True,
  "publication_enabled":False,
  "data_plane_dependency":False,
  "database_capability":False,
  "kubernetes_capability":False,
  "bundle_signer_capability":False,
}
'
curl -fsS --max-time 2 "http://127.0.0.1:${port}/metrics" | grep -F 'authority="none",mode="boundary-only"' >/dev/null
printf '[test_edge_control_image] ok\n'
