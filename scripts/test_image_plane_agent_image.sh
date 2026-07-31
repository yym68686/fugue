#!/usr/bin/env bash
set -euo pipefail

image_ref="${1:?usage: test_image_plane_agent_image.sh IMAGE_REF}"
container="fugue-image-plane-agent-probe-${RANDOM}-$$"
state_dir="$(mktemp -d)"
identity_dir="$(mktemp -d)"

cleanup() {
  docker rm -f "${container}" >/dev/null 2>&1 || true
  rm -rf "${state_dir}" "${identity_dir}"
}
trap cleanup EXIT

entrypoint="$(docker image inspect --format '{{json .Config.Entrypoint}}' "${image_ref}")"
if [[ "${entrypoint}" != '["/usr/local/bin/fugue-image-plane-agent"]' ]]; then
  printf 'unexpected image-plane agent entrypoint: %s\n' "${entrypoint}" >&2
  exit 1
fi
image_user="$(docker image inspect --format '{{.Config.User}}' "${image_ref}")"
if [[ "${image_user}" != "65532:65532" ]]; then
  printf 'unexpected image-plane agent user: %s\n' "${image_user}" >&2
  exit 1
fi
exposed_ports="$(docker image inspect --format '{{json .Config.ExposedPorts}}' "${image_ref}")"
if [[ "${exposed_ports}" != "null" ]]; then
  printf 'image-plane agent artifact declares network ports: %s\n' "${exposed_ports}" >&2
  exit 1
fi

docker run --detach \
  --name "${container}" \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=16m \
  --mount "type=bind,source=${state_dir},target=/var/lib/fugue/image-cache" \
  --mount "type=bind,source=${identity_dir},target=/run/fugue/image-cache,readonly" \
  --env FUGUE_IMAGE_CACHE_LISTEN_ADDR=127.0.0.1:5001 \
  --env FUGUE_IMAGE_CACHE_CLUSTER_NODE_NAME=worker-probe \
  --env FUGUE_IMAGE_CACHE_PLATFORM_PLAN_SHADOW_ENABLED=true \
  --env FUGUE_API_BASE=https://api.invalid.example \
  --env FUGUE_IMAGE_CACHE_PLATFORM_CREDENTIAL_FILE=/run/fugue/image-cache/platform-component-credential.json \
  --env FUGUE_IMAGE_CACHE_REPLICATION_PLAN_PATH=/var/lib/fugue/image-cache/replication-plan.json \
  "${image_ref}" >/dev/null

healthy=false
for _ in $(seq 1 50); do
  if docker exec "${container}" /usr/bin/wget -q -T 1 -Y off --spider \
      http://127.0.0.1:5001/healthz >/dev/null 2>&1; then
    healthy=true
    break
  fi
  sleep 0.1
done
if [[ "${healthy}" != "true" ]]; then
  docker logs "${container}" >&2 || true
  printf 'image-plane agent did not become live on container loopback\n' >&2
  exit 1
fi

health_payload="$(docker exec "${container}" /usr/bin/wget -q -T 2 -Y off -O - \
  http://127.0.0.1:5001/fugue/cache/v1/health)"
python3 -c '
import json, sys
payload = json.load(sys.stdin)
shadow = payload.get("platform_plan_shadow") or {}
if payload.get("status") != "ok" or payload.get("mode") != "platform-plan-shadow" or payload.get("cluster_node") != "worker-probe":
    raise SystemExit("image-plane agent identity or mode is invalid")
if shadow.get("enabled") is not True or shadow.get("observation_only") is not True:
    raise SystemExit("image-plane agent is not enabled and observation-only")
encoded = json.dumps(payload).lower()
if "token" in encoded or "credential_file" in encoded:
    raise SystemExit("image-plane agent health leaked credential material")
' <<<"${health_payload}"

if docker exec "${container}" /usr/bin/wget -q -T 2 -Y off --spider \
    http://127.0.0.1:5001/readyz >/dev/null 2>&1; then
  printf 'image-plane agent became ready without a credential and observation\n' >&2
  exit 1
fi
for forbidden_path in /v2/ /fugue/cache/v1/inventory /fugue/cache/v1/prune; do
  if docker exec "${container}" /usr/bin/wget -q -T 2 -Y off --spider \
      "http://127.0.0.1:5001${forbidden_path}" >/dev/null 2>&1; then
    printf 'image-plane agent exposed forbidden legacy path %s\n' "${forbidden_path}" >&2
    exit 1
  fi
done
if [[ -n "$(docker port "${container}" 2>/dev/null)" ]]; then
  printf 'image-plane agent unexpectedly published a container port\n' >&2
  exit 1
fi
if find "${state_dir}" -mindepth 1 -print -quit | grep -q .; then
  find "${state_dir}" -mindepth 1 -maxdepth 2 -print >&2 || true
  printf 'image-plane agent initialized legacy registry state without a plan\n' >&2
  exit 1
fi
if docker logs "${container}" 2>&1 | grep -Eq 'fugue-image-cache listening|store=|registry_base='; then
  docker logs "${container}" >&2 || true
  printf 'image-plane agent entered the legacy registry startup path\n' >&2
  exit 1
fi

docker stop --time 5 "${container}" >/dev/null
exit_code="$(docker inspect --format '{{.State.ExitCode}}' "${container}")"
if [[ "${exit_code}" != "0" ]]; then
  docker logs "${container}" >&2 || true
  printf 'image-plane agent did not complete graceful shutdown: exit=%s\n' "${exit_code}" >&2
  exit 1
fi
