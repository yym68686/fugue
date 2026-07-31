#!/usr/bin/env bash
set -euo pipefail

image_ref="${1:?usage: test_image_plane_image.sh IMAGE_REF}"
container="fugue-image-plane-probe-${RANDOM}-$$"
invalid_config_container="${container}-invalid-config"
shadow_agent_container="${container}-shadow-agent"
store_dir="$(mktemp -d)"
shadow_state_dir="$(mktemp -d)"
shadow_identity_dir="$(mktemp -d)"

cleanup() {
  docker rm -f "${container}" >/dev/null 2>&1 || true
  docker rm -f "${invalid_config_container}" >/dev/null 2>&1 || true
  docker rm -f "${shadow_agent_container}" >/dev/null 2>&1 || true
  rm -rf "${store_dir}" "${shadow_state_dir}" "${shadow_identity_dir}"
}
trap cleanup EXIT

entrypoint="$(docker image inspect --format '{{json .Config.Entrypoint}}' "${image_ref}")"
if [[ "${entrypoint}" != '["/usr/local/bin/fugue-image-cache"]' ]]; then
  printf 'unexpected image-plane entrypoint: %s\n' "${entrypoint}" >&2
  exit 1
fi

docker run --detach \
  --name "${container}" \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=16m \
  --mount "type=bind,source=${store_dir},target=/var/lib/fugue/image-cache" \
  --publish 127.0.0.1::5000 \
  --env FUGUE_IMAGE_CACHE_DISK_LIMIT_ENABLED=false \
  "${image_ref}" >/dev/null

probe_tool="$(docker exec "${container}" /bin/sh -c 'command -v wget')"
if [[ "${probe_tool}" != "/usr/bin/wget" ]]; then
  docker logs "${container}" >&2 || true
  printf 'image-plane chart probe dependency is unavailable: %s\n' "${probe_tool}" >&2
  exit 1
fi

endpoint=""
for _ in $(seq 1 50); do
  endpoint="$(docker port "${container}" 5000/tcp 2>/dev/null | head -n 1 || true)"
  if [[ "${endpoint}" =~ ^127\.0\.0\.1:[1-9][0-9]*$ ]] &&
    curl --fail --silent --max-time 1 "http://${endpoint}/healthz" >/dev/null; then
    break
  fi
  sleep 0.1
done

if [[ ! "${endpoint}" =~ ^127\.0\.0\.1:[1-9][0-9]*$ ]]; then
  docker logs "${container}" >&2 || true
  printf 'image-plane probe did not obtain a bounded loopback port\n' >&2
  exit 1
fi

health_payload="$(curl --fail --silent --show-error --max-time 2 "http://${endpoint}/fugue/cache/v1/health")"
python3 -c '
import json, sys
payload = json.load(sys.stdin)
shadow = payload.get("platform_plan_shadow") or {}
if payload.get("status") != "ok" or shadow.get("state") != "disabled" or shadow.get("enabled") is not False or shadow.get("observation_only") is not True:
    raise SystemExit("default image-plane shadow status is not disabled and observation-only")
' <<<"${health_payload}"

for path in /healthz /readyz /v2/; do
  if ! curl --fail --silent --show-error --max-time 2 "http://${endpoint}${path}" >/dev/null; then
    docker logs "${container}" >&2 || true
    printf 'image-plane probe failed for %s\n' "${path}" >&2
    exit 1
  fi
done

if [[ ! -d "${store_dir}/registry" ]]; then
  docker logs "${container}" >&2 || true
  printf 'image-plane container did not initialize its external store\n' >&2
  exit 1
fi

docker stop --time 5 "${container}" >/dev/null
exit_code="$(docker inspect --format '{{.State.ExitCode}}' "${container}")"
if [[ "${exit_code}" != "0" ]]; then
  docker logs "${container}" >&2 || true
  printf 'image-plane container did not complete graceful shutdown: exit=%s\n' "${exit_code}" >&2
  exit 1
fi

docker rm "${container}" >/dev/null

docker run --detach \
  --name "${shadow_agent_container}" \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=16m \
  --mount "type=bind,source=${shadow_state_dir},target=/var/lib/fugue/image-cache" \
  --mount "type=bind,source=${shadow_identity_dir},target=/run/fugue/image-cache,readonly" \
  --env FUGUE_IMAGE_CACHE_LISTEN_ADDR=127.0.0.1:5001 \
  --env FUGUE_IMAGE_CACHE_CLUSTER_NODE_NAME=worker-probe \
  --env FUGUE_IMAGE_CACHE_PLATFORM_PLAN_SHADOW_ENABLED=true \
  --env FUGUE_API_BASE=https://api.invalid.example \
  --env FUGUE_IMAGE_CACHE_PLATFORM_CREDENTIAL_FILE=/run/fugue/image-cache/platform-component-credential.json \
  --env FUGUE_IMAGE_CACHE_REPLICATION_PLAN_PATH=/var/lib/fugue/image-cache/replication-plan.json \
  "${image_ref}" platform-plan-shadow >/dev/null

shadow_healthy=false
for _ in $(seq 1 50); do
  if docker exec "${shadow_agent_container}" /usr/bin/wget -q -T 1 -Y off --spider \
      http://127.0.0.1:5001/healthz >/dev/null 2>&1; then
    shadow_healthy=true
    break
  fi
  sleep 0.1
done
if [[ "${shadow_healthy}" != "true" ]]; then
  docker logs "${shadow_agent_container}" >&2 || true
  printf 'image-plane shadow agent did not become live on container loopback\n' >&2
  exit 1
fi

shadow_health_payload="$(docker exec "${shadow_agent_container}" /usr/bin/wget -q -T 2 -Y off -O - \
  http://127.0.0.1:5001/fugue/cache/v1/health)"
python3 -c '
import json, sys
payload = json.load(sys.stdin)
shadow = payload.get("platform_plan_shadow") or {}
if payload.get("status") != "ok" or payload.get("mode") != "platform-plan-shadow" or payload.get("cluster_node") != "worker-probe":
    raise SystemExit("dedicated image-plane agent identity or mode is invalid")
if shadow.get("enabled") is not True or shadow.get("observation_only") is not True:
    raise SystemExit("dedicated image-plane agent is not enabled and observation-only")
encoded = json.dumps(payload).lower()
if "token" in encoded or "credential_file" in encoded:
    raise SystemExit("dedicated image-plane agent health leaked credential material")
' <<<"${shadow_health_payload}"

if docker exec "${shadow_agent_container}" /usr/bin/wget -q -T 2 -Y off --spider \
    http://127.0.0.1:5001/readyz >/dev/null 2>&1; then
  printf 'image-plane shadow agent became ready without a credential and observation\n' >&2
  exit 1
fi
for forbidden_path in /v2/ /fugue/cache/v1/inventory /fugue/cache/v1/prune; do
  if docker exec "${shadow_agent_container}" /usr/bin/wget -q -T 2 -Y off --spider \
      "http://127.0.0.1:5001${forbidden_path}" >/dev/null 2>&1; then
    printf 'image-plane shadow agent exposed forbidden legacy path %s\n' "${forbidden_path}" >&2
    exit 1
  fi
done
if [[ -n "$(docker port "${shadow_agent_container}" 2>/dev/null)" ]]; then
  printf 'image-plane shadow agent unexpectedly published a container port\n' >&2
  exit 1
fi
if find "${shadow_state_dir}" -mindepth 1 -print -quit | grep -q .; then
  find "${shadow_state_dir}" -mindepth 1 -maxdepth 2 -print >&2 || true
  printf 'image-plane shadow agent initialized legacy registry state without a plan\n' >&2
  exit 1
fi
if docker logs "${shadow_agent_container}" 2>&1 | grep -Eq 'fugue-image-cache listening|store=|registry_base='; then
  docker logs "${shadow_agent_container}" >&2 || true
  printf 'image-plane shadow agent entered the legacy registry startup path\n' >&2
  exit 1
fi

docker stop --time 5 "${shadow_agent_container}" >/dev/null
shadow_exit_code="$(docker inspect --format '{{.State.ExitCode}}' "${shadow_agent_container}")"
if [[ "${shadow_exit_code}" != "0" ]]; then
  docker logs "${shadow_agent_container}" >&2 || true
  printf 'image-plane shadow agent did not complete graceful shutdown: exit=%s\n' "${shadow_exit_code}" >&2
  exit 1
fi
docker rm "${shadow_agent_container}" >/dev/null

docker run --detach \
  --name "${invalid_config_container}" \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=16m \
  --mount "type=bind,source=${store_dir},target=/var/lib/fugue/image-cache" \
  --publish 127.0.0.1::5000 \
  --env FUGUE_IMAGE_CACHE_DISK_LIMIT_ENABLED=false \
  --env FUGUE_IMAGE_CACHE_CLUSTER_NODE_NAME=worker-probe \
  --env FUGUE_IMAGE_CACHE_PLATFORM_PLAN_SHADOW_ENABLED=true \
  "${image_ref}" >/dev/null

invalid_endpoint=""
for _ in $(seq 1 50); do
  invalid_endpoint="$(docker port "${invalid_config_container}" 5000/tcp 2>/dev/null | head -n 1 || true)"
  if [[ "${invalid_endpoint}" =~ ^127\.0\.0\.1:[1-9][0-9]*$ ]] &&
    curl --fail --silent --max-time 1 "http://${invalid_endpoint}/healthz" >/dev/null; then
    break
  fi
  sleep 0.1
done

if [[ ! "${invalid_endpoint}" =~ ^127\.0\.0\.1:[1-9][0-9]*$ ]]; then
  docker logs "${invalid_config_container}" >&2 || true
  printf 'image-plane invalid-config probe did not obtain a bounded loopback port\n' >&2
  exit 1
fi

invalid_health_payload="$(curl --fail --silent --show-error --max-time 2 "http://${invalid_endpoint}/fugue/cache/v1/health")"
python3 -c '
import json, sys
payload = json.load(sys.stdin)
shadow = payload.get("platform_plan_shadow") or {}
if payload.get("status") != "ok" or shadow.get("state") != "configuration_error" or shadow.get("observation_only") is not True:
    raise SystemExit("invalid optional shadow configuration did not preserve registry health with an explicit error state")
' <<<"${invalid_health_payload}"

docker stop --time 5 "${invalid_config_container}" >/dev/null
invalid_exit_code="$(docker inspect --format '{{.State.ExitCode}}' "${invalid_config_container}")"
if [[ "${invalid_exit_code}" != "0" ]]; then
  docker logs "${invalid_config_container}" >&2 || true
  printf 'image-plane invalid-config probe did not complete graceful shutdown: exit=%s\n' "${invalid_exit_code}" >&2
  exit 1
fi
