#!/usr/bin/env bash
set -euo pipefail

image_ref="${1:?usage: test_image_plane_image.sh IMAGE_REF}"
container="fugue-image-plane-probe-${RANDOM}-$$"
store_dir="$(mktemp -d)"

cleanup() {
  docker rm -f "${container}" >/dev/null 2>&1 || true
  rm -rf "${store_dir}"
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
