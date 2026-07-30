#!/usr/bin/env bash
set -euo pipefail

readonly image_ref="${1:-}"
if [[ -z "${image_ref}" ]]; then
  printf 'usage: %s IMAGE_REF\n' "$0" >&2
  exit 2
fi
command -v docker >/dev/null 2>&1 || { printf 'docker is required\n' >&2; exit 2; }

readonly expected_entrypoint='["/usr/local/bin/fugue-backup-observer"]'
readonly actual_user="$(docker image inspect --format '{{.Config.User}}' "${image_ref}")"
readonly actual_entrypoint="$(docker image inspect --format '{{json .Config.Entrypoint}}' "${image_ref}")"
readonly exposed_ports="$(docker image inspect --format '{{json .Config.ExposedPorts}}' "${image_ref}")"
[[ "${actual_user}" == '65532:65532' ]] || {
  printf 'backup observer image is not non-root: %s\n' "${actual_user}" >&2
  exit 1
}
[[ "${actual_entrypoint}" == "${expected_entrypoint}" ]] || {
  printf 'backup observer entrypoint drifted: %s\n' "${actual_entrypoint}" >&2
  exit 1
}
[[ "${exposed_ports}" == 'null' ]] || {
  printf 'backup observer image declares a network port: %s\n' "${exposed_ports}" >&2
  exit 1
}

readonly container="fugue-backup-observer-probe-${RANDOM}-$$"
cleanup() {
  docker rm --force "${container}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --detach \
  --name "${container}" \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=8m \
  "${image_ref}" >/dev/null

healthy=false
for _ in $(seq 1 50); do
  if docker exec "${container}" /usr/local/bin/fugue-backup-observer probe health >/dev/null 2>&1; then
    healthy=true
    break
  fi
  sleep 0.1
done
if [[ "${healthy}" != 'true' ]]; then
  docker logs "${container}" >&2 || true
  printf 'disabled backup observer did not become live on loopback\n' >&2
  exit 1
fi
if docker exec "${container}" /usr/local/bin/fugue-backup-observer probe ready >/dev/null 2>&1; then
  printf 'disabled backup observer unexpectedly became ready\n' >&2
  exit 1
fi
if [[ -n "$(docker port "${container}" 2>/dev/null)" ]]; then
  printf 'backup observer unexpectedly published a container port\n' >&2
  exit 1
fi
if docker logs "${container}" 2>&1 | grep -Eqi 'bearer|credential|token='; then
  docker logs "${container}" >&2 || true
  printf 'backup observer logs exposed credential-shaped material\n' >&2
  exit 1
fi

docker stop --time 5 "${container}" >/dev/null
readonly exit_code="$(docker inspect --format '{{.State.ExitCode}}' "${container}")"
if [[ "${exit_code}" != '0' ]]; then
  docker logs "${container}" >&2 || true
  printf 'backup observer did not complete graceful shutdown: exit=%s\n' "${exit_code}" >&2
  exit 1
fi
printf '[backup-observer-image] non-root, read-only, default-disabled probe passed\n'
