#!/usr/bin/env bash
set -euo pipefail

readonly image_ref="${1:-}"
if [[ -z "${image_ref}" ]]; then
  printf 'usage: %s IMAGE_REF\n' "$0" >&2
  exit 2
fi
command -v docker >/dev/null 2>&1 || { printf 'docker is required\n' >&2; exit 2; }

readonly binary='/usr/local/bin/fugue-backup-materializer-validator'
readonly expected_entrypoint='["/usr/local/bin/fugue-backup-materializer-validator"]'
readonly actual_user="$(docker image inspect --format '{{.Config.User}}' "${image_ref}")"
readonly actual_entrypoint="$(docker image inspect --format '{{json .Config.Entrypoint}}' "${image_ref}")"
readonly exposed_ports="$(docker image inspect --format '{{json .Config.ExposedPorts}}' "${image_ref}")"
[[ "${actual_user}" == '65532:65532' ]] || {
  printf 'backup materializer validator image is not non-root: %s\n' "${actual_user}" >&2
  exit 1
}
[[ "${actual_entrypoint}" == "${expected_entrypoint}" ]] || {
  printf 'backup materializer validator entrypoint drifted: %s\n' "${actual_entrypoint}" >&2
  exit 1
}
[[ "${exposed_ports}" == 'null' ]] || {
  printf 'backup materializer validator image declares a network port: %s\n' "${exposed_ports}" >&2
  exit 1
}

readonly container="fugue-backup-materializer-validator-probe-${RANDOM}-$$"
cleanup() {
  docker rm --force "${container}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Invalid private capability inputs prove default-off construction does not
# read any projection or open an outbound connection before becoming live.
docker run --detach \
  --name "${container}" \
  --network none \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --pids-limit 32 \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=8m \
  --env FUGUE_BACKUP_MATERIALIZER_VALIDATOR_ENABLED=false \
  --env FUGUE_BACKUP_MATERIALIZER_VALIDATOR_CELL_KEY=private-invalid-cell \
  --env FUGUE_BACKUP_MATERIALIZER_VALIDATOR_RUN_ID=private-invalid-run \
  --env FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT_API_BASE_URL=private-invalid-url \
  --env FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT_PROJECTION_ROOT=/private/missing/input \
  --env FUGUE_BACKUP_MATERIALIZER_VALIDATOR_KUBERNETES_API_URL=private-invalid-kubernetes-url \
  --env FUGUE_BACKUP_MATERIALIZER_VALIDATOR_READER_PROJECTION_ROOT=/private/missing/reader \
  --env FUGUE_BACKUP_MATERIALIZER_VALIDATOR_DRY_RUN_PROJECTION_ROOT=/private/missing/dry-run \
  --env FUGUE_BACKUP_MATERIALIZER_VALIDATOR_RECONCILE_INTERVAL=private-invalid-interval \
  "${image_ref}" >/dev/null

healthy=false
for _ in $(seq 1 50); do
  if docker exec "${container}" "${binary}" probe health >/dev/null 2>&1; then
    healthy=true
    break
  fi
  sleep 0.1
done
if [[ "${healthy}" != 'true' ]]; then
  docker logs "${container}" >&2 || true
  printf 'disabled backup materializer validator did not become live on loopback\n' >&2
  exit 1
fi
if docker exec "${container}" "${binary}" probe ready >/dev/null 2>&1; then
  printf 'disabled backup materializer validator unexpectedly became ready\n' >&2
  exit 1
fi
if [[ -n "$(docker port "${container}" 2>/dev/null)" ]]; then
  printf 'backup materializer validator unexpectedly published a container port\n' >&2
  exit 1
fi
if docker exec "${container}" /bin/sh -c true >/dev/null 2>&1; then
  printf 'backup materializer validator scratch image unexpectedly contains a shell\n' >&2
  exit 1
fi
if docker logs "${container}" 2>&1 | grep -Eqi 'bearer|credential|token=|private-invalid|/private/missing'; then
  docker logs "${container}" >&2 || true
  printf 'backup materializer validator logs exposed capability-shaped material\n' >&2
  exit 1
fi

docker stop --time 5 "${container}" >/dev/null
readonly exit_code="$(docker inspect --format '{{.State.ExitCode}}' "${container}")"
if [[ "${exit_code}" != '0' ]]; then
  docker logs "${container}" >&2 || true
  printf 'backup materializer validator did not complete graceful shutdown: exit=%s\n' "${exit_code}" >&2
  exit 1
fi
printf '[backup-materializer-validator-image] scratch, non-root, read-only, networkless, default-disabled probe passed\n'
