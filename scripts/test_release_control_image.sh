#!/usr/bin/env bash
set -euo pipefail

readonly image_ref="${1:-}"
if [[ -z "${image_ref}" ]]; then
  printf 'usage: %s IMAGE_REF\n' "$0" >&2
  exit 2
fi
command -v docker >/dev/null 2>&1 || { printf 'docker is required\n' >&2; exit 2; }
command -v curl >/dev/null 2>&1 || { printf 'curl is required\n' >&2; exit 2; }
command -v jq >/dev/null 2>&1 || { printf 'jq is required\n' >&2; exit 2; }

readonly expected_entrypoint='["/usr/local/bin/fugue-release-control"]'
readonly actual_user="$(docker image inspect --format '{{.Config.User}}' "${image_ref}")"
readonly actual_entrypoint="$(docker image inspect --format '{{json .Config.Entrypoint}}' "${image_ref}")"
[[ "${actual_user}" == '65532:65532' ]] || {
  printf 'release-control image is not non-root: %s\n' "${actual_user}" >&2
  exit 1
}
[[ "${actual_entrypoint}" == "${expected_entrypoint}" ]] || {
  printf 'release-control image entrypoint drifted: %s\n' "${actual_entrypoint}" >&2
  exit 1
}

container_id=""
cleanup() {
  if [[ -n "${container_id}" ]]; then
    docker rm --force "${container_id}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

container_id="$(docker run --detach --publish 127.0.0.1::8091 \
  --env FUGUE_RELEASE_CONTROL_BIND_ADDR=0.0.0.0:8091 "${image_ref}")"
readonly host_port="$(docker port "${container_id}" 8091/tcp | sed -n 's/.*:\([0-9][0-9]*\)$/\1/p')"
[[ "${host_port}" =~ ^[1-9][0-9]*$ ]] || {
  printf 'could not determine release-control probe port\n' >&2
  exit 1
}
readonly base_url="http://127.0.0.1:${host_port}"

health_body=''
for _ in $(seq 1 30); do
  if health_body="$(curl --silent --show-error --max-time 2 "${base_url}/healthz" 2>/dev/null)"; then
    break
  fi
  sleep 1
done
[[ -n "${health_body}" ]] || { printf 'release-control image never became live\n' >&2; exit 1; }
printf '%s' "${health_body}" | jq -e '
  .apiVersion == "release-control.fugue.dev/v1" and
  .kind == "ComponentPlanServiceStatus" and
  .mode == "disabled" and
  .ready == false and
  .observationOnly == true and
  .productionMutationAllowed == false and
  .attemptCount == 0
' >/dev/null

readonly ready_code="$(curl --silent --output /dev/null --write-out '%{http_code}' --max-time 2 "${base_url}/readyz")"
[[ "${ready_code}" == '503' ]] || {
  printf 'disabled release-control image readiness code drifted: %s\n' "${ready_code}" >&2
  exit 1
}
printf '[release-control-image] non-root disabled image probe passed\n'
