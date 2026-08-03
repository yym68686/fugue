#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/fugue-historical-controller-build-only-test.XXXXXX")"
trap 'rm -rf "${FIXTURE_ROOT}"' EXIT

SOURCE_ROOT="${FIXTURE_ROOT}/source"
RUNNER_ROOT="${FIXTURE_ROOT}/runner"
SHIM_ROOT="${FIXTURE_ROOT}/shim"
REAL_BIN_ROOT="${FIXTURE_ROOT}/real-bin"
mkdir -p "${SOURCE_ROOT}/scripts" "${RUNNER_ROOT}" "${SHIM_ROOT}" "${REAL_BIN_ROOT}"
cp "${REPO_ROOT}/scripts/build_control_plane_images.sh" "${SOURCE_ROOT}/scripts/build_control_plane_images.sh"
chmod 0700 "${SOURCE_ROOT}/scripts/build_control_plane_images.sh"
printf 'FROM scratch\n' >"${SOURCE_ROOT}/Dockerfile.controller"

cat >"${SOURCE_ROOT}/scripts/verify_registry_image.py" <<'PY'
import json
import sys

arguments = sys.argv[1:]
if arguments != [
    "--image",
    "ghcr.io/yym68686/fugue-controller@sha256:" + "a" * 64,
    "--platform",
    "linux/amd64",
    "--expected-revision",
    "58fc2e560064214e3f329765c9ec7839ee513c27",
]:
    raise SystemExit(f"unexpected verifier arguments: {arguments!r}")
print(
    json.dumps(
        {
            "blob_count": 3,
            "config_digest": "sha256:" + "c" * 64,
            "image": arguments[1],
            "index_digest": "sha256:" + "a" * 64,
            "layer_get_probe_count": 2,
            "manifest_digest": "sha256:" + "b" * 64,
            "oci_revision": arguments[5],
            "platform": arguments[3],
            "request_count": 7,
            "total_layer_bytes": 1024,
            "verification": "registry_manifest_config_and_layer_get",
        },
        sort_keys=True,
        separators=(",", ":"),
    )
)
PY

cat >"${REAL_BIN_ROOT}/docker-real" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
[[ "$#" -ge 2 && "$1" == 'buildx' && "$2" == 'build' ]]
printf '%q\n' "$@" >"${FUGUE_MOCK_DOCKER_CALL}"
metadata_file=''
push_count=0
cache_to_count=0
arguments=("$@")
for ((index = 0; index < ${#arguments[@]}; index++)); do
  case "${arguments[index]}" in
    --metadata-file)
      metadata_file="${arguments[index + 1]}"
      ;;
    --push)
      push_count=$((push_count + 1))
      ;;
    --cache-to|--cache-to=*)
      cache_to_count=$((cache_to_count + 1))
      ;;
  esac
done
[[ -n "${metadata_file}" && "${push_count}" == '1' && "${cache_to_count}" == '0' ]]
[[ " ${arguments[*]} " == *' --platform linux/amd64 '* ]]
[[ " ${arguments[*]} " == *' --file Dockerfile.controller '* ]]
[[ " ${arguments[*]} " == *' --tag ghcr.io/yym68686/fugue-controller:58fc2e560064214e3f329765c9ec7839ee513c27 '* ]]
[[ " ${arguments[*]} " == *' --label org.opencontainers.image.revision=58fc2e560064214e3f329765c9ec7839ee513c27 '* ]]
printf '{"containerimage.digest":"sha256:%s"}\n' "$(printf 'a%.0s' {1..64})" >"${metadata_file}"
SH
chmod 0700 "${REAL_BIN_ROOT}/docker-real"

cat >"${SHIM_ROOT}/docker" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
[[ "$#" -ge 2 && "$1" == 'buildx' && "$2" == 'build' ]]
forwarded=("$1" "$2")
shift 2
removed_cache_export=0
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --cache-to)
      [[ "$#" -ge 2 ]]
      [[ "$2" == 'type=gha,scope=fugue-control-plane-controller,mode=max,ignore-error=true' ]]
      removed_cache_export=$((removed_cache_export + 1))
      shift 2
      ;;
    --cache-to=*)
      [[ "$1" == '--cache-to=type=gha,scope=fugue-control-plane-controller,mode=max,ignore-error=true' ]]
      removed_cache_export=$((removed_cache_export + 1))
      shift
      ;;
    *)
      forwarded+=("$1")
      shift
      ;;
  esac
done
[[ "${removed_cache_export}" == '1' ]]
for argument in "${forwarded[@]}"; do
  [[ "${argument}" != --cache-to && "${argument}" != --cache-to=* ]]
done
{
  printf 'cache_export_removed=%s\n' "${removed_cache_export}"
  printf 'forwarded_argument=%q\n' "${forwarded[@]}"
} >"${FUGUE_DOCKER_SHIM_AUDIT}"
printf 'registry_write_entered\n' >"${FUGUE_DOCKER_WRITE_STATE}"
set +e
"${FUGUE_REAL_DOCKER}" "${forwarded[@]}"
status=$?
set -e
if [[ "${status}" == '0' ]]; then
  printf 'registry_write_completed\n' >"${FUGUE_DOCKER_WRITE_STATE}"
fi
exit "${status}"
SH
chmod 0700 "${SHIM_ROOT}/docker"

OUTPUT_FILE="${FIXTURE_ROOT}/github-output"
MOCK_CALL_FILE="${FIXTURE_ROOT}/docker-call"
SHIM_AUDIT_FILE="${FIXTURE_ROOT}/docker-shim-audit"
WRITE_STATE_FILE="${FIXTURE_ROOT}/registry-write-state"
printf 'sentinel=preserved\n' >"${OUTPUT_FILE}"

PATH="${SHIM_ROOT}:${PATH}" \
  RUNNER_TEMP="${RUNNER_ROOT}" \
  GITHUB_OUTPUT="${OUTPUT_FILE}" \
  FUGUE_REAL_DOCKER="${REAL_BIN_ROOT}/docker-real" \
  FUGUE_MOCK_DOCKER_CALL="${MOCK_CALL_FILE}" \
  FUGUE_DOCKER_SHIM_AUDIT="${SHIM_AUDIT_FILE}" \
  FUGUE_DOCKER_WRITE_STATE="${WRITE_STATE_FILE}" \
  FUGUE_CONTROL_PLANE_IMAGE_TARGETS=controller \
  FUGUE_IMAGE_TAG=58fc2e560064214e3f329765c9ec7839ee513c27 \
  FUGUE_CONTROLLER_IMAGE_REPOSITORY=ghcr.io/yym68686/fugue-controller \
  "${SOURCE_ROOT}/scripts/build_control_plane_images.sh" >"${FIXTURE_ROOT}/build.log"

[[ "$(cat "${WRITE_STATE_FILE}")" == 'registry_write_completed' ]]
[[ "$(grep -c '^cache_export_removed=1$' "${SHIM_AUDIT_FILE}")" == '1' ]]
[[ -z "$(grep -E -- '--cache-to($|=)' "${MOCK_CALL_FILE}" || true)" ]]
[[ "$(grep -c '^--push$' "${MOCK_CALL_FILE}")" == '1' ]]
[[ "$(grep -c '^sentinel=preserved$' "${OUTPUT_FILE}")" == '1' ]]
[[ "$(grep -c '^controller_image_digest=sha256:a\{64\}$' "${OUTPUT_FILE}")" == '1' ]]
[[ -z "$(grep '_image_digest=' "${OUTPUT_FILE}" | grep -v '^controller_image_digest=' || true)" ]]

python3 - "${OUTPUT_FILE}" <<'PY'
import hashlib
import json
from pathlib import Path
import sys

values = {}
for line in Path(sys.argv[1]).read_text(encoding="utf-8").splitlines():
    key, value = line.split("=", 1)
    values[key] = value
raw = values["verified_image_artifacts_json"]
artifacts = json.loads(raw)
canonical = json.dumps(artifacts, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
if raw != canonical or len(artifacts) != 1:
    raise SystemExit("controller-only mock receipt is not canonical singleton JSON")
artifact = artifacts[0]
target = "58fc2e560064214e3f329765c9ec7839ee513c27"
repository = "ghcr.io/yym68686/fugue-controller"
expected = {
    "component": "controller",
    "config_digest": "sha256:" + "c" * 64,
    "immutable_ref": repository + "@sha256:" + "a" * 64,
    "oci_revision": target,
    "platform_manifest_digest": "sha256:" + "b" * 64,
    "repository": repository,
    "source_tag": target,
    "top_digest": "sha256:" + "a" * 64,
    "verification": "registry_manifest_config_and_layer_get",
}
if artifact != expected:
    raise SystemExit(f"controller-only mock receipt drifted: {artifact!r}")
provenance = "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()
if values["verified_image_artifacts_digest"] != provenance:
    raise SystemExit("controller-only mock provenance digest drifted")
PY

printf 'historical Controller build-only mock test passed\n'
