#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

trim_field() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "${value}"
}

require_env() {
  local name="$1"
  if [[ -z "$(trim_field "${!name:-}")" ]]; then
    printf 'required environment variable %s is missing\n' "${name}" >&2
    exit 1
  fi
}

require_git_revision() {
  local value
  value="$(trim_field "${FUGUE_IMAGE_TAG:-}")"
  if [[ ! "${value}" =~ ^[0-9a-f]{40}$ ]]; then
    printf 'FUGUE_IMAGE_TAG must be a complete lowercase 40-character Git revision\n' >&2
    exit 1
  fi
  if [[ "${value}" != "${FUGUE_IMAGE_TAG}" ]]; then
    printf 'FUGUE_IMAGE_TAG must not contain surrounding whitespace\n' >&2
    exit 1
  fi
}

image_repository_var() {
  case "$1" in
    api) printf 'FUGUE_API_IMAGE_REPOSITORY' ;;
    controller) printf 'FUGUE_CONTROLLER_IMAGE_REPOSITORY' ;;
    drain_agent) printf 'FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY' ;;
    telemetry_agent) printf 'FUGUE_TELEMETRY_AGENT_IMAGE_REPOSITORY' ;;
    image_cache) printf 'FUGUE_IMAGE_CACHE_IMAGE_REPOSITORY' ;;
    edge) printf 'FUGUE_EDGE_IMAGE_REPOSITORY' ;;
    app_ssh) printf 'FUGUE_APP_SSH_IMAGE_REPOSITORY' ;;
    *) return 1 ;;
  esac
}

image_dockerfile() {
  case "$1" in
    api) printf 'Dockerfile.api' ;;
    controller) printf 'Dockerfile.controller' ;;
    drain_agent) printf 'Dockerfile.drain-agent' ;;
    telemetry_agent) printf 'Dockerfile.telemetry-agent' ;;
    image_cache) printf 'Dockerfile.image-cache' ;;
    edge) printf 'Dockerfile.edge' ;;
    app_ssh) printf 'Dockerfile.app-ssh' ;;
    *) return 1 ;;
  esac
}

image_digest_from_metadata() {
  local metadata_file="$1"
  python3 - "${metadata_file}" <<'PY'
import json
from pathlib import Path
import re
import sys

path = Path(sys.argv[1])
try:
    metadata = json.loads(path.read_text())
except (OSError, json.JSONDecodeError) as exc:
    print(f"invalid build metadata {path}: {exc}", file=sys.stderr)
    raise SystemExit(1)

if not isinstance(metadata, dict):
    print(f"build metadata {path} must be a JSON object", file=sys.stderr)
    raise SystemExit(1)

digest = metadata.get("containerimage.digest")
if not isinstance(digest, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
    print(f"build metadata {path} has no complete containerimage.digest", file=sys.stderr)
    raise SystemExit(1)

print(digest)
PY
}

publish_outputs() {
  local staged_output="$1"
  if [[ -z "${GITHUB_OUTPUT:-}" ]]; then
    cat "${staged_output}"
    return
  fi

  python3 - "${GITHUB_OUTPUT}" "${staged_output}" <<'PY'
import os
from pathlib import Path
import stat
import sys
import tempfile

destination = Path(sys.argv[1]).absolute()
payload_path = Path(sys.argv[2])
payload = payload_path.read_bytes()
if not payload or not payload.endswith(b"\n"):
    raise SystemExit("staged GitHub output must be a non-empty newline-terminated file")

previous = b""
mode = 0o600
if destination.exists():
    destination_stat = destination.stat()
    if not stat.S_ISREG(destination_stat.st_mode):
        raise SystemExit("GITHUB_OUTPUT must be a regular file")
    previous = destination.read_bytes()
    mode = stat.S_IMODE(destination_stat.st_mode)

destination.parent.mkdir(parents=False, exist_ok=True)
fd, temporary_name = tempfile.mkstemp(prefix=".fugue-build-output.", dir=str(destination.parent))
try:
    os.fchmod(fd, mode)
    with os.fdopen(fd, "wb") as handle:
        handle.write(previous)
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary_name, destination)
except BaseException:
    try:
        os.close(fd)
    except OSError:
        pass
    try:
        os.unlink(temporary_name)
    except FileNotFoundError:
        pass
    raise
PY
}

targets="$(trim_field "${FUGUE_CONTROL_PLANE_IMAGE_TARGETS:-}")"
if [[ -z "${targets}" ]]; then
  printf 'no control-plane images selected for build\n'
  exit 0
fi

require_env FUGUE_IMAGE_TAG
require_git_revision

metadata_root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
metadata_dir="$(mktemp -d "${metadata_root%/}/fugue-build-metadata.XXXXXX")"
pids=()
cleanup() {
  rm -rf "${metadata_dir}"
}
terminate_builds() {
  trap '' INT TERM
  local status="$1"
  local pid
  local running_pids
  running_pids="$(jobs -pr)"
  for pid in ${running_pids}; do
    kill -TERM "${pid}" 2>/dev/null || true
  done
  for pid in ${running_pids}; do
    wait "${pid}" 2>/dev/null || true
  done
  for pid in "${pids[@]-}"; do
    if [[ -n "${pid}" ]]; then
      wait "${pid}" 2>/dev/null || true
    fi
  done
  exit "${status}"
}
trap cleanup EXIT
trap 'terminate_builds 130' INT
trap 'terminate_builds 143' TERM

names=()
repositories=()
dockerfiles=()
seen_targets=' '
for target in ${targets}; do
  repo_var="$(image_repository_var "${target}")" || {
    printf 'unknown image build target: %s\n' "${target}" >&2
    exit 1
  }
  case "${seen_targets}" in
    *" ${target} "*)
      printf 'duplicate image build target: %s\n' "${target}" >&2
      exit 1
      ;;
  esac
  require_env "${repo_var}"
  seen_targets="${seen_targets}${target} "
  names+=("${target}")
  repositories+=("${!repo_var}")
  dockerfiles+=("$(image_dockerfile "${target}")")
done

for index in "${!names[@]}"; do
  target="${names[${index}]}"
  repository="${repositories[${index}]}"
  dockerfile="${dockerfiles[${index}]}"
  tag="${repository}:${FUGUE_IMAGE_TAG}"
  cache_scope="fugue-control-plane-${target}"
  metadata_file="${metadata_dir}/${target}.json"
  printf 'building %s -> %s\n' "${target}" "${tag}"
  (
    cd "${REPO_ROOT}"
    exec docker buildx build \
      --platform linux/amd64 \
      --file "${dockerfile}" \
      --tag "${tag}" \
      --metadata-file "${metadata_file}" \
      --label "org.opencontainers.image.revision=${FUGUE_IMAGE_TAG}" \
      --cache-from "type=gha,scope=${cache_scope}" \
      --cache-to "type=gha,scope=${cache_scope},mode=max,ignore-error=true" \
      --push \
      .
  ) &
  pids+=("$!")
done

rc=0
for index in "${!pids[@]}"; do
  if ! wait "${pids[${index}]}"; then
    printf 'image build failed: %s\n' "${names[${index}]}" >&2
    rc=1
  fi
  pids[${index}]=''
done
pids=()

if [[ "${rc}" -ne 0 ]]; then
  exit "${rc}"
fi

digests=()
for target in "${names[@]}"; do
  metadata_file="${metadata_dir}/${target}.json"
  if ! digest="$(image_digest_from_metadata "${metadata_file}")"; then
    printf 'image digest metadata verification failed: %s\n' "${target}" >&2
    rc=1
    continue
  fi
  digests+=("${digest}")
done

if [[ "${rc}" -ne 0 ]]; then
  exit "${rc}"
fi

verification_files=()
for index in "${!names[@]}"; do
  target="${names[${index}]}"
  repository="${repositories[${index}]}"
  digest="${digests[${index}]}"
  verification_file="${metadata_dir}/${target}.verified.json"
  printf 'verifying %s -> %s@%s\n' "${target}" "${repository}" "${digest}"
  if ! python3 "${REPO_ROOT}/scripts/verify_registry_image.py" \
    --image "${repository}@${digest}" \
    --platform linux/amd64 \
    --expected-revision "${FUGUE_IMAGE_TAG}" \
    >"${verification_file}"; then
    printf 'registry image verification failed: %s\n' "${target}" >&2
    exit 1
  fi
  verification_files+=("${verification_file}")
done

verified_artifacts_file="${metadata_dir}/verified-image-artifacts.json"
verified_artifacts_digest_file="${metadata_dir}/verified-image-artifacts.digest"
artifact_args=()
for index in "${!names[@]}"; do
  artifact_args+=(
    "${names[${index}]}"
    "${repositories[${index}]}"
    "${digests[${index}]}"
    "${verification_files[${index}]}"
  )
done

python3 - \
  "${FUGUE_IMAGE_TAG}" \
  "${verified_artifacts_file}" \
  "${verified_artifacts_digest_file}" \
  "${artifact_args[@]}" <<'PY'
import hashlib
import json
from pathlib import Path
import re
import sys

DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
VERIFICATION = "registry_manifest_config_and_layer_get"
EXPECTED_FIELDS = {
    "blob_count",
    "config_digest",
    "image",
    "index_digest",
    "layer_get_probe_count",
    "manifest_digest",
    "oci_revision",
    "platform",
    "request_count",
    "total_layer_bytes",
    "verification",
}


def reject_duplicate_keys(pairs):
    document = {}
    for key, value in pairs:
        if key in document:
            raise ValueError(f"duplicate verifier JSON key: {key}")
        document[key] = value
    return document


def load_verification(path):
    raw = Path(path).read_bytes()
    if not raw or len(raw) > 1024 * 1024:
        raise ValueError(f"verifier output {path} must contain between 1 byte and 1 MiB")
    return json.loads(
        raw,
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=lambda value: (_ for _ in ()).throw(
            ValueError(f"non-finite verifier JSON number: {value}")
        ),
    )


revision, artifacts_path, digest_path, *arguments = sys.argv[1:]
if re.fullmatch(r"[0-9a-f]{40}", revision) is None:
    raise SystemExit("image revision must be a complete lowercase 40-character Git revision")
if not arguments or len(arguments) % 4 != 0:
    raise SystemExit("verified artifact inputs must be non-empty component/repository/digest/file groups")

artifacts = []
components = set()
for offset in range(0, len(arguments), 4):
    component, repository, top_digest, verification_path = arguments[offset : offset + 4]
    if component in components:
        raise SystemExit(f"duplicate verified image component: {component}")
    components.add(component)
    if DIGEST_RE.fullmatch(top_digest) is None:
        raise SystemExit(f"invalid top digest for {component}")
    immutable_ref = f"{repository}@{top_digest}"
    try:
        result = load_verification(verification_path)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise SystemExit(f"invalid verifier output for {component}: {exc}") from exc
    if not isinstance(result, dict) or set(result) != EXPECTED_FIELDS:
        raise SystemExit(f"verifier output for {component} has an unexpected schema")
    for field in (
        "blob_count",
        "layer_get_probe_count",
        "request_count",
        "total_layer_bytes",
    ):
        value = result[field]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise SystemExit(f"verifier output {component}.{field} must be a non-negative integer")
    if result["image"] != immutable_ref:
        raise SystemExit(f"verifier output image does not match the requested ref for {component}")
    if result["platform"] != "linux/amd64":
        raise SystemExit(f"verifier output platform does not match linux/amd64 for {component}")
    if result["oci_revision"] != revision:
        raise SystemExit(f"verifier output revision does not match the build revision for {component}")
    if result["verification"] != VERIFICATION:
        raise SystemExit(f"verifier output method is invalid for {component}")
    manifest_digest = result["manifest_digest"]
    config_digest = result["config_digest"]
    index_digest = result["index_digest"]
    if not isinstance(manifest_digest, str) or DIGEST_RE.fullmatch(manifest_digest) is None:
        raise SystemExit(f"verifier output manifest digest is invalid for {component}")
    if not isinstance(config_digest, str) or DIGEST_RE.fullmatch(config_digest) is None:
        raise SystemExit(f"verifier output config digest is invalid for {component}")
    if not isinstance(index_digest, str):
        raise SystemExit(f"verifier output index digest must be a string for {component}")
    if index_digest:
        if DIGEST_RE.fullmatch(index_digest) is None or index_digest != top_digest:
            raise SystemExit(f"verifier output index digest does not match the top digest for {component}")
    elif manifest_digest != top_digest:
        raise SystemExit(f"verifier output manifest digest does not match the top digest for {component}")
    artifacts.append(
        {
            "component": component,
            "config_digest": config_digest,
            "immutable_ref": immutable_ref,
            "oci_revision": revision,
            "platform_manifest_digest": manifest_digest,
            "repository": repository,
            "source_tag": revision,
            "top_digest": top_digest,
            "verification": VERIFICATION,
        }
    )

artifacts.sort(key=lambda artifact: artifact["component"])
canonical = json.dumps(artifacts, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
Path(artifacts_path).write_bytes(canonical)
Path(digest_path).write_text("sha256:" + hashlib.sha256(canonical).hexdigest(), encoding="ascii")
PY

staged_output="${metadata_dir}/outputs"
: >"${staged_output}"
for index in "${!names[@]}"; do
  printf '%s=%s\n' "${names[${index}]}_image_digest" "${digests[${index}]}" >>"${staged_output}"
done
printf 'verified_image_artifacts_json=%s\n' "$(cat "${verified_artifacts_file}")" >>"${staged_output}"
printf 'verified_image_artifacts_digest=%s\n' "$(cat "${verified_artifacts_digest_file}")" >>"${staged_output}"
trap '' INT TERM
publish_outputs "${staged_output}"
