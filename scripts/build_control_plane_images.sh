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

resolve_image_revision() {
  local value="${FUGUE_IMAGE_REVISION:-${FUGUE_IMAGE_TAG:-}}"
  local trimmed
  trimmed="$(trim_field "${value}")"
  if [[ ! "${trimmed}" =~ ^[0-9a-f]{40}$ ]]; then
    printf 'FUGUE_IMAGE_REVISION must be a complete lowercase 40-character Git revision\n' >&2
    return 1
  fi
  if [[ "${trimmed}" != "${value}" ]]; then
    printf 'FUGUE_IMAGE_REVISION must not contain surrounding whitespace\n' >&2
    return 1
  fi
  printf '%s' "${trimmed}"
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

destination = Path(sys.argv[1])
if not destination.is_absolute():
    raise SystemExit("GITHUB_OUTPUT must be an absolute path")
payload_path = Path(sys.argv[2])
payload = payload_path.read_bytes()
if not payload or not payload.endswith(b"\n"):
    raise SystemExit("staged GitHub output must be a non-empty newline-terminated file")

previous = b""
mode = 0o600
destination_existed = False
destination_identity = None

def file_identity(value):
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )

try:
    destination_stat = destination.lstat()
except FileNotFoundError:
    pass
else:
    destination_existed = True
    if (
        not stat.S_ISREG(destination_stat.st_mode)
        or destination_stat.st_uid != os.geteuid()
        or destination_stat.st_nlink != 1
    ):
        raise SystemExit(
            "GITHUB_OUTPUT must be a current-user regular file with one link"
        )
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    existing_fd = os.open(destination, flags)
    try:
        opened_stat = os.fstat(existing_fd)
        if file_identity(opened_stat) != file_identity(destination_stat):
            raise SystemExit("GITHUB_OUTPUT changed before it was opened")
        with os.fdopen(existing_fd, "rb", closefd=False) as existing:
            previous = existing.read()
        final_stat = os.fstat(existing_fd)
        if file_identity(final_stat) != file_identity(opened_stat):
            raise SystemExit("GITHUB_OUTPUT changed while it was read")
    finally:
        os.close(existing_fd)
    destination_identity = file_identity(final_stat)
    mode = stat.S_IMODE(final_stat.st_mode)
    if previous and not previous.endswith(b"\n"):
        raise SystemExit("existing GITHUB_OUTPUT must be newline-terminated")

destination.parent.mkdir(parents=False, exist_ok=True)
fd, temporary_name = tempfile.mkstemp(prefix=".fugue-build-output.", dir=str(destination.parent))
try:
    os.fchmod(fd, mode)
    with os.fdopen(fd, "wb") as handle:
        handle.write(previous)
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        current_stat = destination.lstat()
    except FileNotFoundError:
        if destination_existed:
            raise SystemExit("GITHUB_OUTPUT disappeared before publication")
    else:
        if not destination_existed or file_identity(current_stat) != destination_identity:
            raise SystemExit("GITHUB_OUTPUT changed before publication")
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

reuse_authorized_image_cache_artifact() {
  local authorization_file="${FUGUE_CONTROL_PLANE_IMAGE_REUSE_AUTHORIZATION_FILE:-}"
  local metadata_root=""
  local metadata_dir=""
  local verified_artifacts_file=""
  local verified_artifacts_digest_file=""
  local top_digest_file=""
  local verification_file=""
  local staged_output=""
  local top_digest=""
  local cleanup_command=""

  [[ "$(trim_field "${FUGUE_CONTROL_PLANE_IMAGE_TARGETS:-}")" == "image_cache" ]] || {
    printf 'convergence artifact reuse requires the exact image_cache target\n' >&2
    return 1
  }
  require_git_revision
  require_env FUGUE_IMAGE_CACHE_IMAGE_REPOSITORY
  require_env FUGUE_IMAGE_CACHE_IMAGE_BASE_REF
  require_env FUGUE_CONVERGENCE_SOURCE_RUN_ID
  require_env GITHUB_REPOSITORY
  require_env GITHUB_RUN_ID
  require_env GITHUB_RUN_NUMBER
  [[ "${GITHUB_RUN_ATTEMPT:-}" == "1" ]] || {
    printf 'convergence artifact reuse requires run attempt 1\n' >&2
    return 1
  }
  [[ -f "${authorization_file}" && ! -L "${authorization_file}" ]] || {
    printf 'convergence artifact authorization must be a regular non-symlink file\n' >&2
    return 1
  }

  metadata_root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
  metadata_dir="$(mktemp -d "${metadata_root%/}/fugue-build-reuse-metadata.XXXXXX")"
  verified_artifacts_file="${metadata_dir}/verified-image-artifacts.json"
  verified_artifacts_digest_file="${metadata_dir}/verified-image-artifacts.digest"
  top_digest_file="${metadata_dir}/image-cache.digest"
  verification_file="${metadata_dir}/image-cache.verified.json"
  staged_output="${metadata_dir}/outputs"
  printf -v cleanup_command 'rm -rf -- %q' "${metadata_dir}"
  trap "${cleanup_command}" EXIT

  python3 - \
    "${authorization_file}" \
    "${GITHUB_REPOSITORY}" \
    "${FUGUE_CONVERGENCE_SOURCE_RUN_ID}" \
    "${GITHUB_RUN_ID}" \
    "${GITHUB_RUN_NUMBER}" \
    "${FUGUE_IMAGE_TAG}" \
    "${FUGUE_IMAGE_CACHE_IMAGE_REPOSITORY}" \
    "${FUGUE_IMAGE_CACHE_IMAGE_BASE_REF}" \
    "${verified_artifacts_file}" \
    "${verified_artifacts_digest_file}" \
    "${top_digest_file}" <<'PY'
import datetime
import hashlib
import json
from pathlib import Path
import re
import sys

(
    authorization_path,
    expected_repository,
    expected_source_run_id,
    expected_successor_run_id,
    expected_successor_run_number,
    expected_revision,
    expected_image_repository,
    expected_image_base_ref,
    artifacts_path,
    artifacts_digest_path,
    top_digest_path,
) = sys.argv[1:]

SHA_RE = re.compile(r"[0-9a-f]{40}")
DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
AUTHORIZATION_KEYS = {
    "baseline_advanced",
    "cluster_mutation_attempted",
    "pending_activation_artifacts",
    "recorded_at",
    "repository",
    "schema_version",
    "source_head_sha",
    "source_image_cache_artifact",
    "source_image_cache_artifacts_digest",
    "source_image_cache_base_ref",
    "source_run_attempt",
    "source_run_id",
    "successor_run_id",
    "successor_run_number",
    "successor_status",
    "successor_target_sha",
    "workflow",
    "workflow_dispatch_attempted",
}
ARTIFACT_KEYS = {
    "component",
    "config_digest",
    "immutable_ref",
    "oci_revision",
    "platform_manifest_digest",
    "repository",
    "source_tag",
    "top_digest",
    "verification",
}

raw = Path(authorization_path).read_bytes()
if not raw or len(raw) > 128 * 1024:
    raise SystemExit("convergence authorization size is invalid")
value = json.loads(raw)
if type(value) is not dict or set(value) != AUTHORIZATION_KEYS:
    raise SystemExit("convergence authorization shape is invalid")
if value != json.loads(json.dumps(value, sort_keys=True, separators=(",", ":"))):
    raise SystemExit("convergence authorization value is not JSON-stable")
canonical_authorization = json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"
if raw != canonical_authorization:
    raise SystemExit("convergence authorization bytes are not canonical")
if value["schema_version"] != 2 or value["workflow"] != "deploy-control-plane":
    raise SystemExit("convergence authorization version is invalid")
if value["repository"] != expected_repository:
    raise SystemExit("convergence authorization repository is invalid")
if value["source_run_id"] != expected_source_run_id or value["source_run_attempt"] != 1:
    raise SystemExit("convergence authorization source run is invalid")
if value["successor_run_id"] != expected_successor_run_id:
    raise SystemExit("convergence authorization successor run is invalid")
if value["successor_run_number"] != int(expected_successor_run_number):
    raise SystemExit("convergence authorization successor number is invalid")
if value["successor_status"] not in {"queued", "in_progress", "waiting", "pending", "requested"}:
    raise SystemExit("convergence authorization successor status is invalid")
if value["source_head_sha"] != expected_revision or value["successor_target_sha"] != expected_revision:
    raise SystemExit("convergence authorization revision is invalid")
if value["pending_activation_artifacts"] != ["image_cache"]:
    raise SystemExit("convergence authorization pending artifact is invalid")
if value["baseline_advanced"] is not False or value["cluster_mutation_attempted"] is not False:
    raise SystemExit("convergence authorization mutation state is invalid")
if value["workflow_dispatch_attempted"] is not True:
    raise SystemExit("convergence authorization dispatch state is invalid")
timestamp = datetime.datetime.fromisoformat(value["recorded_at"])
if timestamp.tzinfo is None or timestamp.utcoffset() != datetime.timedelta(0):
    raise SystemExit("convergence authorization timestamp is invalid")
if SHA_RE.fullmatch(value["source_image_cache_base_ref"] or "") is None:
    raise SystemExit("convergence image-cache base ref is invalid")
if value["source_image_cache_base_ref"] != expected_image_base_ref:
    raise SystemExit("convergence image-cache base ref changed after authorization")

artifact = value["source_image_cache_artifact"]
if type(artifact) is not dict or set(artifact) != ARTIFACT_KEYS:
    raise SystemExit("convergence image-cache artifact shape is invalid")
if artifact["component"] != "image_cache":
    raise SystemExit("convergence artifact component is invalid")
if artifact["repository"] != expected_image_repository:
    raise SystemExit("convergence artifact repository is invalid")
if artifact["source_tag"] != expected_revision or artifact["oci_revision"] != expected_revision:
    raise SystemExit("convergence artifact revision is invalid")
for field in ("top_digest", "config_digest", "platform_manifest_digest"):
    if DIGEST_RE.fullmatch(artifact[field] or "") is None:
        raise SystemExit(f"convergence artifact {field} is invalid")
if artifact["immutable_ref"] != f'{expected_image_repository}@{artifact["top_digest"]}':
    raise SystemExit("convergence artifact immutable ref is invalid")
if artifact["verification"] != "registry_manifest_config_and_layer_get":
    raise SystemExit("convergence artifact verification is invalid")

canonical_artifacts = json.dumps([artifact], ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
artifacts_digest = "sha256:" + hashlib.sha256(canonical_artifacts).hexdigest()
if value["source_image_cache_artifacts_digest"] != artifacts_digest:
    raise SystemExit("convergence artifact provenance digest is invalid")
Path(artifacts_path).write_bytes(canonical_artifacts)
Path(artifacts_digest_path).write_text(artifacts_digest, encoding="ascii")
Path(top_digest_path).write_text(artifact["top_digest"], encoding="ascii")
PY

  top_digest="$(cat "${top_digest_file}")"
  printf 're-verifying authorized image_cache -> %s@%s\n' \
    "${FUGUE_IMAGE_CACHE_IMAGE_REPOSITORY}" "${top_digest}"
  python3 "${REPO_ROOT}/scripts/verify_registry_image.py" \
    --image "${FUGUE_IMAGE_CACHE_IMAGE_REPOSITORY}@${top_digest}" \
    --platform linux/amd64 \
    --expected-revision "${FUGUE_IMAGE_TAG}" >"${verification_file}"

  python3 - \
    "${verified_artifacts_file}" \
    "${verification_file}" \
    "${FUGUE_IMAGE_CACHE_IMAGE_REPOSITORY}" \
    "${top_digest}" \
    "${FUGUE_IMAGE_TAG}" <<'PY'
import json
from pathlib import Path
import re
import sys

artifacts_path, verification_path, repository, top_digest, revision = sys.argv[1:]
DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
artifacts = json.loads(Path(artifacts_path).read_text())
verification = json.loads(Path(verification_path).read_text())
if type(artifacts) is not list or len(artifacts) != 1:
    raise SystemExit("authorized image artifact inventory is invalid")
artifact = artifacts[0]
expected_verification_keys = {
    "blob_count", "config_digest", "image", "index_digest",
    "layer_get_probe_count", "manifest_digest", "oci_revision", "platform",
    "request_count", "total_layer_bytes", "verification",
}
if type(verification) is not dict or set(verification) != expected_verification_keys:
    raise SystemExit("registry re-verification shape is invalid")
for field in ("blob_count", "layer_get_probe_count", "request_count", "total_layer_bytes"):
    value = verification[field]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SystemExit(f"registry re-verification {field} is invalid")
if verification["image"] != f"{repository}@{top_digest}":
    raise SystemExit("registry re-verification image is invalid")
if verification["platform"] != "linux/amd64" or verification["oci_revision"] != revision:
    raise SystemExit("registry re-verification revision is invalid")
if verification["verification"] != "registry_manifest_config_and_layer_get":
    raise SystemExit("registry re-verification method is invalid")
if verification["config_digest"] != artifact["config_digest"]:
    raise SystemExit("registry config digest changed after authorization")
if verification["manifest_digest"] != artifact["platform_manifest_digest"]:
    raise SystemExit("registry platform manifest digest changed after authorization")
index_digest = verification["index_digest"]
if not isinstance(index_digest, str):
    raise SystemExit("registry index digest type is invalid")
if index_digest:
    if DIGEST_RE.fullmatch(index_digest) is None or index_digest != top_digest:
        raise SystemExit("registry index digest changed after authorization")
elif verification["manifest_digest"] != top_digest:
    raise SystemExit("registry manifest digest changed after authorization")
PY

  : >"${staged_output}"
  printf 'image_cache_image_digest=%s\n' "${top_digest}" >>"${staged_output}"
  printf 'verified_image_artifacts_json=%s\n' "$(cat "${verified_artifacts_file}")" >>"${staged_output}"
  printf 'verified_image_artifacts_digest=%s\n' "$(cat "${verified_artifacts_digest_file}")" >>"${staged_output}"
  trap '' INT TERM
  publish_outputs "${staged_output}"
}

targets="$(trim_field "${FUGUE_CONTROL_PLANE_IMAGE_TARGETS:-}")"
if [[ -n "${FUGUE_CONTROL_PLANE_IMAGE_REUSE_AUTHORIZATION_FILE:-}" ]]; then
	[[ -z "${FUGUE_CONTROL_PLANE_BUILD_RECEIPT_FILE:-}" ]] || {
		printf 'build receipt reuse cannot overlap convergence authorization reuse\n' >&2
		exit 1
	}
  reuse_authorized_image_cache_artifact
  exit 0
fi
if [[ -z "${targets}" ]]; then
  metadata_root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
  metadata_dir="$(mktemp -d "${metadata_root%/}/fugue-build-metadata.XXXXXX")"
  trap 'rm -rf "${metadata_dir}"' EXIT
  staged_output="${metadata_dir}/outputs"
  : >"${staged_output}"
  printf 'verified_image_artifacts_json=[]\n' >>"${staged_output}"
  printf 'verified_image_artifacts_digest=sha256:4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945\n' >>"${staged_output}"
  printf 'no control-plane images selected for build\n'
  trap '' INT TERM
  publish_outputs "${staged_output}"
  exit 0
fi

require_env FUGUE_IMAGE_TAG
require_git_revision
IMAGE_REVISION="$(resolve_image_revision)" || exit 1

build_source_root="${REPO_ROOT}"
explicit_build_source_root="$(trim_field "${FUGUE_CONTROL_PLANE_BUILD_SOURCE_ROOT:-}")"
if [[ -n "${explicit_build_source_root}" ]]; then
  [[ "${explicit_build_source_root}" == "${FUGUE_CONTROL_PLANE_BUILD_SOURCE_ROOT}" &&
    "${explicit_build_source_root}" == /* &&
    "${explicit_build_source_root}" != */ &&
    -d "${explicit_build_source_root}" &&
    ! -L "${explicit_build_source_root}" &&
    -d "${explicit_build_source_root}/.git" &&
    ! -L "${explicit_build_source_root}/.git" ]] || {
    printf 'explicit build source root must be an absolute canonical non-symlink Git worktree\n' >&2
    exit 1
  }
  canonical_build_source_root="$(cd "${explicit_build_source_root}" && pwd -P)" || {
    printf 'explicit build source root is not accessible\n' >&2
    exit 1
  }
  [[ "${canonical_build_source_root}" == "${explicit_build_source_root}" ]] || {
    printf 'explicit build source root contains a symlink or non-canonical path\n' >&2
    exit 1
  }
  [[ "$(git -C "${canonical_build_source_root}" rev-parse --show-toplevel)" == "${canonical_build_source_root}" ]] || {
    printf 'explicit build source root must be the exact Git worktree root\n' >&2
    exit 1
  }
  [[ "$(git -C "${canonical_build_source_root}" rev-parse --verify 'HEAD^{commit}')" == "${IMAGE_REVISION}" ]] || {
    printf 'explicit build source root HEAD does not match the image revision\n' >&2
    exit 1
  }
  source_head_tree="$(git -C "${canonical_build_source_root}" rev-parse --verify 'HEAD^{tree}')" || exit 1
  source_tag_tree="$(git -C "${canonical_build_source_root}" rev-parse --verify "${IMAGE_REVISION}^{tree}")" || exit 1
  [[ "${source_head_tree}" =~ ^[0-9a-f]{40}$ && "${source_head_tree}" == "${source_tag_tree}" ]] || {
    printf 'explicit build source root tree does not match the image revision\n' >&2
    exit 1
  }
  if git -C "${canonical_build_source_root}" symbolic-ref -q HEAD >/dev/null 2>&1; then
    printf 'explicit build source root must be detached at the desired revision\n' >&2
    exit 1
  fi
  [[ -z "$(git -C "${canonical_build_source_root}" status --porcelain=v1 --untracked-files=all)" ]] || {
    printf 'explicit build source root must be clean\n' >&2
    exit 1
  }
  if [[ -n "${GITHUB_WORKSPACE:-}" ]]; then
    canonical_workspace="$(cd "${GITHUB_WORKSPACE}" && pwd -P)" || exit 1
    case "${canonical_build_source_root}" in
      "${canonical_workspace}"/*) ;;
      *)
        printf 'explicit build source root must remain inside GITHUB_WORKSPACE\n' >&2
        exit 1
        ;;
    esac
  fi
  build_source_root="${canonical_build_source_root}"
fi

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
digests=()
historical_incident_reuse=false
canonical_receipt_reuse=false
canonical_receipt_file="$(trim_field "${FUGUE_CONTROL_PLANE_BUILD_RECEIPT_FILE:-}")"
immutable_tag_preflight="$(trim_field "${FUGUE_CONTROL_PLANE_IMMUTABLE_TAG_PREFLIGHT:-false}")"
[[ "${immutable_tag_preflight}" == true || "${immutable_tag_preflight}" == false ]] || {
  printf 'FUGUE_CONTROL_PLANE_IMMUTABLE_TAG_PREFLIGHT must be true or false\n' >&2
  exit 1
}
tag_preflight_reused=()
tag_preflight_files=()
registry_verifier="${FUGUE_REGISTRY_IMAGE_VERIFIER:-${REPO_ROOT}/scripts/verify_registry_image.py}"
[[ -f "${registry_verifier}" && ! -L "${registry_verifier}" ]] || {
  printf 'registry image verifier must be a regular non-symlink file\n' >&2
  exit 1
}
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
  tag_preflight_reused+=(false)
  tag_preflight_files+=("")
done

if [[ -n "${explicit_build_source_root}" ]]; then
  for dockerfile in "${dockerfiles[@]}"; do
    [[ -f "${build_source_root}/${dockerfile}" && ! -L "${build_source_root}/${dockerfile}" ]] || {
      printf 'explicit build source root is missing a regular Dockerfile: %s\n' "${dockerfile}" >&2
      exit 1
    }
  done
fi

if [[ "${immutable_tag_preflight}" == true ]]; then
  [[ -z "${canonical_receipt_file}" && -z "${FUGUE_CONTROL_PLANE_HISTORICAL_INCIDENT_BUILD_PLAN:-}" ]] || {
    printf 'immutable tag preflight cannot overlap another artifact reuse mode\n' >&2
    exit 1
  }
  [[ "${#names[@]}" -eq 1 ]] || {
    printf 'immutable tag preflight requires exactly one component target\n' >&2
    exit 1
  }
fi

if [[ -n "${canonical_receipt_file}" ]]; then
	[[ -f "${canonical_receipt_file}" && ! -L "${canonical_receipt_file}" ]] || {
		printf 'canonical build receipt must be a regular non-symlink file\n' >&2
		exit 1
	}
	receipt_digests_file="${metadata_dir}/receipt-digests"
	receipt_arguments=()
	for index in "${!names[@]}"; do
		receipt_arguments+=("${names[${index}]}" "${repositories[${index}]}")
	done
	python3 - "${canonical_receipt_file}" "${FUGUE_IMAGE_TAG}" "${IMAGE_REVISION}" "${receipt_digests_file}" "${receipt_arguments[@]}" <<'PY'
import json
import os
from pathlib import Path
import re
import stat
import sys

receipt_path, source_tag, revision, digest_path, *arguments = sys.argv[1:]
DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
SHA_RE = re.compile(r"[0-9a-f]{40}")
ARTIFACT_KEYS = {
    "component", "config_digest", "immutable_ref", "oci_revision",
    "platform_manifest_digest", "repository", "source_tag", "top_digest",
    "verification",
}

if SHA_RE.fullmatch(source_tag) is None or SHA_RE.fullmatch(revision) is None or not arguments or len(arguments) % 2:
    raise SystemExit("build-receipt-revision-or-targets-invalid")
path = Path(receipt_path)
info = path.lstat()
if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) & 0o022:
    raise SystemExit("build-receipt-permissions-invalid")
raw = path.read_bytes()
if not raw or len(raw) > 128 * 1024:
    raise SystemExit("build-receipt-size-invalid")

def reject_duplicate_keys(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate key {key}")
        value[key] = item
    return value

try:
    artifacts = json.loads(
        raw,
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
    )
except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
    raise SystemExit(f"build-receipt-json-invalid: {exc}") from exc
if type(artifacts) is not list:
    raise SystemExit("build-receipt-schema-type-invalid")
canonical = json.dumps(artifacts, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("ascii")
if raw != canonical:
    raise SystemExit("build-receipt-canonical-bytes-invalid")
expected = dict(zip(arguments[0::2], arguments[1::2]))
if len(expected) != len(arguments) // 2 or len(artifacts) != len(expected):
    raise SystemExit("build-receipt-artifact-set-missing")
if [item.get("component") for item in artifacts if type(item) is dict] != sorted(expected):
    raise SystemExit("build-receipt-component-order-invalid")
by_component = {}
for artifact in artifacts:
    if type(artifact) is not dict or set(artifact) != ARTIFACT_KEYS:
        raise SystemExit("build-receipt-artifact-shape-invalid")
    if any(type(value) is not str for value in artifact.values()):
        raise SystemExit("build-receipt-artifact-type-invalid")
    component = artifact["component"]
    if component in by_component or component not in expected:
        raise SystemExit("build-receipt-artifact-component-invalid")
    repository = expected[component]
    if artifact["repository"] != repository:
        raise SystemExit(f"build-receipt-repository-mismatch:{component}")
    if artifact["source_tag"] != source_tag or artifact["oci_revision"] != revision:
        raise SystemExit(f"build-receipt-stale:{component}")
    for field in ("top_digest", "config_digest", "platform_manifest_digest"):
        if DIGEST_RE.fullmatch(artifact[field]) is None:
            raise SystemExit(f"build-receipt-digest-invalid:{component}:{field}")
    if artifact["immutable_ref"] != f'{repository}@{artifact["top_digest"]}':
        raise SystemExit(f"build-receipt-immutable-ref-mismatch:{component}")
    if artifact["verification"] != "registry_manifest_config_and_layer_get":
        raise SystemExit(f"build-receipt-verification-mismatch:{component}")
    by_component[component] = artifact
if set(by_component) != set(expected):
    raise SystemExit("build-receipt-artifact-set-missing")
Path(digest_path).write_text(
    "".join(by_component[component]["top_digest"] + "\n" for component in arguments[0::2]),
    encoding="ascii",
)
PY
	digests=()
	while IFS= read -r digest; do
		digests+=("${digest}")
	done <"${receipt_digests_file}"
	[[ "${#digests[@]}" == "${#names[@]}" ]] || {
		printf 'build-receipt-artifact-set-missing\n' >&2
		exit 1
	}
	canonical_receipt_reuse=true
	printf 'reusing exact canonical build receipt; rebuild and push are disabled\n'
fi

if [[ "${immutable_tag_preflight}" == true ]]; then
  index=0
  repository="${repositories[0]}"
  tag_reference="${repository}:${FUGUE_IMAGE_TAG}"
  tag_verification_file="${metadata_dir}/immutable-tag-preflight.json"
  if ! python3 "${registry_verifier}" \
    --image "${tag_reference}" \
    --platform linux/amd64 \
    --expected-revision "${IMAGE_REVISION}" \
    --allow-missing-tag \
    --timeout-seconds 18 \
    --request-timeout-seconds 5 \
    --max-attempts 2 \
    --retry-delay-seconds 0.1 \
    >"${tag_verification_file}"; then
    printf 'immutable-tag-preflight-failed:%s\n' "${names[0]}" >&2
    exit 1
  fi
  tag_resolution="$(python3 - "${tag_verification_file}" "${tag_reference}" "${repository}" "${IMAGE_REVISION}" <<'PY'
import json
from pathlib import Path
import re
import sys

path, tag_reference, repository, revision = sys.argv[1:]
raw = Path(path).read_bytes()
if not raw or len(raw) > 1024 * 1024:
    raise SystemExit("immutable-tag-preflight-output-size-invalid")

def reject_duplicate_keys(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate key {key}")
        value[key] = item
    return value

try:
    value = json.loads(
        raw,
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)),
    )
except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
    raise SystemExit(f"immutable-tag-preflight-output-json-invalid:{exc}") from exc
canonical = json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("ascii") + b"\n"
if raw != canonical:
    raise SystemExit("immutable-tag-preflight-output-noncanonical")
if value == {"exists": False, "image": tag_reference}:
    print("missing")
    raise SystemExit(0)

expected_fields = {
    "blob_count", "config_digest", "image", "index_digest", "layer_get_probe_count",
    "manifest_digest", "oci_revision", "platform", "request_count", "total_layer_bytes",
    "verification",
}
digest_pattern = re.compile(r"sha256:[0-9a-f]{64}")
if type(value) is not dict or set(value) != expected_fields:
    raise SystemExit("immutable-tag-preflight-output-shape-invalid")
if value["oci_revision"] != revision or value["platform"] != "linux/amd64":
    raise SystemExit("immutable-tag-preflight-output-identity-mismatch")
if value["verification"] != "registry_manifest_config_and_layer_get":
    raise SystemExit("immutable-tag-preflight-output-verification-mismatch")
for field in ("blob_count", "layer_get_probe_count", "request_count", "total_layer_bytes"):
    item = value[field]
    if type(item) is not int or item < 0:
        raise SystemExit(f"immutable-tag-preflight-output-{field}-invalid")
if value["blob_count"] < 2 or value["layer_get_probe_count"] < 1:
    raise SystemExit("immutable-tag-preflight-output-layer-verification-missing")
for field in ("config_digest", "manifest_digest"):
    if type(value[field]) is not str or digest_pattern.fullmatch(value[field]) is None:
        raise SystemExit(f"immutable-tag-preflight-output-{field}-invalid")
index_digest = value["index_digest"]
if type(index_digest) is not str or (index_digest and digest_pattern.fullmatch(index_digest) is None):
    raise SystemExit("immutable-tag-preflight-output-index-digest-invalid")
top_digest = index_digest or value["manifest_digest"]
if value["image"] != f"{repository}@{top_digest}":
    raise SystemExit("immutable-tag-preflight-output-repository-or-digest-mismatch")
print(top_digest)
PY
  )" || {
    printf 'immutable-tag-preflight-shape-invalid:%s\n' "${names[0]}" >&2
    exit 1
  }
  if [[ "${tag_resolution}" == missing ]]; then
    printf 'immutable source tag is absent; one build and push is authorized: %s\n' "${tag_reference}"
  elif [[ "${tag_resolution}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    tag_preflight_reused[0]=true
    tag_preflight_files[0]="${tag_verification_file}"
    digests[0]="${tag_resolution}"
    printf 'reusing fresh-verified immutable source tag; build and push are disabled: %s@%s\n' \
      "${repository}" "${tag_resolution}"
  else
    printf 'immutable-tag-preflight-resolution-invalid:%s\n' "${names[0]}" >&2
    exit 1
  fi
fi

if [[ "${canonical_receipt_reuse}" == true && -n "${FUGUE_CONTROL_PLANE_HISTORICAL_INCIDENT_BUILD_PLAN:-}" ]]; then
	printf 'canonical receipt reuse cannot overlap historical incident reuse\n' >&2
	exit 1
elif [[ -n "${FUGUE_CONTROL_PLANE_HISTORICAL_INCIDENT_BUILD_PLAN:-}" ]]; then
  [[ "${targets}" == "api controller telemetry_agent edge" ]] || {
    printf 'historical incident reuse requires the exact four image targets\n' >&2
    exit 1
  }
  [[ "${FUGUE_IMAGE_TAG}" == "d1e7ed9cdedbaa09db9bd78b4e433b94c7357510" ]] || {
    printf 'historical incident reuse target is invalid\n' >&2
    exit 1
  }
  [[ -f "${FUGUE_CONTROL_PLANE_HISTORICAL_INCIDENT_BUILD_PLAN}" &&
    ! -L "${FUGUE_CONTROL_PLANE_HISTORICAL_INCIDENT_BUILD_PLAN}" ]] || {
    printf 'historical incident build plan must be a regular non-symlink file\n' >&2
    exit 1
  }
  python3 - "${FUGUE_CONTROL_PLANE_HISTORICAL_INCIDENT_BUILD_PLAN}" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
raw = path.read_bytes()
if not raw or len(raw) > 128 * 1024:
    raise SystemExit("historical incident build plan size is invalid")
value = json.loads(raw)
expected = {
    "apiVersion": "release-domain.fugue.dev/v2",
    "kind": "BuildArtifactPlan",
    "policy": "artifact-build-plan-v1",
    "baseCommit": "d2844418b0464a9bd32d3a147841e99b46140b39",
    "targetCommit": "d1e7ed9cdedbaa09db9bd78b4e433b94c7357510",
    "changedFilesDigest": "sha256:0fe56458a677c84469c471e4095159a924558b516db2bd350c27fd6a94051be4",
    "artifacts": [
        ("api", "b9cc03ded110b5e869dfbabcbdd73f107475a516", "410a1c75efe1fe9dd51dd83e32d535d548ab4471281223be7a8bc6b7297ae9d8", "fugue-api"),
        ("controller", "d2844418b0464a9bd32d3a147841e99b46140b39", "e636b35fe8718e1f20895c0a290924a0d48a6cb7d1072d741612df18483fa13d", "fugue-controller"),
        ("edge", "7bf04c33ec153bc63bf9a47d5e23c7026bafccf1", "d85c269268335f32f93d4253dc2331ffa4f48bc197e3005e2835a2dec1483f1b", "fugue-edge"),
        ("telemetry_agent", "d2844418b0464a9bd32d3a147841e99b46140b39", "3c79d82c3e094e3bf404df39e8c2a052d734dc7b54cac5e32c208e8a970a0eeb", "fugue-telemetry-agent"),
    ],
    "digest": "sha256:6f83fee8095f5fe0e824883f7226dbee51936cd90d5a564dcdac70d91ebb1ae5",
}
if type(value) is not dict or set(value) != set(expected):
    raise SystemExit("historical incident build plan shape is invalid")
for key in ("apiVersion", "kind", "policy", "baseCommit", "targetCommit", "changedFilesDigest", "digest"):
    if value[key] != expected[key]:
        raise SystemExit(f"historical incident build plan {key} is invalid")
artifacts = value["artifacts"]
if type(artifacts) is not list or len(artifacts) != 4:
    raise SystemExit("historical incident artifact count is invalid")
provenance = "sha256:fb14c704a84253ac4df59ba38d3cf83f91ae9e10b78b998ca95ffee2b555e495"
for artifact, (name, source_base, digest, repository) in zip(artifacts, expected["artifacts"]):
    if type(artifact) is not dict or set(artifact) != {
        "name", "sourceBaseCommit", "artifactDigest", "provenanceDigest", "publishedImageRef"
    }:
        raise SystemExit("historical incident artifact shape is invalid")
    digest = "sha256:" + digest
    image_repository = "ghcr.io/yym68686/" + repository
    if artifact != {
        "name": name,
        "sourceBaseCommit": source_base,
        "artifactDigest": digest,
        "provenanceDigest": provenance,
        "publishedImageRef": image_repository + "@" + digest,
    }:
        raise SystemExit(f"historical incident artifact {name} is invalid")
PY
  historical_incident_reuse=true
  digests=(
    "sha256:410a1c75efe1fe9dd51dd83e32d535d548ab4471281223be7a8bc6b7297ae9d8"
    "sha256:e636b35fe8718e1f20895c0a290924a0d48a6cb7d1072d741612df18483fa13d"
    "sha256:3c79d82c3e094e3bf404df39e8c2a052d734dc7b54cac5e32c208e8a970a0eeb"
    "sha256:d85c269268335f32f93d4253dc2331ffa4f48bc197e3005e2835a2dec1483f1b"
  )
  printf 'reusing exact historical incident artifacts; rebuild and push are disabled\n'
elif [[ "${canonical_receipt_reuse}" != true ]]; then
  for index in "${!names[@]}"; do
    if [[ "${tag_preflight_reused[${index}]}" == true ]]; then
      continue
    fi
    target="${names[${index}]}"
    repository="${repositories[${index}]}"
    dockerfile="${dockerfiles[${index}]}"
    tag="${repository}:${FUGUE_IMAGE_TAG}"
    cache_scope="fugue-control-plane-${target}"
    metadata_file="${metadata_dir}/${target}.json"
    printf 'building %s -> %s\n' "${target}" "${tag}"
    (
      cd "${build_source_root}"
      exec docker buildx build \
        --platform linux/amd64 \
        --file "${dockerfile}" \
        --tag "${tag}" \
        --metadata-file "${metadata_file}" \
        --label "org.opencontainers.image.revision=${IMAGE_REVISION}" \
        --cache-from "type=gha,scope=${cache_scope}" \
        --cache-to "type=gha,scope=${cache_scope},mode=max,ignore-error=true" \
        --push \
        .
    ) &
    pids+=("$!")
  done
fi

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

if [[ "${historical_incident_reuse}" != true && "${canonical_receipt_reuse}" != true ]]; then
  for index in "${!names[@]}"; do
    if [[ "${tag_preflight_reused[${index}]}" == true ]]; then
      continue
    fi
    target="${names[${index}]}"
    metadata_file="${metadata_dir}/${target}.json"
    if ! digest="$(image_digest_from_metadata "${metadata_file}")"; then
      printf 'image digest metadata verification failed: %s\n' "${target}" >&2
      rc=1
      continue
    fi
    digests+=("${digest}")
  done
fi

if [[ "${rc}" -ne 0 ]]; then
  exit "${rc}"
fi

verification_files=()
for index in "${!names[@]}"; do
  target="${names[${index}]}"
  repository="${repositories[${index}]}"
  digest="${digests[${index}]}"
  if [[ "${tag_preflight_reused[${index}]}" == true ]]; then
    verification_files+=("${tag_preflight_files[${index}]}")
    printf 'verified immutable source tag %s -> %s@%s\n' "${target}" "${repository}" "${digest}"
    continue
  fi
  verification_file="${metadata_dir}/${target}.verified.json"
  verification_files+=("${verification_file}")
  printf 'verifying %s -> %s@%s\n' "${target}" "${repository}" "${digest}"
  if [[ "${canonical_receipt_reuse}" == true ]]; then
    python3 "${registry_verifier}" \
      --image "${repository}@${digest}" \
      --platform linux/amd64 \
      --expected-revision "${IMAGE_REVISION}" \
      --metadata-only \
      --timeout-seconds 18 \
      --request-timeout-seconds 5 \
      --max-attempts 2 \
      --retry-delay-seconds 0.1 \
      >"${verification_file}" &
    pids+=("$!")
  elif [[ "${historical_incident_reuse}" == true ]]; then
    python3 "${registry_verifier}" \
      --image "${repository}@${digest}" \
      --platform linux/amd64 \
      --expected-revision "${IMAGE_REVISION}" \
      >"${verification_file}" &
    pids+=("$!")
  elif ! python3 "${registry_verifier}" \
    --image "${repository}@${digest}" \
    --platform linux/amd64 \
    --expected-revision "${IMAGE_REVISION}" \
    >"${verification_file}"; then
    printf 'registry image verification failed: %s\n' "${target}" >&2
    exit 1
  fi
done
if [[ "${historical_incident_reuse}" == true || "${canonical_receipt_reuse}" == true ]]; then
  rc=0
  for index in "${!pids[@]}"; do
    if ! wait "${pids[${index}]}"; then
      printf 'registry-reverification-failed:%s\n' "${names[${index}]}" >&2
      rc=1
    fi
    pids[${index}]=''
  done
  pids=()
  [[ "${rc}" == 0 ]] || exit "${rc}"
fi

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
  "${canonical_receipt_reuse}" \
  "${FUGUE_IMAGE_TAG}" \
  "${IMAGE_REVISION}" \
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
METADATA_VERIFICATION = "registry_manifest_config_get"
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


reuse_mode, source_tag, revision, artifacts_path, digest_path, *arguments = sys.argv[1:]
if reuse_mode not in {"true", "false"}:
    raise SystemExit("canonical receipt reuse mode must be true or false")
if re.fullmatch(r"[0-9a-f]{40}", source_tag) is None:
    raise SystemExit("image source tag must be a complete lowercase 40-character Git revision")
if re.fullmatch(r"[0-9a-f]{40}", revision) is None:
    raise SystemExit("image OCI revision must be a complete lowercase 40-character Git revision")
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
    expected_method = METADATA_VERIFICATION if reuse_mode == "true" else VERIFICATION
    if result["verification"] != expected_method:
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
            "source_tag": source_tag,
            "top_digest": top_digest,
            "verification": VERIFICATION,
        }
    )

artifacts.sort(key=lambda artifact: artifact["component"])
canonical = json.dumps(artifacts, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
Path(artifacts_path).write_bytes(canonical)
Path(digest_path).write_text("sha256:" + hashlib.sha256(canonical).hexdigest(), encoding="ascii")
PY

if [[ "${historical_incident_reuse}" == true ]]; then
  [[ "$(cat "${verified_artifacts_digest_file}")" == "sha256:fb14c704a84253ac4df59ba38d3cf83f91ae9e10b78b998ca95ffee2b555e495" ]] || {
    printf 'historical incident verified artifact provenance changed\n' >&2
    exit 1
  }
fi

if [[ "${canonical_receipt_reuse}" == true ]] && ! cmp -s "${verified_artifacts_file}" "${canonical_receipt_file}"; then
	printf 'registry-reverification-receipt-mismatch\n' >&2
	exit 1
fi

staged_output="${metadata_dir}/outputs"
: >"${staged_output}"
for index in "${!names[@]}"; do
  printf '%s=%s\n' "${names[${index}]}_image_digest" "${digests[${index}]}" >>"${staged_output}"
done
printf 'verified_image_artifacts_json=%s\n' "$(cat "${verified_artifacts_file}")" >>"${staged_output}"
printf 'verified_image_artifacts_digest=%s\n' "$(cat "${verified_artifacts_digest_file}")" >>"${staged_output}"
trap '' INT TERM
publish_outputs "${staged_output}"
