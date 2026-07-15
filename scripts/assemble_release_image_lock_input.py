#!/usr/bin/env python3

"""Assemble a strict release-image lock source from full build provenance."""

import argparse
import hashlib
import json
import os
import re
import stat
import sys

from build_release_image_lock import (
    ACTIVATION_FIELDS,
    MAX_INPUT_BYTES,
    LockBuildError,
    build_lock,
    canonical_bytes,
    reject_control_characters,
    reject_duplicate_keys,
    reject_json_constant,
    require_exact_fields,
    require_string,
    validate_artifact,
    write_once,
)


WORKFLOW = "deploy-control-plane"
PLATFORM = "linux/amd64"
MAX_ARTIFACTS = 64
MAX_ACTIVATIONS = 4096
COMPONENT_ORDER = (
    "api",
    "controller",
    "drain_agent",
    "telemetry_agent",
    "image_cache",
    "edge",
    "app_ssh",
)
DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
COUNT_RE = re.compile(r"[0-7]")


def stat_identity(value):
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


def _read_fd(file_fd, limit):
    chunks = []
    total = 0
    while True:
        chunk = os.read(file_fd, min(64 * 1024, limit + 1 - total))
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
        if total > limit:
            raise LockBuildError(f"input exceeds {limit} bytes")
    return b"".join(chunks)


def read_secure_input(path_value, label, limit=MAX_INPUT_BYTES):
    path = os.fspath(path_value)
    if not os.path.isabs(path) or os.path.normpath(path) != path:
        raise LockBuildError(f"{label} path must be absolute and normalized")
    parent_path = os.path.dirname(path)
    file_name = os.path.basename(path)
    if not file_name or file_name in {".", ".."}:
        raise LockBuildError(f"{label} path must name a file")

    try:
        parent_lstat = os.lstat(parent_path)
    except OSError as exc:
        raise LockBuildError(f"could not inspect {label} parent: {exc}") from exc
    if not stat.S_ISDIR(parent_lstat.st_mode):
        raise LockBuildError(f"{label} parent must be a non-symlink directory")
    if parent_lstat.st_uid != os.geteuid() or stat.S_IMODE(parent_lstat.st_mode) & 0o077:
        raise LockBuildError(f"{label} parent must be private and owned by the current user")

    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        directory_fd = os.open(parent_path, directory_flags)
    except OSError as exc:
        raise LockBuildError(f"could not securely open {label} parent: {exc}") from exc

    file_fd = -1
    try:
        parent_fstat = os.fstat(directory_fd)
        if stat_identity(parent_fstat) != stat_identity(parent_lstat):
            raise LockBuildError(f"{label} parent changed during validation")
        try:
            file_lstat = os.stat(file_name, dir_fd=directory_fd, follow_symlinks=False)
        except OSError as exc:
            raise LockBuildError(f"could not inspect {label}: {exc}") from exc
        if (
            not stat.S_ISREG(file_lstat.st_mode)
            or file_lstat.st_uid != os.geteuid()
            or stat.S_IMODE(file_lstat.st_mode) != 0o600
            or file_lstat.st_nlink != 1
        ):
            raise LockBuildError(
                f"{label} must be a current-user 0600 regular file with one link"
            )
        if file_lstat.st_size > limit:
            raise LockBuildError(f"{label} exceeds {limit} bytes")

        file_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
        file_flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            file_fd = os.open(file_name, file_flags, dir_fd=directory_fd)
        except OSError as exc:
            raise LockBuildError(f"could not securely open {label}: {exc}") from exc
        opened_stat = os.fstat(file_fd)
        if stat_identity(opened_stat) != stat_identity(file_lstat):
            raise LockBuildError(f"{label} changed before it was opened")
        raw = _read_fd(file_fd, limit)
        final_stat = os.fstat(file_fd)
        if stat_identity(final_stat) != stat_identity(opened_stat) or len(raw) != opened_stat.st_size:
            raise LockBuildError(f"{label} changed while it was read")
        return raw, (opened_stat.st_dev, opened_stat.st_ino)
    finally:
        if file_fd >= 0:
            os.close(file_fd)
        os.close(directory_fd)


def decode_canonical_array(raw, label):
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise LockBuildError(f"{label} must be UTF-8 JSON") from exc
    try:
        value = json.loads(
            text,
            object_pairs_hook=reject_duplicate_keys,
            parse_constant=reject_json_constant,
            parse_int=lambda value: (_ for _ in ()).throw(
                LockBuildError(f"{label} must not contain JSON numbers")
            ),
            parse_float=lambda value: (_ for _ in ()).throw(
                LockBuildError(f"{label} must not contain JSON numbers")
            ),
        )
    except LockBuildError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise LockBuildError(f"{label} is not valid JSON: {exc}") from exc
    try:
        reject_control_characters(value, label)
    except RecursionError as exc:
        raise LockBuildError(f"{label} nesting is too deep") from exc
    if not isinstance(value, list):
        raise LockBuildError(f"{label} must be a JSON array")
    try:
        encoded = canonical_bytes(value)
    except (TypeError, ValueError, RecursionError) as exc:
        raise LockBuildError(f"{label} cannot be canonicalized") from exc
    if encoded != raw:
        raise LockBuildError(f"{label} must use canonical JSON bytes without a newline")
    return value


def parse_expected_components(value, expected_count):
    if value != value.strip() or any(character.isspace() and character != " " for character in value):
        raise LockBuildError("expected built components must use canonical single-space separation")
    components = [] if value == "" else value.split(" ")
    if "" in components or " ".join(components) != value:
        raise LockBuildError("expected built components must use canonical single-space separation")
    if len(components) != len(set(components)):
        raise LockBuildError("expected built components contain a duplicate")
    canonical = [component for component in COMPONENT_ORDER if component in set(components)]
    if components != canonical:
        raise LockBuildError("expected built components are unknown or out of canonical order")
    if COUNT_RE.fullmatch(expected_count) is None:
        raise LockBuildError("expected built component count must be an integer from 0 through 7")
    if int(expected_count) != len(components):
        raise LockBuildError("expected built component count does not match the component list")
    return components


def require_private_output_parent(output_path):
    if not os.path.isabs(output_path) or os.path.normpath(output_path) != output_path:
        raise LockBuildError("--output must be an absolute normalized path")
    try:
        parent = os.lstat(os.path.dirname(output_path))
    except OSError as exc:
        raise LockBuildError(f"could not inspect output parent: {exc}") from exc
    if (
        not stat.S_ISDIR(parent.st_mode)
        or parent.st_uid != os.geteuid()
        or stat.S_IMODE(parent.st_mode) & 0o077
    ):
        raise LockBuildError("output parent must be private and owned by the current user")


def assemble(args):
    artifact_raw, artifact_identity = read_secure_input(
        args.verified_artifacts, "verified artifacts"
    )
    activation_raw, activation_identity = read_secure_input(args.activations, "activations")
    if artifact_identity == activation_identity:
        raise LockBuildError("verified artifacts and activations must be different files")

    if DIGEST_RE.fullmatch(args.expected_verified_artifacts_digest) is None:
        raise LockBuildError("expected verified artifacts digest must be a lowercase sha256 digest")
    actual_digest = "sha256:" + hashlib.sha256(artifact_raw).hexdigest()
    if actual_digest != args.expected_verified_artifacts_digest:
        raise LockBuildError("verified artifacts do not match the externally fenced digest")

    expected_components = parse_expected_components(
        args.expected_built_components, args.expected_built_component_count
    )
    artifacts = decode_canonical_array(artifact_raw, "verified artifacts")
    activations = decode_canonical_array(activation_raw, "activations")
    if len(artifacts) > MAX_ARTIFACTS:
        raise LockBuildError("verified artifact count exceeds the supported limit")
    if not activations or len(activations) > MAX_ACTIVATIONS:
        raise LockBuildError("activation count is outside the supported limit")

    validated_artifacts = []
    artifacts_by_component = {}
    for index, artifact_value in enumerate(artifacts):
        artifact = validate_artifact(artifact_value, index, args.head_sha)
        component = artifact["component"]
        if component in artifacts_by_component:
            raise LockBuildError(f"duplicate verified artifact component: {component}")
        artifacts_by_component[component] = artifact
        validated_artifacts.append(artifact)
    actual_components = [artifact["component"] for artifact in validated_artifacts]
    if actual_components != sorted(actual_components):
        raise LockBuildError("verified artifacts are not in producer canonical order")
    if set(actual_components) != set(expected_components):
        raise LockBuildError("verified artifact components do not match the fenced build plan")
    fenced_repository_tags = {
        (artifact["repository"], artifact["source_tag"])
        for artifact in validated_artifacts
    }
    fenced_repository_digests = {
        (artifact["repository"], image_digest)
        for artifact in validated_artifacts
        for image_digest in (
            artifact["top_digest"],
            artifact["platform_manifest_digest"],
        )
    }

    built_components = set()
    for index, activation in enumerate(activations):
        require_exact_fields(activation, ACTIVATION_FIELDS, f"activations[{index}]")
        component = require_string(activation["component"], f"activations[{index}].component")
        source_mode = require_string(
            activation["source_mode"], f"activations[{index}].source_mode"
        )
        if source_mode != "built":
            repository = require_string(
                activation["repository"], f"activations[{index}].repository"
            )
            source_tag = require_string(
                activation["source_tag"], f"activations[{index}].source_tag"
            )
            selected_digest = require_string(
                activation["digest"], f"activations[{index}].digest"
            )
            runtime_manifest_digest = require_string(
                activation["runtime_manifest_digest"],
                f"activations[{index}].runtime_manifest_digest",
            )
            if (
                (repository, source_tag) in fenced_repository_tags
                or (repository, selected_digest) in fenced_repository_digests
                or (repository, runtime_manifest_digest) in fenced_repository_digests
            ):
                raise LockBuildError(
                    "an activation selecting the fenced build repository and identity "
                    "must use source_mode=built"
                )
        if source_mode == "built":
            built_components.add(component)
    missing = built_components - set(artifacts_by_component)
    if missing:
        raise LockBuildError("a built activation has no verified artifact from the fenced build plan")

    selected_artifacts = [
        artifact
        for artifact in validated_artifacts
        if artifact["component"] in built_components or artifact["component"] == "app_ssh"
    ]
    source = {
        "schema_version": 1,
        "release": {
            "repository": args.repository,
            "workflow": WORKFLOW,
            "run_id": args.run_id,
            "run_attempt": args.run_attempt,
            "head_sha": args.head_sha,
            "platform": PLATFORM,
        },
        "artifacts": selected_artifacts,
        "activations": activations,
    }
    validated_lock = build_lock(source)
    normalized = {
        "schema_version": validated_lock["schema_version"],
        "release": validated_lock["release"],
        "artifacts": validated_lock["artifacts"],
        "activations": validated_lock["activations"],
    }
    payload = canonical_bytes(normalized) + b"\n"
    if len(payload) > MAX_INPUT_BYTES:
        raise LockBuildError(f"assembled lock input exceeds {MAX_INPUT_BYTES} bytes")
    return payload


def reject_duplicate_options(argv):
    seen = set()
    for argument in argv:
        if not argument.startswith("--") or argument == "--":
            continue
        name = argument.split("=", 1)[0]
        if name in seen:
            raise LockBuildError(f"{name} was supplied more than once")
        seen.add(name)


def parse_args(argv=None):
    actual_argv = list(sys.argv[1:] if argv is None else argv)
    reject_duplicate_options(actual_argv)
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--verified-artifacts", required=True)
    parser.add_argument("--expected-verified-artifacts-digest", required=True)
    parser.add_argument("--expected-built-components", required=True)
    parser.add_argument("--expected-built-component-count", required=True)
    parser.add_argument("--activations", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-attempt", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args(actual_argv)


def main(argv=None):
    try:
        args = parse_args(argv)
        require_private_output_parent(args.output)
        payload = assemble(args)
        write_once(args.output, payload)
    except (LockBuildError, OSError) as exc:
        print(f"release image lock input assembly failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
