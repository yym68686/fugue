#!/usr/bin/env python3

"""Validate and atomically publish a deterministic release image lock."""

import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import stat
import sys


MAX_INPUT_BYTES = 1024 * 1024
PRODUCER = "fugue-release-image-lock"
VERIFICATION = "registry_manifest_config_and_layer_get"
WORKFLOW = "deploy-control-plane"
PLATFORM = "linux/amd64"

DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
GIT_REVISION_RE = re.compile(r"[0-9a-f]{40}")
POSITIVE_INTEGER_STRING_RE = re.compile(r"[1-9][0-9]*")
TAG_RE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}")
REPOSITORY_COMPONENT_RE = re.compile(r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*")
REGISTRY_LABEL_RE = re.compile(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?")
RELEASE_OWNER_RE = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?")
RELEASE_NAME_RE = re.compile(r"[A-Za-z0-9_.-]{1,100}")
DNS_LABEL_RE = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
HELM_PATH_RE = re.compile(
    r"[A-Za-z][A-Za-z0-9_-]*(?:\[(?:0|[1-9][0-9]*)\])*"
    r"(?:\.[A-Za-z][A-Za-z0-9_-]*(?:\[(?:0|[1-9][0-9]*)\])*)*"
)

ARTIFACT_COMPONENTS = {
    "api",
    "app_ssh",
    "controller",
    "drain_agent",
    "edge",
    "image_cache",
    "telemetry_agent",
}
ACTIVATION_COMPONENTS = ARTIFACT_COMPONENTS - {"app_ssh"}
SOURCE_MODES = {"built", "migration", "preserve"}
PIN_STATES = {"pinned", "legacy_unpinned"}
COMPONENT_HELM_PATH_SHAPES = {
    "api": {"api.image": {"Deployment": {"api"}}},
    "controller": {
        "controller.image": {
            "CronJob": {"registry-gc", "registry-janitor"},
            "Deployment": {"controller"},
        },
        "imagePrePull.image": {"DaemonSet": {"image-prepull"}},
        "nodeJanitor.image": {"DaemonSet": {"node-janitor"}},
        "topologyLabeler.image": {"DaemonSet": {"topology-labeler"}},
    },
    "drain_agent": {
        "runtime.strictDrain.agent.image": {"Configuration": {"controller"}}
    },
    "image_cache": {"imageCache.image": {"DaemonSet": {"image-cache"}}},
    "telemetry_agent": {
        "observability.agent.image": {"Deployment": {"telemetry-agent"}}
    },
}
ARRAY_INDEX = r"(?:0|[1-9][0-9]*)"
# helm_path is the logical image leaf owned by the workload. It remains the
# leaf even when the effective identity is inherited from an ancestor; the
# selected/source refs record the resolved identity. Release preparation
# materializes mutable inherited cohorts before this lock is finalized.
EDGE_HELM_PATH_SHAPES = (
    (
        re.compile(
            rf"(?:edge\.image|edge\.blueGreen\.slots\.[ab]\.image|"
            rf"edge\.dynamic\.blueGreen\.slots\.[ab]\.image|"
            rf"edge\.groups\[{ARRAY_INDEX}\]\."
            rf"(?:image|blueGreen\.slots\.[ab]\.image))"
        ),
        {"edge"},
    ),
    (
        re.compile(
            rf"(?:edge\.blueGreen\.front\.image|"
            rf"edge\.dynamic\.blueGreen\.front\.image|"
            rf"edge\.groups\[{ARRAY_INDEX}\]\.blueGreen\.front\.image)"
        ),
        {"edge-front"},
    ),
    (
        re.compile(
            rf"(?:edge\.sshFront\.image|"
            rf"edge\.groups\[{ARRAY_INDEX}\]\.sshFront\.image)"
        ),
        {"ssh-front"},
    ),
    (re.compile(rf"(?:dns\.image|dns\.groups\[{ARRAY_INDEX}\]\.image)"), None),
    (re.compile(r"meshRecovery\.image"), {"mesh-recovery"}),
)

ROOT_FIELDS = {"schema_version", "release", "artifacts", "activations"}
RELEASE_FIELDS = {
    "repository",
    "workflow",
    "run_id",
    "run_attempt",
    "head_sha",
    "platform",
}
ARTIFACT_FIELDS = {
    "component",
    "repository",
    "source_tag",
    "top_digest",
    "platform_manifest_digest",
    "config_digest",
    "oci_revision",
    "immutable_ref",
    "verification",
}
ACTIVATION_FIELDS = {
    "component",
    "source_mode",
    "source_template_ref",
    "selected_ref",
    "repository",
    "source_tag",
    "digest",
    "runtime_manifest_digest",
    "pin_state",
    "migration_allowed",
    "helm_path",
    "workload",
}
WORKLOAD_FIELDS = {"kind", "name", "container"}


class LockBuildError(Exception):
    pass


class DuplicateKeyError(LockBuildError):
    pass


def reject_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateKeyError(f"duplicate JSON key: {key!r}")
        result[key] = value
    return result


def reject_json_constant(value):
    raise LockBuildError(f"non-finite JSON number is not allowed: {value}")


def has_control_character(value):
    return any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)


def reject_control_characters(value, location="input"):
    if isinstance(value, str):
        if has_control_character(value):
            raise LockBuildError(f"{location} contains a control character")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            reject_control_characters(item, f"{location}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            reject_control_characters(key, f"{location} key")
            reject_control_characters(item, f"{location}.{key}")


def require_exact_fields(value, required, location):
    if not isinstance(value, dict):
        raise LockBuildError(f"{location} must be an object")
    unknown = sorted(set(value) - required)
    missing = sorted(required - set(value))
    if unknown:
        raise LockBuildError(f"{location} has unknown fields: {', '.join(unknown)}")
    if missing:
        raise LockBuildError(f"{location} is missing fields: {', '.join(missing)}")


def require_string(value, location):
    if not isinstance(value, str):
        raise LockBuildError(f"{location} must be a string")
    return value


def require_boolean(value, location):
    if not isinstance(value, bool):
        raise LockBuildError(f"{location} must be a boolean")
    return value


def validate_digest(value, location, allow_empty=False):
    value = require_string(value, location)
    if allow_empty and value == "":
        return value
    if DIGEST_RE.fullmatch(value) is None:
        raise LockBuildError(f"{location} must be a lowercase sha256 digest")
    return value


def validate_tag(value, location):
    value = require_string(value, location)
    if TAG_RE.fullmatch(value) is None:
        raise LockBuildError(f"{location} is not a valid image tag")
    return value


def validate_registry_host(host):
    if not host or len(host) > 253 or host.startswith(".") or host.endswith(".") or ".." in host:
        return False
    return all(
        1 <= len(label) <= 63 and REGISTRY_LABEL_RE.fullmatch(label) is not None
        for label in host.split(".")
    )


def validate_image_repository(value, location):
    value = require_string(value, location)
    if (
        not value
        or len(value) > 255
        or value.startswith("/")
        or value.endswith("/")
        or "//" in value
        or "@" in value
        or any(character.isspace() for character in value)
    ):
        raise LockBuildError(f"{location} is not a valid image repository")

    components = value.split("/")
    first = components[0]
    repository_components = components
    if ":" in first:
        if first.count(":") != 1 or len(components) < 2:
            raise LockBuildError(f"{location} has an invalid registry authority")
        host, port = first.split(":", 1)
        if (
            not validate_registry_host(host)
            or re.fullmatch(r"[1-9][0-9]{0,4}", port) is None
            or int(port) > 65535
        ):
            raise LockBuildError(f"{location} has an invalid registry authority")
        repository_components = components[1:]
    elif len(components) >= 2 and (first == "localhost" or "." in first):
        if not validate_registry_host(first):
            raise LockBuildError(f"{location} has an invalid registry authority")
        repository_components = components[1:]

    if not repository_components or any(
        REPOSITORY_COMPONENT_RE.fullmatch(component) is None
        for component in repository_components
    ):
        raise LockBuildError(f"{location} is not a valid image repository")
    return value


def validate_release_repository(value):
    value = require_string(value, "release.repository")
    if any(character.isspace() for character in value) or has_control_character(value):
        raise LockBuildError("release.repository contains whitespace or a control character")
    parts = value.split("/")
    if (
        len(parts) != 2
        or RELEASE_OWNER_RE.fullmatch(parts[0]) is None
        or RELEASE_NAME_RE.fullmatch(parts[1]) is None
        or parts[1] in {".", ".."}
    ):
        raise LockBuildError("release.repository must be an owner/repository name")
    return value


def parse_image_ref(value, location, source_tag_hint=""):
    value = require_string(value, location)
    if not value or "://" in value or any(character.isspace() for character in value):
        raise LockBuildError(f"{location} is not a strict image reference")
    if value.count("@") > 1:
        raise LockBuildError(f"{location} has multiple digest separators")

    if "@" in value:
        name_and_tag, digest = value.rsplit("@", 1)
        if not name_and_tag or DIGEST_RE.fullmatch(digest) is None:
            raise LockBuildError(f"{location} has an invalid digest")
    else:
        name_and_tag = value
        digest = ""

    last = name_and_tag.rsplit("/", 1)[-1]
    if ":" in last:
        repository, tag = name_and_tag.rsplit(":", 1)
        validate_tag(tag, f"{location} tag")
    else:
        repository = name_and_tag
        tag = ""
    validate_image_repository(repository, f"{location} repository")
    if not digest and not tag:
        raise LockBuildError(f"{location} must contain an explicit tag or digest")

    explicit_tag = tag
    if source_tag_hint:
        validate_tag(source_tag_hint, f"{location} source tag hint")
        if tag and tag != source_tag_hint:
            raise LockBuildError(f"{location} disagrees with its source tag hint")

    if explicit_tag and digest:
        form = "tag_digest"
        exact_ref = f"{repository}:{tag}@{digest}"
    elif digest:
        form = "digest"
        exact_ref = f"{repository}@{digest}"
        tag = source_tag_hint
    else:
        form = "tag"
        exact_ref = f"{repository}:{tag}"
    return {
        "repository": repository,
        "source_tag": tag,
        "digest": digest,
        "form": form,
        "exact_ref": exact_ref,
    }


def validate_release(value):
    require_exact_fields(value, RELEASE_FIELDS, "release")
    release = {
        "repository": validate_release_repository(value["repository"]),
        "workflow": require_string(value["workflow"], "release.workflow"),
        "run_id": require_string(value["run_id"], "release.run_id"),
        "run_attempt": require_string(value["run_attempt"], "release.run_attempt"),
        "head_sha": require_string(value["head_sha"], "release.head_sha"),
        "platform": require_string(value["platform"], "release.platform"),
    }
    if release["workflow"] != WORKFLOW:
        raise LockBuildError(f"release.workflow must be {WORKFLOW}")
    for field in ("run_id", "run_attempt"):
        if POSITIVE_INTEGER_STRING_RE.fullmatch(release[field]) is None:
            raise LockBuildError(f"release.{field} must be a positive integer string")
    if GIT_REVISION_RE.fullmatch(release["head_sha"]) is None:
        raise LockBuildError("release.head_sha must be a lowercase 40-character commit SHA")
    if release["platform"] != PLATFORM:
        raise LockBuildError(f"release.platform must be {PLATFORM}")
    return release


def validate_artifact(value, index, head_sha):
    location = f"artifacts[{index}]"
    require_exact_fields(value, ARTIFACT_FIELDS, location)
    component = require_string(value["component"], f"{location}.component")
    if component not in ARTIFACT_COMPONENTS:
        raise LockBuildError(f"{location}.component is not supported")
    repository = validate_image_repository(value["repository"], f"{location}.repository")
    source_tag = validate_tag(value["source_tag"], f"{location}.source_tag")
    top_digest = validate_digest(value["top_digest"], f"{location}.top_digest")
    platform_manifest_digest = validate_digest(
        value["platform_manifest_digest"], f"{location}.platform_manifest_digest"
    )
    config_digest = validate_digest(value["config_digest"], f"{location}.config_digest")
    oci_revision = require_string(value["oci_revision"], f"{location}.oci_revision")
    immutable_ref = require_string(value["immutable_ref"], f"{location}.immutable_ref")
    verification = require_string(value["verification"], f"{location}.verification")

    if source_tag != head_sha:
        raise LockBuildError(f"{location}.source_tag must equal release.head_sha")
    if oci_revision != head_sha:
        raise LockBuildError(f"{location}.oci_revision must equal release.head_sha")
    if immutable_ref != f"{repository}@{top_digest}":
        raise LockBuildError(f"{location}.immutable_ref does not match repository and top_digest")
    if verification != VERIFICATION:
        raise LockBuildError(f"{location}.verification must be {VERIFICATION}")
    return {
        "component": component,
        "repository": repository,
        "source_tag": source_tag,
        "top_digest": top_digest,
        "platform_manifest_digest": platform_manifest_digest,
        "config_digest": config_digest,
        "oci_revision": oci_revision,
        "immutable_ref": immutable_ref,
        "verification": verification,
    }


def validate_workload(value, location):
    require_exact_fields(value, WORKLOAD_FIELDS, location)
    kind = require_string(value["kind"], f"{location}.kind")
    name = require_string(value["name"], f"{location}.name")
    container = require_string(value["container"], f"{location}.container")
    for field, field_value in (("name", name), ("container", container)):
        if DNS_LABEL_RE.fullmatch(field_value) is None:
            raise LockBuildError(f"{location}.{field} must be a DNS label")
    return {"kind": kind, "name": name, "container": container}


def refs_semantically_equal(left, right):
    return all(left[field] == right[field] for field in ("repository", "source_tag", "digest"))


def validate_activation(value, index, head_sha, artifacts_by_component):
    location = f"activations[{index}]"
    require_exact_fields(value, ACTIVATION_FIELDS, location)
    component = require_string(value["component"], f"{location}.component")
    if component not in ACTIVATION_COMPONENTS:
        raise LockBuildError(f"{location}.component is not activatable")
    source_mode = require_string(value["source_mode"], f"{location}.source_mode")
    if source_mode not in SOURCE_MODES:
        raise LockBuildError(f"{location}.source_mode is not supported")
    repository = validate_image_repository(value["repository"], f"{location}.repository")
    source_tag = validate_tag(value["source_tag"], f"{location}.source_tag")
    digest = validate_digest(value["digest"], f"{location}.digest", allow_empty=True)
    runtime_manifest_digest = validate_digest(
        value["runtime_manifest_digest"],
        f"{location}.runtime_manifest_digest",
        allow_empty=True,
    )
    pin_state = require_string(value["pin_state"], f"{location}.pin_state")
    if pin_state not in PIN_STATES:
        raise LockBuildError(f"{location}.pin_state is not supported")
    migration_allowed = require_boolean(
        value["migration_allowed"], f"{location}.migration_allowed"
    )
    helm_path = require_string(value["helm_path"], f"{location}.helm_path")
    if len(helm_path) > 255 or HELM_PATH_RE.fullmatch(helm_path) is None:
        raise LockBuildError(f"{location}.helm_path must be a safe dotted Helm path")
    workload = validate_workload(value["workload"], f"{location}.workload")
    if component == "edge":
        helm_path_valid = False
        if workload["kind"] == "DaemonSet":
            for path_pattern, allowed_containers in EDGE_HELM_PATH_SHAPES:
                if path_pattern.fullmatch(helm_path) is None:
                    continue
                if allowed_containers is None:
                    helm_path_valid = (
                        re.search(r"(?:^|-)dns(?:-|$)", workload["name"]) is not None
                    )
                else:
                    helm_path_valid = workload["container"] in allowed_containers
                break
        if not helm_path_valid:
            raise LockBuildError(
                f"{location} has an invalid edge Helm path/workload binding"
            )
    else:
        path_shapes = COMPONENT_HELM_PATH_SHAPES[component].get(helm_path, {})
        expected_containers = path_shapes.get(workload["kind"], set())
        if workload["container"] not in expected_containers:
            raise LockBuildError(
                f"{location} has an invalid Helm path/workload binding for {component}"
            )

    selected_ref = parse_image_ref(
        value["selected_ref"], f"{location}.selected_ref", source_tag_hint=source_tag
    )
    source_template_ref = parse_image_ref(
        value["source_template_ref"],
        f"{location}.source_template_ref",
        source_tag_hint=source_tag,
    )
    if selected_ref["repository"] != repository or selected_ref["source_tag"] != source_tag:
        raise LockBuildError(f"{location} repository/source_tag disagree with selected_ref")
    if selected_ref["digest"] != digest:
        raise LockBuildError(f"{location}.digest disagrees with selected_ref")
    if source_template_ref["repository"] != repository:
        raise LockBuildError(f"{location}.source_template_ref uses a different repository")
    if source_template_ref["source_tag"] and source_template_ref["source_tag"] != source_tag:
        raise LockBuildError(f"{location}.source_template_ref uses a different source tag")

    if pin_state == "pinned":
        if not digest or not runtime_manifest_digest:
            raise LockBuildError(f"{location} pinned activations require both image digests")
    else:
        if selected_ref["form"] != "tag" or digest or runtime_manifest_digest:
            raise LockBuildError(
                f"{location} legacy_unpinned activations must be tag-only with empty digests"
            )
        if source_mode != "preserve" or migration_allowed is not True:
            raise LockBuildError(
                f"{location} legacy_unpinned activations require preserve mode and explicit migration_allowed=true"
            )

    if source_mode == "built":
        artifact = artifacts_by_component.get(component)
        if artifact is None:
            raise LockBuildError(f"{location} built activation has no matching artifact")
        if source_template_ref["digest"] and source_template_ref["digest"] != artifact["top_digest"]:
            raise LockBuildError(
                f"{location}.source_template_ref digest does not match its artifact"
            )
        if (
            selected_ref["exact_ref"] != artifact["immutable_ref"]
            or repository != artifact["repository"]
            or source_tag != head_sha
            or digest != artifact["top_digest"]
            or runtime_manifest_digest != artifact["platform_manifest_digest"]
            or pin_state != "pinned"
        ):
            raise LockBuildError(f"{location} built activation does not match its artifact")
        if migration_allowed:
            raise LockBuildError(f"{location} built activation cannot allow migration")
    elif source_mode == "preserve":
        if not refs_semantically_equal(source_template_ref, selected_ref):
            raise LockBuildError(f"{location} preserve activation changes its source reference")
        if pin_state == "pinned" and migration_allowed:
            raise LockBuildError(f"{location} pinned preserve activation cannot allow migration")
    else:
        if (
            source_template_ref["form"] != "tag"
            or pin_state != "pinned"
            or not digest
            or not runtime_manifest_digest
            or not migration_allowed
            or source_template_ref["repository"] != selected_ref["repository"]
            or source_template_ref["source_tag"] != selected_ref["source_tag"]
        ):
            raise LockBuildError(
                f"{location} migration must explicitly pin one legacy tag-only source"
            )

    return {
        "component": component,
        "source_mode": source_mode,
        "source_template_ref": source_template_ref["exact_ref"],
        "selected_ref": selected_ref["exact_ref"],
        "repository": repository,
        "source_tag": source_tag,
        "digest": digest,
        "runtime_manifest_digest": runtime_manifest_digest,
        "pin_state": pin_state,
        "migration_allowed": migration_allowed,
        "helm_path": helm_path,
        "workload": workload,
    }


def canonical_bytes(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def build_lock(document):
    require_exact_fields(document, ROOT_FIELDS, "input")
    schema_version = document["schema_version"]
    if type(schema_version) is not int or schema_version != 1:
        raise LockBuildError("schema_version must be the integer 1")
    release = validate_release(document["release"])
    if not isinstance(document["artifacts"], list):
        raise LockBuildError("artifacts must be an array")
    if not isinstance(document["activations"], list):
        raise LockBuildError("activations must be an array")
    if not document["activations"]:
        raise LockBuildError("activations must contain at least one workload binding")

    artifacts = []
    artifacts_by_component = {}
    for index, raw_artifact in enumerate(document["artifacts"]):
        artifact = validate_artifact(raw_artifact, index, release["head_sha"])
        component = artifact["component"]
        if component in artifacts_by_component:
            raise LockBuildError(f"duplicate artifact component: {component}")
        artifacts_by_component[component] = artifact
        artifacts.append(artifact)

    activations = []
    activation_bindings = set()
    workload_bindings = set()
    helm_path_bindings = {}
    built_components = set()
    for index, raw_activation in enumerate(document["activations"]):
        activation = validate_activation(
            raw_activation, index, release["head_sha"], artifacts_by_component
        )
        workload = activation["workload"]
        binding = (
            activation["component"],
            activation["helm_path"],
            workload["kind"],
            workload["name"],
            workload["container"],
        )
        if binding in activation_bindings:
            raise LockBuildError("duplicate activation workload binding")
        activation_bindings.add(binding)
        workload_binding = (workload["kind"], workload["name"], workload["container"])
        if workload_binding in workload_bindings:
            raise LockBuildError("one workload container cannot have multiple activation bindings")
        workload_bindings.add(workload_binding)
        helm_path_binding = tuple(
            activation[field]
            for field in (
                "component",
                "source_mode",
                "source_template_ref",
                "selected_ref",
                "repository",
                "source_tag",
                "digest",
                "runtime_manifest_digest",
                "pin_state",
                "migration_allowed",
            )
        )
        prior_helm_path_binding = helm_path_bindings.setdefault(
            activation["helm_path"], helm_path_binding
        )
        if prior_helm_path_binding != helm_path_binding:
            raise LockBuildError("one Helm image path cannot select multiple image identities")
        if activation["source_mode"] == "built":
            built_components.add(activation["component"])
        activations.append(activation)

    artifact_only = set(artifacts_by_component) - built_components
    if artifact_only - {"app_ssh"}:
        components = ", ".join(sorted(artifact_only - {"app_ssh"}))
        raise LockBuildError(f"only app_ssh may be artifact-only; found: {components}")

    artifacts.sort(key=lambda artifact: artifact["component"])
    activations.sort(
        key=lambda activation: (
            activation["component"],
            activation["helm_path"],
            activation["workload"]["kind"],
            activation["workload"]["name"],
            activation["workload"]["container"],
            activation["source_mode"],
            activation["selected_ref"],
        )
    )
    result = {
        "schema_version": 1,
        "producer": PRODUCER,
        "release": release,
        "artifacts": artifacts,
        "activations": activations,
    }
    result["lock_digest"] = "sha256:" + hashlib.sha256(canonical_bytes(result)).hexdigest()
    return result


def read_input(path):
    try:
        with open(path, "rb") as stream:
            raw = stream.read(MAX_INPUT_BYTES + 1)
    except OSError as exc:
        raise LockBuildError(f"could not read input: {exc}") from exc
    if len(raw) > MAX_INPUT_BYTES:
        raise LockBuildError(f"input exceeds {MAX_INPUT_BYTES} bytes")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise LockBuildError("input must be UTF-8 JSON") from exc
    try:
        document = json.loads(
            text,
            object_pairs_hook=reject_duplicate_keys,
            parse_constant=reject_json_constant,
        )
    except (json.JSONDecodeError, RecursionError) as exc:
        raise LockBuildError(f"input is not valid JSON: {exc}") from exc
    reject_control_characters(document)
    return document


def rename_noreplace(directory_fd, source_name, target_name):
    libc = ctypes.CDLL(None, use_errno=True)
    source = os.fsencode(source_name)
    target = os.fsencode(target_name)
    if sys.platform == "darwin" and hasattr(libc, "renameatx_np"):
        function = libc.renameatx_np
        function.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        function.restype = ctypes.c_int
        result = function(directory_fd, source, directory_fd, target, 0x00000004)
    elif hasattr(libc, "renameat2"):
        function = libc.renameat2
        function.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        function.restype = ctypes.c_int
        result = function(directory_fd, source, directory_fd, target, 0x00000001)
    else:
        try:
            os.link(
                source_name,
                target_name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
            os.unlink(source_name, dir_fd=directory_fd)
            return
        except OSError as exc:
            raise LockBuildError(f"could not atomically publish output: {exc}") from exc
    if result != 0:
        error_number = ctypes.get_errno()
        raise LockBuildError(
            f"could not atomically publish output: {os.strerror(error_number)}"
        )


def write_once(output_path, payload):
    if not os.path.isabs(output_path):
        raise LockBuildError("--output must be an absolute path")
    parent_path = os.path.dirname(output_path)
    target_name = os.path.basename(output_path)
    if not target_name or target_name in {".", ".."}:
        raise LockBuildError("--output must name a file")

    try:
        parent_lstat = os.lstat(parent_path)
    except OSError as exc:
        raise LockBuildError(f"could not inspect output parent: {exc}") from exc
    if not stat.S_ISDIR(parent_lstat.st_mode):
        raise LockBuildError("output parent must be an existing non-symlink directory")
    if parent_lstat.st_uid != os.geteuid():
        raise LockBuildError("output parent must be owned by the current user")
    if stat.S_IMODE(parent_lstat.st_mode) & 0o022:
        raise LockBuildError("output parent must not be group- or world-writable")

    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        directory_fd = os.open(parent_path, directory_flags)
    except OSError as exc:
        raise LockBuildError(f"could not securely open output parent: {exc}") from exc

    temp_name = ""
    published = False
    try:
        parent_fstat = os.fstat(directory_fd)
        if (
            not stat.S_ISDIR(parent_fstat.st_mode)
            or parent_fstat.st_dev != parent_lstat.st_dev
            or parent_fstat.st_ino != parent_lstat.st_ino
            or parent_fstat.st_uid != os.geteuid()
            or stat.S_IMODE(parent_fstat.st_mode) & 0o022
        ):
            raise LockBuildError("output parent changed during validation")
        try:
            os.stat(target_name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise LockBuildError(f"could not inspect output target: {exc}") from exc
        else:
            raise LockBuildError("output target already exists")

        temp_name = f".{target_name}.tmp.{os.getpid()}.{secrets.token_hex(8)}"
        file_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        file_flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            file_fd = os.open(temp_name, file_flags, 0o600, dir_fd=directory_fd)
        except OSError as exc:
            raise LockBuildError(f"could not create output temporary file: {exc}") from exc
        try:
            os.fchmod(file_fd, 0o600)
            file_stat = os.fstat(file_fd)
            if (
                not stat.S_ISREG(file_stat.st_mode)
                or file_stat.st_uid != os.geteuid()
                or stat.S_IMODE(file_stat.st_mode) != 0o600
            ):
                raise LockBuildError("output temporary file failed its security checks")
            view = memoryview(payload)
            while view:
                written = os.write(file_fd, view)
                if written <= 0:
                    raise LockBuildError("short write while creating output")
                view = view[written:]
            os.fsync(file_fd)
        finally:
            os.close(file_fd)

        rename_noreplace(directory_fd, temp_name, target_name)
        published = True
        temp_name = ""
        try:
            os.fsync(directory_fd)
        except OSError as exc:
            try:
                os.unlink(target_name, dir_fd=directory_fd)
                os.fsync(directory_fd)
                published = False
            except OSError:
                pass
            raise LockBuildError(f"could not make output durable: {exc}") from exc
    finally:
        if temp_name:
            try:
                os.unlink(temp_name, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
            except OSError:
                pass
        os.close(directory_fd)
    if not published:
        raise LockBuildError("output was not published")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="schema-version 1 source JSON")
    parser.add_argument("--output", required=True, help="absolute write-once lock path")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        document = read_input(Path(args.input))
        lock = build_lock(document)
        write_once(args.output, canonical_bytes(lock) + b"\n")
    except (LockBuildError, OSError) as exc:
        print(f"release image lock build failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
