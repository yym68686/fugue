#!/usr/bin/env python3

import copy
import json
import os
import sys


CRD_NAME = "managedapps.fugue.pro"
REQUIRED_RELEASE_FIELDS = {
    "currentReleaseKey": "string",
    "currentReleaseReadyAt": "string",
    "currentReleaseStartedAt": "string",
    "pendingReleaseKey": "string",
    "pendingReleaseStartedAt": "string",
}
REQUIRED_BACKING_SERVICE_FIELDS = {
    "currentRuntimeReadyAt": "string",
    "currentRuntimeStartedAt": "string",
    "desiredInstances": "integer",
    "message": "string",
    "phase": "string",
    "readyInstances": "integer",
    "runtimeKey": "string",
    "serviceID": "string",
}


class TransitionError(ValueError):
    pass


def _mapping(value, label):
    if not isinstance(value, dict):
        raise TransitionError(f"{label} must be an object")
    return value


def _versions(document, label):
    spec = _mapping(_mapping(document, label).get("spec"), f"{label}.spec")
    versions = spec.get("versions")
    if not isinstance(versions, list) or not versions:
        raise TransitionError(f"{label}.spec.versions must be a non-empty array")
    by_name = {}
    for version in versions:
        version = _mapping(version, f"{label}.spec.versions[]")
        name = version.get("name")
        if not isinstance(name, str) or not name or name in by_name:
            raise TransitionError(f"{label} contains an invalid or duplicate version name")
        by_name[name] = version
    return spec, by_name


def _status_properties(version, label):
    schema = _mapping(version.get("schema"), f"{label}.schema")
    openapi = _mapping(schema.get("openAPIV3Schema"), f"{label}.schema.openAPIV3Schema")
    properties = _mapping(openapi.get("properties"), f"{label}.schema.openAPIV3Schema.properties")
    status = _mapping(properties.get("status"), f"{label}.schema.openAPIV3Schema.properties.status")
    return _mapping(status.get("properties"), f"{label}.status.properties")


def _validate_required_status_schema(properties):
    for field, expected_type in REQUIRED_RELEASE_FIELDS.items():
        schema = _mapping(properties.get(field), f"target status property {field}")
        if schema.get("type") != expected_type:
            raise TransitionError(f"target status property {field} must have type {expected_type}")

    backing = _mapping(properties.get("backingServices"), "target status property backingServices")
    if backing.get("type") != "array":
        raise TransitionError("target status property backingServices must have type array")
    items = _mapping(backing.get("items"), "target status property backingServices.items")
    if items.get("type") != "object":
        raise TransitionError("target status property backingServices.items must have type object")
    item_properties = _mapping(
        items.get("properties"), "target status property backingServices.items.properties"
    )
    for field, expected_type in REQUIRED_BACKING_SERVICE_FIELDS.items():
        schema = _mapping(
            item_properties.get(field), f"target backingServices item property {field}"
        )
        if schema.get("type") != expected_type:
            raise TransitionError(
                f"target backingServices item property {field} must have type {expected_type}"
            )


def validate_transition(live, target):
    live_metadata = _mapping(_mapping(live, "live").get("metadata"), "live.metadata")
    target_metadata = _mapping(_mapping(target, "target").get("metadata"), "target.metadata")
    if live_metadata.get("name") != CRD_NAME or target_metadata.get("name") != CRD_NAME:
        raise TransitionError(f"both CRDs must be named {CRD_NAME}")

    live_spec, live_versions = _versions(live, "live")
    target_spec, target_versions = _versions(target, "target")
    if set(live_versions) != set(target_versions):
        raise TransitionError("CRD version inventory must not change in an additive status sync")

    normalized_target = copy.deepcopy(target_spec)
    normalized_target_versions = {
        version["name"]: version for version in normalized_target["versions"]
    }
    additions = []
    for name, live_version in live_versions.items():
        target_version = target_versions[name]
        live_properties = _status_properties(live_version, f"live version {name}")
        target_properties = _status_properties(target_version, f"target version {name}")
        for field, schema in live_properties.items():
            if field not in target_properties:
                raise TransitionError(f"target removes existing status property {name}.{field}")
            if target_properties[field] != schema:
                raise TransitionError(f"target changes existing status property {name}.{field}")
        for field in sorted(set(target_properties) - set(live_properties)):
            additions.append(f"{name}.{field}")

        normalized_properties = _status_properties(
            normalized_target_versions[name], f"normalized target version {name}"
        )
        normalized_properties.clear()
        normalized_properties.update(copy.deepcopy(live_properties))
        _validate_required_status_schema(target_properties)

    if normalized_target != live_spec:
        raise TransitionError(
            "target changes CRD spec outside additive status properties"
        )
    return additions


def build_json_patch(live, target):
    additions = validate_transition(live, target)
    if not additions:
        return []

    metadata = _mapping(_mapping(live, "live").get("metadata"), "live.metadata")
    resource_version = metadata.get("resourceVersion")
    if not isinstance(resource_version, str) or not resource_version:
        raise TransitionError("live.metadata.resourceVersion must be a non-empty string")

    live_spec, _ = _versions(live, "live")
    _, target_versions = _versions(target, "target")
    version_indexes = {
        version["name"]: index for index, version in enumerate(live_spec["versions"])
    }
    patch = [
        {"op": "test", "path": "/metadata/name", "value": CRD_NAME},
        {
            "op": "test",
            "path": "/metadata/resourceVersion",
            "value": resource_version,
        },
    ]
    for addition in additions:
        version_name, field = addition.split(".", 1)
        properties = _status_properties(
            target_versions[version_name], f"target version {version_name}"
        )
        patch.append(
            {
                "op": "add",
                "path": (
                    f"/spec/versions/{version_indexes[version_name]}/schema/"
                    f"openAPIV3Schema/properties/status/properties/{field}"
                ),
                "value": copy.deepcopy(properties[field]),
            }
        )
    return patch


def _load(path, label):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TransitionError(f"cannot read {label} JSON: {exc}") from exc


def _write_json(path, value):
    temporary = f"{path}.tmp.{os.getpid()}"
    try:
        with open(temporary, "x", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
        os.replace(temporary, path)
    except (OSError, UnicodeError, TypeError, ValueError) as exc:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise TransitionError(f"cannot write JSON patch: {exc}") from exc


def main(argv):
    if len(argv) not in (3, 5) or (len(argv) == 5 and argv[3] != "--patch-output"):
        print(
            f"usage: {argv[0]} LIVE.json TARGET.json [--patch-output PATCH.json]",
            file=sys.stderr,
        )
        return 2
    try:
        live = _load(argv[1], "live")
        target = _load(argv[2], "target")
        additions = validate_transition(live, target)
        if len(argv) == 5:
            patch = build_json_patch(live, target)
            if not patch:
                raise TransitionError("refusing to create an empty JSON patch")
            _write_json(argv[4], patch)
    except TransitionError as exc:
        print(f"managed app CRD transition rejected: {exc}", file=sys.stderr)
        return 1
    if additions:
        print("additive:" + ",".join(additions))
    else:
        print("noop")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
