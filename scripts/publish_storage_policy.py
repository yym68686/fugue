#!/usr/bin/env python3
"""Publish bounded storage constraints without depending on component builds."""
import argparse
import json
import re
import subprocess
from pathlib import Path


def manifest(document):
    if not {"namespace", "name", "policy"} <= set(document) or set(document) - {"namespace", "name", "policy", "observationRetention", "storageLifecycle", "compilerInputCodec", "compilerInputShared", "compilerInputSeparated", "artifactContentReferences", "compilerInputBlocks"}:
        raise ValueError("unexpected storage policy configuration")
    for field in ("namespace", "name"):
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", document[field]):
            raise ValueError("invalid ConfigMap identity")
    p = document["policy"]
    if set(p) != {"version", "compiler_input_chunks", "checkpoint_pages", "database_max_open", "database_max_idle", "database_idle_seconds"}:
        raise ValueError("invalid storage policy fields")
    if p["version"] != 1 or any(type(p[k]) is not bool for k in ("compiler_input_chunks", "checkpoint_pages")):
        raise ValueError("invalid storage format policy")
    for key, maximum in (("database_max_open", 256), ("database_max_idle", 128), ("database_idle_seconds", 3600)):
        if type(p[key]) is not int or not 0 <= p[key] <= maximum:
            raise ValueError("invalid database budget")
    if p["database_max_open"] and (p["database_max_idle"] > p["database_max_open"] or not p["database_idle_seconds"]):
        raise ValueError("invalid connection/idle budget")
    data = {"policy.json": json.dumps(p, sort_keys=True, separators=(",", ":"))}
    for key, filename in (("compilerInputCodec", "compiler-input-codec.json"), ("compilerInputShared", "compiler-input-shared.json"), ("compilerInputSeparated", "compiler-input-separated.json"), ("artifactContentReferences", "artifact-content-references.json")):
        if key not in document:
            continue
        codec = document[key]
        if set(codec) != {"version", "enabled"} or type(codec["version"]) is not int or codec["version"] != 1 or type(codec["enabled"]) is not bool:
            raise ValueError("invalid compiler input codec policy")
        data[filename] = json.dumps(codec, sort_keys=True, separators=(",", ":"))
    if "compilerInputBlocks" in document:
        b = document['compilerInputBlocks']
        if (set(b) != {'version', 'minimum_bytes', 'boundary_bits'} or
                type(b['version']) is not int or b['version'] != 1 or
                type(b['minimum_bytes']) is not int or b['minimum_bytes'] not in (256, 2048, 4096, 8192) or
                type(b['boundary_bits']) is not int or b['boundary_bits'] not in (0, 3, 4)):
            raise ValueError('invalid compiler input block profile')
        data['compiler-input-blocks.json'] = json.dumps(b, sort_keys=True, separators=(',', ':'))
    if "observationRetention" in document:
        r = document["observationRetention"]
        bounds = {"interval_seconds": (30, 3600), "batch_rows": (1, 1000), "heartbeat_days": (1, 90),
                  "heartbeat_rows_per_chain": (1000, 100000), "probe_days": (7, 90), "probe_rows_per_table": (10000, 10000000)}
        if set(r) != {"version", "enabled", *bounds} or type(r["version"]) is not int or r["version"] != 1 or type(r["enabled"]) is not bool:
            raise ValueError("invalid observation retention policy")
        if any(type(r[k]) is not int or not low <= r[k] <= high for k, (low, high) in bounds.items()):
            raise ValueError("observation retention exceeds safety bounds")
        data["observation-retention.json"] = json.dumps(r, sort_keys=True, separators=(",", ":"))
    if "storageLifecycle" in document:
        from datetime import datetime
        lifecycle = document["storageLifecycle"]
        bounds = {"interval_seconds": (300, 3600), "source_page_rows": (1, 128),
                  "history_days": (7, 90), "minimum_verified_versions": (3, 100),
                  "unused_upload_hours": (24, 720), "unreferenced_upload_days": (30, 365),
                  "warning_free_percent": (5, 50), "pressure_free_percent": (5, 50), "critical_free_percent": (5, 50)}
        if (set(lifecycle) != {"version", "mode", "effective_from", *bounds} or
                type(lifecycle["version"]) is not int or lifecycle["version"] != 1 or
                lifecycle["mode"] not in ("disabled", "audit") or
                any(type(lifecycle[k]) is not int or not low <= lifecycle[k] <= high for k, (low, high) in bounds.items()) or
                not lifecycle["critical_free_percent"] < lifecycle["pressure_free_percent"] < lifecycle["warning_free_percent"]):
            raise ValueError("invalid or unsupported storage lifecycle policy")
        at = lifecycle["effective_from"]
        if (not isinstance(at, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:Z|[+-][0-9]{2}:[0-9]{2})", at) or
                datetime.fromisoformat(at.replace('Z', '+00:00')).year <= 1):
            raise ValueError("invalid lifecycle effective timestamp")
        data["storage-lifecycle.json"] = json.dumps(lifecycle, sort_keys=True, separators=(",", ":"))
    return {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"namespace": document["namespace"], "name": document["name"]}, "data": data}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    value = manifest(json.loads(Path(args.path).read_text()))
    separated = json.loads(value['data'].get('compiler-input-separated.json', '{}'))
    if separated.get('enabled'):
        # New writes require BOTH current and automatic rollback readers.
        subprocess.run(['python3', '-m', 'scripts.compiler_input_storage'], check=True, timeout=90)
    if json.loads(value['data'].get('artifact-content-references.json', '{}')).get('enabled'):
        # This publisher is also invoked as a standalone script. Its sys.path
        # starts at scripts/, not the repository; run the gate as a module just
        # like the separated-input gate rather than importing scripts inline.
        subprocess.run(['python3', '-m', 'scripts.artifact_content_storage'], check=True, timeout=90)
    body = json.dumps(value)
    command = ["kubectl", "apply", "--server-side", "--field-manager=fugue-storage-policy", "-f", "-"]
    if not args.apply:
        command.append("--dry-run=server")
    subprocess.run(command, input=body, text=True, check=True, timeout=30)


if __name__ == "__main__":
    main()
