#!/usr/bin/env python3
"""Publish bounded storage constraints without depending on component builds."""
import argparse
import json
import re
import subprocess
from pathlib import Path


def manifest(document):
    if not {"namespace", "name", "policy"} <= set(document) or set(document) - {"namespace", "name", "policy", "observationRetention", "compilerInputCodec"}:
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
    if "compilerInputCodec" in document:
        codec = document["compilerInputCodec"]
        if set(codec) != {"version", "enabled"} or type(codec["version"]) is not int or codec["version"] != 1 or type(codec["enabled"]) is not bool:
            raise ValueError("invalid compiler input codec policy")
        data["compiler-input-codec.json"] = json.dumps(codec, sort_keys=True, separators=(",", ":"))
    if "observationRetention" in document:
        r = document["observationRetention"]
        bounds = {"interval_seconds": (30, 3600), "batch_rows": (1, 1000), "heartbeat_days": (1, 90),
                  "heartbeat_rows_per_chain": (1000, 100000), "probe_days": (7, 90), "probe_rows_per_table": (10000, 10000000)}
        if set(r) != {"version", "enabled", *bounds} or type(r["version"]) is not int or r["version"] != 1 or type(r["enabled"]) is not bool:
            raise ValueError("invalid observation retention policy")
        if any(type(r[k]) is not int or not low <= r[k] <= high for k, (low, high) in bounds.items()):
            raise ValueError("observation retention exceeds safety bounds")
        data["observation-retention.json"] = json.dumps(r, sort_keys=True, separators=(",", ":"))
    return {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"namespace": document["namespace"], "name": document["name"]}, "data": data}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    value = manifest(json.loads(Path(args.path).read_text()))
    body = json.dumps(value)
    command = ["kubectl", "apply", "--server-side", "--field-manager=fugue-storage-policy", "-f", "-"]
    if not args.apply:
        command.append("--dry-run=server")
    subprocess.run(command, input=body, text=True, check=True, timeout=30)


if __name__ == "__main__":
    main()
