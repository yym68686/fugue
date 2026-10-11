#!/usr/bin/env python3
"""Publish bounded storage constraints without depending on component builds."""
import argparse
import json
import re
import subprocess
from pathlib import Path


def manifest(document):
    if set(document) != {"namespace", "name", "policy"}:
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
    return {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"namespace": document["namespace"], "name": document["name"]}, "data": {"policy.json": json.dumps(p, sort_keys=True, separators=(",", ":"))}}


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
