#!/usr/bin/env python3
"""Create one declared private Runtime identity and persist its credentials.

Existing runtime identities are never enrolled again or rotated implicitly.
An uncertain create must be investigated if its credential Secret is absent.
"""
import base64
import json
import os
from pathlib import Path
import re
import sys

try:
    from .publish_agent_edge_shadow import API, canonical, digest
    from .reconcile_agent_edge_trust import kubectl, strict_json
except ImportError:
    from publish_agent_edge_shadow import API, canonical, digest
    from reconcile_agent_edge_trust import kubectl, strict_json

MANAGER = "fugue-runtime-agent-identity"


def validate(config):
    if set(config) != {"apiVersion", "kind", "origin", "namespace", "secretName", "runtime"} or config["apiVersion"] != "identity.fugue.dev/v1" or config["kind"] != "RuntimeAgentIdentity":
        raise ValueError("invalid runtime identity declaration")
    if not re.fullmatch(r"https://[a-z0-9][a-z0-9.-]+[a-z0-9]", config["origin"]):
        raise ValueError("canonical HTTPS origin required")
    for key in ["namespace", "secretName"]:
        if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", config[key]):
            raise ValueError("canonical Kubernetes identity required")
    runtime = config["runtime"]
    if set(runtime) != {"name", "type", "endpoint", "labels"} or runtime["type"] != "external-owned" or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", runtime["name"]) or not isinstance(runtime["labels"], dict):
        raise ValueError("only a declared private external runtime is supported")
    return config


def validate_runtime(runtime, config):
    expected = config["runtime"]
    if runtime.get("name") != expected["name"] or runtime.get("type") != expected["type"] or runtime.get("endpoint", "") != expected["endpoint"] or runtime.get("access_mode") != "private" or runtime.get("pool_mode") != "dedicated" or not re.fullmatch(r"runtime_[a-zA-Z0-9_]+", runtime.get("id", "")):
        raise ValueError("runtime identity differs from its private declaration")
    for key, value in expected["labels"].items():
        if runtime.get("labels", {}).get(key) != value:
            raise ValueError("runtime identity labels drifted")


def provision(config, api, kube=kubectl):
    name, namespace = config["secretName"], config["namespace"]
    raw = kube(["get", "secret", name, "-n", namespace, "--ignore-not-found", "-o", "json"])
    if raw.strip():
        existing = strict_json(raw)
        meta, data = existing.get("metadata", {}), existing.get("data", {})
        if meta.get("name") != name or meta.get("namespace") != namespace or meta.get("deletionTimestamp") or meta.get("labels", {}).get("app.kubernetes.io/managed-by") != MANAGER or meta.get("annotations", {}).get("identity.fugue.dev/declaration-digest") != digest(config) or existing.get("immutable") is not True or set(data) != {"runtime_id", "runtime_key"}:
            raise ValueError("existing runtime credential resource is foreign or changed")
        runtime_id = base64.b64decode(data["runtime_id"], validate=True).decode()
        runtime_key = base64.b64decode(data["runtime_key"], validate=True).decode()
        if not re.fullmatch(r"runtime_[a-zA-Z0-9_]+", runtime_id) or not runtime_key.startswith("fugue_rt"):
            raise ValueError("existing runtime credentials malformed")
        runtime = api("GET", "/v1/runtimes/" + runtime_id)["runtime"]
        validate_runtime(runtime, config)
        return runtime_id
    existing = api("GET", "/v1/runtimes").get("runtimes", [])
    if any(r.get("name", "").lower() == config["runtime"]["name"].lower() for r in existing):
        raise ValueError("runtime exists without owned credentials; refusing automatic key rotation")
    template = {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name, "namespace": namespace, "labels": {"app.kubernetes.io/managed-by": MANAGER}, "annotations": {"identity.fugue.dev/declaration-digest": digest(config)}}, "type": "Opaque", "immutable": True, "data": {"runtime_id": base64.b64encode(b"preflight").decode(), "runtime_key": base64.b64encode(b"preflight").decode()}}
    kube(["create", "--dry-run=server", "-f", "-"], template)
    result = api("POST", "/v1/runtimes", config["runtime"])
    runtime, key = result["runtime"], result["runtime_key"]
    validate_runtime(runtime, config)
    if not isinstance(key, str) or not key.startswith("fugue_rt"):
        raise ValueError("runtime API did not return a valid secret")
    template["data"] = {"runtime_id": base64.b64encode(runtime["id"].encode()).decode(), "runtime_key": base64.b64encode(key.encode()).decode()}
    kube(["create", "-f", "-"], template)
    persisted = strict_json(kube(["get", "secret", name, "-n", namespace, "-o", "json"]))
    if persisted.get("data") != template["data"] or persisted.get("immutable") is not True:
        raise ValueError("runtime credential persistence did not verify")
    return runtime["id"]


def main():
    if len(sys.argv) != 2:
        raise ValueError("one runtime identity declaration required")
    raw = Path(sys.argv[1]).read_bytes()
    if len(raw) > 65536:
        raise ValueError("identity declaration exceeds bound")
    config = validate(strict_json(raw))
    token = os.environ.pop("FUGUE_API_KEY", "")
    if not token:
        raise ValueError("identity writer credential missing")
    runtime_id = provision(config, API(config["origin"], token))
    print(canonical({"verified": True, "runtime_id": runtime_id, "private": True}))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit("Runtime identity provisioning failed: " + type(error).__name__ + "; no credential data emitted") from None
