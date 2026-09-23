#!/usr/bin/env python3
"""Reconcile explicitly declared DNS network listeners, without changing Pods.

Services supply node-local transport only. DNS records, answers, policy and
runtime probes remain owned by the current signed artifact and its consumers.
"""
import argparse
import hashlib
import ipaddress
import json
import re
import subprocess
from pathlib import Path

MANAGER = "fugue-dns-transport"
GENERATION = "transport.fugue.dev/generation"
DIGEST = "transport.fugue.dev/digest"


def load_config(path):
    config = json.loads(Path(path).read_text())
    if set(config) != {"apiVersion", "kind", "generation", "namespace", "listeners"}:
        raise ValueError("unknown or missing transport configuration fields")
    if config["apiVersion"] != "transport.fugue.dev/v1" or config["kind"] != "DNSLocalTransport":
        raise ValueError("invalid transport schema")
    if type(config["generation"]) is not int or config["generation"] <= 0:
        raise ValueError("positive integer generation is required")
    name_pattern = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    if not isinstance(config["namespace"], str) or not re.fullmatch(name_pattern, config["namespace"]):
        raise ValueError("canonical namespace is required")
    listeners = config["listeners"]
    if not isinstance(listeners, list) or not 1 <= len(listeners) <= 32:
        raise ValueError("bounded nonempty listeners are required")
    names, addresses = set(), set()
    for listener in listeners:
        if set(listener) != {"name", "address", "port", "targetPort", "selector"}:
            raise ValueError("unknown or missing listener fields")
        if not isinstance(listener["name"], str) or not re.fullmatch(name_pattern, listener["name"]) or listener["name"] in names:
            raise ValueError("listener name invalid or duplicated")
        names.add(listener["name"])
        address = ipaddress.ip_address(listener["address"])
        if str(address) != listener["address"] or not address.is_global:
            raise ValueError("listener requires canonical public address")
        for field in ["port", "targetPort"]:
            if type(listener[field]) is not int or not 1 <= listener[field] <= 65535:
                raise ValueError("listener port outside allowed range")
        key = (str(address), listener["port"])
        if key in addresses:
            raise ValueError("duplicated listener socket")
        addresses.add(key)
        selector = listener["selector"]
        if not isinstance(selector, dict) or not selector or len(selector) > 16 or any(not isinstance(k, str) or not isinstance(v, str) or not k or not v or any(c in k + v for c in "\n\r,= ") for k, v in selector.items()):
            raise ValueError("explicit bounded selector is required")
    return config


def service(config, listener):
    spec = {"type": "ClusterIP", "externalIPs": [listener["address"]],
            "externalTrafficPolicy": "Local", "internalTrafficPolicy": "Local",
            "publishNotReadyAddresses": False, "selector": listener["selector"],
            "ports": [{"name": "dns-" + protocol.lower(), "protocol": protocol,
                       "port": listener["port"], "targetPort": listener["targetPort"]} for protocol in ["UDP", "TCP"]]}
    digest = "sha256:" + hashlib.sha256(json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {"apiVersion": "v1", "kind": "Service",
            "metadata": {"name": listener["name"], "namespace": config["namespace"],
                         "labels": {"app.kubernetes.io/managed-by": MANAGER},
                         "annotations": {GENERATION: str(config["generation"]), DIGEST: digest}}, "spec": spec}


def validate_existing(current, desired):
    if not current:
        return
    metadata = current["metadata"]
    if metadata.get("deletionTimestamp") or metadata.get("labels", {}).get("app.kubernetes.io/managed-by") != MANAGER:
        raise ValueError("refusing to replace an unowned or terminating Service")
    generation = int(metadata.get("annotations", {}).get(GENERATION, "0"))
    target = int(desired["metadata"]["annotations"][GENERATION])
    if target < generation or target == generation and metadata.get("annotations", {}).get(DIGEST) != desired["metadata"]["annotations"][DIGEST]:
        raise ValueError("generation replay or same-generation content change")
    # A Git declaration cannot silently heal an unrelated live networking edit.
    observed = {key: current.get("spec", {}).get(key) for key in desired["spec"]}
    observed["publishNotReadyAddresses"] = bool(observed["publishNotReadyAddresses"])
    digest = "sha256:" + hashlib.sha256(json.dumps(observed, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if digest != metadata.get("annotations", {}).get(DIGEST):
        raise ValueError("current Service differs from its last transport declaration")


def kubectl(*args, body=None):
    result = subprocess.run(["kubectl", *args], input=body, capture_output=True, text=True, timeout=45)
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result.stdout


def reconcile(config, apply=False):
    planned = []
    inventory = json.loads(kubectl("get", "services", "--all-namespaces", "-o", "json"))["items"]
    for listener in config["listeners"]:
        desired = service(config, listener)
        for other in inventory:
            if other["metadata"]["name"] == listener["name"] and other["metadata"]["namespace"] == config["namespace"]:
                continue
            spec = other.get("spec", {})
            if listener["address"] in spec.get("externalIPs", []) and any(p.get("port") == listener["port"] for p in spec.get("ports", [])):
                raise ValueError("listener address/port belongs to another Service")
        existing = kubectl("get", "service", listener["name"], "-n", config["namespace"], "--ignore-not-found", "-o", "json")
        current = json.loads(existing) if existing.strip() else None
        validate_existing(current, desired)
        selector = ",".join(k + "=" + v for k, v in sorted(listener["selector"].items()))
        pods = json.loads(kubectl("get", "pods", "-n", config["namespace"], "-l", selector, "-o", "json"))["items"]
        ready = [pod for pod in pods if not pod["metadata"].get("deletionTimestamp") and any(c.get("type") == "Ready" and c.get("status") == "True" for c in pod.get("status", {}).get("conditions", []))]
        if not ready:
            raise ValueError("listener selector has no ready backend")
        nodes = {pod["spec"].get("nodeName") for pod in ready}
        if not all(isinstance(node, str) and node for node in nodes):
            raise ValueError("ready backend has no bound node")
        addresses = []
        for node in sorted(nodes):
            info = json.loads(kubectl("get", "node", node, "-o", "json"))
            addresses.extend(a["address"] for a in info.get("status", {}).get("addresses", []))
        if listener["address"] not in addresses:
            raise ValueError("public listener address has no ready node-local backend")
        if current:
            desired["metadata"]["resourceVersion"] = current["metadata"]["resourceVersion"]
        command = ["apply", "--server-side", "--field-manager=" + MANAGER] if current else ["create", "--field-manager=" + MANAGER]
        body = json.dumps(desired)
        kubectl(*command, "--dry-run=server", "-f", "-", body=body)
        planned.append((command, body, listener["name"]))
    if apply:
        for command, body, name in planned:
            kubectl(*command, "-f", "-", body=body)
            print("reconciled Service/" + name, flush=True)
    return len(planned)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print("listeners=" + str(reconcile(load_config(args.path), args.apply)))
