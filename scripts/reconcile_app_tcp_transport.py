#!/usr/bin/env python3
"""Git-owned TCP listeners for exact managed app identities; no Pod mutation.

Only the production workflow invokes apply. Tenant app port declarations alone
never grant a public socket. Existing HTTP/SSH/DNS transports remain separate.
"""
import argparse
import copy
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import subprocess
import tempfile

try:
    from .observe_front_candidate import read
    from .reconcile_front_probe_transport import check_listener_collision
except ImportError:
    from observe_front_candidate import read
    from reconcile_front_probe_transport import check_listener_collision

MANAGER = "fugue-app-tcp-transport"
GENERATION = "transport.fugue.dev/generation"
DIGEST = "transport.fugue.dev/digest"
APP = "fugue.pro/app-id"
TENANT = "fugue.pro/tenant-id"
WORKLOAD = "fugue.pro/app-workload"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value):
    return "sha256:" + hashlib.sha256(canonical(value).encode()).hexdigest()


def validate(config):
    if set(config) != {"schema", "generation", "listeners"} or config["schema"] != "fugue.app-tcp-transport/v1" or type(config["generation"]) is not int or config["generation"] < 1:
        raise ValueError("invalid TCP transport declaration")
    if not isinstance(config["listeners"], list) or len(config["listeners"]) > 32:
        raise ValueError("bounded listener list required")
    sockets, names = set(), set()
    for item in config["listeners"]:
        if set(item) != {"name", "namespace", "appId", "tenantId", "managedName", "managedUID", "deployment", "node", "address", "port", "targetPort", "enabled"}:
            raise ValueError("explicit listener identity required")
        for key in ["name", "namespace", "managedName", "deployment", "node"]:
            if not isinstance(item[key], str) or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", item[key]):
                raise ValueError("noncanonical Kubernetes identity")
        for key in ["appId", "tenantId", "managedUID"]:
            if not isinstance(item[key], str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,160}", item[key]):
                raise ValueError("exact ownership identity required")
        address = ipaddress.ip_address(item["address"])
        if address.version != 4 or not address.is_global or str(address) != item["address"] or type(item["enabled"]) is not bool:
            raise ValueError("explicit public IPv4 and activation required")
        for key in ["port", "targetPort"]:
            if type(item[key]) is not int or not 1 <= item[key] <= 65535:
                raise ValueError("invalid TCP port")
        # Reserved platform ingress is owned by separate declarations.
        if item["port"] in {22, 53, 80, 443, 2379, 2380, 6443, 10250}:
            raise ValueError("reserved platform socket")
        socket = (item["address"], item["port"])
        name = (item["namespace"], item["name"])
        if socket in sockets or name in names:
            raise ValueError("duplicate TCP socket or Service identity")
        sockets.add(socket)
        names.add(name)
    return config


def observe(item):
    ns = item["namespace"]
    managed = read("get", "managedapps.fugue.pro", item["managedName"], "-n", ns, "-o", "json")
    meta = managed["metadata"]
    if meta.get("uid") != item["managedUID"] or meta.get("deletionTimestamp") or meta.get("labels", {}).get(APP) != item["appId"] or meta.get("labels", {}).get(TENANT) != item["tenantId"]:
        raise ValueError("managed application ownership changed")
    owner = {"apiVersion": "fugue.pro/v1alpha1", "kind": "ManagedApp", "name": item["managedName"], "uid": item["managedUID"], "controller": False, "blockOwnerDeletion": True}
    if not item["enabled"]:
        return owner
    node = read("get", "node", item["node"], "-o", "json")
    if node["metadata"].get("deletionTimestamp") or not node["metadata"].get("uid") or item["address"] not in [a["address"] for a in node.get("status", {}).get("addresses", [])] or not any(c.get("type") == "Ready" and c.get("status") == "True" for c in node.get("status", {}).get("conditions", [])):
        raise ValueError("listener node address or readiness changed")
    deployment = read("get", "deployment", item["deployment"], "-n", ns, "-o", "json")
    selector = {APP: item["appId"], TENANT: item["tenantId"], WORKLOAD: item["deployment"]}
    labels = deployment["spec"]["template"]["metadata"].get("labels", {})
    if deployment["metadata"].get("deletionTimestamp") or any(labels.get(k) != v for k, v in selector.items()) or not any(x.get("uid") == item["managedUID"] for x in deployment["metadata"].get("ownerReferences", [])):
        raise ValueError("TCP workload ownership changed")
    spec, status = deployment["spec"], deployment.get("status", {})
    desired = spec.get("replicas", 1)
    if desired < 1 or status.get("observedGeneration", 0) < deployment["metadata"].get("generation", 1) or any(status.get(k, 0) != desired for k in ["updatedReplicas", "readyReplicas", "availableReplicas"]):
        raise ValueError("TCP workload current cohort is not ready")
    if item["targetPort"] not in [p.get("containerPort") for c in spec["template"]["spec"]["containers"] for p in c.get("ports", []) if p.get("protocol", "TCP") == "TCP"]:
        raise ValueError("target port is not declared by the application")
    query = ",".join(k + "=" + v for k, v in sorted(selector.items()))
    pods = read("get", "pods", "-n", ns, "-l", query, "-o", "json")
    if pods.get("metadata", {}).get("continue"):
        raise ValueError("incomplete TCP endpoint observation")
    active = [p for p in pods["items"] if not p["metadata"].get("deletionTimestamp")]
    local = [p for p in active if p["spec"].get("nodeName") == item["node"]]
    if not local or any(p.get("status", {}).get("phase") != "Running" or not any(c.get("type") == "Ready" and c.get("status") == "True" for c in p.get("status", {}).get("conditions", [])) for p in local):
        raise ValueError("no ready node-local TCP endpoint")
    services = read("get", "services", "-A", "-o", "json")
    if services.get("metadata", {}).get("continue"):
        raise ValueError("incomplete public socket ownership observation")
    for service in services["items"]:
        if (service["metadata"]["namespace"], service["metadata"]["name"]) == (ns, item["name"]):
            continue
        for port in service.get("spec", {}).get("ports", []):
            if port.get("protocol", "TCP") == "TCP" and (port.get("nodePort") == item["port"] or item["address"] in service.get("spec", {}).get("externalIPs", []) and port.get("port") == item["port"]):
                raise ValueError("public socket is already owned")
    # Also inspect hostPort/hostNetwork declarations and a live node-network
    # socket witness; a Service list cannot see an existing host daemon.
    check_listener_collision({"namespace": ns}, item, {"node": item["node"]})
    return owner


def targets(config, item, owner):
    selector = {APP: item["appId"], TENANT: item["tenantId"], WORKLOAD: item["deployment"]}
    service_spec = {"type": "ClusterIP", "selector": selector, "ports": [{"name": "tcp", "protocol": "TCP", "port": item["port"], "targetPort": item["targetPort"]}], "externalIPs": [item["address"]] if item["enabled"] else [], "publishNotReadyAddresses": False}
    if item["enabled"]:
        service_spec["externalTrafficPolicy"] = "Local"
    policy_spec = {"podSelector": {"matchLabels": selector}, "policyTypes": ["Ingress"], "ingress": [{"from": [{"ipBlock": {"cidr": "0.0.0.0/0"}}], "ports": [{"protocol": "TCP", "port": item["targetPort"]}]}] if item["enabled"] else []}
    results = []
    for kind, spec in [("Service", service_spec), ("NetworkPolicy", policy_spec)]:
        results.append({"apiVersion": "v1" if kind == "Service" else "networking.k8s.io/v1", "kind": kind, "metadata": {"name": item["name"], "namespace": item["namespace"], "labels": {"app.kubernetes.io/managed-by": MANAGER, APP: item["appId"]}, "annotations": {GENERATION: str(config["generation"]), DIGEST: digest({"owner": owner, "spec": spec})}, "ownerReferences": [owner]}, "spec": spec})
    return results


def controlled_spec(current, target):
    value = {k: current.get("spec", {}).get(k) for k in target["spec"]}
    if target["kind"] == "NetworkPolicy":
        value["ingress"] = current.get("spec", {}).get("ingress", [])
    if target["kind"] == "Service":
        # These fields remain controlled across activation/deactivation.
        for key, default in [("externalIPs", []), ("externalTrafficPolicy", None), ("publishNotReadyAddresses", False)]:
            value[key] = current.get("spec", {}).get(key, default)
        if value["externalTrafficPolicy"] is None:
            del value["externalTrafficPolicy"]
    return value


def mutation(current, target):
    if current is None:
        return ["create", "-f", "-", "--field-manager=" + MANAGER], target
    meta = current["metadata"]
    if not meta.get("uid") or not meta.get("resourceVersion") or meta.get("deletionTimestamp") or meta.get("labels", {}).get("app.kubernetes.io/managed-by") != MANAGER or meta.get("labels", {}).get(APP) != target["metadata"]["labels"][APP] or meta.get("ownerReferences") != target["metadata"]["ownerReferences"]:
        raise ValueError("refusing foreign TCP resource")
    previous = controlled_spec(current, target)
    if digest({"owner": meta["ownerReferences"][0], "spec": previous}) != meta.get("annotations", {}).get(DIGEST):
        raise ValueError("TCP resource differs from its last declaration")
    generation = int(meta.get("annotations", {}).get(GENERATION, "0"))
    next_generation = int(target["metadata"]["annotations"][GENERATION])
    if next_generation < generation or next_generation == generation and meta["annotations"][DIGEST] != target["metadata"]["annotations"][DIGEST]:
        raise ValueError("TCP declaration generation replay")
    if meta["annotations"][DIGEST] == target["metadata"]["annotations"][DIGEST] and next_generation == generation:
        return None, None
    patch = [{"op": "test", "path": "/metadata/" + key, "value": meta[key]} for key in ["uid", "resourceVersion", "ownerReferences", "annotations", "labels"]]
    patch.append({"op": "test", "path": "/spec", "value": current["spec"]})
    desired_spec = copy.deepcopy(current["spec"])
    if target["kind"] == "Service":
        desired_spec.pop("externalTrafficPolicy", None)
    desired_spec.update(target["spec"])
    patch.append({"op": "replace", "path": "/spec", "value": desired_spec})
    for key, value in target["metadata"]["annotations"].items():
        patch.append({"op": "add", "path": "/metadata/annotations/" + key.replace("/", "~1"), "value": value})
    return ["patch", target["kind"], meta["name"], "-n", meta["namespace"], "--type=json", "--patch-file", "-", "--field-manager=" + MANAGER], patch


def write(command, body, dry_run=False):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", prefix="fugue-tcp-") as file:
        file.write(canonical(body))
        file.flush()
        args = [file.name if arg == "-" else arg for arg in command]
        result = subprocess.run(["kubectl", *args, *(["--dry-run=server"] if dry_run else []), "-o", "json"], capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise ValueError("TCP resource CAS/dry-run failed")
    return json.loads(result.stdout)


def current_resource(target):
    # A named --ignore-not-found read produces an empty stdout on first create.
    # A filtered list always has a JSON envelope, including the absent case.
    result = read("get", target["kind"], "-n", target["metadata"]["namespace"], "--field-selector=metadata.name=" + target["metadata"]["name"], "-o", "json")
    if result.get("metadata", {}).get("continue") or not isinstance(result.get("items"), list) or len(result["items"]) > 1:
        raise ValueError("incomplete TCP resource observation")
    return result["items"][0] if result["items"] else None


def reconcile(config, apply=False):
    pending, evidence = [], []
    # All declarations must pass observation and dry-run before any mutation.
    for item in config["listeners"]:
        owner = observe(item)
        for target in targets(config, item, owner):
            current = current_resource(target)
            command, body = mutation(current, target)
            if command:
                write(command, body, dry_run=True)
            pending.append((item, owner, current, target, command, body))
    for item, owner, current, target, command, body in pending:
        if apply and command:
            if observe(item) != owner:
                raise ValueError("application identity changed before TCP publication")
            fresh = current_resource(target)
            if fresh != current:
                raise ValueError("TCP resource changed after preflight")
            current = write(command, body)
        evidence.append({"namespace": item["namespace"], "name": item["name"], "kind": target["kind"], "enabled": item["enabled"], "intentDigest": target["metadata"]["annotations"][DIGEST], "changed": bool(command), "applied": apply, "uid": (current or {}).get("metadata", {}).get("uid")})
    return {"schema": "fugue.app-tcp-transport-receipt/v1", "configDigest": digest(config), "resources": evidence}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--evidence", required=True)
    args = parser.parse_args()
    config = validate(json.loads(Path(args.config).read_text()))
    result = reconcile(config, args.apply)
    Path(args.evidence).write_text(canonical(result) + "\n")


if __name__ == "__main__":
    main()
