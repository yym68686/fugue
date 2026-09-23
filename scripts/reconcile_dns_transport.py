#!/usr/bin/env python3
"""Reconcile explicitly declared DNS network listeners, without changing Pods.

Services supply node-local transport only. DNS records, answers, policy and
runtime probes remain owned by the current signed artifact and its consumers.
"""
import argparse
import datetime
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
    for owner in metadata.get("managedFields", []):
        if owner.get("manager") == MANAGER:
            continue
        fields = owner.get("fieldsV1", {})
        owned_meta = fields.get("f:metadata", {})
        if fields.get("f:spec") or any("f:" + key in owned_meta.get("f:annotations", {}) for key in [GENERATION, DIGEST]) or "f:app.kubernetes.io/managed-by" in owned_meta.get("f:labels", {}):
            raise ValueError("transport fields have a foreign manager")


def update_patch(current, desired):
    validate_existing(current, desired)
    metadata = current["metadata"]
    if not metadata.get("uid") or not metadata.get("resourceVersion"):
        raise ValueError("Service UID/resourceVersion required for update")
    patch = [{"op": "test", "path": "/metadata/uid", "value": metadata["uid"]},
             {"op": "test", "path": "/metadata/resourceVersion", "value": metadata["resourceVersion"]},
             {"op": "test", "path": "/spec", "value": current["spec"]},
             {"op": "test", "path": "/metadata/annotations", "value": metadata["annotations"]},
             {"op": "test", "path": "/metadata/labels", "value": metadata["labels"]}]
    for key, value in desired["spec"].items():
        if current["spec"].get(key) != value:
            patch.append({"op": "add", "path": "/spec/" + key, "value": value})
    for key, value in desired["metadata"]["annotations"].items():
        if metadata["annotations"].get(key) != value:
            patch.append({"op": "add", "path": "/metadata/annotations/" + key.replace("~", "~0").replace("/", "~1"), "value": value})
    return patch


def kubectl(*args, body=None):
    result = subprocess.run(["kubectl", *args], input=body, capture_output=True, text=True, timeout=45)
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result.stdout


def read_json(*args):
    raw = kubectl(*args)
    if len(raw) > 8 << 20:
        raise ValueError("DNS transport observation exceeds limit")
    return json.loads(raw)


def ready_pod(pod):
    return (not pod.get("metadata", {}).get("deletionTimestamp") and
            pod.get("status", {}).get("phase") == "Running" and
            any(c.get("type") == "Ready" and c.get("status") == "True"
                for c in pod.get("status", {}).get("conditions", [])))


def handoff_backend(namespace, selector, node_name, target_port):
    query = ",".join(k + "=" + v for k, v in sorted(selector.items()))
    pods = read_json("get", "pods", "-n", namespace, "-l", query, "-o", "json")["items"]
    local = [p for p in pods if p.get("spec", {}).get("nodeName") == node_name]
    if len(local) != 1 or not ready_pod(local[0]):
        raise ValueError("DNS handoff requires exactly one live Ready backend per selector on the listener node")
    pod = local[0]
    meta, spec = pod["metadata"], pod["spec"]
    if not meta.get("uid") or not meta.get("resourceVersion") or meta.get("namespace") != namespace or not spec.get("serviceAccountName"):
        raise ValueError("DNS handoff backend identity is incomplete")
    policy = json.loads(meta.get("annotations", {}).get("fugue.pro/consumer-identity", "{}"))
    if policy != {"version": "v1", "component": "dns-server", "scope_key": "global", "artifact_kinds": ["dns_answer_bundle"]}:
        raise ValueError("DNS handoff backend has no typed consumer authorization")
    owners = [c for c in spec["containers"] if all(sum(p.get("containerPort") == target_port and p.get("protocol", "TCP") == protocol for p in c.get("ports", [])) == 1 for protocol in ["UDP", "TCP"])]
    if len(owners) != 1:
        raise ValueError("DNS handoff listener does not have a unique container owner")
    container = owners[0]
    probe = container.get("readinessProbe", {}).get("httpGet", {})
    port = probe.get("port")
    if isinstance(port, str):
        ports = [p["containerPort"] for p in container.get("ports", []) if p.get("name") == port and p.get("protocol", "TCP") == "TCP"]
        port = ports[0] if len(ports) == 1 else None
    if type(port) is not int or not 1 <= port <= 65535 or probe.get("host") or probe.get("scheme", "HTTP") != "HTTP":
        raise ValueError("DNS handoff observation port is invalid")
    path = "/api/v1/namespaces/" + namespace + "/pods/" + meta["name"] + ":" + str(port) + "/proxy/runtime-facts"
    snapshot = read_json("get", "--raw", path)
    current = read_json("get", "pod", meta["name"], "-n", namespace, "-o", "json")
    if current["metadata"].get("uid") != meta["uid"] or current["metadata"].get("resourceVersion") != meta["resourceVersion"]:
        raise ValueError("DNS handoff backend changed while reading runtime facts")
    return {"pod": pod, "snapshot": snapshot}


def validate_handoff_snapshots(old, candidate, node_name, now=None):
    now = now or datetime.datetime.now(datetime.timezone.utc)
    def timestamp(value):
        parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("DNS handoff timestamp has no timezone")
        return parsed
    def digest(value):
        return isinstance(value, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", value)
    for snapshot in [old, candidate]:
        assignment = snapshot.get("assignment", {})
        if (snapshot.get("schema") != "fugue.dns.runtime-facts/v1" or snapshot.get("node_id") != node_name or
                not snapshot.get("edge_group_id") or snapshot.get("ready") is not True or
                assignment.get("artifact_kind") != "dns_answer_bundle" or assignment.get("scope_key") != "global" or
                assignment.get("release_channel") not in ["gray", "full"] or
                any(not assignment.get(k) for k in ["artifact_id", "artifact_release_id", "expected_consumer_set_id", "release_set_id", "expected_generation", "fencing_token", "generation_sequence"]) or
                not all(digest(v) for v in [assignment.get("content_hash"), snapshot.get("parent_digest"), snapshot.get("plan_digest")])):
            raise ValueError("DNS handoff runtime snapshot is not ready and assignment-bound")
        observed, evaluated, until = [timestamp(snapshot[k]) for k in ["observed_at", "evaluated_at", "checkpoint_valid_until"]]
        if not observed <= evaluated <= now < until:
            raise ValueError("DNS handoff runtime snapshot has expired or future observations")
        facts, seen = snapshot.get("facts"), set()
        if not isinstance(facts, list) or len(facts) > 4096:
            raise ValueError("DNS handoff proof set is invalid")
        for fact in facts:
            identity, proof = fact.get("probe_id"), fact.get("proof", {})
            binding = proof.get("traffic_release", {})
            if not digest(identity) or identity in seen or fact.get("ready") is not True:
                raise ValueError("DNS handoff requires unique positive proofs")
            seen.add(identity)
            if not timestamp(proof["checked_at"]) <= evaluated <= now < timestamp(proof["valid_until"]):
                raise ValueError("DNS handoff proof is expired or from the future")
            if any(binding.get(k) != v for k,v in {
                "release_set_id": assignment["release_set_id"], "release_set_digest": snapshot["parent_digest"],
                "route_artifact_id": snapshot["route_artifact_id"], "release_id": assignment["artifact_release_id"],
                "release_channel": assignment["release_channel"], "fencing_token": assignment["fencing_token"], "scope_key": assignment["scope_key"]}.items()):
                raise ValueError("DNS handoff proof belongs to another release")
    for field in ["node_id", "edge_group_id", "assignment", "parent_digest", "route_artifact_id", "plan_digest"]:
        if old.get(field) != candidate.get(field):
            raise ValueError("DNS handoff backends have different artifact assignments")
    if {f["probe_id"] for f in old["facts"]} != {f["probe_id"] for f in candidate["facts"]}:
        raise ValueError("DNS handoff backends have different probe membership")


def handoff_witness(config, listener, current):
    if not current or current["spec"]["selector"] == listener["selector"]:
        return None
    namespace = config["namespace"]
    nodes = read_json("get", "nodes", "-o", "json")["items"]
    owners = [n for n in nodes if listener["address"] in [a["address"] for a in n.get("status", {}).get("addresses", [])]]
    if len(owners) != 1 or not owners[0]["metadata"].get("uid") or owners[0]["metadata"].get("deletionTimestamp"):
        raise ValueError("DNS handoff listener must belong to one live node")
    node = owners[0]["metadata"]["name"]
    old = handoff_backend(namespace, current["spec"]["selector"], node, listener["targetPort"])
    candidate = handoff_backend(namespace, listener["selector"], node, listener["targetPort"])
    if old["pod"]["metadata"]["uid"] == candidate["pod"]["metadata"]["uid"]:
        raise ValueError("DNS handoff selectors do not isolate different instances")
    slices = read_json("get", "endpointslices", "-n", namespace, "-l",
                       "kubernetes.io/service-name=" + current["metadata"]["name"], "-o", "json")["items"]
    validate_handoff_endpoints(current, old["pod"], slices, listener["targetPort"])
    validate_handoff_snapshots(old["snapshot"], candidate["snapshot"], node)
    return {"node_uid": owners[0]["metadata"]["uid"], "old_uid": old["pod"]["metadata"]["uid"],
            "candidate_uid": candidate["pod"]["metadata"]["uid"],
            "assignment": candidate["snapshot"]["assignment"], "parent_digest": candidate["snapshot"]["parent_digest"],
            "plan_digest": candidate["snapshot"]["plan_digest"]}


def validate_handoff_endpoints(service, pod, slices, target_port):
    local = []
    ips = {i["ip"] for i in pod.get("status", {}).get("podIPs", [])}
    if not ips:
        raise ValueError("DNS handoff old backend has no Pod IP")
    for item in slices:
        metadata = item.get("metadata", {})
        owners = metadata.get("ownerReferences", [])
        if (metadata.get("deletionTimestamp") or metadata.get("namespace") != pod["metadata"]["namespace"] or
                not any(o.get("kind") == "Service" and o.get("uid") == service["metadata"]["uid"] and
                        o.get("name") == service["metadata"]["name"] and o.get("controller") is True for o in owners)):
            raise ValueError("DNS handoff EndpointSlice does not belong to the current Service")
        ports = item.get("ports", [])
        if len(ports) != 2 or {(p.get("protocol"), p.get("port")) for p in ports} != {("UDP", target_port), ("TCP", target_port)}:
            raise ValueError("DNS handoff EndpointSlice ports do not match the listener")
        for endpoint in item.get("endpoints", []):
            if endpoint.get("nodeName") != pod["spec"]["nodeName"]:
                continue
            ref, conditions = endpoint.get("targetRef", {}), endpoint.get("conditions", {})
            if (ref.get("kind") != "Pod" or ref.get("uid") != pod["metadata"]["uid"] or
                    ref.get("name") != pod["metadata"]["name"] or ref.get("namespace") != pod["metadata"]["namespace"] or
                    conditions.get("ready") is not True or conditions.get("terminating") is True or
                    not endpoint.get("addresses") or not set(endpoint["addresses"]).issubset(ips)):
                raise ValueError("DNS handoff current local endpoint is stale, ambiguous or unready")
            local.extend(endpoint["addresses"])
    if not local or len(local) != len(set(local)):
        raise ValueError("DNS handoff current endpoint is missing or duplicated")


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
        witness = handoff_witness(config, listener, current)
        if current:
            command = ["patch", "service", listener["name"], "-n", config["namespace"], "--type=json", "--field-manager=" + MANAGER, "--patch-file=/dev/stdin"]
            body = json.dumps(update_patch(current, desired))
        else:
            command = ["create", "--field-manager=" + MANAGER, "-f", "-"]
            body = json.dumps(desired)
        kubectl(*command, "--dry-run=server", body=body)
        planned.append((command, body, listener, current, witness))
    if apply:
        for command, body, listener, current, witness in planned:
            if witness is not None and handoff_witness(config, listener, current) != witness:
                raise ValueError("DNS handoff identity or assignment changed before selector CAS")
            kubectl(*command, body=body)
            print("reconciled Service/" + listener["name"], flush=True)
    return len(planned)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print("listeners=" + str(reconcile(load_config(args.path), args.apply)))
