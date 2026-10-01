#!/usr/bin/env python3
"""Bind DNS HTTPS probe egress to exact declared public Front Services.

NetworkPolicy sees the backend Pod after public externalIP destination NAT.
This independent configuration writer permits only TCP 443 to the selectors
of pinned, currently public Services. It never selects traffic or changes Pods.
"""
import argparse
import ipaddress
import json
from pathlib import Path
import re
import subprocess

from scripts.bootstrap_cell_producer import digest
from scripts.publish_agent_edge_shadow import canonical

MANAGER = "fugue-dns-probe-egress"
FRONT_MANAGER = "fugue-front-serving-transport"
GENERATION = "policy.fugue.dev/generation"
DIGEST = "policy.fugue.dev/digest"
NAME = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
HASH = re.compile(r"sha256:[0-9a-f]{64}")


def validate(c):
    if set(c) != {"schema", "generation", "namespace", "policy_name", "authority_cell_id", "services"} or c["schema"] != "fugue.dns-probe-egress/v1" or type(c["generation"]) is not int or c["generation"] < 1:
        raise ValueError("explicit versioned DNS probe egress intent required")
    if any(not isinstance(c[k], str) or len(c[k]) > 63 or not NAME.fullmatch(c[k]) for k in ["namespace", "policy_name", "authority_cell_id"]) or not c["authority_cell_id"].startswith("cell-"):
        raise ValueError("exact namespace, policy and neutral DNS authority required")
    refs = c["services"]
    if not isinstance(refs, list) or not 1 <= len(refs) <= 16:
        raise ValueError("bounded nonempty public Front set required")
    names = []
    for ref in refs:
        if set(ref) != {"name", "uid", "generation", "digest"} or not NAME.fullmatch(ref.get("name", "")) or not re.fullmatch(r"[a-zA-Z0-9-]{1,128}", ref.get("uid", "")) or type(ref["generation"]) is not int or ref["generation"] < 1 or not HASH.fullmatch(ref.get("digest", "")):
            raise ValueError("public Service UID, generation and spec digest required")
        names.append(ref["name"])
    if names != sorted(set(names)):
        raise ValueError("public Service references must be sorted and unique")
    return c


def public_peer(c, ref, obj):
    meta, spec = obj.get("metadata", {}), obj.get("spec", {})
    annotations = meta.get("annotations", {})
    if meta.get("name") != ref["name"] or meta.get("namespace") != c["namespace"] or meta.get("uid") != ref["uid"] or not meta.get("resourceVersion") or meta.get("deletionTimestamp") or meta.get("labels", {}).get("app.kubernetes.io/managed-by") != FRONT_MANAGER or annotations.get("transport.fugue.dev/phase") != "serving" or annotations.get("transport.fugue.dev/generation") != str(ref["generation"]) or annotations.get("transport.fugue.dev/digest") != ref["digest"]:
        raise ValueError("public Front Service identity or declared authority differs")
    ports = spec.get("ports", [])
    if len(ports) != 2 or any(p.get("protocol", "TCP") != "TCP" or type(p.get("port")) is not int or type(p.get("targetPort")) is not int or p["targetPort"] != p["port"] or p.get("nodePort") for p in ports) or {p["port"] for p in ports} != {80, 443}:
        raise ValueError("only declared public HTTP/HTTPS ports are eligible")
    addresses = spec.get("externalIPs", [])
    if spec.get("type") != "ClusterIP" or spec.get("externalName") or spec.get("loadBalancerIP") or spec.get("externalTrafficPolicy") != "Local" or spec.get("publishNotReadyAddresses", False) or not 1 <= len(addresses) <= 16:
        raise ValueError("public Front transport is not active or isolated")
    if len(set(addresses)) != len(addresses) or any(not ipaddress.ip_address(a).is_global or ipaddress.ip_address(a).is_multicast or str(ipaddress.ip_address(a)) != a for a in addresses):
        raise ValueError("canonical public listener addresses required")
    selector = spec.get("selector")
    if not isinstance(selector, dict) or not 1 <= len(selector) <= 16 or any(not isinstance(k, str) or not k or not isinstance(v, str) or not v for k, v in selector.items()):
        raise ValueError("an exact nonempty public backend selector is required")
    logical = {k: spec.get(k) for k in ["type", "selector", "ports", "externalIPs", "externalTrafficPolicy", "publishNotReadyAddresses"]}
    logical["publishNotReadyAddresses"] = bool(logical["publishNotReadyAddresses"])
    if digest(logical) != ref["digest"]:
        raise ValueError("public Service spec differs from the pinned intent")
    return {"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": c["namespace"]}}, "podSelector": {"matchLabels": selector}}


def desired(c, services):
    if len(services) != len(c["services"]):
        raise ValueError("public Service observation is incomplete")
    peers = [public_peer(c, ref, obj) for ref, obj in zip(c["services"], services)]
    spec = {"podSelector": {"matchLabels": {"app.kubernetes.io/component": "dns-server", "fugue.io/authority-cell-id": c["authority_cell_id"]}}, "policyTypes": ["Egress"], "egress": [{"ports": [{"protocol": "TCP", "port": 443}], "to": peers}]}
    return {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": {"namespace": c["namespace"], "name": c["policy_name"], "labels": {"app.kubernetes.io/managed-by": MANAGER, "fugue.io/authority-cell-id": c["authority_cell_id"]}, "annotations": {GENERATION: str(c["generation"]), DIGEST: digest(spec), "policy.fugue.dev/declaration-digest": digest(c)}}, "spec": spec}


def mutation(current, target):
    if current is None:
        return "create", target
    m, t = current.get("metadata", {}), target["metadata"]
    if m.get("namespace") != t["namespace"] or m.get("name") != t["name"] or m.get("labels") != t["labels"] or not m.get("uid") or not m.get("resourceVersion") or m.get("deletionTimestamp") or m.get("ownerReferences"):
        raise ValueError("DNS egress policy has a foreign or incomplete owner")
    annotations = m.get("annotations", {})
    if digest(current.get("spec")) != annotations.get(DIGEST):
        raise ValueError("DNS egress policy has unreviewed drift")
    for owner in m.get("managedFields", []):
        if owner.get("manager") != MANAGER and owner.get("fieldsV1", {}).get("f:spec"):
            raise ValueError("DNS egress policy has another spec writer")
    generation = int(annotations.get(GENERATION, "0"))
    if generation < 1 or generation > int(t["annotations"][GENERATION]) or generation == int(t["annotations"][GENERATION]) and (current["spec"] != target["spec"] or annotations != t["annotations"]):
        raise ValueError("DNS egress policy generation replay or mutation")
    if current["spec"] == target["spec"] and annotations == t["annotations"]:
        return None, None
    return "patch", [{"op": "test", "path": "/metadata/uid", "value": m["uid"]}, {"op": "test", "path": "/metadata/resourceVersion", "value": m["resourceVersion"]}, {"op": "test", "path": "/metadata/labels", "value": m["labels"]}, {"op": "test", "path": "/metadata/annotations", "value": annotations}, {"op": "test", "path": "/spec", "value": current["spec"]}, {"op": "replace", "path": "/spec", "value": target["spec"]}, {"op": "replace", "path": "/metadata/annotations", "value": t["annotations"]}]


def kubectl(*args, body=None):
    r = subprocess.run(["kubectl", *args], input=body, text=True, capture_output=True, timeout=30)
    if r.returncode:
        raise RuntimeError(r.stderr[-4096:])
    return json.loads(r.stdout) if r.stdout.strip() else None


def reconcile(c, apply, save, kube=kubectl):
    validate(c)
    read_sources = lambda: [kube("get", "service", ref["name"], "-n", c["namespace"], "-o", "json") for ref in c["services"]]
    sources = read_sources()
    target = desired(c, sources)
    current = kube("get", "networkpolicy", c["policy_name"], "-n", c["namespace"], "-o", "json", "--ignore-not-found")
    action, body = mutation(current, target)
    witness = {"schema": "fugue.dns-probe-egress-result/v1", "declaration_digest": digest(c), "policy_digest": target["metadata"]["annotations"][DIGEST], "public_transport_changed": False, "sources": [{"name": s["metadata"]["name"], "uid": s["metadata"]["uid"], "resource_version": s["metadata"]["resourceVersion"]} for s in sources], "action": action, "applied": False}
    save(witness)
    if action:
        fresh = read_sources()
        if desired(c, fresh) != target or any((a["metadata"]["uid"], a["metadata"]["resourceVersion"]) != (b["metadata"]["uid"], b["metadata"]["resourceVersion"]) for a, b in zip(sources, fresh)):
            raise ValueError("public Service changed during DNS egress planning")
        args = ["create", "--field-manager=" + MANAGER, "-f", "-", "-o", "json"] if action == "create" else ["patch", "networkpolicy", c["policy_name"], "-n", c["namespace"], "--field-manager=" + MANAGER, "--type=json", "--patch-file", "-", "-o", "json"]
        kube(*args, "--dry-run=server", body=canonical(body))
        if apply:
            kube(*args, body=canonical(body))
    if apply:
        fresh = read_sources()
        if desired(c, fresh) != target or any((a["metadata"]["uid"], a["metadata"]["resourceVersion"]) != (b["metadata"]["uid"], b["metadata"]["resourceVersion"]) for a, b in zip(sources, fresh)):
            raise ValueError("public Service changed before DNS egress acceptance")
        live = kube("get", "networkpolicy", c["policy_name"], "-n", c["namespace"], "-o", "json")
        if mutation(live, target)[0] is not None:
            raise ValueError("DNS egress policy did not converge")
        witness["applied"] = True
    save(witness)
    return witness


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("config")
    p.add_argument("--validate-only", action="store_true")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--evidence")
    args = p.parse_args()
    raw = Path(args.config).read_bytes()
    if len(raw) > 65536: raise ValueError("DNS egress configuration exceeds bound")
    c = validate(json.loads(raw))
    if args.validate_only:
        print(canonical({"valid": True, "declaration_digest": digest(c)}))
        return
    if not args.evidence: raise ValueError("retained DNS egress evidence is required")
    save = lambda data: Path(args.evidence).write_text(canonical(data) + "\n")
    print(canonical(reconcile(c, args.apply, save)))


if __name__ == "__main__":
    main()
