#!/usr/bin/env python3
"""Verify and deliver an already signed production pilot lease.

The protected Actions job can run without an envelope to record a witness.
It never issues leases, renews them, changes Kubernetes objects, or sends a
business payload. Delivery uses the exact retained Worker state mount.
"""

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import sys
import time

from scripts import reconcile_front_probe_transport as transport
from scripts import stage_edge_origin_relay_trust as trust
from scripts import stage_front_serving_transport as serving


SHA = re.compile(r"[0-9a-f]{40}")
HEX256 = re.compile(r"[0-9a-f]{64}")
HOST = re.compile(r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
PATH = re.compile(r"/[a-zA-Z0-9_/-]{1,255}")
PORT = 8443


def validate_declaration(d, t):
    keys = {"schema", "worker_config_sha", "monitor", "front", "front_uid", "host", "path",
            "source", "peer", "public_ips", "mode", "generation", "ca_sha256"}
    if not isinstance(d, dict):
        raise ValueError("invalid pilot declaration")
    schema = d.get("schema")
    service_bound = schema == "fugue.edge.origin-relay-policy-release/v2"
    if service_bound:
        keys = keys | {"serving_fronts"}
    if schema not in ("fugue.edge.origin-relay-policy-release/v1", "fugue.edge.origin-relay-policy-release/v2") or set(d) not in (keys, keys | {"withdrawal_of"}):
        raise ValueError("invalid pilot declaration")
    if not (trust.matches(SHA, d["worker_config_sha"]) and trust.matches(trust.DNS_LABEL, d["monitor"])
            and trust.matches(trust.DNS_LABEL, d["front"]) and trust.matches(trust.UID, d["front_uid"])
            and trust.matches(HOST, d["host"]) and len(d["host"]) <= 253
            and trust.matches(PATH, d["path"]) and "//" not in d["path"]
            and d["mode"] in ("off", "shadow", "active") and type(d["generation"]) is int
            and 0 < d["generation"] < 2 ** 53 and d["ca_sha256"] == t["ca_sha256"]):
        raise ValueError("invalid exact rollout scope")
    edges = {p["edge"] for p in t["targets"]}
    if (not all(trust.matches(trust.DNS_LABEL, d[x]) for x in ("source", "peer"))
            or d["source"] == d["peer"] or {d["source"], d["peer"]} != edges
            or not isinstance(d["public_ips"], dict) or set(d["public_ips"]) != edges
            or any(not isinstance(x, str) or not ipaddress.IPv4Address(x).is_global for x in d["public_ips"].values())
            or len(set(d["public_ips"].values())) != 2):
        raise ValueError("pilot must use exactly the two independently pinned front addresses")
    if service_bound:
        fronts = d["serving_fronts"]
        if not isinstance(fronts, dict) or set(fronts) != edges:
            raise ValueError("each pilot edge requires its own serving Front Service")
        for pin in fronts.values():
            if (not isinstance(pin, dict) or set(pin) != {"service", "uid", "generation"}
                    or not trust.matches(trust.DNS_LABEL, pin["service"])
                    or not trust.matches(trust.UID, pin["uid"])
                    or type(pin["generation"]) is not int or pin["generation"] <= 0):
                raise ValueError("invalid serving Front Service pin")
        if any(len({pin[key] for pin in fronts.values()}) != 2 for key in ("service", "uid")):
            raise ValueError("pilot edges must pin different serving Front Services")
    if "withdrawal_of" in d:
        prior = d["withdrawal_of"]
        if (d["mode"] != "off" or not isinstance(prior, dict) or set(prior) != {"generation", "payload_sha256", "slots"}
                or type(prior["generation"]) is not int or not 0 < prior["generation"] < d["generation"]
                or not trust.matches(HEX256, prior["payload_sha256"]) or not isinstance(prior["slots"], dict)
                or set(prior["slots"]) != edges or any(slot not in ("a", "b") for slot in prior["slots"].values())
                or not all(any((edge, slot) == (p["edge"], p["slot"]) for p in t["targets"])
                           for edge, slot in prior["slots"].items())):
            raise ValueError("withdrawal requires a signed off and a pinned prior fence for each enrolled slot")
    return d


def proof_headers(raw):
    lines = raw.decode("ascii").splitlines()
    if not lines or not re.fullmatch(r"HTTP/[0-9.]+ 204(?: .*)?", lines[0]):
        raise ValueError("public route did not return one 204 proof")
    h = defaultdict(list)
    for line in lines[1:]:
        if not line:
            continue
        key, value = line.split(":", 1)
        h[key.lower()].append(value.strip())
    return h


def expires_at(raw):
    if not isinstance(raw, str) or not raw.endswith("Z"):
        raise ValueError("expected UTC expiry")
    return datetime.fromisoformat(raw[:-1] + "+00:00")


def route_proof(d, t, edge, now):
    nonce = secrets.token_hex(16)
    h = proof_headers(trust.run([
        "curl", "--noproxy", "*", "--head", "--silent", "--show-error", "--max-time", "6",
        "--resolve", f'{d["host"]}:443:{d["public_ips"][edge]}',
        "--header", "X-Fugue-Route-Probe: 1", "--header", f"X-Fugue-Route-Probe-Nonce: {nonce}",
        f'https://{d["host"]}{d["path"]}',
    ]))
    fields = ["x-fugue-route-bundle-version", "x-fugue-route-valid-until", "x-fugue-route-proof", "x-fugue-app-traffic-proof"]
    if (h["x-fugue-route-probe-nonce"] != [nonce] or h["x-fugue-route-edge-id"] != [edge]
            or h["x-fugue-route-edge-group"] != [t["group"]] or h["x-fugue-route-probe-mode"]
            or any(len(h[field]) != 1 or not h[field][0] for field in fields)
            or any(not trust.matches(trust.DIGEST, h[field][0]) for field in fields[-2:])
            or (expires_at(h[fields[1]][0]) - now).total_seconds() < 30):
        raise ValueError("front did not attest a freshly applied route on the exact edge")
    return {"version": h[fields[0]][0], "proof": h[fields[2]][0], "app_traffic_proof": h[fields[3]][0]}


def release_image(d, t):
    ns = t["namespace"]
    pointer = trust.strict_json(trust.kubectl("-n", ns, "get", "configmap", d["monitor"], "-o", "json"))
    record_name = pointer["data"]["recordName"]
    if not trust.matches(trust.DNS_LABEL, record_name):
        raise ValueError("monitor does not point to an exact record")
    record = trust.strict_json(trust.kubectl("-n", ns, "get", "configmap", record_name, "-o", "json"))
    fields = record["data"]
    receipt = trust.strict_json(fields["terminal-result.json"])
    provenance = trust.strict_json(fields["record.json"])
    artifact = trust.strict_json(fields["artifact-receipt.json"])
    final = receipt["final"]
    if (record.get("immutable") is not True or receipt["status"] != "verified" or not receipt["final"]["present"]
            or any(x != d["worker_config_sha"] for x in (receipt["configSha"], provenance["configSha"],
                final["configSha"], final["manifestSha"], final["ociRevision"], artifact["configSha"], artifact["ociRevision"]))
            or final.get("uid") != d["front_uid"] or final.get("primary", {}).get("name") != d["front"]
            or not trust.matches(trust.IMAGE, final["imageRef"]) or final["imageRef"] != artifact["immutableRef"]
            or artifact["receiptDigest"] != provenance["artifactDigest"]
            or receipt["receiptDigest"] != provenance["terminalReceiptDigest"]):
        raise ValueError("Worker release is not the pinned positive immutable record")
    return final["imageRef"]


def front_witness(d, t, edge, image):
    ns = t["namespace"]
    if "serving_fronts" in d:
        pin = d["serving_fronts"][edge]
        svc = trust.strict_json(trust.kubectl("-n", ns, "get", "service", pin["service"], "-o", "json"))
        meta, spec = svc["metadata"], svc["spec"]
        ann = meta.get("annotations", {})
        selector = spec.get("selector", {})
        projection = {key: spec.get(key) for key in (
            "type", "selector", "ports", "externalIPs", "externalTrafficPolicy", "publishNotReadyAddresses",
        )}
        projection["publishNotReadyAddresses"] = bool(projection["publishNotReadyAddresses"])
        ports = spec.get("ports", [])
        if ((meta.get("name"), meta.get("namespace"), meta.get("uid")) != (pin["service"], ns, pin["uid"])
                or meta.get("deletionTimestamp")
                or meta.get("labels", {}).get("app.kubernetes.io/managed-by") != serving.MANAGER
                or ann.get("transport.fugue.dev/phase") != "serving"
                or ann.get(serving.GENERATION) != str(pin["generation"])
                or ann.get(serving.DIGEST) != transport.digest(projection)
                or spec.get("type") != "ClusterIP" or spec.get("externalTrafficPolicy") != "Local"
                or spec.get("externalIPs") != [d["public_ips"][edge]] or projection["publishNotReadyAddresses"]
                or selector.get("fugue.io/edge-node") != edge
                or len(ports) != 2
                or {(p.get("port"), p.get("targetPort"), p.get("protocol", "TCP")) for p in ports}
                    != {(80, 80, "TCP"), (443, 443, "TCP")}):
            raise ValueError("serving Front Service differs from its exact pilot pin")
        # Match the whole selector across the namespace before comparing it to
        # the EndpointSlice. A local Pod alone does not establish public ingress.
        query = ",".join(f"{k}={v}" for k, v in sorted(selector.items()))
        result = trust.strict_json(trust.kubectl("-n", ns, "get", "pods", "-l", query, "-o", "json"))
        pods = result["items"]
        if result["metadata"].get("continue") or len(pods) != 1:
            raise ValueError("serving Front Service must select one exact Pod")
        pod = pods[0]
        pm, ps = pod["metadata"], pod["spec"]
        containers = [c for c in pod["status"].get("containerStatuses", []) if c["name"] == "edge-front"]
        if (pm.get("namespace") != ns or pm.get("deletionTimestamp") or ps.get("nodeName") != edge
                or pm.get("labels", {}).get("fugue.io/edge-group-id") != t["group"]
                or any(pm.get("labels", {}).get(k) != v for k, v in selector.items())
                or not trust.ready(pod) or len(containers) != 1 or containers[0].get("ready") is not True
                or not containers[0].get("containerID")):
            raise ValueError("serving Front Service selected a different or unready process")
        endpoint = serving.selected_endpoint_witness({"namespace": ns}, {"name": pin["service"]}, {"node": edge}, pod, svc)
        ip = private_ip(endpoint["address"])
        path = f"/api/v1/namespaces/{ns}/pods/{pm['name']}:7831/proxy/readyz"
        health = trust.strict_json(trust.kubectl("get", "--raw", path))
        activation = witness_activation(d, image, health)
        return (pm["uid"], *activation, endpoint["service_uid"], endpoint["service_version"],
                str(ip), containers[0]["containerID"])

    workload = trust.strict_json(trust.kubectl("-n", ns, "get", "daemonset", d["front"], "-o", "json"))
    meta = workload["metadata"]
    if (meta.get("name"), meta.get("uid"), meta.get("namespace")) != (d["front"], d["front_uid"], ns) or meta.get("deletionTimestamp"):
        raise ValueError("front owner changed")
    pods = trust.strict_json(trust.kubectl("-n", ns, "get", "pods", "--field-selector", f"spec.nodeName={edge}", "-o", "json"))["items"]
    owned = [p for p in pods if trust.owned_slot(t, {"edge": edge, "workload": d["front"], "workload_uid": d["front_uid"]}, p)]
    if len(owned) != 1 or not trust.ready(owned[0]) or owned[0]["metadata"].get("deletionTimestamp"):
        raise ValueError("front is not an exact Ready retained Pod")
    pod = owned[0]
    front_ip = private_ip(pod["status"]["podIP"])
    health = trust.strict_json(trust.run(["curl", "--noproxy", "*", "--fail", "--silent", "--show-error", "--max-time", "5",
                                          f"http://{host_literal(front_ip)}:7831/healthz"]))
    return (pod["metadata"]["uid"], *witness_activation(d, image, health))


def witness_activation(d, image, health):
    generation = health.get("activation_generation")
    if (health.get("status") != "ok" or health.get("route_authority") != "edge-control"
            or health.get("active_slot") not in ("a", "b") or type(generation) is not int or generation <= 0
            or health.get("worker_source_commit") != d["worker_config_sha"]
            or health.get("worker_image_digest") != image.split("@")[1]):
        raise ValueError("front is not actively serving the declared Worker release")
    return health["active_slot"], generation


def private_ip(value):
    ip = ipaddress.ip_address(value)
    subnets = ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "fc00::/7")
    if not any(ip in ipaddress.ip_network(subnet) for subnet in subnets):
        raise ValueError("not a supported private Pod IP")
    return ip


def host_literal(ip):
    return str(ip) if ip.version == 4 else f"[{ip}]"


READ_FENCE = trust.CHECK + r'''
[ -d "$epoch/trust" ]
[ ! -L "$epoch/trust" ]
[ "$(stat -c '%u:%a' "$epoch/trust")" = "$owner:700" ]
[ -d "$epoch" ]
for file in ca.pem leaf.pem key.pem policy-verify-key; do
  path="$epoch/trust/$file"
  [ ! -L "$path" ]
  [ -f "$path" ]
  [ "$(stat -c '%u:%a:%h' "$path")" = "$owner:600:1" ]
done
fence="$epoch/fence.json"
[ ! -L "$fence" ]
if [ -e "$fence" ]; then
  [ -f "$fence" ]
  [ "$(stat -c '%u:%a:%h' "$fence")" = "$owner:600:1" ]
  [ "$(wc -c <"$fence")" -le 4096 ]
  cat "$fence"
fi
'''

READ_POLICY = trust.CHECK + r'''
path="$epoch/policy.json"
[ ! -L "$path" ]
[ -f "$path" ]
[ "$(stat -c '%u:%a:%h' "$path")" = "$owner:600:1" ]
[ "$(wc -c <"$path")" -le 49152 ]
cat "$path"
'''


def slot_witness(d, t, edge, image):
    front = front_witness(d, t, edge, image)
    target, = [{**p, "retained_image": image} for p in t["targets"] if (p["edge"], p["slot"]) == (edge, front[1])]
    return retained_witness(t, target, front)


def retained_witness(t, target, front=None):
    edge = target["edge"]
    witness = trust.read_checkpoint(t, target)
    pod = trust.strict_json(trust.kubectl("-n", t["namespace"], "get", "pod", witness[0], "-o", "json"))
    daemonset = trust.strict_json(trust.kubectl("-n", t["namespace"], "get", "daemonset", target["workload"], "-o", "json"))
    template = trust.attest_mount(t, target, daemonset["spec"]["template"]["spec"])
    if trust.attest_retained(t, target, pod, template) != witness or daemonset["metadata"]["uid"] != target["workload_uid"]:
        raise ValueError("active Worker changed during witness collection")
    worker = trust.attest_mount(t, target, pod["spec"])
    settings = {e["name"]: e for e in worker["env"] if e["name"].startswith("FUGUE_EDGE_ORIGIN_RELAY_")}
    epoch = t["ca_sha256"][7:]
    if (set(settings) != {"FUGUE_EDGE_ORIGIN_RELAY_TRUST_EPOCH", "FUGUE_EDGE_ORIGIN_RELAY_POD_IP"}
            or settings["FUGUE_EDGE_ORIGIN_RELAY_TRUST_EPOCH"].get("value") != epoch
            or settings["FUGUE_EDGE_ORIGIN_RELAY_POD_IP"].get("valueFrom") !=
            {"fieldRef": {"apiVersion": "v1", "fieldPath": "status.podIP"}}):
        raise ValueError("active Worker lacks the expected exact listener binding")
    ip = private_ip(pod["status"]["podIP"])
    expected = {"edge": edge, "slot": target["slot"], "address": f"{host_literal(ip)}:{PORT}", "tls_name": target["tls_name"]}
    raw = trust.worker_exec(t, target, witness[0], READ_FENCE)
    fence = trust.strict_json(raw) if raw else None
    if fence is not None and (fence.get("edge") != edge or fence.get("slot") != target["slot"]
            or fence.get("verifier") != t["policy_verify_key"] or type(fence.get("generation")) is not int
            or not 0 < fence["generation"] < 2 ** 53 or not trust.matches(HEX256, fence.get("payload_sha256"))):
        raise ValueError("active Worker has an unrelated retained policy fence")
    return {"target": target, "pod": witness, "front": front, "endpoint": expected, "fence": fence}


def verify_envelope(executable, t, raw):
    if len(raw) > trust.LIMIT:
        raise ValueError("pilot envelope exceeds delivery bound")
    envelope = trust.strict_json(raw)
    if not isinstance(envelope, dict) or set(envelope) != {"payload", "signature"}:
        raise ValueError("expected an envelope without any signing credentials")
    verified = trust.strict_json(trust.run([executable, "verify"], trust.canonical({
        "verify_key": t["policy_verify_key"], "envelope": envelope,
    })))
    if not trust.matches(HEX256, verified.get("payload_sha256")):
        raise ValueError("invalid verified policy digest")
    return verified


def admit_policy(d, verified, witnesses, proofs):
    p, digest = verified["policy"], verified["payload_sha256"]
    if (p["mode"], p["generation"], p["max_concurrent"], p["relay_percent"]) != (
            d["mode"], d["generation"], 1, 1 if d["mode"] == "active" else 0):
        raise ValueError("signed permissions or generation differ from the declared pilot")
    if d["mode"] == "off":
        if p["endpoints"] or p["bindings"]:
            raise ValueError("off lease must carry no routing authorization")
    else:
        if len(witnesses) != 2 or len(p["endpoints"] or []) != 2 or len(p["bindings"] or []) != 1:
            raise ValueError("pilot policy is not one path between the two exact endpoints")
        pairs = {e["edge"]: e for e in p["endpoints"]}
        b = p["bindings"][0]
        if (pairs != {edge: w["endpoint"] for edge, w in witnesses.items()}
                or (b["source"], b["peer"], b["host"], b["path"]) != (d["source"], d["peer"], d["host"], d["path"])
                or b["source_digest"] != proofs[d["source"]]["proof"]
                or b["peer_digest"] != proofs[d["peer"]]["proof"]
                or proofs[d["source"]]["app_traffic_proof"] != proofs[d["peer"]]["app_traffic_proof"]
                or not 1 <= b["max_body_bytes"] <= 512 * 1024):
            raise ValueError("policy does not match freshly observed serving pair and declared scope")
    for w in witnesses.values():
        f = w["fence"]
        if f is None and d["mode"] != "off":
            raise ValueError("persistent fence has not been initialized by signed off")
        if f and (f["generation"] > d["generation"] or
                  (f["generation"] == d["generation"] and f["payload_sha256"] != digest)):
            raise ValueError("policy delivery would regress or conflict with retained generation")


def require_shadow(d, t, verifier, witnesses, proofs):
    phase = {**d, "mode": "shadow", "generation": d["generation"] - 1}
    for w in witnesses.values():
        if w["fence"] is None or w["fence"]["generation"] != phase["generation"]:
            raise ValueError("active admission requires the preceding shadow phase on both Workers")
        raw = trust.worker_exec(t, w["target"], w["pod"][0], READ_POLICY)
        previous = verify_envelope(verifier, t, raw)
        if previous["payload_sha256"] != w["fence"]["payload_sha256"]:
            raise ValueError("preceding shadow lease is not loaded in the exact fence")
        admit_policy(phase, previous, witnesses, proofs)


WRITE_POLICY = trust.CHECK + r'''
umask 077
trust="$epoch/trust"
[ ! -L "$trust" ]
[ -d "$trust" ]
[ "$(stat -c '%u:%a' "$trust")" = "$owner:700" ]
dest="$epoch/policy.json"
[ ! -L "$dest" ]
if [ -e "$dest" ]; then
  [ -f "$dest" ]
  [ "$(stat -c '%u:%a:%h' "$dest")" = "$owner:600:1" ]
fi
tmp=$(mktemp "$epoch/.policy.XXXXXXXX")
trap 'rm -f -- "$tmp"' EXIT
cat >"$tmp"
[ -s "$tmp" ]
[ "$(wc -c <"$tmp")" -le 49153 ]
sync "$tmp"
mv -T -- "$tmp" "$dest"
sync "$epoch"
printf 'relay-policy-delivered\n'
'''


def assert_confirmed(t, w, d, digest):
    for _ in range(16):
        polled = trust.worker_exec(t, w["target"], w["pod"][0], READ_FENCE)
        if polled:
            fence = trust.strict_json(polled)
            if fence == {"edge": w["endpoint"]["edge"], "slot": w["endpoint"]["slot"],
                         "verifier": t["policy_verify_key"], "generation": d["generation"], "payload_sha256": digest}:
                if trust.read_checkpoint(t, w["target"]) != w["pod"]:
                    raise ValueError("recipient changed after committing the exact fence")
                return
        time.sleep(1)
    raise ValueError("exact Worker did not acknowledge the policy in its durable fence")


def withdraw_retained(d, t, raw, verifier):
    # All permissions come from the fresh, non-routing off envelope. The
    # declaration identifies the *previously acknowledged* exact recipients;
    # this branch cannot enroll a new Worker, renew a lease, or switch slots.
    validate_declaration(d, t)
    prior = d["withdrawal_of"]
    available, delivered, unavailable = [], [], []
    auth = verify_envelope(verifier, t, raw) if raw else None
    if auth:
        admit_policy(d, auth, {}, {})
    for edge in (d["source"], d["peer"]):
        target, = [p for p in t["targets"] if (p["edge"], p["slot"]) == (edge, prior["slots"][edge])]
        try:
            w = retained_witness(t, target)
            f = w["fence"]
            acknowledged = auth is not None and f is not None and (f["generation"], f["payload_sha256"]) == (
                d["generation"], auth["payload_sha256"])
            if not acknowledged and (not f or (f["generation"], f["payload_sha256"]) !=
                                     (prior["generation"], prior["payload_sha256"])):
                raise ValueError("retained recipient does not hold the declared prior fence")
            available.append(edge)
            if not auth:
                continue
            if verify_envelope(verifier, t, raw) != auth or retained_witness(t, target) != w:
                raise ValueError("prior fence, process, or withdrawal envelope changed before delivery")
            admit_policy(d, auth, {edge: w}, {})
            if not acknowledged and trust.worker_exec(t, target, w["pod"][0], WRITE_POLICY, data=raw + b"\n") != b"relay-policy-delivered\n":
                raise ValueError("withdrawal storage not acknowledged")
            assert_confirmed(t, w, d, auth["payload_sha256"])
            delivered.append(edge)
        except Exception:
            unavailable.append(edge)  # Continue to the other exact recipient; never infer its ACK.
    return {"mode": "off", "generation": d["generation"], "config_sha": d["worker_config_sha"], "available": available,
            "verified_fences": delivered, "unavailable": unavailable, "lease_published": len(delivered) == 2,
            "payload_sha256": auth["payload_sha256"] if auth else "", "route_verified": [],
            "withdrawal_of": prior}


def execute(d, t, raw, verifier):
    if "withdrawal_of" in d:
        return withdraw_retained(d, t, raw, verifier)
    image = release_image(d, t)
    source, peer = d["source"], d["peer"]
    order = (source, peer) if d["mode"] == "off" else (peer, source)
    authenticated = verify_envelope(verifier, t, raw) if raw else None
    if authenticated and d["mode"] == "off":
        admit_policy(d, authenticated, {}, {})
    witnesses, proofs, delivered, unavailable = {}, {}, [], []
    for edge in order:
        try:
            w = slot_witness(d, t, edge, image)
        except Exception:
            if d["mode"] != "off":
                raise
            unavailable.append(edge)
            continue  # An unreachable peer must not stop withdrawal on a surviving source.
        witnesses[edge] = w
        if d["mode"] != "off":
            proofs[edge] = route_proof(d, t, edge, datetime.now(timezone.utc))
        elif authenticated:
            # Isolate withdrawal per recipient: no route, peer, or candidate proof required.
            try:
                deliver(d, t, edge, image, raw, verifier, authenticated, {edge: w}, {})
                delivered.append(edge)
            except Exception:
                unavailable.append(edge)
    if authenticated and d["mode"] != "off":
        admit_policy(d, authenticated, witnesses, proofs)
        digest = authenticated["payload_sha256"]
        if all(w["fence"] and w["fence"]["generation"] == d["generation"] and
               w["fence"]["payload_sha256"] == digest for w in witnesses.values()):
            delivered.extend(order)  # Exact prior publication; do not rewrite the lease.
            order = ()
        if order and d["mode"] == "active":
            require_shadow(d, t, verifier, witnesses, proofs)
        for edge in order:
            try:
                deliver(d, t, edge, image, raw, verifier, authenticated, witnesses, proofs)
                delivered.append(edge)
            except Exception:
                unavailable.append(edge)
                break  # Ingress cannot activate before egress has acknowledged.
    return {"mode": d["mode"], "generation": d["generation"], "config_sha": d["worker_config_sha"], "available": sorted(witnesses),
            "verified_fences": delivered, "unavailable": unavailable, "lease_published": len(delivered) == 2,
            "payload_sha256": authenticated["payload_sha256"] if authenticated else "",
            "route_verified": sorted(proofs)}


def deliver(d, t, edge, image, raw, verifier, authenticated, witnesses, proofs):
    digest = authenticated["payload_sha256"]
    current = verify_envelope(verifier, t, raw)
    if current != authenticated:
        raise ValueError("envelope changed between witness and delivery")
    w = slot_witness(d, t, edge, image)
    original = witnesses[edge]
    if any(w[k] != original[k] for k in ("pod", "front", "endpoint")):
        raise ValueError("active recipient changed after authorization")
    if d["mode"] != "off":
        live = route_proof(d, t, edge, datetime.now(timezone.utc))
        if any(live[k] != proofs[edge][k] for k in ("proof", "app_traffic_proof")):
            raise ValueError("route proof changed after authorization")
    admit_policy(d, authenticated, {**witnesses, edge: w}, proofs)
    if trust.worker_exec(t, w["target"], w["pod"][0], WRITE_POLICY, data=raw + b"\n") != b"relay-policy-delivered\n":
        raise ValueError("retained Worker did not confirm policy storage")
    assert_confirmed(t, w, d, digest)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("declaration")
    parser.add_argument("--trust", required=True)
    parser.add_argument("--verifier", required=True)
    options = parser.parse_args()
    private = os.environ.pop("FUGUE_ORIGIN_RELAY_SIGNED_LEASE", "").encode()
    t = trust.validate_declaration(trust.strict_json(Path(options.trust).read_bytes()))
    raw = Path(options.declaration).read_bytes()
    if len(raw) > trust.LIMIT:
        raise ValueError("deployment declaration exceeds bound")
    d = validate_declaration(trust.strict_json(raw), t)
    receipt = execute(d, t, private, options.verifier)
    print(trust.canonical(receipt).decode())
    if receipt["unavailable"] or (private and not receipt["lease_published"]):
        raise SystemExit("one or more required relay recipients did not acknowledge delivery")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # Never include the raw envelope, route metadata, or a subprocess reply in CI.
        print("Origin relay policy release failed closed; inspect exact live witness before retrying", file=sys.stderr)
        raise SystemExit(1) from None
