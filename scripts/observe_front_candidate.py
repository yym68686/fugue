#!/usr/bin/env python3
"""Read-only Front equivalence evidence; never changes traffic or activation."""
import argparse
import base64
import datetime
import hashlib
import http.client
import ipaddress
import json
from pathlib import Path
import re
import secrets
import socket
import ssl
import subprocess
import time


def now():
    return datetime.datetime.now(datetime.timezone.utc)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def timestamp(value):
    result = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("observation timestamp has no timezone")
    return result


def read(*args):
    # Read-only transport failures may be retried; the caller still compares
    # Pod identity and activation before and after the complete observation.
    # Invalid returned data is never replaced with an earlier successful read.
    for attempt in range(3):
        try:
            result = subprocess.run(["kubectl", *args], capture_output=True, text=True, timeout=20)
        except subprocess.TimeoutExpired:
            result = None
        if result is not None and result.returncode == 0:
            if len(result.stdout) > 4 << 20:
                raise ValueError("Front observation exceeds size bound")
            return json.loads(result.stdout)
        if attempt < 2:
            time.sleep(1)
    raise ValueError("Front observation unavailable after bounded read attempts")


def validate(config):
    required = {"schema", "namespace", "node", "group", "legacySelector", "candidateSelector", "candidateSource", "activationPath", "probes", "samples", "intervalSeconds"}
    if set(config) != required or config["schema"] != "fugue.front-observation/v1":
        raise ValueError("invalid Front observation declaration")
    for key in ["namespace", "node", "group"]:
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", config[key]):
            raise ValueError("explicit canonical observation identity required")
    if not re.fullmatch(r"[0-9a-f]{40}", config["candidateSource"]) or not config["activationPath"].startswith("/"):
        raise ValueError("immutable Front source and absolute activation path required")
    for key in ["legacySelector", "candidateSelector"]:
        if not isinstance(config[key], dict) or not config[key] or any(not isinstance(k, str) or not isinstance(v, str) or any(c in k + v for c in "\n\r,= ") for k, v in config[key].items()):
            raise ValueError("explicit Front selector required")
    if config["legacySelector"] == config["candidateSelector"]:
        raise ValueError("Front observation must isolate distinct executors")
    if type(config["samples"]) is not int or type(config["intervalSeconds"]) is not int or not 3 <= config["samples"] <= 20 or not 10 <= config["intervalSeconds"] <= 60:
        raise ValueError("bounded observation window required")
    if not isinstance(config["probes"], list) or not 2 <= len(config["probes"]) <= 256:
        raise ValueError("explicit bounded hostname proofs required")
    for probe in config["probes"]:
        if set(probe) != {"host", "path"} or not re.fullmatch(r"[a-z0-9][a-z0-9.-]+[a-z0-9]", probe["host"]) or not probe["path"].startswith("/") or any(x in probe["path"] for x in ["\r", "\n", "?", "#"]):
            raise ValueError("canonical route proof target required")
    return config


def pod(config, selector, candidate):
    query = ",".join(k + "=" + v for k, v in sorted(selector.items()))
    items = read("get", "pods", "-n", config["namespace"], "-l", query, "-o", "json")["items"]
    items = [p for p in items if not p["metadata"].get("deletionTimestamp") and p["spec"].get("nodeName") == config["node"]]
    if len(items) != 1:
        raise ValueError("Front ownership is ambiguous")
    p = items[0]
    if p["status"].get("phase") != "Running" or not any(c.get("type") == "Ready" and c.get("status") == "True" for c in p["status"].get("conditions", [])):
        raise ValueError("Front is not Ready")
    c = next(c for c in p["spec"]["containers"] if c["name"] == "edge-front")
    status = next(c for c in p["status"]["containerStatuses"] if c["name"] == "edge-front")
    env = {e["name"]: e.get("value") for e in c["env"]}
    if env.get("FUGUE_EDGE_FRONT_EDGE_GROUP_ID") != config["group"] or env.get("FUGUE_EDGE_FRONT_ACTIVE_SLOT_FILE") != config["activationPath"] or env.get("FUGUE_EDGE_FRONT_REQUIRE_ACTIVATION_STATE") != "true":
        raise ValueError("Front is not bound to the declared activation authority")
    if not status.get("ready") or "@sha256:" not in status.get("imageID", "") or p["metadata"].get("namespace") != config["namespace"]:
        raise ValueError("Front image or runtime identity is invalid")
    if candidate:
        if p["metadata"]["annotations"].get("fugue.pro/source-commit") != config["candidateSource"] or status.get("restartCount") != 0 or p["spec"].get("hostNetwork") or p["spec"].get("automountServiceAccountToken") is not False or any(x.get("hostPort") for x in c.get("ports", [])) or any(m.get("readOnly") is not True for m in c.get("volumeMounts", [])):
            raise ValueError("candidate Front changed code, restart state or observation isolation")
    ipaddress.ip_address(p["status"]["podIP"])
    return p


def state(config, p):
    namespace, name = config["namespace"], p["metadata"]["name"]
    activation = read("-n", namespace, "exec", name, "-c", "edge-front", "--", "cat", config["activationPath"])
    health = read("get", "--raw", "/api/v1/namespaces/" + namespace + "/pods/" + name + ":7831/proxy/readyz")
    expected = {"active_slot": activation["active_slot"], "activation_generation": activation["generation"], "bundle_generation": activation["bundle_generation"], "worker_source_commit": activation["worker_source_commit"], "worker_image_digest": activation["worker_image_digest"], "route_authority": "edge-control"}
    if activation.get("edge_group_id") != config["group"] or activation.get("authority") != "edge-control" or health.get("status") != "ok" or any(health.get(k) != v for k, v in expected.items()):
        raise ValueError("Front readiness differs from durable activation")
    return activation


def proof(address, host, path, port=443):
    context = ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    nonce = secrets.token_hex(16)
    conn = http.client.HTTPSConnection(host, 443, context=context, timeout=5)
    raw = socket.create_connection((address, port), timeout=5)
    try:
        conn.sock = context.wrap_socket(raw, server_hostname=host)
        certificate_until = datetime.datetime.fromtimestamp(ssl.cert_time_to_seconds(conn.sock.getpeercert()["notAfter"]), datetime.timezone.utc)
        conn.request("HEAD", path, headers={"Host": host, "X-Fugue-Route-Probe": "1", "X-Fugue-Route-Probe-Nonce": nonce})
        response = conn.getresponse()
        return parse_proof(response, nonce, certificate_until)
    finally:
        conn.close()
        raw.close()


def parse_proof(response, nonce, certificate_until):
    headers = response.getheaders()
    def exact(name):
        values = [v for k, v in headers if k.lower() == name.lower()]
        if len(values) != 1 or not values[0] or values[0] != values[0].strip():
            raise ValueError("ambiguous or missing Front route proof")
        return values[0]
    if response.status != 204 or exact("X-Fugue-Route-Probe-Nonce") != nonce or exact("Cache-Control") != "no-store" or any(k.lower() == "x-fugue-route-probe-state" for k, _ in headers):
        raise ValueError("Front route proof is not fresh positive evidence")
    expiry = min(timestamp(exact("X-Fugue-Route-Valid-Until")), certificate_until)
    digest = exact("X-Fugue-Route-Proof")
    if expiry <= now() + datetime.timedelta(seconds=15) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ValueError("Front proof lease or digest is invalid")
    traffic = exact("X-Fugue-Traffic-Release")
    if len(traffic) > 8192:
        raise ValueError("Front traffic binding exceeds limit")
    binding = json.loads(base64.urlsafe_b64decode(traffic + "=" * (-len(traffic) % 4)))
    if not binding.get("release_id") or type(binding.get("fencing_token")) is not int or binding["fencing_token"] <= 0 or binding.get("release_channel") not in ["gray", "full"]:
        raise ValueError("Front traffic publication is missing")
    return {"digest": digest, "version": exact("X-Fugue-Route-Bundle-Version"), "edge": exact("X-Fugue-Route-Edge-Id"), "group": exact("X-Fugue-Route-Edge-Group"), "traffic": binding, "app_traffic": response.getheader("X-Fugue-App-Traffic-Proof", "")}


def stable_proof_pair(address, probe_port, host, path, node, group):
    """Accept only an exact stable pair, retrying an observed publication race."""
    for attempt in range(3):
        current = proof(address, host, path)
        candidate = proof(address, host, path, probe_port)
        after = proof(address, host, path)
        values = [current, candidate, after]
        if any(value.get("edge") != node or value.get("group") != group for value in values):
            raise ValueError("public/probe route proof has a different Edge or authority")
        if current == candidate == after:
            return {"proof": current, "publication_retries": attempt}
        # Both samples of the same public endpoint must prove that publication
        # advanced around the candidate read. A stable but different candidate,
        # bad TLS, missing/negative evidence or wrong identity never retries.
        if current != after and candidate in [current, after]:
            continue
        details = {"host": host, "differences": sorted(k for k in set(current) | set(candidate) | set(after) if current.get(k) != candidate.get(k) or after.get(k) != candidate.get(k)),
                   "versions": [v.get("version") for v in values], "releases": [v.get("traffic", {}).get("release_id") for v in values]}
        raise ValueError("public/probe route proofs differ: " + canonical(details))
    raise ValueError("public/probe publication did not stabilize within the bounded observation")


def observe(config):
    observations = []
    for index in range(config["samples"]):
        old = pod(config, config["legacySelector"], False)
        candidate = pod(config, config["candidateSelector"], True)
        if old["metadata"]["uid"] == candidate["metadata"]["uid"]:
            raise ValueError("candidate reused the public Front")
        before = state(config, old)
        if before != state(config, candidate):
            raise ValueError("Front executors have different activation state")
        proofs = []
        for target in config["probes"]:
            previous = proof(old["status"]["podIP"], target["host"], target["path"])
            current = proof(candidate["status"]["podIP"], target["host"], target["path"])
            if previous != current or current["edge"] != config["node"] or current["group"] != config["group"]:
                raise ValueError("candidate Front route or publication differs")
            proofs.append({"host": target["host"], "path": target["path"], "proof_digest": "sha256:" + hashlib.sha256(canonical(current).encode()).hexdigest()})
        if state(config, old) != before or state(config, candidate) != before:
            raise ValueError("activation changed during observation")
        for observed, selector, is_candidate in [(old, config["legacySelector"], False), (candidate, config["candidateSelector"], True)]:
            latest = pod(config, selector, is_candidate)
            if latest["metadata"]["uid"] != observed["metadata"]["uid"] or latest["spec"] != observed["spec"]:
                raise ValueError("Front identity changed during observation")
        observations.append({"at": now().isoformat(), "old_uid": old["metadata"]["uid"], "candidate_uid": candidate["metadata"]["uid"], "candidate_image": candidate["status"]["containerStatuses"][0]["imageID"], "activation": before, "proofs": proofs})
        if index + 1 < config["samples"]:
            time.sleep(config["intervalSeconds"])
    if len({o["candidate_uid"] for o in observations}) != 1 or len({o["old_uid"] for o in observations}) != 1:
        raise ValueError("Front changed during observation window")
    return {"schema": "fugue.front-observation-evidence/v1", "authorizes_traffic": False, "declaration_digest": "sha256:" + hashlib.sha256(canonical(config).encode()).hexdigest(), "observations": observations}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config")
    parser.add_argument("--evidence", required=True)
    args = parser.parse_args()
    result = observe(validate(json.loads(Path(args.config).read_text())))
    Path(args.evidence).write_text(canonical(result) + "\n")
    print(canonical({"observed": True, "authorizes_traffic": False, "samples": len(result["observations"])}))


if __name__ == "__main__":
    main()
