#!/usr/bin/env python3
"""Declare isolated Front probe listeners; public serving ports are forbidden."""
import argparse
import datetime
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import subprocess
import time

try:
    from . import observe_front_candidate as front
except ImportError:
    import observe_front_candidate as front

MANAGER = "fugue-front-probe-transport"
GENERATION = "transport.fugue.dev/generation"
DIGEST = "transport.fugue.dev/digest"


def digest(value):
    return "sha256:" + hashlib.sha256(front.canonical(value).encode()).hexdigest()


def kubectl(*args, body=None):
    result = subprocess.run(["kubectl", *args], input=body, capture_output=True, text=True, timeout=20)
    if result.returncode:
        raise ValueError("Front probe transport operation failed: " + result.stderr.strip())
    return json.loads(result.stdout) if result.stdout.strip() else None


def validate(config):
    if set(config) != {"schema", "generation", "namespace", "listeners"} or config["schema"] != "fugue.front-probe-transport/v1" or type(config["generation"]) is not int or config["generation"] <= 0 or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", config["namespace"]):
        raise ValueError("explicit Front probe transport declaration required")
    if not isinstance(config["listeners"], list) or not 1 <= len(config["listeners"]) <= 16:
        raise ValueError("bounded probe listener set required")
    names, sockets = set(), set()
    for listener in config["listeners"]:
        if set(listener) != {"name", "address", "port", "observation"} or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", listener["name"]):
            raise ValueError("probe listener identity invalid")
        address = ipaddress.ip_address(listener["address"])
        if str(address) != listener["address"] or not address.is_global or address.version != 4 or type(listener["port"]) is not int or not 1024 <= listener["port"] <= 65535:
            raise ValueError("probe transport cannot acquire public serving ports or an implicit address")
        path = Path(listener["observation"])
        if path.parent != Path("deploy/environments/production/front-observation") or path.suffix != ".json" or path.name in [".json", "..json"]:
            raise ValueError("probe transport requires an explicit observation profile")
        key = (listener["address"], listener["port"])
        if listener["name"] in names or key in sockets:
            raise ValueError("duplicate Front probe listener")
        names.add(listener["name"])
        sockets.add(key)
    return config


def profile(config, listener):
    value = front.validate(json.loads(Path(listener["observation"]).read_text()))
    if value["namespace"] != config["namespace"]:
        raise ValueError("probe listener and consumer namespaces differ")
    node = front.read("get", "node", value["node"], "-o", "json")
    if node["metadata"].get("deletionTimestamp") or not node["metadata"].get("uid") or listener["address"] not in [a["address"] for a in node.get("status", {}).get("addresses", [])]:
        raise ValueError("probe address is not owned by the declared live node")
    return value


def desired(config, listener, observation):
    spec = {"type": "ClusterIP", "externalIPs": [listener["address"]], "externalTrafficPolicy": "Local", "internalTrafficPolicy": "Local", "publishNotReadyAddresses": False, "selector": observation["candidateSelector"], "ports": [{"name": "https-probe", "protocol": "TCP", "port": listener["port"], "targetPort": 443}]}
    return {"apiVersion": "v1", "kind": "Service", "metadata": {"name": listener["name"], "namespace": config["namespace"], "labels": {"app.kubernetes.io/managed-by": MANAGER}, "annotations": {GENERATION: str(config["generation"]), DIGEST: digest(spec)}}, "spec": spec}


def validate_existing(current, target):
    if current is None:
        return
    meta = current["metadata"]
    if meta.get("deletionTimestamp") or meta.get("labels", {}).get("app.kubernetes.io/managed-by") != MANAGER or not meta.get("uid") or not meta.get("resourceVersion"):
        raise ValueError("Front probe Service is unowned or lacks a live CAS identity")
    previous = {key: current.get("spec", {}).get(key) for key in target["spec"]}
    previous["publishNotReadyAddresses"] = bool(previous["publishNotReadyAddresses"])
    if digest(previous) != meta.get("annotations", {}).get(DIGEST):
        raise ValueError("Front probe transport has undeclared live drift")
    generation = int(meta["annotations"].get(GENERATION, "0"))
    next_generation = int(target["metadata"]["annotations"][GENERATION])
    if next_generation < generation or next_generation == generation and target["metadata"]["annotations"][DIGEST] != meta["annotations"][DIGEST]:
        raise ValueError("Front probe declaration is replayed or mutated at the same generation")
    for owner in meta.get("managedFields", []):
        if owner.get("manager") != MANAGER:
            fields = owner.get("fieldsV1", {})
            metadata = fields.get("f:metadata", {})
            if fields.get("f:spec") or any("f:" + k in metadata.get("f:annotations", {}) for k in [GENERATION, DIGEST]) or "f:app.kubernetes.io/managed-by" in metadata.get("f:labels", {}):
                raise ValueError("Front probe transport has a foreign field writer")


def mutation(current, target):
    validate_existing(current, target)
    if current is None:
        return ["create", "-f", "-", "--field-manager=" + MANAGER], front.canonical(target)
    patch = [{"op": "test", "path": "/metadata/uid", "value": current["metadata"]["uid"]}, {"op": "test", "path": "/metadata/resourceVersion", "value": current["metadata"]["resourceVersion"]}, {"op": "test", "path": "/spec", "value": current["spec"]}, {"op": "test", "path": "/metadata/annotations", "value": current["metadata"]["annotations"]}]
    for key, value in target["spec"].items():
        actual = current["spec"].get(key)
        if key == "publishNotReadyAddresses":
            actual = bool(actual)
        if actual != value:
            patch.append({"op": "add", "path": "/spec/" + key, "value": value})
    for key, value in target["metadata"]["annotations"].items():
        if current["metadata"]["annotations"].get(key) != value:
            patch.append({"op": "add", "path": "/metadata/annotations/" + key.replace("~", "~0").replace("/", "~1"), "value": value})
    if len(patch) == 4:
        return None, None
    return ["patch", "service", target["metadata"]["name"], "-n", target["metadata"]["namespace"], "--type=json", "--patch-file", "-", "--field-manager=" + MANAGER], front.canonical(patch)


def current_service(config, listener):
    return kubectl("get", "service", listener["name"], "-n", config["namespace"], "-o", "json", "--ignore-not-found")


def check_listener_collision(config, listener, observation):
    services = front.read("get", "services", "-A", "-o", "json")
    pods = front.read("get", "pods", "-A", "--field-selector=spec.nodeName=" + observation["node"], "-o", "json")
    if services.get("metadata", {}).get("continue") or pods.get("metadata", {}).get("continue"):
        raise ValueError("probe port ownership observation is incomplete")
    for service in services["items"]:
        meta, spec = service["metadata"], service["spec"]
        if meta["namespace"] == config["namespace"] and meta["name"] == listener["name"]:
            continue
        for port in spec.get("ports", []):
            if port.get("protocol", "TCP") == "TCP" and (port.get("nodePort") == listener["port"] or listener["address"] in spec.get("externalIPs", []) and port.get("port") == listener["port"]):
                raise ValueError("Front probe port is already owned by another Service")
    for pod in pods["items"]:
        for container in pod["spec"]["containers"]:
            for port in container.get("ports", []):
                host = port.get("hostPort", port.get("containerPort") if pod["spec"].get("hostNetwork") else None)
                if port.get("protocol", "TCP") == "TCP" and host == listener["port"]:
                    raise ValueError("Front probe port is already declared by a node workload")
    observers = [p for p in pods["items"] if p["spec"].get("hostNetwork") and p["status"].get("phase") == "Running" and not p["metadata"].get("deletionTimestamp")]
    if not observers:
        raise ValueError("no existing node-network observer for probe port occupancy")
    observed = False
    for pod in observers:
        for container in pod["spec"]["containers"]:
            result = subprocess.run(["kubectl", "-n", pod["metadata"]["namespace"], "exec", pod["metadata"]["name"], "-c", container["name"], "--", "cat", "/proc/net/tcp", "/proc/net/tcp6"], capture_output=True, text=True, timeout=20)
            if result.returncode or len(result.stdout) > 4 << 20:
                continue
            rows = [line.split() for line in result.stdout.splitlines() if ":" in line and "local_address" not in line]
            if not rows:
                continue
            for row in rows:
                if len(row) < 4 or ":" not in row[1]:
                    raise ValueError("node socket observation is invalid")
                if row[3] == "0A" and int(row[1].split(":")[-1], 16) == listener["port"]:
                    raise ValueError("Front probe port has an existing node TCP listener")
            observed = True
            break
        if observed:
            break
    if not observed:
        raise ValueError("cannot verify node TCP listener occupancy")


def prepare(config):
    plans = []
    for listener in config["listeners"]:
        observation = profile(config, listener)
        check_listener_collision(config, listener, observation)
        evidence = front.observe(observation)
        target = desired(config, listener, observation)
        current = current_service(config, listener)
        command, body = mutation(current, target)
        if command:
            kubectl(*command, "--dry-run=server", "-o", "json", body=body)
        plans.append({"listener": listener, "profile_digest": digest(observation), "evidence": evidence, "current": current, "desired": target})
    result = {"schema": "fugue.front-probe-transport-evidence/v1", "declaration_digest": digest(config), "prepared_at": front.now().isoformat(), "plans": plans}
    result["evidence_digest"] = digest(result)
    return result


def apply(config, witness):
    unsigned = dict(witness)
    recorded = unsigned.pop("evidence_digest", None)
    if recorded != digest(unsigned) or witness.get("schema") != "fugue.front-probe-transport-evidence/v1" or witness.get("declaration_digest") != digest(config) or not front.now() - datetime.timedelta(minutes=5) < front.timestamp(witness["prepared_at"]) <= front.now() or [p["listener"] for p in witness.get("plans", [])] != config["listeners"]:
        raise ValueError("probe transport evidence is stale, altered or for another declaration")
    # Preflight every listener before performing the first network write.
    pending = []
    for plan in witness["plans"]:
        listener = plan["listener"]
        observation = profile(config, listener)
        check_listener_collision(config, listener, observation)
        if digest(observation) != plan["profile_digest"] or desired(config, listener, observation) != plan["desired"]:
            raise ValueError("Front observation declaration changed after evidence retention")
        fresh = front.observe(dict(observation, samples=1))["observations"][0]
        previous = plan["evidence"]["observations"][-1]
        if any(fresh[k] != previous[k] for k in ["old_uid", "candidate_uid", "candidate_image", "activation"]):
            raise ValueError("Front identity or activation changed before probe transport write")
        current = current_service(config, listener)
        if current != plan["current"]:
            raise ValueError("Front probe Service changed after preparation")
        command, body = mutation(current, plan["desired"])
        pending.append((listener, observation, command, body))
    for listener, observation, command, body in pending:
        if command:
            kubectl(*command, "-o", "json", body=body)
        # New flows must reach the independently observed executor through the
        # declared public probe port. No serving listener or Pod is replaced.
        last_error = None
        for attempt in range(6):
            try:
                candidate = front.pod(observation, observation["candidateSelector"], True)
                for target in observation["probes"]:
                    private = front.proof(candidate["status"]["podIP"], target["host"], target["path"])
                    public = front.proof(listener["address"], target["host"], target["path"], listener["port"])
                    if private != public:
                        raise ValueError("Front probe listener routes to different authority")
                last_error = None
                break
            except (ValueError, OSError) as error:
                last_error = error
                if attempt < 5:
                    time.sleep(2)
        if last_error:
            raise ValueError("Front probe listener did not converge to its observed candidate") from last_error
        print(front.canonical({"probe_listener_verified": listener["name"], "public_serving_ports_changed": False}), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["prepare", "apply"])
    parser.add_argument("config")
    parser.add_argument("--evidence", required=True)
    args = parser.parse_args()
    config = validate(json.loads(Path(args.config).read_text()))
    if args.operation == "prepare":
        Path(args.evidence).write_text(front.canonical(prepare(config)) + "\n")
    else:
        apply(config, json.loads(Path(args.evidence).read_text()))


if __name__ == "__main__":
    main()
