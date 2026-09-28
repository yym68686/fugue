#!/usr/bin/env python3
"""Prepare internal-only Front Services before any public address handoff."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time

try:
    from . import observe_front_candidate as front
    from . import reconcile_front_probe_transport as probe
except ImportError:
    import observe_front_candidate as front
    import reconcile_front_probe_transport as probe

MANAGER = "fugue-front-serving-transport"
GENERATION = "transport.fugue.dev/generation"
DIGEST = "transport.fugue.dev/digest"


def validate(config):
    if set(config) != {"schema", "generation", "namespace", "services"} or config["schema"] != "fugue.front-serving-stage/v1" or type(config["generation"]) is not int or config["generation"] <= 0 or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", config["namespace"]):
        raise ValueError("explicit internal Front transport stage required")
    if not isinstance(config["services"], list) or not 1 <= len(config["services"]) <= 16:
        raise ValueError("bounded internal Front service set required")
    seen = set()
    for service in config["services"]:
        if set(service) != {"name", "observation", "probeListener"} or service["name"] in seen or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", service["name"]) or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", service["probeListener"]):
            raise ValueError("internal Front service identity invalid")
        if Path(service["observation"]).parent != Path("deploy/environments/production/front-observation") or Path(service["observation"]).suffix != ".json":
            raise ValueError("explicit Front observation profile required")
        seen.add(service["name"])
    return config


def desired(config, service, profile):
    spec = {"type": "ClusterIP", "externalIPs": [], "publishNotReadyAddresses": False, "selector": profile["candidateSelector"], "ports": [{"name": "http", "protocol": "TCP", "port": 80, "targetPort": 80}, {"name": "https", "protocol": "TCP", "port": 443, "targetPort": 443}]}
    return {"apiVersion": "v1", "kind": "Service", "metadata": {"namespace": config["namespace"], "name": service["name"], "labels": {"app.kubernetes.io/managed-by": MANAGER}, "annotations": {GENERATION: str(config["generation"]), DIGEST: probe.digest(spec), "transport.fugue.dev/phase": "staged"}}, "spec": spec}


def validate_existing(current, target):
    if current is None:
        return
    meta, spec = current["metadata"], current["spec"]
    if meta.get("labels", {}).get("app.kubernetes.io/managed-by") != MANAGER or meta.get("deletionTimestamp") or not meta.get("uid") or not meta.get("resourceVersion") or meta.get("annotations", {}).get("transport.fugue.dev/phase") != "staged" or spec.get("externalIPs") or spec.get("type") != "ClusterIP" or spec.get("externalTrafficPolicy") or spec.get("loadBalancerIP"):
        raise ValueError("staging cannot modify a public or foreign Front Service")
    normalized = {key: spec.get(key) for key in target["spec"]}
    normalized["externalIPs"] = normalized["externalIPs"] or []
    normalized["publishNotReadyAddresses"] = bool(normalized["publishNotReadyAddresses"])
    if probe.digest(normalized) != meta["annotations"].get(DIGEST):
        raise ValueError("internal Front Service drifted from its declaration")
    generation = int(meta["annotations"].get(GENERATION, "0"))
    target_generation = int(target["metadata"]["annotations"][GENERATION])
    if generation > target_generation or generation == target_generation and meta["annotations"][DIGEST] != target["metadata"]["annotations"][DIGEST]:
        raise ValueError("internal Front stage generation replay or mutation")
    for owner in meta.get("managedFields", []):
        if owner.get("manager") != MANAGER and owner.get("fieldsV1", {}).get("f:spec"):
            raise ValueError("internal Front Service has a foreign field writer")


def endpoint_witness(config, service, profile, candidate):
    current = probe.kubectl("get", "service", service["name"], "-n", config["namespace"], "-o", "json")
    validate_existing(current, desired(config, service, profile))
    slices = front.read("get", "endpointslices", "-n", config["namespace"], "-l", "kubernetes.io/service-name=" + service["name"], "-o", "json")
    if slices.get("metadata", {}).get("continue"):
        raise ValueError("Front endpoint observation incomplete")
    local = []
    for item in slices["items"]:
        owners = item["metadata"].get("ownerReferences", [])
        if item["metadata"].get("deletionTimestamp") or not any(o.get("kind") == "Service" and o.get("uid") == current["metadata"]["uid"] and o.get("controller") is True for o in owners):
            raise ValueError("Front EndpointSlice has foreign ownership")
        if {(p.get("name"), p.get("protocol"), p.get("port")) for p in item.get("ports", [])} != {("http", "TCP", 80), ("https", "TCP", 443)}:
            raise ValueError("Front endpoint ports differ from declared listeners")
        for endpoint in item.get("endpoints", []):
            if endpoint.get("nodeName") == profile["node"]:
                local.append(endpoint)
    if len(local) != 1:
        raise ValueError("exactly one local Front endpoint required")
    endpoint = local[0]
    ref, conditions = endpoint.get("targetRef", {}), endpoint.get("conditions", {})
    if ref.get("kind") != "Pod" or ref.get("uid") != candidate["metadata"]["uid"] or ref.get("name") != candidate["metadata"]["name"] or ref.get("namespace") != config["namespace"] or conditions.get("ready") is not True or conditions.get("terminating") is True or endpoint.get("addresses") != [candidate["status"]["podIP"]]:
        raise ValueError("Front endpoint does not select the observed candidate")
    return {"service_uid": current["metadata"]["uid"], "service_version": current["metadata"]["resourceVersion"], "pod_uid": candidate["metadata"]["uid"], "address": candidate["status"]["podIP"]}


def profile(config, service):
    value = front.validate(json.loads(Path(service["observation"]).read_text()))
    if value["namespace"] != config["namespace"]:
        raise ValueError("internal stage and observation namespaces differ")
    return value


def verify_probe_listener(config, service, observation):
    declared = probe.validate(json.loads(Path("deploy/environments/production/front-probe-transport/listeners.json").read_text()))
    matches = [x for x in declared["listeners"] if x["name"] == service["probeListener"] and x["observation"] == service["observation"]]
    if len(matches) != 1 or declared["namespace"] != config["namespace"]:
        raise ValueError("Front internal stage lacks a declared probe listener")
    listener = matches[0]
    live = probe.current_service(declared, listener)
    target = probe.desired(declared, listener, observation)
    probe.validate_existing(live, target)
    if live is None or probe.mutation(live, target)[0] is not None:
        raise ValueError("Front probe listener has not converged")
    candidate = front.pod(observation, observation["candidateSelector"], True)
    for route in observation["probes"]:
        direct = front.proof(candidate["status"]["podIP"], route["host"], route["path"])
        external = front.proof(listener["address"], route["host"], route["path"], listener["port"])
        if direct != external:
            raise ValueError("public probe transport differs from the candidate")
    return candidate


def prepare(config):
    plans = []
    for service in config["services"]:
        observation = profile(config, service)
        candidate = verify_probe_listener(config, service, observation)
        evidence = front.observe(observation)
        target = desired(config, service, observation)
        current = probe.kubectl("get", "service", service["name"], "-n", config["namespace"], "-o", "json", "--ignore-not-found")
        validate_existing(current, target)
        if current is not None and current["metadata"]["annotations"][DIGEST] != target["metadata"]["annotations"][DIGEST]:
            raise ValueError("internal Front stage update requires a separate reviewed transition")
        if current is None:
            probe.kubectl("create", "--dry-run=server", "--field-manager=" + MANAGER, "-f", "-", "-o", "json", body=front.canonical(target))
        plans.append({"service": service, "profile_digest": probe.digest(observation), "candidate_uid": candidate["metadata"]["uid"], "evidence": evidence, "current": current, "desired": target})
    result = {"schema": "fugue.front-serving-stage-evidence/v1", "declaration_digest": probe.digest(config), "at": front.now().isoformat(), "plans": plans}
    result["digest"] = probe.digest(result)
    return result


def apply(config, witness):
    unsigned = dict(witness)
    recorded = unsigned.pop("digest", None)
    if recorded != probe.digest(unsigned) or witness.get("schema") != "fugue.front-serving-stage-evidence/v1" or witness.get("declaration_digest") != probe.digest(config) or not front.now()-datetime.timedelta(minutes=5) < front.timestamp(witness["at"]) <= front.now() or [p["service"] for p in witness.get("plans", [])] != config["services"]:
        raise ValueError("internal Front stage witness invalid")
    for plan in witness["plans"]:
        service = plan["service"]
        observation = profile(config, service)
        candidate = verify_probe_listener(config, service, observation)
        if probe.digest(observation) != plan["profile_digest"] or candidate["metadata"]["uid"] != plan["candidate_uid"] or desired(config, service, observation) != plan["desired"]:
            raise ValueError("candidate changed after internal stage observation")
        current = probe.kubectl("get", "service", service["name"], "-n", config["namespace"], "-o", "json", "--ignore-not-found")
        if current != plan["current"]:
            raise ValueError("internal Front Service changed after preparation")
        validate_existing(current, plan["desired"])
    for plan in witness["plans"]:
        service = plan["service"]
        observation = profile(config, service)
        if plan["current"] is None:
            probe.kubectl("create", "--field-manager=" + MANAGER, "-f", "-", "-o", "json", body=front.canonical(plan["desired"]))
        for attempt in range(10):
            try:
                candidate = front.pod(observation, observation["candidateSelector"], True)
                evidence = endpoint_witness(config, service, observation, candidate)
                print(front.canonical({"internal_front_ready": service["name"], "public_listener_changed": False, "evidence": evidence}), flush=True)
                break
            except ValueError:
                if attempt == 9:
                    raise
                time.sleep(2)


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
