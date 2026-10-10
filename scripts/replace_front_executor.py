"""Replace only a declared Front Service selector, retaining both executors."""
import argparse
import copy
import datetime
import ipaddress
import json
from pathlib import Path
import re
import signal
import time

from scripts import front_connection_witness as connection
from scripts import handoff_front_serving_transport as handoff
from scripts import observe_front_candidate as front
from scripts import reconcile_front_probe_transport as transport
from scripts import stage_front_serving_transport as stage


def validate(config):
    fields = {"schema", "service", "serviceUID", "address", "expectedGeneration", "generation", "rollbackGeneration", "verificationSeconds", "observation"}
    if set(config) != fields or config["schema"] != "fugue.front-executor-replacement/v1":
        raise ValueError("explicit executor replacement declaration required")
    for key in ["service", "serviceUID"]:
        if not isinstance(config[key], str) or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", config[key]):
            raise ValueError("exact Service identity required")
    for key in ["expectedGeneration", "generation", "rollbackGeneration", "verificationSeconds"]:
        if type(config[key]) is not int:
            raise ValueError("explicit integer replacement fences required")
    if config["expectedGeneration"] < 1 or config["generation"] != config["expectedGeneration"] + 1 or config["rollbackGeneration"] != config["generation"] + 1 or not 20 <= config["verificationSeconds"] <= 120:
        raise ValueError("bounded verification and consecutive compensation generation required")
    address = ipaddress.ip_address(config["address"])
    if not address.is_global or address.version != 4 or str(address) != config["address"]:
        raise ValueError("explicit public IPv4 required")
    front.validate(config["observation"])
    return config


def current(config):
    return front.read("get", "service", config["service"], "-n", config["observation"]["namespace"], "-o", "json")


def projection(spec):
    result = {key: copy.deepcopy(spec.get(key)) for key in ["type", "selector", "ports", "externalIPs", "externalTrafficPolicy", "publishNotReadyAddresses"]}
    result["publishNotReadyAddresses"] = bool(result["publishNotReadyAddresses"])
    return result


def baseline(config, value):
    meta, spec = value["metadata"], value["spec"]
    annotations = meta.get("annotations", {})
    if meta.get("uid") != config["serviceUID"] or meta.get("deletionTimestamp") or not meta.get("resourceVersion") or meta.get("name") != config["service"] or meta.get("namespace") != config["observation"]["namespace"]:
        raise ValueError("Service identity changed")
    if meta.get("labels", {}).get("app.kubernetes.io/managed-by") != stage.MANAGER or annotations.get(stage.GENERATION) != str(config["expectedGeneration"]) or annotations.get("transport.fugue.dev/phase") != "serving" or annotations.get(stage.DIGEST) != transport.digest(projection(spec)):
        raise ValueError("Service ownership, generation or serving digest changed")
    if spec.get("selector") != config["observation"]["legacySelector"] or spec.get("type") != "ClusterIP" or spec.get("externalIPs") != [config["address"]] or spec.get("externalTrafficPolicy") != "Local" or spec.get("publishNotReadyAddresses", False):
        raise ValueError("Service serving path differs from declared baseline")
    if {(port.get("port"), port.get("targetPort"), port.get("protocol", "TCP")) for port in spec.get("ports", [])} != {(80, 80, "TCP"), (443, 443, "TCP")} or len(spec["ports"]) != 2:
        raise ValueError("Service listener contract is unexpected")
    return value


def selected(config, previous, rollback=False):
    spec = copy.deepcopy(previous["spec"])
    if not rollback:
        spec["selector"] = config["observation"]["candidateSelector"]
    annotations = copy.deepcopy(previous["metadata"]["annotations"])
    annotations[stage.GENERATION] = str(config["rollbackGeneration"] if rollback else config["generation"])
    annotations[stage.DIGEST] = transport.digest(projection(spec))
    if rollback:
        annotations["transport.fugue.dev/rollback-of-generation"] = str(config["generation"])
    return spec, annotations


def cas_patch(observed, spec, annotations):
    preserved = copy.deepcopy(observed["spec"])
    preserved["selector"] = spec["selector"]
    if preserved != spec:
        raise ValueError("executor replacement may change only the selector")
    patch = [{"op": "test", "path": "/metadata/" + key, "value": observed["metadata"][key]} for key in ["uid", "resourceVersion", "annotations", "labels"]]
    patch.append({"op": "test", "path": "/spec", "value": observed["spec"]})
    patch.extend([{"op": "replace", "path": "/spec/selector", "value": spec["selector"]}, {"op": "replace", "path": "/metadata/annotations", "value": annotations}])
    return patch


def write(config, observed, spec, annotations, dry_run=False):
    args = ["patch", "service", config["service"], "-n", config["observation"]["namespace"], "--type=json", "--patch-file", "-", "--field-manager=" + stage.MANAGER, "-o", "json"]
    if dry_run:
        args.append("--dry-run=server")
    return transport.kubectl(*args, body=front.canonical(cas_patch(observed, spec, annotations)))


def exact_state(live, previous, spec, annotations):
    return live["metadata"]["uid"] == previous["metadata"]["uid"] and live["spec"] == spec and live["metadata"].get("annotations") == annotations and live["metadata"].get("labels") == previous["metadata"].get("labels")


def executors(config):
    profile = config["observation"]
    old = front.pod(profile, profile["legacySelector"], False)
    candidate = front.pod(profile, profile["candidateSelector"], True)
    if old["metadata"]["uid"] == candidate["metadata"]["uid"]:
        raise ValueError("replacement must retain two independent executors")
    if any(port.get("hostPort") for container in old["spec"]["containers"] for port in container.get("ports", [])) or old["spec"].get("hostNetwork"):
        raise ValueError("selector replacement requires an existing Service-owned public Front")
    return old, candidate


def prepare(config):
    profile = config["observation"]
    previous = baseline(config, current(config))
    old, candidate = executors(config)
    stage.selected_endpoint_witness(profile, {"name": config["service"]}, profile, old, previous)
    observed = front.observe(profile)
    for target in profile["probes"]:
        handoff.redirect(config["address"], target["host"])
        handoff.redirect(candidate["status"]["podIP"], target["host"])
    if current(config) != previous:
        raise ValueError("Service changed during replacement preparation")
    spec, annotations = selected(config, previous)
    write(config, previous, spec, annotations, dry_run=True)
    result = {"schema": "fugue.front-executor-replacement-evidence/v1", "declaration_digest": transport.digest(config), "at": front.now().isoformat(), "baseline": previous,
              "old_uid": old["metadata"]["uid"], "candidate_uid": candidate["metadata"]["uid"], "observations": observed}
    result["digest"] = transport.digest(result)
    return result


def validate_evidence(config, evidence, minutes=5):
    unsigned = dict(evidence)
    digest = unsigned.pop("digest", None)
    if digest != transport.digest(unsigned) or evidence.get("schema") != "fugue.front-executor-replacement-evidence/v1" or evidence.get("declaration_digest") != transport.digest(config) or not front.now() - datetime.timedelta(minutes=minutes) < front.timestamp(evidence["at"]) <= front.now():
        raise ValueError("replacement evidence is stale or changed")
    baseline(config, evidence["baseline"])


def recover(config, evidence):
    validate_evidence(config, evidence, minutes=30)
    previous = evidence["baseline"]
    live = current(config)
    if exact_state(live, previous, previous["spec"], previous["metadata"]["annotations"]):
        return {"result": "unchanged"}
    restored_spec, restored_annotations = selected(config, previous, rollback=True)
    if exact_state(live, previous, restored_spec, restored_annotations):
        return {"result": "already_compensated"}
    spec, annotations = selected(config, previous)
    if not exact_state(live, previous, spec, annotations):
        raise ValueError("replacement recovery conflicts with a different live writer")
    profile = config["observation"]
    old = front.pod(profile, profile["legacySelector"], False)
    if old["metadata"]["uid"] != evidence["old_uid"]:
        raise ValueError("previous Front was replaced before recovery")
    for target in profile["probes"]:
        proof = front.proof(old["status"]["podIP"], target["host"], target["path"])
        if proof["edge"] != profile["node"] or proof["group"] != profile["group"]:
            raise ValueError("previous Front no longer serves declared authority")
    write(config, live, restored_spec, restored_annotations)
    target = profile["probes"][0]
    for attempt in range(10):
        restored = None
        try:
            restored = connection.HeldTLS(config["address"], target["host"])
            proof = restored.proof(target["path"])
            fact = connection.fact(profile, old, restored)
            if proof["edge"] != profile["node"] or proof["group"] != profile["group"] or not exact_state(current(config), previous, restored_spec, restored_annotations):
                raise ValueError("restored public path changed authority")
            return {"result": "compensated", "generation": config["rollbackGeneration"], "both_executors_retained": True, "restored_connection": fact}
        except (ValueError, OSError):
            if attempt == 9:
                raise
            time.sleep(2)
        finally:
            if restored is not None:
                restored.close()


def apply(config, evidence):
    validate_evidence(config, evidence)
    previous, profile = evidence["baseline"], config["observation"]
    old, candidate = executors(config)
    if current(config) != previous or old["metadata"]["uid"] != evidence["old_uid"] or candidate["metadata"]["uid"] != evidence["candidate_uid"]:
        raise ValueError("executor or Service changed before replacement")
    activation = front.state(profile, old)
    if activation != front.state(profile, candidate) or activation != evidence["observations"]["observations"][-1]["activation"]:
        raise ValueError("activation changed before replacement")
    target = profile["probes"][0]
    held, fresh = None, None
    attempted = False
    try:
        held = connection.HeldTLS(config["address"], target["host"])
        proof = held.proof(target["path"])
        original = connection.fact(profile, old, held)
        if proof != front.proof(candidate["status"]["podIP"], target["host"], target["path"]):
            raise ValueError("candidate authority differs before replacement")
        spec, annotations = selected(config, previous)
        attempted = True
        write(config, previous, spec, annotations)
        deadline = time.monotonic() + config["verificationSeconds"]
        observations = []
        while time.monotonic() < deadline:
            old_proof = held.proof(target["path"])
            if connection.fact(profile, old, held) != original:
                raise ValueError("existing public connection changed during replacement")
            if fresh is None:
                fresh = connection.HeldTLS(config["address"], target["host"])
            new_proof = fresh.proof(target["path"])
            try:
                new_fact = connection.fact(profile, candidate, fresh)
            except ValueError:
                fresh.close()
                fresh = None
                time.sleep(2)
                continue
            if old_proof != new_proof or new_proof["edge"] != profile["node"] or new_proof["group"] != profile["group"]:
                raise ValueError("new public connection differs from preserved authority")
            for route in profile["probes"]:
                handoff.redirect(config["address"], route["host"])
                if front.proof(config["address"], route["host"], route["path"]) != front.proof(candidate["status"]["podIP"], route["host"], route["path"]):
                    raise ValueError("public route differs after executor replacement")
            observations.append({"at": front.now().isoformat(), "old_connection": original, "new_connection": new_fact})
            time.sleep(5)
        if len(observations) < 3:
            raise ValueError("replacement did not converge in the bounded window")
        latest_old, latest_candidate = executors(config)
        live = current(config)
        if latest_old["metadata"]["uid"] != evidence["old_uid"] or latest_candidate["metadata"]["uid"] != evidence["candidate_uid"] or not exact_state(live, previous, spec, annotations):
            raise ValueError("executor identity or Service changed during verification")
        stage.selected_endpoint_witness(profile, {"name": config["service"]}, profile, candidate, live)
        return {"schema": "fugue.front-executor-replacement-result/v1", "result": "verified", "declaration_digest": transport.digest(config), "generation": config["generation"], "both_executors_retained": True, "observations": observations}
    except BaseException:
        if attempted:
            print(front.canonical(recover(config, evidence)), flush=True)
        raise
    finally:
        if held is not None:
            held.close()
        if fresh is not None:
            fresh.close()


def external(config):
    profile, observations = config["observation"], []
    for index in range(5):
        proofs = []
        for route in profile["probes"]:
            value = front.proof(config["address"], route["host"], route["path"])
            if value["edge"] != profile["node"] or value["group"] != profile["group"]:
                raise ValueError("external ingress changed physical route authority")
            handoff.redirect(config["address"], route["host"])
            proofs.append({"host": route["host"], "path": route["path"], "proof": value})
        observations.append({"at": front.now().isoformat(), "proofs": proofs})
        if index < 4:
            time.sleep(15)
    return {"schema": "fugue.front-executor-external-evidence/v1", "declaration_digest": transport.digest(config), "observations": observations}


def main():
    def interrupted(signum, frame):
        raise RuntimeError("replacement interrupted; compensate only the exact applied state")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["prepare", "apply", "recover", "external", "validate"])
    parser.add_argument("config")
    parser.add_argument("--evidence")
    parser.add_argument("--result")
    args = parser.parse_args()
    config = validate(json.loads(Path(args.config).read_text()))
    if args.operation == "validate":
        return
    if args.operation in ["prepare", "external"]:
        result = prepare(config) if args.operation == "prepare" else external(config)
        Path(args.evidence).write_text(front.canonical(result) + "\n")
    else:
        evidence = json.loads(Path(args.evidence).read_text())
        result = apply(config, evidence) if args.operation == "apply" else recover(config, evidence)
        if args.result:
            Path(args.result).write_text(front.canonical(result) + "\n")
        print(front.canonical(result))


if __name__ == "__main__":
    main()
