#!/usr/bin/env python3
"""Fence public Front transport changes independently of code and connections."""
import argparse
import copy
import datetime
import http.client
import ipaddress
import json
from pathlib import Path
import re
import signal
import time

try:
    from . import observe_front_candidate as front
    from . import reconcile_front_probe_transport as probe
    from . import stage_front_serving_transport as stage
    from . import front_connection_witness as connection
except ImportError:
    import observe_front_candidate as front
    import reconcile_front_probe_transport as probe
    import stage_front_serving_transport as stage
    import front_connection_witness as connection


def validate(config):
    fields = {"schema", "namespace", "service", "address", "observation", "expectedGeneration", "generation", "rollbackGeneration", "verificationSeconds"}
    if set(config) != fields or config["schema"] != "fugue.front-serving-handoff/v1":
        raise ValueError("explicit Front handoff declaration required")
    for key in ["namespace", "service"]:
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", config[key]):
            raise ValueError("canonical Front handoff identity required")
    if any(type(config[k]) is not int for k in ["expectedGeneration", "generation", "rollbackGeneration", "verificationSeconds"]) or config["expectedGeneration"] < 1 or config["generation"] != config["expectedGeneration"]+1 or config["rollbackGeneration"] != config["generation"]+1 or not 15 <= config["verificationSeconds"] <= 120:
        raise ValueError("bounded handoff and explicit monotonic compensation generation required")
    address = ipaddress.ip_address(config["address"])
    if address.version != 4 or str(address) != config["address"] or not address.is_global or Path(config["observation"]).parent != Path("deploy/environments/production/front-observation") or Path(config["observation"]).suffix != ".json":
        raise ValueError("explicit public node address and observation profile required")
    return config


def load_stage(config):
    staging = stage.validate(json.loads(Path("deploy/environments/production/front-serving-stage/services.json").read_text()))
    members = [s for s in staging["services"] if s["name"] == config["service"] and s["observation"] == config["observation"]]
    if len(members) != 1 or staging["namespace"] != config["namespace"] or staging["generation"] != config["expectedGeneration"]:
        raise ValueError("public handoff does not name the exact internal stage")
    observation = stage.profile(staging, members[0])
    return staging, members[0], observation


def selected_spec(config, baseline):
    result = {key: copy.deepcopy(baseline["spec"].get(key)) for key in ["type", "selector", "ports"]}
    result.update(externalIPs=[config["address"]], externalTrafficPolicy="Local", publishNotReadyAddresses=False)
    return result


def target_annotations(config, current, spec, rollback=False):
    annotations = dict(current["metadata"].get("annotations", {}))
    annotations[stage.GENERATION] = str(config["rollbackGeneration"] if rollback else config["generation"])
    annotations[stage.DIGEST] = probe.digest(spec)
    annotations["transport.fugue.dev/phase"] = "staged" if rollback else "serving"
    if rollback:
        annotations["transport.fugue.dev/rollback-of-generation"] = str(config["generation"])
    return annotations


def cas_patch(current, spec, annotations):
    meta = current["metadata"]
    if not meta.get("uid") or not meta.get("resourceVersion"):
        raise ValueError("Front handoff lacks exact Service identity")
    patch = [{"op": "test", "path": "/metadata/uid", "value": meta["uid"]}, {"op": "test", "path": "/metadata/resourceVersion", "value": meta["resourceVersion"]}, {"op": "test", "path": "/spec", "value": current["spec"]}, {"op": "test", "path": "/metadata/annotations", "value": meta["annotations"]}, {"op": "test", "path": "/metadata/labels", "value": meta["labels"]}]
    for key in ["externalIPs", "externalTrafficPolicy", "publishNotReadyAddresses"]:
        if key not in spec:
            if key in current["spec"]:
                patch.append({"op": "remove", "path": "/spec/"+key})
        elif current["spec"].get(key) != spec[key]:
            patch.append({"op": "add", "path": "/spec/"+key, "value": spec[key]})
    patch.append({"op": "replace", "path": "/metadata/annotations", "value": annotations})
    return patch


def current(config):
    return front.read("get", "service", config["service"], "-n", config["namespace"], "-o", "json")


def require_original_listener_ownership(config, old):
    services = front.read("get", "services", "-A", "-o", "json")
    if services.get("metadata", {}).get("continue"):
        raise ValueError("public Front listener ownership observation is incomplete")
    for item in services["items"]:
        if item["metadata"].get("namespace") == config["namespace"] and item["metadata"].get("name") == config["service"]:
            continue
        addresses = set(item["spec"].get("externalIPs", [])) | {x.get("ip") for x in item.get("status", {}).get("loadBalancer", {}).get("ingress", [])}
        if config["address"] in addresses and any(p.get("protocol", "TCP") == "TCP" and p.get("port") in [80, 443] for p in item["spec"].get("ports", [])):
            raise ValueError("another Service already owns the public Front address")
    owned = []
    for container in old["spec"]["containers"]:
        for port in container.get("ports", []):
            if port.get("protocol", "TCP") == "TCP" and port.get("hostPort") in [80, 443]:
                if port.get("hostIP", "0.0.0.0") not in ["", "0.0.0.0", config["address"]]:
                    raise ValueError("legacy Front does not own the declared public address")
                owned.append(port["hostPort"])
    if sorted(owned) != [80, 443]:
        raise ValueError("legacy Front public host-port ownership is incomplete")


def write(config, observed, spec, annotations, dry_run=False):
    args = ["patch", "service", config["service"], "-n", config["namespace"], "--type=json", "--patch-file", "-", "--field-manager="+stage.MANAGER, "-o", "json"]
    if dry_run:
        args.append("--dry-run=server")
    return probe.kubectl(*args, body=front.canonical(cas_patch(observed, spec, annotations)))


def external_evidence(config, observation, evidence):
    transport = probe.validate(json.loads(Path("deploy/environments/production/front-probe-transport/listeners.json").read_text()))
    listeners = [x for x in transport["listeners"] if x["address"] == config["address"] and x["observation"] == config["observation"]]
    if len(listeners) != 1 or evidence.get("schema") != "fugue.front-external-probe-evidence/v1" or evidence.get("authorizes_traffic") is not False or evidence.get("declaration_digest") != probe.digest(transport):
        raise ValueError("Front handoff lacks independent external ingress evidence")
    rows = [r for r in evidence.get("observations", []) if r.get("listener") == listeners[0]["name"]]
    if len(rows) < 5 or len(rows) > 20:
        raise ValueError("Front external ingress observation window is incomplete")
    times = [front.timestamp(r["at"]) for r in rows]
    if times != sorted(times) or len(set(times)) != len(times) or not front.now()-datetime.timedelta(minutes=10) < times[0] <= times[-1] <= front.now() or (times[-1]-times[0]).total_seconds() < 45:
        raise ValueError("Front external ingress observations are stale or replayed")
    for row in rows:
        if row.get("profile_digest") != probe.digest(observation) or row.get("address") != config["address"] or row.get("port") != listeners[0]["port"] or [(x.get("host"), x.get("path")) for x in row.get("proofs", [])] != [(x["host"], x["path"]) for x in observation["probes"]] or any(not re.fullmatch(r"sha256:[0-9a-f]{64}", p.get("proof_digest", "")) for p in row["proofs"]):
            raise ValueError("Front external ingress evidence differs from declared route requirements")
    return listeners[0]


def redirect(address, host):
    client = http.client.HTTPConnection(address, 80, timeout=5)
    try:
        client.request("HEAD", "/", headers={"Host": host})
        response = client.getresponse()
        if response.status != 308 or response.getheader("Location") != "https://"+host+"/":
            raise ValueError("Front HTTP redirect differs from the existing entry contract")
    finally:
        client.close()


def prepare(config, external):
    staging, service, observation = load_stage(config)
    listener = external_evidence(config, observation, external)
    candidate = stage.verify_probe_listener(staging, service, observation)
    old = front.pod(observation, observation["legacySelector"], False)
    require_original_listener_ownership(config, old)
    before = current(config)
    stage.validate_existing(before, stage.desired(staging, service, observation))
    if int(before["metadata"]["annotations"][stage.GENERATION]) != config["expectedGeneration"]:
        raise ValueError("Front stage generation changed")
    endpoint = stage.endpoint_witness(staging, service, observation, candidate)
    observed = front.observe(observation)
    held = connection.observe_pair(observation, config["address"], listener["port"])
    for target in observation["probes"]:
        redirect(config["address"], target["host"])
        redirect(candidate["status"]["podIP"], target["host"])
    if current(config) != before or front.pod(observation, observation["candidateSelector"], True)["metadata"]["uid"] != candidate["metadata"]["uid"]:
        raise ValueError("Front stage changed during public handoff preparation")
    selected = selected_spec(config, before)
    annotations = target_annotations(config, before, selected)
    write(config, before, selected, annotations, dry_run=True)
    result = {"schema": "fugue.front-public-handoff-evidence/v1", "declaration_digest": probe.digest(config), "profile_digest": probe.digest(observation), "external_evidence_digest": probe.digest(external), "at": front.now().isoformat(), "baseline": before, "candidate_uid": candidate["metadata"]["uid"], "old_uid": old["metadata"]["uid"], "endpoint": endpoint, "observed": observed, "held": held, "selected_spec": selected, "selected_annotations": annotations}
    result["digest"] = probe.digest(result)
    return result


def compensate(config, baseline, applied):
    live = current(config)
    if live["metadata"]["uid"] != applied["metadata"]["uid"] or live["spec"] != applied["spec"] or live["metadata"]["annotations"] != applied["metadata"]["annotations"]:
        raise ValueError("public Front compensation conflicts with newer live authority")
    old_spec = {key: copy.deepcopy(baseline["spec"].get(key)) for key in ["type", "selector", "ports"]}
    old_spec.update(externalIPs=[], publishNotReadyAddresses=False)
    annotations = target_annotations(config, baseline, old_spec, rollback=True)
    return write(config, live, old_spec, annotations)


def recover(config, evidence):
    """Compensate a failed independent ingress check, only for our exact write."""
    unsigned = dict(evidence)
    digest = unsigned.pop("digest", None)
    if digest != probe.digest(unsigned) or evidence.get("schema") != "fugue.front-public-handoff-evidence/v1" or evidence.get("declaration_digest") != probe.digest(config) or not front.now()-datetime.timedelta(minutes=30) < front.timestamp(evidence["at"]) <= front.now():
        raise ValueError("public Front recovery witness is stale or changed")
    _, _, observation = load_stage(config)
    if probe.digest(observation) != evidence["profile_digest"]:
        raise ValueError("Front recovery profile changed")
    expected = selected_spec(config, evidence["baseline"])
    annotations = target_annotations(config, evidence["baseline"], expected)
    if evidence["selected_spec"] != expected or evidence["selected_annotations"] != annotations:
        raise ValueError("retained recovery mutation differs from declaration")
    live = current(config)
    baseline = evidence["baseline"]
    if live["metadata"]["uid"] != baseline["metadata"]["uid"]:
        raise ValueError("Front recovery conflicts with a replaced Service")
    if live["spec"] == baseline["spec"] and live["metadata"].get("annotations") == baseline["metadata"]["annotations"]:
        return {"result": "unchanged", "generation": config["expectedGeneration"], "service_uid": live["metadata"]["uid"]}
    old_spec = {key: copy.deepcopy(baseline["spec"].get(key)) for key in ["type", "selector", "ports"]}
    old_spec.update(externalIPs=[], publishNotReadyAddresses=False)
    if live["metadata"].get("annotations") == target_annotations(config, baseline, old_spec, rollback=True):
        actual_old = {key: live["spec"].get(key) for key in old_spec}
        actual_old["externalIPs"] = actual_old["externalIPs"] or []
        actual_old["publishNotReadyAddresses"] = bool(actual_old["publishNotReadyAddresses"])
        if actual_old == old_spec and not live["spec"].get("externalTrafficPolicy"):
            return {"result": "already_compensated", "generation": config["rollbackGeneration"], "service_uid": live["metadata"]["uid"]}
    old = front.pod(observation, observation["legacySelector"], False)
    if old["metadata"]["uid"] != evidence["old_uid"]:
        raise ValueError("previous Front executor changed before compensation")
    require_original_listener_ownership(config, old)
    for route in observation["probes"]:
        proof = front.proof(old["status"]["podIP"], route["host"], route["path"])
        if proof["edge"] != observation["node"] or proof["group"] != observation["group"]:
            raise ValueError("previous Front no longer proves the declared authority")
    live = current(config)
    actual = {k: live["spec"].get(k) for k in expected}
    actual["publishNotReadyAddresses"] = bool(actual["publishNotReadyAddresses"])
    if live["metadata"]["uid"] != evidence["baseline"]["metadata"]["uid"] or live["metadata"].get("annotations") != annotations or actual != expected:
        raise ValueError("Front recovery conflicts with a different live generation")
    restored = compensate(config, evidence["baseline"], live)
    # Both executors stay alive. Removing the public Service address only
    # restores the original host-port path for new connections.
    target = observation["probes"][0]
    for attempt in range(15):
        held = connection.HeldTLS(config["address"], target["host"])
        try:
            held.proof(target["path"])
            fact = connection.fact(observation, old, held)
            return {"result": "compensated", "generation": config["rollbackGeneration"], "service_uid": restored["metadata"]["uid"], "previous_front": fact, "both_fronts_retained": True}
        except ValueError:
            if attempt == 14:
                raise
            time.sleep(2)
        finally:
            held.close()


def apply(config, evidence, external):
    unsigned = dict(evidence)
    digest = unsigned.pop("digest", None)
    if digest != probe.digest(unsigned) or evidence.get("schema") != "fugue.front-public-handoff-evidence/v1" or evidence.get("declaration_digest") != probe.digest(config) or evidence.get("external_evidence_digest") != probe.digest(external) or not front.now()-datetime.timedelta(minutes=5) < front.timestamp(evidence["at"]) <= front.now():
        raise ValueError("public Front handoff witness is stale or changed")
    staging, service, observation = load_stage(config)
    external_evidence(config, observation, external)
    if probe.digest(observation) != evidence["profile_digest"]:
        raise ValueError("Front profile changed after handoff preparation")
    expected_spec = selected_spec(config, evidence["baseline"])
    if evidence["selected_spec"] != expected_spec or evidence["selected_annotations"] != target_annotations(config, evidence["baseline"], expected_spec):
        raise ValueError("retained Front mutation does not match the explicit declaration")
    old = front.pod(observation, observation["legacySelector"], False)
    require_original_listener_ownership(config, old)
    candidate = stage.verify_probe_listener(staging, service, observation)
    if old["metadata"]["uid"] != evidence["old_uid"] or candidate["metadata"]["uid"] != evidence["candidate_uid"] or current(config) != evidence["baseline"] or stage.endpoint_witness(staging, service, observation, candidate) != evidence["endpoint"]:
        raise ValueError("Front identities or internal Service endpoints changed before public CAS")
    activation = front.state(observation, old)
    if activation != front.state(observation, candidate) or activation != evidence["observed"]["observations"][-1]["activation"]:
        raise ValueError("Front activation changed before public CAS")
    target = observation["probes"][0]
    held, new = None, None
    applied = None
    attempted = False
    try:
        held = connection.HeldTLS(config["address"], target["host"])
        initial = held.proof(target["path"])
        original = connection.fact(observation, old, held)
        if initial != front.proof(candidate["status"]["podIP"], target["host"], target["path"]):
            raise ValueError("current public route differs immediately before transport CAS")
        attempted = True
        applied = write(config, evidence["baseline"], evidence["selected_spec"], evidence["selected_annotations"])
        # Never delete either Pod or remove a conntrack entry. Existing flows
        # retain their original executor; each new attempt is independently
        # checked against the candidate's exact client tuple.
        deadline = time.monotonic()+config["verificationSeconds"]
        observations = []
        while time.monotonic() < deadline:
            old_proof = held.proof(target["path"])
            if connection.fact(observation, old, held) != original:
                raise ValueError("original Front connection changed during public handoff")
            if new is None:
                new = connection.HeldTLS(config["address"], target["host"])
            new_proof = new.proof(target["path"])
            try:
                selected = connection.fact(observation, candidate, new)
            except ValueError:
                new.close()
                new = None
                time.sleep(2)
                continue
            if old_proof != new_proof or new_proof["edge"] != observation["node"] or new_proof["group"] != observation["group"]:
                raise ValueError("new public Front changed route authority")
            for route in observation["probes"]:
                if front.proof(config["address"], route["host"], route["path"]) != front.proof(candidate["status"]["podIP"], route["host"], route["path"]):
                    raise ValueError("public Front proof differs after handoff")
                redirect(config["address"], route["host"])
            observations.append({"at": front.now().isoformat(), "old_connection": original, "new_connection": selected, "old_requests": held.requests, "new_requests": new.requests})
            time.sleep(5)
        if len(observations) < 3 or new is None:
            raise ValueError("public Front did not converge with preserved old connection")
        live = current(config)
        if live["spec"] != applied["spec"] or live["metadata"]["uid"] != applied["metadata"]["uid"] or live["metadata"]["annotations"] != applied["metadata"]["annotations"] or front.pod(observation, observation["legacySelector"], False)["metadata"]["uid"] != evidence["old_uid"] or front.pod(observation, observation["candidateSelector"], True)["metadata"]["uid"] != evidence["candidate_uid"]:
            raise ValueError("Front identity changed during handoff verification")
        return {"schema": "fugue.front-public-handoff-result/v1", "result": "verified", "declaration_digest": probe.digest(config), "service_uid": live["metadata"]["uid"], "generation": config["generation"], "observations": observations, "old_front_retained": True}
    except BaseException:
        if attempted and applied is None:
            # A lost mutation response is not proof that Kubernetes rejected
            # the CAS. Recover only this exact generation and desired digest.
            live = current(config)
            if live["metadata"]["uid"] != evidence["baseline"]["metadata"]["uid"]:
                raise ValueError("Front mutation outcome conflicts with a replaced Service")
            if live["metadata"]["annotations"] == evidence["selected_annotations"]:
                projection = {k: live["spec"].get(k) for k in evidence["selected_spec"]}
                projection["publishNotReadyAddresses"] = bool(projection["publishNotReadyAddresses"])
                if projection != evidence["selected_spec"]:
                    raise ValueError("Front mutation response was lost and the live spec drifted")
                applied = live
            elif live["spec"] != evidence["baseline"]["spec"] or live["metadata"]["annotations"] != evidence["baseline"]["metadata"]["annotations"]:
                raise ValueError("Front mutation outcome conflicts with newer live state")
        if applied is not None:
            restored = compensate(config, evidence["baseline"], applied)
            print(front.canonical({"front_handoff_compensated": True, "generation": config["rollbackGeneration"], "service_uid": restored["metadata"]["uid"], "both_fronts_retained": True}), flush=True)
        raise
    finally:
        if held is not None:
            held.close()
        if new is not None:
            new.close()


def main():
    def interrupted(signum, frame):
        raise RuntimeError("Front handoff interrupted; recover the exact applied state")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["prepare", "apply", "recover"])
    parser.add_argument("config")
    parser.add_argument("--external-evidence")
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--result")
    args = parser.parse_args()
    config = validate(json.loads(Path(args.config).read_text()))
    if args.operation == "recover":
        result = recover(config, json.loads(Path(args.evidence).read_text()))
        print(front.canonical(result), flush=True)
        return
    if not args.external_evidence:
        parser.error("prepare/apply require independent external evidence")
    external = json.loads(Path(args.external_evidence).read_text())
    if args.operation == "prepare":
        Path(args.evidence).write_text(front.canonical(prepare(config, external))+"\n")
    else:
        if not args.result:
            raise ValueError("durable handoff result path required")
        result = apply(config, json.loads(Path(args.evidence).read_text()), external)
        Path(args.result).write_text(front.canonical(result)+"\n")
        print(front.canonical({"public_front_verified": True, "generation": config["generation"], "old_front_retained": True}))


if __name__ == "__main__":
    main()
