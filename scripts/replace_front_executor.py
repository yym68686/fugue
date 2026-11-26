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


PROBE_FOLLOW = "fugue.front-executor-probe-replacement/v1"
SERVING_READ_ONLY = "serving-read-only-v1"


def validate(config):
    fields = {"schema", "service", "serviceUID", "address", "expectedGeneration", "generation", "rollbackGeneration", "verificationSeconds", "observation"}
    mode = config.get("schema")
    if mode in {"fugue.front-executor-replacement/v2", PROBE_FOLLOW}:
        fields.update({"candidateAuthority", "candidateImageDigest"})
    if mode == PROBE_FOLLOW:
        fields.update({"probePort", "servingSource"})
    if set(config) != fields or mode not in {"fugue.front-executor-replacement/v1", "fugue.front-executor-replacement/v2", PROBE_FOLLOW}:
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
    if mode in {"fugue.front-executor-replacement/v2", PROBE_FOLLOW}:
        profile = config["observation"]
        allowed_authority = {"node-worker-v1", SERVING_READ_ONLY} if mode == PROBE_FOLLOW else {"node-worker-v1"}
        if (config["candidateAuthority"] not in allowed_authority
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", config["candidateImageDigest"])
                or profile["candidateSelector"].get("fugue.io/edge-node") != profile["node"]
                or profile["activationPath"] != "/var/lib/fugue-edge-front/activation.json"):
            raise ValueError("Front replacement needs an allowed authority, exact node selector and local state path")
    if mode == PROBE_FOLLOW:
        source = config["servingSource"]
        if (not isinstance(source, dict) or set(source) != {"service", "serviceUID", "generation", "podUID"}
                or any(not isinstance(source[k], str) or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", source[k]) for k in ["service", "serviceUID", "podUID"])
                or type(source["generation"]) is not int or source["generation"] < 1
                or source["service"] == config["service"] or source["serviceUID"] == config["serviceUID"]
                or type(config["probePort"]) is not int or not 1024 <= config["probePort"] <= 65535
                or profile["legacySelector"] == profile["candidateSelector"]
                or profile["legacySelector"].get("fugue.io/edge-node") != profile["node"]):
            raise ValueError("probe replacement requires a separate serving Service, exact local executors and non-serving port")
    return config


def current(config):
    extra = ["--show-managed-fields"] if "probePort" in config else []
    return front.read("get", "service", config["service"], "-n", config["observation"]["namespace"], "-o", "json", *extra)


def projection(spec, *, probe=False):
    keys = ["type", "selector", "ports", "externalIPs", "externalTrafficPolicy", "publishNotReadyAddresses"]
    if probe:
        keys.append("internalTrafficPolicy")
    result = {key: copy.deepcopy(spec.get(key)) for key in keys}
    result["publishNotReadyAddresses"] = bool(result["publishNotReadyAddresses"])
    return result


def baseline(config, value):
    meta, spec = value["metadata"], value["spec"]
    annotations = meta.get("annotations", {})
    if meta.get("uid") != config["serviceUID"] or meta.get("deletionTimestamp") or not meta.get("resourceVersion") or meta.get("name") != config["service"] or meta.get("namespace") != config["observation"]["namespace"]:
        raise ValueError("Service identity changed")
    if "probePort" in config:
        profile = config["observation"]
        listener = {"name": config["service"], "address": config["address"], "port": config["probePort"]}
        target = transport.desired({"namespace": profile["namespace"], "generation": config["expectedGeneration"]}, listener,
                                   dict(profile, candidateSelector=profile["legacySelector"]))
        transport.validate_existing(value, target)
        allowed_annotations = {stage.GENERATION, stage.DIGEST}
        # A previously migrated probe can follow the next executor, but only
        # for the same serving Service. Its Pod and generation are fenced
        # separately by serving_source.
        if annotations.get(transport.FOLLOWS_SERVING) == config["servingSource"]["serviceUID"]:
            allowed_annotations.add(transport.FOLLOWS_SERVING)
        prior_rollback = "transport.fugue.dev/rollback-of-generation"
        if annotations.get(prior_rollback) == str(config["expectedGeneration"] - 1):
            allowed_annotations.add(prior_rollback)
        if (not meta.get("managedFields") or set(annotations) != allowed_annotations
                or annotations.get(stage.GENERATION) != str(config["expectedGeneration"])
                or projection(spec, probe=True) != target["spec"]):
            raise ValueError("probe Service listener, owner, generation or selector differs from the exact baseline")
        return value
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
    annotations[stage.DIGEST] = transport.digest(projection(spec, probe="probePort" in config))
    if "probePort" in config and not rollback:
        annotations[transport.FOLLOWS_SERVING] = config["servingSource"]["serviceUID"]
        annotations.pop("transport.fugue.dev/rollback-of-generation", None)
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
    manager = transport.MANAGER if "probePort" in config else stage.MANAGER
    args = ["patch", "service", config["service"], "-n", config["observation"]["namespace"], "--type=json", "--patch-file", "-", "--field-manager=" + manager, "-o", "json"]
    if dry_run:
        args.append("--dry-run=server")
    return transport.kubectl(*args, body=front.canonical(cas_patch(observed, spec, annotations)))


def exact_state(live, previous, spec, annotations):
    return live["metadata"]["uid"] == previous["metadata"]["uid"] and live["spec"] == spec and live["metadata"].get("annotations") == annotations and live["metadata"].get("labels") == previous["metadata"].get("labels")


def executors(config):
    profile = config["observation"]
    # A node-local observation may find one Pod while the actual Service
    # selector matches additional Pods on other nodes. Reject before any CAS.
    old = front.pod(profile, profile["legacySelector"], False, singleton_selector=True)
    node_worker = config.get("candidateAuthority") == "node-worker-v1"
    candidate = front.pod(profile, profile["candidateSelector"], True, singleton_selector=True, node_worker=node_worker)
    if old["metadata"]["uid"] == candidate["metadata"]["uid"]:
        raise ValueError("replacement must retain two independent executors")
    if any(port.get("hostPort") for container in old["spec"]["containers"] for port in container.get("ports", [])) or old["spec"].get("hostNetwork"):
        raise ValueError("selector replacement requires an existing Service-owned public Front")
    if "candidateImageDigest" in config:
        declared = next(c["image"] for c in candidate["spec"]["containers"] if c["name"] == "edge-front")
        observed = next(c["imageID"] for c in candidate["status"]["containerStatuses"] if c["name"] == "edge-front")
        digest_suffix = "@" + config["candidateImageDigest"]
        if not declared.endswith(digest_suffix) or not observed.endswith(digest_suffix):
            raise ValueError("Front declared and running image must match the exact release")
    if node_worker:
        front.node_worker_recipient(profile, candidate)
    return old, candidate


def executor_identities(pods):
    return [(pod["metadata"]["uid"], pod["status"]["podIP"]) for pod in pods]


def endpoint(config, pod, service):
    profile = config["observation"]
    return stage.selected_endpoint_witness(profile, {"name": config["service"]}, profile, pod, service,
                                           **({"probe": True} if "probePort" in config else {}))


def serving_source(config, candidate):
    if "probePort" not in config:
        return None
    source, profile = config["servingSource"], config["observation"]
    if candidate["metadata"]["uid"] != source["podUID"]:
        raise ValueError("probe candidate is not the exact in-service Front")
    source_config = {"service": source["service"], "serviceUID": source["serviceUID"], "address": config["address"],
                     "expectedGeneration": source["generation"],
                     "observation": dict(profile, legacySelector=profile["candidateSelector"])}
    service = baseline(source_config, current(source_config))
    return {"service": service, "endpoint": endpoint(source_config, candidate, service)}


def require_source(config, candidate, expected):
    if serving_source(config, candidate) != expected:
        raise ValueError("public serving Service or endpoint changed during probe replacement")


def held_tls(config, host):
    if "probePort" in config:
        return connection.HeldTLS(config["address"], host, config["probePort"])
    return connection.HeldTLS(config["address"], host)


def listener_proof(config, target):
    if "probePort" in config:
        return front.proof(config["address"], target["host"], target["path"], config["probePort"])
    handoff.redirect(config["address"], target["host"])
    return front.proof(config["address"], target["host"], target["path"])


def prepare(config):
    profile = config["observation"]
    previous = baseline(config, current(config))
    old, candidate = executors(config)
    endpoint(config, old, previous)
    source = serving_source(config, candidate)
    observed = front.observe(profile, node_worker=config.get("candidateAuthority") == "node-worker-v1")
    for target in profile["probes"]:
        if source is None:
            handoff.redirect(config["address"], target["host"])
            handoff.redirect(candidate["status"]["podIP"], target["host"])
        elif listener_proof(config, target) != front.proof(candidate["status"]["podIP"], target["host"], target["path"]):
            raise ValueError("probe listener and serving candidate have different authority")
    if executor_identities(executors(config)) != executor_identities((old, candidate)) or current(config) != previous:
        raise ValueError("executor or Service changed during replacement preparation")
    require_source(config, candidate, source)
    spec, annotations = selected(config, previous)
    write(config, previous, spec, annotations, dry_run=True)
    result = {"schema": "fugue.front-executor-replacement-evidence/v1", "declaration_digest": transport.digest(config), "at": front.now().isoformat(), "baseline": previous,
              "old_uid": old["metadata"]["uid"], "candidate_uid": candidate["metadata"]["uid"], "observations": observed}
    if source is not None:
        result["serving_source"] = source
    result["digest"] = transport.digest(result)
    return result


def validate_evidence(config, evidence, minutes=5):
    unsigned = dict(evidence)
    digest = unsigned.pop("digest", None)
    if digest != transport.digest(unsigned) or evidence.get("schema") != "fugue.front-executor-replacement-evidence/v1" or evidence.get("declaration_digest") != transport.digest(config) or not front.now() - datetime.timedelta(minutes=minutes) < front.timestamp(evidence["at"]) <= front.now():
        raise ValueError("replacement evidence is stale or changed")
    baseline(config, evidence["baseline"])
    if "probePort" in config:
        expected = config["servingSource"]
        source = evidence.get("serving_source", {})
        if (source.get("service", {}).get("metadata", {}).get("uid") != expected["serviceUID"]
                or source.get("endpoint", {}).get("pod_uid") != expected["podUID"]
                or evidence["candidate_uid"] != expected["podUID"]):
            raise ValueError("probe replacement evidence omits the exact public serving baseline")


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
    old = front.pod(profile, profile["legacySelector"], False, singleton_selector=True)
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
            restored = held_tls(config, target["host"])
            proof = restored.proof(target["path"])
            fact = connection.fact(profile, old, restored)
            if proof["edge"] != profile["node"] or proof["group"] != profile["group"] or not exact_state(current(config), previous, restored_spec, restored_annotations):
                raise ValueError("restored public path changed authority")
            if "probePort" in config:
                endpoint(config, old, current(config))
            return {"result": "compensated", "generation": config["rollbackGeneration"], "both_executors_retained": True, "restored_connection": fact}
        except (ValueError, OSError):
            if attempt == 9:
                raise
            time.sleep(2)
        finally:
            if restored is not None:
                restored.close()


def same_publication(profile, first, second, repeat):
    """Require exact proofs, distinguishing a witnessed refresh from skew."""
    def pinned(proof):
        return proof.get("edge") == profile["node"] and proof.get("group") == profile["group"]

    if not pinned(first) or not pinned(second):
        raise ValueError("route proof lost the pinned Edge or authority")
    if first == second:
        return True
    after = repeat()
    if (not pinned(after) or not first.get("version") or not after.get("version")
            or first["version"] == after["version"] or second != after):
        differences = sorted(key for key in set(first) | set(second) | set(after)
                             if first.get(key) != second.get(key) or after.get(key) != second.get(key))
        versions = [proof.get("version") for proof in (first, second, after)]
        raise ValueError(f"route proofs differ outside a witnessed publication refresh: versions={versions} fields={differences}")
    # A signed publication advanced across the comparison on the same
    # endpoint. Do not count this sample; require later full equality.
    return False


def apply(config, evidence):
    validate_evidence(config, evidence)
    previous, profile = evidence["baseline"], config["observation"]
    old, candidate = executors(config)
    if current(config) != previous or old["metadata"]["uid"] != evidence["old_uid"] or candidate["metadata"]["uid"] != evidence["candidate_uid"]:
        raise ValueError("executor or Service changed before replacement")
    source = evidence.get("serving_source")
    require_source(config, candidate, source)
    if source is not None:
        endpoint(config, old, previous)
    activation = front.state(profile, old)
    if activation != front.state(profile, candidate) or activation != evidence["observations"]["observations"][-1]["activation"]:
        raise ValueError("activation changed before replacement")
    target = profile["probes"][0]
    held, fresh = None, None
    attempted = False
    try:
        held = held_tls(config, target["host"])
        proof = held.proof(target["path"])
        original = connection.fact(profile, old, held)
        if proof != front.proof(candidate["status"]["podIP"], target["host"], target["path"]):
            raise ValueError("candidate authority differs before replacement")
        if executor_identities(executors(config)) != executor_identities((old, candidate)):
            raise ValueError("executor changed during pre-switch connection proof")
        require_source(config, candidate, source)
        spec, annotations = selected(config, previous)
        attempted = True
        write(config, previous, spec, annotations)
        deadline = time.monotonic() + config["verificationSeconds"]
        observations, publication_retries = [], 0
        while time.monotonic() < deadline:
            old_proof = held.proof(target["path"])
            if connection.fact(profile, old, held) != original:
                raise ValueError("existing public connection changed during replacement")
            if fresh is None:
                fresh = held_tls(config, target["host"])
            new_proof = fresh.proof(target["path"])
            try:
                new_fact = connection.fact(profile, candidate, fresh)
            except ValueError:
                fresh.close()
                fresh = None
                time.sleep(2)
                continue
            if source is not None and any(new_fact[key] != original[key] for key in ["slot", "target"]):
                raise ValueError("probe replacement changed the selected Worker")
            if not same_publication(profile, old_proof, new_proof, lambda: held.proof(target["path"])):
                publication_retries += 1
                continue
            for route in profile["probes"]:
                if not same_publication(profile, listener_proof(config, route),
                                        front.proof(candidate["status"]["podIP"], route["host"], route["path"]),
                                        lambda: listener_proof(config, route)):
                    publication_retries += 1
                    break
            else:
                observations.append({"at": front.now().isoformat(), "old_connection": original, "new_connection": new_fact})
            time.sleep(5)
        if len(observations) < 3:
            raise ValueError("replacement did not converge in the bounded window")
        latest_old, latest_candidate = executors(config)
        live = current(config)
        if latest_old["metadata"]["uid"] != evidence["old_uid"] or latest_candidate["metadata"]["uid"] != evidence["candidate_uid"] or not exact_state(live, previous, spec, annotations):
            raise ValueError("executor identity or Service changed during verification")
        endpoint(config, latest_candidate, live)
        require_source(config, latest_candidate, source)
        result = {"schema": "fugue.front-executor-replacement-result/v1", "result": "verified", "declaration_digest": transport.digest(config), "generation": config["generation"], "both_executors_retained": True, "observations": observations, "publication_retries": publication_retries}
        if source is not None:
            if front.state(profile, latest_old) != activation or front.state(profile, latest_candidate) != activation:
                raise ValueError("Worker activation changed during probe replacement")
            result.update(listener_port=config["probePort"], serving_source=source, activation=activation)
        return result
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
            value = listener_proof(config, route)
            if value["edge"] != profile["node"] or value["group"] != profile["group"]:
                raise ValueError("external ingress changed physical route authority")
            proofs.append({"host": route["host"], "path": route["path"], "proof": value})
        observations.append({"at": front.now().isoformat(), "proofs": proofs})
        if index < 4:
            time.sleep(15)
    result = {"schema": "fugue.front-executor-external-evidence/v1", "declaration_digest": transport.digest(config), "observations": observations}
    if "probePort" in config:
        result["listener_port"] = config["probePort"]
    return result


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
