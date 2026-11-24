#!/usr/bin/env python3
"""Reconcile explicitly declared DNS network listeners, without changing Pods.

Services supply node-local transport only. DNS records, answers, policy and
runtime probes remain owned by the current signed artifact and its consumers.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import datetime
import hashlib
import ipaddress
import json
import os
import re
import signal
import subprocess
import tempfile
import time
from pathlib import Path

MANAGER = "fugue-dns-transport"
GENERATION = "transport.fugue.dev/generation"
DIGEST = "transport.fugue.dev/digest"
HANDOFF_ATTEMPTS = 6
HANDOFF_RETRY_SECONDS = 5
RECOVERY_ATTEMPTS = 24


class ElapsedInTransit(ValueError):
    """A positive proof was valid at capture, but elapsed before comparison."""


class ObservationUnavailable(RuntimeError):
    """A read-only Kubernetes observation is temporarily unavailable."""


class PublicDNSVerificationFailed(RuntimeError):
    """An actual authoritative UDP/TCP sample failed after a switch."""

    def __init__(self, samples):
        super().__init__("post-handoff public UDP/TCP verification failed")
        self.samples = [{key: item.get(key) for key in ("hostname", "type", "transport", "reason", "response_code")}
                        for item in samples]


def valid_listener_selector(selector):
    return isinstance(selector, dict) and 1 <= len(selector) <= 16 and all(
        isinstance(k, str) and isinstance(v, str) and k and v and not any(c in k + v for c in "\n\r,= ")
        for k, v in selector.items())


def load_config(path, *, preview=False):
    config = json.loads(Path(path).read_text())
    if preview:
        attempt = config.pop("preview_attempt", 1)
        if type(attempt) is not int or not 1 <= attempt <= 10000:
            raise ValueError("invalid read-only preview attempt")
    fields = {"apiVersion", "kind", "generation", "namespace", "listeners"}
    if config.get("apiVersion") == "transport.fugue.dev/v2":
        fields.add("recovery")
    version = config.get("apiVersion")
    if version == "transport.fugue.dev/v3":
        fields.update({"recoveries", "verifier_revision"})
    if version == "transport.fugue.dev/v4":
        fields.add("handoffs")
        if "rollback_generation" in config:
            fields.add("rollback_generation")
    if set(config) != fields:
        raise ValueError("unknown or missing transport configuration fields")
    if version not in {f"transport.fugue.dev/v{n}" for n in (1, 2, 3, 4)} or config["kind"] != "DNSLocalTransport":
        raise ValueError("invalid transport schema")
    if type(config["generation"]) is not int or config["generation"] <= 0:
        raise ValueError("positive integer generation is required")
    if "rollback_generation" in config and (type(config["rollback_generation"]) is not int or
            config["rollback_generation"] != config["generation"] + 1):
        raise ValueError("staged DNS compensation requires a reserved next generation")
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
        if not valid_listener_selector(selector):
            raise ValueError("explicit bounded selector is required")
    if "recovery" in config:
        from scripts.recover_dns_transport import validate
        validate(config)
    if "recoveries" in config:
        from scripts.recover_dns_transport import validate
        if not re.fullmatch(r'[a-f0-9]{40}', config['verifier_revision']):
            raise ValueError('verified reader source revision required')
        recoveries = config['recoveries']
        if not isinstance(recoveries, list) or not 1 <= len(recoveries) <= len(listeners):
            raise ValueError('bounded nonempty listener recoveries required')
        seen = set()
        for recovery in recoveries:
            validate(dict(config, recovery=recovery))
            if recovery['listener'] in seen:
                raise ValueError('duplicate listener recovery')
            seen.add(recovery['listener'])
    if "handoffs" in config:
        stages = config["handoffs"]
        if not isinstance(stages, list) or len(stages) > 8:
            raise ValueError("bounded DNS handoff sequence required")
        if not stages and "rollback_generation" in config:
            raise ValueError("DNS transport hold cannot reserve a compensation generation")
        seen = set()
        for stage in stages:
            mandatory = {"address", "previous_revision", "candidate_revision"}
            if not isinstance(stage, dict) or (set(stage) - {"recovery_fallback", "failed_predecessor"}) != mandatory:
                raise ValueError("DNS handoff stage requires an address and exact revisions")
            address = stage["address"]
            public = [x for x in listeners if x["address"] == address and x["port"] == 53]
            if not isinstance(address, str) or len(public) != 1 or address in seen:
                raise ValueError("each DNS handoff stage must select a distinct public listener")
            stage_diagnostic(config, address)
            seen.add(address)
            for key in ("previous_revision", "candidate_revision"):
                if not isinstance(stage[key], str) or not re.fullmatch(r"[a-f0-9]{40}", stage[key]):
                    raise ValueError("exact DNS handoff revision required")
            if "recovery_fallback" in stage:
                fallback = stage["recovery_fallback"]
                if (not isinstance(fallback, dict) or set(fallback) != {"selector", "revision", "pod_uid"} or
                        not valid_listener_selector(fallback["selector"]) or fallback["selector"] == public[0]["selector"] or
                        not isinstance(fallback["revision"], str) or not re.fullmatch(r"[a-f0-9]{40}", fallback["revision"]) or
                        not isinstance(fallback["pod_uid"], str) or not re.fullmatch(
                            r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}", fallback["pod_uid"])):
                    raise ValueError("DNS recovery fallback must declare a distinct selector, revision and Pod UID")
            if "failed_predecessor" in stage:
                from scripts.dns_transport_failed_predecessor import validate_stage
                if "rollback_generation" not in config:
                    raise ValueError("DNS failed predecessor recovery requires a reserved compensation generation")
                validate_stage(stage)
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
        if args and args[0] == "get" and result.stderr.strip().startswith("Error from server (ServiceUnavailable)"):
            raise ObservationUnavailable("DNS handoff observation temporarily unavailable")
        raise RuntimeError(result.stderr.strip())
    return result.stdout


def read_json(*args):
    read_only = args and args[0] == 'get'
    if args and args[0] == 'exec' and '--' in args:
        command = args[args.index('--') + 1:]
        read_only = len(command) == 2 and command[0] == 'cat' and command[1].startswith('/')
    try:
        raw = kubectl(*args)
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        message = str(error)
        if read_only and (isinstance(error, subprocess.TimeoutExpired) or
                          'error sending request:' in message and message.rstrip().endswith('EOF') or
                          'connection reset by peer' in message or 'TLS handshake timeout' in message):
            raise ObservationUnavailable('DNS observation read transport temporarily unavailable') from error
        raise
    if len(raw) > 8 << 20:
        raise ValueError("DNS transport observation exceeds limit")
    return json.loads(raw)


def ready_pod(pod):
    return (not pod.get("metadata", {}).get("deletionTimestamp") and
            pod.get("status", {}).get("phase") == "Running" and
            any(c.get("type") == "Ready" and c.get("status") == "True"
                for c in pod.get("status", {}).get("conditions", [])))


def handoff_backend(namespace, selector, node_name, target_port, *, read_runtime=True):
    query = ",".join(k + "=" + v for k, v in sorted(selector.items()))
    pods = read_json("get", "pods", "-n", namespace, "-l", query, "-o", "json")["items"]
    local = [p for p in pods if p.get("spec", {}).get("nodeName") == node_name]
    if len(local) != 1 or not ready_pod(local[0]):
        raise ValueError("DNS handoff requires exactly one live Ready backend per selector on the listener node")
    return bound_backend(namespace, local[0], target_port, read_runtime=read_runtime)


def bound_backend(namespace, pod, target_port, *, read_runtime):
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
    snapshot = read_json("get", "--raw", path) if read_runtime else None
    current = read_json("get", "pod", meta["name"], "-n", namespace, "-o", "json")
    if current["metadata"].get("uid") != meta["uid"] or current["metadata"].get("resourceVersion") != meta["resourceVersion"]:
        raise ValueError("DNS handoff backend changed while reading runtime facts")
    return {"pod": pod, "snapshot": snapshot, "snapshot_path": path}


def validate_handoff_snapshots(old, candidate, node_name, now=None, *, serving_pair_inputs=None):
    now = now or datetime.datetime.now(datetime.timezone.utc)
    elapsed_in_transit = False
    if serving_pair_inputs is not None and (
            set(serving_pair_inputs) != {"candidate", "fallback"} or
            serving_pair_inputs["candidate"].get("snapshot") is not old or
            serving_pair_inputs["fallback"].get("snapshot") is not candidate):
        raise ValueError("DNS serving pair verifier inputs do not bind the compared snapshots")
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
            if not digest(identity) or identity in seen:
                raise ValueError("DNS handoff requires unique positive proofs")
            seen.add(identity)
            if fact.get("ready") is not True:
                continue
            # No one can repair a false positive at capture. In transit a
            # redundant proof may elapse. Only a new cryptographic check of
            # both full inputs at one verifier time can decide if it mattered.
            expires = timestamp(proof["valid_until"])
            if not timestamp(proof["checked_at"]) <= evaluated < expires:
                raise ValueError("DNS handoff proof is expired or from the future")
            if expires <= now and serving_pair_inputs is None:
                elapsed_in_transit = True
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
    if elapsed_in_transit:
        raise ElapsedInTransit("DNS handoff proof is expired or from the future")
    if serving_pair_inputs is not None:
        from scripts.recover_dns_transport import verify_observation_input
        verify_observation_input(serving_pair_inputs, None, ["--verify-serving-pair"], '{"verified_serving_pair":true}')


def staged_backend_binding(candidate, namespace, port):
    pod = candidate["pod"]
    images = [c["image"] for c in pod["spec"]["containers"]
              if all(any(p.get("containerPort") == port and p.get("protocol", "TCP") == protocol
                         for p in c.get("ports", [])) for protocol in ("UDP", "TCP"))]
    if len(images) != 1 or not re.fullmatch(r"[^\s@]+@sha256:[a-f0-9]{64}", images[0]):
        raise ValueError("immutable DNS handoff backend image required")
    return {"namespace": namespace, "recovery": {"candidate_image": images[0],
               "candidate_pod_uid": pod["metadata"]["uid"]}}


def verify_staged_backend(candidate, namespace, port, verifier=None, *, degraded_predecessor=False):
    from scripts.recover_dns_transport import DNSObservationRejected, verify_candidate

    pod = candidate["pod"]
    binding = staged_backend_binding(candidate, namespace, port)
    try:
        verify_candidate(candidate, binding, verifier)
    except DNSObservationRejected as error:
        # The verifier already authenticated the authority, complete probe
        # membership, and every positive proof before testing materialization.
        if not degraded_predecessor or error.category != "wire_materialization":
            raise
    # The verifier takes a new checkpoint/runtime pair, then the caller
    # compares that exact snapshot with the live predecessor assignment.
    observed = read_json("get", "pod", pod["metadata"]["name"], "-n", namespace, "-o", "json")
    if not ready_pod(observed) or any(observed["metadata"].get(k) != pod["metadata"][k]
                                     for k in ("uid", "resourceVersion")):
        raise ValueError("DNS handoff backend changed while reading runtime facts")


def verify_handoff_backend(role, backend, namespace, port, *, recovery=False):
    from scripts.recover_dns_transport import DNSObservationRejected

    try:
        if recovery and role == "predecessor":
            verify_staged_backend(backend, namespace, port, degraded_predecessor=True)
        else:
            verify_staged_backend(backend, namespace, port)
    except DNSObservationRejected as error:
        error.backend_role = role
        raise


def recheck_ordinary_serving_pair(namespace, node, previous, candidate, previous_port, candidate_port):
    """Private verifier inputs never enter a witness, artifact, or log."""
    from scripts.recover_dns_transport import candidate_request
    def read(backend, port):
        copy = dict(backend)  # candidate_request replaces the snapshot
        request = candidate_request(copy, staged_backend_binding(copy, namespace, port))
        before, after = backend["snapshot"], request["snapshot"]
        for k in ("node_id", "edge_group_id", "assignment", "parent_digest", "route_artifact_id", "plan_digest"):
            if before[k] != after[k]:
                raise ValueError("DNS handoff authority changed during paired read")
        if {f["probe_id"] for f in before["facts"]} != {f["probe_id"] for f in after["facts"]}:
            raise ValueError("DNS handoff probe membership changed during paired read")
        return request
    with ThreadPoolExecutor(max_workers=2) as pool:
        a = pool.submit(read, previous, previous_port)
        b = pool.submit(read, candidate, candidate_port)
        pair = {"candidate": a.result(), "fallback": b.result()}
    validate_handoff_snapshots(pair["candidate"]["snapshot"], pair["fallback"]["snapshot"], node,
                               serving_pair_inputs=pair)
    for backend in (previous, candidate):
        meta = backend["pod"]["metadata"]
        pod = read_json("get", "pod", meta["name"], "-n", namespace, "-o", "json")
        if not ready_pod(pod) or any(not meta.get(k) or pod["metadata"].get(k) != meta[k] for k in ("uid", "resourceVersion")):
            raise ValueError("DNS handoff backend changed during paired read")


def recovery_fallback_backend(config, listener, stage, node, *, verifier=None):
    fallback = stage["recovery_fallback"]
    # verify_staged_backend reads the snapshot together with the authenticated
    # checkpoint. A preliminary runtime read would be discarded, and can fail
    # during an otherwise harmless publication snapshot replacement.
    backend = handoff_backend(config["namespace"], fallback["selector"], node, listener["targetPort"],read_runtime=False)
    meta = backend["pod"]["metadata"]
    if meta["uid"] != fallback["pod_uid"] or meta.get("annotations", {}).get("fugue.pro/oci-revision") != fallback["revision"]:
        raise ValueError("DNS recovery fallback differs from the reviewed Pod UID or revision")
    verify_staged_backend(backend, config["namespace"], listener["targetPort"], verifier)
    return backend


def handoff_witness(config, listener, current):
    if not current:
        return None
    recoveries = config.get('recoveries', [config.get('recovery', {})])
    selected = [recovery for recovery in recoveries if recovery.get('listener') == listener['name']]
    old_port = listener["targetPort"] if selected else current_target_port(current)
    if current["spec"]["selector"] == listener["selector"]:
        if old_port != listener["targetPort"]:
            raise ValueError("DNS target port change requires a different verified backend")
        return None
    namespace = config["namespace"]
    owner = listener_node(listener)
    node = owner["metadata"]["name"]
    last_error = None
    stage = next((s for s in config.get("handoffs", []) if s["address"] == listener["address"]), None)
    failed = stage is not None and "failed_predecessor" in stage
    refused = failed and "refused_listener" in stage["failed_predecessor"]
    from scripts.recover_dns_transport import DNSObservationRejected
    attempts = RECOVERY_ATTEMPTS if selected else HANDOFF_ATTEMPTS
    for attempt in range(attempts):
        try:
            if selected:
                from scripts.recover_dns_transport import witness
                return witness(dict(config, recovery=selected[0]), listener, current, owner)
            if refused:
                from scripts.dns_refused_listener import handoff
                return handoff(config,stage,listener,current,owner)
            if stage:
                # Each runtime snapshot keeps its original absolute deadline.
                # Observe the two backends together so slow proxying of the
                # candidate cannot consume the predecessor's proof window.
                # Both calls are read-only and independently recheck Pod UID
                # and resourceVersion; later EndpointSlice/CAS checks remain.
                with ThreadPoolExecutor(max_workers=2) as pool:
                    old_args = {"read_runtime": False} if failed else {}
                    old_read = pool.submit(handoff_backend, namespace, current["spec"]["selector"], node, old_port, **old_args)
                    candidate_read = pool.submit(handoff_backend, namespace, listener["selector"], node, listener["targetPort"])
                    old, candidate = old_read.result(), candidate_read.result()
            else:
                old = handoff_backend(namespace, current["spec"]["selector"], node, old_port)
                candidate = handoff_backend(namespace, listener["selector"], node, listener["targetPort"])
            if old["pod"]["metadata"]["uid"] == candidate["pod"]["metadata"]["uid"]:
                raise ValueError("DNS handoff selectors do not isolate different instances")
            revisions = {}
            if stage:
                for key, backend in (("previous_revision", old), ("candidate_revision", candidate)):
                    revision = backend["pod"]["metadata"].get("annotations", {}).get("fugue.pro/oci-revision")
                    if revision != stage[key]:
                        raise ValueError("DNS handoff backend does not run the declared revision")
                    revisions[key] = revision
            slices = read_json("get", "endpointslices", "-n", namespace, "-l",
                               "kubernetes.io/service-name=" + current["metadata"]["name"], "-o", "json")["items"]
            validate_handoff_endpoints(current, old["pod"], slices, old_port)
            if stage:
                for role, backend, port in (("predecessor", old, old_port),
                                            ("candidate", candidate, listener["targetPort"])):
                    if failed and role == "predecessor":
                        continue  # Requires a signed *unready* snapshot, not the normal ready predecessor.
                    try:
                        verify_handoff_backend(role, backend, namespace, port, recovery="recovery_fallback" in stage)
                    except DNSObservationRejected as error:
                        # Include the side, not the private verifier input.
                        error.add_note("DNS staged listener " + listener["name"] + " " + role)
                        raise
            if not failed:
                try:
                    validate_handoff_snapshots(old["snapshot"], candidate["snapshot"], node)
                except ElapsedInTransit:
                    if not stage or "recovery_fallback" in stage:
                        raise
                    recheck_ordinary_serving_pair(namespace, node, old, candidate, old_port, listener["targetPort"])
            if stage and "recovery_fallback" in stage:
                try:
                    fallback = recovery_fallback_backend(config, listener, stage, node)
                except DNSObservationRejected as error:
                    error.backend_role = "fallback"
                    error.add_note("DNS staged listener " + listener["name"] + " fallback")
                    raise
                if fallback["pod"]["metadata"]["uid"] in (old["pod"]["metadata"]["uid"], candidate["pod"]["metadata"]["uid"]):
                    raise ValueError("DNS recovery fallback must be an independent backend")
                revisions["fallback_uid"] = fallback["pod"]["metadata"]["uid"]
                if failed:
                    from scripts.dns_transport_failed_predecessor import witness
                    revisions["failed_predecessor"] = witness(config, stage, listener, current, owner["metadata"],
                                                               old, candidate, fallback)
                else:
                    validate_handoff_snapshots(candidate["snapshot"], fallback["snapshot"], node)
            return {"node_uid": owner["metadata"]["uid"], "old_uid": old["pod"]["metadata"]["uid"],
                    "candidate_uid": candidate["pod"]["metadata"]["uid"], **revisions,
                    "assignment": candidate["snapshot"]["assignment"], "parent_digest": candidate["snapshot"]["parent_digest"],
                    "plan_digest": candidate["snapshot"]["plan_digest"]}
        except (ValueError, ObservationUnavailable) as error:
            last_error = error
            message = str(error)
            retryable = isinstance(error, ObservationUnavailable) or any(fragment in message for fragment in [
                "exactly one live Ready backend", "runtime snapshot is not ready", "requires unique positive proofs",
                "different artifact assignments", "different probe membership", "expired or future observations",
                "proof is expired or from the future",
                "proof belongs to another release", "backend changed while reading runtime facts",
                "DNS recovery observation",
                "DNS recovery candidate does not match current verified full authority",
            ])
            if isinstance(error, DNSObservationRejected):
                # A transiently incomplete set can get a fresh observation;
                # an identity, signature, or positive-proof defect cannot.
                retryable = stage is not None and error.category in ("expired", "record_readiness", "wire_materialization")
            if not retryable or attempt + 1 == attempts:
                raise
            time.sleep(HANDOFF_RETRY_SECONDS)
    raise last_error or ValueError("DNS handoff witness unavailable")


def current_target_port(current):
    ports = current["spec"].get("ports", [])
    targets = {p.get("targetPort") for p in ports}
    if len(ports) != 2 or {p.get("protocol") for p in ports} != {"UDP", "TCP"} or len(targets) != 1:
        raise ValueError("current DNS transport requires a shared UDP/TCP target port")
    old_port = targets.pop()
    if type(old_port) is not int or not 1 <= old_port <= 65535:
        raise ValueError("current DNS transport target port is invalid")
    return old_port


def listener_node(listener):
    nodes = read_json("get", "nodes", "-o", "json")["items"]
    owners = [n for n in nodes if listener["address"] in [a["address"] for a in n.get("status", {}).get("addresses", [])]]
    if len(owners) != 1 or not owners[0]["metadata"].get("uid") or owners[0]["metadata"].get("deletionTimestamp"):
        raise ValueError("DNS handoff listener must belong to one live node")
    return owners[0]


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


def stage_diagnostic(config, address):
    local = [x for x in config["listeners"] if x["address"] == address]
    public = next(x for x in local if x["port"] == 53)
    diagnostic = [x for x in local if x["port"] != 53 and x["selector"] == public["selector"] and
                  x["targetPort"] == public["targetPort"]]
    if len(diagnostic) != 1:
        raise ValueError("each DNS handoff stage requires a diagnostic listener to the same candidate")
    return diagnostic[0]


def predecessor_listener_config(config, listener, previous, generation):
    predecessor = dict(listener, selector=previous["spec"]["selector"], targetPort=current_target_port(previous))
    return dict(config, generation=generation,
                listeners=[predecessor if item["name"] == listener["name"] else item for item in config["listeners"]]), predecessor


def recovery_listener_config(config, listener, stage):
    fallback = dict(listener, selector=stage["recovery_fallback"]["selector"])
    return dict(config, generation=config["rollback_generation"],
                listeners=[fallback if item["name"] == listener["name"] else item for item in config["listeners"]]), fallback


def predecessor_stage_identity(stage, witness):
    return dict(stage, candidate_revision=witness["previous_revision"],
                require_observed_predecessor="recovery_fallback" in stage), {
        "candidate_uid": witness["old_uid"], "node_uid": witness["node_uid"]}


def observe_stage_result(verify_stage, config, stage, witness, port, evidence, report, save_evidence):
    entry = {"address": stage["address"], "port": port}
    evidence[report].append(entry)
    try:
        observed = verify_stage(config, stage, witness, port)
    except (ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        entry["passed"] = False
        if isinstance(error, PublicDNSVerificationFailed):
            entry["failed_samples"] = error.samples
        save_evidence()
        raise
    entry["result"] = observed
    save_evidence()
    return observed


def observe_dns_socket(raw, expectations, observer):
    with tempfile.TemporaryDirectory(prefix="fugue-dns-handoff-") as directory:
        intent, expected = Path(directory) / "public.json", Path(directory) / "expectations.json"
        intent.write_bytes(raw)
        expected.write_text(json.dumps(expectations))
        result = subprocess.run([observer, str(intent), str(expected)], capture_output=True, text=True, timeout=90)
    return result.returncode, json.loads(result.stdout)


def observe_recovery_fallback(config, stage, listener, public_service, witness, *, policy_path, observer, verifier, role="fallback"):
    # Start each retry with a new Pod/checkpoint pair. Nothing below mutates a
    # listener. A negative wire response, invalid signature, changed identity
    # or different release is not a transient transport read.
    deadline = time.monotonic()+45
    for attempt in range(3):
        try:
            return _observe_private_receiver(config,stage,listener,public_service,witness,policy_path=policy_path,
                                             observer=observer,verifier=verifier,role=role)
        except ObservationUnavailable:
            if attempt == 2 or time.monotonic()+2>=deadline:
                raise
            time.sleep(2)


def _observe_private_receiver(config, stage, listener, public_service, witness, *, policy_path, observer, verifier, role):
    from scripts.capture_dns_observation_policy import coherent_policy, unchanged_backend

    namespace = config["namespace"]
    owner = listener_node(listener)["metadata"]
    failure = stage.get("failed_predecessor")
    if failure:
        from scripts.dns_transport_failed_predecessor import require_listener_identity
        require_listener_identity(listener, public_service, owner, failure)
    if owner["uid"] != witness["node_uid"]:
        raise ValueError("DNS recovery fallback node was replaced")
    if role=="fallback":
        backend = recovery_fallback_backend(config, listener, stage, owner["name"], verifier=verifier)
    elif role=="candidate" and failure and "refused_listener" in failure:
        backend = handoff_backend(namespace,listener["selector"],owner["name"],listener["targetPort"],read_runtime=False)
        if backend["pod"]["metadata"].get("annotations",{}).get("fugue.pro/oci-revision")!=stage["candidate_revision"]:
            raise ValueError("private DNS candidate revision changed")
        verify_staged_backend(backend,namespace,listener["targetPort"],verifier)
    else:
        raise ValueError("unknown private DNS verification role")
    uid=witness[role+"_uid"]
    pod = backend["pod"]
    if pod["metadata"]["uid"] != uid:
        raise ValueError("DNS recovery fallback changed after preflight")
    addresses = [ipaddress.ip_address(entry["ip"]) for entry in pod.get("status", {}).get("podIPs", [])]
    v4 = [address for address in addresses if address.version == 4]
    if len(v4) != 1 or v4[0].is_global or pod["spec"].get("hostNetwork"):
        raise ValueError("DNS recovery fallback requires a unique private IPv4 Pod address")
    address = str(v4[0])
    declaration = read_observation_declaration(policy_path)
    if listener["address"] not in declaration["servers"]:
        raise ValueError("DNS recovery site has no declared observation")
    declaration["servers"] = [address]
    declaration["authority_port"] = listener["targetPort"]
    declaration.pop("recursive_servers", None)
    raw = json.dumps(declaration, sort_keys=True).encode()
    queries = [{key: query[key] for key in ("hostname", "type")} for query in declaration["queries"]
               if query.get("answer_source") == "verified_serving_policy"]
    images = [container["image"] for container in pod["spec"]["containers"]
              if all(any(port.get("containerPort") == listener["targetPort"] and
                         port.get("protocol", "TCP") == protocol for port in container.get("ports", []))
                     for protocol in ("UDP", "TCP"))]
    if len(images) != 1:
        raise ValueError("DNS recovery fallback requires one observation image")
    binding = dict(config, recovery={"candidate_image": images[0], "candidate_pod_uid": uid})
    policy = coherent_policy(backend, binding, verifier, queries, public_service)
    expected = {"schema": "fugue.dns-observation-expectations/v1",
                "declaration_digest": "sha256:" + hashlib.sha256(raw).hexdigest(),
                "servers": [{"server": address, "service_uid": public_service["metadata"]["uid"],
                             "pod_uid": uid, "policy": policy}]}
    status, result = observe_dns_socket(raw, expected, observer)
    if status or result.get("passed") is not True:
        raise ValueError("DNS recovery fallback failed direct UDP/TCP observation")
    verify_staged_backend(backend, namespace, listener["targetPort"], verifier)
    snapshot = backend["snapshot"]
    if policy["assignment"] != snapshot["assignment"] or policy["parent_digest"] != snapshot["parent_digest"]:
        raise ValueError("DNS recovery fallback changed assignment during UDP/TCP observation")
    if failure:
        from scripts.dns_transport_rollback_authority import require_current_rollback
        require_current_rollback(snapshot, failure)
    unchanged_backend(namespace, public_service, pod)
    return {"candidate_uid": uid, "assignment": snapshot["assignment"],
            "parent_digest": snapshot["parent_digest"], "plan_digest": snapshot["plan_digest"]}


def observe_stage(config, stage, witness, port, *, policy_path, observer, verifier):
    from scripts.capture_dns_observation_policy import capture, unchanged_backend

    address = stage["address"]
    degraded_predecessor = stage.get("require_observed_predecessor") is True
    failure = stage.get("failed_predecessor")
    listener = next(x for x in config["listeners"] if x["address"] == address and x["port"] == port)
    namespace = config["namespace"]
    owner = listener_node(listener)["metadata"]
    declaration = read_observation_declaration(policy_path)
    if address not in declaration["servers"]:
        raise ValueError("DNS handoff address has no declared public observation")
    declaration["servers"] = [address]
    declaration.pop("recursive_servers", None)
    declaration["authority_port"] = port
    raw = json.dumps(declaration, sort_keys=True).encode()
    for attempt in range(HANDOFF_ATTEMPTS):
        try:
            service_before = read_json("get", "service", listener["name"], "-n", namespace, "-o", "json")
            desired = service(config, listener)
            validate_existing(service_before, desired)
            if service_before["metadata"]["annotations"][DIGEST] != desired["metadata"]["annotations"][DIGEST]:
                raise ValueError("DNS handoff Service has not selected the declared transport")
            candidate = handoff_backend(namespace, listener["selector"], owner["name"], listener["targetPort"])
            pod = candidate["pod"]
            uid = pod["metadata"]["uid"]
            if failure:
                from scripts.dns_transport_failed_predecessor import require_listener_identity
                require_listener_identity(listener, service_before, owner, failure)
                fallback = stage["recovery_fallback"]
                expected_uid = fallback["pod_uid"] if listener["selector"] == fallback["selector"] else failure["candidate_pod_uid"]
                if uid != expected_uid:
                    raise ValueError("DNS recovery socket does not select its declared backend")
            if pod["metadata"].get("annotations", {}).get("fugue.pro/oci-revision") != stage["candidate_revision"] or (
                    witness and (uid != witness["candidate_uid"] or owner["uid"] != witness["node_uid"])):
                raise ValueError("DNS handoff candidate revised or replaced before public verification")
            expectations = capture(declaration, raw, config, verifier)
            if len(expectations["servers"]) != 1 or any(expectations["servers"][0][k] != v for k, v in (
                    ("server", address), ("pod_uid", uid), ("service_uid", service_before["metadata"]["uid"]))):
                raise ValueError("DNS observation backend does not match the handoff")
            status, accepted = observe_dns_socket(raw, expectations, observer)
            if status or accepted.get("passed") is not True:
                # Retry stale expectations, but do not leave a publicly failing
                # socket on the candidate for several more observation rounds.
                failures = [sample for sample in accepted.get("observations", [])
                            if not sample.get("passed") and sample.get("reason") not in
                            ("verified_expectation_unavailable", "verified_expectation_expired")]
                if port == 53 and failures:
                    raise PublicDNSVerificationFailed(failures)
                raise ValueError("post-handoff public UDP/TCP verification failed")
            if degraded_predecessor:
                declaration_after = json.loads(raw)
                expectations_after = capture(declaration_after, raw, config, verifier)
                before_policy, after_policy = expectations["servers"][0]["policy"], expectations_after["servers"][0]["policy"]
                if expectations_after["servers"][0]["pod_uid"] != uid or any(
                        before_policy.get(key) != after_policy.get(key)
                        for key in ("assignment", "parent_digest", "records")):
                    raise ValueError("DNS predecessor policy changed during external UDP/TCP observation")
                verify_staged_backend(candidate, namespace, listener["targetPort"], verifier, degraded_predecessor=True)
            else:
                verify_staged_backend(candidate, namespace, listener["targetPort"], verifier)
            signed = candidate["snapshot"]
            policy = expectations["servers"][0]["policy"]
            if signed["assignment"] != policy["assignment"] or signed["parent_digest"] != policy["parent_digest"]:
                raise ValueError("DNS handoff assignment changed during socket verification")
            if failure:
                from scripts.dns_transport_rollback_authority import require_current_rollback
                require_current_rollback(signed, failure)
            unchanged_backend(namespace, service_before, pod)
            accepted["verified_backend"] = {"candidate_uid": uid, "assignment": signed["assignment"],
                                            "parent_digest": signed["parent_digest"], "plan_digest": signed["plan_digest"]}
            return accepted
        except (ValueError, ObservationUnavailable, subprocess.TimeoutExpired):
            if attempt + 1 == HANDOFF_ATTEMPTS:
                raise
            time.sleep(HANDOFF_RETRY_SECONDS)


def read_observation_declaration(policy_path):
    with policy_path.open("rb") as file:
        raw = file.read((64 << 10) + 1)
    if len(raw) > 64 << 10:
        raise ValueError("public DNS observation declaration exceeds bound")
    return json.loads(raw)


def restore_staged_listener(config, operation, witness, write_acknowledged, *, raced_forward=False,
                            verify_fallback=None):
    _, _, listener, previous, _ = operation
    namespace = config["namespace"]
    args = ("get", "service", listener["name"], "-n", namespace, "-o", "json")
    live = read_json(*args)
    before_meta, live_meta = previous["metadata"], live["metadata"]
    if live_meta.get("uid") != before_meta["uid"] or live_meta.get("labels") != before_meta["labels"]:
        raise ValueError("DNS compensation refuses a replaced or relabeled Service")
    on_predecessor = live["spec"] == previous["spec"] and live_meta["annotations"] == before_meta["annotations"]
    if on_predecessor:
        if write_acknowledged:
            raise ValueError("acknowledged DNS handoff reverted outside this execution")
        if live_meta["resourceVersion"] != before_meta["resourceVersion"]:
            return None  # The forward CAS cannot still commit against this version.
        # The failed client call might still have a write in flight. Fence it
        # by advancing only our own generation on the exact predecessor.
    desired = service(config, listener)
    def normalized(spec):
        return dict(spec, publishNotReadyAddresses=bool(spec.get("publishNotReadyAddresses")))
    def is_attempted_state(state):
        meta = state["metadata"]
        return (meta.get("uid") == before_meta["uid"] and meta.get("labels") == before_meta["labels"] and
                normalized(state["spec"]) == normalized(previous["spec"] | desired["spec"]) and
                meta.get("annotations") == before_meta["annotations"] | desired["metadata"]["annotations"])
    if raced_forward and not is_attempted_state(live):
        raise ValueError("DNS compensation cannot confirm the raced forward write")
    if not on_predecessor and not is_attempted_state(live):
        raise ValueError("DNS compensation refuses Service state not written by this handoff")
    validate_existing(live, desired)
    stage = next((item for item in config.get("handoffs", [])
                  if item["address"] == listener["address"] and
                  (listener["port"] == 53 or "failed_predecessor" in item)), None)
    use_recovery = stage is not None and "recovery_fallback" in stage
    if use_recovery:
        if verify_fallback is None:
            raise ValueError("DNS recovery compensation requires independent fallback verification")
        validate_witness_policy(verify_fallback(config, stage, listener, live, witness), witness, "fallback_uid")
        rollback_config, restore_listener = recovery_listener_config(config, listener, stage)
    else:
        owner = listener_node(listener)["metadata"]
        port = current_target_port(previous)
        old = handoff_backend(namespace, previous["spec"]["selector"], owner["name"], port)
        identity = old["pod"]["metadata"]
        if (owner["uid"] != witness["node_uid"] or identity["uid"] != witness["old_uid"] or
                identity.get("annotations", {}).get("fugue.pro/oci-revision") != witness["previous_revision"]):
            raise ValueError("DNS compensation predecessor identity changed")
        verify_staged_backend(old, namespace, port)
        rollback_config, restore_listener = predecessor_listener_config(config, listener, previous, config["rollback_generation"])
    restore = service(rollback_config, restore_listener)
    if not use_recovery and restore["metadata"]["annotations"][DIGEST] != before_meta["annotations"][DIGEST]:
        raise ValueError("DNS compensation could not reproduce the original Service declaration")
    body = json.dumps(update_patch(live, restore))
    write_error = None
    try:
        kubectl("patch", "service", listener["name"], "-n", namespace, "--type=json", "--field-manager=" + MANAGER,
                "--patch-file=/dev/stdin", body=body)
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        write_error = error
    # Read back even after an ambiguous response to the compensation CAS.
    restored = read_json(*args)
    if on_predecessor and write_error is not None and is_attempted_state(restored):
        # The outstanding forward CAS won before we could fence it. Verify its
        # exact write and the predecessor again before one compensating CAS.
        return restore_staged_listener(config, operation, witness, False, raced_forward=True,
                                       verify_fallback=verify_fallback)
    validate_existing(restored, restore)
    if (restored["metadata"].get("uid") != before_meta["uid"] or
            restored["metadata"].get("labels") != before_meta["labels"] or
            restored["metadata"].get("annotations") != before_meta["annotations"] | restore["metadata"]["annotations"] or
            normalized(restored["spec"]) != normalized(previous["spec"] | restore["spec"])):
        raise ValueError("DNS compensation restore could not be read back") from write_error
    return rollback_config


def validate_witness_policy(policy, witness, uid_key):
    if witness is None or policy["candidate_uid"] != witness[uid_key] or any(
            policy[k] != witness[k] for k in ("assignment", "parent_digest", "plan_digest")):
        raise ValueError("DNS handoff assignment changed after " + uid_key + " verification")


def apply_transport_steps(config, planned, evidence, save_evidence, verify_stage, verify_fallback=None, verify_failure=None):
    from scripts.dns_transport_failed_predecessor import validate_failure_observation
    used_challenges = set()

    def apply_one(operation, *, diagnostic_policy=None, predecessor_policy=None, fallback_policy=None,
                  failure_observation=None, stage=None, attempted=None):
        command, body, listener, current, witness = operation
        refused = stage is not None and "refused_listener" in stage.get("failed_predecessor", {})
        if refused:
            from scripts.dns_refused_listener import advanced_node_only
            # The wire challenge and final signed witness must agree exactly.
            # A kubelet status write can invalidate a whole observation without
            # changing a single socket. Discard and recollect, with a new nonce;
            # never re-date the old receipt or relax its temporal checks.
            for attempt in range(2):
                failure_observation = verify_failure(config, stage, listener, current)
                challenge = failure_observation["challenge"]
                if challenge in used_challenges:
                    raise ValueError("refused DNS listener challenge reused across writes")
                used_challenges.add(challenge)
                fresh = revalidate_handoff_witness(config, listener, current, witness, fresh_refusal=True)
                if not attempt and advanced_node_only(failure_observation.get("topology"),
                        fresh.get("failed_predecessor",{}).get("empty_topology")):
                    continue
                validate_failure_observation(failure_observation, stage, current, fresh)
                witness = fresh
                evidence["predecessor_verifications"].append(failure_observation)
                save_evidence()
                break
        elif witness is not None:
            witness = revalidate_handoff_witness(config, listener, current, witness)
        for role, policy, uid_key in (("diagnostic", diagnostic_policy, "candidate_uid"),
                                      ("predecessor", predecessor_policy, "old_uid"),
                                      ("recovery fallback", fallback_policy, "fallback_uid")):
            if policy is not None:
                try:
                    validate_witness_policy(policy, witness, uid_key)
                except ValueError as error:
                    raise ValueError("DNS handoff assignment changed after " + role + " verification") from error
        if failure_observation is not None:
            validate_failure_observation(failure_observation, stage, current, witness)
        if attempted is not None:
            attempted.append([operation, witness, False])
        kubectl(*command, body=body)
        if attempted is not None:
            attempted[-1][2] = True
        evidence['applied'].append(listener['name'])
        save_evidence()
        print("reconciled Service/" + listener["name"], flush=True)
        return witness

    if "handoffs" not in config:
        for operation in planned:
            apply_one(operation)
        return
    for name in ("diagnostic_verifications", "predecessor_verifications", "public_verifications", "compensations"):
        evidence[name] = []
    for stage in config["handoffs"]:
        use_recovery = "recovery_fallback" in stage
        recover_failure = "failed_predecessor" in stage
        if use_recovery:
            evidence.setdefault("fallback_verifications", [])
        diagnostic = stage_diagnostic(config, stage["address"])
        diagnostic_policy = None
        attempted = []
        try:
            for port, report in [(diagnostic["port"], "diagnostic_verifications"), (53, "public_verifications")]:
                operation = next((op for op in planned if (op[2]["address"], op[2]["port"]) == (stage["address"], port)), None)
                predecessor_policy = None
                fallback_policy = None
                failure_observation = None
                if operation and (port == 53 or recover_failure):
                    _, _, listener, previous, witness = operation
                    if use_recovery:
                        fallback_policy = verify_fallback(config, stage, listener, previous, witness)
                        validate_witness_policy(fallback_policy, witness, "fallback_uid")
                        evidence["fallback_verifications"].append(fallback_policy)
                        save_evidence()
                    if port == 53:
                        if recover_failure:
                            if "refused_listener" not in stage["failed_predecessor"]:
                                failure_observation = verify_failure(config, stage, listener, previous)
                                validate_failure_observation(failure_observation, stage, previous, witness)
                                evidence["predecessor_verifications"].append(failure_observation)
                                save_evidence()
                        else:
                            # A signed expected answer does not prove the
                            # predecessor can serve it on the public socket.
                            old_config, _ = predecessor_listener_config(config, listener, previous, config["generation"])
                            old_stage, old_binding = predecessor_stage_identity(stage, witness)
                            verified = observe_stage_result(verify_stage, old_config, old_stage, old_binding, 53, evidence,
                                                            "predecessor_verifications", save_evidence)
                            predecessor_policy = verified["verified_backend"]
                witness = apply_one(operation, diagnostic_policy=diagnostic_policy,
                                    predecessor_policy=predecessor_policy, fallback_policy=fallback_policy,
                                    failure_observation=failure_observation, stage=stage, attempted=attempted) if operation else None
                # Re-observe even if the socket was switched by a previous run.
                observed = observe_stage_result(verify_stage, config, stage, witness, port, evidence, report, save_evidence)
                if recover_failure and witness is not None:
                    validate_witness_policy(observed["verified_backend"], witness, "candidate_uid")
                if port == diagnostic["port"]:
                    diagnostic_policy = observed["verified_backend"]
        except (Exception, KeyboardInterrupt):
            compensation_errors, restored_sockets = [], []
            public_attempted = any(operation[2]["port"] == 53 for operation, _, _ in attempted)
            for operation, witness, acknowledged in reversed(attempted):
                listener = operation[2]
                entry = {"listener": listener["name"], "result": "unverified"}
                evidence["compensations"].append(entry)
                if (not public_attempted or use_recovery) and listener["port"] == diagnostic["port"] and diagnostic_policy is not None:
                    # The diagnostic socket independently passed signed/wire
                    # verification and public traffic has not been written.
                    # Keep it staged rather than moving it to an unverified
                    # predecessor. A resumed run must verify it again.
                    entry["result"] = "retained_diagnostic_stage"
                    continue
                try:
                    if use_recovery and (listener["port"] == 53 or recover_failure):
                        rollback = restore_staged_listener(config, operation, witness, acknowledged,
                                                           verify_fallback=verify_fallback)
                    else:
                        rollback = restore_staged_listener(config, operation, witness, acknowledged)
                    entry["result"] = "restored" if rollback is not None else "not_applied"
                    if rollback is not None and (listener["port"] == 53 or recover_failure):
                        restored_sockets.append((rollback, witness, entry, listener["port"]))
                except (ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
                    compensation_errors.append(error)
            for rollback, witness, entry, port in restored_sockets:
                try:
                    if use_recovery:
                        restored_stage = dict(stage, candidate_revision=stage["recovery_fallback"]["revision"])
                        restored_binding = {"candidate_uid": witness["fallback_uid"], "node_uid": witness["node_uid"]}
                    else:
                        restored_stage, restored_binding = predecessor_stage_identity(stage, witness)
                    key = "public_verification" if port == 53 else "diagnostic_verification"
                    entry[key] = verify_stage(rollback, restored_stage, restored_binding, port)
                    if recover_failure:
                        validate_witness_policy(entry[key]["verified_backend"], witness, "fallback_uid")
                except (ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
                    entry["result"] = "restored_public_unverified" if port == 53 else "restored_diagnostic_unverified"
                    compensation_errors.append(error)
            if attempted:
                save_evidence()
            if compensation_errors:
                raise RuntimeError("DNS handoff failed and compensation could not be fully verified") from ExceptionGroup(
                    "DNS compensation errors", compensation_errors)
            raise


def prepare_transport_steps(config, preflight, operations):
    planned = []
    inventory = json.loads(kubectl("get", "services", "--all-namespaces", "-o", "json"))["items"]
    for listener in config["listeners"]:
        preflight.update(listener=listener["name"], gate="service_contract")
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
        if current and current["metadata"].get("annotations", {}).get(DIGEST) == desired["metadata"]["annotations"][DIGEST]:
            continue
        if "handoffs" in config and (not current or not any(
                s["address"] == listener["address"] and listener["port"] in (53, stage_diagnostic(config, s["address"])["port"])
                for s in config["handoffs"]) or
                current["spec"]["externalIPs"] != [listener["address"]] or
                {p.get("port") for p in current["spec"]["ports"]} != {listener["port"]}):
            raise ValueError("staged DNS handoff can only update declared sites on their existing sockets")
        preflight["gate"] = "node_local_backend"
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
        preflight["gate"] = "handoff_witness"
        witness = handoff_witness(config, listener, current)
        if "handoffs" in config and witness is None:
            raise ValueError("staged DNS handoff requires a verified change of backend")
        preflight["gate"] = "server_dry_run"
        if current:
            command = ["patch", "service", listener["name"], "-n", config["namespace"], "--type=json", "--field-manager=" + MANAGER, "--patch-file=/dev/stdin"]
            body = json.dumps(update_patch(current, desired))
        else:
            command = ["create", "--field-manager=" + MANAGER, "-f", "-"]
            body = json.dumps(desired)
        kubectl(*command, "--dry-run=server", body=body)
        planned.append((command, body, listener, current, witness))
        operations.append({"listener": listener["name"], "patch": json.loads(body), "witness": witness})
    return planned


def reconcile(config, apply=False, evidence_path=None, verify_stage=None, verify_fallback=None, verify_failure=None):
    from scripts.recover_dns_transport import DNSObservationRejected

    if apply and (config.get('recovery') or config.get('recoveries') or config.get('handoffs')) and not evidence_path:
        raise ValueError('DNS handoff or recovery requires durable prewrite evidence')
    if apply and config.get('handoffs') and not verify_stage:
        raise ValueError("DNS staged handoff requires a public verifier")
    if apply and config.get('handoffs') and "rollback_generation" not in config:
        raise ValueError("DNS staged handoff requires a reserved compensation generation")
    if apply and any("recovery_fallback" in stage for stage in config.get("handoffs", [])) and not verify_fallback:
        raise ValueError("DNS staged recovery requires independent fallback verification")
    if apply and any("failed_predecessor" in stage for stage in config.get("handoffs", [])) and not verify_failure:
        raise ValueError("DNS failed predecessor recovery requires real public failure observation")
    preflight = {"status": "started", "gate": "service_inventory"}
    evidence = {'schema': 'fugue.dns-transport-execution/v1', 'generation': config['generation'],
                'declaration_digest': 'sha256:' + hashlib.sha256(json.dumps(config, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
                'operations': [], 'applied': [], 'preflight': preflight}
    if "rollback_generation" in config:
        evidence["rollback_generation"] = config["rollback_generation"]
    def save_evidence():
        if evidence_path:
            evidence_path.parent.mkdir(parents=True, exist_ok=True)
            descriptor, path = tempfile.mkstemp(prefix=".dns-transport-", dir=evidence_path.parent)
            try:
                with os.fdopen(descriptor, "w") as receipt:
                    receipt.write(json.dumps(evidence, sort_keys=True) + '\n')
                os.replace(path, evidence_path)
            finally:
                Path(path).unlink(missing_ok=True)
    save_evidence()
    try:
        planned = prepare_transport_steps(config, preflight, evidence["operations"])
    except (Exception, KeyboardInterrupt) as error:
        preflight["status"] = "rejected"
        if isinstance(error, DNSObservationRejected):
            preflight["reason"] = error.category
            role = getattr(error, "backend_role", None)
            if role in ("predecessor", "candidate", "fallback"):
                preflight["backend_role"] = role
        elif isinstance(error, (ObservationUnavailable, subprocess.TimeoutExpired)):
            preflight["reason"] = "observation_unavailable"
        elif isinstance(error, KeyboardInterrupt):
            preflight["reason"] = "interrupted"
        # Do not copy arbitrary error details or command output to the receipt.
        save_evidence()
        raise
    preflight.clear()
    preflight["status"] = "verified"
    save_evidence()
    if apply:
        apply_transport_steps(config, planned, evidence, save_evidence, verify_stage, verify_fallback, verify_failure)
    return len(planned)


def revalidate_handoff_witness(config, listener, current, witness, *, fresh_refusal=False):
    # Planning several listeners may span a publication. Require two complete
    # matching observations of the latest assignment before this selector CAS;
    # Pod/node replacement or Service CAS failure is never retried here.
    identities = {k: witness[k] for k in ("node_uid", "old_uid", "candidate_uid", "fallback_uid") if k in witness}
    for attempt in range(HANDOFF_ATTEMPTS):
        fresh = handoff_witness(config, listener, current)
        if fresh is None or any(fresh.get(k) != v for k, v in identities.items()):
            raise ValueError("DNS handoff identity changed before selector CAS")
        if fresh == witness:
            return fresh
        if fresh_refusal:
            from scripts.dns_refused_listener import same_plan_after_node_update
            if same_plan_after_node_update(witness, fresh):
                # This value alone cannot authorize a write. The caller still
                # requires an exact new two-site challenge on this Node RV.
                return fresh
        if "failed_predecessor" in witness:
            raise ValueError("DNS failed predecessor recovery assignment changed before selector CAS")
        witness = fresh
        if attempt + 1 < HANDOFF_ATTEMPTS:
            time.sleep(HANDOFF_RETRY_SECONDS)
    raise ValueError("DNS handoff assignment did not stabilize before selector CAS")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--public-observation", type=Path)
    parser.add_argument("--observer", type=Path)
    parser.add_argument("--verifier", type=Path)
    args = parser.parse_args()
    config = load_config(args.path)
    if args.verifier:
        os.environ["FUGUE_DNS_OBSERVATION_VERIFIER"] = str(args.verifier.resolve())
    verify_stage = None
    verify_fallback = None
    verify_failure = None
    if args.apply and config.get("handoffs"):
        if not args.public_observation or not args.observer or not args.verifier:
            parser.error("staged handoff requires public observation declaration, observer and verifier")
        from functools import partial
        def interrupted(_signum, _frame):
            raise RuntimeError("DNS staged handoff interrupted")
        signal.signal(signal.SIGTERM, interrupted)
        verify_stage = partial(observe_stage, policy_path=args.public_observation, observer=str(args.observer.resolve()),
                               verifier=str(args.verifier.resolve()))
        verify_fallback = partial(observe_recovery_fallback, policy_path=args.public_observation,
                                  observer=str(args.observer.resolve()), verifier=str(args.verifier.resolve()))
        from scripts.dns_transport_failed_predecessor import observe_failed_predecessor
        verify_failure = partial(observe_failed_predecessor, policy_path=args.public_observation,
                                 observer=str(args.observer.resolve()), verifier=str(args.verifier.resolve()))
    print("listeners=" + str(reconcile(config, args.apply, args.evidence, verify_stage, verify_fallback, verify_failure)))


if __name__ == "__main__":
    # Keep the CLI and imported recovery/observation helpers on the same
    # typed read errors when launched with python -m.
    from scripts.reconcile_dns_transport import main
    main()
