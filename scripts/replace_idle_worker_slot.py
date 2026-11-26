"""Free one idle fixed Worker slot for a verified declarative release."""
import argparse
import copy
import hashlib
import json
import signal
import subprocess
import time
from contextlib import ExitStack, closing
from pathlib import Path

from scripts import front_connection_witness as connection
from scripts import observe_front_candidate as front
from scripts import reconcile_front_probe_transport as kube
from scripts import retire_front_hostport as parking
from scripts import run_node_worker_handoff as handoff

ANNOTATION = "transport.fugue.dev/idle-worker-replacement"
PORTS = ((18080, 18443), (28080, 28443))
require = handoff.require


def validate(c):
    retired = isinstance(c, dict) and "retirement" in c
    handoff.fields(c, "schema namespace group node nodeUID old peers active front probeIngress next host"
                   + (" retirement" if retired else "") + (" slot" if isinstance(c, dict) and "slot" in c else ""))
    handoff.fields(c["old"], "controller controllerUID generation templateDigest podUID")
    handoff.fields(c["active"], "podUID source imageDigest generation")
    handoff.fields(c["front"], "service uid generation address source imageDigest podUID")
    handoff.fields(c["probeIngress"], "service uid generation port")
    handoff.fields(c["next"], "component controller")
    require(c["schema"] == "fugue.idle-fixed-worker-replacement/v1" and
            all(handoff.matches(handoff.NAME, c[k]) for k in ("namespace", "group", "node")) and
            handoff.matches(handoff.NAME, c["old"]["controller"]) and
            handoff.matches(handoff.NAME, c["next"]["component"]) and c["next"]["component"].startswith("edge-worker-") and
            c["next"]["controller"] == "fugue-" + c["next"]["component"] and c["next"]["controller"] != c["old"]["controller"],
            "exact, distinct fixed Worker controllers required")
    if retired:
        handoff.fields(c["retirement"], "journalDigest receiptUID" +
                       (" controllerPeers" if "controllerPeers" in c["retirement"] else ""))
        require(handoff.matches(handoff.DIGEST, c["retirement"]["journalDigest"]) and
                handoff.matches(handoff.UID, c["retirement"]["receiptUID"]), "exact Guardian retirement receipt required")
        controller_peers = c["retirement"].get("controllerPeers", {})
        require(isinstance(controller_peers, dict) and set(controller_peers) <= set(c["peers"]) and
                all(handoff.matches(handoff.UID, uid) for uid in controller_peers.values()),
                "shared retired controller must pin every retained peer")
    require(c.get("slot", "a") in ("a", "b") and ("slot" not in c or retired),
            "an explicit slot requires a Guardian retirement receipt")
    require(c["probeIngress"]["port"] == 15443 and isinstance(c["peers"], dict)
            and len(c["peers"]) == 2 and c["node"] not in c["peers"],
            "independent probe and exact peer roster required")
    for uid in (c["nodeUID"], c["old"]["controllerUID"], c["old"]["podUID"], c["active"]["podUID"],
                c["front"]["uid"], c["front"]["podUID"], c["probeIngress"]["uid"], *c["peers"].values()):
        require(handoff.matches(handoff.UID, uid), "exact object UID required")
    return c


def slot_ports(slot):
    return PORTS[0 if slot == "a" else 1]


def require_retired(c, ds, receipt):
    old, retirement, node = c["old"], c["retirement"], c["node"]
    meta, spec, status = ds["metadata"], ds["spec"], ds["status"]
    digest = retirement["journalDigest"]
    name = "fugue-node-worker-retired-" + digest[7:39]
    rm = receipt["metadata"]
    require(rm.get("name") == name and rm.get("namespace") == c["namespace"] and rm.get("uid") == retirement["receiptUID"] and
            rm.get("resourceVersion") and not rm.get("deletionTimestamp") and
            rm.get("labels", {}).get("app.kubernetes.io/managed-by") == "fugue-release-guardian" and
            rm.get("labels", {}).get("fugue.pro/group") == c["group"] and
            rm.get("labels", {}).get("fugue.pro/authority-kind") == "node-worker-retired" and
            receipt.get("immutable") is True and not receipt.get("binaryData") and set(receipt.get("data", {})) == {"retired.json"},
            "inactive slot lacks its exact immutable Guardian receipt")
    raw = receipt["data"]["retired.json"]
    decision = json.loads(raw)
    node_key = hashlib.sha256(node.encode()).hexdigest()[:20]
    require(decision.get("node") == node and decision.get("worker_uid") == old["podUID"] and
            decision.get("plan") == {"schema": "edge-node-worker-retirement/v1", "journal_digest": digest,
                                     "daemonset": old["controller"], "daemonset_uid": old["controllerUID"],
                                     "generation": old["generation"] - 1} and
            meta.get("annotations", {}).get("fugue.pro/worker-retired-" + node_key) == raw and
            decision.get("outcome") in ("empty", "deadline"),
            "retirement receipt does not bind the old controller, worker and node")
    template = spec["template"]
    original = handoff.remove_target_exclusion(template, node)
    shared = "controllerPeers" in retirement
    survivors = len(retirement.get("controllerPeers", {}))
    slot = c.get("slot", "a")
    original_spec = original["spec"]
    scope = (not original_spec.get("nodeSelector", {}).get("kubernetes.io/hostname") if shared else
             original_spec.get("nodeSelector", {}).get("kubernetes.io/hostname") == node and
             original_spec.get("affinity") == parking.node_affinity(node, "In"))
    require(meta.get("uid") == old["controllerUID"] and meta.get("name") == old["controller"] and
            meta.get("namespace") == c["namespace"] and not meta.get("deletionTimestamp") and
            meta.get("generation") == status.get("observedGeneration") == old["generation"] and
            all(status.get(k, 0) == survivors for k in ("desiredNumberScheduled", "currentNumberScheduled", "numberReady")) and
            status.get("numberMisscheduled", 0) == 0 and scope and
            spec["updateStrategy"] == {"type": "OnDelete"} and kube.digest(template) == old["templateDigest"] and
            not template["spec"].get("hostNetwork") and hostports(template["spec"]) == slot_ports(slot) and
            template["metadata"]["labels"].get("fugue.io/edge-slot") == slot and
            template["metadata"]["labels"].get("fugue.io/edge-group-id") == c["group"],
            "retired controller no longer matches its exact single-node exclusion")


def parked_patch(c, ds):
    meta, spec, old = ds["metadata"], ds["spec"], c["old"]
    require(meta["uid"] == old["controllerUID"] and meta["name"] == old["controller"] and
            not meta.get("deletionTimestamp") and spec["updateStrategy"] == {"type": "OnDelete"},
            "old A must be the exact OnDelete controller")
    template = copy.deepcopy(spec["template"])
    pod_spec = template["spec"]
    require(not pod_spec.get("hostNetwork") and hostports(pod_spec) == PORTS[0],
            "old A no longer owns the exact slot A ports")
    annotations = meta.get("annotations", {})
    recorded = annotations.get(ANNOTATION)
    affinity = parking.node_affinity(c["node"], "NotIn")
    if recorded is not None:
        require(recorded == kube.digest(c) and meta["generation"] == old["generation"] + 1 and pod_spec.pop("affinity", None) == affinity,
                "existing node exclusion was not made by this declaration")
    else:
        require(meta["generation"] == old["generation"] and not pod_spec.get("affinity"),
                "shared Worker generation or affinity changed")
    require(kube.digest(template) == old["templateDigest"], "shared Worker template changed")
    if recorded is not None:
        return []
    return [
        {"op": "test", "path": "/metadata/uid", "value": meta["uid"]},
        {"op": "test", "path": "/metadata/resourceVersion", "value": meta["resourceVersion"]},
        {"op": "test", "path": "/spec", "value": spec},
        {"op": "add", "path": "/metadata/annotations", "value": dict(annotations, **{ANNOTATION: kube.digest(c)})},
        {"op": "add", "path": "/spec/template/spec/affinity", "value": affinity},
    ]


def hostports(spec):
    return tuple(sorted(p["hostPort"] for x in spec["containers"] + spec.get("initContainers", [])
                        for p in x.get("ports", []) if p.get("hostPort")))


def endpoint_targets_worker(endpoint_slices, uid, address):
    return any((ep.get("targetRef") or {}).get("uid") == uid or address in (ep.get("addresses") or [])
               for s in endpoint_slices for ep in (s.get("endpoints") or []))


def require_empty_tcp(raw):
    require(raw.count("local_address") == 2, "IPv4 and IPv6 socket tables are required")
    for line in raw.splitlines():
        fields = line.split()
        if not fields or fields[0] == "sl":
            continue
        require(len(fields) >= 4 and ":" in fields[1], "invalid slot socket inventory")
        if int(fields[1].rsplit(":", 1)[1], 16) in PORTS[0]:
            require(fields[3] in ("0A", "06"), "idle A still has an established or in-flight connection")


def ready_scope(c, forward):
    ds = handoff.one(forward["items"], "single-node release manifest")
    meta, spec = ds["metadata"], ds["spec"]
    template = spec["template"]
    labels = template["metadata"]["labels"]
    pod = template["spec"]
    slot = c.get("slot", "a")
    require(ds["kind"] == "DaemonSet" and meta["name"] == c["next"]["controller"] and meta["namespace"] == c["namespace"] and
            spec["updateStrategy"] == {"type": "OnDelete"} and not pod.get("hostNetwork") and hostports(pod) == slot_ports(slot) and
            not pod.get("nodeName") and pod["nodeSelector"]["kubernetes.io/hostname"] == c["node"] and
            pod.get("affinity") == parking.node_affinity(c["node"], "In") and
            labels["fugue.io/edge-node"] == c["node"] and labels["fugue.io/edge-group-id"] == c["group"] and
            labels["fugue.io/edge-slot"] == slot and spec["selector"]["matchLabels"]["fugue.io/edge-node"] == c["node"],
            f"verified release would not stay in the freed single-node {slot.upper()} slot")
    return labels


def release(c, execution, run, record):
    labels = ready_scope(c, json.loads((execution / "forward.json").read_text()))
    old = c["old"]
    slot = c.get("slot", "a")
    active_slot = "b" if slot == "a" else "a"
    retired = c.get("retirement")
    k = parking.Cluster(dict(c, legacy=old["controller"]))
    live = handoff.ActiveCluster(c)

    def inventory():
        ds = k.get("daemonset", old["controller"])
        if retired:
            name = "fugue-node-worker-retired-" + retired["journalDigest"][7:39]
            require_retired(c, ds, k.get("configmap", name))
            patch = []
        else:
            patch = parked_patch(c, ds)
        selector = ds["spec"]["selector"]
        require(set(selector) == {"matchLabels"} and selector["matchLabels"], "old Worker selector changed")
        pods = [p for p in live.pods(selector["matchLabels"]) if parking.owner(p, old["controller"], old["controllerUID"])]
        target = [p for p in pods if p["spec"]["nodeName"] == c["node"]]
        require(len(target) <= 1 and (not patch or len(target) == 1) and
                all(p["metadata"]["uid"] == old["podUID"] for p in target), "old A pod changed")
        peers = {p["spec"]["nodeName"]: (p["metadata"]["uid"], handoff.worker_process(p))
                 for p in pods if p["spec"]["nodeName"] != c["node"]}
        expected = retired.get("controllerPeers", {}) if retired else c["peers"]
        require({n: p[0] for n, p in peers.items()} == expected, "old Worker peer roster changed")
        return ds, patch, target, peers

    def retired_peers():
        # A retired isolated controller has no peers of its own. Pin the
        # existing peer processes independently from its empty selector.
        peers = {}
        for node, uid in c["peers"].items():
            pod = handoff.one([p for p in live.pods(node=node) if handoff.obj_uid(p) == uid], "peer Worker")
            peers[node] = (uid, handoff.worker_process(pod))
        return peers

    def check_ingress():
        k.no_worker_transaction()
        public = live.read_service(dict(c["front"], node=c["node"]))
        selected, _ = live.service_front(public, c["node"])
        live.require_probe_ingress(c["probeIngress"], public, selected)
        require(selected["metadata"]["uid"] == c["front"]["podUID"], "selected Front changed")
        handoff.pin_image(selected, "edge-front", c["front"]["source"], c["front"]["imageDigest"])
        state = front.state(live.profile, selected)
        active = c["active"]
        require(state["active_slot"] == active_slot and state["generation"] == active["generation"] and
                state["worker_source_commit"] == active["source"] and state["worker_image_digest"] == active["imageDigest"],
                "the replacement slot is not the declared inactive slot")
        local = live.pods(node=c["node"])
        worker = live.worker_at(local, slot_ports(active_slot)[1])
        require(worker["metadata"]["uid"] == active["podUID"], "serving Worker Pod changed")
        handoff.pin_image(worker, "edge", active["source"], active["imageDigest"])
        return selected, state, handoff.worker_process(worker)

    def no_direct_ingress(pod):
        identities = [labels, pod["metadata"]["labels"]] if pod else [labels]
        for s in k.list("services", "-A"):
            selector = s["spec"].get("selector")
            if s["metadata"]["namespace"] == c["namespace"] and selector:
                require(not any(all(identity.get(key) == val for key, val in selector.items()) for identity in identities),
                        "a Service directly selects an A Worker")
        if pod:
            require(not endpoint_targets_worker(k.list("endpointslices", "-A"), old["podUID"], pod["status"]["podIP"]),
                    "an EndpointSlice directly selects the old A Worker")

    def idle(pod):
        raw = subprocess.run(["kubectl", "-n", c["namespace"], "exec", pod["metadata"]["name"], "-c", "caddy",
                              "--", "cat", "/proc/net/tcp", "/proc/net/tcp6"], check=True, capture_output=True, text=True, timeout=12).stdout
        require(len(raw) < 2 << 20, "old Worker socket inventory exceeds limit")
        require_empty_tcp(raw)

    def unchanged(previous):
        selected, state, process = check_ingress()
        require((state, process) == previous, "serving Worker activation or process changed during replacement")
        return selected

    def same_authority(held, path):
        proof = held.proof(path)
        require(proof["edge"] == c["node"] and proof["group"] == c["group"], "held connection lost the pinned node authority")

    node = front.read("get", "node", c["node"], "-o", "json")
    require(node["metadata"]["uid"] == c["nodeUID"] and node["metadata"]["labels"]["kubernetes.io/hostname"] == c["node"],
            "replacement node changed")
    group = c["group"]
    mode = k.get("configmap", "fugue-authority-node-mode-" + group)
    require(mode.get("immutable") is True, "the legacy group writer is not fenced")
    component = "edge-worker-us"
    with ExitStack() as cleanup:
        locks = []
        for name, manager, key, val, seconds in [
            ("fugue-production-" + component, "fugue-declarative-release", "component", component, 900),
            ("fugue-authority-" + group, "fugue-release-guardian", "group", group, 120),
        ]:
            lock = k.lock(name, {"app.kubernetes.io/managed-by": manager, "fugue.pro/" + key: val}, seconds)
            locks.append(lock)
            cleanup.callback(k.unlock, lock)
        selected, state, process = check_ingress()
        path, address = "/healthz", c["front"]["address"]
        with closing(connection.HeldTLS(address, c["host"])) as held:
            same_authority(held, path)
            fact = connection.fact(live.profile, selected, held)
            require(fact["slot"] == active_slot and fact["target"] == f"{address}:{slot_ports(active_slot)[1]}",
                    "public connection does not reach the pinned serving Worker")
            front.stable_proof_pair(address, c["probeIngress"]["port"], c["host"], path, c["node"], group)
            _, patch, target, peers = inventory()
            if retired:
                require(not target, "retired controller still owns a Pod")
                controller_peers = peers
                peers = retired_peers()
            no_direct_ingress(target[0] if target else None)
            if patch:
                require(k.get("daemonset", c["next"]["controller"], optional=True) is None, "new A exists before the old A is parked")
                require(not parked_patch(c, k.write_exclusion(patch, dry=True)), "dry-run exclusion changed outside the declared node")
                idle(target[0])
                time.sleep(3)
                fresh = unchanged((state, process))
                _, patch2, current, peer_now = inventory()
                require(patch2 and peer_now == peers and handoff.obj_uid(current[0]) == handoff.obj_uid(target[0]), "A inventory changed")
                no_direct_ingress(current[0])
                idle(current[0])
                require(connection.fact(live.profile, fresh, held) == fact, "held B connection changed before parking idle A")
                for lock in locks:
                    k.held(lock, renew=True)
                record("prepared", peers=peers, old_uid=old["podUID"], held=fact)
                require(not parked_patch(c, k.write_exclusion(patch2)), "parked A controller did not match")
            end = time.monotonic() + 80
            while True:
                ds, pending, local, peer_now = inventory()
                require(not pending and (retired_peers() if retired else peer_now) == peers
                        and (not retired or peer_now == controller_peers),
                        "peer process or target exclusion changed")
                if not local and ds["status"].get("observedGeneration") == ds["metadata"]["generation"] and ds["status"]["desiredNumberScheduled"] == len(peer_now):
                    break
                require(time.monotonic() < end, "idle A did not stop on the declared node")
                time.sleep(3)
            record(f"old-{slot}-parked")
            current_front = unchanged((state, process))
            if retired:
                port_holders = [p for p in live.pods(node=c["node"])
                                if p["status"]["phase"] not in ("Succeeded", "Failed") and
                                set(hostports(p["spec"])).intersection(slot_ports(slot))]
                require(not port_holders and not endpoint_targets_worker(k.list("endpointslices", "-A"), old["podUID"], None),
                        "retired Worker still owns an endpoint or the fixed slot ports")
                require(connection.fact(live.profile, current_front, held) == fact, "serving connection changed before release")
                require(not inventory()[2], "retired controller regained a Pod")
            for lock in locks:
                k.held(lock, renew=True)
            def renew():
                for lock in locks:
                    k.held(lock, renew=True)
                same_authority(held, path)

            rc = run(renew)
            require(rc == 0, "declarative release did not verify; serving Worker remains selected and old slot parked")
            end_pod = unchanged((state, process))
            same_authority(held, path)
            observed_peers = inventory()[3]
            require(connection.fact(live.profile, end_pod, held) == fact and
                    (retired_peers() if retired else observed_peers) == peers
                    and (not retired or observed_peers == controller_peers),
                    "serving connection or peer process changed during Worker release")
            installed = live.worker_at(live.pods(node=c["node"]), slot_ports(slot)[1])
            controller = k.get("daemonset", c["next"]["controller"])
            require(parking.owner(installed, c["next"]["controller"], handoff.obj_uid(controller)) and
                    controller["status"]["desiredNumberScheduled"] == 1, "new Worker is not the one declared replacement")
            record("verified", new_uid=handoff.obj_uid(installed))


def main():
    parser = argparse.ArgumentParser()
    for arg in ("config", "execution", "tool", "result", "evidence"):
        parser.add_argument(arg, type=Path)
    args = parser.parse_args()
    c = validate(json.loads(args.config.read_text()))
    evidence = {"schema": c["schema"], "declaration": kube.digest(c)}

    def record(phase, **data):
        evidence.update(result=phase, **data)
        args.evidence.write_text(front.canonical(evidence) + "\n")

    def run(renew):
        with args.result.open("wb") as output, subprocess.Popen([str(args.tool), "execute", str(args.execution)], stdout=output) as p:
            try:
                while True:
                    try:
                        return p.wait(timeout=25)
                    except subprocess.TimeoutExpired:
                        renew()
            finally:
                if p.poll() is None:
                    p.terminate()
                    try:
                        p.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        p.kill()

    def interrupted(*_):
        raise RuntimeError("replacement interrupted; retain serving B and exact parked state")

    signal.signal(signal.SIGTERM, interrupted)
    record("started")
    release(c, args.execution, run, record)


if __name__ == "__main__":
    main()
