"""Free one idle fixed Worker slot for a verified declarative release."""
import argparse
import copy
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
    handoff.fields(c, "schema namespace group node nodeUID old peers active front probeIngress next host")
    handoff.fields(c["old"], "controller controllerUID generation templateDigest podUID")
    handoff.fields(c["active"], "podUID source imageDigest generation")
    handoff.fields(c["front"], "service uid generation address source imageDigest podUID")
    handoff.fields(c["probeIngress"], "service uid generation port")
    handoff.fields(c["next"], "component controller")
    require(c["schema"] == "fugue.idle-fixed-worker-replacement/v1" and
            c["next"]["component"] == "edge-worker-us-primary-a" and c["next"]["controller"] == "fugue-" + c["next"]["component"],
            "only the declared single-node A replacement is implemented")
    require(c["probeIngress"]["port"] == 15443 and len(c["peers"]) == 2 and c["node"] not in c["peers"],
            "independent probe and exact peer roster required")
    for uid in (c["nodeUID"], c["old"]["controllerUID"], c["old"]["podUID"], c["active"]["podUID"],
                c["front"]["uid"], c["front"]["podUID"], c["probeIngress"]["uid"], *c["peers"].values()):
        require(handoff.matches(handoff.UID, uid), "exact object UID required")
    return c


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
    require(ds["kind"] == "DaemonSet" and meta["name"] == c["next"]["controller"] and meta["namespace"] == c["namespace"] and
            spec["updateStrategy"] == {"type": "OnDelete"} and not pod.get("hostNetwork") and hostports(pod) == PORTS[0] and
            not pod.get("nodeName") and pod["nodeSelector"]["kubernetes.io/hostname"] == c["node"] and
            pod.get("affinity") == parking.node_affinity(c["node"], "In") and
            labels["fugue.io/edge-node"] == c["node"] and labels["fugue.io/edge-group-id"] == c["group"] and
            labels["fugue.io/edge-slot"] == "a" and spec["selector"]["matchLabels"]["fugue.io/edge-node"] == c["node"],
            "verified release would not stay in the freed single-node A slot")
    return labels


def release(c, execution, run, record):
    labels = ready_scope(c, json.loads((execution / "forward.json").read_text()))
    old = c["old"]
    k = parking.Cluster(dict(c, legacy=old["controller"]))
    live = handoff.ActiveCluster(c)

    def inventory():
        ds = k.get("daemonset", old["controller"])
        patch = parked_patch(c, ds)
        selector = ds["spec"]["selector"]
        require(set(selector) == {"matchLabels"} and selector["matchLabels"], "old Worker selector changed")
        pods = [p for p in live.pods(selector["matchLabels"]) if parking.owner(p, old["controller"], old["controllerUID"])]
        target = [p for p in pods if p["spec"]["nodeName"] == c["node"]]
        require(len(target) <= 1 and (not patch or len(target) == 1) and
                all(p["metadata"]["uid"] == old["podUID"] for p in target), "old A pod changed")
        peers = {p["spec"]["nodeName"]: (p["metadata"]["uid"], handoff.worker_process(p))
                 for p in pods if p["spec"]["nodeName"] != c["node"]}
        require({n: p[0] for n, p in peers.items()} == c["peers"], "shared A peer roster changed")
        return ds, patch, target, peers

    def check_ingress():
        k.no_worker_transaction()
        public = live.read_service(dict(c["front"], node=c["node"]))
        selected, _ = live.service_front(public, c["node"])
        live.require_probe_ingress(c["probeIngress"], public, selected)
        require(selected["metadata"]["uid"] == c["front"]["podUID"], "selected Front changed")
        handoff.pin_image(selected, "edge-front", c["front"]["source"], c["front"]["imageDigest"])
        state = front.state(live.profile, selected)
        active = c["active"]
        require(state["active_slot"] == "b" and state["generation"] == active["generation"] and
                state["worker_source_commit"] == active["source"] and state["worker_image_digest"] == active["imageDigest"],
                "slot A is not the declared inactive slot")
        local = live.pods(node=c["node"])
        b = live.worker_at(local, PORTS[1][1])
        require(b["metadata"]["uid"] == active["podUID"], "active B Pod changed")
        handoff.pin_image(b, "edge", active["source"], active["imageDigest"])
        return selected, state, handoff.worker_process(b)

    def no_direct_ingress(pod):
        identities = [labels, pod["metadata"]["labels"]] if pod else [labels]
        for s in k.list("services", "-A"):
            selector = s["spec"].get("selector")
            if s["metadata"]["namespace"] == c["namespace"] and selector:
                require(not any(all(identity.get(key) == val for key, val in selector.items()) for identity in identities),
                        "a Service directly selects an A Worker")
        if pod:
            for s in k.list("endpointslices", "-A"):
                require(not any((ep.get("targetRef") or {}).get("uid") == old["podUID"] or
                                pod["status"]["podIP"] in ep.get("addresses", []) for ep in s.get("endpoints", [])),
                        "an EndpointSlice directly selects the old A Worker")

    def idle(pod):
        raw = subprocess.run(["kubectl", "-n", c["namespace"], "exec", pod["metadata"]["name"], "-c", "caddy",
                              "--", "cat", "/proc/net/tcp", "/proc/net/tcp6"], check=True, capture_output=True, text=True, timeout=12).stdout
        require(len(raw) < 2 << 20, "old Worker socket inventory exceeds limit")
        require_empty_tcp(raw)

    def unchanged(previous):
        selected, state, process = check_ingress()
        require((state, process) == previous, "live B activation or process changed during A replacement")
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
            require(fact["slot"] == "b" and fact["target"] == f"{address}:{PORTS[1][1]}",
                    "public connection does not reach the pinned B Worker")
            front.stable_proof_pair(address, c["probeIngress"]["port"], c["host"], path, c["node"], group)
            _, patch, target, peers = inventory()
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
                require(not pending and peer_now == peers, "peer process or target exclusion changed")
                if not local and ds["status"].get("observedGeneration") == ds["metadata"]["generation"] and ds["status"]["desiredNumberScheduled"] == len(peers):
                    break
                require(time.monotonic() < end, "idle A did not stop on the declared node")
                time.sleep(3)
            record("old-a-parked")
            unchanged((state, process))
            def renew():
                for lock in locks:
                    k.held(lock, renew=True)
                same_authority(held, path)

            rc = run(renew)
            require(rc == 0, "declarative A release did not verify; B remains selected and old A parked")
            end_pod = unchanged((state, process))
            same_authority(held, path)
            require(connection.fact(live.profile, end_pod, held) == fact and inventory()[3] == peers,
                    "B connection or peer process changed during new A release")
            installed = live.worker_at(live.pods(node=c["node"]), PORTS[0][1])
            controller = k.get("daemonset", c["next"]["controller"])
            require(parking.owner(installed, c["next"]["controller"], handoff.obj_uid(controller)) and
                    controller["status"]["desiredNumberScheduled"] == 1, "new A is not the one declared replacement")
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
