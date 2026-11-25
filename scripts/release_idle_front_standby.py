"""Free a retired host-port Front's idle single-node slot without adding a Pod."""
import argparse
import copy
import json
import re
import time
from contextlib import ExitStack, closing
from pathlib import Path

from scripts import retire_front_hostport as retirement

front, kube = retirement.front, retirement.kube
require = retirement.require
ANNOTATION = "transport.fugue.dev/idle-standby-release"
CONFIG_ROOT = Path("deploy/environments/production/front-hostport-retirement")


def validate(config):
    require(isinstance(config, dict) and set(config) == {"schema", "retirement", "daemonSetUID", "specDigest", "podUID", "idleSeconds"}
            and config["schema"] == "fugue.idle-front-standby-release/v1"
            and type(config["idleSeconds"]) is int and 3 <= config["idleSeconds"] <= 15
            and re.fullmatch(r"[a-z0-9][a-z0-9-]*\.json", config["retirement"])
            and all(retirement.UID.fullmatch(config[k]) for k in ("daemonSetUID", "podUID"))
            and re.fullmatch(r"sha256:[a-f0-9]{64}", config["specDigest"]), "exact idle Front standby declaration required")
    origin = CONFIG_ROOT / config["retirement"]
    require(origin.is_file() and not origin.is_symlink(), "retirement declaration must be a regular file")
    return retirement.validate(json.loads(origin.read_text()))


def workload(config, origin, live):
    meta, current = live["metadata"], live["spec"]
    template = current["template"]["spec"]
    annotations = meta["annotations"]
    name, node = origin["isolated"], origin["node"]
    require(meta["name"] == name and meta["uid"] == config["daemonSetUID"] and not meta.get("deletionTimestamp")
            and annotations.get(retirement.GEN) == kube.digest(origin) and current["updateStrategy"] == {"type": "OnDelete"}
            and template["nodeSelector"].get("kubernetes.io/hostname") == node and not template.get("hostNetwork")
            and not retirement.has_host_ports(template), "standby is not the exact hostPort-free, single-node retired workload")
    disabled = annotations.get(ANNOTATION)
    baseline = copy.deepcopy(current)
    old_affinity, new_affinity = (retirement.node_affinity(node, op) for op in ("In", "NotIn"))
    require(template["affinity"] == (new_affinity if disabled else old_affinity)
            and (not disabled or disabled == kube.digest(config)), "standby affinity or release owner changed")
    baseline["template"]["spec"]["affinity"] = old_affinity
    require(kube.digest(baseline) == config["specDigest"], "standby workload template or selector changed")
    if disabled:
        return []
    return [{"op": "test", "path": "/metadata/uid", "value": meta["uid"]},
            {"op": "test", "path": "/metadata/resourceVersion", "value": meta["resourceVersion"]},
            {"op": "test", "path": "/spec", "value": current},
            {"op": "replace", "path": "/metadata/annotations", "value": dict(annotations, **{ANNOTATION: kube.digest(config)})},
            {"op": "replace", "path": "/spec/template/spec/affinity", "value": new_affinity}]


def run(config, origin, evidence):
    cluster = retirement.Cluster(origin)
    name, node = origin["isolated"], origin["node"]
    require(front.read("get", "node", node, "-o", "json")["metadata"]["uid"] == origin["nodeUID"], "standby Node was replaced")
    report = {"schema": config["schema"], "declaration": kube.digest(config), "result": "planned"}

    def save(**kw):
        report.update(kw)
        evidence.write_text(front.canonical(report) + "\n")

    def inspect():
        obj = cluster.get("daemonset", name)
        patch = workload(config, origin, obj)
        pods = cluster.list("pods", "-l", f"{retirement.ISOLATED}={node}")
        require(len(pods) <= 1 and (not patch or len(pods) == 1), "standby Pod roster changed")
        pod = pods[0] if pods else None
        if pod:
            require(pod["metadata"]["uid"] == config["podUID"] and retirement.owner(pod, name, config["daemonSetUID"])
                    and pod["spec"]["nodeName"] == node and not pod["spec"].get("hostNetwork")
                    and not retirement.has_host_ports(pod["spec"]), "standby Pod no longer matches its pinned incarnation")
        selected, transport = cluster.ingress(obj["spec"]["template"]["metadata"]["labels"], pod)
        if pod:
            require(not any((endpoint.get("targetRef") or {}).get("uid") == config["podUID"]
                            for item in cluster.list("endpointslices", "-A") for endpoint in item.get("endpoints", [])),
                    "an EndpointSlice still references the isolated standby Pod")
        return obj, patch, pod, selected, transport

    def write(patch, dry=False):
        return kube.kubectl("patch", "daemonset", name, "-n", origin["namespace"], "--type=json", "--patch-file", "-",
                            "--field-manager=fugue-idle-front-standby-release", "-o", "json",
                            *(["--dry-run=server"] if dry else []), body=front.canonical(patch))

    save()
    with ExitStack() as cleanup:
        component, group = origin["workerComponent"], origin["group"]
        locks = [
            ("fugue-production-" + component, {"app.kubernetes.io/managed-by": "fugue-declarative-release", "fugue.pro/component": component}, 900),
            ("fugue-authority-" + group, {"app.kubernetes.io/managed-by": "fugue-release-guardian", "fugue.pro/group": group}, 120),
        ]
        for i, args in enumerate(locks):
            locks[i] = cluster.lock(*args)
            cleanup.callback(cluster.unlock, locks[i])
        cluster.no_worker_transaction()
        _, _, _, old, peers = cluster.workloads()
        require(not old, "shared hostPort Front returned to the target node")
        _, patch, pod, selected, transport = inspect()
        activation = front.state(cluster.profile, selected)
        sock = cleanup.enter_context(closing(retirement.connection.HeldTLS(origin["address"], origin["hosts"][0])))
        proof = sock.proof("/")
        held = retirement.connection.fact(cluster.profile, selected, sock)
        if patch:
            require(not workload(config, origin, write(patch, dry=True)), "standby exclusion server dry run diverged")
            cluster.idle(pod)
            time.sleep(config["idleSeconds"])
            _, patch, pod, current, current_transport = inspect()
            require(patch and current_transport == transport and front.state(cluster.profile, current) == activation,
                    "serving state or standby changed before releasing capacity")
            cluster.no_worker_transaction()
            cluster.idle(pod)
            for lock in locks:
                cluster.held(lock, renew=True)
            save(result="idle_confirmed", old_uid=config["podUID"], serving_uid=held["pod_uid"])
            require(not workload(config, origin, write(patch)), "standby exclusion differs from the exact declaration")
            save(result="standby_excluded")
        end = time.monotonic() + 70
        while True:
            obj = cluster.get("daemonset", name)
            require(not workload(config, origin, obj), "standby exclusion disappeared")
            status = obj["status"]
            if (status.get("observedGeneration") == obj["metadata"]["generation"] and status["desiredNumberScheduled"] == 0
                    and not cluster.list("pods", "-l", f"{retirement.ISOLATED}={node}")):
                break
            require(time.monotonic() < end, "idle standby Pod did not leave the node")
            time.sleep(2)
        save(result="capacity_released")
        _, _, _, current, current_transport = inspect()
        require(current_transport == transport and front.state(cluster.profile, current) == activation
                and cluster.workloads()[-1] == peers and sock.proof("/") == proof
                and retirement.connection.fact(cluster.profile, current, sock) == held,
                "serving connection, ingress, Worker, or peer Front changed")
        save(result="verified", retained_connection=held, activation=activation)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate", "run"))
    parser.add_argument("config", type=Path)
    parser.add_argument("--evidence", type=Path)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    origin = validate(config)
    if args.command == "run":
        require(args.evidence is not None, "standby release evidence path required")
        run(config, origin, args.evidence)


if __name__ == "__main__":
    main()
