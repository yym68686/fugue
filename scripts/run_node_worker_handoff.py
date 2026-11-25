"""Run one declared node Worker handoff via the installed Release Guardian."""
import argparse
import hashlib
import http.client
import ipaddress
import json
import os
from pathlib import Path
import re
import subprocess
import time

from scripts import front_connection_witness as connection
from scripts import node_worker_handoff as handoff
from scripts import observe_front_candidate as front
from scripts import reconcile_front_probe_transport as kube
from scripts import replace_front_executor as replacement
from scripts import stage_front_serving_transport as stage

GUARDIAN = "/usr/local/bin/fugue-release-guardian"
NATIVE = "/usr/local/bin/fugue-edge-front-cas"
STATE = "/var/lib/fugue-edge-front/activation.json"
NAME = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
SHA = re.compile(r"[0-9a-f]{40}")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
UID = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}")


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def fields(obj, names):
    require(isinstance(obj, dict) and set(obj) == set(names.split()), "unexpected fields in node Worker declaration")


def matches(pattern, value):
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def validate(config):
    fields(config, "schema namespace group node nodeUID front guardian candidate expected peers probe"
           + (" probeIngress" if isinstance(config, dict) and "probeIngress" in config else "")
           + (" verifyDrain" if isinstance(config, dict) and "verifyDrain" in config else "")
           + (" fixedRotation" if isinstance(config, dict) and "fixedRotation" in config else ""))
    require("verifyDrain" not in config or (config["verifyDrain"] is True and "probeIngress" in config),
            "drain evidence requires an explicit independent probe ingress")
    require(config["schema"] == "fugue.node-worker-handoff/v1"
            and all(matches(NAME, config[k]) for k in ("namespace", "group", "node"))
            and matches(UID, config["nodeUID"]), "exact named node Worker handoff required")
    public, guardian, candidate = (config[k] for k in ("front", "guardian", "candidate"))
    fields(public, "service uid generation address source imageDigest")
    fields(guardian, "deployment source imageDigest")
    fixed = isinstance(candidate, dict) and "daemonSet" in candidate
    fields(candidate, ("daemonSet" if fixed else "deployment") + " component source imageDigest slot bundleGeneration releaseDigest")
    require(not fixed or "probeIngress" in config, "fixed Worker handoff requires the independent probe transport")
    require("fixedRotation" not in config or (config["fixedRotation"] is True and fixed and config.get("verifyDrain") is True),
            "fixed rotation requires a fixed Worker and retained-connection drain evidence")
    addr = ipaddress.ip_address(public["address"])
    require(addr.version == 4 and addr.is_global and public["address"] == str(addr)
            and matches(NAME, public["service"]) and matches(UID, public["uid"])
            and type(public["generation"]) is int and public["generation"] > 0, "exact serving Front Service is required")
    for actor in (public, guardian, candidate):
        require(matches(SHA, actor["source"]) and matches(DIGEST, actor["imageDigest"]), "actor must pin immutable source and image")
    if "probeIngress" in config:
        ingress = config["probeIngress"]
        fields(ingress, "service uid generation port")
        require(matches(NAME, ingress["service"]) and matches(UID, ingress["uid"])
                and ingress["service"] != public["service"] and ingress["uid"] != public["uid"]
                and type(ingress["generation"]) is int and ingress["generation"] > 0
                and type(ingress["port"]) is int and ingress["port"] == 15443,
                "exact independently selected Front probe transport required")
    require(matches(NAME, guardian["deployment"]) and matches(NAME, candidate["daemonSet" if fixed else "deployment"])
            and matches(NAME, candidate["component"])
            and matches(DIGEST, candidate["releaseDigest"]) and candidate["slot"] in ("a", "b")
            and matches(re.compile(r"[a-zA-Z0-9_.:-]{1,253}"), candidate["bundleGeneration"]),
            "independently monitored candidate is required")
    before = config["expected"]
    base_keys = set("schema edge_group_id generation active_slot bundle_generation worker_source_commit worker_image_digest authority operation updated_at reason".split())
    require(isinstance(before, dict) and base_keys <= set(before)
            and set(before) <= base_keys | {"previous_slot", "rollback_of_generation"}
            and before.get("schema") == "edge-front-group-activation/v1" and before["edge_group_id"] == config["group"]
            and before["authority"] == "edge-control" and before["active_slot"] in ("a", "b")
            and before["active_slot"] != candidate["slot"] and type(before["generation"]) is int and before["generation"] > 0
            and matches(SHA, before["worker_source_commit"]) and matches(DIGEST, before["worker_image_digest"]),
            "declaration must pin the exact predecessor state, not a group-level version")
    front.timestamp(before["updated_at"])
    probe = config["probe"]
    fields(probe, "host path" + (" bodyDigest" if "bodyDigest" in probe else ""))
    require(matches(re.compile(r"[a-z0-9][a-z0-9.-]{0,251}[a-z0-9]"), probe["host"])
            and matches(re.compile(r"/[^\s?#]{0,510}"), probe["path"])
            and ("bodyDigest" not in probe or matches(DIGEST, probe["bodyDigest"]))
            and (not config.get("verifyDrain") or "bodyDigest" in probe), "invalid TLS canary")
    require(isinstance(config["peers"], list) and len(config["peers"]) <= 12, "explicit peer roster required")
    nodes, services = {config["node"]}, {public["service"]}
    for peer in config["peers"]:
        fields(peer, "node service uid")
        require(matches(NAME, peer["node"]) and matches(NAME, peer["service"]) and matches(UID, peer["uid"])
                and peer["node"] not in nodes and peer["service"] not in services, "peer roster is not exact and unique")
        nodes.add(peer["node"])
        services.add(peer["service"])
    return config


def sha(raw):
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def one(items, description):
    require(len(items) == 1, f"{description}: expected one object, found {len(items)}")
    return items[0]


def list_items(response):
    require(not response["metadata"].get("continue"), "Kubernetes inventory incomplete")
    return response["items"]


def peer_serving_identity(namespace, pod):
    # The peer is not a CAS participant. Read the live serving identity
    # without a kubelet exec (which uses a distinct streaming transport).
    name = pod["metadata"]["name"]
    health = front.read("get", "--raw", f"/api/v1/namespaces/{namespace}/pods/{name}:7831/proxy/readyz")
    require(health.get("status") == "ok" and health.get("route_authority") == "edge-control"
            and health.get("active_slot") in ("a", "b")
            and type(health.get("activation_generation")) is int and health["activation_generation"] > 0
            and matches(re.compile(r"[a-zA-Z0-9_.:-]{1,253}"), health.get("bundle_generation"))
            and matches(SHA, health.get("worker_source_commit")) and matches(DIGEST, health.get("worker_image_digest")),
            f"peer Front {name} did not report a valid serving Worker identity")
    return {k: health[k] for k in ("active_slot", "activation_generation", "bundle_generation", "worker_source_commit",
                                    "worker_image_digest", "route_authority")}


def process(pod, name):
    spec = one([c for c in pod["spec"]["containers"] if c["name"] == name], name + " spec")
    runtime = one([c for c in pod["status"]["containerStatuses"] if c["name"] == name], name + " runtime")
    meta = pod["metadata"]
    require(meta.get("uid") and not meta.get("deletionTimestamp") and pod["status"]["phase"] == "Running"
            and runtime.get("containerID") and runtime.get("imageID") and runtime.get("state", {}).get("running", {}).get("startedAt"),
            f"{meta['name']}: {name} process is not pinned and running")
    return {"image": spec["image"], "runtime_image": runtime["imageID"], "id": runtime["containerID"],
            "started_at": runtime["state"]["running"]["startedAt"], "restarts": runtime["restartCount"], "ready": runtime["ready"]}


def pin_image(pod, container, source, digest):
    proc = process(pod, container)
    require(proc["ready"] is True and proc["image"].endswith("@" + digest) and proc["runtime_image"].endswith("@" + digest)
            and pod["metadata"].get("annotations", {}).get("fugue.pro/source-commit") == source,
            f"{container} is not running its pinned ready release")
    return proc


def worker_process(pod):
    edge, caddy = (process(pod, c) for c in ("edge", "caddy"))
    return sha(("fugue-node-worker-process/v1\n" + edge["id"] + "\n" + caddy["id"]).encode())


def candidate_wire(declared):
    # ActivationRepairRelease uses Go's field names on the wire; the
    # activation record itself uses explicit snake_case JSON tags.
    return {"Slot": declared["slot"], "BundleGeneration": declared["bundleGeneration"],
            "WorkerSourceCommit": declared["source"], "WorkerImageDigest": declared["imageDigest"]}


def obj_uid(obj):
    return obj["metadata"]["uid"]


def controller(obj):
    return one([r for r in obj["metadata"].get("ownerReferences", []) if r.get("controller") is True], "controller owner")


class ActiveCluster:
    def __init__(self, config):
        self.config, self.ns = config, config["namespace"]
        self.profile = {k: config[k] for k in ("namespace", "node", "group")}
        self.profile["activationPath"] = STATE

    def get(self, kind, name):
        return front.read("get", kind, name, "-n", self.ns, "-o", "json")

    def pods(self, selector=None, node=None):
        args = ["-l", ",".join(k + "=" + v for k, v in sorted(selector.items()))] if selector else []
        if node:
            args.extend(["--field-selector", "spec.nodeName=" + node])
        return list_items(front.read("get", "pods", "-n", self.ns, *args, "-o", "json"))

    def read_service(self, declared):
        svc = self.get("service", declared["service"])
        meta, spec = svc["metadata"], svc["spec"]
        ann = meta.get("annotations", {})
        require(obj_uid(svc) == declared["uid"] and not meta.get("deletionTimestamp") and spec.get("selector")
                and meta.get("labels", {}).get("app.kubernetes.io/managed-by") == stage.MANAGER
                and ann.get("transport.fugue.dev/phase") == "serving"
                and spec["type"] == "ClusterIP" and spec.get("externalTrafficPolicy") == "Local"
                and len(spec.get("externalIPs", [])) == 1 and not spec.get("publishNotReadyAddresses", False),
                "foreign or drifted serving Front Service")
        if "generation" in declared:
            require(ann.get(stage.GENERATION) == str(declared["generation"]) and spec["externalIPs"] == [declared["address"]]
                    and ann.get(stage.DIGEST) == kube.digest(replacement.projection(spec))
                    and spec["selector"].get("fugue.io/edge-node") == declared["node"],
                    "target Front Service is not on the declared serving generation and node")
        return svc

    def service_front(self, svc, node, *, probe=False):
        name = svc["metadata"]["name"]
        selected = one(self.pods(svc["spec"]["selector"]), "complete Service selector")
        require(selected["spec"]["nodeName"] == node, "Service selected a different node")
        address = stage.selected_endpoint_witness(self.profile, {"name": name}, {"node": node}, selected, svc, probe=probe)
        return selected, address

    def require_probe_ingress(self, expected, public, selected):
        svc = self.get("service", expected["service"])
        meta, spec = svc["metadata"], svc["spec"]
        ann = meta.get("annotations", {})
        projected = replacement.projection(spec, probe=True)
        require(obj_uid(svc) == expected["uid"] and not meta.get("deletionTimestamp")
                and meta.get("labels", {}).get("app.kubernetes.io/managed-by") == kube.MANAGER
                and ann.get(kube.FOLLOWS_SERVING) == obj_uid(public)
                and ann.get(kube.GENERATION) == str(expected["generation"])
                and ann.get(kube.DIGEST) == kube.digest(projected)
                and spec.get("selector") == public["spec"]["selector"] and spec.get("externalIPs") == public["spec"]["externalIPs"]
                and projected.get("ports") == [{"name": "https-probe", "protocol": "TCP", "port": expected["port"], "targetPort": 443}],
                "probe transport no longer follows the selected public Front")
        same, _ = self.service_front(svc, self.config["node"], probe=True)
        require(obj_uid(same) == obj_uid(selected), "probe transport picked a different Front process")

    def deployment_actor(self, declared, container):
        deployment = self.get("deployment", declared["deployment"])
        selector = deployment["spec"]["selector"]
        status = deployment["status"]
        require(not deployment["metadata"].get("deletionTimestamp") and not selector.get("matchExpressions")
                and selector.get("matchLabels") and deployment["spec"]["replicas"] == 1
                and status.get("observedGeneration") == deployment["metadata"]["generation"]
                and status.get("replicas") == status.get("readyReplicas") == status.get("updatedReplicas") == 1,
                f"{container} has not reached one current deployed replica")
        pod = one(self.pods(selector["matchLabels"]), f"{container} deployment selector")
        rs_ref = controller(pod)
        require(rs_ref["kind"] == "ReplicaSet", f"{container} actor is not Deployment-owned")
        rs = self.get("replicaset", rs_ref["name"])
        owner = controller(rs)
        require(obj_uid(rs) == rs_ref["uid"] and owner["kind"] == "Deployment"
                and owner["name"] == declared["deployment"] and owner["uid"] == obj_uid(deployment),
                f"{container} process lost its declared Deployment")
        pin_image(pod, container, declared["source"], declared["imageDigest"])
        return pod

    def worker_at(self, pods, port):
        return one([p for p in pods if p["status"]["phase"] not in ("Succeeded", "Failed")
                    and any(s.get("hostPort") == port and s.get("protocol", "TCP") == "TCP"
                            for c in p["spec"]["containers"] for s in c.get("ports", []))], f"registered host port {port}")

    def native(self, pod, argument, allow_missing=False):
        args = ["kubectl", "-n", self.ns, "exec", pod["metadata"]["name"], "-c", "edge-front", "--", NATIVE, argument]
        run = subprocess.run(args, capture_output=True, timeout=9)
        if allow_missing and run.returncode and b"ordinary Worker journal is unavailable" in run.stderr:
            return None
        require(run.returncode == 0 and len(run.stdout) <= 16 << 10, "selected Front native observation failed")
        return json.loads(run.stdout)

    def exact_actors(self):
        cfg = self.config
        roster = list_items(front.read("get", "nodes", "-l", "fugue.io/edge-group-id=" + cfg["group"] + ",fugue.io/role.edge=true", "-o", "json"))
        require({n["metadata"]["name"] for n in roster} == {cfg["node"], *(p["node"] for p in cfg["peers"])},
                "declared peers do not cover every physical member of the group")
        pilot = one([n for n in roster if n["metadata"]["name"] == cfg["node"]], "pilot Node")
        require(obj_uid(pilot) == cfg["nodeUID"], "pilot Node UID changed")
        public = cfg["front"]
        svc = self.read_service(dict(public, node=cfg["node"]))
        selected, bound = self.service_front(svc, cfg["node"])
        if "probeIngress" in cfg:
            self.require_probe_ingress(cfg["probeIngress"], svc, selected)
        pin_image(selected, "edge-front", public["source"], public["imageDigest"])
        container = one([c for c in selected["spec"]["containers"] if c["name"] == "edge-front"], "Front container")
        front.require_node_worker_mounts(self.profile, selected, container)
        recipient = front.node_worker_recipient(self.profile, selected)
        expected = front.state(self.profile, selected)
        require(expected == cfg["expected"], "in-service Front does not match declared predecessor activation")
        if cfg.get("fixedRotation"):
            require(self.native(selected, "--worker-fixed-rotation") == expected,
                    "in-service Front cannot rotate its exact fixed Worker binding")
        local = self.pods(node=cfg["node"])
        prior = self.worker_at(local, 18443 if expected["active_slot"] == "a" else 28443)
        pin_image(prior, "edge", expected["worker_source_commit"], expected["worker_image_digest"])
        can = cfg["candidate"]
        if "daemonSet" in can:
            candidate = self.worker_at(local, 18443 if can["slot"] == "a" else 28443)
            pin_image(candidate, "edge", can["source"], can["imageDigest"])
            owner = controller(candidate)
            ds = self.get("daemonset", can["daemonSet"])
            require(owner["kind"] == "DaemonSet" and owner["name"] == can["daemonSet"] and owner["uid"] == obj_uid(ds)
                    and ds["spec"]["updateStrategy"]["type"] == "OnDelete",
                    "fixed Worker was not created by its declared OnDelete DaemonSet")
        else:
            candidate = self.deployment_actor(can, "edge")
            require(candidate["spec"]["nodeName"] == cfg["node"] and obj_uid(self.worker_at(local, 38443)) == obj_uid(candidate),
                    "independently monitored Worker is not the node's registered overlap process")
        guardian = self.deployment_actor(cfg["guardian"], "release-guardian")
        journal = {"schema": "edge-node-worker-advance/v1", "recipient": recipient, "expected": expected,
                   "node_uid": cfg["nodeUID"], "service_uid": bound["service_uid"],
                   "predecessor_worker_uid": obj_uid(prior), "predecessor_process": worker_process(prior),
                   "candidate_worker_uid": obj_uid(candidate), "candidate_process": worker_process(candidate),
                   "candidate_component": can["component"], "release_digest": can["releaseDigest"],
                   "candidate": candidate_wire(can)}
        if "probeIngress" in cfg:
            journal["probe_service_uid"] = cfg["probeIngress"]["uid"]
        if "daemonSet" in can:
            journal["fixed_transport"] = True
        if cfg.get("fixedRotation"):
            journal["fixed_rotation"] = True
        return selected, guardian, journal

    def peer_snapshot(self):
        snapshots = {}
        for peer in self.config["peers"]:
            node = peer["node"]
            svc = self.read_service(peer)
            selected, endpoint = self.service_front(svc, node)
            related = [p for p in self.pods(node=node) if p["metadata"].get("labels", {}).get("fugue.io/edge-group-id") == self.config["group"]
                       and ({"edge", "caddy"} <= {c["name"] for c in p["spec"]["containers"]}
                            or "edge-front" in {c["name"] for c in p["spec"]["containers"]})]
            require(obj_uid(selected) in {obj_uid(p) for p in related}, "peer Front was absent from the group runtime")
            workers = [p for p in related if {"edge", "caddy"} <= {c["name"] for c in p["spec"]["containers"]}]
            require({"a", "b"} <= {p["metadata"].get("labels", {}).get("fugue.io/edge-slot") for p in workers},
                    "peer runtime is missing a Worker slot")
            snapshot = {}
            for pod in related:
                r = controller(pod)
                require(r["kind"] in ("DaemonSet", "ReplicaSet"), "peer runtime lacks an exact declarative controller")
                owner = self.get(r["kind"].lower(), r["name"])
                require(obj_uid(owner) == r["uid"], "peer runtime controller UID changed")
                templates = {obj_uid(owner): kube.digest(owner["spec"]["template"])}
                if r["kind"] == "ReplicaSet":
                    ref = controller(owner)
                    require(ref["kind"] == "Deployment", "peer ReplicaSet lacks its parent Deployment")
                    deployment = self.get("deployment", ref["name"])
                    require(obj_uid(deployment) == ref["uid"], "peer Deployment UID changed")
                    templates[obj_uid(deployment)] = kube.digest(deployment["spec"]["template"])
                snapshot[obj_uid(pod)] = {"pod": pod["metadata"]["name"], "templates": templates,
                                          "processes": {c["name"]: process(pod, c["name"]) for c in pod["spec"]["containers"]}}
            snapshots[node] = {"service": svc["spec"], "endpoint": endpoint,
                               "serving_identity": peer_serving_identity(self.ns, selected), "runtimes": snapshot}
        return snapshots

    def exec_guardian(self, pod, command, raw):
        require(command in ("node-worker-seal", "node-worker-claim"), "unexpected Guardian command")
        name = pod["metadata"]["name"]
        current = self.get("pod", name)
        require(obj_uid(current) == obj_uid(pod) and process(current, "release-guardian") == process(pod, "release-guardian"),
                "configured Guardian incarnation changed")
        result = subprocess.run(["kubectl", "-n", self.ns, "exec", "-i", name, "-c", "release-guardian", "--",
                                 GUARDIAN, command], input=raw, capture_output=True, timeout=49)
        if result.returncode != 0 or not 0 < len(result.stdout) <= 32 << 10:
            detail = result.stderr.decode("utf-8", "replace").strip()
            raise ValueError(f"Guardian {command} did not succeed: {detail[:500]}")
        return result.stdout

    def get_config(self, name):
        return kube.kubectl("get", "configmap", name, "-n", self.ns, "--ignore-not-found", "-o", "json")


class ClaimedHandoff:
    def __init__(self, cluster, selected, journal):
        self.cluster, self.selected, self.journal = cluster, selected, journal

    def selected_activation(self):
        return front.read("-n", self.cluster.ns, "exec", self.selected["metadata"]["name"], "-c", "edge-front", "--", "cat", STATE)

    @staticmethod
    def connection_fact(profile, selected_front, sock):
        return connection.fact(profile, selected_front, sock)

    def drain_grant(self):
        recipient = self.journal["recipient"]
        name = front.node_worker_drain_name(recipient["group"], recipient["node"])
        obj = self.cluster.get_config(name)
        if obj is None:
            return None
        meta = obj["metadata"]
        labels = meta.get("labels", {})
        require(meta["name"] == name and meta["namespace"] == self.cluster.ns and meta.get("uid")
                and meta.get("resourceVersion") and not meta.get("deletionTimestamp")
                and labels.get("app.kubernetes.io/managed-by") == "fugue-release-guardian"
                and labels.get("fugue.pro/authority-store") == "true"
                and labels.get("fugue.pro/authority-kind") == "node-worker-drain"
                and labels.get("fugue.pro/group") == recipient["group"]
                and meta.get("annotations", {}).get("fugue.io/edge-node") == recipient["node"]
                and not obj.get("binaryData") and set(obj.get("data", {})) == {"grant.json"},
                "unexpected Guardian Worker drain projection metadata")
        return json.loads(obj["data"]["grant.json"])

    def drain_metrics(self):
        client = http.client.HTTPConnection(self.selected["status"]["podIP"], 7831, timeout=5)
        try:
            client.request("GET", "/metrics")
            response = client.getresponse()
            raw = response.read((1 << 20) + 1)
            require(response.status == 200 and len(raw) <= 1 << 20, "Front Worker drain metrics unavailable")
        finally:
            client.close()
        values = {}
        for key, value in re.findall(rb"^fugue_edge_front_worker_drain_([a-z_]+) ([0-9]+)$", raw, re.MULTILINE):
            name = key.decode("ascii")
            require(name not in values, "duplicate Front Worker drain metric")
            values[name] = int(value)
        return values

    def forced_connection(self, conn_id, generation, deadline):
        name = self.selected["metadata"]["name"]
        pod = self.cluster.get("pod", name)
        require(obj_uid(pod) == obj_uid(self.selected) and process(pod, "edge-front") == process(self.selected, "edge-front"),
                "Front incarnation changed during drain observation")
        since = handoff.wire_time(deadline.replace(microsecond=0))
        logs = subprocess.run(["kubectl", "-n", self.cluster.ns, "logs", name, "-c", "edge-front",
                               "--timestamps", "--since-time=" + since, "--limit-bytes=2097152"],
                              capture_output=True, text=True, timeout=10)
        require(logs.returncode == 0 and len(logs.stdout) <= 2 << 20, "exact Front drain receipt logs unavailable")
        suffix = f"worker drain forced: generation={generation} connection={conn_id}"
        events = [front.timestamp(line.split(maxsplit=1)[0]) for line in logs.stdout.splitlines() if line.endswith(" " + suffix)]
        require(len(events) <= 1, "duplicate exact Front drain receipt")
        return events[0] if events else None

    def name(self, phase):
        return "fugue-node-worker-" + phase + "-" + self.journal["digest"][7:39]

    def require_record(self, obj, name, kind, key):
        meta = obj["metadata"]
        require(meta.get("name") == name and meta.get("namespace") == self.cluster.ns and meta.get("uid")
                and meta.get("resourceVersion") and not meta.get("deletionTimestamp") and obj.get("immutable") is True
                and meta.get("labels", {}).get("fugue.pro/group") == self.journal["recipient"]["group"]
                and meta.get("labels", {}).get("fugue.pro/authority-kind") == kind
                and set(obj.get("data", {})) == {key} and not obj.get("binaryData"),
                "node Worker transaction ConfigMap is not an exact immutable record")
        return obj["data"][key]

    def put_input(self, phase, kind, key, payload):
        name, data = self.name(phase), front.canonical(payload)
        cm = {"apiVersion": "v1", "kind": "ConfigMap", "immutable": True, "data": {key: data},
              "metadata": {"name": name, "namespace": self.cluster.ns, "labels": {
                  "fugue.pro/group": self.journal["recipient"]["group"], "fugue.pro/authority-kind": kind}}}
        try:
            got = kube.kubectl("create", "-f", "-", "-o", "json", body=front.canonical(cm))
        except (ValueError, subprocess.TimeoutExpired):
            # Do not issue a second write after a possibly lost API reply.
            got = self.cluster.get_config(name)
            if got is None:
                raise
        require(self.require_record(got, name, kind, key) == data, "another input already owns the journal")

    def arm(self, payload):
        self.put_input("armed", "node-worker-arm", "arm.json", payload)

    def witness(self, payload):
        self.put_input("connected", "node-worker-witness", "witness.json", payload)

    def result(self):
        name = self.name("result")
        obj = self.cluster.get_config(name)
        if obj is None:
            return None
        return json.loads(self.require_record(obj, name, "node-worker-result", "result.json"))

    def require_claim(self, sealed):
        name = "fugue-authority-transition-prepared-" + self.journal["recipient"]["group"]
        obj = self.cluster.get_config(name)
        require(obj is not None and self.require_record(obj, name, "node-worker-journal", "journal.json").encode() == sealed,
                "Guardian did not persist the exact claimed journal")

    def wait_projection(self, sealed):
        end = time.monotonic() + 125
        expected = {"recipient": self.journal["recipient"], "content_digest": sha(sealed)}
        while time.monotonic() < end:
            require(handoff.slot_phase(self.journal, self.selected_activation()) == "pending", "activation moved before arm")
            projection = self.cluster.native(self.selected, "--worker-journal", allow_missing=True)
            if projection == expected:
                return
            if projection is not None:
                require(projection.get("recipient") == expected["recipient"], "Front incarnation changed during mount refresh")
            time.sleep(2)
        raise ValueError("the selected Front did not mount the exact Guardian journal")


def run(config, evidence_path):
    cluster = ActiveCluster(config)
    selected, guardian, unsigned = cluster.exact_actors()
    peers = cluster.peer_snapshot()
    require(cluster.peer_snapshot() == peers, "peer state changed across baseline observations")
    unsigned["created_at"] = handoff.wire_time(front.now())
    sealed = cluster.exec_guardian(guardian, "node-worker-seal", front.canonical(unsigned).encode())
    journal = json.loads(sealed)
    echoed = {k: v for k, v in journal.items() if k != "digest"}
    if front.timestamp(echoed["created_at"]) == front.timestamp(unsigned["created_at"]):
        echoed["created_at"] = unsigned["created_at"]
    require(matches(DIGEST, journal.get("digest")) and echoed == unsigned,
            "sealed Guardian journal differs from the observed declaration")
    adapter = ClaimedHandoff(cluster, selected, journal)
    evidence = {"schema": "fugue.node-worker-handoff-evidence/v1", "declaration_digest": kube.digest(config),
                "journal": journal, "peers_before": peers}

    def save():
        evidence_path.write_text(front.canonical(evidence) + "\n")

    def record(**details):
        evidence.update(details)
        save()
        for phase in ("drain_started", "drain_completed"):
            if phase in details:
                print(phase + ": " + front.canonical(details[phase]), flush=True)

    save()
    failure = None
    try:
        try:
            cluster.exec_guardian(guardian, "node-worker-claim", sealed)
        except (ValueError, subprocess.TimeoutExpired) as err:
            # If the claim reached Kubernetes, the Guardian owns it. A lost
            # claim reply is safe to observe, never grounds for a fresh claim.
            adapter.require_claim(sealed)
            raise ValueError("an exact claim exists but Guardian completion was unproven; leave it unarmed") from err
        adapter.require_claim(sealed)
        adapter.wait_projection(sealed)
        evidence["guardian_result"] = handoff.handoff(adapter, cluster.profile, selected, config["front"]["address"],
                                                       config["probe"], journal,
                                                       probe_port=config.get("probeIngress", {}).get("port"),
                                                       drain_record=record if config.get("verifyDrain") else None)
    except (ValueError, OSError, subprocess.TimeoutExpired) as err:
        evidence["error"] = str(err)
        failure = err
    finally:
        save()
    try:
        current = cluster.peer_snapshot()
        evidence["peers_after"] = current
        evidence["peers_unchanged"] = current == peers and cluster.peer_snapshot() == current
    except (ValueError, OSError, subprocess.TimeoutExpired) as err:
        evidence["peer_observation_error"] = str(err)
        if failure is None:
            failure = err
    save()
    if failure is not None:
        raise failure
    require(evidence["peers_unchanged"], "non-target release template or process changed during the pilot")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("validate", "run"))
    parser.add_argument("config", type=Path)
    parser.add_argument("--evidence", type=Path)
    args = parser.parse_args()
    cfg = validate(json.loads(args.config.read_text()))
    if args.operation == "run":
        require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("GITHUB_EVENT_NAME") == "push"
                and os.environ.get("GITHUB_REF") == "refs/heads/main" and matches(SHA, os.environ.get("GITHUB_SHA")),
                "node Worker handoff only runs in the ordinary main GitHub Actions deployment")
        require(args.evidence is not None and not args.evidence.exists(), "new evidence file required")
        run(cfg, args.evidence)


if __name__ == "__main__":
    main()
