"""Retire the legacy Front host ports on exactly one node, retaining its Pod budget."""
import argparse
import copy
import datetime
import http.client
import ipaddress
import json
import os
from pathlib import Path
import re
import subprocess
import time
from contextlib import ExitStack, closing

from scripts import front_connection_witness as connection
from scripts import observe_front_candidate as front
from scripts import reconcile_front_probe_transport as kube
from scripts import replace_front_executor as replacement
from scripts import stage_front_serving_transport as stage

GEN = "transport.fugue.dev/front-hostport-retirement"
ISOLATED = "transport.fugue.dev/hostport-isolated"
MANAGER = "fugue-front-hostport-retirement"
NAME = re.compile(r"[a-z0-9][a-z0-9.-]{0,62}")
UID = re.compile(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}")
COMPONENT = "app.kubernetes.io/component"


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def validate(c):
    require(isinstance(c, dict) and set(c) == set("schema namespace group node nodeUID address legacy legacyUID legacyGeneration legacyTemplateDigest oldUID isolated frontUID workerComponent serving probe peers hosts".split())
            and c["schema"] == "fugue.front-hostport-retirement/v1", "exact node Front retirement declaration required")
    for key in ("namespace", "group", "node", "legacy", "isolated", "workerComponent"):
        require(isinstance(c[key], str) and NAME.fullmatch(c[key]), "invalid Front retirement name")
    for key in ("nodeUID", "legacyUID", "oldUID", "frontUID"):
        require(isinstance(c[key], str) and UID.fullmatch(c[key]), "exact Kubernetes UID required")
    require(c["legacy"] != c["isolated"] and c["oldUID"] != c["frontUID"] and type(c["legacyGeneration"]) is int
            and c["legacyGeneration"] > 0 and re.fullmatch(r"sha256:[a-f0-9]{64}", c["legacyTemplateDigest"]),
            "invalid exact legacy workload predecessor")
    address = ipaddress.ip_address(c["address"])
    require(address.is_global and address.version == 4 and str(address) == c["address"], "canonical public address required")
    for key in ("serving", "probe"):
        p = c[key]
        require(isinstance(p, dict) and set(p) == {"name", "uid", "generation"} and NAME.fullmatch(p["name"])
                and UID.fullmatch(p["uid"]) and type(p["generation"]) is int and p["generation"] > 0, "exact public and probe Services required")
    require(c["serving"]["name"] != c["probe"]["name"] and c["serving"]["uid"] != c["probe"]["uid"], "independent probe required")
    peers, hosts = c["peers"], c["hosts"]
    require(isinstance(peers, dict) and 1 <= len(peers) <= 16 and c["node"] not in peers
            and all(NAME.fullmatch(n) and UID.fullmatch(u) for n, u in peers.items()) and len(set(peers.values())) == len(peers),
            "exact non-target Pod roster required")
    require(isinstance(hosts, list) and 2 <= len(hosts) <= 8 and len(set(hosts)) == len(hosts)
            and all(re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,252}", h) for h in hosts), "explicit TLS route proof targets required")
    return c


def owner(pod, name, uid):
    return any(r.get("controller") is True and r.get("kind") == "DaemonSet" and r.get("name") == name and r.get("uid") == uid
               for r in pod["metadata"].get("ownerReferences", []))


def node_affinity(node, operator):
    return {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
        {"matchFields": [{"key": "metadata.name", "operator": operator, "values": [node]}]},
    ]}}}


def derive(c, ds):
    meta, spec = ds["metadata"], ds["spec"]
    require(meta["uid"] == c["legacyUID"] and not meta.get("deletionTimestamp") and spec["updateStrategy"] == {"type": "OnDelete"},
            "legacy Front must be the exact OnDelete DaemonSet")
    template = copy.deepcopy(spec["template"])
    annotations = meta.get("annotations", {})
    retired = annotations.get(GEN)
    pod_spec = template["spec"]
    if retired is not None:
        require(retired == kube.digest(c) and pod_spec.pop("affinity", None) == node_affinity(c["node"], "NotIn"),
                "the live node exclusion was not made by this declaration")
    else:
        require(meta["generation"] == c["legacyGeneration"] and not pod_spec.get("affinity"),
                "shared Front generation or affinity changed")
    require(kube.digest(template) == c["legacyTemplateDigest"] and not pod_spec.get("hostNetwork"),
            "shared Front template is not the exact pinned predecessor")
    require(len(pod_spec["containers"]) == 1 and pod_spec["containers"][0]["name"] == "edge-front"
            and not pod_spec.get("initContainers"), "unexpected legacy Front containers")
    container = pod_spec["containers"][0]
    ports = [p for p in container.get("ports", []) if p.get("hostPort")]
    require(sorted((p["hostPort"], p["containerPort"], p.get("protocol", "TCP")) for p in ports) == [(80, 80, "TCP"), (443, 443, "TCP")],
            "legacy Front transport is not exactly TCP 80/443")
    for p in ports:
        p.pop("hostPort")
        p.pop("hostIP", None)
    selector = copy.deepcopy(spec["selector"])
    labels = template["metadata"]["labels"]
    require(set(selector) == {"matchLabels"} and COMPONENT in selector["matchLabels"] and COMPONENT in labels
            and labels[COMPONENT] == selector["matchLabels"][COMPONENT] != c["isolated"]
            and ISOLATED not in selector["matchLabels"] and ISOLATED not in labels, "legacy and isolated selectors are ambiguous")
    clone_selector = copy.deepcopy(selector)
    identity = {ISOLATED: c["node"], COMPONENT: c["isolated"]}
    clone_selector["matchLabels"].update(identity)
    labels.update(identity)
    require("kubernetes.io/hostname" not in pod_spec.get("nodeSelector", {}), "legacy Front already has a different node restriction")
    pod_spec.setdefault("nodeSelector", {})["kubernetes.io/hostname"] = c["node"]
    pod_spec["affinity"] = node_affinity(c["node"], "In")
    clone = {"apiVersion": "apps/v1", "kind": "DaemonSet", "metadata": {"name": c["isolated"], "namespace": c["namespace"],
              "labels": {"app.kubernetes.io/managed-by": MANAGER}, "annotations": {GEN: kube.digest(c)}},
             "spec": {"selector": clone_selector, "template": template, "updateStrategy": {"type": "OnDelete"},
                      "revisionHistoryLimit": spec.get("revisionHistoryLimit", 10)}}
    patch = [] if retired else [
        {"op": "test", "path": "/metadata/uid", "value": meta["uid"]},
        {"op": "test", "path": "/metadata/resourceVersion", "value": meta["resourceVersion"]},
        {"op": "test", "path": "/spec", "value": spec},
        {"op": "add", "path": "/metadata/annotations", "value": dict(annotations, **{GEN: kube.digest(c)})},
        {"op": "add", "path": "/spec/template/spec/affinity", "value": node_affinity(c["node"], "NotIn")},
    ]
    return patch, clone


def has_host_ports(pod_spec):
    return any(p.get("hostPort") for x in pod_spec["containers"] + pod_spec.get("initContainers", []) for p in x.get("ports", []))


def retirable_tcp(raw):
    count = 0
    for line in raw.splitlines():
        cols = line.split()
        if not cols or cols[0] == "sl":
            continue
        require(len(cols) >= 4 and ":" in cols[1] and re.fullmatch(r"[A-F0-9]{2}", cols[3]), "malformed kernel TCP inventory")
        if int(cols[1].rsplit(":", 1)[1], 16) in (80, 443) and cols[3] not in ("0A", "06"):
            count += 1
    return count


def http_redirect(address, host):
    # The active 80/TCP listener handles the redirect; a TLS-only witness
    # would not see it disappear if the wrong host port were retired.
    with closing(http.client.HTTPConnection(address, timeout=5)) as conn:
        conn.request("HEAD", "/healthz", headers={"Host": host})
        with closing(conn.getresponse()) as response:
            locations = [v for k, v in response.getheaders() if k.lower() == "location"]
            require(response.status in (301, 302, 307, 308) and locations == ["https://" + host + "/healthz"],
                    "public HTTP is not redirecting to the current HTTPS entry")
            return response.status, locations[0]


class Cluster:
    def __init__(self, c):
        self.c, self.ns = c, c["namespace"]
        self.profile = {**{k: c[k] for k in ("namespace", "group", "node")}, "activationPath": "/var/lib/fugue-edge-front/activation.json"}

    def get(self, kind, name, *, optional=False):
        if optional:
            return kube.kubectl("get", kind, name, "-n", self.ns, "--ignore-not-found", "-o", "json")
        return front.read("get", kind, name, "-n", self.ns, "-o", "json")

    def list(self, *args):
        options = []
        if "-A" not in args and not any(arg == "-n" or arg == "--namespace" for arg in args):
            options.extend(["-n", self.ns])
        value = front.read("get", *args, *options, "-o", "json")
        require(not value.get("metadata", {}).get("continue"), "incomplete Kubernetes inventory")
        return value["items"]

    def workloads(self):
        c = self.c
        ds = self.get("daemonset", c["legacy"])
        patch, clone = derive(c, ds)
        pods = self.list("pods", "-n", self.ns, "-l", ",".join(f"{k}={v}" for k, v in ds["spec"]["selector"]["matchLabels"].items()))
        origin = [p for p in pods if owner(p, c["legacy"], c["legacyUID"])]
        old = [p for p in origin if p["spec"]["nodeName"] == c["node"]]
        require(len(old) <= 1 and (not old or old[0]["metadata"]["uid"] == c["oldUID"]), "target node does not contain the pinned old Front")
        peers = {p["spec"]["nodeName"]: {"uid": p["metadata"]["uid"], "processes": sorted(
            (s["name"], s.get("containerID"), s["restartCount"]) for s in p["status"].get("containerStatuses", [])),
            "host_ports": sorted(port["hostPort"] for container in p["spec"]["containers"]
                                 for port in container.get("ports", []) if port.get("hostPort"))}
            for p in origin if p["spec"]["nodeName"] != c["node"]}
        require(len(origin) == len(peers) + len(old) and {n: p["uid"] for n, p in peers.items()} == c["peers"]
                and all(p["host_ports"] == [80, 443] and p["processes"] and all(pid for _, pid, _ in p["processes"]) for p in peers.values()),
                "a peer Pod was replaced or cannot be observed")
        if patch:
            require(len(old) == 1 and not old[0]["metadata"].get("deletionTimestamp"), "exact old Front is not live")
        return ds, patch, clone, old, peers

    def ingress(self, isolated_labels, old=None):
        c = self.c
        svc, probe = (self.get("service", c[k]["name"]) for k in ("serving", "probe"))
        selected = None
        for key, service in (("serving", svc), ("probe", probe)):
            p, meta, spec = c[key], service["metadata"], service["spec"]
            is_probe = key == "probe"
            projected = replacement.projection(spec, probe=is_probe)
            require(meta["uid"] == p["uid"] and not meta.get("deletionTimestamp") and meta["annotations"][kube.GENERATION] == str(p["generation"])
                    and meta["annotations"][kube.DIGEST] == kube.digest(projected) and spec["externalIPs"] == [c["address"]]
                    and meta["labels"]["app.kubernetes.io/managed-by"] == (kube.MANAGER if is_probe else stage.MANAGER)
                    and spec["selector"].get("fugue.io/edge-node") == c["node"], "serving/probe immutable binding changed")
            if not is_probe:
                require(meta["annotations"]["transport.fugue.dev/phase"] == "serving", "public Service is not serving")
                picked = self.list("pods", "-n", self.ns, "-l", ",".join(f"{k}={v}" for k, v in spec["selector"].items()))
                require(len(picked) == 1 and picked[0]["metadata"]["uid"] == c["frontUID"], "public Front Pod changed")
                selected = picked[0]
            else:
                require(meta["annotations"][kube.FOLLOWS_SERVING] == c["serving"]["uid"] and spec["selector"] == svc["spec"]["selector"]
                        and len(spec["ports"]) == 1 and spec["ports"][0]["port"] == 15443, "probe Service selected a different Front")
            stage.selected_endpoint_witness(self.profile, {"name": meta["name"]}, self.profile, selected, service, probe=is_probe)
        for service in self.list("services", "-A"):
            meta, spec = service["metadata"], service["spec"]
            selector = spec.get("selector")
            public = set(spec.get("externalIPs") or [])
            public.update(x["ip"] for x in (service.get("status") or {}).get("loadBalancer", {}).get("ingress", []) if x.get("ip"))
            if c["address"] in public and meta["uid"] not in {c["serving"]["uid"], c["probe"]["uid"]}:
                require(not any(p.get("protocol") == "TCP" and p["port"] in (80, 443, 15443) for p in spec.get("ports", [])),
                        "another Service overlaps a current public listener")
            if meta["namespace"] == self.ns and selector:
                require(not all(isolated_labels.get(k) == v for k, v in selector.items()),
                        "a Service selector could admit the isolated Front")
                if old is not None:
                    require(not all(old["metadata"]["labels"].get(k) == v for k, v in selector.items()),
                            "a Service selector still targets the old Front")
        if old is not None:
            for s in self.list("endpointslices", "-A"):
                require(not any((x.get("targetRef") or {}).get("uid") == c["oldUID"] or old["status"]["podIP"] in x.get("addresses", [])
                                for x in (s.get("endpoints") or [])),
                        "an EndpointSlice still targets the old Front")
        return selected, {k: {"uid": x["metadata"]["uid"], "spec": x["spec"], "annotations": x["metadata"]["annotations"]}
                          for k, x in (("serving", svc), ("probe", probe))}

    def idle(self, pod):
        name = pod["metadata"]["name"]
        result = subprocess.run(["kubectl", "-n", self.ns, "exec", name, "-c", "edge-front", "--", "cat", "/proc/net/tcp", "/proc/net/tcp6"],
                                capture_output=True, text=True, timeout=12)
        require(result.returncode == 0 and bool(result.stdout) and len(result.stdout) < 2 << 20, "old Front kernel TCP inventory unavailable")
        require(retirable_tcp(result.stdout) == 0, "old Front still holds active or in-flight TCP connections")

    def write_exclusion(self, patch, dry=False):
        flags = ["--dry-run=server"] if dry else []
        return kube.kubectl("patch", "daemonset", self.c["legacy"], "-n", self.ns, "--type=json",
                            "--patch-file", "-", "--field-manager="+MANAGER, "-o", "json", *flags, body=front.canonical(patch))

    def create_isolated(self, clone, dry=False):
        flags = ["--dry-run=server"] if dry else []
        return kube.kubectl("create", "-f", "-", "-o", "json", "--field-manager="+MANAGER, *flags, body=front.canonical(clone))

    def lock(self, name, labels, duration):
        current = self.get("lease", name)
        require(current["metadata"]["labels"] == labels and not current["spec"].get("holderIdentity")
                and current["spec"]["leaseDurationSeconds"] == duration, "an authority Lease was replaced or is in use")
        holder = f"{MANAGER}:{os.environ['GITHUB_RUN_ID']}:{os.environ['GITHUB_RUN_ATTEMPT']}"
        now = front.now().isoformat(timespec="microseconds").replace("+00:00", "Z")
        spec = dict(current["spec"], holderIdentity=holder, acquireTime=now, renewTime=now)
        self.lease_patch(current, spec)
        return (name, current["metadata"]["uid"], holder)

    def lease_patch(self, current, spec):
        meta = current["metadata"]
        patch = [{"op": "test", "path": "/metadata/uid", "value": meta["uid"]},
                 {"op": "test", "path": "/metadata/resourceVersion", "value": meta["resourceVersion"]},
                 {"op": "test", "path": "/spec", "value": current["spec"]},
                 {"op": "replace", "path": "/spec", "value": spec}]
        return kube.kubectl("patch", "lease", meta["name"], "-n", self.ns, "--type=json", "--patch-file", "-",
                            "--field-manager="+MANAGER, "-o", "json", body=front.canonical(patch))

    def held(self, lock, *, renew=False):
        name, uid, holder = lock
        current = self.get("lease", name)
        spec = current["spec"]
        require(current["metadata"]["uid"] == uid and spec["holderIdentity"] == holder
                and front.now() + datetime.timedelta(seconds=5) <
                front.timestamp(spec["renewTime"]) + datetime.timedelta(seconds=spec["leaseDurationSeconds"]),
                "retirement lost its authority Lease")
        if renew:
            self.lease_patch(current, dict(spec, renewTime=front.now().isoformat(timespec="microseconds").replace("+00:00", "Z")))
        return current

    def unlock(self, lock):
        current = self.held(lock)
        spec = dict(current["spec"], holderIdentity="")
        for key in ("acquireTime", "renewTime"):
            spec.pop(key, None)
        self.lease_patch(current, spec)

    def no_worker_transaction(self):
        for phase in ("prepared", "activated"):
            require(self.get("configmap", f"fugue-authority-transition-{phase}-{self.c['group']}", optional=True) is None,
                    "a Worker authority transaction still references the shared Front")

    def check_clone(self, declared, live):
        require(live["metadata"]["annotations"].get(GEN) == kube.digest(self.c) and not live["metadata"].get("deletionTimestamp")
                and all(live["spec"].get(k) == v for k, v in declared["spec"].items()), "isolated Front differs from the declared clone")
        ds_uid = live["metadata"]["uid"]
        pods = self.list("pods", "-n", self.ns, "-l", f"{ISOLATED}={self.c['node']}")
        require(len(pods) <= 1, "isolated Front has more than one Pod")
        if not pods:
            return None
        pod = pods[0]
        require(owner(pod, declared["metadata"]["name"], ds_uid) and pod["spec"].get("nodeName", self.c["node"]) == self.c["node"]
                and not pod["metadata"].get("deletionTimestamp") and not pod["spec"].get("hostNetwork") and not has_host_ports(pod["spec"]),
                "isolated Front Pod has a foreign owner, placement or host port")
        expected = [(x["name"], x["image"]) for x in declared["spec"]["template"]["spec"]["containers"]]
        require([(x["name"], x["image"]) for x in pod["spec"]["containers"]] == expected, "isolated Front image differs from its predecessor")
        if pod["spec"].get("nodeName") == self.c["node"] and any(
                x["type"] == "Ready" and x["status"] == "True" for x in pod["status"].get("conditions", [])):
            return pod
        return None


def run(cluster, evidence):
    c = cluster.c
    node = front.read("get", "node", c["node"], "-o", "json")
    require(node["metadata"]["uid"] == c["nodeUID"] and node["metadata"]["labels"]["kubernetes.io/hostname"] == c["node"]
            and any(x["type"] == "Ready" and x["status"] == "True" for x in node["status"]["conditions"]), "target Node is not the pinned Ready node")
    report = {"schema": c["schema"], "declaration": kube.digest(c), "node": c["node"], "node_uid": c["nodeUID"], "result": "planned"}

    def save(**extra):
        report.update(extra)
        evidence.write_text(front.canonical(report) + "\n")

    def route_proofs():
        redirects = {}
        for host in c["hosts"]:
            redirects[host] = http_redirect(c["address"], host)
            front.stable_proof_pair(c["address"], 15443, host, "/", c["node"], c["group"])
        return redirects

    def peers_match():
        ds, patch, clone, old, observed = cluster.workloads()
        require(observed == peers, "non-target Front Pod or process changed")
        return ds, patch, clone, old

    save()
    with ExitStack() as cleanup:
        component = c["workerComponent"]
        locks = [
            ("fugue-production-" + component, {"app.kubernetes.io/managed-by": "fugue-declarative-release", "fugue.pro/component": component}, 900),
            ("fugue-authority-" + c["group"], {"app.kubernetes.io/managed-by": "fugue-release-guardian", "fugue.pro/group": c["group"]}, 120),
        ]
        for index, args in enumerate(locks):
            lock = cluster.lock(*args)
            cleanup.callback(cluster.unlock, lock)
            locks[index] = lock
        cluster.no_worker_transaction()
        ds, patch, clone, old, peers = cluster.workloads()
        isolated_labels = clone["spec"]["template"]["metadata"]["labels"]
        selected, transport = cluster.ingress(isolated_labels, old[0] if old else None)
        activation = front.state(cluster.profile, selected)
        with closing(connection.HeldTLS(c["address"], c["hosts"][0])) as sock:
            proof = sock.proof("/")
            held = connection.fact(cluster.profile, selected, sock)
            require(held["pod_uid"] == c["frontUID"], "public TLS did not reach the pinned Front")
            http_before = route_proofs()
            isolated = cluster.get("daemonset", c["isolated"], optional=True)
            require(not patch or isolated is None, "an unexpected isolated workload already exists")
            clone_expected = clone
            if isolated is None:
                dry_clone = cluster.create_isolated(clone, dry=True)
                require(all(dry_clone["spec"].get(k) == v for k, v in clone["spec"].items())
                        and not has_host_ports(dry_clone["spec"]["template"]["spec"]),
                        "server-side isolated Front dry run changed the declared spec")
                clone_expected = dry_clone
            if patch:
                dry = cluster.write_exclusion(patch, dry=True)
                require(derive(c, dry) == ([], clone), "server-side exclusion changed more than this node")
                cluster.idle(old[0])
                time.sleep(3)
                fresh, next_patch, _, now_old = peers_match()
                require(fresh["metadata"]["resourceVersion"] == ds["metadata"]["resourceVersion"] and next_patch == patch,
                        "shared workload changed before applying the exact exclusion")
                new_selected, fresh_transport = cluster.ingress(isolated_labels, now_old[0])
                require(fresh_transport == transport and front.state(cluster.profile, new_selected) == activation, "entry authority changed before retirement")
                cluster.no_worker_transaction()
                cluster.idle(now_old[0])
                for lock in locks:
                    cluster.held(lock, renew=True)
                save(result="prepared", target_old_uid=c["oldUID"], peers_before=peers, front_uid=c["frontUID"],
                     worker={key: activation[key] for key in ("active_slot", "generation", "worker_source_commit", "worker_image_digest")})
                live = cluster.write_exclusion(next_patch)
                require(derive(c, live) == ([], clone), "committed exclusion did not match the declared scope")
                save(result="exclusion_applied")
            end = time.monotonic() + 75
            while True:
                live, pending, _, old = peers_match()
                require(not pending, "the node exclusion disappeared")
                if not old and live["status"].get("observedGeneration") == live["metadata"]["generation"] and live["status"]["desiredNumberScheduled"] == len(peers):
                    break
                require(time.monotonic() < end, "the DaemonSet controller did not retire the old primary Front")
                time.sleep(3)
            save(result="old_released", old_uid_absent=True)
            for lock in locks:
                cluster.held(lock, renew=True)
            if isolated is None:
                try:
                    isolated = cluster.create_isolated(clone)
                except ValueError:
                    isolated = cluster.get("daemonset", c["isolated"], optional=True)
                    if isolated is None:
                        raise
            end = time.monotonic() + 80
            while True:
                live = cluster.get("daemonset", c["isolated"])
                require(live["metadata"]["uid"] == isolated["metadata"]["uid"], "isolated workload was replaced during verification")
                pod = cluster.check_clone(clone_expected, live)
                if pod:
                    break
                require(time.monotonic() < end, "the no-hostPort Front did not become Ready")
                time.sleep(3)
            save(result="isolated_ready", isolated_uid=pod["metadata"]["uid"], host_ports=[])
            peers_match()
            current, observed = cluster.ingress(isolated_labels)
            require(observed == transport and front.state(cluster.profile, current) == activation, "public ingress or Worker changed")
            require(route_proofs() == http_before, "the public 80/TCP redirect changed")
            after_proof = sock.proof("/")
            after_held = connection.fact(cluster.profile, current, sock)
            require(after_proof["edge"] == proof["edge"] == c["node"] and after_proof["group"] == proof["group"] == c["group"]
                    and after_held == held, "the same public TLS connection did not survive retirement")
            save(result="verified", peers_unchanged=True, ingress_unchanged=True, public_connection_preserved=True,
                 public_http_redirects=http_before, public_connection_before=held, public_connection_after=after_held,
                 replacement_image=pod["spec"]["containers"][0]["image"])


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("operation", choices=("validate", "run"))
    p.add_argument("config", type=Path)
    p.add_argument("--evidence", type=Path)
    args = p.parse_args()
    c = validate(json.loads(args.config.read_text()))
    if args.operation == "run":
        require(os.getenv("GITHUB_ACTIONS") == "true" and os.getenv("GITHUB_EVENT_NAME") == "push"
                and os.getenv("GITHUB_REF") == "refs/heads/main" and args.evidence is not None and not args.evidence.exists(),
                "retirement only runs through the main deployment lane")
        run(Cluster(c), args.evidence)


if __name__ == "__main__":
    main()
