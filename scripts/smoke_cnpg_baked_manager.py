#!/usr/bin/env python3
"""Exercise both manager modes on a disposable, explicitly fenced kind cluster."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time

try:
    from . import baked_postgres_contract as contract
except ImportError:
    import baked_postgres_contract as contract

MANIFEST_SHA256 = "858854263f9cbb4f0cf3f393305396f123a72073d33a0710c8cc389f0b7c4a5b"
CHECKS = ["default_initdb_without_bootstrap", "sql_read_write", "container_restart", "pod_recreation", "legacy_unchanged"]


def manifest_objects(value):
    # kubectl create -f a multi-document YAML emits consecutive JSON objects,
    # not a JSON List. Decode every document without accepting trailing junk.
    decoder, items = json.JSONDecoder(), []
    remaining = value.strip()
    while remaining:
        document, end = decoder.raw_decode(remaining)
        if not isinstance(document, dict):
            raise ValueError("installation manifest contains a non-object")
        items.extend(document["items"] if document.get("kind") == "List" else [document])
        if len(items) > 256:
            raise ValueError("installation manifest exceeds object bound")
        remaining = remaining[end:].lstrip()
    if not items:
        raise ValueError("installation manifest is empty")
    return {"apiVersion": "v1", "kind": "List", "items": items}


def verify_initdb_job(job):
    spec = job["spec"]["template"]["spec"]
    containers = spec.get("containers", [])
    if spec.get("initContainers") or not any(c.get("command", [])[:1] == ["/usr/local/bin/fugue-cnpg-manager"] for c in containers):
        raise ValueError("observed initdb job still reserves bootstrap resources or uses the old command")
    return job["metadata"]["uid"]


class Rehearsal:
    def __init__(self, args):
        self.args = args
        self.context = "kind-" + args.cluster_name
        if not re.fullmatch(r"[0-9a-f]{40}", args.revision) or args.cluster_name != "fugue-baked-" + args.revision[:12]:
            raise ValueError("only the exact revision-scoped disposable cluster is permitted")
        # Every call has both an explicit kubeconfig and context; never inherit
        # a developer's or a production runner's active kubectl context.
        self.command = ["kubectl", "--kubeconfig", str(args.kubeconfig), "--context", self.context, "--request-timeout=30s"]
        config = self.kube("config", "view", "--minify", "-o", "json")
        server = config["clusters"][0]["cluster"]["server"]
        if not re.fullmatch(r"https://127\.0\.0\.1:[0-9]+", server):
            raise ValueError("rehearsal refuses a non-loopback Kubernetes server")
        nodes = self.kube("get", "nodes", "-o", "json")["items"]
        if len(nodes) != 1 or nodes[0]["metadata"]["name"] != args.cluster_name + "-control-plane":
            raise ValueError("rehearsal cluster identity differs")

    def kube(self, *args, body=None, raw=False):
        result = subprocess.run([*self.command, *args], input=None if body is None else json.dumps(body),
                                capture_output=True, text=True, timeout=150)
        if result.returncode:
            raise RuntimeError("kubectl " + " ".join(args[:4]) + ": " + result.stderr[-5000:])
        if raw:
            return result.stdout.strip()
        return json.loads(result.stdout) if result.stdout.strip() else None

    def apply(self, value):
        self.kube("apply", "--server-side", "-f", "-", body=value, raw=True)

    def wait(self, observe, predicate, message, timeout=420):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            last = observe()
            if predicate(last):
                return last
            time.sleep(2)
        raise RuntimeError(message + ": " + json.dumps(last)[-2500:])

    def pod(self, cluster):
        values = self.kube("-n", "manager-check", "get", "pods", "-l", "cnpg.io/cluster=" + cluster, "-o", "json")["items"]
        values = [p for p in values if p["metadata"].get("labels", {}).get("cnpg.io/podRole") == "instance" and not p["metadata"].get("deletionTimestamp")]
        return values[0] if len(values) == 1 else None

    @staticmethod
    def ready(pod):
        return bool(pod) and any(c.get("type") == "Ready" and c.get("status") == "True" for c in pod.get("status", {}).get("conditions", []))

    def sql(self, pod, statement):
        return self.kube("-n", "manager-check", "exec", pod["metadata"]["name"], "-c", "postgres", "--",
                         "psql", "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-Atc", statement, raw=True)

    def cluster(self, name, image=None, hold_initdb=False):
        spec = {"instances": 1, "storage": {"size": "1Gi"}, "enableSuperuserAccess": False,
                "resources": {"requests": {"cpu": "5m", "memory": "10Mi"}, "limits": {"cpu": "1", "memory": "512Mi"}}}
        if image:
            spec["imageName"] = image
        if hold_initdb:
            # Only this disposable fixture: keep the completed-job GC from
            # racing the observation of the real initialization Job spec.
            spec["bootstrap"] = {"initdb": {"postInitSQL": ["SELECT pg_sleep(10)"]}}
        self.apply({"apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster", "metadata": {"name": name, "namespace": "manager-check"}, "spec": spec})

    def execute(self):
        args = self.args
        catalog = contract.validate(json.loads(args.catalog.read_text()))
        if hashlib.sha256(args.operator_manifest.read_bytes()).hexdigest() != MANIFEST_SHA256:
            raise ValueError("operator installation manifest is not the pinned upstream version")
        objects = manifest_objects(self.kube("create", "--dry-run=client", "--validate=false", "-f", str(args.operator_manifest), "-o", "json", raw=True))
        deployments = [item for item in objects["items"] if item["kind"] == "Deployment"]
        if len(deployments) != 1:
            raise ValueError("upstream controller Deployment is ambiguous")
        deployment = deployments[0]
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        container["image"] = args.candidate
        container["command"] = ["/fugue-cnpg-controller"]
        for env in container.get("env", []):
            if env["name"] == "OPERATOR_IMAGE_NAME":
                env["value"] = contract.MANAGER_IMAGE
        self.apply(objects)
        ns, name = deployment["metadata"]["namespace"], deployment["metadata"]["name"]
        self.kube("-n", ns, "rollout", "status", "deployment/" + name, "--timeout=120s", raw=True)
        self.apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "manager-check"}})
        self.cluster("legacy", contract.POSTGRES_IMAGE)
        legacy = self.wait(lambda: self.pod("legacy"), self.ready, "legacy database failed startup")
        if not any(c["name"] == "bootstrap-controller" for c in legacy["spec"].get("initContainers", [])):
            raise ValueError("unapproved legacy image lost its bootstrap")
        self.cluster("baked", hold_initdb=True)
        jobs = self.wait(lambda: self.kube("-n", "manager-check", "get", "jobs", "-l", "cnpg.io/cluster=baked", "-o", "json")["items"],
                         bool, "initialization Job was not observed before cleanup", timeout=120)
        initdb_job_uid = verify_initdb_job(jobs[0])
        baked = self.wait(lambda: self.pod("baked"), self.ready, "baked database failed startup")
        current = self.kube("-n", "manager-check", "get", "cluster", "baked", "-o", "json")
        if current["spec"]["imageName"] != catalog["defaultImage"]:
            raise ValueError("new Cluster did not persist its approved default image")
        if baked["spec"].get("initContainers"):
            raise ValueError("baked instance still reserves bootstrap resources")
        pg = next(c for c in baked["spec"]["containers"] if c["name"] == "postgres")
        if pg["resources"]["requests"]["memory"] != "10Mi" or pg["command"][0] != "/usr/local/bin/fugue-cnpg-manager":
            raise ValueError("baked startup command or scheduling request changed")
        self.sql(baked, "CREATE TABLE continuity_check (value text); INSERT INTO continuity_check VALUES ('kept'); CHECKPOINT;")
        if self.sql(baked, "SELECT value FROM continuity_check") != "kept":
            raise ValueError("SQL read/write failed")
        name = baked["metadata"]["name"]
        uid = baked["metadata"]["uid"]
        # Atomic replacement must survive a container restart, but not be
        # recopied from the image. This does not touch PostgreSQL's data volume.
        self.kube("-n", "manager-check", "exec", name, "-c", "postgres", "--", "sh", "-ec",
                  "test -L /controller/manager; cp /usr/local/libexec/cnpg-manager /controller/manager.new; chmod 755 /controller/manager.new; mv -f /controller/manager.new /controller/manager; kill -TERM 1", raw=True)
        restarts = baked["status"]["containerStatuses"][0]["restartCount"]
        baked = self.wait(lambda: self.pod("baked"), lambda p: self.ready(p) and p["status"]["containerStatuses"][0]["restartCount"] > restarts,
                          "baked manager did not restart")
        if baked["metadata"]["uid"] != uid or self.sql(baked, "SELECT value FROM continuity_check") != "kept":
            raise ValueError("container restart changed database identity or data")
        self.kube("-n", "manager-check", "exec", name, "-c", "postgres", "--", "sh", "-ec", "test ! -L /controller/manager; test -x /controller/manager", raw=True)
        self.kube("-n", "manager-check", "delete", "pod", name, "--wait=true", "--timeout=120s", raw=True)
        baked = self.wait(lambda: self.pod("baked"), lambda p: self.ready(p) and p["metadata"]["uid"] != uid, "baked Pod recreation failed")
        if self.sql(baked, "SELECT value FROM continuity_check") != "kept":
            raise ValueError("Pod replacement lost database contents")
        self.kube("-n", "manager-check", "exec", baked["metadata"]["name"], "-c", "postgres", "--", "sh", "-ec", "test -L /controller/manager", raw=True)
        time.sleep(20)
        stable, old = self.pod("baked"), self.pod("legacy")
        if not self.ready(stable) or stable["metadata"]["uid"] != baked["metadata"]["uid"] or stable["status"]["containerStatuses"][0]["restartCount"] != 0:
            raise ValueError("baked manager enters a restart or reconciliation loop")
        if not self.ready(old) or old["metadata"]["uid"] != legacy["metadata"]["uid"] or old["status"]["containerStatuses"] != legacy["status"]["containerStatuses"]:
            raise ValueError("legacy database changed during baked operations")
        return {"candidate_image": args.candidate, "source_revision": args.revision, "baked_image": catalog["defaultImage"],
                "checks": CHECKS, "initdb_job_uid": initdb_job_uid,
                "baked_pod_uid": stable["metadata"]["uid"], "legacy_pod_uid": old["metadata"]["uid"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--cluster-name", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--operator-manifest", type=Path, required=True)
    args = parser.parse_args()
    rehearsal = Rehearsal(args)
    try:
        print(json.dumps(rehearsal.execute(), sort_keys=True))
    except Exception:
        for command in (("get", "pods,jobs,clusters.postgresql.cnpg.io", "-A", "-o", "wide"),
                        ("-n", "manager-check", "get", "pods,jobs,clusters.postgresql.cnpg.io", "-o", "json"),
                        ("get", "events", "-A", "--sort-by=.lastTimestamp"),
                        ("-n", "cnpg-system", "logs", "deployment/cnpg-controller-manager", "--tail=150")):
            try:
                print(rehearsal.kube(*command, raw=True), file=sys.stderr)
            except Exception as diagnostic_error:
                print(str(diagnostic_error), file=sys.stderr)
        try:
            for pod in rehearsal.kube("-n", "manager-check", "get", "pods", "-o", "json")["items"][:10]:
                for container in (pod["spec"].get("initContainers", []) + pod["spec"].get("containers", []))[:4]:
                    for previous in ([], ["--previous"]):
                        try:
                            print(rehearsal.kube("-n", "manager-check", "logs", pod["metadata"]["name"], "-c", container["name"],
                                                 "--tail=120", *previous, raw=True), file=sys.stderr)
                        except Exception as diagnostic_error:
                            print(str(diagnostic_error), file=sys.stderr)
        except Exception as diagnostic_error:
            print(str(diagnostic_error), file=sys.stderr)
        raise
