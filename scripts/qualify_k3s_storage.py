"""Real Longhorn/CNPG continuity fixtures, ONLY in the disposable qualification VM."""
import base64
import json
import os
from pathlib import Path
import threading
import time

from scripts.qualify_k3s_bootstrap import ROOT, kubectl, run


class StorageQualification:
    def __init__(self, evidence):
        self.evidence = evidence
        self.config = json.loads((ROOT / "tests/k3s-upgrade/storage-qualification.json").read_text())
        self.stop = threading.Event()
        self.threads = []
        self.connections = {}
        self.failures = []
        self.samples = {"single": [], "replicated": []}
        self.baseline = None

    def helm_longhorn(self, version):
        env = dict(os.environ, KUBECONFIG="/etc/rancher/k3s/k3s.yaml")
        result = run("helm", "upgrade", "--install", "qualification-longhorn", "longhorn/longhorn",
                     "--version", version, "--namespace", "longhorn-system", "--create-namespace",
                     "--set", "defaultSettings.defaultReplicaCount=1",
                     "--set", "persistence.defaultClassReplicaCount=1",
                     "--set", "defaultSettings.guaranteedInstanceManagerCPU=1",
                     "--wait", "--timeout", "10m", timeout=660, env=env)
        (self.evidence / ("longhorn-" + version + ".txt")).write_text(result.stdout + result.stderr)

    def setup(self):
        import yaml
        import psycopg
        run("helm", "repo", "add", "longhorn", "https://charts.longhorn.io")
        run("helm", "repo", "update", timeout=300)
        self.helm_longhorn(self.config["longhorn_baseline"])
        kubectl("create", "namespace", "cnpg-system")
        chart = ROOT / self.config["cnpg_chart"]
        rendered = run("helm", "template", "qualification-cnpg", str(chart), "--namespace",
                       "cnpg-system", "--include-crds", "--set", "image.tag=1.29.0").stdout
        documents = [document for document in yaml.safe_load_all(rendered) if document is not None]
        deployments = []
        for document in documents:
            if document and document.get("kind") == "Deployment":
                deployments.append(document["metadata"]["name"])
                for container in document["spec"]["template"]["spec"]["containers"]:
                    if container["name"] == "manager":
                        container["image"] = self.config["cnpg_operator_image"]
                        container["command"] = ["/fugue-cnpg-controller"]
                        container["resources"] = {"requests": {"cpu": "50m", "memory": "128Mi"},
                                                  "limits": {"cpu": "1", "memory": "512Mi"}}
                        for variable in container["env"]:
                            if variable["name"] == "OPERATOR_IMAGE_NAME":
                                variable["value"] = self.config["cnpg_operator_image"]
        manifest = self.evidence / "cnpg-operator.yaml"
        manifest.write_text(yaml.safe_dump_all(documents))
        kubectl("apply", "--server-side", "-f", str(manifest))
        assert len(deployments) == 1
        run("k3s", "kubectl", "-n", "cnpg-system", "rollout", "status",
            "deployment/" + deployments[0], "--timeout=300s", timeout=320)
        for offset, (name, instances) in enumerate((("single", 1), ("replicated", 2))):
            cluster = {"apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster",
                       "metadata": {"name": name, "namespace": "upgrade-qualification"},
                       "spec": {"instances": instances, "imageName": self.config["postgres_image"],
                                "storage": {"size": "2Gi", "storageClass": "longhorn"},
                                "resources": {"requests": {"cpu": "50m", "memory": "256Mi"},
                                              "limits": {"cpu": "1", "memory": "512Mi"}}}}
            service = {"apiVersion": "v1", "kind": "Service",
                       "metadata": {"name": name + "-probe", "namespace": "upgrade-qualification"},
                       "spec": {"type": "NodePort", "selector": {"cnpg.io/cluster": name, "cnpg.io/instanceRole": "primary"},
                                "ports": [{"port": 5432, "targetPort": 5432, "nodePort": 30432 + offset}]}}
            path = self.evidence / (name + ".json")
            path.write_text(json.dumps({"apiVersion": "v1", "kind": "List", "items": [cluster, service]}, indent=2))
            kubectl("apply", "-f", str(path))
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            clusters = json.loads(kubectl("-n", "upgrade-qualification", "get", "clusters", "-o", "json").stdout)["items"]
            self.write("clusters-latest.json", clusters)
            if len(clusters) == 2 and all(c.get("status", {}).get("readyInstances") == c["spec"]["instances"] for c in clusters):
                break
            time.sleep(3)
        else:
            raise RuntimeError("Longhorn-backed CNPG fixtures failed to become fully Ready")
        for offset, name in enumerate(self.samples):
            # The secret is generated solely in this disposable test cluster.
            # Never log credentials or include Secrets in uploaded evidence.
            secret = json.loads(kubectl("-n", "upgrade-qualification", "get", "secret", name + "-app", "-o", "json").stdout)["data"]
            password = base64.b64decode(secret["password"]).decode()
            connection = psycopg.connect(host="127.0.0.1", port=30432 + offset, dbname="app", user="app",
                                        password=password, connect_timeout=5, autocommit=True,
                                        options="-c statement_timeout=3000")
            connection.execute("CREATE TABLE continuity_ledger (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, value text NOT NULL)")
            self.connections[name] = connection
        self.baseline = self.capture()
        self.write("storage-baseline.json", self.baseline)

    def write(self, filename, value):
        (self.evidence / filename).write_text(json.dumps(value, indent=2, default=str) + "\n")

    def diagnostics(self):
        # Failed baseline setup is not an upgrade failure. Preserve enough
        # evidence to distinguish admission, image, initdb, I/O and API waits.
        for namespace, resource in [("upgrade-qualification", "clusters"),
                                    ("upgrade-qualification", "pvc"),
                                    ("longhorn-system", "volumes.longhorn.io"),
                                    ("longhorn-system", "engines.longhorn.io"),
                                    ("longhorn-system", "replicas.longhorn.io")]:
            result = kubectl("-n", namespace, "get", resource, "-o", "json", check=False)
            (self.evidence / (namespace + "-" + resource + ".json")).write_text(result.stdout + result.stderr)
        for namespace in ("upgrade-qualification", "cnpg-system"):
            result = kubectl("-n", namespace, "get", "pods", "-o", "json", check=False)
            if result.returncode:
                continue
            for item in json.loads(result.stdout)["items"]:
                name = item["metadata"]["name"]
                result = kubectl("-n", namespace, "logs", name, "--all-containers", "--tail=200", check=False)
                (self.evidence / (namespace + "-" + name + ".log")).write_text(result.stdout + result.stderr)
                if "initdb" in name:
                    result = kubectl("-n", namespace, "exec", name, "-c", "initdb", "--", "ps",
                                     "-e", "-o", "pid,ppid,stat,wchan:30,comm", check=False)
                    (self.evidence / (name + "-processes.txt")).write_text(result.stdout + result.stderr)

    def capture(self):
        pods = json.loads(kubectl("-n", "upgrade-qualification", "get", "pods", "-l", "cnpg.io/cluster", "-o", "json").stdout)["items"]
        identities = {}
        for item in pods:
            identities[item["metadata"]["name"]] = {
                "uid": item["metadata"]["uid"],
                "containers": {c["name"]: {"id": c["containerID"], "restarts": c["restartCount"]}
                               for c in item["status"].get("containerStatuses", [])},
                "mountinfo": kubectl("-n", "upgrade-qualification", "exec", item["metadata"]["name"],
                                     "-c", "postgres", "--", "cat", "/proc/self/mountinfo").stdout}
        volumes = json.loads(kubectl("-n", "longhorn-system", "get", "volumes.longhorn.io", "-o", "json").stdout)["items"]
        assert len(volumes) == 3
        ownership = {}
        for volume in volumes:
            status, spec = volume["status"], volume["spec"]
            assert status["state"] == "attached" and status["robustness"] == "healthy", volume["metadata"]["name"]
            ownership[volume["metadata"]["name"]] = {
                "uid": volume["metadata"]["uid"], "node": status["currentNodeID"],
                "data_engine": spec.get("dataEngine", "v1"), "image": spec.get("image")}
        return {"pods": identities, "volumes": ownership}

    def observe(self):
        def worker(name, connection):
            expected_identity = None
            counter = 0
            while not self.stop.is_set():
                try:
                    value = "committed-write-" + str(counter)
                    row = connection.execute("INSERT INTO continuity_ledger (value) VALUES (%s) RETURNING id, pg_backend_pid(), pg_postmaster_start_time()", (value,)).fetchone()
                    identity = (row[1], row[2].isoformat())
                    if expected_identity is None:
                        expected_identity = identity
                    assert identity == expected_identity, "database backend or postmaster restarted"
                    assert row[0] == counter + 1
                    self.samples[name].append({"at": time.time(), "id": row[0], "backend_pid": row[1], "postmaster_start": identity[1]})
                    counter += 1
                except Exception as exc:
                    self.failures.append({"database": name, "at": time.time(), "error": type(exc).__name__, "detail": str(exc)})
                    return
                self.stop.wait(0.2)
        for name, connection in self.connections.items():
            thread = threading.Thread(target=worker, args=(name, connection))
            self.threads.append(thread)
            thread.start()

    def checkpoint(self, version):
        state = self.capture()
        self.write(version + "-storage.json", state)
        assert state == self.baseline, "database identity, mount, or storage ownership changed"
        assert not self.failures, "established PostgreSQL connection or committed writes failed"
        self.write("postgres-continuity.json", {"samples": self.samples, "failures": self.failures})

    def upgrade_longhorn(self):
        self.helm_longhorn(self.config["longhorn_candidate"])
        self.checkpoint("longhorn-" + self.config["longhorn_candidate"])

    def finish(self):
        self.stop.set()
        for thread in self.threads:
            thread.join(timeout=10)
        for name, connection in self.connections.items():
            rows = connection.execute("SELECT id, value FROM continuity_ledger ORDER BY id").fetchall()
            assert len(rows) == len(self.samples[name])
            assert all(row == (index + 1, "committed-write-" + str(index)) for index, row in enumerate(rows))
            connection.close()
        self.write("postgres-continuity.json", {"samples": self.samples, "failures": self.failures})
        assert not self.failures
