import base64
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts import stage_edge_origin_relay_trust as relay


def uid(i):
    return f"00000000-0000-0000-0000-{i:012d}"


def fixture():
    return relay.validate_declaration({"schema": "fugue.edge.origin-relay-trust-stage/v1", "namespace": "test-system", "group": "edge-group-test",
            "installer_image": "registry.example.test/team/edge@sha256:" + "7" * 64, "ca_sha256": "sha256:" + "2" * 64,
            "policy_verify_key": "4" * 64, "targets": [
                {"edge": f"node-{i}", "slot": "b", "tls_name": f"slot-b.node-{i}.example.test", "leaf_sha256": "sha256:" + str(i) * 64,
                 "node_uid": uid(i), "workload": "test-worker-b", "workload_uid": uid(10)} for i in (1, 2)]})


def private(d):
    return {"ca_pem": "fixture CA", "packages": [
        {"edge": t["edge"], "slot": t["slot"], "cert_pem": f"certificate for {t['edge']}", "key_pem": f"PRIVATE for {t['edge']}"} for t in d["targets"]]}


def observed_pod(job):
    return {"metadata": {"name": job["metadata"]["name"] + "-f01", "namespace": job["metadata"]["namespace"], "uid": uid(80),
            "ownerReferences": [{"kind": "Job", "controller": True, "uid": uid(72)}]},
            "spec": copy.deepcopy(job["spec"]["template"]["spec"]),
            "status": {"conditions": [{"type": "Ready", "status": "True"}]}}


class StageRelayTest(unittest.TestCase):
    def test_package_is_bound_and_no_issuer_private_material_accepted(self):
        d = fixture()
        p = private(d)
        paired = relay.pair_packages(d, p)
        self.assertEqual(paired["packages"][1]["key_pem"], p["packages"][1]["key_pem"])
        self.assertEqual(paired["packages"][0]["leaf_sha256"], d["targets"][0]["leaf_sha256"])
        for field in ("policy_signer", "ca_key"):
            with self.assertRaises(ValueError):
                relay.pair_packages(d, {**p, field: "foreign"})
        p["packages"][1]["slot"] = "a"
        with self.assertRaises(ValueError):
            relay.pair_packages(d, p)

    def test_exact_node_and_workload_uid_and_slot_mount(self):
        d = fixture()
        t = d["targets"][0]
        n = {"metadata": {"name": t["edge"], "uid": t["node_uid"]}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
        ds = {"metadata": {"name": t["workload"], "namespace": d["namespace"], "uid": t["workload_uid"]}, "spec": {"template": {"spec": {
            "containers": [{"name": "edge", "image": d["installer_image"], "volumeMounts": [{"name": "worker-state", "mountPath": "/var/lib/fugue/edge"}]}],
            "volumes": [{"name": "worker-state", "hostPath": {"type": "DirectoryOrCreate", "path": relay.slot_home(d, t)}}]}}}}
        self.assertEqual(d["installer_image"], relay.attest(d, t, n, ds))
        with self.assertRaises(ValueError):
            relay.attest(d, t, {**n, "metadata": {**n["metadata"], "uid": uid(3)}}, ds)
        with self.assertRaises(ValueError):
            relay.attest(d, {**t, "slot": "a"}, n, ds)

    def test_installer_cannot_see_other_slots_or_service_account(self):
        d = fixture()
        t = d["targets"][0]
        job = relay.installer(d, t, "attempt")
        spec = job["spec"]["template"]["spec"]
        self.assertEqual(t["edge"], spec["nodeName"])
        self.assertFalse(spec["automountServiceAccountToken"])
        self.assertEqual(relay.slot_home(d, t) + "/origin-relay-v1/" + "2" * 64, spec["volumes"][0]["hostPath"]["path"])
        self.assertNotEqual(relay.trust_home(d, t), relay.trust_home({**d, "ca_sha256": "sha256:" + "6" * 64}, t))
        pod = observed_pod(job)
        relay.verify_installer_pod(pod, job, uid(72))
        with self.assertRaises(ValueError):
            relay.verify_installer_pod(pod, job, uid(73))
        pod["spec"]["containers"].append({"name": "unexpected"})
        with self.assertRaises(ValueError):
            relay.verify_installer_pod(pod, job, uid(72))

    def test_private_stream_is_sent_only_to_owned_job_after_all_prechecks(self):
        d = fixture()
        checked = relay.pair_packages(d, private(d))
        live_job = None
        stages, deliveries = [], []

        def kube(*args, data=None, **_):
            nonlocal live_job
            if "create" in args:
                self.assertNotIn(b"PRIVATE", data)
                if "--dry-run=server" in args:
                    stages.append("precheck")
                    return b""
                live_job = json.loads(data)
                stages.append("create")
                return relay.canonical({"metadata": {"name": live_job["metadata"]["name"], "uid": uid(72)}})
            if "get" in args:
                return relay.canonical({"items": [observed_pod(live_job)]})
            if "exec" in args:
                deliveries.append((live_job["spec"]["template"]["spec"]["nodeName"], base64.b64decode(data.splitlines()[2]).decode()))
                return b"relay-trust-staged\n"
            if "wait" in args:
                return b""
            raise AssertionError("unexpected Kube operation")

        with patch.object(relay, "read_checkpoint", return_value=d["installer_image"]) as checkpoint, patch.object(relay, "kubectl", side_effect=kube):
            receipt = relay.stage(d, checked)
        self.assertEqual(stages, ["precheck", "precheck", "create", "create"])
        self.assertGreaterEqual(checkpoint.call_count, 6)
        self.assertEqual(deliveries, [("node-1", "PRIVATE for node-1"), ("node-2", "PRIVATE for node-2")])
        self.assertNotIn(b"PRIVATE", relay.canonical(receipt))
        self.assertFalse(receipt["lease_published"])

    @unittest.skipUnless(sys.platform == "linux", "requires Linux coreutils support for durable publication")
    def test_installer_is_atomic_write_once(self):
        with tempfile.TemporaryDirectory() as root:
            destination, done = Path(root) / "mount", Path(root) / "exit"
            destination.mkdir()
            done.mkdir()
            script = relay.INSTALL.replace("/relay", str(destination)).replace("/exit", str(done))

            def install(key):
                data = b"".join(base64.b64encode(x) + b"\n" for x in (b"root", b"leaf", key, b"verify"))
                return subprocess.run(["/bin/sh", "-ec", script], input=data, capture_output=True, timeout=10)

            self.assertEqual(install(b"key-1").returncode, 0)
            self.assertEqual(install(b"key-1").returncode, 0)
            self.assertNotEqual(install(b"foreign-key").returncode, 0)
            self.assertEqual((destination / "trust" / "key.pem").read_bytes(), b"key-1")
            self.assertEqual({p.name for p in destination.iterdir()}, {"trust"})


if __name__ == "__main__":
    unittest.main()
