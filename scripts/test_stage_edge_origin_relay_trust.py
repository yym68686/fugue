import base64
import copy
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
    return relay.validate_declaration({"schema": "fugue.edge.origin-relay-trust-stage/v2", "namespace": "test-system", "group": "edge-group-test",
            "ca_sha256": "sha256:" + "2" * 64,
            "policy_verify_key": "4" * 64, "targets": [
                {"edge": f"node-{i}", "slot": "b", "tls_name": f"slot-b.node-{i}.example.test", "leaf_sha256": "sha256:" + str(i) * 64,
                 "retained_image": "registry.example.test/team/edge@sha256:" + "7" * 64,
                 "node_uid": uid(i), "workload": "test-worker-b", "workload_uid": uid(10)} for i in (1, 2)]})


def private(d):
    return {"ca_pem": "fixture CA", "packages": [
        {"edge": t["edge"], "slot": t["slot"], "cert_pem": f"certificate for {t['edge']}", "key_pem": f"PRIVATE for {t['edge']}"} for t in d["targets"]]}


def observed(d, t):
    worker = {"name": "edge", "image": t["retained_image"], "resources": {"limits": {"memory": "1Gi"}},
              "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
              "volumeMounts": [{"name": "worker-state", "mountPath": relay.MOUNT}]}
    spec = {"nodeName": t["edge"], "automountServiceAccountToken": False, "containers": [worker],
            "volumes": [{"name": "worker-state", "hostPath": {"type": "DirectoryOrCreate", "path": relay.slot_home(d, t)}}]}
    node = {"metadata": {"name": t["edge"], "uid": t["node_uid"]}, "status": {"conditions": [
        {"type": "Ready", "status": "True"},
        *({"type": c, "status": "False"} for c in ("MemoryPressure", "DiskPressure", "PIDPressure")),
    ]}}
    ds = {"metadata": {"name": t["workload"], "namespace": d["namespace"], "uid": t["workload_uid"]},
          "spec": {"template": {"spec": copy.deepcopy(spec)}}}
    pod = {"metadata": {"name": "test-worker-b-pod", "namespace": d["namespace"], "uid": uid(72),
                        "labels": {"fugue.io/edge-slot": t["slot"]},
                        "ownerReferences": [{"kind": "DaemonSet", "apiVersion": "apps/v1", "controller": True,
                                             "name": t["workload"], "uid": t["workload_uid"]}]},
           "spec": spec, "status": {"conditions": [{"type": "Ready", "status": "True"}], "containerStatuses": [
               {"name": "edge", "state": {"running": {"startedAt": "fixture"}}, "ready": True, "started": True,
                "imageID": t["retained_image"], "containerID": "containerd://fixture"}]}}
    return node, ds, pod


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
        n, ds, pod = observed(d, t)
        worker = relay.attest_workload(d, t, n, ds)
        self.assertEqual(("test-worker-b-pod", uid(72), "containerd://fixture"), relay.attest_retained(d, t, pod, worker))
        with self.assertRaises(ValueError):
            relay.attest_workload(d, t, {**n, "metadata": {**n["metadata"], "uid": uid(3)}}, ds)
        with self.assertRaises(ValueError):
            relay.attest_workload(d, {**t, "slot": "a"}, n, ds)
        n["status"]["conditions"][1]["status"] = "True"
        with self.assertRaises(ValueError):
            relay.attest_workload(d, t, n, ds)

    def test_reject_inexact_retained_container(self):
        d = fixture()
        t = d["targets"][0]
        for keys, value in (
            (("metadata", "ownerReferences", 0, "name"), "other-workload"),
            (("metadata", "labels", "fugue.io/edge-slot"), "a"),
            (("spec", "nodeName"), "other-node"),
            (("spec", "automountServiceAccountToken"), True),
            (("spec", "hostNetwork"), True),
            (("spec", "containers", 0, "resources"), {"limits": {"memory": "2Gi"}}),
            (("spec", "containers", 0, "volumeMounts", 0, "subPath"), "foreign"),
            (("status", "containerStatuses", 0, "ready"), False),
            (("status", "containerStatuses", 0, "imageID"), "untrusted-image"),
        ):
            with self.subTest(keys=keys):
                n, ds, pod = observed(d, t)
                changed = pod
                for k in keys[:-1]:
                    changed = changed[k]
                changed[keys[-1]] = value
                with self.assertRaises(ValueError):
                    relay.attest_retained(d, t, pod, relay.attest_workload(d, t, n, ds))

    def test_private_stream_is_sent_only_to_retained_container_after_all_prechecks(self):
        d = fixture()
        checked = relay.pair_packages(d, private(d))
        calls = []

        def kube(*args, data=None, **_):
            self.assertEqual("exec", args[2])
            self.assertIn("edge", args)
            self.assertNotIn("PRIVATE", repr(args))
            edge = args[-4]
            if data is None:
                self.assertNotIn("-i", args)
                calls.append(("check", edge))
                return b"relay-trust-checked\n"
            self.assertIn("-i", args)
            self.assertIn("pod-" + edge, args)
            calls.append((edge, base64.b64decode(data.splitlines()[2]).decode()))
            return b"relay-trust-staged\n"

        def retained(d, t):
            return ("pod-" + t["edge"], uid(72), "containerd://" + t["edge"])

        with patch.object(relay, "read_checkpoint", side_effect=retained) as checkpoint, patch.object(relay, "kubectl", side_effect=kube):
            receipt = relay.stage(d, checked)
        self.assertEqual(calls, [("check", "node-1"), ("check", "node-2"),
                                 ("node-1", "PRIVATE for node-1"), ("node-2", "PRIVATE for node-2")])
        self.assertGreaterEqual(checkpoint.call_count, 6)
        self.assertNotIn(b"PRIVATE", relay.canonical(receipt))
        self.assertFalse(receipt["lease_published"])

    def test_no_leaf_streamed_on_preflight_failure_or_container_churn(self):
        d = fixture()
        checked = relay.pair_packages(d, private(d))
        for checkpoints, replies in (
            ([("pod-one", uid(72), "containerd://one"), ("pod-two", uid(73), "containerd://two")],
             [b"relay-trust-checked\n", b"rejected"]),
            ([("pod-one", uid(72), "containerd://one"), ("pod-two", uid(73), "containerd://two"),
              ("pod-one", uid(72), "containerd://replacement")], [b"relay-trust-checked\n"] * 2),
        ):
            with patch.object(relay, "read_checkpoint", side_effect=checkpoints), patch.object(relay, "worker_exec", side_effect=replies) as execute:
                with self.assertRaises(ValueError):
                    relay.stage(d, checked)
                self.assertTrue(all(not c.kwargs for c in execute.call_args_list))

    @unittest.skipUnless(sys.platform == "linux", "requires Linux coreutils support for durable publication")
    def test_installer_is_atomic_write_once(self):
        with tempfile.TemporaryDirectory() as root:
            destination, identity = Path(root) / "mount", Path(root) / "identity"
            destination.mkdir()
            identity.mkdir()
            for key, value in (("edge_id", "node-1"), ("slot", "b"), ("edge_group_id", "test-group")):
                (identity / key).write_text(value)
            script = relay.INSTALL.replace(relay.MOUNT, str(destination)).replace("/var/run/fugue/edge-identity", str(identity))
            epoch = destination / "origin-relay-v1" / ("2" * 64)

            def install(key, edge="node-1"):
                data = b"".join(base64.b64encode(x) + b"\n" for x in (b"root", b"leaf", key, b"verify"))
                return subprocess.run(["/bin/sh", "-ec", script, "fixture", edge, "b", "test-group", "2" * 64],
                                      input=data, capture_output=True, timeout=10)

            self.assertNotEqual(install(b"key-1", "wrong-node").returncode, 0)
            self.assertFalse(epoch.exists())
            self.assertEqual(install(b"key-1").returncode, 0)
            self.assertEqual(install(b"key-1").returncode, 0)
            self.assertNotEqual(install(b"foreign-key").returncode, 0)
            key_path = epoch / "trust" / "key.pem"
            self.assertEqual(key_path.read_bytes(), b"key-1")
            self.assertEqual({p.name for p in epoch.iterdir()}, {"trust"})
            key_path.unlink()
            external = Path(root) / "foreign"
            external.write_bytes(b"key-1")
            key_path.symlink_to(external)
            self.assertNotEqual(install(b"key-1").returncode, 0)
            self.assertEqual(external.read_bytes(), b"key-1")


if __name__ == "__main__":
    unittest.main()
