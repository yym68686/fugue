import copy
import unittest

from scripts import retire_front_hostport as retirement
from scripts import reconcile_front_probe_transport as kube


def declaration():
    return {
        "schema": "fugue.front-hostport-retirement/v1", "namespace": "system-fixture", "group": "edge-group-fixture",
        "node": "edge-primary", "nodeUID": "00000000-0000-4000-8000-000000000001",
        "address": "8.8.8.8", "legacy": "old-front", "legacyUID": "00000000-0000-4000-8000-000000000002",
        "legacyGeneration": 7, "legacyTemplateDigest": "sha256:" + "0" * 64,
        "oldUID": "00000000-0000-4000-8000-000000000003", "isolated": "isolated-old-front",
        "frontUID": "00000000-0000-4000-8000-000000000004", "workerComponent": "edge-worker-fixture",
        "serving": {"name": "serving-front", "uid": "00000000-0000-4000-8000-000000000005", "generation": 8},
        "probe": {"name": "probe-front", "uid": "00000000-0000-4000-8000-000000000006", "generation": 4},
        "peers": {"edge-peer-a": "00000000-0000-4000-8000-000000000007"},
        "hosts": ["api.example.test", "app.example.test"],
    }


def workload():
    return {
        "metadata": {"name": "old-front", "namespace": "system-fixture", "uid": "00000000-0000-4000-8000-000000000002",
                     "resourceVersion": "17", "generation": 7, "annotations": {"helm.sh/test": "keep"}},
        "spec": {
            "selector": {"matchLabels": {"app": "front", retirement.COMPONENT: "legacy"}},
            "updateStrategy": {"type": "OnDelete"}, "revisionHistoryLimit": 3,
            "template": {
                "metadata": {"labels": {"app": "front", retirement.COMPONENT: "legacy"}, "annotations": {"runtime": "keep"}},
                "spec": {"containers": [{"name": "edge-front", "image": "example/front@sha256:" + "1" * 64,
                                         "ports": [{"name": "http", "containerPort": 80, "hostPort": 80, "protocol": "TCP"},
                                                   {"name": "https", "containerPort": 443, "hostPort": 443, "protocol": "TCP"},
                                                   {"name": "health", "containerPort": 7831, "protocol": "TCP"}]}],
                         "volumes": [{"name": "activation", "hostPath": {"path": "/var/lib/fixture", "type": "Directory"}}],
                         "serviceAccountName": "fixture", "terminationGracePeriodSeconds": 30},
            },
        },
    }


class FrontHostPortRetirementTests(unittest.TestCase):
    def fixture(self):
        c, ds = declaration(), workload()
        c["legacyTemplateDigest"] = kube.digest(ds["spec"]["template"])
        return c, ds

    def test_only_target_node_is_excluded_and_new_template_has_no_public_hostports(self):
        c, ds = self.fixture()
        patch, isolated = retirement.derive(c, ds)
        self.assertEqual([x["path"] for x in patch], [
            "/metadata/uid", "/metadata/resourceVersion", "/spec", "/metadata/annotations", "/spec/template/spec/affinity"])
        self.assertEqual(patch[-1]["value"], retirement.node_affinity(c["node"], "NotIn"))
        self.assertEqual(isolated["spec"]["updateStrategy"], {"type": "OnDelete"})
        self.assertEqual(isolated["spec"]["template"]["spec"]["nodeSelector"]["kubernetes.io/hostname"], c["node"])
        self.assertEqual(isolated["spec"]["template"]["spec"]["affinity"], retirement.node_affinity(c["node"], "In"))
        self.assertEqual(isolated["spec"]["selector"]["matchLabels"][retirement.ISOLATED], c["node"])
        self.assertEqual(isolated["spec"]["template"]["metadata"]["labels"][retirement.ISOLATED], c["node"])
        self.assertNotEqual(isolated["spec"]["selector"]["matchLabels"][retirement.COMPONENT], ds["spec"]["selector"]["matchLabels"][retirement.COMPONENT])
        self.assertEqual(isolated["spec"]["template"]["spec"]["containers"][0]["ports"], [
            {"name": "http", "containerPort": 80, "protocol": "TCP"},
            {"name": "https", "containerPort": 443, "protocol": "TCP"},
            {"name": "health", "containerPort": 7831, "protocol": "TCP"},
        ])
        self.assertEqual(ds, workload())

    def test_server_dryrun_readback_is_idempotent_and_keeps_template_digest(self):
        c, ds = self.fixture()
        _, isolated = retirement.derive(c, ds)
        live = copy.deepcopy(ds)
        live["metadata"]["generation"] = 8
        live["metadata"]["annotations"][retirement.GEN] = kube.digest(c)
        live["spec"]["template"]["spec"]["affinity"] = retirement.node_affinity(c["node"], "NotIn")
        self.assertEqual(retirement.derive(c, live), ([], isolated))

    def test_rejects_a_different_host_port_or_preexisting_node_affinity(self):
        c, ds = self.fixture()
        bad = copy.deepcopy(ds)
        bad["spec"]["template"]["spec"]["containers"][0]["ports"][0]["hostPort"] = 8080
        c["legacyTemplateDigest"] = kube.digest(bad["spec"]["template"])
        with self.assertRaisesRegex(ValueError, "exactly TCP 80/443"):
            retirement.derive(c, bad)
        c, ds = self.fixture()
        ds["spec"]["template"]["spec"]["affinity"] = {"nodeAffinity": {}}
        c["legacyTemplateDigest"] = kube.digest(ds["spec"]["template"])
        with self.assertRaisesRegex(ValueError, "generation or affinity changed"):
            retirement.derive(c, ds)

    def test_rejects_scope_expansion_and_invalid_listener_identity(self):
        c = retirement.validate(declaration())
        for mutate in (
            lambda x: x.update(node="edge-peer-a"),
            lambda x: x["serving"].update(uid=x["probe"]["uid"]),
            lambda x: x.update(hosts=["same.example", "same.example"]),
            lambda x: x.update(peers={"": "00000000-0000-4000-8000-000000000007"}),
        ):
            bad = copy.deepcopy(c)
            mutate(bad)
            with self.assertRaises(ValueError):
                retirement.validate(bad)

    def test_kernel_inventory_fails_closed_and_ignores_only_listen_and_time_wait(self):
        self.assertEqual(retirement.retirable_tcp("sl local_address rem_address st\n0: 00000000:01BB 00000000:0000 0A\n1: 00000000:0050 00000000:0000 06\n"), 0)
        self.assertEqual(retirement.retirable_tcp("sl local_address rem_address st\n0: 00000000:01BB 00000000:0000 01\n"), 1)
        with self.assertRaisesRegex(ValueError, "malformed"):
            retirement.retirable_tcp("malformed")


if __name__ == "__main__":
    unittest.main()
