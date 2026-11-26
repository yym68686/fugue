import copy
import hashlib
import json
import unittest
from pathlib import Path

from scripts import replace_idle_worker_slot as replacement


class IdleWorkerSlotTest(unittest.TestCase):
    def setUp(self):
        directory = Path(__file__).resolve().parents[1] / "deploy/releases/edge-worker-us-primary-a"
        self.config = replacement.validate(json.loads((directory / "idle-slot.json").read_text()))
        self.resources = json.loads((directory / "resources.json").read_text())
        self.predecessor = copy.deepcopy(self.resources["items"][0])
        old = self.config["old"]
        self.predecessor["metadata"].update(name=old["controller"], uid=old["controllerUID"], generation=old["generation"],
                                            resourceVersion="42")
        template = self.predecessor["spec"]["template"]
        template["spec"].pop("affinity")
        old["templateDigest"] = replacement.kube.digest(template)

    def test_node_exclusion_only_changes_the_declared_node(self):
        patch = replacement.parked_patch(self.config, self.predecessor)
        self.assertEqual([p["op"] for p in patch], ["test", "test", "test", "add", "add"])
        self.assertEqual(patch[-1]["value"], replacement.parking.node_affinity(self.config["node"], "NotIn"))
        parked = copy.deepcopy(self.predecessor)
        parked["metadata"]["annotations"] = patch[-2]["value"]
        parked["metadata"]["generation"] += 1
        parked["spec"]["template"]["spec"]["affinity"] = patch[-1]["value"]
        original = copy.deepcopy(parked)
        self.assertEqual(replacement.parked_patch(self.config, parked), [])
        self.assertEqual(original, parked)
        parked["spec"]["template"]["spec"]["containers"][0]["image"] = "other"
        with self.assertRaisesRegex(ValueError, "template changed"):
            replacement.parked_patch(self.config, parked)

    def test_candidate_cannot_add_a_workload_or_use_a_third_slot(self):
        replacement.ready_scope(self.config, self.resources)
        other = copy.deepcopy(self.resources)
        other["items"].append(copy.deepcopy(other["items"][0]))
        with self.assertRaisesRegex(ValueError, "expected one object"):
            replacement.ready_scope(self.config, other)
        other = copy.deepcopy(self.resources)
        other["items"][0]["spec"]["template"]["spec"]["containers"][1]["ports"][1]["hostPort"] = 38443
        with self.assertRaisesRegex(ValueError, "freed single-node A slot"):
            replacement.ready_scope(self.config, other)

    def test_socket_inventory_counts_in_flight_and_draining_connections(self):
        header = "sl local_address rem_address st\n"
        for state in ("01", "02", "05", "08"):
            with self.assertRaisesRegex(ValueError, "established or in-flight"):
                replacement.require_empty_tcp(header + f"1: 00000000:480B 00000000:0000 {state}\n" + header)
        replacement.require_empty_tcp(header + "1: 00000000:480B 00000000:0000 0A\n"
                                      + header + "1: 00000000:480B 00000000:0000 06\n")
        with self.assertRaisesRegex(ValueError, "IPv4 and IPv6"):
            replacement.require_empty_tcp(header)

    def test_empty_endpoint_slice_does_not_hide_other_slices(self):
        empty = [{}, {"endpoints": None}, {"endpoints": []}]
        self.assertFalse(replacement.endpoint_targets_worker(empty, "worker-uid", "10.0.0.1"))
        for endpoint in ({"targetRef": {"uid": "worker-uid"}, "addresses": None}, {"addresses": ["10.0.0.1"]}):
            self.assertTrue(replacement.endpoint_targets_worker(empty + [{"endpoints": [endpoint]}], "worker-uid", "10.0.0.1"))

    def test_existing_retired_controller_requires_receipt_exclusion_and_zero_schedule(self):
        config = self.config
        node = config["node"]
        config["retirement"] = {"journalDigest": "sha256:" + "a" * 64,
                                "receiptUID": "00000000-0000-0000-0000-000000000042"}
        replacement.validate(config)
        old = config["old"]
        ds = copy.deepcopy(self.predecessor)
        ds["metadata"]["namespace"] = config["namespace"]
        template = ds["spec"]["template"]
        template["spec"]["affinity"] = replacement.parking.node_affinity(node, "In")
        term = template["spec"]["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"][0]
        term["matchFields"].append({"key": "metadata.name", "operator": "NotIn", "values": [node]})
        old["templateDigest"] = replacement.kube.digest(template)
        ds["status"] = {"observedGeneration": old["generation"], "desiredNumberScheduled": 0, "currentNumberScheduled": 0}
        decision = {"plan": {"schema": "edge-node-worker-retirement/v1", "journal_digest": config["retirement"]["journalDigest"],
                             "daemonset": old["controller"], "daemonset_uid": old["controllerUID"],
                             "generation": old["generation"] - 1},
                    "node": node, "worker_uid": old["podUID"], "outcome": "deadline"}
        raw = replacement.front.canonical(decision)
        key = "fugue.pro/worker-retired-" + hashlib.sha256(node.encode()).hexdigest()[:20]
        ds["metadata"]["annotations"][key] = raw
        receipt = {"metadata": {"name": "fugue-node-worker-retired-" + "a" * 32,
                                 "namespace": config["namespace"], "uid": config["retirement"]["receiptUID"],
                                 "resourceVersion": "42", "labels": {"app.kubernetes.io/managed-by": "fugue-release-guardian",
                                     "fugue.pro/authority-kind": "node-worker-retired", "fugue.pro/group": config["group"]}},
                   "immutable": True, "data": {"retired.json": raw}}
        replacement.require_retired(config, ds, receipt)
        for field, value in (("observedGeneration", old["generation"] - 1), ("desiredNumberScheduled", 1), ("currentNumberScheduled", 1)):
            changed = copy.deepcopy(ds)
            changed["status"][field] = value
            with self.assertRaisesRegex(ValueError, "exact single-node exclusion"):
                replacement.require_retired(config, changed, receipt)
        changed = copy.deepcopy(ds)
        changed["metadata"]["annotations"][key] = "{}"
        with self.assertRaisesRegex(ValueError, "does not bind"):
            replacement.require_retired(config, changed, receipt)
        with self.assertRaisesRegex(ValueError, "immutable Guardian receipt"):
            replacement.require_retired(config, ds, dict(receipt, immutable=False))
        term["matchFields"].pop()
        with self.assertRaisesRegex(ValueError, "exclusion changed"):
            replacement.require_retired(config, ds, receipt)

        # The same proof may release a different fixed slot on one node
        # without requiring its shared former controller to have no peers.
        peer = next(iter(config["peers"]))
        config["slot"] = "b"
        config["retirement"]["controllerPeers"] = {peer: config["peers"][peer]}
        replacement.validate(config)
        template["metadata"]["labels"]["fugue.io/edge-slot"] = "b"
        spec = template["spec"]
        spec.pop("nodeSelector")
        spec["affinity"] = replacement.parking.node_affinity(node, "NotIn")
        for container in spec["containers"]:
            for port in container.get("ports", []):
                if port.get("hostPort") in replacement.PORTS[0]:
                    index = replacement.PORTS[0].index(port["hostPort"])
                    port["hostPort"] = replacement.PORTS[1][index]
        old["templateDigest"] = replacement.kube.digest(template)
        ds["status"].update(desiredNumberScheduled=1, currentNumberScheduled=1, numberReady=1)
        replacement.require_retired(config, ds, receipt)
        config["retirement"]["controllerPeers"] = {}
        with self.assertRaisesRegex(ValueError, "exact single-node exclusion"):
            replacement.require_retired(config, ds, receipt)
