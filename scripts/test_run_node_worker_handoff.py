import copy
import json
from pathlib import Path
from subprocess import TimeoutExpired
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from scripts import run_node_worker_handoff as driver
from scripts.test_node_worker_handoff import Fixture


def pinned():
    f = Fixture()
    f.before["schema"] = "edge-front-group-activation/v1"
    def uid(i):
        return "00000000-0000-4000-8000-%012d" % i
    def actor(name):
        return {"deployment": name, "source": "a"*40, "imageDigest": "sha256:" + "a"*64}
    return {"schema": "fugue.node-worker-handoff/v1", "namespace": "system-fixture", "group": "edge-group-fixture",
            "node": "node-pilot", "nodeUID": uid(1),
            "front": {"service": "front-public", "uid": uid(2), "generation": 3, "address": "8.8.4.4",
                      "source": "b"*40, "imageDigest": "sha256:" + "b"*64},
            "guardian": actor("guardian"), "candidate": dict(actor("worker-overlap"), component="worker-pilot",
                releaseDigest="sha256:" + "d"*64, slot="a", bundleGeneration="bundle.p45.r1"),
            "expected": f.before, "peers": [{"node": "node-peer", "service": "peer-front", "uid": uid(3)}],
            "probe": {"host": "probe.example.test", "path": "/healthz"}}


class NodeWorkerDriverTests(unittest.TestCase):
    def test_candidate_uses_guardian_repair_release_wire_fields(self):
        candidate = pinned()["candidate"]
        self.assertEqual(driver.candidate_wire(candidate), {
            "Slot": "a", "BundleGeneration": "bundle.p45.r1",
            "WorkerSourceCommit": "a"*40, "WorkerImageDigest": "sha256:" + "a"*64,
        })

    def test_declaration_allows_no_generic_command_or_unpinned_candidate(self):
        config = pinned()
        self.assertEqual(driver.validate(config), config)
        for key, value in (("nodeUID", "other"), ("probe", {"host": "ok.example.test", "path": "/\r\nHeader"}),
                           ("peers", [{"node": config["node"], "service": "peer-front", "uid": config["nodeUID"]}]),
                           ("front", dict(config["front"], address="127.0.0.1")),
                           ("guardian", dict(config["guardian"], command="anything")),
                           ("candidate", dict(config["candidate"], slot="b")),
                           ("candidate", dict(config["candidate"], imageDigest="latest"))):
            with self.subTest(key=key), self.assertRaises(ValueError):
                driver.validate(dict(config, **{key: value}))

    def test_immutable_input_readback_does_not_repeat_a_lost_write(self):
        adapter = driver.ClaimedHandoff(unittest.mock.Mock(), {}, {"recipient": {"group": "edge-group-fixture"},
                                                                  "digest": "sha256:" + "b"*64})
        adapter.cluster.ns = "system-fixture"
        target = {}

        def lose(*_, body):
            target.update(json.loads(body))
            target["metadata"].update(uid="created", resourceVersion="1")
            raise TimeoutExpired("fixture", 1)

        adapter.cluster.get_config.side_effect = lambda name: target if name == target["metadata"]["name"] else None
        with patch.object(driver.kube, "kubectl", side_effect=lose) as kubectl:
            adapter.arm({"held": "old"})
            kubectl.assert_called_once()
        adapter.cluster.get_config.side_effect = None
        adapter.cluster.get_config.return_value = dict(target, data={"arm.json": "{}"})
        with patch.object(driver.kube, "kubectl", side_effect=TimeoutExpired("fixture", 1)) as kubectl:
            with self.assertRaisesRegex(ValueError, "another input"):
                adapter.arm({"held": "old"})
            kubectl.assert_called_once()

    def test_lost_claim_is_left_unarmed_and_peer_change_fails_real_receipt(self):
        for result in ("lost_claim", "peer_moved", "ok"):
            with self.subTest(result=result), TemporaryDirectory() as td:
                config, f = pinned(), Fixture()
                evidence = Path(td) / "receipt.json"
                unsigned = {k: v for k, v in f.journal.items() if k not in ("digest", "created_at")}
                baseline = {"peer": {"process": "same"}}
                different = {"peer": {"process": "new"}}
                with patch.object(driver, "ActiveCluster") as cluster_class, patch.object(
                        driver, "ClaimedHandoff") as adapter_class, patch.object(driver.handoff, "handoff") as exercise:
                    cluster = cluster_class.return_value
                    cluster.exact_actors.return_value = ({"name": "front"}, {"name": "guardian"}, copy.deepcopy(unsigned))
                    cluster.peer_snapshot.side_effect = [baseline, baseline, different if result == "peer_moved" else baseline, baseline]

                    def execute(_, subcommand, raw):
                        if subcommand == "node-worker-seal":
                            payload = json.loads(raw)
                            payload["digest"] = f.journal["digest"]
                            return json.dumps(payload).encode()
                        if result == "lost_claim":
                            raise TimeoutExpired("fixture", 1)
                        return b"{}"

                    cluster.exec_guardian.side_effect = execute
                    exercise.return_value = {"outcome": "verified"}
                    if result == "ok":
                        driver.run(config, evidence)
                    else:
                        with self.assertRaises(ValueError):
                            driver.run(config, evidence)
                    actual = json.loads(evidence.read_text())
                    self.assertEqual(actual["peers_unchanged"], result != "peer_moved")
                    if result == "lost_claim":
                        exercise.assert_not_called()
                        adapter_class.return_value.require_claim.assert_called_once()
                        self.assertNotIn("guardian_result", actual)
                    else:
                        exercise.assert_called_once()

    def test_actions_mutations_require_an_explicit_declaration_and_common_concurrency(self):
        workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml").read_text()
        plan, body = workflow.split("\n  node_worker_handoff_plan:", 1)[1].split("\n  node_worker_handoff:", 1)
        self.assertIn("git diff --name-only", plan)
        self.assertIn("test ! -L", plan)
        self.assertIn("needs.node_worker_handoff_plan.outputs.config != ''", body)
        self.assertIn("git diff --exit-code HEAD origin/main", body)
        for name in ("node_worker_handoff", "front_replacement", "front_replacement_recovery", "front_serving_handoff", "front_public_recovery"):
            job = workflow.split("\n  " + name + ":\n", 1)[1].split("\n    steps:", 1)[0]
            self.assertIn("group: fugue-production-front-executor-replacement", job)
