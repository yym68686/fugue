import copy
import json
from pathlib import Path
from subprocess import TimeoutExpired
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from scripts import run_node_worker_handoff as driver
from scripts import edge_node_participation as participation
from scripts.test_node_worker_handoff import Fixture


class PeerServingIdentityTest(unittest.TestCase):
    def test_peer_can_have_only_its_exact_serving_slot(self):
        cluster = driver.ActiveCluster({"namespace": "sample", "node": "pilot", "group": "group"})
        image = "sample.test/worker@sha256:" + "a" * 64
        identity = {"active_slot": "a", "worker_source_commit": "b" * 40, "worker_image_digest": image.split("@")[1]}
        pod = {"metadata": {"uid": "active", "name": "worker", "labels": {"fugue.io/edge-slot": "a"},
                            "annotations": {"fugue.pro/source-commit": identity["worker_source_commit"]}},
               "spec": {"containers": [{"name": "edge", "image": image},
                                       {"name": "caddy", "image": image, "ports": [{"hostPort": 18443}]}]},
               "status": {"phase": "Running", "containerStatuses": [
                   {"name": name, "imageID": image, "containerID": name, "ready": True, "restartCount": 0,
                    "state": {"running": {"startedAt": "2026-11-01T00:00:00Z"}}}
                   for name in ["edge", "caddy"]]}}
        self.assertEqual(driver.peer_selected_worker(cluster, [pod], identity), "active")
        with self.assertRaisesRegex(ValueError, "host port"):
            driver.peer_selected_worker(cluster, [], identity)
        with self.assertRaisesRegex(ValueError, "pinned ready release"):
            driver.peer_selected_worker(cluster, [pod], dict(identity, worker_source_commit="c" * 40))

    def test_snapshots_legacy_peer_readyz_without_streaming_exec(self):
        pod = {"metadata": {"name": "peer-front"}}
        health = {"status": "ok", "route_authority": "edge-control", "active_slot": "a",
                  "activation_generation": 913, "bundle_generation": "bundle.p1.r2",
                  "worker_source_commit": "e" * 40, "worker_image_digest": "sha256:" + "a" * 64}
        with patch.object(driver.front, "read", return_value=health) as read:
            self.assertEqual(driver.peer_serving_identity("fugue-system", pod),
                             {k: v for k, v in health.items() if k != "status"})
            read.assert_called_once_with("get", "--raw", "/api/v1/namespaces/fugue-system/pods/peer-front:7831/proxy/readyz")
        for key, invalid in [("status", "degraded"), ("activation_generation", True), ("worker_image_digest", "unknown")]:
            with self.subTest(key=key), patch.object(driver.front, "read", return_value=dict(health, **{key: invalid})):
                with self.assertRaisesRegex(ValueError, "peer-front"):
                    driver.peer_serving_identity("fugue-system", pod)


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
    def test_discovered_peer_must_prove_exclusion_before_omitting_live_checks(self):
        config = pinned()
        config.pop("peers")
        driver.validate(config)
        cluster = driver.ActiveCluster(config)
        physical = {"metadata": {"name": "peer-node", "uid": "peer-uid"},
                    "status": {"conditions": [{"type": "Ready", "status": "Unknown"}]}}
        member = {"node": "peer-node", "node_uid": "peer-uid", "group": config["group"], "reason": "network_unavailable"}
        policy = {"namespace": config["namespace"], "members": [member]}
        cluster.participation_policy = policy
        selector = {"fugue.io/edge-node": "peer-node"}
        spec = {"type": "ClusterIP", "selector": selector, "externalIPs": ["9.9.9.9"], "externalTrafficPolicy": "Local",
                "ports": [{"name": "http", "port": 80, "targetPort": 80, "protocol": "TCP"},
                          {"name": "https", "port": 443, "targetPort": 443, "protocol": "TCP"}]}
        svc = {"metadata": {"name": "peer-service", "uid": "service-peer", "annotations": {
            "transport.fugue.dev/phase": "serving", driver.stage.GENERATION: "1",
            driver.stage.DIGEST: driver.kube.digest(driver.replacement.projection(spec))}},
               "spec": spec}
        edge = {"public_ipv4": "9.9.9.9", "observation": {"reason": "heartbeat_stale", "heartbeat_fresh": False,
                "dns_admission_intent": {"state": "suspended", "serving_evidence": "excluded_from_verified_full"}}}
        endpoints = {"items": [{"metadata": {"uid": "slice", "ownerReferences": [
            {"kind": "Service", "uid": "service-peer", "controller": True}]},
            "endpoints": [{"nodeName": "peer-node", "conditions": {"ready": False, "serving": False}}]}]}
        roster = {config["node"]: {}, "peer-node": physical}
        with patch.object(cluster, "roster", return_value=roster), patch.object(participation, "serving_services", return_value=[svc]), \
                patch.object(cluster, "read_service", return_value=svc), patch.object(participation, "read_edge_node", return_value=edge), \
                patch.object(driver.front, "read", return_value=endpoints), \
                patch.object(cluster, "service_front", side_effect=ValueError("live Front proof required")) as live:
            self.assertEqual(cluster.peer_snapshot()["peer-node"]["state"], "isolated")
            live.assert_not_called()
            for mutation in ("unknown_dns_selection", "missing_owner_policy", "serving_endpoint", "recovered"):
                original, state = copy.deepcopy(edge), copy.deepcopy(physical)
                try:
                    if mutation == "unknown_dns_selection":
                        edge["observation"]["dns_admission_intent"]["serving_evidence"] = "not_evaluated"
                    elif mutation == "missing_owner_policy":
                        cluster.participation_policy = None
                    elif mutation == "serving_endpoint":
                        endpoints["items"][0]["endpoints"][0]["conditions"]["serving"] = True
                    else:
                        physical["status"]["conditions"][0]["status"] = "True"
                    with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError, "live Front proof|serving or unproven"):
                        cluster.peer_snapshot()
                finally:
                    edge.update(original)
                    physical.update(state)
                    endpoints["items"][0]["endpoints"][0]["conditions"]["serving"] = False
                    cluster.participation_policy = policy

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

    def test_fixed_rotation_requires_exact_fixed_candidate_and_real_drain_witness(self):
        config = pinned()
        with self.assertRaisesRegex(ValueError, "fixed rotation"):
            driver.validate(dict(config, fixedRotation=True))
        config["candidate"]["daemonSet"] = config["candidate"].pop("deployment")
        config["probeIngress"] = {"service": "probe-front", "uid": "00000000-0000-4000-8000-000000000004",
                                  "generation": 1, "port": 15443}
        with self.assertRaisesRegex(ValueError, "drain evidence"):
            driver.validate(dict(config, fixedRotation=True))
        config.update(verifyDrain=True, fixedRotation=True)
        config["probe"]["bodyDigest"] = "sha256:" + "f"*64
        self.assertEqual(driver.validate(config), config)
        f = Fixture()
        f.journal["fixed_rotation"] = True
        f.activation["reason"] = "advance fixed Worker using journal " + f.journal["digest"]
        self.assertEqual(driver.handoff.slot_phase(f.journal, f.activation), "activated")

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
