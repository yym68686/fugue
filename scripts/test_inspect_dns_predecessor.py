import copy
import unittest
from contextlib import ExitStack
from unittest.mock import patch

from scripts import inspect_dns_predecessor as inspect


class DNSPredecessorInspectionTests(unittest.TestCase):
    def test_inspection_fences_the_observation_on_current_identity_and_assignment(self):
        address, hostname = "192.0.2.1", "example.test"
        query = {"hostname": hostname, "type": "A", "minimum_answers": 1, "answer_source": "verified_serving_policy"}
        listener = {"name": "public", "port": 53, "targetPort": 53, "address": address, "selector": {"app": "old"}}
        config = {"listeners": [listener], "namespace": "system", "generation": 1}
        current = inspect.transport.service(config, listener)
        current["metadata"].update(uid="service", resourceVersion="1")
        owner = {"metadata": {"name": "node", "uid": "node"}}
        pod = {"metadata": {"uid": "old"}, "spec": {"containers": [
            {"image": "registry.example/dns@sha256:" + "a" * 64,
             "ports": [{"containerPort": 53, "protocol": protocol} for protocol in ("UDP", "TCP")]}
        ]}}
        snapshot = {"assignment": {"fencing_token": 8}, "parent_digest": "parent", "plan_digest": "plan"}
        policy = {"assignment": snapshot["assignment"], "parent_digest": "parent",
                  "records": [{"hostname": hostname, "type": "A", "allowed_answers": ["192.0.2.10"]}]}
        expectations = {"servers": [{"server": address, "pod_uid": "old", "service_uid": "service", "policy": policy}]}
        for scenario in ("exact", "moved", "new release", "service changed", "policy mismatched"):
            with self.subTest(scenario=scenario), ExitStack() as stack:
                backend = {"pod": pod}
                captured = copy.deepcopy(expectations)
                if scenario == "policy mismatched":
                    captured["servers"][0]["policy"]["records"][0]["hostname"] = "other.example.test"
                stack.enter_context(patch.object(inspect.transport, "read_observation_declaration",
                                                 return_value={"servers": [address], "queries": [query]}))
                calls = stack.enter_context(patch.object(
                    inspect.transport, "read_json", side_effect=lambda *args: current if args[1] == "service" else {"items": []}))
                stack.enter_context(patch.object(inspect.transport, "validate_handoff_endpoints"))
                stack.enter_context(patch.object(inspect.transport, "handoff_backend", return_value=backend))
                node_change = {"metadata": {"uid": "other"}}
                stack.enter_context(patch.object(
                    inspect.transport, "listener_node", side_effect=[owner, owner, node_change if scenario == "moved" else owner]))
                service_change = ValueError("service changed")
                stack.enter_context(patch.object(
                    inspect, "unchanged_backend", side_effect=[None, service_change if scenario == "service changed" else None]))
                stack.enter_context(patch.object(inspect, "capture", return_value=captured))
                result = {"expectations": captured}
                stack.enter_context(patch.object(inspect.transport, "observe_dns_socket", return_value=(1, result)))
                stack.enter_context(patch.object(inspect, "observe_failure_samples", return_value=[]))
                observations = iter([snapshot, dict(snapshot, assignment={"fencing_token": 9}) if scenario == "new release" else snapshot])
                stack.enter_context(patch.object(
                    inspect.recovery, "verify_unready_predecessor",
                    side_effect=lambda *args: backend.update(snapshot=next(observations))))
                writes = stack.enter_context(patch.object(inspect.transport, "kubectl"))
                params = (config, "public", None, hostname, "A", "/verifier", "/observer")
                if scenario == "exact":
                    evidence = inspect.inspect(*params)
                    self.assertFalse(evidence["authorizes_traffic"])
                    self.assertEqual(evidence["pod_uid"], "old")
                    self.assertEqual(evidence["assignment"], snapshot["assignment"])
                    self.assertEqual(calls.call_count, 3)
                else:
                    with self.assertRaises(ValueError):
                        inspect.inspect(*params)
                writes.assert_not_called()

    def test_only_paired_authoritative_servfail_is_verified(self):
        query = {"hostname": "example.test", "type": "A"}
        address = "192.0.2.1"
        sample = dict(query, source="authoritative", server=address, authoritative=True, response_code=2,
                      reason="non_authoritative_or_failed_answer", passed=False, started_at="start",
                      finished_at="finish", expectation_source="verified_serving_policy", policy_digests=["sha256:synthetic"])
        observed = {"authorizes_traffic": False, "passed": False,
                    "observations": [dict(sample, transport="udp"), dict(sample, transport="tcp")]}
        self.assertEqual({item["transport"] for item in inspect.observe_failure_samples(observed, address, query)},
                         {"udp", "tcp"})
        for change in (
            {"transport": "tcp"},
            {"source": "recursive_sample"},
            {"response_code": 3},
            {"authoritative": False},
            {"hostname": "other.example.test"},
            {"type": "AAAA"},
            {"server": "192.0.2.2"},
            {"reason": "transport_unavailable"},
            {"reason": "verified_expectation_expired"},
            {"reason": "response_identity_or_completeness"},
            {"policy_digests": []},
            {"passed": True},
        ):
            foreign = copy.deepcopy(observed)
            foreign["observations"][0].update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                inspect.observe_failure_samples(foreign, address, query)
        with self.assertRaises(ValueError):
            inspect.observe_failure_samples(dict(observed, authorizes_traffic=True), address, query)
