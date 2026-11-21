import copy
import datetime
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import dns_transport_failed_predecessor as failed
from scripts import reconcile_dns_transport as transport


def uid(index):
    return "00000000-0000-0000-0000-%012d" % index


class FailedPredecessorTests(unittest.TestCase):
    def setUp(self):
        public = {"address": "1.1.1.1", "name": "public", "port": 53, "targetPort": 5353, "selector": {"app": "new"}}
        self.diagnostic = dict(public, name="diagnostic", port=15353)
        self.config = {"apiVersion": "transport.fugue.dev/v4", "kind": "DNSLocalTransport", "namespace": "system",
                       "generation": 2, "rollback_generation": 3, "listeners": [self.diagnostic, public]}
        self.failure = {"node_uid": uid(1), "public_service_uid": uid(2), "diagnostic_service_uid": uid(3),
                        "pod_uid": uid(4), "candidate_pod_uid": uid(5), "release_id": "failed_release",
                        "parent_digest": "sha256:" + "1"*64, "fencing_token": 8,
                        "query": {"hostname": "example.test", "type": "A"},
                        "api": {"origin": "https://control.example.test", "api_address": "1.0.0.1"},
                        "rollback": {"artifact_id": "parent", "parent_digest": "sha256:" + "2"*64,
                                     "release_id": "restored_full", "fencing_token": 9, "lkg_release_id": "lkg",
                                     "lkg_evidence_hash": "sha256:" + "3"*64}}
        self.fallback = {"selector": {"app": "backup"}, "revision": "3"*40, "pod_uid": uid(6)}
        self.stage = {"address": public["address"], "previous_revision": "1"*40, "candidate_revision": "2"*40,
                      "recovery_fallback": self.fallback, "failed_predecessor": self.failure}
        self.config["handoffs"] = [self.stage]
        self.old = {"assignment": {"artifact_release_id": "failed_release", "fencing_token": 8},
                    "parent_digest": self.failure["parent_digest"], "plan_digest": "sha256:" + "4"*64}
        self.witness = {"old_uid": uid(4), "candidate_uid": uid(5), "fallback_uid": uid(6), "node_uid": uid(1),
                        "assignment": {"fencing_token": 9}, "parent_digest": self.failure["rollback"]["parent_digest"],
                        "plan_digest": "sha256:" + "5"*64, "failed_predecessor": self.old}
        self.operations = []
        for listener, identity in ((self.diagnostic, uid(3)), (public, uid(2))):
            previous = transport.service(dict(self.config, generation=1), dict(listener, selector={"app": "old"}))
            previous["metadata"].update(uid=identity, resourceVersion="7")
            self.operations.append((["patch", listener["name"]], "{}", listener, previous, self.witness))
        self.public = public
        now = datetime.datetime.now(datetime.timezone.utc)
        sample = dict(self.failure["query"], response_code=2, policy_digests=["sha256:" + "6"*64],
                      started_at=(now - datetime.timedelta(seconds=1)).isoformat(), finished_at=now.isoformat())
        self.report = dict(self.old, schema="fugue.dns-unready-predecessor-observation/v1", authorizes_traffic=False,
                           node_uid=uid(1), service_uid=uid(2), pod_uid=uid(4), service_resource_version="7",
                           samples=[dict(sample, transport=protocol) for protocol in ("udp", "tcp")])

    def test_declaration_requires_three_exact_backends_and_both_service_identities(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "intent.json"
            path.write_text(json.dumps(self.config))
            self.assertEqual(transport.load_config(path), self.config)
            for change in ({"pod_uid": uid(5)}, {"candidate_pod_uid": uid(6)}, {"diagnostic_service_uid": uid(2)},
                           {"query": {"hostname": "other.example.test", "type": "TXT"}}, {"rollback": {}},
                           {"api": {"origin": "http://control.example.test", "api_address": "1.0.0.1"}},
                           {"fencing_token": True}):
                with self.subTest(change=change), self.assertRaises(ValueError):
                    broken = copy.deepcopy(self.config)
                    broken["handoffs"][0]["failed_predecessor"].update(change)
                    path.write_text(json.dumps(broken))
                    transport.load_config(path)
            missing = dict(self.stage)
            del missing["recovery_fallback"]
            with self.assertRaisesRegex(ValueError, "independent fallback"):
                failed.validate_stage(missing)

    def test_witness_binds_current_socket_old_signed_assignment_and_both_ready_backends(self):
        owner = {"uid": uid(1), "name": "node"}
        current = self.operations[1][3]
        old = {"pod": {"metadata": {"uid": uid(4)}}}
        candidate = {"pod": {"metadata": {"uid": uid(5)}}, "snapshot": {"name": "next"}}
        fallback = {"pod": {"metadata": {"uid": uid(6)}}, "snapshot": {"name": "fallback"}}
        with patch("scripts.inspect_dns_predecessor.verify_unready_backend", return_value=self.old) as verify, \
             patch.object(transport, "validate_handoff_snapshots") as same, \
             patch.object(failed, "require_current_rollback") as authority:
            observe = lambda: failed.witness(self.config, self.stage, self.public, current, owner, old, candidate, fallback)
            self.assertEqual(observe(), self.old)
            same.assert_called_once_with(candidate["snapshot"], fallback["snapshot"], "node")
            authority.assert_called_once_with(candidate["snapshot"], self.failure)
            verify.return_value = dict(self.old, assignment={"artifact_release_id": "moved", "fencing_token": 10})
            with self.assertRaisesRegex(ValueError, "reviewed signed assignment"):
                observe()
            current["metadata"]["uid"] = uid(7)
            with self.assertRaisesRegex(ValueError, "Service, socket or node"):
                observe()

    def test_negative_evidence_is_fresh_exact_and_not_traffic_authority(self):
        current = self.operations[1][3]
        failed.validate_failure_observation(self.report, self.stage, current, self.witness)
        for changes in ({"service_resource_version": "8"}, {"pod_uid": uid(9)}, {"authorizes_traffic": True},
                        {"parent_digest": "changed"}, {"samples": self.report["samples"][:1]}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                failed.validate_failure_observation(dict(self.report, **changes), self.stage, current, self.witness)
        for changes in ({"response_code": -1}, {"response_code": 0}, {"started_at": "2020-01-01T00:00:00Z"},
                        {"finished_at": "2090-01-01T00:00:00Z"}, {"hostname": "unrelated.example.test"},
                        {"transport": "tcp"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                broken = copy.deepcopy(self.report)
                broken["samples"][0].update(changes)
                failed.validate_failure_observation(broken, self.stage, current, self.witness)

    def test_public_cas_requires_failure_and_only_compensates_to_independent_fallback(self):
        for scenario in ("success", "unknown_failure", "fallback_unready", "diagnostic_failed", "candidate_failed"):
            with self.subTest(scenario=scenario):
                events = []
                evidence = {"applied": []}

                def verify_stage(config, stage, binding, port):
                    events.append(("observed", port, binding["candidate_uid"]))
                    if scenario == "candidate_failed" and port == 53 and binding["candidate_uid"] == uid(5):
                        raise transport.PublicDNSVerificationFailed([])
                    if scenario == "diagnostic_failed" and port == 15353 and binding["candidate_uid"] == uid(5):
                        raise ValueError("diagnostic candidate cannot serve")
                    return {"verified_backend": dict(self.witness, candidate_uid=binding["candidate_uid"])}

                def verify_fallback(config, stage, listener, current, witness):
                    events.append(("fallback", listener["port"]))
                    if scenario == "fallback_unready":
                        raise ValueError("fallback cannot serve")
                    return dict(self.witness, candidate_uid=uid(6))

                def verify_failure(*args):
                    events.append(("failed", 53))
                    if scenario == "unknown_failure":
                        raise ValueError("DNS wire timeout is not SERVFAIL")
                    return self.report

                def restore(config, operation, witness, acknowledged, **kwargs):
                    events.append(("restore", operation[2]["port"]))
                    self.assertIs(kwargs["verify_fallback"], verify_fallback)
                    return transport.recovery_listener_config(config, operation[2], self.stage)[0]

                def execute():
                    transport.apply_transport_steps(self.config, self.operations, evidence, lambda: None, verify_stage,
                                                    verify_fallback, verify_failure)

                with patch.object(transport, "kubectl", side_effect=lambda *args, **kwargs: events.append(("write", args[1]))), \
                     patch.object(transport, "revalidate_handoff_witness", return_value=self.witness), \
                     patch.object(transport, "restore_staged_listener", side_effect=restore):
                    if scenario == "success":
                        execute()
                        self.assertEqual(events, [("fallback", 15353), ("write", "diagnostic"), ("observed", 15353, uid(5)),
                                                  ("fallback", 53), ("failed", 53), ("write", "public"), ("observed", 53, uid(5))])
                    else:
                        with self.assertRaises((ValueError, transport.PublicDNSVerificationFailed)):
                            execute()
                if scenario in ("unknown_failure", "fallback_unready", "diagnostic_failed"):
                    self.assertNotIn(("write", "public"), events)
                if scenario == "diagnostic_failed":
                    self.assertEqual(events[-2:], [("restore", 15353), ("observed", 15353, uid(6))])
                if scenario == "candidate_failed":
                    self.assertEqual(events[-2:], [("restore", 53), ("observed", 53, uid(6))])
                self.assertFalse(any(event[:1] == ("observed",) and event[-1] == uid(4) for event in events))

    def test_revalidation_does_not_adopt_an_assignment_change(self):
        moved = dict(self.witness, failed_predecessor=dict(self.old, plan_digest="changed"))
        with patch.object(transport, "handoff_witness", return_value=moved), self.assertRaisesRegex(ValueError, "changed"):
            transport.revalidate_handoff_witness(self.config, self.public, self.operations[1][3], self.witness)
