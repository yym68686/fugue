from contextlib import ExitStack
from datetime import datetime
import unittest
from unittest.mock import Mock, patch

from scripts import verify_observation_worker_recovery as recovery


WORKER = dict(deployment="observation", container="observation-worker",
              image="registry.test/worker@sha256:" + "a" * 64, revision="b" * 40, replicas=1)
CUTOFF = "2026-11-28T11:12:28+00:00"
BEFORE = dict(owner="worker", fence=True, transferAt="2026-11-26T10:00:00+00:00",
              completedAt="2026-11-28T11:13:00+00:00", completedPasses=7)
AFTER = dict(BEFORE, completedAt="2026-11-28T11:14:00+00:00", completedPasses=8)
LIVE = ("deployment", 2, [("observation", "new-pod")])


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.provision = Mock()
        self.provision.find_primary.return_value = ("primary", "uid")
        methods = ExitStack()
        self.addCleanup(methods.close)
        self.live = methods.enter_context(patch.object(recovery, "verify_workload", return_value=LIVE))
        self.rollback = methods.enter_context(patch.object(recovery, "checked_rollback", return_value=("previous", "image")))
        self.policy = methods.enter_context(patch.object(recovery, "verify_policy", return_value=("policy", "revision")))
        self.database = methods.enter_context(patch.object(
            recovery, "observe_database", return_value={"owner": "worker", "watermark": CUTOFF}))
        self.rollup = methods.enter_context(patch.object(
            recovery, "verify_committed_progress", return_value={"podUID": "new-pod"}))
        self.samples = methods.enter_context(patch.object(recovery, "sample_progress"))
        self.samples.side_effect = [{target: BEFORE for target in recovery_targets()},
                                    {target: AFTER for target in recovery_targets()}]
        self.evidence = methods.enter_context(patch.object(recovery, "evidence_progress", side_effect=[BEFORE, AFTER]))
        self.collector = methods.enter_context(patch.object(recovery, "verify_collector"))

    def verify(self):
        return recovery.verify_recovery(self.provision, WORKER, datetime.fromisoformat(CUTOFF), {}, {},
                                        retention_wait_seconds=0)

    def test_requires_fresh_progress_and_receipts(self):
        result = self.verify()
        self.assertIs(result["ownershipChanged"], False)
        self.assertEqual(result["qualityEvidence"]["after"]["completedPasses"], 8)
        self.assertEqual(self.collector.call_count, 3)
        self.assertEqual(self.rollback.call_count, 2)

    def test_fails_if_a_retention_lane_stalls(self):
        self.evidence.side_effect = [BEFORE, BEFORE]
        with self.assertRaisesRegex(recovery.ProvisionError, "not all retention lanes advanced"):
            self.verify()

    def test_fails_if_owner_changes(self):
        self.evidence.side_effect = [BEFORE, dict(AFTER, owner="api")]
        with self.assertRaisesRegex(recovery.ProvisionError, "ownership or retention fence changed"):
            self.verify()

    def test_fails_if_exact_pod_or_rollback_changes(self):
        self.rollback.side_effect = [("previous", "image"), ("changed", "image")]
        with self.assertRaisesRegex(recovery.ProvisionError, "rollback changed"):
            self.verify()

    def test_rejects_unpinned_image(self):
        with self.assertRaisesRegex(ValueError, "worker identity"):
            recovery.parse_request({"worker": dict(WORKER, image="registry.test/worker:latest"), "notBefore": CUTOFF})


def recovery_targets():
    return ("network", "performance", "resource-usage")
