import unittest
from unittest.mock import Mock, patch

from scripts import verify_observation_sample_retention_api_rollout as rollout
from scripts.apply_observation_sample_retention_handoff import TARGETS
from scripts.provision_observation_worker_database import ProvisionError


class SampleRetentionContinuityTest(unittest.TestCase):
    def setUp(self):
        self.provision = Mock()
        self.provision.find_primary.return_value = ("primary", "primary-uid")
        self.worker = [("worker", "worker-uid")]
        self.api = [("new-api-1", "new-api-1-uid"), ("new-api-2", "new-api-2-uid")]
        self.policy = ("policy-uid", "version")
        self.row = {"owner": "worker", "fence": True, "transferAt": "2026-11-28T00:00:00+00:00",
                    "completedPasses": 1, "completedAt": "2026-11-28T00:01:00+00:00"}
        self.previous = {
            "primary": ["primary", "primary-uid"], "policy": list(self.policy),
            "worker": [["worker", "worker-uid"]], "api": [["old-api", "old-api-uid"]],
            "progress": {target: self.row.copy() for target in TARGETS},
        }
        self.progress = {
            target: {**self.row, "completedPasses": 2, "completedAt": "2026-11-28T00:02:00+00:00"}
            for target in TARGETS
        }

    def verify(self):
        workloads = [("worker-deployment", 1, self.worker), ("api-deployment", 2, self.api)] * 2
        with (patch.object(rollout, "verify_policy", return_value=self.policy),
              patch.object(rollout, "verify_workload", side_effect=workloads),
              patch.object(rollout, "read_progress", return_value=self.progress),
              patch.object(rollout, "verify_collector") as collector):
            result = rollout.continuity(self.provision, {}, {}, {}, {}, self.previous, wait_seconds=0)
            collector.assert_called_once_with(self.provision, {}, ["worker"], rollout.COLLECTOR_EXPRESSION)
            return result

    def test_each_target_advances_with_same_worker(self):
        self.assertEqual(self.verify()["progress"], self.progress)

    def test_one_lagging_target_blocks_completion(self):
        self.progress["performance"] = self.row.copy()
        with self.assertRaisesRegex(ProvisionError, "not all sample retention targets"):
            self.verify()

    def test_reset_transfer_timestamp_blocks_completion(self):
        self.progress["network"]["transferAt"] = "2026-11-28T00:01:00+00:00"
        with self.assertRaisesRegex(ProvisionError, "not all sample retention targets"):
            self.verify()

    def test_missing_fence_and_serving_predecessor_both_stop(self):
        self.progress["resource-usage"]["fence"] = False
        with self.assertRaisesRegex(ProvisionError, "fence changed"):
            self.verify()
        self.api[0] = tuple(self.previous["api"][0])
        with self.assertRaisesRegex(ProvisionError, "preceding API pod"):
            self.verify()


if __name__ == "__main__":
    unittest.main()
