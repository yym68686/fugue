import unittest
from unittest.mock import Mock, patch

from scripts import verify_hosted_dns_api_rollout as rollout
from scripts.provision_observation_worker_database import ProvisionError


class HostedDNSCleanupContinuityTest(unittest.TestCase):
    def setUp(self):
        self.provision = Mock()
        self.provision.find_primary.return_value = ("primary", "primary-uid")
        self.worker = ("worker", "worker-uid")
        self.api = [("new-api-1", "new-api-1-uid"), ("new-api-2", "new-api-2-uid")]
        self.previous = {
            "primary": ["primary", "primary-uid"], "workerPod": list(self.worker),
            "apiPods": [["old-api-1", "old-api-1-uid"], ["old-api-2", "old-api-2-uid"]],
            "owner": "worker", "transferAt": "2026-11-28T00:00:00+00:00",
            "completedAt": "2026-11-28T00:01:00+00:00", "updated": 0,
        }
        self.progress = {key: self.previous[key] for key in ("owner", "transferAt", "updated")}
        self.progress["completedAt"] = "2026-11-28T00:02:00+00:00"
        self.scraped = True

    def verify(self, previous=True):
        workloads = [("worker-deployment", 2, [self.worker]), ("api-deployment", 1239, self.api)]
        with (patch.object(rollout, "verify_workload", side_effect=[*workloads, *reversed(workloads)]),
              patch.object(rollout, "prometheus_pods", return_value=["prometheus"]),
              patch.object(rollout, "observe", return_value=self.progress),
              patch.object(rollout, "scrape_observed", return_value=self.scraped) as collector):
            result = rollout.continuity(self.provision, {"worker": {}}, {}, {},
                                        self.previous if previous else None, wait_seconds=0)
            collector.assert_called_once_with(self.provision, "prometheus", self.worker, "worker",
                                              self.progress["completedAt"])
            return result

    def test_same_worker_commits_and_replaces_all_api_pods(self):
        result = self.verify()
        self.assertEqual(result["workerPod"], self.worker)
        self.assertEqual(result["apiPods"], self.api)
        self.assertEqual(result["completedAt"], self.progress["completedAt"])

    def test_baseline_requires_a_commit_after_transfer(self):
        self.progress["completedAt"] = self.progress["transferAt"]
        with self.assertRaisesRegex(ProvisionError, "no independently observed"):
            self.verify(previous=False)

    def test_stale_progress_or_missing_independent_sample_fails(self):
        self.scraped = False
        with self.assertRaisesRegex(ProvisionError, "no independently observed"):
            self.verify()
        self.scraped = True
        self.progress["completedAt"] = self.previous["completedAt"]
        with self.assertRaisesRegex(ProvisionError, "no independently observed"):
            self.verify()

    def test_transfer_epoch_cannot_change(self):
        self.progress["transferAt"] = "2026-11-28T00:00:01+00:00"
        with self.assertRaisesRegex(ProvisionError, "writer ownership changed"):
            self.verify()

    def test_old_api_uid_or_replaced_worker_fails(self):
        self.api[0] = ("renamed-api", "old-api-1-uid")
        with self.assertRaisesRegex(ProvisionError, "preceding API Pod"):
            self.verify()
        self.api[0] = ("new-api-1", "new-api-1-uid")
        self.worker = ("worker", "replacement-worker-uid")
        with self.assertRaisesRegex(ProvisionError, "worker or database changed"):
            self.verify()


if __name__ == "__main__":
    unittest.main()
