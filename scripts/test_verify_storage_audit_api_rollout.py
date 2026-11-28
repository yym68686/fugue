import unittest
from unittest.mock import Mock, patch

from scripts import verify_storage_audit_api_rollout as rollout
from scripts.provision_observation_worker_database import ProvisionError


class StorageAuditContinuityTest(unittest.TestCase):
    def setUp(self):
        self.provision = Mock(config={"secret": "worker-database"})
        self.provision.find_primary.return_value = ("primary", "primary-uid")
        self.provision.credential.return_value = ("secret-uid", None)
        self.worker = ("new-worker", "worker-uid")
        self.api = [("new-api", "new-api-uid"), ("new-api-2", "new-api-uid-2")]
        self.before = {"workerPod": list(self.worker), "primary": ["primary", "primary-uid"],
                       "apiPods": [["old-api", "old-api-uid"]], "completedPasses": 3,
                       "sampleAt": "2026-11-28T00:00:00+00:00"}
        self.state = {"owner": "worker", "completedPasses": 4, "failed": False,
                      "policyDigest": "policy", "samplePolicyDigest": "policy",
                      "sampleAt": "2026-11-28T00:01:00+00:00", "activatedAt": "2026-11-27T23:59:00+00:00"}

    def observe(self):
        with (patch.object(rollout, "verify_workload", return_value=("worker-deployment", 1, [self.worker])),
              patch.object(rollout, "prometheus_pods", return_value=["collector"]),
              patch.object(rollout, "activated_status", return_value=self.state),
              patch.object(rollout, "api_audit_metrics_retired", return_value=True),
              patch.object(rollout, "scrape_observed", return_value=True)):
            return rollout.continuity(self.provision, {}, {}, {"worker": {}},
                                      lambda: ("api-deployment", 2, self.api), self.before, wait_seconds=0)

    def test_new_api_with_same_worker_and_new_committed_pass(self):
        result = self.observe()
        self.assertEqual(result["apiPods"], self.api)
        self.assertEqual(result["workerPod"], self.worker)
        self.assertEqual(result["completedPasses"], 4)

    def test_requires_advance(self):
        self.state["completedPasses"] = 3
        with self.assertRaisesRegex(ProvisionError, "progress did not converge"):
            self.observe()

    def test_requires_same_worker_and_disjoint_api(self):
        self.before["workerPod"] = ["different-worker", "different-uid"]
        with self.assertRaisesRegex(ProvisionError, "worker changed"):
            self.observe()
        self.before["workerPod"] = list(self.worker)
        self.api.append(tuple(self.before["apiPods"][0]))
        with self.assertRaisesRegex(ProvisionError, "preceding API pod"):
            self.observe()

    def test_uses_immediate_api_predecessor_after_an_intervening_release(self):
        repository = "ghcr.io/example/fugue-api"
        prior = "b" * 40
        candidate = "c" * 40
        workload = {"deployment": "api", "container": "api", "replicas": 2,
                    "revision": "a" * 40, "image": repository + "@sha256:" + "1" * 64}
        intent = {"apiVersion": "release.fugue.dev/v2", "kind": "ProductionComponentIntent",
                  "component": "api", "expectedPreviousPresent": True, "expectedPreviousOciRevision": prior,
                  "expectedPreviousConfigSha": prior, "expectedPreviousManifestSha": prior,
                  "expectedPreviousImageDigest": "sha256:" + "2" * 64}
        receipt = {"apiVersion": intent["apiVersion"], "kind": "ImmutableComponentArtifactReceipt",
                   "component": "api", "sourceSha": candidate, "configSha": candidate, "ociRevision": candidate,
                   "immutableRef": repository + "@sha256:" + "3" * 64}
        before = rollout.api_target({"api": workload}, intent, None)
        self.assertEqual(before, {**workload, "revision": prior, "image": repository + "@sha256:" + "2" * 64})
        after = rollout.api_target({"api": workload}, intent, receipt)
        self.assertEqual(after, {**workload, "revision": candidate, "image": receipt["immutableRef"]})
        receipt.update(sourceSha=prior, configSha=prior, ociRevision=prior)
        with self.assertRaisesRegex(ProvisionError, "outside the cleanup rollout"):
            rollout.api_target({"api": workload}, intent, receipt)
