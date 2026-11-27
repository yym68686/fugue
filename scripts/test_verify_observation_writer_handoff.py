import copy
import json
import unittest
from unittest.mock import Mock, patch

from scripts.apply_observation_writer_handoff import verify_collector, verify_committed_progress
from scripts.provision_observation_worker_database import ProvisionError
from scripts.verify_observation_writer_handoff import RELEASE_SCHEMA, load_release, verify_workload


WORKER = {"deployment": "worker-test", "container": "worker", "image": "registry.example/worker@sha256:" + "1" * 64,
          "revision": "a" * 40, "replicas": 1}
TEMPLATE = {"metadata": {"annotations": {"fugue.pro/oci-revision": WORKER["revision"]}},
            "spec": {"containers": [{"name": "worker", "image": WORKER["image"]}]}}


class FakeProvision:
    def __init__(self):
        selector = {"matchLabels": {"app": "worker-test"}}
        self.deployment = {"metadata": {"uid": "deployment-uid", "generation": 3},
                           "spec": {"replicas": 1, "template": TEMPLATE, "selector": selector},
                           "status": {"observedGeneration": 3, "replicas": 1, "updatedReplicas": 1,
                                      "readyReplicas": 1, "availableReplicas": 1}}
        self.replica_sets = [{"metadata": {"uid": "replica-uid", "ownerReferences": [
            {"kind": "Deployment", "uid": "deployment-uid", "controller": True}]}}]
        self.pods = [copy.deepcopy(TEMPLATE)]
        self.pods[0]["metadata"].update(name="worker-pod", uid="pod-uid", ownerReferences=[
            {"kind": "ReplicaSet", "uid": "replica-uid", "controller": True}])
        self.pods[0]["status"] = {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}],
                                  "containerStatuses": [{"name": "worker", "ready": True, "imageID": "runtime-id"}]}

    def get(self, kind, name):
        assert kind == "deployment" and name == WORKER["deployment"]
        return self.deployment

    def kube(self, verb, kind, selector_flag, selector, output_flag, output):
        assert (verb, selector_flag, selector, output_flag, output) == ("get", "-l", "app=worker-test", "-o", "json")
        return json.dumps({"items": self.replica_sets if kind == "replicasets" else self.pods})


class PreflightTests(unittest.TestCase):
    def test_verifies_pod_identity_and_readiness(self):
        provision = FakeProvision()
        self.assertEqual(("deployment-uid", 3, [("worker-pod", "pod-uid")]), verify_workload(provision, WORKER))
        provision.pods[0]["metadata"]["deletionTimestamp"] = "test-deletion"
        with self.assertRaisesRegex(ProvisionError, "draining"):
            verify_workload(provision, WORKER)

    def test_rejects_old_pod_even_if_deployment_reports_healthy(self):
        provision = FakeProvision()
        old = copy.deepcopy(provision.pods[0])
        old["metadata"]["annotations"]["fugue.pro/oci-revision"] = "b" * 40
        provision.pods.append(old)
        with self.assertRaisesRegex(ProvisionError, "old"):
            verify_workload(provision, WORKER)

    def test_rejects_reusing_api_as_worker(self):
        fake = {"schema": RELEASE_SCHEMA, "api": dict(WORKER), "worker": dict(WORKER)}
        with self.assertRaisesRegex(ProvisionError, "independent"):
            load_release(fake)
        fake["api"].update(deployment="api-test", image="registry.example/api:latest")
        with self.assertRaisesRegex(ProvisionError, "workload identity"):
            load_release(fake)

    def test_collector_must_observe_the_bound_pod(self):
        provision = Mock(config={"namespace": "test-system"})
        config = {"namespace": "test-system", "deployment": "metrics-test"}
        provision.get.return_value = {"spec": {"selector": {"matchLabels": {"app": "test-metrics"}}}}
        def query(*args):
            if args[:2] == ("get", "pods"):
                return json.dumps({"items": [{"metadata": {"name": "metrics-pod"},
                                             "status": {"conditions": [{"type": "Ready", "status": "True"}]}}]})
            self.assertTrue(args[2].startswith("/api/v1/namespaces/test-system/pods/metrics-pod:9090/proxy/api/v1/query?"))
            return json.dumps({"status": "success", "data": {"result": [{"metric": {"pod": "old-pod"}}]}})
        provision.kube.side_effect = query
        with self.assertRaisesRegex(ProvisionError, "every candidate"):
            verify_collector(provision, config, ["new-pod"])
        verify_collector(provision, config, ["old-pod"])

    @patch("scripts.apply_observation_writer_handoff.worker_metrics")
    @patch("scripts.apply_observation_writer_handoff.verify_workload")
    @patch("scripts.apply_observation_writer_handoff.observe_database")
    def test_progress_requires_both_a_later_watermark_and_worker_success(self, observe, workload, metrics):
        previous = {"watermark": "2026-11-01T19:00:00+00:00"}
        provision = Mock()
        provision.find_primary.return_value = ("primary", "uid")
        primary = provision.find_primary()
        release = {"worker": WORKER}
        workload.return_value = ("uid", 1, [("worker-pod", "worker-uid")])
        metrics.return_value = "fugue_edge_quality_rollup_last_success_timestamp_seconds 99.000000\n"
        observe.return_value = {"owner": "worker", "watermark": "2026-11-01T19:05:00+00:00"}
        with self.assertRaisesRegex(ProvisionError, "new committed watermark"):
            verify_committed_progress(provision, release, primary, previous, 100, wait_seconds=0)
        metrics.return_value = "fugue_edge_quality_rollup_last_success_timestamp_seconds 102.000000\n"
        self.assertEqual("worker-uid", verify_committed_progress(
            provision, release, primary, previous, 100, wait_seconds=0)["podUID"])
        observe.return_value = {"owner": "worker", **previous}
        with self.assertRaisesRegex(ProvisionError, "new committed watermark"):
            verify_committed_progress(provision, release, primary, previous, 100, wait_seconds=0)


if __name__ == "__main__":
    unittest.main()
