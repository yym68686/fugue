import unittest
import json
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

from scripts import apply_hosted_dns_writer_handoff as handoff
from scripts.provision_observation_worker_database import ProvisionError, ProvisionLockBusy
from scripts.test_provision_observation_worker_database import SECRET_UID


class HostedDNSHandoffTest(unittest.TestCase):
    def setUp(self):
        self.provision = Mock(config={"secret": "worker-credential"})
        self.provision.credential.return_value = SECRET_UID, "fixture-unused-key"
        self.provision.find_primary.return_value = ("primary", "primary-uid")
        self.worker = ("worker-pod", SECRET_UID)
        self.collector = "prometheus-pod"
        discovery = patch.object(handoff, "prometheus_pods", return_value=[self.collector])
        discovery.start()
        self.addCleanup(discovery.stop)
        executors = (("deployment-uid", 1, [self.worker]), ("a" * 40, "test-image"))
        self.executors = {"api": executors, "worker": executors}
        self.before = {"owner": "api", "completedAt": None, "transferAt": None, "updated": None}
        self.after = {"owner": "worker", "completedAt": "2026-11-28T00:01:00+00:00",
                      "transferAt": "2026-11-28T00:00:00+00:00", "updated": 0}

    def test_busy_retry_rechecks_exact_runtime_before_committing(self):
        self.provision.sql.side_effect = [ProvisionLockBusy("busy"), ""]
        with (patch.object(handoff, "check_executors", return_value=self.executors) as check,
              patch.object(handoff, "observe", side_effect=[self.before, self.before, self.after]),
              patch.object(handoff, "scrape_observed", return_value=True) as collector,
              patch.object(handoff.time, "sleep") as delay):
            self.assertEqual(handoff.apply(self.provision, {}, {}, 0)["completedAt"], self.after["completedAt"])
        self.assertEqual(check.call_count, 4)
        self.assertEqual(collector.call_count, 3)
        collector.assert_called_with(self.provision, self.collector, self.worker, "worker", self.after["completedAt"])
        self.assertEqual(self.provision.sql.call_count, 2)
        self.assertIn("pg_try_advisory_xact_lock", self.provision.sql.call_args.args[1])
        delay.assert_called_once_with(2)

    def test_unknown_result_never_retries_a_mutation(self):
        self.provision.sql.side_effect = ProvisionError("unknown transport")
        with (patch.object(handoff, "check_executors", return_value=self.executors),
              patch.object(handoff, "observe", return_value=self.before),
              patch.object(handoff, "scrape_observed", return_value=True)):
            with self.assertRaisesRegex(ProvisionError, "unknown transport"):
                handoff.apply(self.provision, {}, {}, 0)
        self.provision.sql.assert_called_once()

    def test_unchanged_commit_timestamp_does_not_complete(self):
        with (patch.object(handoff, "check_executors", return_value=self.executors),
              patch.object(handoff, "observe", return_value=self.after),
              patch.object(handoff, "scrape_observed", return_value=True)):
            with self.assertRaisesRegex(ProvisionError, "preserve the worker owner"):
                handoff.apply(self.provision, {}, {}, 0)
        self.provision.sql.assert_not_called()

    def test_unobserved_standby_is_never_activated(self):
        with (patch.object(handoff, "check_executors", return_value=self.executors),
              patch.object(handoff, "observe", return_value=self.before),
              patch.object(handoff, "scrape_observed", return_value=False)):
            with self.assertRaisesRegex(ProvisionError, "exact hosted DNS worker and owner"):
                handoff.apply(self.provision, {}, {}, 0)
        self.provision.sql.assert_not_called()

    def test_new_database_progress_must_be_observed_independently(self):
        with (patch.object(handoff, "check_executors", return_value=self.executors),
              patch.object(handoff, "observe", side_effect=[self.before, self.after]),
              patch.object(handoff, "scrape_observed", side_effect=[True, False])):
            with self.assertRaisesRegex(ProvisionError, "not independently confirmed; preserve"):
                handoff.apply(self.provision, {}, {}, 0)
        self.provision.sql.assert_called_once()

    def test_scrape_binds_namespace_identity_and_owner(self):
        self.provision.config["namespace"] = "test-system"
        metric = {"job": handoff.DEPLOYMENT, "namespace": "test-system", "pod": self.worker[0], "pod_uid": self.worker[1]}

        def response(*results):
            self.provision.kube.return_value = json.dumps({"status": "success", "data": {"resultType": "vector", "result": list(results)}})

        sample = {"metric": metric, "value": [1, "1"]}
        response(sample)
        self.assertTrue(handoff.scrape_observed(self.provision, self.collector, self.worker, "api"))
        query = parse_qs(urlsplit(self.provision.kube.call_args.args[-1]).query)["query"][0]
        self.assertIn(f'pod_uid="{SECRET_UID}"', query)
        self.assertIn('owner="api"', query)
        self.assertIn(" unless on(job,namespace,pod,pod_uid) fugue_hosted_dns_last_completed_at_seconds", query)
        for results in ((), (sample, sample), (dict(sample, metric=dict(metric, pod_uid="old-uid")),)):
            response(*results)
            self.assertFalse(handoff.scrape_observed(self.provision, self.collector, self.worker, "api"))

    def test_incompatible_rollback_is_rejected_before_writing(self):
        with (patch.object(handoff, "verify_workload", return_value=self.executors["api"][0]),
              patch.object(handoff, "checked_rollback", return_value=self.executors["api"][1]),
              patch.object(handoff, "check_source", side_effect=[None, ProvisionError("incompatible rollback")])):
            with self.assertRaisesRegex(ProvisionError, "incompatible rollback"):
                handoff.apply(self.provision, {"api": {"revision": "b" * 40}, "apiFloor": "c" * 40}, {}, 0)
        self.provision.sql.assert_not_called()


if __name__ == "__main__":
    unittest.main()
