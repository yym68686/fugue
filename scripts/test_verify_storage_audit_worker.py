import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

from scripts.provision_observation_worker_database import ProvisionError
from scripts.publish_storage_audit_policy import publish
from scripts.rehearse_storage_lifecycle_audit import pinned_postgres
from scripts.verify_storage_audit_worker import (
    COMPONENT, JOB, database_status, release_component, scrape_observed, witness,
)


class StorageAuditObservationTests(unittest.TestCase):
    def setUp(self):
        self.head = "a" * 40
        self.image = "registry.example/fugue/audit@sha256:" + "b" * 64
        self.receipt = {
            "apiVersion": "release.fugue.dev/v2", "kind": "ImmutableComponentArtifactReceipt",
            "component": COMPONENT, "sourceSha": self.head, "configSha": self.head,
            "ociRevision": self.head, "immutableRef": self.image,
        }

    def test_release_requires_exact_component_revision_and_digest(self):
        self.assertEqual(release_component(self.receipt, self.head)["image"], self.image)
        for field, value in (("component", "api"), ("sourceSha", "c" * 40), ("configSha", "c" * 40),
                             ("ociRevision", "c" * 40), ("immutableRef", "registry.example/fugue/audit:latest")):
            with self.subTest(field=field), self.assertRaisesRegex(ProvisionError, "not bound"):
                release_component({**self.receipt, field: value}, self.head)

    def test_standby_requires_read_only_statement_and_no_committed_samples(self):
        provision = Mock(policy_digest="c" * 64)
        state = {"owner": "api", "policyDigest": provision.policy_digest, "completedPasses": 0,
                 "failed": False, "hasSample": False}
        provision.sql.return_value = "1\n" + json.dumps(state)
        self.assertEqual(database_status(provision, "test-primary", True), state)
        body = provision.sql.call_args.args[1]
        self.assertIn("BEGIN READ ONLY;", body)
        self.assertIn("FROM fugue_storage_audit.lifecycle_status_v1", body)
        for invalid in (dict(state, owner="worker"), dict(state, completedPasses=1),
                        dict(state, hasSample=True), dict(state, policyDigest="d" * 64)):
            provision.sql.return_value = "1\n" + json.dumps(invalid)
            with self.assertRaises(ProvisionError):
                database_status(provision, "test-primary", True)

    def test_scrape_must_be_fresh_dedicated_and_passive(self):
        provision = Mock(config={"namespace": "test-system"})
        uid = "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0"
        worker = ("audit-pod", uid)
        sample = {"metric": {"job": JOB, "namespace": "test-system", "pod": "audit-pod", "pod_uid": uid},
                  "value": [1_798_000_000, "1"]}

        def response(*rows):
            return json.dumps({"status": "success", "data": {"resultType": "vector", "result": rows}})

        provision.kube.return_value = response(sample)
        self.assertTrue(scrape_observed(provision, "prometheus-pod", worker, "api"))
        url = provision.kube.call_args.args[2]
        self.assertIn("/test-system/pods/prometheus-pod:9090/proxy/", url)
        query = parse_qs(urlsplit(url).query)["query"][0]
        for required in ("timestamp(up", "< 75", "fugue_storage_audit_committed_passes",
                         'pod="audit-pod"', 'namespace="test-system"', 'job="' + JOB + '"', f'pod_uid="{uid}"',
                         "(fugue_storage_audit_executor_owner"):
            self.assertIn(required, query)
        self.assertTrue(query.endswith(" == 0)"))
        for results in ([], [sample, sample], [dict(sample, metric=dict(sample["metric"], pod_uid="different-uid"))]):
            provision.kube.return_value = response(*results)
            self.assertFalse(scrape_observed(provision, "prometheus-pod", worker, "api"))
        provision.kube.return_value = response(sample)
        self.assertTrue(scrape_observed(provision, "prometheus-pod", worker, "worker", published=True))
        query = parse_qs(urlsplit(provision.kube.call_args.args[2]).query)["query"][0]
        for series in ("fugue_storage_audit_committed_passes", "fugue_storage_lifecycle_sample_current_policy",
                       "fugue_storage_lifecycle_last_run_failed", "fugue_storage_lifecycle_free_percent_threshold"):
            self.assertIn(series, query)
        with self.assertRaises(ProvisionError):
            scrape_observed(provision, "prometheus-pod", worker, "api", published=True)

    @patch("scripts.verify_storage_audit_worker.scrape_observed", return_value=True)
    @patch("scripts.verify_storage_audit_worker.prometheus_pods", return_value=["prometheus-pod"])
    @patch("scripts.verify_storage_audit_worker.database_status", return_value={"owner": "api", "completedPasses": 0})
    @patch("scripts.verify_storage_audit_worker.verify_workload")
    def test_cannot_receipt_old_or_replaced_pod(self, workload, _database, _prometheus, _scrape):
        provision = Mock()
        provision.find_primary.return_value = ("test-primary", "db-uid")
        workload.side_effect = [("deploy", 1, [("audit-old", "uid-a")]),
                                ("deploy", 1, [("audit-new", "uid-b")])]
        with self.assertRaisesRegex(ProvisionError, "changed while being observed"):
            witness(provision, {}, {}, release_component(self.receipt, self.head), standby=True)


class PinnedPostgresHarnessTests(unittest.TestCase):
    @patch("scripts.rehearse_storage_lifecycle_audit.sp.run")
    def test_creates_and_removes_only_private_pinned_test_container(self, run):
        container = "e" * 64
        image = "registry.example/postgres@sha256:" + "f" * 64
        run.side_effect = [
            subprocess.CompletedProcess([], 0, stdout=container + "\n"),
            subprocess.CompletedProcess([], 0),
            subprocess.CompletedProcess([], 0, stdout=container + "\n"),
            subprocess.CompletedProcess([], 0),
        ]
        with tempfile.TemporaryDirectory(prefix="test-audit-") as path:
            with pinned_postgres(image, Path(path)) as (socket, psql, mount):
                self.assertTrue(socket.startswith(path))
                self.assertEqual(psql, ("docker", "exec", "-i", container, "psql"))
                self.assertEqual(mount, "/audit-socket")
        calls = [call.args[0] for call in run.call_args_list]
        self.assertIn(image, calls[0])
        self.assertIn("--network", calls[0])
        self.assertIn("none", calls[0])
        self.assertTrue(any(arg.startswith("label=fugue.pro/storage-audit-rehearsal=fugue-test-audit-") for arg in calls[2]))
        self.assertEqual(calls[3], ["docker", "rm", "--force", container])

    @patch("scripts.rehearse_storage_lifecycle_audit.sp.run")
    def test_never_runs_a_mutable_postgres_tag(self, run):
        with self.assertRaisesRegex(ValueError, "immutable"):
            with pinned_postgres("registry.example/postgres:latest", Path("/unused-test-path")):
                self.fail("mutable fixture image unexpectedly accepted")
        run.assert_not_called()


class StorageAuditPolicyPublicationTests(unittest.TestCase):
    def setUp(self):
        self.provision = Mock(policy_digest="b" * 64)
        self.primary = ("primary", "primary-uid")
        self.provision.find_primary.return_value = self.primary
        self.provision.get.return_value = {"metadata": {}, "data": {"storage-lifecycle.json": '{"mode":"audit"}'}}
        self.desired = {"name": "test-policy", "storageLifecycle": {"mode": "disabled"}}

    @patch("scripts.publish_storage_audit_policy.policy_update_sql", return_value="bounded policy CAS")
    def test_confirms_bound_database_and_carries_a_redacted_outcome(self, sql):
        self.provision.sql.return_value = "1\n\nworker:unchanged"
        reported = publish(self.provision, self.desired, apply=True)
        self.assertEqual(reported, {"candidatePolicyDigest": "b" * 64, "outcome": "worker:unchanged", "applied": True})
        sql.assert_called_once_with(self.provision, {"name": "test-policy", "storageLifecycle": {"mode": "audit"}}, apply=True)
        self.provision.sql.assert_called_once_with("primary", "bounded policy CAS")
        self.provision.sql.return_value = "1\n\nworker:unexpected"
        with self.assertRaisesRegex(ProvisionError, "did not confirm"):
            publish(self.provision, self.desired, apply=True)

    def test_primary_change_stops_before_policy_transaction(self):
        self.provision.find_primary.side_effect = [self.primary, ("replacement", "replacement-uid")]
        with self.assertRaisesRegex(ProvisionError, "database changed"):
            publish(self.provision, self.desired, apply=True)
        self.provision.sql.assert_not_called()


if __name__ == "__main__":
    unittest.main()
