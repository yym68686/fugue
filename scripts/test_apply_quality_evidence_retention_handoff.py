import unittest
from unittest.mock import Mock, patch

from scripts import apply_quality_evidence_retention_handoff as handoff
from scripts.provision_observation_worker_database import Provision, ProvisionError
from scripts.test_provision_quality_evidence_retention import CONFIG, UID


class QualityEvidenceHandoffTest(unittest.TestCase):
    def fixture(self):
        provision = Mock()
        provision.config = CONFIG
        provision.credential.return_value = UID, "synthetic-unused-password"
        provision.find_primary.return_value = ("ready-primary", UID)
        release = {
            "api": {"revision": "a" * 40},
            "worker": {"revision": "b" * 40},
            "apiFloor": "a" * 40,
            "workerFloor": "b" * 40,
        }
        target = ((UID, 1, [("synthetic-pod", UID)]), ("f" * 40, "synthetic-image"))
        return provision, release, target

    def test_unsafe_rollback_refuses_before_write(self):
        provision, release, _ = self.fixture()
        with patch.object(handoff, "check_workload", side_effect=ProvisionError("incompatible rollback")):
            with self.assertRaises(ProvisionError):
                handoff.apply(provision, release, {}, "", wait_seconds=0)
        provision.sql.assert_not_called()

    def test_binds_capability_and_reports_only_a_post_transfer_commit(self):
        provision, release, target = self.fixture()
        before = {"owner": "api", "completedPasses": 0, "completedAt": None, "transferAt": None, "fence": False}
        transfer = "2026-01-01T00:00:00+00:00"
        after = {
            "owner": "worker", "completedPasses": 1, "fence": True, "transferAt": transfer,
            "completedAt": "2026-01-01T00:00:01+00:00",
        }
        with (patch.object(handoff, "check_workload", return_value=target),
              patch.object(handoff, "verify_collector"),
              patch.object(handoff, "read_progress", side_effect=[before, after])):
            self.assertEqual(handoff.apply(provision, release, {}, handoff.ACTIVATION.read_text(), wait_seconds=0), after)
        provision.sql.assert_called_once()
        self.assertIn("has_function_privilege", provision.sql.call_args.args[1])
        self.assertIn("pg_try_advisory_xact_lock", provision.sql.call_args.args[1])
        self.assertIn("ENABLE ALWAYS TRIGGER", provision.sql.call_args.args[1])

    def test_freshness_timeout_does_not_reset_the_writer(self):
        provision, release, target = self.fixture()
        before = {"owner": "api", "completedPasses": 0, "completedAt": None, "transferAt": None, "fence": False}
        after = {"owner": "worker", "completedPasses": 0, "completedAt": None, "fence": True,
                 "transferAt": "2026-01-01T00:00:00+00:00"}
        with (patch.object(handoff, "check_workload", return_value=target),
              patch.object(handoff, "verify_collector"),
              patch.object(handoff, "read_progress", side_effect=[before, after])):
            with self.assertRaisesRegex(ProvisionError, "do not reset ownership"):
                handoff.apply(provision, release, {}, handoff.ACTIVATION.read_text(), wait_seconds=0)
        provision.sql.assert_called_once()

    def test_rejects_broken_transaction_envelope(self):
        with self.assertRaises(ProvisionError):
            handoff.activation_sql(Provision(CONFIG), UID, "BEGIN;\nSELECT 1;")


if __name__ == "__main__":
    unittest.main()
