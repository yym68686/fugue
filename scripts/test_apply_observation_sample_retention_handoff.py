import unittest
from unittest.mock import Mock, patch

from scripts import apply_observation_sample_retention_handoff as handoff
from scripts.provision_observation_worker_database import ProvisionError
from scripts.test_provision_observation_sample_retention import CONFIG, UID


class SampleRetentionHandoffTest(unittest.TestCase):
    def fixture(self):
        provision = Mock()
        provision.config = CONFIG
        provision.credential.return_value = UID, "synthetic-unused-password"
        provision.find_primary.return_value = ("synthetic-primary", UID)
        release = {
            "api": {"revision": "a" * 40}, "worker": {"revision": "b" * 40},
            "apiForwardFloor": "a" * 40, "apiRollbackFloor": "c" * 40,
            "workerForwardFloor": "b" * 40, "workerRollbackFloor": "d" * 40,
        }
        target = ((UID, 1, [("synthetic-pod", UID)]), ("f" * 40, "synthetic-image"))
        before = {name: {
            "owner": "api", "fence": False, "transferAt": None, "completedAt": None,
            "completedPasses": 0, "totalDeleted": 0,
        } for name in handoff.TARGETS}
        after = {name: {
            "owner": "worker", "fence": True, "transferAt": "2026-01-01T00:00:00+00:00",
            "completedAt": "2026-01-01T00:00:01+00:00", "completedPasses": 1,
        } for name in handoff.TARGETS}
        return provision, release, target, before, after

    def test_old_rollback_or_changed_policy_stops_before_activation(self):
        provision, release, target, before, _ = self.fixture()
        with patch.object(handoff, "check_workload", side_effect=ProvisionError("incompatible rollback")):
            with self.assertRaises(ProvisionError):
                handoff.apply(provision, release, {}, {}, "", wait_seconds=0)
        provision.sql.assert_not_called()

        with (patch.object(handoff, "check_workload", return_value=target),
              patch.object(handoff, "verify_collector"),
              patch.object(handoff, "verify_policy", side_effect=[(UID, "10"), (UID, "11")]),
              patch.object(handoff, "read_progress", return_value=before)):
            with self.assertRaisesRegex(ProvisionError, "policy.* changed"):
                handoff.apply(provision, release, {}, {}, "", wait_seconds=0)
        provision.sql.assert_not_called()

    def test_reports_completion_only_after_all_three_fresh_commits(self):
        provision, release, target, before, after = self.fixture()
        with (patch.object(handoff, "check_workload", return_value=target) as check,
              patch.object(handoff, "verify_collector") as scrape,
              patch.object(handoff, "verify_policy", return_value=(UID, "10")),
              patch.object(handoff, "read_progress", side_effect=[before, after])):
            actual = handoff.apply(provision, release, {}, {}, handoff.ACTIVATION.read_text(), wait_seconds=0)
        self.assertEqual(actual, after)
        self.assertEqual(check.call_count, 4)
        self.assertEqual(scrape.call_count, 2)
        self.assertEqual(check.call_args_list[1].args[4], release["workerRollbackFloor"])
        provision.sql.assert_called_once()
        self.assertIn("excess sample write rights", provision.sql.call_args.args[1])

    def test_partial_progress_cannot_reenable_the_api_writer(self):
        provision, release, target, before, after = self.fixture()
        after["resource-usage"].update(completedAt=None, completedPasses=0)
        with (patch.object(handoff, "check_workload", return_value=target),
              patch.object(handoff, "verify_collector"),
              patch.object(handoff, "verify_policy", return_value=(UID, "10")),
              patch.object(handoff, "read_progress", side_effect=[before, after])):
            with self.assertRaisesRegex(ProvisionError, "do not reset ownership"):
                handoff.apply(provision, release, {}, {}, handoff.ACTIVATION.read_text(), wait_seconds=0)
        provision.sql.assert_called_once()


if __name__ == "__main__":
    unittest.main()
