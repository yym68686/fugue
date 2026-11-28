import unittest
from unittest.mock import Mock, patch

from scripts import verify_app_mail_standby as standby
from scripts.provision_observation_worker_database import ProvisionError
from scripts.test_apply_app_mail_reconcile_handoff import API, DECLARATION, LEADER


class AppMailStandbyTests(unittest.TestCase):
    def test_read_only_with_pre_fence_rollback(self):
        p = Mock()
        p.find_primary.return_value = ("postgres", "uid")
        progress = {**API, "missing": 0}
        rollback_revision = "f" * 40

        def fence(_floor, revision, paths):
            if revision == rollback_revision and paths == standby.API_PATHS:
                raise ProvisionError("old API")

        with (patch.object(standby, "verify_source", side_effect=fence),
              patch.object(standby, "verify_workload", return_value=("deployment", 5, [(LEADER[1], LEADER[2])])),
              patch.object(standby, "checked_rollback", return_value=(rollback_revision, "pinned")),
              patch.object(standby, "read_progress", return_value=progress),
              patch.object(standby, "check_leader", return_value=LEADER)):
            result = standby.verify_standby(p, DECLARATION)
        self.assertTrue(result["providerExactGenerationRead"])
        self.assertFalse(result["writerTransferred"])
        self.assertFalse(result["rollback"]["api"]["compatible"])
        self.assertTrue(result["rollback"]["controller"]["compatible"])
        p.sql.assert_not_called()

    def test_refuses_missing_coverage_or_a_changed_writer(self):
        p = Mock()
        p.find_primary.return_value = ("postgres", "uid")
        with (patch.object(standby, "verify_source"),
              patch.object(standby, "verify_workload"),
              patch.object(standby, "checked_rollback", return_value=("f" * 40, "pinned")),
              patch.object(standby, "read_progress") as read,
              patch.object(standby, "check_leader") as leader):
            for state, message in (
                ({**API, "missing": 1}, "queue coverage"),
                ({**API, "missing": 0, "owner": "controller"}, "standby writer"),
            ):
                with self.subTest(state=state):
                    read.return_value = state
                    with self.assertRaisesRegex(ProvisionError, message):
                        standby.verify_standby(p, DECLARATION)
            leader.assert_not_called()
        p.sql.assert_not_called()
