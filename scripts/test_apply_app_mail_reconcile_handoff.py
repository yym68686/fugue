from datetime import datetime, timezone
import unittest
from unittest.mock import Mock, patch

from scripts import apply_app_mail_reconcile_handoff as mail
from scripts.provision_observation_worker_database import ProvisionError


def target(component):
    return {"revision": "a" * 40, "image": f"ghcr.io/test/{component}@sha256:" + "1" * 64,
            "deployment": f"control-{component}", "container": component, "replicas": 1}


DECLARATION = {
    "schema": mail.SCHEMA, "api": target("api"), "controller": target("controller"),
    **{role + kind + "Floor": "b" * 40 for role in ("api", "controller") for kind in ("Forward", "Rollback")},
}
API = {"owner": "api", "activated": None, "seeded": True, "active": 2, "confirmed": 0}
CONTROLLER = {"owner": "controller", "activated": "2026-11-01T12:15:01Z",
              "seeded": True, "active": 2, "confirmed": 2}
LEADER = ("lease-uid", "controller-one", "pod-uid")
RUNTIME = {"controller": (("deployment-uid", 4, [(LEADER[1], LEADER[2])]), ("rollback", "image"))}
NOW = datetime(2026, 11, 1, 12, 15, 2, tzinfo=timezone.utc)


def metrics(*, owner=0, read_age=8, check_age=1):
    return "\n".join("fugue_controller_app_mail_" + key + f" {value:.6f}" for key, value in {
        "seed_complete": 1, "provider_config_ready": 1, "executor_owner": owner,
        "last_check_timestamp_seconds": NOW.timestamp() - check_age,
        "provider_read_timestamp_seconds": NOW.timestamp() - read_age,
    }.items())


class MailHandoffTests(unittest.TestCase):
    def test_declaration_requires_immutable_roles_and_floors(self):
        self.assertEqual(mail.parse_release(DECLARATION), DECLARATION)
        for candidate in (
            {**DECLARATION, "controller": {**DECLARATION["controller"], "container": "worker"}},
            {**DECLARATION, "api": {**DECLARATION["api"], "image": "image:latest"}},
            {**DECLARATION, "controllerRollbackFloor": "main"},
        ):
            with self.assertRaises(ProvisionError):
                mail.parse_release(candidate)

    def test_standby_requires_current_leader_and_authenticated_provider_get(self):
        p = Mock(config={"namespace": "test"})
        p.get.return_value = {"metadata": {"uid": LEADER[0]}, "spec": {
            "holderIdentity": LEADER[1], "renewTime": "2026-11-01T12:15:01Z", "leaseDurationSeconds": 15,
        }}
        pods = [(LEADER[1], LEADER[2])]
        workload = DECLARATION["controller"]
        p.kube.return_value = metrics()
        self.assertEqual(mail.check_leader(p, workload, pods, "api", NOW), LEADER)
        for series, owner, expected in [
            (metrics(read_age=90), "api", "no fresh authenticated read"),
            (metrics(check_age=65), "api", "not freshly ready"),
            (metrics(owner=0), "controller", "not freshly ready"),
        ]:
            p.kube.return_value = series
            with self.assertRaisesRegex(ProvisionError, expected):
                mail.check_leader(p, workload, pods, owner, NOW)
        p.kube.return_value = metrics(owner=1, read_age=9999)
        self.assertEqual(mail.check_leader(p, workload, pods, "controller", NOW), LEADER)
        with self.assertRaisesRegex(ProvisionError, "no fresh leader"):
            mail.check_leader(p, workload, [("old-pod", "old-uid")], "api", NOW)

    def test_preview_and_changed_primary_cannot_write(self):
        p = Mock()
        p.find_primary.return_value = ("primary", "uid-one")
        with (patch.object(mail, "runtime_snapshot", return_value=RUNTIME),
              patch.object(mail, "read_progress", return_value=API),
              patch.object(mail, "check_leader", return_value=LEADER)):
            self.assertFalse(mail.handoff(p, DECLARATION, "unused")["applied"])
            p.find_primary.side_effect = [("primary", "uid-one"), ("new-primary", "uid-two")]
            with self.assertRaisesRegex(ProvisionError, "changed before handoff"):
                mail.handoff(p, DECLARATION, "unused", apply=True)
        p.sql.assert_not_called()

    def test_postcutover_progress_is_required_and_replay_is_read_only(self):
        p = Mock()
        p.find_primary.return_value = ("primary", "uid-one")
        p.identity_sql.return_value = "SELECT 1 /* primary identity fixture */;"
        with (patch.object(mail, "runtime_snapshot", return_value=RUNTIME),
              patch.object(mail, "read_progress", side_effect=[API, {**CONTROLLER, "confirmed": 1}]),
              patch.object(mail, "check_leader", return_value=LEADER)):
            with self.assertRaisesRegex(ProvisionError, "did not advance.*do not reset"):
                mail.handoff(p, DECLARATION, mail.ACTIVATION.read_text(), apply=True, wait_seconds=0)
        p.sql.assert_called_once()
        self.assertIn("primary identity fixture", p.sql.call_args.args[1])
        p.reset_mock()
        with (patch.object(mail, "runtime_snapshot", return_value=RUNTIME),
              patch.object(mail, "read_progress", return_value=CONTROLLER),
              patch.object(mail, "check_leader", return_value=LEADER)):
            self.assertEqual(mail.handoff(p, DECLARATION, mail.ACTIVATION.read_text(), apply=True)["database"], CONTROLLER)
        p.sql.assert_not_called()

    def test_rollback_source_is_checked_for_both_components(self):
        with (patch.object(mail, "verify_workload", return_value=("live",)),
              patch.object(mail, "checked_rollback", side_effect=[("api-lkg", "image"), ("controller-lkg", "image")]),
              patch.object(mail, "verify_source") as source):
            mail.runtime_snapshot(Mock(), DECLARATION)
        self.assertEqual([call.args[1] for call in source.call_args_list],
                         [DECLARATION["api"]["revision"], "api-lkg", DECLARATION["controller"]["revision"], "controller-lkg"])
