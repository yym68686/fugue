import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import node_resource_policy as policy


class ResourcePolicyTests(unittest.TestCase):
    def setUp(self):
        self.intent = json.loads(Path("../../deploy/environments/production/node-resource-policy/policy.json").read_text())
        self.current = {"max_user_instances": 128, "max_user_watches": 8192}
        self.facts = {"by_uid_upper_bounds": {"0": {"instances_upper_bound": 132, "watches_upper_bound": 3000}}, "unreadable": 0}

    def test_density_headroom_caps_and_no_lowering(self):
        low, unmet = policy.planned_limits(self.intent, 100, self.facts, self.current)
        high, _ = policy.planned_limits(self.intent, 200, self.facts, self.current)
        self.assertGreater(high["max_user_instances"], low["max_user_instances"])
        self.assertGreaterEqual(low["max_user_instances"], (256 + 4 * 100) * 2)
        self.assertFalse(unmet)
        limits, unmet = policy.planned_limits(self.intent, 100000, self.facts, self.current)
        self.assertEqual(limits["max_user_instances"], self.intent["max_instances"])
        self.assertTrue(unmet)
        existing = {"max_user_instances": 65536, "max_user_watches": 8000000}
        self.assertEqual(policy.planned_limits(self.intent, 0, self.facts, existing)[0], existing)

    def test_validates_policy_before_any_writes(self):
        for key, value in (("warning_percent", 100), ("enabled", "true"), ("max_instances", 127), ("min_watches", -1), ("unknown", 1)):
            intent = dict(self.intent, **{key: value})
            with patch.object(policy, "atomic_write") as write:
                with self.assertRaises(ValueError):
                    policy.reconcile(intent, persist=True)
                write.assert_not_called()

    def test_dry_run_preserves_policy_and_kernel_and_disabled_preserves_limits(self):
        with patch.object(policy, "inventory", return_value=self.facts), patch.object(policy, "container_density", return_value=150), patch.object(policy, "limits", return_value=self.current), patch.object(policy, "atomic_write") as write, patch.object(policy, "allocation_probe") as probe:
            result = policy.reconcile(self.intent, dry_run=True, persist=True)
            self.assertEqual(result["state"], "dry_run")
            write.assert_not_called()
            probe.assert_not_called()
            result = policy.reconcile(dict(self.intent, enabled=False))
            self.assertTrue(result["limits_preserved"])
            write.assert_not_called()

    def test_apply_persists_intent_without_restarts_and_retains_last_capacity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sysctl = root / "proc/sys/fs/inotify"
            sysctl.mkdir(parents=True)
            for key, value in self.current.items():
                (sysctl / key).write_text(str(value))
            with patch.object(policy, "PROC", root / "proc"), patch.object(policy, "POLICY_PATH", root / "intent.json"), patch.object(policy, "SYSCTL_PATH", root / "sysctl.conf"), patch.object(policy, "inventory", return_value=self.facts), patch.object(policy, "container_density", return_value=100), patch.object(policy, "limits", side_effect=lambda: {k: int((sysctl / k).read_text()) for k in self.current}), patch.object(policy, "allocation_probe", return_value={"status": "pass"}):
                first = policy.reconcile(self.intent, persist=True)
                self.assertEqual(first["effective"], first["desired"])
                self.assertEqual(json.loads((root / "intent.json").read_text()), self.intent)
                lowered = copy.deepcopy(self.intent)
                lowered.update(min_instances=128, base_instances=64, instances_per_container=1, headroom_percent=25)
                second = policy.reconcile(lowered, persist=True)
                self.assertEqual(second["effective"], first["effective"])

    def test_emfile_is_failure_even_with_missing_policy(self):
        with patch.object(policy, "allocation_probe", return_value={"status": "fail", "errno": 24}), patch.object(policy, "inventory", return_value=self.facts), patch.object(policy, "limits", return_value=self.current), patch.object(policy, "POLICY_PATH", Path("/nonexistent-fugue-test-policy")):
            self.assertEqual(policy.observation()["status"], "fail")


if __name__ == "__main__":
    unittest.main()
