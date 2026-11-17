import json
from pathlib import Path
import shutil
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.qualify_k3s_bootstrap import VERSIONS, memory_bytes, rehearse_recovery


class BootstrapQualificationTests(unittest.TestCase):
    def test_exact_adjacent_minor_versions(self):
        self.assertEqual(VERSIONS, ("v1.35.10+k3s1", "v1.36.6+k3s1"))
        minors = [int(version.split(".")[1]) for version in VERSIONS]
        self.assertTrue(all(right == left + 1 for left, right in zip(minors, minors[1:])))

    def test_scheduler_memory_units(self):
        for raw, expected in [("0", 0), ("128Mi", 134217728), ("1Gi", 1073741824),
                              ("1024Ki", 1048576), ("1.5Gi", 1610612736),
                              ("128M", 128000000), ("1000m", 1), ("2k", 2000)]:
            with self.subTest(raw=raw):
                self.assertEqual(memory_bytes(raw), expected)

    def test_invalid_scheduler_memory_is_not_guessed(self):
        for value in ("unknown", "128MiB", "", "-1Mi"):
            with self.subTest(value=value), self.assertRaises(AssertionError):
                memory_bytes(value)


class RecoveryQualificationTests(unittest.TestCase):
    def exercise(self, *, restored="before-snapshot", uid="node-uid", reset_error=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence, recovery, snapshots = (root / name for name in ("evidence", "recovery", "snapshots"))
            for path in (evidence, recovery, snapshots):
                path.mkdir()
            kubeconfig, token = root / "kubeconfig", root / "original-token"
            kubeconfig.write_text("disposable-only")
            token.write_text("test-only-token-never-upload")
            (snapshots / "qualification-recovery-qualification-control-1").write_bytes(b"synthetic-snapshot")
            paths = {"/etc/rancher/k3s/k3s.yaml": kubeconfig,
                     "/var/lib/rancher/k3s/server/db/snapshots": snapshots}
            copied = shutil.copyfile

            def copy(source, destination):
                return copied(token if str(source) == "/var/lib/rancher/k3s/server/token" else source, destination)

            commands = []

            def run(*args, **kwargs):
                commands.append(args)
                if reset_error and "--cluster-reset" in args:
                    raise RuntimeError("injected disposable reset failure")
                return SimpleNamespace(stdout="", stderr="", returncode=0)

            with patch("scripts.qualify_k3s_bootstrap.Path", side_effect=lambda value: paths.get(str(value), Path(value))), \
                 patch("scripts.qualify_k3s_bootstrap.tempfile.mkdtemp", return_value=str(recovery)), \
                 patch("scripts.qualify_k3s_bootstrap.shutil.copyfile", side_effect=copy), \
                 patch("scripts.qualify_k3s_bootstrap.run", side_effect=run), \
                 patch("scripts.qualify_k3s_bootstrap.kubectl", return_value=SimpleNamespace(stdout=json.dumps({"data": {"value": restored}}))), \
                 patch("scripts.qualify_k3s_bootstrap.wait_ready", return_value=uid):
                try:
                    rehearse_recovery(evidence, VERSIONS[-1], "node-uid")
                finally:
                    if reset_error:
                        self.assertEqual(commands[-1], ("systemctl", "start", "k3s"))
            report = (evidence / "recovery.json").read_text()
            self.assertNotIn(token.read_text(), report)
            self.assertEqual(list(evidence.iterdir()), [evidence / "recovery.json"])
            self.assertEqual((recovery / "token").stat().st_mode & 0o777, 0o600)
            self.assertEqual((recovery / "snapshot").stat().st_mode & 0o777, 0o600)
            self.assertTrue(json.loads(report)["passed"])
            reset = next(command for command in commands if "--cluster-reset" in command)
            self.assertIn("--token-file", reset)
            self.assertNotIn(token.read_text(), reset)

    def test_restored_canary_and_private_matching_token(self):
        self.exercise()

    def test_unchanged_canary_is_not_false_restore_success(self):
        with self.assertRaisesRegex(AssertionError, "did not revert"):
            self.exercise(restored="after-snapshot")

    def test_changed_node_identity_is_not_success(self):
        with self.assertRaises(AssertionError):
            self.exercise(uid="different-node")

    def test_disposable_service_start_attempted_after_reset_failure(self):
        with self.assertRaisesRegex(RuntimeError, "injected disposable"):
            self.exercise(reset_error=True)


if __name__ == "__main__":
    unittest.main()
