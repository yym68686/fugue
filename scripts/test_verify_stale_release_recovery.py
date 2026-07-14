#!/usr/bin/env python3

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


MODULE_PATH = Path(__file__).with_name("verify_stale_release_recovery.py")
SPEC = importlib.util.spec_from_file_location("verify_stale_release_recovery", MODULE_PATH)
assert SPEC and SPEC.loader
recovery = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(recovery)


class StaleReleaseRecoveryProofTests(unittest.TestCase):
    def setUp(self) -> None:
        self.failure = "NodeLocal pre-Helm verification failed"
        self.source = f'''#!/usr/bin/env bash

fail() {{
  echo "$*"
  exit 1
}}

main() {{
  fail "{self.failure}"
  CONTROL_PLANE_RELEASE_HELM_MUTATION_STARTED="true"
  helm upgrade "${{FUGUE_RELEASE_NAME}}"
}}

patch='{{"op": "add", "path": "/spec/leaseDurationSeconds", "value": 0}}'
'''.encode()

    def test_source_proves_terminal_failure_before_helm(self) -> None:
        recovery.validate_release_source(
            self.source,
            expected_blob=recovery.git_blob_sha1(self.source),
            expected_failure=self.failure,
        )

    def test_source_rejects_failure_after_mutation(self) -> None:
        unsafe = self.source.replace(
            f'  fail "{self.failure}"\n  CONTROL_PLANE_RELEASE_HELM_MUTATION_STARTED="true"'.encode(),
            f'  CONTROL_PLANE_RELEASE_HELM_MUTATION_STARTED="true"\n  fail "{self.failure}"'.encode(),
        )
        with self.assertRaises(recovery.RecoveryProofError):
            recovery.validate_release_source(
                unsafe,
                expected_blob=recovery.git_blob_sha1(unsafe),
                expected_failure=self.failure,
            )

    def _run_payload(self) -> dict:
        return {
            "id": 29305932747,
            "run_attempt": 1,
            "status": "completed",
            "conclusion": "failure",
            "path": ".github/workflows/deploy-control-plane.yml",
            "head_sha": "5a03e8704d075d3b5e2b8724d75071cc16eff7e5",
            "repository": {"full_name": "owner/fugue"},
        }

    def _jobs_payload(self) -> dict:
        return {
            "jobs": [
                {
                    "id": 86999716294,
                    "name": "deploy",
                    "run_attempt": 1,
                    "status": "completed",
                    "conclusion": "failure",
                    "runner_name": "control-plane-runner",
                }
            ]
        }

    def test_github_evidence_binds_exact_attempt_and_runner(self) -> None:
        job_id = recovery.validate_github_evidence(
            self._run_payload(),
            self._run_payload(),
            self._jobs_payload(),
            repository="owner/fugue",
            workflow_path=".github/workflows/deploy-control-plane.yml",
            expected_run_id=29305932747,
            expected_attempt=1,
            expected_head_sha="5a03e8704d075d3b5e2b8724d75071cc16eff7e5",
            expected_job_name="deploy",
            expected_runner_name="control-plane-runner",
        )
        self.assertEqual(job_id, 86999716294)

    def test_github_evidence_rejects_newer_rerun(self) -> None:
        latest = self._run_payload()
        latest["run_attempt"] = 2
        with self.assertRaises(recovery.RecoveryProofError):
            recovery.validate_github_evidence(
                self._run_payload(),
                latest,
                self._jobs_payload(),
                repository="owner/fugue",
                workflow_path=".github/workflows/deploy-control-plane.yml",
                expected_run_id=29305932747,
                expected_attempt=1,
                expected_head_sha="5a03e8704d075d3b5e2b8724d75071cc16eff7e5",
                expected_job_name="deploy",
                expected_runner_name="control-plane-runner",
            )

    def test_job_log_requires_exact_failure_and_no_helm_command(self) -> None:
        log = "\n".join(
            (
                f"previous Helm revision: 706",
                f"ERROR: {self.failure}",
                "spec.leaseDurationSeconds: Invalid value: 0: must be greater than 0",
                "owner-CAS release of the control-plane backup coordination Lease failed; holder was not overwritten",
            )
        )
        recovery.validate_job_log(log, expected_failure=self.failure, expected_revision=706)
        with self.assertRaises(recovery.RecoveryProofError):
            recovery.validate_job_log(
                log + "\nHelm upgrade",
                expected_failure=self.failure,
                expected_revision=706,
            )

    def test_helm_history_rejects_any_newer_revision(self) -> None:
        recovery.validate_helm_history(
            [{"revision": 705, "status": "superseded"}, {"revision": 706, "status": "deployed"}],
            expected_revision=706,
        )
        with self.assertRaises(recovery.RecoveryProofError):
            recovery.validate_helm_history(
                [{"revision": 706, "status": "superseded"}, {"revision": 707, "status": "deployed"}],
                expected_revision=706,
            )

    def test_old_run_process_check_is_exact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            proc = Path(directory)
            (proc / "101").mkdir()
            (proc / "101" / "environ").write_bytes(b"GITHUB_RUN_ID=42\0")
            recovery.assert_old_run_process_absent(41, proc_root=proc)
            with self.assertRaises(recovery.RecoveryProofError):
                recovery.assert_old_run_process_absent(42, proc_root=proc)

    def test_old_run_process_check_ignores_esrch_race(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            proc = Path(directory)
            (proc / "101").mkdir()
            with mock.patch.object(Path, "read_bytes", side_effect=ProcessLookupError()):
                recovery.assert_old_run_process_absent(42, proc_root=proc)

    def test_github_evidence_uses_complete_curl_output(self) -> None:
        def fake_download(_url: str, _token: str, output: Path, *, limit: int) -> None:
            self.assertEqual(limit, 32 * 1024 * 1024)
            output.write_bytes(b'{"status":"completed"}')

        with mock.patch.object(recovery, "_curl_download", side_effect=fake_download):
            self.assertEqual(
                recovery.github_get("https://api.github.test/evidence", "test-token"),
                {"status": "completed"},
            )

    def test_curl_token_is_not_in_process_arguments(self) -> None:
        def fake_run(command, **kwargs):
            self.assertNotIn("secret-token", " ".join(command))
            self.assertIs(kwargs["stdout"], recovery.subprocess.PIPE)
            output = Path(command[command.index("--output") + 1])
            output.write_bytes(b"{}")
            self.assertIn(b"Authorization: Bearer secret-token", kwargs["input"])
            return recovery.subprocess.CompletedProcess(command, 0, stdout=b"200")

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "response"
            with mock.patch.object(recovery.subprocess, "run", side_effect=fake_run):
                recovery._curl_download("https://api.github.test/evidence", "secret-token", output, limit=1024)

    def test_cross_host_redirect_never_receives_authorization(self) -> None:
        calls = 0

        def fake_run(command, **kwargs):
            nonlocal calls
            calls += 1
            output = Path(command[command.index("--output") + 1])
            if calls == 1:
                self.assertIs(kwargs["stdout"], recovery.subprocess.PIPE)
                self.assertIn(b"Authorization: Bearer secret-token", kwargs["input"])
                self.assertNotIn(b"\nlocation\n", kwargs["input"])
                headers = Path(command[command.index("--dump-header") + 1])
                headers.write_bytes(b"HTTP/1.1 302 Found\r\nLocation: https://storage.example.test/log\r\n\r\n")
                output.write_bytes(b"")
                return recovery.subprocess.CompletedProcess(command, 0, stdout=b"302")
            self.assertNotIn(b"Authorization", kwargs["input"])
            self.assertIn(b'url = "https://storage.example.test/log"', kwargs["input"])
            output.write_bytes(b"complete log")
            return recovery.subprocess.CompletedProcess(command, 0, stdout=b"")

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "response"
            with mock.patch.object(recovery.subprocess, "run", side_effect=fake_run):
                recovery._curl_download("https://api.github.test/logs", "secret-token", output, limit=1024)
            self.assertEqual(calls, 2)
            self.assertEqual(output.read_bytes(), b"complete log")

    def test_proof_is_private_and_contains_no_token(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "proof.json"
            recovery.write_proof(path, {"version": 1, "old_holder": "release/1-1"})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            payload = json.loads(path.read_text())
            self.assertNotIn("token", payload)


if __name__ == "__main__":
    unittest.main()
