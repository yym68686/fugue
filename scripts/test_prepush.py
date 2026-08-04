import json
import io
import os
from pathlib import Path
import re
import tempfile
import threading
import time
import unittest
from unittest import mock

from scripts import prepush


class CanonicalReceiptTest(unittest.TestCase):
    def test_ci_release_classifier_matches_only_exact_prepush_paths(self) -> None:
        source = (Path(__file__).resolve().parent.parent / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        line = next(line.strip() for line in source.splitlines() if line.strip().startswith("if grep -Eq '"))
        expression = line.split("if grep -Eq '", 1)[1].split("' <<<", 1)[0]
        classifier = re.compile(expression)
        for path in ("scripts/prepush.py", "scripts/test_prepush.py"):
            self.assertIsNotNone(classifier.search(path), path)
        for path in (
            "scripts/prepush.sh",
            "scripts/prepush.py.bak",
            "scripts/prepush_extra.py",
            "scripts/subdir/prepush.py",
            "scripts/test_prepush.py.bak",
            "scripts/verify_registry_image.py",
        ):
            self.assertIsNone(classifier.search(path), path)

    def run_main_with_fake(self, fake_run, paths=None):
        receipts = []

        def capture_receipt(receipt):
            receipts.append(receipt)
            return b""

        with (
            mock.patch.dict(os.environ, {"PREPUSH_TIMEOUT_SECONDS": "5"}),
            mock.patch.object(prepush, "resolve_base", return_value="HEAD"),
            mock.patch.object(prepush, "changed_files", return_value=paths or ["cmd/fugue-api/main.go"]),
            mock.patch.object(prepush, "diff_check", return_value=(0, "")),
            mock.patch.object(prepush, "gofmt_check", return_value=(0, "")),
            mock.patch.object(prepush, "shell_syntax_check", return_value=(0, "")),
            mock.patch.object(prepush, "git_output", return_value=b"1" * 40 + b"\n"),
            mock.patch.object(prepush, "run", side_effect=fake_run),
            mock.patch.object(prepush, "write_receipt", side_effect=capture_receipt),
            mock.patch("sys.stderr", new=io.StringIO()),
        ):
            result = prepush.main()
        self.assertEqual(len(receipts), 1)
        return result, receipts[0]

    def test_cold_compile_and_non_go_tests_overlap(self) -> None:
        barrier = threading.Barrier(2)
        intervals = {}

        def fake_run(command, _timeout):
            key = None
            if command == ["go", "build", "-p", "4", "./..."]:
                key = "compile-all"
            elif command == ["python3", "-m", "unittest", "scripts.test_prepush"]:
                key = "prepush-receipt-tests"
            if key is None:
                return 0, ""
            started = time.monotonic()
            barrier.wait(timeout=2)
            ended = time.monotonic()
            intervals[key] = (started, ended)
            return 0, ""

        result, receipt = self.run_main_with_fake(
            fake_run,
            paths=["scripts/prepush.py"],
        )
        self.assertEqual(result, 0)
        self.assertLess(intervals["compile-all"][0], intervals["prepush-receipt-tests"][1])
        self.assertLess(intervals["prepush-receipt-tests"][0], intervals["compile-all"][1])
        self.assertEqual(receipt["checks"]["compile-all"]["status"], "pass")
        self.assertEqual(receipt["checks"]["prepush-receipt-tests"]["status"], "pass")

    def test_compile_and_affected_tests_overlap(self) -> None:
        barrier = threading.Barrier(2)
        intervals = {}

        def fake_run(command, _timeout):
            key = None
            if command == ["go", "build", "-p", "4", "./..."]:
                key = "compile-all"
            elif command[:2] == ["go", "test"]:
                key = "affected-tests"
            if key is None:
                return 0, ""
            started = time.monotonic()
            barrier.wait(timeout=2)
            ended = time.monotonic()
            intervals[key] = (started, ended)
            return 0, ""

        result, receipt = self.run_main_with_fake(fake_run)
        self.assertEqual(result, 0)
        self.assertLess(intervals["compile-all"][0], intervals["affected-tests"][1])
        self.assertLess(intervals["affected-tests"][0], intervals["compile-all"][1])
        self.assertEqual(receipt["checks"]["compile-all"]["status"], "pass")
        self.assertEqual(receipt["checks"]["affected-tests"]["status"], "pass")

    def test_other_go_tasks_start_only_after_compile_success(self) -> None:
        compile_finished = threading.Event()
        dependent_observations = []

        def fake_run(command, _timeout):
            if command == ["go", "build", "-p", "4", "./..."]:
                time.sleep(0.01)
                compile_finished.set()
                return 0, ""
            if command and command[0] == "go" and command[:2] != ["go", "test"]:
                dependent_observations.append(compile_finished.is_set())
            return 0, ""

        result, receipt = self.run_main_with_fake(fake_run)
        self.assertEqual(result, 0)
        self.assertEqual(len(dependent_observations), 2)
        self.assertTrue(all(dependent_observations))
        for name in ("affected-vet", "openapi-generated"):
            self.assertEqual(receipt["checks"][name]["status"], "pass")

    def test_helm_checks_start_only_after_compile_success(self) -> None:
        compile_finished = threading.Event()
        helm_observations = []

        def fake_run(command, _timeout):
            if command == ["go", "build", "-p", "4", "./..."]:
                time.sleep(0.01)
                compile_finished.set()
                return 0, ""
            if command[0] == "helm" or command[:3] == [
                "go", "test", "./cmd/fugue-release-domain-evidence",
            ]:
                helm_observations.append(compile_finished.is_set())
            return 0, ""

        result, receipt = self.run_main_with_fake(
            fake_run,
            paths=["deploy/helm/fugue/values.yaml"],
        )
        self.assertEqual(result, 0)
        self.assertEqual(helm_observations, [True, True, True, True])
        self.assertEqual(receipt["checks"]["helm-lint-render"]["status"], "pass")

    def test_compile_failure_does_not_start_helm_checks(self) -> None:
        helm_commands = []

        def fake_run(command, _timeout):
            if command == ["go", "build", "-p", "4", "./..."]:
                return 9, "compile failed"
            if command[0] == "helm":
                helm_commands.append(command)
            return 0, ""

        result, receipt = self.run_main_with_fake(
            fake_run,
            paths=["deploy/helm/fugue/values.yaml"],
        )
        self.assertEqual(result, 1)
        self.assertEqual(helm_commands, [])
        self.assertEqual(
            receipt["checks"]["helm-lint-render"],
            {"durationMs": 0, "status": "skipped"},
        )

    def test_compile_failure_does_not_start_go_dependent_tasks(self) -> None:
        dependent_commands = []

        def fake_run(command, _timeout):
            if command == ["go", "build", "-p", "4", "./..."]:
                return 9, "compile failed"
            if command and command[0] == "go" and command[:2] != ["go", "test"]:
                dependent_commands.append(command)
            return 0, ""

        result, receipt = self.run_main_with_fake(fake_run)
        self.assertEqual(result, 1)
        self.assertEqual(dependent_commands, [])
        self.assertEqual(receipt["status"], "fail")
        self.assertEqual(receipt["checks"]["compile-all"]["status"], "fail")
        self.assertEqual(receipt["checks"]["affected-tests"]["status"], "pass")
        for name in ("affected-vet", "openapi-generated"):
            self.assertEqual(receipt["checks"][name], {"durationMs": 0, "status": "skipped"})

    def test_telemetry_declarative_shell_change_selects_focused_contract(self) -> None:
        commands = []

        def fake_run(command, _timeout):
            commands.append(command)
            return 0, ""

        result, receipt = self.run_main_with_fake(
            fake_run,
            paths=["scripts/apply_telemetry_declarative.sh"],
        )
        self.assertEqual(result, 0)
        self.assertIn(
            ["bash", "./scripts/test_apply_telemetry_declarative.sh"],
            commands,
        )
        self.assertEqual(
            receipt["checks"]["telemetry-declarative-tests"]["status"],
            "pass",
        )

    def test_controller_declarative_shell_change_selects_focused_contract(self) -> None:
        for path in (
            "scripts/apply_controller_declarative.sh",
            "deploy/environments/production/controller/release.json",
        ):
            with self.subTest(path=path):
                commands = []
                controller_timeouts = []

                def fake_run(command, timeout):
                    commands.append(command)
                    if command == ["bash", "./scripts/test_apply_controller_declarative.sh"]:
                        controller_timeouts.append(timeout)
                    return 0, ""

                result, receipt = self.run_main_with_fake(fake_run, paths=[path])
                self.assertEqual(result, 0)
                self.assertIn(
                    ["bash", "./scripts/test_apply_controller_declarative.sh"],
                    commands,
                )
                self.assertEqual(
                    receipt["checks"]["controller-declarative-tests"]["status"],
                    "pass",
                )
                self.assertEqual(
                    controller_timeouts,
                    [prepush.CONTROLLER_DECLARATIVE_TIMEOUT_SECONDS],
                )

    def test_controller_declarative_runs_after_compile_and_affected_tests(self) -> None:
        intervals = {}

        def fake_run(command, _timeout):
            if command == ["go", "build", "-p", "4", "./..."]:
                name = "compile-all"
            elif command[:2] == ["go", "test"]:
                name = "affected-tests"
            elif command == ["bash", "./scripts/test_apply_controller_declarative.sh"]:
                name = "controller-declarative-tests"
            else:
                return 0, ""
            started = time.monotonic()
            if name != "controller-declarative-tests":
                time.sleep(0.02)
            intervals[name] = (started, time.monotonic())
            return 0, ""

        result, receipt = self.run_main_with_fake(
            fake_run,
            paths=[
                "deploy/environments/production/controller/release.json",
                "internal/example/example.go",
            ],
        )
        self.assertEqual(result, 0)
        controller_started = intervals["controller-declarative-tests"][0]
        self.assertLessEqual(intervals["compile-all"][1], controller_started)
        self.assertLessEqual(intervals["affected-tests"][1], controller_started)
        self.assertEqual(receipt["checks"]["controller-declarative-tests"]["status"], "pass")

    def test_test_only_package_selects_exact_current_tests(self) -> None:
        diff = b"\n".join(
            (
                b"@@ -2855 +2855 @@ func TestControlPlaneDeployRequiresInternalReleaseGate(t *testing.T) {",
                b"@@ -5699 +5700 @@ func TestControlPlaneReleaseConvergenceAuthorizationHarness(t *testing.T) {",
            )
        )
        with mock.patch.object(prepush, "unified_diff_for_path", return_value=diff):
            commands = prepush.affected_test_commands(
                "HEAD^", ["internal/platformsafety/release_workflow_test.go"]
            )
        self.assertEqual(
            commands,
            [[
                "go", "test", "./internal/platformsafety", "-run",
                "^(TestControlPlaneDeployRequiresInternalReleaseGate|"
                "TestControlPlaneReleaseConvergenceAuthorizationHarness)$",
            ]],
        )

    def test_helper_or_global_hunk_falls_back_to_full_package(self) -> None:
        for header in (
            b"@@ -12 +12 @@ func helper(t *testing.T) {",
            b"@@ -1 +1 @@",
        ):
            with self.subTest(header=header):
                with mock.patch.object(prepush, "unified_diff_for_path", return_value=header):
                    commands = prepush.affected_test_commands(
                        "HEAD^", ["internal/example/example_test.go"]
                    )
                self.assertEqual(commands, [["go", "test", "./internal/example"]])

    def test_non_test_go_change_runs_full_package(self) -> None:
        with mock.patch.object(prepush, "unified_diff_for_path") as unified:
            commands = prepush.affected_test_commands(
                "HEAD^", ["internal/releasedomain/control_plane_hotfix_adoption.go"]
            )
        unified.assert_not_called()
        self.assertEqual(commands, [["go", "test", "./internal/releasedomain"]])

    def test_near_miss_test_name_falls_back_to_full_package(self) -> None:
        diff = b"@@ -12 +12 @@ func Testhelper(t *testing.T) {"
        with mock.patch.object(prepush, "unified_diff_for_path", return_value=diff):
            commands = prepush.affected_test_commands(
                "HEAD^", ["internal/example/example_test.go"]
            )
        self.assertEqual(commands, [["go", "test", "./internal/example"]])

    def test_package_tests_overlap_and_aggregate_failures(self) -> None:
        barrier = threading.Barrier(2)
        intervals = {}

        def fake_run(command, _timeout):
            package = command[2]
            started = time.monotonic()
            barrier.wait(timeout=2)
            ended = time.monotonic()
            intervals[package] = (started, ended)
            if package == "./internal/a":
                return 7, "a failed"
            return 9, "b failed"

        commands = [
            ["go", "test", "./internal/a"],
            ["go", "test", "./internal/b"],
        ]
        with mock.patch.object(prepush, "run", side_effect=fake_run):
            status, output = prepush.run_test_commands(commands, time.monotonic() + 2)
        self.assertEqual(status, 7)
        self.assertLess(intervals["./internal/a"][0], intervals["./internal/b"][1])
        self.assertLess(intervals["./internal/b"][0], intervals["./internal/a"][1])
        self.assertIn("$ go test ./internal/a\na failed", output)
        self.assertIn("$ go test ./internal/b\nb failed", output)

    def test_parallel_task_failure_still_fails_closed(self) -> None:
        controller_commands = []

        def fake_run(command, _timeout):
            if command == ["go", "build", "-p", "4", "./..."]:
                return 9, "compile failed"
            if command == ["bash", "./scripts/test_apply_controller_declarative.sh"]:
                controller_commands.append(command)
            return 0, ""

        result, receipt = self.run_main_with_fake(
            fake_run,
            paths=["deploy/environments/production/controller/release.json"],
        )
        self.assertEqual(result, 1)
        self.assertEqual(receipt["status"], "fail")
        self.assertEqual(receipt["checks"]["compile-all"]["status"], "fail")
        self.assertEqual(
            receipt["checks"]["controller-declarative-tests"],
            {"durationMs": 0, "status": "skipped"},
        )
        self.assertEqual(controller_commands, [])

    def test_non_go_telemetry_failure_still_fails_closed(self) -> None:
        def fake_run(command, _timeout):
            if command == ["bash", "./scripts/test_apply_telemetry_declarative.sh"]:
                return 8, "telemetry failed"
            return 0, ""

        result, receipt = self.run_main_with_fake(
            fake_run,
            paths=["scripts/apply_telemetry_declarative.sh"],
        )
        self.assertEqual(result, 1)
        self.assertEqual(receipt["status"], "fail")
        self.assertEqual(receipt["checks"]["compile-all"]["status"], "pass")
        self.assertEqual(receipt["checks"]["telemetry-declarative-tests"]["status"], "fail")

    def test_non_go_controller_failure_still_fails_closed(self) -> None:
        def fake_run(command, timeout):
            if command == ["bash", "./scripts/test_apply_controller_declarative.sh"]:
                self.assertEqual(timeout, prepush.CONTROLLER_DECLARATIVE_TIMEOUT_SECONDS)
                return 124, f"command exceeded {timeout:.1f}s"
            return 0, ""

        result, receipt = self.run_main_with_fake(
            fake_run,
            paths=["scripts/apply_controller_declarative.sh"],
        )
        self.assertEqual(result, 1)
        self.assertEqual(receipt["status"], "fail")
        self.assertEqual(receipt["checks"]["compile-all"]["status"], "pass")
        self.assertEqual(receipt["checks"]["controller-declarative-tests"]["status"], "fail")

    def test_default_deadline_and_receipt_schema_are_unchanged(self) -> None:
        self.assertEqual(prepush.DEFAULT_TIMEOUT_SECONDS, 55.0)
        self.assertEqual(prepush.DEFAULT_MAX_ELAPSED_SECONDS, 60.0)
        self.assertEqual(prepush.CONTROLLER_DECLARATIVE_TIMEOUT_SECONDS, 120.0)
        self.assertEqual(
            prepush.task_timeout_seconds("controller-declarative-tests", 55.0),
            120.0,
        )
        for name in ("compile-all", "affected-tests", "telemetry-declarative-tests"):
            self.assertEqual(prepush.task_timeout_seconds(name, 55.0), 55.0)
        self.assertEqual(prepush.elapsed_timeout_seconds({"compile-all"}), 60.0)
        self.assertEqual(
            prepush.elapsed_timeout_seconds({"compile-all", "controller-declarative-tests"}),
            120.0,
        )
        self.assertFalse(prepush.elapsed_timeout_exceeded({"compile-all"}, 59_999))
        self.assertTrue(prepush.elapsed_timeout_exceeded({"compile-all"}, 60_000))
        controller_checks = {"compile-all", "controller-declarative-tests"}
        self.assertFalse(prepush.elapsed_timeout_exceeded(controller_checks, 120_000))
        self.assertTrue(prepush.elapsed_timeout_exceeded(controller_checks, 120_001))

        result, receipt = self.run_main_with_fake(lambda _command, _timeout: (0, ""))
        self.assertEqual(result, 0)
        self.assertEqual(
            set(receipt),
            {
                "apiVersion", "baseCommit", "changedFilesDigest", "checks",
                "elapsedMs", "kind", "sourceCommit", "status",
            },
        )

    def test_command_environment_preserves_default_and_explicit_go_cache(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertNotIn("GOCACHE", prepush.command_env())
        explicit = "/tmp/fugue-explicit-go-cache"
        with mock.patch.dict(os.environ, {"GOCACHE": explicit}, clear=True):
            self.assertEqual(prepush.command_env()["GOCACHE"], explicit)

    def test_rejects_tuple_readback_for_json_array(self) -> None:
        with self.assertRaisesRegex(TypeError, "tuple"):
            prepush.validate_json_value({"images": ("api", "edge")}, "$")
        prepush.validate_json_value({"images": ["api", "edge"]}, "$")
        self.assertFalse(
            prepush.exact_json_types_equal(
                {"images": ["api", "edge"]},
                {"images": ("api", "edge")},
            )
        )

    def test_writer_round_trips_array_type_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "receipt.json")
            with mock.patch.dict(os.environ, {"PREPUSH_RECEIPT": path}):
                with self.assertRaisesRegex(TypeError, "tuple"):
                    prepush.write_receipt({"images": ("api", "edge")})
                self.assertFalse(os.path.exists(path))
                encoded = prepush.write_receipt({"images": ["api", "edge"]})
            with open(path, "rb") as handle:
                persisted = handle.read()
            self.assertEqual(encoded, persisted)
            self.assertEqual(json.loads(persisted), {"images": ["api", "edge"]})


if __name__ == "__main__":
    unittest.main()
