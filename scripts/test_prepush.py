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

    def test_compile_and_affected_tests_overlap(self) -> None:
        barrier = threading.Barrier(2)
        intervals = {}

        def fake_run(command, _timeout):
            key = None
            if command == ["go", "build", "./..."]:
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
        def fake_run(command, _timeout):
            if command == ["go", "build", "./..."]:
                return 9, "compile failed"
            return 0, ""

        result, receipt = self.run_main_with_fake(fake_run)
        self.assertEqual(result, 1)
        self.assertEqual(receipt["status"], "fail")
        self.assertEqual(receipt["checks"]["compile-all"]["status"], "fail")

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
