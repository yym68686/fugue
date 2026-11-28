import contextlib
import io
import json
import os
import unittest
from unittest import mock

from scripts import prepush, prepush_local, prepush_shadow


class LocalVerificationTest(unittest.TestCase):
    def test_ci_and_receipt_environment_never_run_local_gate(self):
        for name in ("CI", "GITHUB_ACTIONS", "PREPUSH_RECEIPT"):
            with (
                self.subTest(name=name),
                mock.patch.dict(os.environ, {name: ""}, clear=True),
                mock.patch.object(prepush, "_run_gate") as gate,
                contextlib.redirect_stderr(io.StringIO()),
            ):
                self.assertEqual(prepush_local.main(), 2)
                gate.assert_not_called()

    def test_preserves_full_fallback_never_writes_receipt_and_rejects_source_drift(self):
        plan = prepush_shadow.full("unknown source ownership")

        def run_gate(*, shadow_select, shadow_result, shadow_build_first, shadow_full_timeout):
            self.assertTrue(shadow_build_first)
            self.assertEqual(shadow_full_timeout, 360)
            selected = shadow_select("HEAD")
            self.assertEqual(selected, plan)
            self.assertEqual(selected["goTests"], [["go", "test", "-count=1", "./..."]])
            shadow_result({"status": "pass", "selection": selected, "checks": {}, "elapsedLimitMs": 360000})
            return 0

        for last in ("original", "different"):
            stdout = io.StringIO()
            with (
                self.subTest(last=last),
                mock.patch.dict(os.environ, {}, clear=True),
                mock.patch.object(prepush, "resolve_base", return_value="HEAD"),
                mock.patch.object(prepush_shadow, "snapshot", side_effect=["original", last]),
                mock.patch.object(prepush_shadow, "conservative_report", return_value=plan),
                mock.patch.object(prepush, "_run_gate", side_effect=run_gate),
                mock.patch.object(prepush, "write_receipt", side_effect=AssertionError("unexpected receipt")),
                contextlib.redirect_stdout(stdout),
            ):
                self.assertEqual(prepush_local.main(), int(last != "original"))
                report = json.loads(stdout.getvalue())
                self.assertEqual(report["kind"], "LocalVerification")
                self.assertFalse(report["productionAuthorization"])
                self.assertEqual(report["status"], "pass" if last == "original" else "fail")

    def test_widened_budget_only_applies_to_the_full_local_plan(self):
        with self.assertRaisesRegex(ValueError, "non-authorizing"):
            prepush._run_gate(shadow_full_timeout=360)

        for scope, expected_limit in (("test-only", 60), ("full", 360)):
            with self.subTest(scope=scope):
                seen = {}
                reports = []
                plan = {"scope": scope, "goTests": [["go", "test", "-count=1", "./..."]], "goBuilds": []}

                def run(command, timeout):
                    seen[tuple(command)] = timeout
                    return 0, ""

                with (
                    mock.patch.dict(os.environ, {}, clear=True),
                    mock.patch.object(prepush, "resolve_base", return_value="HEAD"),
                    mock.patch.object(prepush, "changed_files", return_value=[]),
                    mock.patch.object(prepush, "diff_check", return_value=(0, "")),
                    mock.patch.object(prepush, "gofmt_check", return_value=(0, "")),
                    mock.patch.object(prepush, "shell_syntax_check", return_value=(0, "")),
                    mock.patch.object(prepush, "git_output", return_value=b"1" * 40 + b"\n"),
                    mock.patch.object(prepush, "run", side_effect=run),
                    mock.patch.object(prepush, "write_receipt", side_effect=AssertionError("unexpected receipt")),
                ):
                    self.assertEqual(prepush._run_gate(
                        shadow_select=lambda _: plan, shadow_result=reports.append, shadow_full_timeout=360,
                    ), 0)
                self.assertEqual(reports[0]["elapsedLimitMs"], expected_limit * 1000)
                test_timeout = seen[("go", "test", "-count=1", "./...")]
                if scope == "full":
                    self.assertGreater(test_timeout, 300)
                else:
                    self.assertLessEqual(test_timeout, 55)


if __name__ == "__main__":
    unittest.main()
