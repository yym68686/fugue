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

        def run_gate(*, shadow_select, shadow_result, shadow_build_first):
            self.assertTrue(shadow_build_first)
            selected = shadow_select("HEAD")
            self.assertEqual(selected, plan)
            self.assertEqual(selected["goTests"], [["go", "test", "-count=1", "./..."]])
            shadow_result({"status": "pass", "selection": selected, "checks": {}})
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


if __name__ == "__main__":
    unittest.main()
