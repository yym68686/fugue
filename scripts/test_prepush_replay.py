import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import prepush, prepush_replay


class ReplayTest(unittest.TestCase):
    def test_candidate_keeps_non_selection_checks_and_cannot_write_production_receipt(self):
        results = []
        seen = []

        def command(args, _timeout):
            seen.append(args)
            return 0, ""

        selection = {"goTests": [["go", "test", "./internal/api"]], "goBuilds": []}
        with (
            mock.patch.dict(os.environ, {"PREPUSH_TIMEOUT_SECONDS": "5"}),
            mock.patch.object(prepush, "resolve_base", return_value="HEAD"),
            mock.patch.object(prepush, "changed_files", return_value=["internal/api/backup_usage_test.go"]),
            mock.patch.object(prepush, "diff_check", return_value=(0, "")),
            mock.patch.object(prepush, "gofmt_check", return_value=(0, "")),
            mock.patch.object(prepush, "shell_syntax_check", return_value=(0, "")),
            mock.patch.object(prepush, "git_output", return_value=b"1" * 40 + b"\n"),
            mock.patch.object(prepush, "run", side_effect=command),
            mock.patch.object(prepush, "write_receipt", side_effect=AssertionError("shadow must never issue a receipt")),
            mock.patch("sys.stderr", new=io.StringIO()),
        ):
            code = prepush._run_gate(shadow_select=lambda _: selection, shadow_result=results.append)
        self.assertEqual(code, 0)
        self.assertEqual([cmd for cmd in seen if cmd[:2] == ["go", "test"]], selection["goTests"])
        self.assertTrue(any(cmd[:3] == ["go", "run", "./cmd/fugue-openapi-gen"] for cmd in seen))
        self.assertIn(["go", "vet", "./internal/api"], seen)
        self.assertFalse(any(cmd[:2] == ["go", "build"] for cmd in seen))
        self.assertEqual(results[0]["checks"]["compile-all"]["status"], "skipped")
        self.assertNotIn("kind", results[0])

    def test_nearest_rank_and_failure_are_not_censored(self):
        candidate = {"status": "pass", "elapsedMs": 12_000}
        legacy = {"status": "pass", "elapsedMs": 30_000}
        pairs = [{"candidate": candidate, "legacy": legacy}] * 19
        selection = {"scope": "test-only", "omittedLegacyPackagesBeforeFallback": []}
        oracle = {"status": "pass"}
        self.assertFalse(prepush_replay.summarize(pairs, selection, oracle)["warmReplayCriteriaMet"])
        self.assertFalse(prepush_replay.summarize(pairs + [pairs[0]], selection, None)["warmReplayCriteriaMet"])
        report = prepush_replay.summarize(pairs + [pairs[0]], selection, oracle)
        self.assertTrue(report["warmReplayCriteriaMet"])
        self.assertEqual(report["activationReadiness"], "not-evaluated")
        failed = {"status": "fail", "elapsedMs": 55_000}
        report = prepush_replay.summarize(pairs + [{"candidate": failed, "legacy": legacy}], selection, oracle)
        self.assertEqual(report["candidateP95Ms"], 12_000)
        self.assertEqual(report["candidateFailedSamples"], 1)
        self.assertFalse(report["warmReplayCriteriaMet"])

    def test_full_oracle_collects_failure_packages_and_timeout(self):
        with mock.patch.object(prepush, "run", return_value=(
            124, '\n'.join([
                "unstructured output",
                '{"Package":"sample/contract","Action":"run","Test":"TestProjection"}',
                '{"Package":"sample/contract","Action":"fail","Test":"TestProjection"}',
                '{"Package":"sample/contract","Action":"fail"}',
            ]),
        )):
            result = prepush_replay.go_oracle(3)
        self.assertEqual(result, {
            "status": "fail", "failedPackages": ["sample/contract"],
            "packagesWithExecutedTests": 1, "timedOut": True,
        })

    def test_incremental_replay_isolates_both_caches_and_restores_fixture(self):
        selection = {
            "scope": "test-only", "paths": ["suite_test.go"], "goTests": [],
            "goBuilds": [], "omittedLegacyPackagesBeforeFallback": [],
        }
        with tempfile.TemporaryDirectory() as location:
            path = Path(location) / "suite_test.go"
            path.write_text("package demo\n", encoding="ascii")
            original = path.read_bytes()
            seen = {"legacy": [], "candidate": []}

            def sample(name):
                seen[name].append((os.environ["GOCACHE"], path.read_bytes()))
                return {"status": "pass", "elapsedMs": 12, "selection": dict(selection)}

            with (
                mock.patch.object(prepush, "ROOT", path.parent),
                mock.patch.object(prepush, "git_output", return_value=b"head\n"),
                mock.patch.object(prepush_replay.prepush_shadow, "git_paths", return_value=[]),
                mock.patch.object(prepush_replay.prepush_shadow, "snapshot", side_effect=lambda _base: path.read_text()),
                mock.patch.dict(prepush_replay.prepush_shadow_fixture.CASES, {"sample": (("suite_test.go", "", ""),)}),
                mock.patch.object(prepush_replay.prepush_shadow_fixture, "prepare",
                                  side_effect=lambda _case: path.write_bytes(original + b"// edited\n")),
                mock.patch.object(prepush_replay.subprocess, "check_output", return_value="{}"),
                mock.patch.object(prepush, "run", return_value=(0, "")),
                mock.patch.object(prepush_replay, "legacy_sample", side_effect=lambda: sample("legacy")),
                mock.patch.object(prepush_replay, "candidate_sample", side_effect=lambda: sample("candidate")),
                mock.patch.object(prepush_replay, "go_oracle", return_value={"status": "pass"}),
            ):
                report = prepush_replay.replay_incremental("sample", "HEAD", 2, 60)
            self.assertEqual(path.read_bytes(), original)
            legacy_cache, candidate_cache = [values[0][0] for values in seen.values()]
            self.assertNotEqual(legacy_cache, candidate_cache)
            self.assertEqual([data for _, data in seen["legacy"]], [data for _, data in seen["candidate"]])
            self.assertNotEqual(seen["legacy"][0][1], seen["legacy"][1][1])
            self.assertNotEqual(report["samples"][0]["sourceSnapshot"], report["samples"][1]["sourceSnapshot"])
            self.assertFalse(report["productionAuthorization"])


if __name__ == "__main__":
    unittest.main()
