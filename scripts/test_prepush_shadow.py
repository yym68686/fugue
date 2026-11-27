import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from scripts import prepush, prepush_shadow as shadow


ROOT = Path("/checkout")


def pkg(path, **fields):
    return {
        "Dir": str(ROOT / path), "ImportPath": "demo/" + path,
        "GoFiles": ["impl.go"], "TestGoFiles": ["impl_test.go"],
        **fields,
    }


def repository_consumers():
    return [pkg(path.removeprefix("./")) for path in sorted(shadow.REPOSITORY_TEST_CONSUMERS)]


class SelectionTest(unittest.TestCase):
    def setUp(self):
        self.graph = [
            pkg("domain"),
            pkg("facade", Imports=["demo/domain"]),
            pkg("cmd", Imports=["demo/facade"]),
            pkg("contract", XTestImports=["demo/facade"]),
            pkg("extra", TestImports=["demo/domain"]),
            pkg("unrelated"),
        ]

    def test_runtime_reverse_closure_includes_internal_and_external_test_imports(self):
        result = shadow.select(["domain/impl.go"], self.graph, ROOT)
        self.assertEqual(result["goTests"], [
            ["go", "test", "./cmd", "./contract", "./domain", "./extra", "./facade"],
        ])
        self.assertEqual(result["goBuilds"], [["go", "build", "./cmd", "./domain", "./facade"]])

    def test_test_only_does_not_rebuild_or_retest_consumers(self):
        result = shadow.select(["facade/impl_test.go"], self.graph, ROOT)
        self.assertEqual(result["scope"], "test-only")
        self.assertEqual(result["goTests"], [["go", "test", "./facade"]])
        self.assertEqual(result["goBuilds"], [])

    def test_embed_ownership_and_single_package_build(self):
        result = shadow.select(["files/template.txt"], [pkg("files", EmbedFiles=["template.txt"])], ROOT)
        self.assertEqual(result["goTests"], [["go", "test", "./files"]])
        self.assertEqual(result["goBuilds"], [["go", "build", "-o", os.devnull, "./files"]])

    def test_compile_inputs_are_runtime_inputs_and_dependency_errors_fail_wide(self):
        for field in ("CXXFiles", "HFiles", "SFiles", "SysoFiles"):
            graph = [pkg("domain", **{field: ["native"]}), pkg("cmd", Imports=["demo/domain"])]
            with self.subTest(field=field):
                self.assertEqual(shadow.select(["domain/native"], graph, ROOT)["goBuilds"], [
                    ["go", "build", "./cmd", "./domain"],
                ])
        for field in ("Error", "DepsErrors"):
            with self.subTest(field=field):
                graph = [pkg("domain", **{field: {"Err": "missing input"}})]
                self.assertEqual(shadow.select(["domain/impl.go"], graph, ROOT)["scope"], "full")

    def test_unknown_deleted_cross_package_or_incomplete_graph_fail_wide(self):
        for paths in (["go.mod"], ["domain/deleted.go"], ["assets/fixture.json"],
                      ["domain/impl.go", "unrelated/impl.go"], []):
            with self.subTest(paths=paths):
                self.assertEqual(shadow.select(paths, self.graph, ROOT)["scope"], "full")
        self.assertEqual(shadow.select(["domain/impl.go"], [pkg("domain", Incomplete=True)], ROOT)["scope"], "full")

    def test_omitted_legacy_suite_is_visible_and_forces_full_go(self):
        with (
            patch.object(shadow, "git_paths", return_value=["domain/impl.go"]),
            patch.object(prepush, "changed_files", return_value=["domain/impl.go"]),
            patch.object(prepush, "affected_test_commands", return_value=[["go", "test", "./other", "-run", "^TestX$"]]),
            patch.object(prepush, "run", return_value=(0, "")),
            patch.object(shadow, "packages_from_stream", return_value=[pkg("domain"), *repository_consumers()]),
            patch.object(prepush, "ROOT", ROOT),
            patch.object(prepush, "git_output", return_value=b"abc\n"),
        ):
            result = shadow.report("HEAD")
        self.assertEqual(result["scope"], "full")
        self.assertEqual(result["omittedLegacyPackagesBeforeFallback"], ["./other"])
        self.assertEqual(result["gate"], "report-only")

    def test_git_diffs_include_both_rename_sides_and_untracked_paths(self):
        with tempfile.TemporaryDirectory() as location:
            directory = Path(location)

            def git(*args):
                subprocess.run(["git", *args], cwd=directory, capture_output=True, check=True)

            git("init")
            (directory / "old.go").write_text("package example\n")
            git("add", "old.go")
            git("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "base")
            git("mv", "old.go", "new.go")
            (directory / "fixture.txt").touch()
            with patch.object(prepush, "ROOT", directory):
                self.assertEqual(shadow.git_paths("HEAD"), ["fixture.txt", "new.go", "old.go"])

    def test_live_go_graph_catches_a_transitive_test_only_contract_regression(self):
        with tempfile.TemporaryDirectory() as location:
            root = Path(location)
            files = {
                "go.mod": "module test.example/fugue-shadow\n\ngo 1.22\n",
                "domain/core.go": 'package domain\nfunc Value() string { return "stable" }\n',
                "facade/adapter.go": (
                    'package facade\nimport "test.example/fugue-shadow/domain"\n'
                    'func Value() string { return domain.Value() }\n'
                ),
                "contract/contract.go": "package contract\n",
                "contract/contract_test.go": (
                    'package contract_test\nimport ("testing"; "test.example/fugue-shadow/facade")\n'
                    'func TestProjectionContract(t *testing.T) { '
                    'if facade.Value() != "stable" { t.Fatal("projection drift") } }\n'
                ),
                "unrelated/unrelated.go": "package unrelated\n",
            }
            for name, contents in files.items():
                path = root / name
                path.parent.mkdir(exist_ok=True)
                path.write_text(contents)

            def run(command):
                return subprocess.run(command, cwd=root, text=True, capture_output=True, timeout=45, check=False)

            baseline = run(["go", "test", "./..."])
            self.assertEqual(baseline.returncode, 0, baseline.stderr)
            changed = root / "domain/core.go"
            changed.write_text(files["domain/core.go"].replace('"stable"', '"drifted"'))
            graph = run(["go", "list", "-json=" + ",".join(shadow.FIELDS), "./..."])
            self.assertEqual(graph.returncode, 0, graph.stderr)
            plan = shadow.select(["domain/core.go"], shadow.packages_from_stream(graph.stdout), root)
            self.assertEqual(plan["scope"], "dependency-closure")
            self.assertEqual(plan["goTests"], [["go", "test", "./contract", "./domain", "./facade"]])
            self.assertEqual(plan["goBuilds"], [["go", "build", "./domain", "./facade"]])
            witness = run(plan["goTests"][0])
            oracle = run(["go", "test", "-count=1", "./..."])
            for result in (witness, oracle):
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("projection drift", result.stdout, result.stderr)

    def test_literal_source_reader_is_selected_but_is_not_a_build_target(self):
        path = "internal/observability/pipeline.go"
        owner = pkg("internal/observability")
        owner["GoFiles"] = ["pipeline.go"]
        reader = pkg("internal/platformsafety")
        # There is deliberately no import in either direction.
        plan = shadow.select([path], [owner, reader], ROOT)
        self.assertEqual(plan["goTests"], [
            ["go", "test", "./internal/observability"],
            ["go", "test", "-count=1", "./internal/platformsafety"],
        ])
        self.assertEqual(plan["goBuilds"], [["go", "build", "-o", os.devnull, "./internal/observability"]])
        self.assertEqual(shadow.select([path], [owner], ROOT)["scope"], "full")
        unrelated = shadow.select(["internal/observability/impl_test.go"], [owner, reader], ROOT)
        self.assertEqual(unrelated["goTests"], [["go", "test", "./internal/observability"]])

    def test_repository_reader_covers_runtime_and_test_only_changes_uncached(self):
        audits = repository_consumers()
        owner = pkg("domain")
        for name, scope, builds in [
            ("impl.go", "dependency-closure", [["go", "build", "-o", os.devnull, "./domain"]]),
            ("impl_test.go", "test-only", []),
        ]:
            with self.subTest(name=name):
                path = f"domain/{name}"
                kwargs = {"repository_tests": shadow.REPOSITORY_TEST_CONSUMERS}
                plan = shadow.select([path], [owner, *audits], ROOT, **kwargs)
                self.assertEqual(plan["scope"], scope)
                self.assertEqual(plan["goTests"], [
                    ["go", "test", "./domain"],
                    ["go", "test", "-count=1", "./internal/declarativerelease", "./internal/platformsafety"],
                ])
                self.assertEqual(plan["goBuilds"], builds)
                self.assertEqual(shadow.select([path], [owner, *audits[1:]], ROOT, **kwargs)["scope"], "full")

    def test_report_keeps_repository_audit_when_the_go_graph_has_no_edge(self):
        with (
            patch.object(shadow, "git_paths", return_value=["domain/impl_test.go"]),
            patch.object(prepush, "changed_files", return_value=["domain/impl_test.go"]),
            patch.object(prepush, "affected_test_commands", return_value=[]),
            patch.object(prepush, "run", return_value=(0, "")),
            patch.object(shadow, "packages_from_stream", return_value=[pkg("domain"), *repository_consumers()]),
            patch.object(prepush, "ROOT", ROOT),
            patch.object(prepush, "git_output", return_value=b"abc\n"),
        ):
            plan = shadow.report("HEAD")
        self.assertEqual(plan["scope"], "test-only")
        self.assertIn(
            ["go", "test", "-count=1", "./internal/declarativerelease", "./internal/platformsafety"],
            plan["goTests"],
        )
        self.assertEqual(plan["goBuilds"], [])

    def test_image_tracking_guard_is_a_runtime_source_edge_without_a_build_edge(self):
        audit = pkg("internal/imagetracking")
        options = {"application_source_tests": shadow.APPLICATION_SOURCE_TEST_CONSUMERS}
        for path in ("internal/unrelated", "cmd/unrelated"):
            with self.subTest(path=path):
                owner = pkg(path)
                plan = shadow.select([path + "/impl.go"], [owner, audit], ROOT, **options)
                self.assertEqual(plan["goTests"], [
                    ["go", "test", "./" + path],
                    ["go", "test", "-count=1", "./internal/imagetracking"],
                ])
                self.assertEqual(plan["goBuilds"], [["go", "build", "-o", os.devnull, "./" + path]])
                self.assertEqual(shadow.select([path + "/impl.go"], [owner], ROOT, **options)["scope"], "full")
                self.assertEqual(shadow.select([path + "/impl_test.go"], [owner], ROOT, **options)["scope"], "test-only")

    def test_report_installs_the_image_tracking_source_edge(self):
        with (
            patch.object(shadow, "git_paths", return_value=["internal/anotherdomain/impl.go"]),
            patch.object(prepush, "changed_files", return_value=[]),
            patch.object(prepush, "run", return_value=(0, "")),
            patch.object(shadow, "packages_from_stream", return_value=[
                pkg("internal/anotherdomain"), pkg("internal/imagetracking"), *repository_consumers(),
            ]),
            patch.object(prepush, "ROOT", ROOT),
            patch.object(prepush, "git_output", return_value=b"abc\n"),
        ):
            plan = shadow.report("HEAD")
        self.assertEqual(plan["scope"], "dependency-closure")
        self.assertIn("./internal/imagetracking", plan["goTests"][-1])
        self.assertIn("-count=1", plan["goTests"][-1])
        self.assertNotIn("./internal/imagetracking", plan["goBuilds"][0])

    def test_tool_dependency_reaches_the_command_contract_without_an_import(self):
        dep = pkg("render")
        tool = pkg("cmd/fugue-openapi-gen", Imports=["demo/render"])
        contract = pkg("internal/apispec")
        plan = shadow.select(["render/impl.go"], [dep, tool, contract], ROOT)
        self.assertEqual(plan["goTests"], [
            ["go", "test", "./cmd/fugue-openapi-gen", "./render"],
            ["go", "test", "-count=1", "./internal/apispec"],
        ])
        self.assertEqual(plan["goBuilds"], [["go", "build", "./cmd/fugue-openapi-gen", "./render"]])
        self.assertEqual(shadow.select(["render/impl.go"], [dep, tool], ROOT)["scope"], "full")

    def test_go_test_binary_contract_includes_fixture_test_sources_and_test_imports(self):
        probe = pkg("probe")
        fixture = pkg("internal/sshexecutor", TestImports=["demo/probe"])
        consumer = pkg("internal/sshkube")
        helper = pkg("cmd/fugue-ssh-helper")
        graph = [probe, fixture, consumer, helper]
        for path, owner in (
            ("internal/sshexecutor/impl_test.go", "./internal/sshexecutor"),
            ("probe/impl.go", "./probe"),
            ("cmd/fugue-ssh-helper/impl.go", "./cmd/fugue-ssh-helper"),
        ):
            with self.subTest(path=path):
                plan = shadow.select([path], graph, ROOT)
                self.assertIn(owner, plan["goTests"][0])
                self.assertEqual(plan["goTests"][-1], [
                    "go", "test", "-count=1", "./internal/sshkube",
                ])
                self.assertFalse(any("./internal/sshkube" in cmd for cmd in plan["goBuilds"]))
                self.assertEqual(shadow.select([path], [probe, fixture, helper], ROOT)["scope"], "full")
        self.assertEqual(shadow.select(["probe/impl_test.go"], graph, ROOT)["goTests"], [
            ["go", "test", "./probe"],
        ])

    def test_live_target_comparison_fails_wide_when_build_tags_change_consumers(self):
        with tempfile.TemporaryDirectory() as location:
            root = Path(location)
            files = {
                "go.mod": "module test.example/target-audit\n\ngo 1.22\n",
                "domain/domain.go": "package domain\nfunc Value() int { return 1 }\n",
                "consumer/base.go": "package consumer\n",
                "consumer/uses_windows.go": (
                    'package consumer\nimport "test.example/target-audit/domain"\n'
                    'var value = domain.Value()\n'
                ),
            }
            for name, contents in files.items():
                path = root / name
                path.parent.mkdir(exist_ok=True)
                path.write_text(contents)

            def git(*args):
                subprocess.run(["git", *args], cwd=root, capture_output=True, check=True)

            git("init")
            git("add", ".")
            git("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "base")
            (root / "domain/domain.go").write_text(files["domain/domain.go"].replace("return 1", "return 2"))
            with patch.object(prepush, "ROOT", root), patch.object(shadow, "REPOSITORY_TEST_CONSUMERS", set()):
                result = shadow.compare_targets("HEAD", ["linux/amd64", "windows/amd64", "linux/amd64"])
            self.assertEqual(result["disposition"], "full-verification-required")
            self.assertFalse(result["productionAuthorization"])
            linux, windows = result["comparisons"]
            self.assertEqual(linux["profile"]["CGO_ENABLED"], "0")
            self.assertNotIn("./consumer", linux["selection"]["goTests"][0])
            self.assertIn("./consumer", windows["selection"]["goTests"][0])
            self.assertTrue(linux["differencesFromNative"] or windows["differencesFromNative"])

    def test_target_comparison_rejects_content_changes_even_if_paths_are_unchanged(self):
        env = '{"GOVERSION":"go1.25.7","GOOS":"darwin","GOARCH":"arm64","CGO_ENABLED":"1","GOFLAGS":""}'
        unchanged_plan = {
            "scope": "test-only", "baseCommit": "base", "sourceCommit": "source",
            "paths": ["p/p_test.go"], "goTests": [], "goBuilds": [],
            "omittedLegacyPackagesBeforeFallback": [],
        }
        with (
            patch.object(prepush, "git_output", return_value=b"base\n"),
            patch.object(prepush, "run", return_value=(0, env)),
            patch.object(shadow, "snapshot", side_effect=["original", "changed"]),
            patch.object(shadow, "report", return_value=unchanged_plan),
        ):
            with self.assertRaisesRegex(RuntimeError, "source changed during target comparison"):
                shadow.compare_targets("base", ["linux/amd64"])


if __name__ == "__main__":
    unittest.main()
