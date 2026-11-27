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
        "GoFiles": ["impl.go"], "TestGoFiles": ["impl_test.go"], **fields,
    }


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
            patch.object(shadow, "packages_from_stream", return_value=[pkg("domain")]),
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
                self.assertIn("projection drift", result.stdout)


if __name__ == "__main__":
    unittest.main()
