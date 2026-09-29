import pathlib
import subprocess
import tempfile
import unittest

from scripts.prepare_static_caddy import apply_patches


class DependencyPatchIsolation(unittest.TestCase):
    def test_git_patch_applies_inside_dependency_not_enclosing_worktree(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = pathlib.Path(tmp)
            subprocess.run(["git", "init", "--quiet", str(repo)], check=True)
            target = repo / "generated" / "dependency"
            (target / "modules").mkdir(parents=True)
            (repo / "modules").mkdir()
            (repo / "modules" / "fixture").write_text("enclosing tree\n")
            source = target / "modules" / "fixture"
            source.write_text("before\n")
            patches = repo / "patches"
            patches.mkdir()
            (patches / "001.patch").write_text(
                "diff --git a/modules/fixture b/modules/fixture\n"
                "--- a/modules/fixture\n+++ b/modules/fixture\n"
                "@@ -1 +1 @@\n-before\n+after\n"
            )
            apply_patches(target, patches)
            self.assertEqual(source.read_text(), "after\n")
            self.assertEqual((repo / "modules" / "fixture").read_text(), "enclosing tree\n")
            with self.assertRaises(subprocess.CalledProcessError):
                apply_patches(target, patches)

    def test_patch_cannot_escape_dependency_tree(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            target, patches = root / "dependency", root / "patches"
            target.mkdir()
            patches.mkdir()
            outside = root / "outside"
            outside.write_text("preserved\n")
            (patches / "001.patch").write_text(
                "diff --git a/../outside b/../outside\n"
                "--- a/../outside\n+++ b/../outside\n"
                "@@ -1 +1 @@\n-preserved\n+changed\n"
            )
            with self.assertRaises(subprocess.CalledProcessError):
                apply_patches(target, patches)
            self.assertEqual(outside.read_text(), "preserved\n")


if __name__ == "__main__":
    unittest.main()
