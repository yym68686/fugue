import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from scripts import baked_postgres_contract as contract
from scripts.smoke_cnpg_baked_manager import Rehearsal


class BakedPostgresTests(unittest.TestCase):
    def test_rehearsal_refuses_production_contexts_before_writing(self):
        args = SimpleNamespace(revision="a" * 40, cluster_name="production", kubeconfig=Path("unused"))
        with self.assertRaisesRegex(ValueError, "disposable"):
            Rehearsal(args)
        args.cluster_name = "fugue-baked-" + "a" * 12
        with patch.object(Rehearsal, "kube", return_value={"clusters": [{"cluster": {"server": "https://production.invalid"}}]}) as read:
            with self.assertRaisesRegex(ValueError, "non-loopback"):
                Rehearsal(args)
            self.assertEqual(read.call_count, 1)

    def proof(self, suffix="a"):
        return {"image": "registry.invalid/postgres:18.3@sha256:" + suffix * 64,
                "manager_image": contract.MANAGER_IMAGE, "manager_sha256": contract.MANAGER_SHA256.copy(),
                "postgres_base": contract.POSTGRES_IMAGE, "platforms": ["linux/amd64", "linux/arm64"],
                "readonly_startup_verified": True, "upgrade_path_verified": True}

    def test_only_fully_verified_immutable_images_are_approved(self):
        first = contract.build_catalog(self.proof(), {})
        second = contract.build_catalog(self.proof("b"), {"baked_postgres_contract": first})
        self.assertEqual(second["images"], [first["defaultImage"], second["defaultImage"]])
        for field, value in (("image", "registry.invalid/postgres:18.3"), ("manager_sha256", {}),
                             ("platforms", ["linux/amd64"]), ("readonly_startup_verified", False), ("upgrade_path_verified", False)):
            invalid = dict(self.proof(), **{field: value})
            with self.subTest(field=field), self.assertRaises(ValueError):
                contract.build_catalog(invalid, {})
        bad = copy.deepcopy(first)
        bad["managerSHA256"]["manager_amd64"] = "f" * 64
        with self.assertRaisesRegex(ValueError, "explicit database upgrade"):
            contract.build_catalog(self.proof("b"), {"baked_postgres_contract": bad})

    def test_startup_links_once_preserves_upgrades_and_fails_closed(self):
        source = Path("scripts/postgres_baked_manager.sh").read_text()
        with tempfile.TemporaryDirectory(prefix="baked-entry-") as tmp:
            root = Path(tmp)
            manager, baked, entry = root / "manager", root / "baked", root / "entry"
            baked.write_text('#!/bin/sh\nprintf "baked:%s\\n" "$@"\n')
            baked.chmod(0o755)
            entry.write_text(source.replace("/controller/manager", str(manager)).replace("/usr/local/libexec/cnpg-manager", str(baked)))
            call = lambda: subprocess.run(["sh", str(entry), "two words"], capture_output=True, text=True)
            self.assertEqual(call().stdout, "baked:two words\n")
            self.assertTrue(manager.is_symlink())
            inode = manager.lstat().st_ino
            self.assertEqual(call().returncode, 0)
            self.assertEqual(manager.lstat().st_ino, inode)
            replacement = root / "new"
            replacement.write_text('#!/bin/sh\nprintf "upgraded:%s\\n" "$@"\n')
            replacement.chmod(0o755)
            replacement.replace(manager)
            self.assertEqual(call().stdout, "upgraded:two words\n")
            self.assertFalse(manager.is_symlink())
            manager.chmod(0o644)
            self.assertNotEqual(call().returncode, 0)
            manager.unlink()
            manager.symlink_to(root / "missing")
            self.assertNotEqual(call().returncode, 0)
            self.assertTrue(manager.is_symlink())
            manager.unlink()
            manager.mkdir()
            self.assertNotEqual(call().returncode, 0)


if __name__ == "__main__":
    unittest.main()
