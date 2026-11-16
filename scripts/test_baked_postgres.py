import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from scripts import baked_postgres_contract as contract
from scripts.smoke_cnpg_baked_manager import Rehearsal, manifest_objects, verify_initdb_job
from scripts import verify_baked_postgres as verifier


class BakedPostgresTests(unittest.TestCase):
    def test_initialization_evidence_is_verified_before_job_garbage_collection(self):
        job = {"metadata": {"uid": "initialization-job"}, "spec": {"template": {"spec": {
            "containers": [{"command": ["/usr/local/bin/fugue-cnpg-manager", "instance", "init"]}]}}}}
        self.assertEqual(verify_initdb_job(job), "initialization-job")
        for change in ({"initContainers": [{"name": "bootstrap-controller"}]}, {"containers": []}):
            invalid = copy.deepcopy(job)
            invalid["spec"]["template"]["spec"].update(change)
            with self.assertRaisesRegex(ValueError, "observed initdb job"):
                verify_initdb_job(invalid)

    def test_installation_accepts_kubectl_json_stream_and_lists(self):
        items = [{"kind": "Namespace", "metadata": {"name": "operator"}}, {"kind": "Deployment", "metadata": {"name": "controller"}}]
        expected = {"apiVersion": "v1", "kind": "List", "items": items}
        self.assertEqual(manifest_objects("\n".join(json.dumps(i) for i in items)), expected)
        self.assertEqual(manifest_objects(json.dumps(expected)), expected)
        for invalid in ("", "[]", json.dumps(items[0]) + "trailing"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                manifest_objects(invalid)

    def test_platform_checks_pin_child_manifests_not_the_shared_index(self):
        image = "registry.invalid/postgres:18.3@sha256:" + "f" * 64
        entries = [{"platform": {"os": "linux", "architecture": arch}, "digest": "sha256:" + value * 64}
                   for arch, value in (("amd64", "a"), ("arm64", "b"), ("unknown", "c"))]
        with patch.object(verifier, "output", return_value=json.dumps({"manifests": entries})):
            for arch, value in (("amd64", "a"), ("arm64", "b")):
                self.assertEqual(verifier.platform_reference(image, arch), image.rsplit("@", 1)[0] + "@sha256:" + value * 64)
        for invalid in ([], [entries[0]], [entries[1], entries[1]], [dict(entries[1], digest="mutable")]):
            with self.subTest(manifests=invalid), patch.object(verifier, "output", return_value=json.dumps({"manifests": invalid})):
                with self.assertRaisesRegex(ValueError, "exactly one immutable"):
                    verifier.platform_reference(image, "arm64")

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
