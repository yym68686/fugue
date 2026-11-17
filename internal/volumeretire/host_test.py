import importlib.util
import json
import os
import pathlib
import tempfile
import unittest
from unittest import mock


spec = importlib.util.spec_from_file_location("retirement_host", pathlib.Path(__file__).with_name("host.py"))
host = importlib.util.module_from_spec(spec)
spec.loader.exec_module(host)


class RetirementHostTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = os.path.realpath(self.temporary.name) + "/storage"
        os.mkdir(self.root)
        self.path = self.root + "/volume-fixture"
        os.mkdir(self.path)
        pathlib.Path(self.path + "/data").write_bytes(b"retained data" * 4096)
        self.identity = {"hostname": "node-fixture", "machine_id": "machine-fixture", "boot_id": "boot-fixture"}
        self.addCleanup(mock.patch.stopall)
        mock.patch.object(host, "identity", return_value=self.identity).start()
        self.users = mock.patch.object(host, "active_users", return_value=[]).start()

    def plan(self):
        return {"path": self.path, "storage_root": self.root, "pv_uid": "pv-fixture-uid",
                "host": self.identity, "tree": host.scan_tree(self.path)}

    def request(self, plan=None):
        return {"plan": plan or self.plan(), "plan_sha256": "a" * 64}

    def test_inspect_is_read_only_and_counts_allocated_blocks(self):
        before = sorted(os.listdir(self.root))
        result = host.inspect({"path": self.path, "storage_root": self.root,
                               "hostname": self.identity["hostname"], "machine_id": self.identity["machine_id"]})
        self.assertEqual(before, sorted(os.listdir(self.root)))
        self.assertGreater(result["tree"]["allocated_bytes"], 0)
        self.assertEqual(result["tree"]["entry_count"], 2)
        self.assertEqual(result["tree"], host.scan_tree(self.path))

    def test_real_deletion_is_scoped_verified_and_idempotent(self):
        other = self.root + "/other-volume"
        os.mkdir(other)
        pathlib.Path(other + "/keep").write_text("keep")
        request = self.request()
        receipt = host.retire(request)
        self.assertEqual(receipt["state"], "data_deleted")
        self.assertTrue(receipt["verified_absent"])
        self.assertFalse(os.path.exists(self.path))
        self.assertEqual(pathlib.Path(other + "/keep").read_text(), "keep")
        self.assertEqual(host.retire(request)["deleted_allocated_bytes"], request["plan"]["tree"]["allocated_bytes"])
        saved = json.loads(pathlib.Path(self.root + "/.fugue-volume-retire-pv-fixture-uid.json").read_text())
        self.assertEqual(saved["plan_sha256"], "a" * 64)

    def test_changed_data_stops_before_quarantine(self):
        request = self.request()
        pathlib.Path(self.path + "/data").write_text("changed")
        with self.assertRaisesRegex(ValueError, "changed"):
            host.retire(request)
        self.assertTrue(os.path.isdir(self.path))

    def test_symlink_hardlink_and_special_files_fail_closed(self):
        for kind in ["symlink", "hardlink", "fifo"]:
            with self.subTest(kind=kind):
                target = self.path + "/bad"
                if kind == "symlink":
                    os.symlink(self.root, target)
                elif kind == "hardlink":
                    os.link(self.path + "/data", target)
                else:
                    os.mkfifo(target)
                try:
                    with self.assertRaisesRegex(ValueError, "linked, shared, special"):
                        host.scan_tree(self.path)
                finally:
                    os.unlink(target)

    def test_wrong_host_boot_or_broad_path_is_rejected(self):
        for key, value in [("path", self.root), ("storage_root", "/var"),
                           ("host", dict(self.identity, boot_id="changed"))]:
            with self.subTest(key=key):
                plan = self.plan()
                plan[key] = value
                with self.assertRaises(ValueError):
                    host.retire(self.request(plan))
                self.assertTrue(os.path.isdir(self.path))

    def test_open_files_or_mounts_block_retirement(self):
        self.users.return_value = ["open_file"]
        with self.assertRaisesRegex(ValueError, "active"):
            host.retire(self.request())
        self.assertTrue(os.path.isdir(self.path))

    def test_resumes_after_quarantine_and_partial_delete(self):
        request = self.request()
        real_remove = host.shutil.rmtree

        def interrupted(path):
            os.unlink(path + "/data")
            raise OSError("synthetic interruption")

        with mock.patch.object(host.shutil, "rmtree", side_effect=interrupted):
            with self.assertRaisesRegex(OSError, "synthetic interruption"):
                host.retire(request)
        self.assertFalse(os.path.exists(self.path))
        self.assertTrue(os.path.isdir(self.root + "/.fugue-volume-retired-pv-fixture-uid"))
        self.assertTrue(real_remove.avoids_symlink_attacks)
        self.assertTrue(host.retire(request)["verified_absent"])

    def test_recreated_original_path_is_never_deleted(self):
        request = self.request()
        host.retire(request)
        os.mkdir(self.path)
        pathlib.Path(self.path + "/new-data").write_text("new")
        with self.assertRaises(ValueError):
            host.retire(request)
        self.assertEqual(pathlib.Path(self.path + "/new-data").read_text(), "new")

    def test_mount_and_fd_aliases_from_other_namespaces(self):
        # Exercise the real /proc parser using a complete synthetic process.
        process = self.root + "/proc/123"
        os.makedirs(process + "/ns")
        os.mkdir(process + "/fd")
        os.mkdir(self.root + "/proc/self")
        pathlib.Path(self.root + "/proc/self/mountinfo").write_text("1 0 8:1 / " + self.root + " rw - ext4 /dev/test rw\n")
        pathlib.Path(process + "/ns/mnt").touch()
        pathlib.Path(process + "/mountinfo").write_text("1 0 8:1 " + self.path + " /app rw - ext4 /dev/test rw\n")
        os.symlink(self.path + "/data", process + "/fd/7")
        # The saved original function is independent of the mocked public name.
        original = self.real_active_users
        self.assertEqual(original([self.path], self.root + "/proc"), ["mounted_volume", "open_file"])
        pathlib.Path(process + "/mountinfo").write_text("1 0 8:1 /volume-fixture /app rw - ext4 /dev/test rw\n")
        self.assertEqual(original([self.path], self.root + "/proc"), ["mounted_volume", "open_file"])


RetirementHostTest.real_active_users = staticmethod(host.active_users)

if __name__ == "__main__":
    unittest.main()
