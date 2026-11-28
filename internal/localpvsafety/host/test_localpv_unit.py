import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest
from unittest import mock

import localpv_unit as unit


class LocalPVUnitTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / unit.UNIT
        self.image = "/srv/storage/test-vg.img"
        self.vg = "test-vg"
        self.old = unit.render(self.image, self.vg, legacy=True)
        self.desired = unit.render(self.image, self.vg)
        self.path.write_text(self.old)
        self.loaded = self.command(self.old)
        self.calls = []
        self.dropins = ""
        self.enabled = "enabled"
        self.active = "active"
        self.result = "success"
        self.need_reload = "no"
        self.load_state = "loaded"
        self.fragment = str(self.path)
        self.fail_verify = False
        self.fail_reload = False
        self.ineffective_reload = False
        self.concurrent_edit = None
        self.concurrent_after_install = None
        patcher = mock.patch.object(unit, "run", self.run_command)
        patcher.start()
        self.addCleanup(patcher.stop)

    def command(self, raw):
        return shlex.split(unit.parse_unit(raw)["Service"]["ExecStart"])[2]

    def run_command(self, argv):
        self.calls.append(argv)
        if argv[:2] == ["systemd-analyze", "verify"]:
            self.assertEqual(Path(argv[2]).read_text(), self.desired)
            if self.fail_verify:
                raise RuntimeError("verification failed")
            if self.concurrent_edit is not None:
                self.path.write_text(self.concurrent_edit)
            return ""
        if argv == ["systemctl", "daemon-reload"]:
            if self.concurrent_after_install is not None:
                self.path.write_text(self.concurrent_after_install)
                raise RuntimeError("concurrent edit")
            if self.fail_reload:
                self.fail_reload = False
                raise RuntimeError("reload failed")
            self.loaded = self.command(self.path.read_text())
            if self.ineffective_reload:
                self.loaded = "unrecognized override"
                self.ineffective_reload = False
            self.need_reload = "no"
            return ""
        if argv[:2] == ["systemctl", "show"]:
            props = {
                "LoadState": self.load_state,
                "FragmentPath": self.fragment,
                "DropInPaths": self.dropins,
                "NeedDaemonReload": self.need_reload,
                "ExecStart": "{ path=/bin/sh ; argv[]=/bin/sh -ec " + self.loaded + " ; ignore_errors=no ; start_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }",
                "UnitFileState": self.enabled,
                "ActiveState": self.active,
                "Result": self.result,
            }
            return "\n".join(f"{key}={value}" for key, value in props.items())
        self.fail("unexpected command, including any storage operation: " + repr(argv))

    def test_migrates_known_old_unit_without_touching_storage(self):
        result = unit.reconcile(self.path, "apply")
        self.assertEqual(result["state"], "converged")
        self.assertEqual(result["changed"], "true")
        self.assertEqual(result["image_path"], self.image)
        self.assertEqual(result["vg_name"], self.vg)
        self.assertEqual(result["file_sha256"], result["desired_sha256"])
        self.assertEqual(result["effective_sha256"], unit.digest(self.command(self.desired)))
        self.assertEqual(self.path.read_text(), self.desired)
        self.assertEqual(self.path.with_name(unit.UNIT + ".fugue-previous").read_text(), self.old)
        calls = list(self.calls)
        result = unit.reconcile(self.path, "apply")
        self.assertEqual(result["changed"], "false")
        self.assertEqual(self.calls.count(["systemctl", "daemon-reload"]), 1)
        self.assertEqual(len(self.calls), len(calls) + 1)

    def test_migrates_previous_fixed_unit_and_preserves_intent(self):
        self.path.write_text(unit.render(self.image, self.vg, legacy=False))
        self.loaded = self.command(self.path.read_text())
        self.assertEqual(unit.reconcile(self.path, "apply")["state"], "converged")

    def test_file_already_current_but_loaded_state_is_old(self):
        self.path.write_text(self.desired)
        self.need_reload = "yes"
        result = unit.reconcile(self.path, "apply")
        self.assertEqual(result["state"], "converged")
        self.assertEqual(self.loaded, self.command(self.desired))
        self.assertFalse(self.path.with_name(unit.UNIT + ".fugue-previous").exists())

    def test_observe_reports_drift_without_writes(self):
        result = unit.reconcile(self.path, "observe")
        self.assertEqual(result["state"], "drift")
        self.assertEqual(self.path.read_text(), self.old)
        self.assertTrue(all(call[:2] == ["systemctl", "show"] for call in self.calls))

    def test_missing_unit_does_not_enable_storage_on_unconfigured_node(self):
        self.path.unlink()
        self.assertEqual(unit.reconcile(self.path, "apply")["state"], "not_applicable")
        self.assertEqual(self.calls, [])

    def test_override_is_reported_and_preserved(self):
        self.dropins = "/etc/systemd/system/example.service.d/custom.conf"
        result = unit.reconcile(self.path, "apply")
        self.assertEqual(result["state"], "override_conflict")
        self.assertEqual(result["drop_in_paths"], self.dropins)
        self.assertEqual(self.path.read_text(), self.old)
        self.assertNotIn(["systemctl", "daemon-reload"], self.calls)

    def test_unknown_effective_command_is_not_healthy(self):
        self.path.write_text(self.desired)
        self.loaded = "another command"
        self.assertEqual(unit.reconcile(self.path, "observe")["state"], "drift")

    def test_custom_base_unit_is_never_overwritten(self):
        custom = self.old.replace("Type=oneshot", "Type=oneshot\nExecStartPre=/bin/true")
        self.path.write_text(custom)
        with self.assertRaisesRegex(ValueError, "customizations"):
            unit.reconcile(self.path, "apply")
        self.assertEqual(self.path.read_text(), custom)
        self.assertEqual(self.calls, [])

    def test_symlink_and_writable_unit_refused(self):
        other = self.path.with_suffix(".other")
        self.path.rename(other)
        self.path.symlink_to(other)
        with self.assertRaisesRegex(ValueError, "regular file"):
            unit.reconcile(self.path, "apply")
        self.path.unlink()
        other.rename(self.path)
        self.path.chmod(0o666)
        with self.assertRaisesRegex(ValueError, "write access"):
            unit.reconcile(self.path, "apply")

    def test_masked_and_different_fragment_refused(self):
        self.load_state = "masked"
        with self.assertRaisesRegex(ValueError, "masked"):
            unit.reconcile(self.path, "apply")
        self.load_state = "loaded"
        self.fragment = "/usr/lib/systemd/system/another.service"
        with self.assertRaisesRegex(ValueError, "different fragment"):
            unit.reconcile(self.path, "apply")

    def test_bad_candidate_never_replaces_old_file(self):
        self.fail_verify = True
        with self.assertRaisesRegex(RuntimeError, "verification failed"):
            unit.reconcile(self.path, "apply")
        self.assertEqual(self.path.read_text(), self.old)
        self.assertNotIn(["systemctl", "daemon-reload"], self.calls)

    def test_reload_failure_rolls_back_existing_file(self):
        self.fail_reload = True
        with self.assertRaisesRegex(RuntimeError, "reload failed"):
            unit.reconcile(self.path, "apply")
        self.assertEqual(self.path.read_text(), self.old)
        self.assertEqual(self.loaded, self.command(self.old))
        self.assertEqual(self.calls.count(["systemctl", "daemon-reload"]), 2)

    def test_successful_reload_with_wrong_effective_content_rolls_back(self):
        self.ineffective_reload = True
        with self.assertRaisesRegex(RuntimeError, "did not become effective"):
            unit.reconcile(self.path, "apply")
        self.assertEqual(self.path.read_text(), self.old)
        self.assertEqual(self.loaded, self.command(self.old))

    def test_concurrent_edit_is_preserved_before_install(self):
        self.concurrent_edit = self.old + "# concurrent operator edit\n"
        with self.assertRaisesRegex(ValueError, "changed during reconciliation"):
            unit.reconcile(self.path, "apply")
        self.assertEqual(self.path.read_text(), self.concurrent_edit)
        self.assertNotIn(["systemctl", "daemon-reload"], self.calls)

    def test_concurrent_edit_is_preserved_after_install(self):
        self.concurrent_after_install = self.desired + "# concurrent operator edit\n"
        with self.assertRaisesRegex(RuntimeError, "prevents rollback"):
            unit.reconcile(self.path, "apply")
        self.assertEqual(self.path.read_text(), self.concurrent_after_install)

    def test_configured_but_stopped_storage_is_not_started_or_reported_healthy(self):
        self.active = "failed"
        self.result = "exit-code"
        self.assertEqual(unit.reconcile(self.path, "apply")["state"], "runtime_unready")
        self.assertEqual(self.active, "failed")
        self.enabled = "disabled"
        self.assertEqual(unit.reconcile(self.path, "apply")["state"], "runtime_unready")

    def test_parameters_are_literals_not_shell_or_systemd_expressions(self):
        for image, vg in [("/srv/image;reboot", self.vg), ("/srv/%n.img", self.vg),
                          ("/srv/../other.img", self.vg), (self.image, "-evil"),
                          (self.image, "vg;reboot")]:
            with self.subTest(image=image, vg=vg), self.assertRaises(ValueError):
                unit.render(image, vg)


class BootCommandTests(unittest.TestCase):
    def test_boot_query_empty_success_attaches_and_query_error_stops(self):
        for scenario, expected in [
            ("empty", ["query", "attach --find --show --nooverlap /srv/storage/test.img", "activate -ay test-vg"]),
            ("attached", ["query", "activate -ay test-vg"]),
            ("failed", ["query"]),
        ]:
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                actions = root / "actions"
                loop = root / "losetup"
                loop.write_text("#!/bin/sh\n" +
                                'if [ "$1" = -j ]; then\n' +
                                '  echo query >>"$TEST_ACTIONS"\n' +
                                '  case "$TEST_SCENARIO" in failed) exit 1;; attached) echo /dev/loop-test;; esac\n' +
                                'else echo "attach $*" >>"$TEST_ACTIONS"; fi\n')
                loop.chmod(0o755)
                vg = root / "vgchange"
                vg.write_text('#!/bin/sh\necho "activate $*" >>"$TEST_ACTIONS"\n')
                vg.chmod(0o755)
                env = dict(os.environ, PATH=tmp + ":" + os.environ["PATH"],
                           TEST_ACTIONS=str(actions), TEST_SCENARIO=scenario)
                result = subprocess.run(["/bin/sh", "-ec", unit.boot_command("/srv/storage/test.img", "test-vg")],
                                        env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode == 0, scenario != "failed")
                self.assertEqual(actions.read_text().splitlines(), expected)


if __name__ == "__main__":
    unittest.main()
