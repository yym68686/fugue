#!/usr/bin/env python3

import json
import os
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SUBJECT = ROOT / "scripts" / "package_fugue_cli.sh"
TARGETS = "linux/amd64 linux/arm64 darwin/amd64 windows/amd64"


FAKE_GO = r'''#!/usr/bin/env python3
import fcntl, json, os, pathlib, sys, time
state = pathlib.Path(os.environ["TEST_STATE"])
lock_path = state / "lock"
lock_path.touch(exist_ok=True)
with lock_path.open("r+") as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    counts_path = state / "counts.json"
    counts = json.loads(counts_path.read_text()) if counts_path.exists() else {"active": 0, "max": 0, "starts": []}
    counts["active"] += 1
    counts["max"] = max(counts["max"], counts["active"])
    target = os.environ["GOOS"] + "/" + os.environ["GOARCH"]
    counts["starts"].append(target)
    counts_path.write_text(json.dumps(counts, sort_keys=True))
    fcntl.flock(lock, fcntl.LOCK_UN)
time.sleep(0.12)
failure = os.environ.get("FAIL_TARGET", "") == target
if not failure:
    output = pathlib.Path(sys.argv[sys.argv.index("-o") + 1])
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(("binary:" + target).encode())
with lock_path.open("r+") as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    counts = json.loads((state / "counts.json").read_text())
    counts["active"] -= 1
    (state / "counts.json").write_text(json.dumps(counts, sort_keys=True))
    fcntl.flock(lock, fcntl.LOCK_UN)
raise SystemExit(7 if failure else 0)
'''


FAKE_GIT = r'''#!/bin/sh
case "$*" in
  *describe*) exit 1 ;;
  *rev-parse*) printf 'abc1234\n'; exit 0 ;;
esac
exit 2
'''


class PackageParallelTests(unittest.TestCase):
    def fixture(self):
        stack = tempfile.TemporaryDirectory()
        root = Path(stack.name)
        fake_bin = root / "bin"
        state = root / "state"
        fake_bin.mkdir()
        state.mkdir()
        for name, body in (("go", FAKE_GO), ("git", FAKE_GIT)):
            path = fake_bin / name
            path.write_text(textwrap.dedent(body), encoding="utf-8")
            path.chmod(0o755)
        return stack, root, fake_bin, state

    def run_package(self, jobs=None, *, fail_target=""):
        stack, root, fake_bin, state = self.fixture()
        self.addCleanup(stack.cleanup)
        output = root / "dist"
        env = os.environ.copy()
        env.update({
            "PATH": str(fake_bin) + os.pathsep + env["PATH"],
            "TEST_STATE": str(state),
            "FUGUE_CLI_TARGETS": TARGETS,
            "FUGUE_CLI_VERSION": "test-v1",
            "FUGUE_CLI_COMMIT": "abc1234",
            "FUGUE_CLI_BUILD_TIME": "2026-08-02T00:00:00Z",
            "FAIL_TARGET": fail_target,
        })
        if jobs is not None:
            env["FUGUE_CLI_BUILD_JOBS"] = str(jobs)
        process = subprocess.run([str(SUBJECT), str(output)], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10, check=False)
        counts_path = state / "counts.json"
        counts = json.loads(counts_path.read_text()) if counts_path.exists() else {"active": 0, "max": 0, "starts": []}
        return process, output, counts

    def test_default_contract_runs_two_at_a_time_and_keeps_artifacts(self):
        process, output, counts = self.run_package()
        self.assertEqual(process.returncode, 0, process.stderr.decode())
        self.assertEqual(counts["max"], 2)
        self.assertEqual(counts["active"], 0)
        self.assertEqual(sorted(counts["starts"]), sorted(TARGETS.split()))
        self.assertEqual(
            sorted(path.name for path in output.iterdir()),
            ["fugue_checksums.txt", "fugue_darwin_amd64.tar.gz", "fugue_linux_amd64.tar.gz", "fugue_linux_arm64.tar.gz", "fugue_windows_amd64.zip"],
        )
        checksums = (output / "fugue_checksums.txt").read_text().splitlines()
        self.assertEqual(len(checksums), 4)
        self.assertTrue(all(len(line.split()[0]) == 64 for line in checksums))

    def test_one_job_is_serial_and_invalid_bounds_fail_before_build(self):
        process, _, counts = self.run_package(1)
        self.assertEqual(process.returncode, 0, process.stderr.decode())
        self.assertEqual(counts["max"], 1)
        process, _, counts = self.run_package(0)
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(counts["starts"], [])

    def test_worker_failure_waits_for_batch_and_leaves_no_child(self):
        process, _, counts = self.run_package(2, fail_target="linux/arm64")
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(counts["active"], 0)
        self.assertEqual(sorted(counts["starts"]), ["linux/amd64", "linux/arm64"])

    def test_product_script_keeps_exact_target_and_checksum_contract(self):
        source = SUBJECT.read_text(encoding="utf-8")
        self.assertIn('TARGETS=${FUGUE_CLI_TARGETS:-"linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64"}', source)
        self.assertIn('CHECKSUM_FILE="${DIST_DIR}/fugue_checksums.txt"', source)
        self.assertNotIn("xargs", source)
        self.assertNotIn("wait -n", source)


if __name__ == "__main__":
    unittest.main()
