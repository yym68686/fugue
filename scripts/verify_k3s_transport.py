"""Verify the transport patch in the pinned upstream package on an isolated runner.

This prepares and tests a disposable source checkout. It does not install K3s,
contact the production cluster, publish binaries, or change a release intent.
"""
from __future__ import annotations

import argparse
import collections
import datetime
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/k3s-transport"
REMOTE_TEST = "TestFugueTunnelRemoteKubeletFailClosed"
FAULT_TESTS = {f"{REMOTE_TEST}/{route}_{fault}"
               for route in ("node", "ipv4", "ipv6") for fault in ("EOF", "timeout")}
CONTROL_TESTS = {f"{REMOTE_TEST}/{route}_healthy" for route in ("node", "ipv4", "ipv6")} | {
    "TestFugueTunnelLocalKubeletRemainsDirect",
    "TestFugueTunnelMissingRemoteSessionRemainsRejected",
    "TestFugueTunnelPodFallbackKeepsOriginalAddress",
}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def source_spec() -> dict:
    value = json.loads((FIXTURE / "source.json").read_text())
    if not re.fullmatch(r"https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\.git", value["repository"]):
        raise ValueError("source repository must be an explicit public GitHub HTTPS URL")
    if not re.fullmatch(r"[0-9a-f]{40}", value["commit"]):
        raise ValueError("source commit must be pinned in full")
    path = PurePosixPath(value["source_file"])
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("source file must stay inside the checkout")
    for key in ("source_sha256", "patch_sha256"):
        if not re.fullmatch(r"[0-9a-f]{64}", value[key]):
            raise ValueError("source and patch checksums are required")
    if digest(FIXTURE / "remote-kubelet-fail-closed.patch") != value["patch_sha256"]:
        raise ValueError("patch does not match its pinned checksum")
    return value


def command(args: list[str], cwd: Path, timeout: int = 120) -> str:
    result = subprocess.run(args, cwd=cwd, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {args[0]}\n{result.stderr[-4000:]}")
    return result.stdout


def prepare(workspace: Path, evidence: Path) -> None:
    spec = source_spec()
    checkout = workspace / "k3s"
    if checkout.exists():
        raise ValueError("refusing to replace an existing source checkout")
    checkout.mkdir(parents=True)
    command(["git", "init", "--quiet"], checkout)
    command(["git", "remote", "add", "origin", spec["repository"]], checkout)
    command(["git", "fetch", "--depth=1", "origin", spec["commit"]], checkout, 300)
    command(["git", "checkout", "--detach", "FETCH_HEAD"], checkout)
    actual = command(["git", "rev-parse", "HEAD"], checkout).strip()
    if actual != spec["commit"] or digest(checkout / spec["source_file"]) != spec["source_sha256"]:
        raise ValueError("upstream revision or source hash differs from the reproduced version")
    go_module = (checkout / "go.mod").read_text()
    if not re.search(r"^go " + re.escape(spec["go_version"]) + r"$", go_module, re.M):
        raise ValueError("upstream Go version differs from the pinned test toolchain")
    command(["git", "apply", "--ignore-space-change", "--check", str(FIXTURE / "remote-kubelet-fail-closed.patch")], checkout)
    target = checkout / spec["source_file"]
    test_file = target.parent / "fugue_transport_regression_test.go"
    if test_file.exists():
        raise ValueError("upstream already owns the proposed test path")
    shutil.copyfile(FIXTURE / "tunnel_backend_regression_test.go.txt", test_file)
    save(evidence / "preparation.json", {
        "at": now(), "checkout": str(checkout), "source": spec,
        "test_sha256": digest(test_file), "patch_applies": True,
        "scope": "Isolated upstream package verification; no production access or installation.",
    })


def evaluate_test_events(raw: str, returncode: int, *, baseline: bool, repetitions: int) -> dict:
    passed: collections.Counter = collections.Counter()
    failed: collections.Counter = collections.Counter()
    skipped = []
    output = []
    for line in raw.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        test, action = event.get("Test"), event.get("Action")
        if action == "output":
            output.append(event.get("Output", ""))
        if not test:
            continue
        if action == "pass":
            passed[test] += 1
        elif action == "fail":
            failed[test] += 1
        elif action == "skip":
            skipped.append(test)
    if skipped:
        raise ValueError("transport verification skipped tests")
    if baseline:
        expected_failures = FAULT_TESTS | {REMOTE_TEST}
        if returncode != 1 or set(failed) != expected_failures or any(failed[t] != 1 for t in expected_failures):
            raise ValueError("baseline did not reproduce exactly the six unsafe fallback cases")
        if any(passed[t] != 1 for t in CONTROL_TESTS):
            raise ValueError("baseline control cases did not all pass")
        if "".join(output).count("unsafe_local_kubelet_fallback:") != len(FAULT_TESTS):
            raise ValueError("baseline failure is not evidence of the wrong local backend")
    else:
        required = FAULT_TESTS | CONTROL_TESTS | {REMOTE_TEST}
        if returncode != 0 or failed or any(passed[t] != repetitions for t in required):
            raise ValueError("patched upstream package did not pass every required repeated case")
    return {"passed": dict(passed), "failed": dict(failed), "skipped": skipped,
            "baseline": baseline, "repetitions": repetitions, "accepted": True}


def test_phase(checkout: Path, evidence: Path, phase: str, repetitions: int) -> dict:
    args = ["go", "test", "-json", "-race", "-mod=readonly", "-p", "2", "-count=" + str(repetitions),
            "-timeout=3m", "-tags=providerless", "./pkg/daemons/control", "-run", "^TestFugueTunnel"]
    started = now()
    env = dict(os.environ)
    env.setdefault("GOMAXPROCS", "2")
    env.setdefault("GOMEMLIMIT", "3GiB")
    try:
        process = subprocess.run(args, cwd=checkout, env=env, text=True,
                                 capture_output=True, timeout=1800)
    except subprocess.TimeoutExpired as error:
        save(evidence / (phase + "-execution.json"), {"started_at": started, "ended_at": now(),
             "error": "compile or test process exceeded the 30 minute bound", "command": args})
        raise RuntimeError("bounded upstream verification timed out") from error
    (evidence / (phase + ".jsonl")).write_text(process.stdout)
    (evidence / (phase + "-stderr.log")).write_text(process.stderr)
    save(evidence / (phase + "-execution.json"), {
        "started_at": started, "ended_at": now(), "exit": process.returncode, "command": args,
    })
    result = evaluate_test_events(process.stdout, process.returncode,
                                  baseline=phase == "baseline", repetitions=repetitions)
    save(evidence / (phase + "-review.json"), result)
    return result


def verify(workspace: Path, evidence: Path) -> None:
    spec = source_spec()
    checkout = workspace / "k3s"
    prepared = json.loads((evidence / "preparation.json").read_text())
    source = checkout / spec["source_file"]
    test_file = source.parent / "fugue_transport_regression_test.go"
    if prepared["source"] != spec or prepared["checkout"] != str(checkout):
        raise ValueError("preparation belongs to another source or workspace")
    if command(["git", "rev-parse", "HEAD"], checkout).strip() != spec["commit"] or digest(source) != spec["source_sha256"]:
        raise ValueError("baseline checkout changed before verification")
    if digest(test_file) != prepared["test_sha256"] or digest(test_file) != digest(FIXTURE / "tunnel_backend_regression_test.go.txt"):
        raise ValueError("injected test differs from the prepared fixture")
    command(["git", "diff", "--exit-code"], checkout)
    untracked = command(["git", "ls-files", "--others", "--exclude-standard"], checkout).splitlines()
    if untracked != [test_file.relative_to(checkout).as_posix()]:
        raise ValueError("baseline contains unprepared source files")
    toolchain = json.loads(command(["go", "env", "-json", "GOVERSION", "GOOS", "GOARCH", "CGO_ENABLED"], checkout))
    if toolchain != {"GOVERSION": "go" + spec["go_version"], "GOOS": "linux", "GOARCH": "amd64", "CGO_ENABLED": "1"}:
        raise ValueError("verification requires the pinned Linux amd64 toolchain with CGO for race detection")
    save(evidence / "toolchain.json", toolchain)
    baseline = test_phase(checkout, evidence, "baseline", 1)
    command(["git", "apply", "--ignore-space-change", str(FIXTURE / "remote-kubelet-fail-closed.patch")], checkout)
    if command(["git", "diff", "--name-only"], checkout).splitlines() != [spec["source_file"]]:
        raise ValueError("patch modified files outside its verified source")
    patched = test_phase(checkout, evidence, "patched", 3)
    save(evidence / "result.json", {
        "at": now(), "source": spec, "patched_source_sha256": digest(source),
        "test_sha256": digest(test_file), "baseline": baseline, "patched": patched,
        "scope": "Real upstream control package with TCP/WebSocket fault injection under race detection. Not a complete K3s cluster rollout or a production release.",
        "production_modified": False,
    })
    print(json.dumps({"accepted": True, "source_commit": spec["commit"], "production_modified": False}))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("prepare", "verify"))
    parser.add_argument("--workspace", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    workspace, evidence = args.workspace.resolve(), args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    try:
        (prepare if args.phase == "prepare" else verify)(workspace, evidence)
    except Exception as error:
        save(evidence / "failure.json", {"at": now(), "phase": args.phase,
             "error_type": type(error).__name__, "error": str(error), "production_modified": False})
        raise


if __name__ == "__main__":
    main()
