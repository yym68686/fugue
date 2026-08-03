#!/usr/bin/env python3
"""Bounded local/CI pre-push gate with one canonical JSON receipt."""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TIMEOUT_SECONDS = 55.0


def run(command: list[str], timeout: float) -> tuple[int, str]:
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env=command_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=max(0.1, timeout),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout.decode("utf-8", "replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
        stderr = exc.stderr.decode("utf-8", "replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
        output = stdout + stderr
        return 124, output + f"\ncommand exceeded {timeout:.1f}s\n"
    return completed.returncode, completed.stdout


def command_env() -> dict[str, str]:
    return os.environ.copy()


def git_output(arguments: list[str]) -> bytes:
    return subprocess.check_output(["git", *arguments], cwd=ROOT)


def resolve_base() -> str:
    requested = os.environ.get("PREPUSH_BASE_REF", "").strip()
    candidates = [requested] if requested else []
    candidates.extend(["HEAD", "HEAD^"])
    for candidate in candidates:
        if not candidate:
            continue
        result = subprocess.run(
            ["git", "rev-parse", "--verify", f"{candidate}^{{commit}}"],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if result.returncode == 0:
            return candidate
    raise RuntimeError("no usable pre-push base commit")


def nul_paths(arguments: list[str]) -> set[str]:
    output = git_output(arguments)
    return {item.decode("utf-8") for item in output.split(b"\0") if item}


def changed_files(base: str) -> list[str]:
    paths: set[str] = set()
    if git_output(["rev-parse", base]).strip() != git_output(["rev-parse", "HEAD"]).strip():
        paths.update(nul_paths(["diff", "--name-only", "-z", "--diff-filter=ACMRTUXB", f"{base}...HEAD", "--"]))
    paths.update(nul_paths(["diff", "--name-only", "-z", "--diff-filter=ACMRTUXB", "HEAD", "--"]))
    paths.update(nul_paths(["ls-files", "--others", "--exclude-standard", "-z", "--"]))
    return sorted(paths)


def affected_packages(paths: list[str]) -> list[str]:
    packages = set()
    for name in paths:
        if not name.endswith(".go"):
            continue
        parent = Path(name).parent.as_posix()
        packages.add("." if parent == "." else f"./{parent}")
    return sorted(packages)


def diff_check(base: str, paths: list[str], timeout: float) -> tuple[int, str]:
    commands: list[list[str]] = []
    if git_output(["rev-parse", base]).strip() != git_output(["rev-parse", "HEAD"]).strip():
        commands.append(["git", "diff", "--check", f"{base}...HEAD", "--"])
    commands.append(["git", "diff", "--check", "HEAD", "--"])
    untracked = set(nul_paths(["ls-files", "--others", "--exclude-standard", "-z", "--"]))
    output = []
    for command in commands:
        status, text = run(command, timeout)
        if status != 0:
            return status, text
        output.append(text)
    for name in paths:
        if name not in untracked or not (ROOT / name).is_file():
            continue
        status, text = run(["git", "diff", "--no-index", "--check", "--", "/dev/null", name], timeout)
        if text.strip():
            return 1, text
        if status not in (0, 1):
            return status, text
    return 0, "".join(output)


def gofmt_check(files: list[str], timeout: float) -> tuple[int, str]:
    if not files:
        return 0, ""
    status, output = run(["gofmt", "-l", *files], timeout)
    if status != 0:
        return status, output
    if output.strip():
        return 1, "gofmt required for:\n" + output
    return 0, ""


def shell_syntax_check(paths: list[str], timeout: float) -> tuple[int, str]:
    scripts = [name for name in paths if name.endswith(".sh") and (ROOT / name).is_file()]
    for name in scripts:
        status, output = run(["bash", "-n", name], timeout)
        if status != 0:
            return status, output
    return 0, ""


def helm_check(timeout: float) -> tuple[int, str]:
    commands = [
        ["helm", "lint", "deploy/helm/fugue"],
        ["helm", "template", "fugue", "deploy/helm/fugue", "--namespace", "fugue-system"],
        [
            "helm", "template", "fugue", "deploy/helm/fugue", "--namespace", "fugue-system",
            "--values", "deploy/helm/fugue/values-production-ha.yaml",
        ],
        [
            "go", "test", "./cmd/fugue-release-domain-evidence",
            "-run", "^TestReleasePreflightValidatesCurrentChartDownwardAPI$", "-count=1",
        ],
    ]
    for command in commands:
        status, output = run(command, timeout)
        if status != 0:
            return status, output
    return 0, ""


def receipt_path() -> Path:
    configured = os.environ.get("PREPUSH_RECEIPT", "").strip()
    return Path(configured) if configured else Path(tempfile.gettempdir()) / "fugue-prepush-receipt.json"


def write_receipt(receipt: dict[str, object]) -> bytes:
    validate_json_value(receipt, "$")
    encoded = json.dumps(receipt, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("ascii") + b"\n"
    decoded = json.loads(encoded)
    if not exact_json_types_equal(decoded, receipt):
        raise ValueError("pre-push receipt schema/type round-trip drifted")
    repeated = json.dumps(decoded, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("ascii") + b"\n"
    if repeated != encoded:
        raise ValueError("pre-push receipt canonical JSON round-trip drifted")
    output = receipt_path().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp-{os.getpid()}")
    with temporary.open("xb") as handle:
        os.chmod(temporary, 0o600)
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, output)
    return encoded


def validate_json_value(value: object, path: str) -> None:
    if value is None or type(value) in (str, int, bool):
        return
    if type(value) is list:
        for index, item in enumerate(value):
            validate_json_value(item, f"{path}[{index}]")
        return
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise TypeError(f"{path} has a non-string JSON object key")
            validate_json_value(item, f"{path}.{key}")
        return
    raise TypeError(f"{path} uses non-JSON runtime type {type(value).__name__}")


def exact_json_types_equal(left: object, right: object) -> bool:
    if type(left) is not type(right):
        return False
    if type(left) is dict:
        return left.keys() == right.keys() and all(exact_json_types_equal(left[key], right[key]) for key in left)
    if type(left) is list:
        return len(left) == len(right) and all(exact_json_types_equal(a, b) for a, b in zip(left, right))
    return left == right


def main() -> int:
    started = time.monotonic()
    timeout_seconds = float(os.environ.get("PREPUSH_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS))
    if timeout_seconds <= 0 or timeout_seconds >= 60:
        raise SystemExit("PREPUSH_TIMEOUT_SECONDS must be greater than zero and less than 60")
    deadline = started + timeout_seconds
    base = resolve_base()
    paths = changed_files(base)
    packages = affected_packages(paths)
    checks: dict[str, dict[str, object]] = {}

    tasks: dict[str, list[str] | None] = {
        "openapi-generated": [
            "go", "run", "./cmd/fugue-openapi-gen", "-spec", "openapi/openapi.yaml",
            "-routes-out", "internal/api/routes_gen.go", "-spec-out", "internal/apispec/spec_gen.go", "-check",
        ],
    }
    if packages:
        tasks["affected-tests"] = ["go", "test", *packages]
        tasks["affected-vet"] = ["go", "vet", *packages]
    if any(name in {"scripts/prepush.py", "scripts/test_prepush.py"} for name in paths):
        tasks["prepush-receipt-tests"] = ["python3", "-m", "unittest", "scripts.test_prepush"]
    if any(name in {
        ".github/workflows/deploy-control-plane.yml",
        "scripts/build_control_plane_images.sh",
        "scripts/test_control_plane_build_reuse.py",
        "scripts/test_verify_registry_image.py",
        "scripts/verify_registry_image.py",
    } for name in paths):
        tasks["control-plane-build-reuse-tests"] = [
            "python3", "-m", "unittest",
            "scripts.test_control_plane_build_reuse", "scripts.test_verify_registry_image",
        ]

    local_checks = {
        "diff-check": lambda remaining: diff_check(base, paths, remaining),
        "gofmt": lambda remaining: gofmt_check([name for name in paths if name.endswith(".go") and (ROOT / name).is_file()], remaining),
        "shell-syntax": lambda remaining: shell_syntax_check(paths, remaining),
    }
    if any(name.startswith("deploy/helm/fugue/") for name in paths):
        local_checks["helm-lint-render"] = helm_check

    failures: list[tuple[str, str]] = []

    def record(name: str, before: float, status: int, output: str) -> None:
        checks[name] = {"durationMs": round((time.monotonic() - before) * 1000), "status": "pass" if status == 0 else "fail"}
        if status != 0:
            failures.append((name, output[-12000:]))

    for name, check in local_checks.items():
        before = time.monotonic()
        remaining = deadline - before
        if remaining <= 0:
            record(name, before, 124, "pre-push deadline exceeded")
            break
        status, output = check(remaining)
        record(name, before, status, output)

    if not failures:
        before = time.monotonic()
        status, output = run(["go", "build", "./..."], deadline - before)
        record("compile-all", before, status, output)

    if not failures:
        def execute(command: list[str]) -> tuple[int, str, float]:
            before = time.monotonic()
            status, output = run(command, deadline - before)
            return status, output, before

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            futures = {executor.submit(execute, command): name for name, command in tasks.items() if command is not None}
            for future in concurrent.futures.as_completed(futures):
                name = futures[future]
                status, output, before = future.result()
                record(name, before, status, output)

    for name in ("affected-tests", "affected-vet", "helm-lint-render"):
        if name not in checks:
            checks[name] = {"durationMs": 0, "status": "skipped"}

    elapsed_ms = round((time.monotonic() - started) * 1000)
    source_commit = git_output(["rev-parse", "HEAD"]).decode("ascii").strip()
    base_commit = git_output(["rev-parse", base]).decode("ascii").strip()
    changed_digest = hashlib.sha256(b"\0".join(name.encode("utf-8") for name in paths)).hexdigest()
    receipt: dict[str, object] = {
        "apiVersion": "prepush.fugue.dev/v1",
        "baseCommit": base_commit,
        "changedFilesDigest": f"sha256:{changed_digest}",
        "checks": {name: checks[name] for name in sorted(checks)},
        "elapsedMs": elapsed_ms,
        "kind": "PrepushReceipt",
        "sourceCommit": source_commit,
        "status": "fail" if failures else "pass",
    }
    encoded = write_receipt(receipt)
    sys.stdout.buffer.write(encoded)
    if failures:
        for name, output in failures:
            print(f"prepush {name} failed", file=sys.stderr)
            if output.strip():
                print(output.rstrip(), file=sys.stderr)
        return 1
    if elapsed_ms >= 60_000:
        print(f"prepush exceeded 60s: {elapsed_ms}ms", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
