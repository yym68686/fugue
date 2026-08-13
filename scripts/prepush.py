#!/usr/bin/env python3
"""Bounded local/CI pre-push gate with one canonical JSON receipt."""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import threading
import time


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TIMEOUT_SECONDS = 55.0
DEFAULT_MAX_ELAPSED_SECONDS = 60.0
MAX_CONFIGURED_TIMEOUT_SECONDS = 180.0
MAX_CONFIGURED_ELAPSED_SECONDS = 180.0
GO_TASK_CONCURRENCY = 2
SERIAL_WARM_TEST_PACKAGES = {"./internal/api"}
DECLARATIVE_TEST_PACKAGES = {
    "./cmd/fugue-declarative-release",
    "./internal/declarativerelease",
}
DECLARATIVE_DATA_TEST_PATTERN = (
    "^(TestProductionRegistryNamesEveryRuntimeLane|"
    "TestThirdEdgeGroupIsPureDataAndPlansIndependently|"
    "TestSharedEdgeWorkerManifestRollsOneGroupPerIntent)$"
)
TEST_HUNK_RE = re.compile(
    r"^@@ -\d+(?:,\d+)? \+\d+(?:,\d+)? @@ func "
    r"(Test(?:[A-Z0-9_][A-Za-z0-9_]*)?)\("
)


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


def task_timeout_seconds(name: str, remaining: float) -> float:
    return remaining


def elapsed_timeout_seconds(check_names: set[str]) -> float:
    raw = os.environ.get("PREPUSH_MAX_ELAPSED_SECONDS", "").strip()
    if not raw:
        return DEFAULT_MAX_ELAPSED_SECONDS
    try:
        value = float(raw)
    except ValueError as exc:
        raise SystemExit("PREPUSH_MAX_ELAPSED_SECONDS must be numeric") from exc
    if value <= 0 or value > MAX_CONFIGURED_ELAPSED_SECONDS:
        raise SystemExit(
            "PREPUSH_MAX_ELAPSED_SECONDS must be greater than zero and at most 180"
        )
    return value


def elapsed_timeout_exceeded(check_names: set[str], elapsed_ms: int) -> bool:
    limit_ms = round(elapsed_timeout_seconds(check_names) * 1000)
    return elapsed_ms >= limit_ms


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


def unified_diff_for_path(base: str, name: str) -> bytes:
    output = bytearray()
    if git_output(["rev-parse", base]).strip() != git_output(["rev-parse", "HEAD"]).strip():
        output.extend(git_output(["diff", "--unified=0", "--no-ext-diff", f"{base}...HEAD", "--", name]))
    output.extend(git_output(["diff", "--unified=0", "--no-ext-diff", "HEAD", "--", name]))
    return bytes(output)


def exact_test_names_from_diff(diff: bytes) -> set[str] | None:
    headers = [line.decode("utf-8", "replace") for line in diff.splitlines() if line.startswith(b"@@")]
    if not headers:
        return None
    names = set()
    for header in headers:
        match = TEST_HUNK_RE.match(header)
        if match is None:
            return None
        names.add(match.group(1))
    return names or None


def affected_test_commands(base: str, paths: list[str]) -> list[list[str]]:
    package_files: dict[str, list[str]] = {}
    for name in paths:
        if not name.endswith(".go"):
            continue
        parent = Path(name).parent.as_posix()
        package = "." if parent == "." else f"./{parent}"
        package_files.setdefault(package, []).append(name)

    commands = []
    for package in sorted(package_files):
        files = package_files[package]
        if any(not name.endswith("_test.go") for name in files):
            commands.append(["go", "test", package])
            continue
        names = set()
        for name in files:
            selected = exact_test_names_from_diff(unified_diff_for_path(base, name))
            if selected is None:
                names.clear()
                break
            names.update(selected)
        if not names:
            commands.append(["go", "test", package])
            continue
        pattern = "^(" + "|".join(re.escape(name) for name in sorted(names)) + ")$"
        commands.append(["go", "test", package, "-run", pattern])
    return commands


def without_dedicated_declarative_tests(commands: list[list[str]]) -> list[list[str]]:
    return [
        command
        for command in commands
        if not (
            len(command) >= 3
            and command[:2] == ["go", "test"]
            and command[2] in DECLARATIVE_TEST_PACKAGES
        )
    ]


def warm_affected_tests_before_compile(commands: list[list[str]]) -> bool:
    return any(
        len(command) >= 3
        and command[:2] == ["go", "test"]
        and command[2] in SERIAL_WARM_TEST_PACKAGES
        for command in commands
    )


def run_test_commands(
    commands: list[list[str]],
    deadline: float,
    go_slots: threading.BoundedSemaphore | None = None,
) -> tuple[int, str]:
    if not commands:
        return 0, ""
    results: list[tuple[int, str] | None] = [None] * len(commands)

    slots = go_slots or threading.BoundedSemaphore(GO_TASK_CONCURRENCY)

    def execute(index: int, command: list[str]) -> tuple[int, int, str]:
        with slots:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return index, 124, "pre-push deadline exceeded"
            status, output = run(command, remaining)
        return index, status, output

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(len(commands), GO_TASK_CONCURRENCY)
    ) as executor:
        futures = [executor.submit(execute, index, command) for index, command in enumerate(commands)]
        for future in concurrent.futures.as_completed(futures):
            index, status, output = future.result()
            results[index] = (status, output)

    failures = []
    for command, result in zip(commands, results):
        if result is None:
            failures.append((124, command, "affected test command result is missing"))
            continue
        status, output = result
        if status != 0:
            failures.append((status, command, output))
    if not failures:
        return 0, ""
    output = "\n".join(f"$ {' '.join(command)}\n{text}".rstrip() for _, command, text in failures)
    return failures[0][0], output


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
    deadline = time.monotonic() + max(0.0, timeout)
    commands = [
        ["helm", "lint", "deploy/helm/fugue"],
        ["helm", "template", "fugue", "deploy/helm/fugue", "--namespace", "fugue-system"],
        [
            "helm", "template", "fugue", "deploy/helm/fugue", "--namespace", "fugue-system",
            "--values", "deploy/helm/fugue/values-production-ha.yaml",
        ],
    ]
    for command in commands:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return 124, "pre-push deadline exceeded"
        status, output = run(command, remaining)
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
    if timeout_seconds <= 0 or timeout_seconds > MAX_CONFIGURED_TIMEOUT_SECONDS:
        raise SystemExit(
            "PREPUSH_TIMEOUT_SECONDS must be greater than zero and at most 180"
        )
    deadline = started + timeout_seconds
    base = resolve_base()
    paths = changed_files(base)
    packages = affected_packages(paths)
    test_commands = affected_test_commands(base, paths)
    declarative_engine_changed = any(
        name == ".github/workflows/ci.yml"
        or name.startswith(".github/actions/deploy-declarative-component/")
        or name == "deploy/releases/components.json"
        or name == "deploy/releases/edge-groups.json"
        or name.startswith("cmd/fugue-declarative-release/")
        or name.startswith("internal/declarativerelease/")
        for name in paths
    )
    declarative_data_changed = any(
        name.startswith("deploy/releases/")
        and name not in {"deploy/releases/components.json", "deploy/releases/edge-groups.json"}
        for name in paths
    )
    if declarative_engine_changed:
        test_commands = without_dedicated_declarative_tests(test_commands)
    checks: dict[str, dict[str, object]] = {}
    go_slots = threading.BoundedSemaphore(GO_TASK_CONCURRENCY)
    declarative_compile_finished = threading.Event()
    declarative_vet_finished = threading.Event()

    compile_task = ("compile-all", ["go", "build", "-p", "4", "./..."])
    go_dependent_tasks: dict[str, list[str] | None] = {
        "openapi-generated": [
            "go", "run", "./cmd/fugue-openapi-gen", "-spec", "openapi/openapi.yaml",
            "-routes-out", "internal/api/routes_gen.go", "-spec-out", "internal/apispec/spec_gen.go", "-check",
        ],
    }
    non_go_tasks: dict[str, list[str] | None] = {}
    if packages:
        vet_task = ["go", "vet", *packages]
        if declarative_engine_changed:
            non_go_tasks["affected-vet"] = vet_task
        else:
            go_dependent_tasks["affected-vet"] = vet_task
    if any(name in {"scripts/prepush.py", "scripts/test_prepush.py"} for name in paths):
        non_go_tasks["prepush-receipt-tests"] = ["python3", "-m", "unittest", "scripts.test_prepush"]
    if any(name in {"scripts/test_verify_registry_image.py", "scripts/verify_registry_image.py"} for name in paths):
        non_go_tasks["registry-verifier-tests"] = [
            "python3", "-m", "unittest", "scripts.test_verify_registry_image",
        ]

    if declarative_engine_changed:
        non_go_tasks["declarative-release-tests"] = [
            "go", "test", "./internal/declarativerelease", "./cmd/fugue-declarative-release",
        ]
    elif declarative_data_changed:
        non_go_tasks["declarative-release-tests"] = [
            "go", "test", "./internal/declarativerelease", "-run", DECLARATIVE_DATA_TEST_PATTERN,
        ]

    local_checks = {
        "diff-check": lambda remaining: diff_check(base, paths, remaining),
        "gofmt": lambda remaining: gofmt_check([name for name in paths if name.endswith(".go") and (ROOT / name).is_file()], remaining),
        "shell-syntax": lambda remaining: shell_syntax_check(paths, remaining),
    }
    if any(name.startswith("deploy/helm/fugue/") for name in paths):
        go_dependent_tasks["helm-lint-render"] = None

    failures: list[tuple[str, str]] = []

    def record(name: str, before: float, status: int, output: str) -> None:
        checks[name] = {"durationMs": round((time.monotonic() - before) * 1000), "status": "pass" if status == 0 else "fail"}
        if status != 0:
            failures.append((name, output[-12000:]))

    def execute(name: str, command: list[str] | None) -> tuple[int, str, float]:
        before = time.monotonic()
        if name == "affected-vet" and declarative_engine_changed:
            remaining = deadline - before
            if remaining <= 0 or not declarative_compile_finished.wait(remaining):
                return 124, "pre-push deadline exceeded before declarative compile completed", before
        if name == "declarative-release-tests" and (declarative_engine_changed or declarative_data_changed):
            remaining = deadline - before
            if remaining <= 0 or not declarative_compile_finished.wait(remaining):
                return 124, "pre-push deadline exceeded before declarative compile completed", before
        if name == "declarative-release-tests" and "affected-vet" in non_go_tasks:
            remaining = deadline - before
            if remaining <= 0 or not declarative_vet_finished.wait(remaining):
                return 124, "pre-push deadline exceeded before declarative vet completed", before
        if name == "affected-tests":
            status, output = run_test_commands(test_commands, deadline, go_slots)
        elif name == "helm-lint-render":
            status, output = helm_check(deadline - before)
        elif command is None:
            status, output = 124, f"pre-push task {name} has no command"
        else:
            if command and command[0] == "go":
                with go_slots:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        status, output = 124, "pre-push deadline exceeded"
                    else:
                        status, output = run(command, task_timeout_seconds(name, remaining))
            else:
                status, output = run(command, task_timeout_seconds(name, deadline - before))
        if name == "affected-vet" and declarative_engine_changed:
            declarative_vet_finished.set()
        if name == "compile-all" and (declarative_engine_changed or declarative_data_changed):
            declarative_compile_finished.set()
        return status, output, before

    warm_affected = warm_affected_tests_before_compile(test_commands)
    if warm_affected:
        status, output, before = execute("affected-tests", None)
        record("affected-tests", before, status, output)

    phase_one_workers = 1 + len(non_go_tasks) + (1 if test_commands and not warm_affected else 0)
    with concurrent.futures.ThreadPoolExecutor(max_workers=phase_one_workers) as phase_one:
        compile_name, compile_command = compile_task
        compile_future = phase_one.submit(execute, compile_name, compile_command)
        affected_futures = {}
        if test_commands and not warm_affected:
            affected_futures[phase_one.submit(execute, "affected-tests", None)] = "affected-tests"
        non_go_futures = {
            phase_one.submit(execute, name, command): name
            for name, command in non_go_tasks.items()
        }

        for name, check in local_checks.items():
            before = time.monotonic()
            remaining = deadline - before
            if remaining <= 0:
                record(name, before, 124, "pre-push deadline exceeded")
                break
            status, output = check(remaining)
            record(name, before, status, output)

        compile_status, compile_output, compile_before = compile_future.result()
        record(compile_name, compile_before, compile_status, compile_output)

        if not failures:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(go_dependent_tasks)
            ) as go_phase:
                go_futures = {
                    go_phase.submit(execute, name, command): name
                    for name, command in go_dependent_tasks.items()
                }
                all_futures = {**affected_futures, **non_go_futures, **go_futures}
                for future in concurrent.futures.as_completed(all_futures):
                    name = all_futures[future]
                    status, output, before = future.result()
                    record(name, before, status, output)
        else:
            for name in go_dependent_tasks:
                checks[name] = {"durationMs": 0, "status": "skipped"}
            phase_one_futures = {**affected_futures, **non_go_futures}
            for future in concurrent.futures.as_completed(phase_one_futures):
                name = phase_one_futures[future]
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
    elapsed_limit_seconds = elapsed_timeout_seconds(set(checks))
    if elapsed_timeout_exceeded(set(checks), elapsed_ms):
        print(f"prepush exceeded {elapsed_limit_seconds:g}s: {elapsed_ms}ms", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
