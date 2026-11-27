#!/usr/bin/env python3
"""Report Go verification impact without modifying or authorizing the release gate."""

from __future__ import annotations

import argparse
from collections import defaultdict, deque
import json
import os
from pathlib import PurePosixPath
import subprocess
import time

from scripts import prepush


SOURCE = ("GoFiles", "CgoFiles", "EmbedFiles")
TEST = ("TestGoFiles", "XTestGoFiles", "TestEmbedFiles", "XTestEmbedFiles")
FIELDS = ("Dir", "ImportPath", "Incomplete", "Imports", "TestImports", "XTestImports", *SOURCE, *TEST)


def packages_from_stream(stream):
    decoder = json.JSONDecoder()
    cursor = 0
    packages = []
    while cursor < len(stream):
        if stream[cursor].isspace():
            cursor += 1
            continue
        package, cursor = decoder.raw_decode(stream, cursor)
        packages.append(package)
    return packages


def git_paths(base):
    # Disable rename detection: the old path is needed too. Unlike the legacy
    # gate, include deletions so stale graph edges cannot authorise a narrow plan.
    paths = prepush.nul_paths(["diff", "--no-renames", "--name-only", "-z", f"{base}...HEAD", "--"])
    paths.update(prepush.nul_paths(["diff", "--no-renames", "--name-only", "-z", "HEAD", "--"]))
    paths.update(prepush.nul_paths(["ls-files", "--others", "--exclude-standard", "-z", "--"]))
    return sorted(paths)


def commands_for(kind, packages):
    if not packages:
        return []
    flags = (["-o", os.devnull] if len(packages) == 1 else []) if kind == "build" else []
    return [["go", kind, *flags, *sorted(packages)]]


def full(reason):
    return {
        "scope": "full", "reason": reason,
        "goTests": [["go", "test", "./..."]], "goBuilds": [["go", "build", "./..."]],
    }


def select(paths, packages, root):
    if not paths:
        return full("no diff to classify")
    if not packages or any(p.get("Incomplete") for p in packages):
        return full("Go package graph is unavailable or incomplete")

    owners = defaultdict(set)
    labels = {}
    reverse_runtime = defaultdict(set)
    reverse_test = defaultdict(set)
    for p in packages:
        # go list has no canonical package variant suffix without -test; test
        # imports are explicitly included, but never affect runtime builds.
        name = p["ImportPath"]
        directory = PurePosixPath(p["Dir"]).relative_to(PurePosixPath(root))
        labels[name] = "." if directory == PurePosixPath(".") else "./" + directory.as_posix()
        for fields, is_source in ((SOURCE, True), (TEST, False)):
            for field in fields:
                for filename in p.get(field, ()):
                    owners[(directory / filename).as_posix()].add((name, is_source))
        for dependency in p.get("Imports", ()):
            reverse_runtime[dependency].add(name)
        for dependency in (*p.get("TestImports", ()), *p.get("XTestImports", ())):
            reverse_test[dependency].add(name)

    runtime_edits, test_edits = set(), set()
    for path in paths:
        if not owners[path]:
            return full(f"{path} has no owner in the current Go graph")
        for name, is_source in owners[path]:
            (runtime_edits if is_source else test_edits).add(name)
    if len(runtime_edits | test_edits) > 1:
        return full("change touches multiple package owners")

    compiled = set(runtime_edits)
    queue = deque(runtime_edits)
    while queue:
        for dependent in reverse_runtime[queue.popleft()] - compiled:
            compiled.add(dependent)
            queue.append(dependent)

    tests = compiled | test_edits
    for name in compiled:
        tests.update(reverse_test[name])
    return {
        "scope": "dependency-closure" if runtime_edits else "test-only",
        "reason": "one package owner; Go production and test imports resolved",
        "goTests": commands_for("test", {labels[name] for name in tests}),
        "goBuilds": commands_for("build", {labels[name] for name in compiled}),
    }


def report(base):
    started = time.monotonic()
    paths = git_paths(base)
    legacy_paths = prepush.changed_files(base)
    legacy = prepush.affected_test_commands(base, legacy_paths)
    declarative_engine_changed, declarative_data_changed = prepush.declarative_changes(legacy_paths)
    if declarative_engine_changed:
        legacy = prepush.without_dedicated_declarative_tests(legacy)
    declarative = prepush.declarative_test_command(declarative_engine_changed, declarative_data_changed)
    if declarative:
        legacy.append(declarative)
    try:
        status, output = prepush.run(["go", "list", "-json=" + ",".join(FIELDS), "./..."], 12)
        plan = select(paths, packages_from_stream(output) if not status else [], prepush.ROOT)
    except (OSError, TypeError, ValueError, KeyError):
        plan = full("Go package graph could not be read")
    planned = {pkg for cmd in plan["goTests"] for pkg in cmd[2:]}
    baseline = {pkg for cmd in legacy for pkg in cmd[2:cmd.index("-run") if "-run" in cmd else len(cmd)]}
    missing = sorted(baseline - planned) if plan["scope"] != "full" else []
    if missing:
        plan = full("candidate would omit existing Go tests")
    return {
        "apiVersion": "prepush.fugue.dev/shadow-v1", "kind": "GoVerificationSelection",
        "gate": "report-only", "baseCommit": prepush.git_output(["rev-parse", base]).decode("ascii").strip(),
        "sourceCommit": prepush.git_output(["rev-parse", "HEAD"]).decode("ascii").strip(),
        "paths": paths, "legacyTestCommands": legacy, "omittedLegacyPackagesBeforeFallback": missing,
        "legacyBuildCommands": [["go", "build", "-p", "4", "./..."]],
        "elapsedMs": round((time.monotonic() - started) * 1000), **plan,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", help="exact base ref (defaults to PREPUSH_BASE_REF or HEAD)")
    options = parser.parse_args()
    try:
        result = report(options.base or prepush.resolve_base())
    except (OSError, UnicodeError, subprocess.CalledProcessError, RuntimeError) as err:
        parser.exit(2, f"shadow report unavailable: {err}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
