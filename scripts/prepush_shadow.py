#!/usr/bin/env python3
"""Report Go verification impact without modifying or authorizing the release gate."""

from __future__ import annotations

import argparse
from collections import defaultdict, deque
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

from scripts import prepush


# In addition to Go and embed inputs, cmd/go owns these inputs through the
# active build constraints. An input absent from a profile is not assumed safe.
SOURCE = (
    "GoFiles", "CgoFiles", "EmbedFiles", "CFiles", "CXXFiles", "MFiles", "HFiles",
    "FFiles", "SFiles", "SwigFiles", "SwigCXXFiles", "SysoFiles",
)
TEST = ("TestGoFiles", "XTestGoFiles", "TestEmbedFiles", "XTestEmbedFiles")
FIELDS = (
    "Dir", "ImportPath", "Incomplete", "Error", "DepsErrors",
    "Imports", "TestImports", "XTestImports", *SOURCE, *TEST,
)
ENV_FIELDS = ("GOVERSION", "GOOS", "GOARCH", "CGO_ENABLED", "GOFLAGS")
ARTIFACT_PROFILES = ("linux/amd64", "linux/arm64")

# Go's import graph does not see tests reading another package's source or
# executing a command. Keep those contracts as test edges, never build edges.
# An input not owned by Go (e.g. a runtime testdata read) still falls back.
SOURCE_READERS = {
    "cmd/fugue-declarative-release/main.go": "./internal/declarativerelease",
    "internal/edge/service.go": "./internal/platformsafety",
    "internal/edgegroupfront/service.go": "./internal/platformsafety",
    "internal/observability/pipeline.go": "./internal/platformsafety",
}
COMMAND_TEST_CONSUMERS = {
    "./cmd/fugue-openapi-gen": "./internal/apispec",
    "./cmd/fugue-ssh-helper": "./internal/sshkube",
}
TEST_BINARY_CONSUMERS = {
    "./internal/sshexecutor": "./internal/sshkube",
}
# These contracts read every tracked file, including Go tests and sources
# with no import path to the checker. They do not affect runtime builds.
REPOSITORY_TEST_CONSUMERS = {"./internal/platformsafety", "./internal/releasesourceaudit"}
EDGE_GROUP_SOURCE_PREFIXES = ("internal/edge/", "internal/edgecontrol/", "internal/edgegroupfront/")
# These composition guards scan non-test Go files under both source trees.
APPLICATION_SOURCE_TEST_CONSUMERS = {
    "./internal/backupusage", "./internal/buildadmission",
    "./internal/imagetracking", "./internal/releaseretirement",
}
# A source dependency can sit beneath a large composition-root test package.
# Keep its domain contract tests as a first-class, name-scoped edge rather than
# paying to run unrelated root-package suites for every leaf implementation edit.
DEPENDENT_TEST_FILTERS = {
    ("./internal/backupusage/inventoryscan", "./internal/api"):
        r"^Test(BackupInventory|BackupUsage|MetricsReadCannotStartR2Work|InventorySeparatesDeclaredRepositoryBlocksFromArtifactObjects)",
}


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


def snapshot(base):
    """Fingerprint the exact diff, including untracked input contents and links."""
    digest = hashlib.sha256()
    for command in (["rev-parse", base, "HEAD"], ["diff", "--binary", "HEAD", "--"]):
        digest.update(prepush.git_output(command))
        digest.update(b"\0")
    untracked = prepush.nul_paths(["ls-files", "--others", "--exclude-standard", "-z", "--"])
    for path in sorted(untracked):
        file = prepush.ROOT / path
        content = os.fsencode(os.readlink(file)) if file.is_symlink() else file.read_bytes()
        digest.update(path.encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(content).digest())
    return digest.hexdigest()


def commands_for(kind, packages):
    if not packages:
        return []
    flags = (["-o", os.devnull] if len(packages) == 1 else []) if kind == "build" else []
    return [["go", kind, *flags, *sorted(packages)]]


def test_commands_for(packages, filters):
    selected = set(packages)
    filtered = selected & set(filters)
    commands = commands_for("test", selected - filtered)
    commands.extend(["go", "test", "-run", filters[package], package] for package in sorted(filtered))
    return commands


def full(reason):
    return {
        "scope": "full", "reason": reason,
        "goTests": [["go", "test", "-count=1", "./..."]], "goBuilds": [["go", "build", "./..."]],
    }


def select(paths, packages, root, *, repository_tests=(), application_source_tests=()):
    if not paths:
        return full("no diff to classify")
    if not packages or any(p.get("Incomplete") or p.get("Error") or p.get("DepsErrors") for p in packages):
        return full("Go package graph is unavailable or incomplete")

    owners = defaultdict(set)
    labels = {}
    reverse_runtime = defaultdict(set)
    reverse_test = defaultdict(set)
    checkout = Path(root).resolve()
    for p in packages:
        # go list has no canonical package variant suffix without -test; test
        # imports are explicitly included, but never affect runtime builds.
        name = p["ImportPath"]
        # macOS Go reports the real directory even when the checkout was
        # reached through a symlink (including /var -> /private/var).
        directory = Path(p["Dir"]).resolve().relative_to(checkout)
        labels[name] = "." if directory == Path(".") else "./" + directory.as_posix()
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
    available_labels = set(labels.values())
    implicit_tests = set(repository_tests)
    if any(path.startswith(("internal/", "cmd/")) and path.endswith(".go")
           and not path.endswith("_test.go") and "testdata" not in path.split("/")[:-1]
           for path in paths):
        implicit_tests.update(application_source_tests)
    implicit_tests.update(SOURCE_READERS[path] for path in paths if path in SOURCE_READERS)
    if any(path.startswith(EDGE_GROUP_SOURCE_PREFIXES) and path.endswith(".go") and not path.endswith("_test.go")
           for path in paths):
        implicit_tests.add("./internal/declarativerelease")
    implicit_tests.update(
        COMMAND_TEST_CONSUMERS[labels[name]]
        for name in compiled if labels[name] in COMMAND_TEST_CONSUMERS
    )
    # The executable contains the owner's tests as well as its runtime code.
    # A test-only edit or one of its test imports can change the fixture.
    implicit_tests.update(
        TEST_BINARY_CONSUMERS[labels[name]]
        for name in tests if labels[name] in TEST_BINARY_CONSUMERS
    )
    if implicit_tests - available_labels:
        return full("declared implicit test consumer is missing from the Go graph")
    test_packages = {labels[name] for name in tests} - implicit_tests
    package_filters = {}
    if runtime_edits:
        source = labels[next(iter(runtime_edits))]
        for (owner, dependent), pattern in DEPENDENT_TEST_FILTERS.items():
            if source == owner and dependent in test_packages:
                package_filters[dependent] = pattern
    test_commands = test_commands_for(test_packages, package_filters)
    if implicit_tests:
        # In particular, inputs read by a child process are not a reliable
        # cache key for the Go test that invokes it.
        test_commands.append(["go", "test", "-count=1", *sorted(implicit_tests)])
    return {
        "scope": "dependency-closure" if runtime_edits else "test-only",
        "reason": "one package owner; Go imports and declared test inputs resolved",
        "goTests": test_commands,
        "goBuilds": commands_for("build", {labels[name] for name in compiled}),
    }


def graph_command():
    return ["go", "list", "-json=" + ",".join(FIELDS), "./..."]


def report(base, *, target=None):
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
        command = graph_command()
        if target:
            # These profiles mirror the CGO-disabled component container
            # builds. Do not mutate the environment of the release gate.
            goos, goarch = target.split("/")
            command = ["env", f"GOOS={goos}", f"GOARCH={goarch}", "CGO_ENABLED=0", *command]
        status, output = prepush.run(command, 12)
        plan = select(
            paths, packages_from_stream(output) if not status else [], prepush.ROOT,
            repository_tests=REPOSITORY_TEST_CONSUMERS,
            application_source_tests=APPLICATION_SOURCE_TEST_CONSUMERS,
        )
    except (OSError, TypeError, ValueError, KeyError):
        plan = full("Go package graph could not be read")
    planned = {pkg for cmd in plan["goTests"] for pkg in cmd[2:] if not pkg.startswith("-")}
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


def compare_targets(base, targets):
    """Compare plans; no target commands are executed and none can authorize a push."""
    base = prepush.git_output(["rev-parse", "--verify", f"{base}^{{commit}}"]).decode("ascii").strip()
    initial = snapshot(base)
    status, output = prepush.run(["go", "env", "-json", *ENV_FIELDS], 12)
    if status != 0:
        raise RuntimeError("native Go build profile is unavailable")
    native_env = json.loads(output)
    if not isinstance(native_env, dict) or not all(key in native_env for key in ENV_FIELDS):
        raise RuntimeError("native Go build profile is incomplete")
    native = report(base)
    comparisons = []
    for target in dict.fromkeys(targets):
        goos, goarch = target.split("/")
        candidate = report(base, target=target)
        if any(candidate[key] != native[key] for key in ("baseCommit", "sourceCommit", "paths")):
            raise RuntimeError("source or changed paths drifted during target comparison")
        differences = [key for key in (
            "scope", "goTests", "goBuilds", "omittedLegacyPackagesBeforeFallback",
        ) if candidate[key] != native[key]]
        comparisons.append({
            "profile": {**native_env, "GOOS": goos, "GOARCH": goarch, "CGO_ENABLED": "0"},
            "selection": candidate,
            "differencesFromNative": differences,
        })
    if snapshot(base) != initial:
        raise RuntimeError("source changed during target comparison; discard the plans")
    if native["scope"] == "full" or any(
        row["selection"]["scope"] == "full" or row["differencesFromNative"] for row in comparisons
    ):
        disposition = "full-verification-required"
    else:
        disposition = "same-selection-across-requested-profiles"
    return {
        "apiVersion": "prepush.fugue.dev/target-comparison-v1",
        "kind": "GoVerificationTargetComparison",
        "gate": "report-only",
        "productionAuthorization": False,
        "sourceSnapshot": initial,
        "native": {"profile": native_env, "selection": native},
        "comparisons": comparisons,
        "disposition": disposition,
    }


def selection_from_comparison(comparison):
    native = comparison["native"]["selection"]
    if comparison["disposition"] != "same-selection-across-requested-profiles" and native["scope"] != "full":
        return {**native, **full("native and artifact target selections require wider verification")}
    return native


def conservative_report(base):
    """Time and enforce target agreement for a non-authorizing replay."""
    started = time.monotonic()
    result = selection_from_comparison(compare_targets(base, ARTIFACT_PROFILES))
    return {**result, "elapsedMs": round((time.monotonic() - started) * 1000)}


def parse_target(value):
    if not re.fullmatch(r"[a-z0-9]+/[a-z0-9]+", value):
        raise argparse.ArgumentTypeError("target must be a GOOS/GOARCH pair, e.g. linux/amd64")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", help="exact base ref (defaults to PREPUSH_BASE_REF or HEAD)")
    parser.add_argument(
        "--compare-target", action="append", default=[], type=parse_target,
        help="compare the native plan with a CGO-disabled GOOS/GOARCH graph; repeat to add targets",
    )
    options = parser.parse_args()
    try:
        base = options.base or prepush.resolve_base()
        result = compare_targets(base, options.compare_target) if options.compare_target else report(base)
    except (OSError, ValueError, subprocess.CalledProcessError, RuntimeError) as err:
        parser.exit(2, f"shadow report unavailable: {err}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
