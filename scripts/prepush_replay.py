#!/usr/bin/env python3
"""Measure paired warm-cache or incremental shadow runs without a push receipt."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import time

from scripts import prepush, prepush_local, prepush_shadow
from scripts import prepush_shadow_fixture


MIN_SAMPLES = 20


def p95_ms(samples):
    values = sorted(sample["elapsedMs"] for sample in samples)
    return values[math.ceil(0.95 * len(values)) - 1] if values else None


def legacy_sample():
    # The baseline runs the real CLI, but writes its receipt in a private
    # directory. Only durations/status escape as non-authorizing evidence.
    with tempfile.TemporaryDirectory(prefix="fugue-prepush-replay-") as directory:
        receipt_path = os.path.join(directory, "legacy.json")
        started = time.monotonic()
        run = subprocess.run(
            [sys.executable, "-m", "scripts.prepush"],
            cwd=prepush.ROOT,
            env={**os.environ, "PREPUSH_RECEIPT": receipt_path},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        elapsed = round((time.monotonic() - started) * 1000)
        if run.returncode != 0:
            print(run.stderr, file=sys.stderr, end="")
        try:
            with open(receipt_path, encoding="ascii") as file:
                receipt = json.load(file)
        except (OSError, UnicodeError, ValueError):
            receipt = {}
        return {
            "elapsedMs": elapsed,
            "status": "pass" if run.returncode == 0 and receipt.get("status") == "pass" else "fail",
            "checks": receipt.get("checks", {}),
        }


def candidate_sample(*, build_first=False, verify_targets=False):
    results = []
    started = time.monotonic()
    selector = prepush_shadow.conservative_report if verify_targets else prepush_shadow.report
    result = prepush._run_gate(shadow_select=selector, shadow_result=results.append,
                               shadow_build_first=build_first)
    sample = results.pop()
    sample["elapsedMs"] = round((time.monotonic() - started) * 1000)
    if result:
        sample["status"] = "fail"
    return sample


def replay_full_fallback(case, base):
    """Exercise the full local scheduler budget once, outside the P95 samples."""
    initial = prepush_shadow.snapshot(base)
    comparisons, results = [], []
    started = time.monotonic()

    def select(selected_base):
        if selected_base != base:
            raise ValueError("verification base changed")
        comparison = prepush_shadow.compare_targets(base, prepush_shadow.ARTIFACT_PROFILES)
        if comparison["sourceSnapshot"] != initial:
            raise ValueError("source changed before full fallback selection")
        comparisons.append(comparison)
        selected = prepush_shadow.selection_from_comparison(comparison)
        full = prepush_shadow.full("")
        if not selected["paths"] or any(selected[key] != full[key] for key in ("scope", "goTests", "goBuilds")):
            raise ValueError("full fallback witness requires a diff selecting uncached whole-module tests and builds")
        return selected

    code = prepush._run_gate(
        shadow_select=select, shadow_result=results.append, shadow_build_first=True,
        shadow_full_timeout=prepush_local.FULL_VERIFICATION_TIMEOUT_SECONDS,
    )
    result = results.pop()
    if initial != prepush_shadow.snapshot(base):
        raise ValueError("source changed during full fallback replay")
    result["elapsedMs"] = round((time.monotonic() - started) * 1000)
    if code or result["elapsedMs"] >= result["elapsedLimitMs"]:
        result["status"] = "fail"
    return {
        "kind": "FullLocalFallbackWitness",
        "case": case,
        "productionAuthorization": False,
        "sourceSnapshot": initial,
        "targetComparison": comparisons.pop(),
        "result": result,
    }


def go_oracle(timeout):
    command = ["go", "test", "-json", "-count=1", "./..."]
    status, output = prepush.run(command, timeout)
    failed = set()
    failed_tests = set()
    ran = set()
    for line in output.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if not isinstance(event, dict) or not event.get("Package"):
            continue
        if event.get("Action") == "fail":
            failed.add(event["Package"])
            if event.get("Test"):
                failed_tests.add((event["Package"], event["Test"]))
        elif event.get("Action") == "run" and event.get("Test"):
            ran.add(event["Package"])
    return {
        "status": "pass" if status == 0 and not failed and ran else "fail",
        "failedPackages": sorted(failed),
        "failedTests": [{"package": package, "test": test} for package, test in sorted(failed_tests)[:100]],
        "failedTestCount": len(failed_tests),
        "packagesWithExecutedTests": len(ran),
        "timedOut": status == 124,
    }


def summarize(samples, selection, oracle):
    legacy = [pair["legacy"] for pair in samples]
    candidate = [pair["candidate"] for pair in samples]
    warm_replay_pass = (
        len(samples) >= MIN_SAMPLES
        and selection["scope"] != "full"
        and not selection["omittedLegacyPackagesBeforeFallback"]
        and all(s["status"] == "pass" for s in candidate)
        and p95_ms(candidate) <= 60_000
        and oracle is not None
        and oracle["status"] == "pass"
    )
    return {
        "legacyP95Ms": p95_ms(legacy),
        "candidateP95Ms": p95_ms(candidate),
        "legacyFailedSamples": sum(s["status"] != "pass" for s in legacy),
        "candidateFailedSamples": sum(s["status"] != "pass" for s in candidate),
        "warmReplayCriteriaMet": warm_replay_pass,
        "activationReadiness": "not-evaluated",
    }


def summarize_incremental(samples, selection, oracle, target_comparison):
    summary = summarize(samples, selection, oracle)
    timing_criteria_met = summary.pop("warmReplayCriteriaMet")
    targets_agree = target_comparison["disposition"] == "same-selection-across-requested-profiles"
    summary["incrementalReplayCriteriaMet"] = timing_criteria_met and targets_agree
    return summary


@contextmanager
def go_cache(path):
    old = os.environ.get("GOCACHE")
    os.environ["GOCACHE"] = str(path)
    try:
        yield
    finally:
        if old is None:
            os.environ.pop("GOCACHE", None)
        else:
            os.environ["GOCACHE"] = old


def check_selection(sample, expected, label):
    observed = sample.pop("selection")
    for field in ("scope", "paths", "goTests", "goBuilds", "omittedLegacyPackagesBeforeFallback"):
        if observed[field] != expected[field]:
            raise ValueError(f"shadow selection drifted in {label}: {field}")


def replay(case, base, count, run_oracle, oracle_timeout):
    initial = prepush_shadow.snapshot(base)
    # Capture the case's edit paths and graph before warming. Empty/no-op diffs
    # are still reportable via prepush-shadow, but not benchmark cases.
    selection = prepush_shadow.report(base)
    if not selection["paths"]:
        raise ValueError("a replay requires a real, non-empty source diff")
    go_env = subprocess.check_output(
        ["go", "env", "-json", "GOVERSION", "GOOS", "GOARCH", "GOFLAGS", "CGO_ENABLED", "GOMAXPROCS"],
        cwd=prepush.ROOT,
        text=True,
    )
    warmup = {"legacy": legacy_sample(), "candidate": candidate_sample()}
    pairs = []
    for index in range(count):
        variants = [("legacy", legacy_sample), ("candidate", candidate_sample)]
        if index % 2:
            variants.reverse()
        pair = {}
        for name, sample in variants:
            pair[name] = sample()
        check_selection(pair["candidate"], selection, f"sample {index + 1}")
        pairs.append(pair)
    if "selection" in warmup["candidate"]:
        warmup["candidate"].pop("selection")
    oracle = go_oracle(oracle_timeout) if run_oracle else None
    if initial != prepush_shadow.snapshot(base):
        raise ValueError("source changed during replay; discard the samples")
    return {
        "apiVersion": "prepush.fugue.dev/replay-v1",
        "kind": "ShadowExperiment",
        "productionAuthorization": False,
        "case": case,
        "cacheState": "warm-repeated-same-diff",
        "sourceSnapshot": initial,
        "machine": {"system": platform.system(), "release": platform.release(),
                    "machine": platform.machine(), "logicalCPU": os.cpu_count(), "goEnv": json.loads(go_env)},
        "selection": selection,
        "warmup": warmup,
        "samples": pairs,
        "fullTestOracle": oracle,
        "summary": summarize(pairs, selection, oracle),
    }


def replay_incremental(case, base, count, oracle_timeout, *, build_first=False, verify_targets=False):
    # Each policy warms its own baseline cache. Every measured pair uses the
    # same Go source, and neither cache has seen those exact bytes previously.
    if (prepush.git_output(["rev-parse", base]) != prepush.git_output(["rev-parse", "HEAD"])
            or prepush_shadow.git_paths(base)):
        raise ValueError("incremental replay requires a clean checkout at the base commit")
    baseline = prepush_shadow.snapshot(base)
    go_env = subprocess.check_output(
        ["go", "env", "-json", "GOVERSION", "GOOS", "GOARCH", "GOFLAGS", "CGO_ENABLED", "GOMAXPROCS"],
        cwd=prepush.ROOT, text=True,
    )
    warm_commands = [["go", "test", "-run", "^$", "./..."], ["go", "build", "-p", "4", "./..."]]
    pairs = []
    with tempfile.TemporaryDirectory(prefix="fugue-incremental-caches-") as directory:
        caches = {name: Path(directory) / name for name in ("legacy", "candidate")}
        for cache in caches.values():
            cache.mkdir()
            with go_cache(cache):
                for command in warm_commands:
                    status, output = prepush.run(command, oracle_timeout)
                    if status:
                        raise ValueError(f"baseline cache preparation failed for {command}:\n{output}")
        if baseline != prepush_shadow.snapshot(base):
            raise ValueError("baseline warming changed the checkout")
        fixture = prepush_shadow_fixture.CASES[case]
        files = [prepush.ROOT / name for name, _, _ in fixture]
        originals = [path.read_bytes() for path in files]
        selection = None
        target_comparison = None
        try:
            prepush_shadow_fixture.prepare(case)
            edited = [path.read_bytes() for path in files]
            marker = prepush_shadow_fixture.SAMPLE_MARKER.encode("ascii")
            if any(source.count(marker) != 1 for source in edited):
                raise ValueError("each incremental fixture file must contain one sample marker")
            for index in range(count):
                for path, source in zip(files, edited):
                    # Change a test identifier or runtime literal so each edit
                    # reaches the Go compiler output, including importers.
                    path.write_bytes(source.replace(marker, f"fugue_shadow_sample_{index + 1:03}".encode("ascii"), 1))
                snapshot = prepush_shadow.snapshot(base)
                if index == 0:
                    target_comparison = prepush_shadow.compare_targets(base, prepush_shadow.ARTIFACT_PROFILES)
                candidate = candidate_sample
                if build_first or verify_targets:
                    candidate = lambda: candidate_sample(build_first=build_first, verify_targets=verify_targets)
                order = [("legacy", legacy_sample), ("candidate", candidate)]
                if index % 2:
                    order.reverse()
                pair = {"sourceSnapshot": snapshot}
                for name, sample in order:
                    with go_cache(caches[name]):
                        pair[name] = sample()
                if selection is None:
                    selection = dict(pair["candidate"]["selection"])
                    expected = (prepush_shadow.selection_from_comparison(target_comparison)
                                if verify_targets else target_comparison["native"]["selection"])
                    compare_selection = {"selection": expected}
                    check_selection(compare_selection, selection, "native target comparison")
                check_selection(pair["candidate"], selection, f"sample {index + 1}")
                if snapshot != prepush_shadow.snapshot(base):
                    raise ValueError("source changed during incremental replay")
                pairs.append(pair)
            with go_cache(caches["candidate"]):
                oracle = go_oracle(oracle_timeout)
            if pairs[-1]["sourceSnapshot"] != prepush_shadow.snapshot(base):
                raise ValueError("source changed while running the full test oracle")
        finally:
            for path, original in zip(files, originals):
                path.write_bytes(original)
    return {
        "apiVersion": "prepush.fugue.dev/replay-v1",
        "kind": "ShadowExperiment",
        "productionAuthorization": False,
        "case": case,
        "cacheState": "separate-warm-baselines-distinct-edit-inputs",
        "candidateSchedule": "dependency-build-first" if build_first else "parallel",
        "candidateTargetVerification": "per-sample" if verify_targets else "first-sample-outside-budget",
        "baselineSnapshot": baseline,
        "baselineWarmCommands": warm_commands,
        "machine": {"system": platform.system(), "release": platform.release(),
                    "machine": platform.machine(), "logicalCPU": os.cpu_count(), "goEnv": json.loads(go_env)},
        "selection": selection,
        "targetComparison": target_comparison,
        "samples": pairs,
        "fullTestOracle": oracle,
        "summary": summarize_incremental(pairs, selection, oracle, target_comparison),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", required=True, help="representative edit class being replayed")
    parser.add_argument("--base", default=prepush.resolve_base())
    parser.add_argument("--samples", type=int, default=MIN_SAMPLES)
    parser.add_argument("--full-oracle", action="store_true", help="also run all Go tests uncached")
    parser.add_argument("--oracle-timeout", type=int, default=600)
    parser.add_argument("--incremental", action="store_true", help="replay distinct edits against two separate caches")
    parser.add_argument("--build-first", action="store_true", help="test dependency-based compile ordering in shadow")
    parser.add_argument("--verify-targets", action="store_true", help="include artifact graph verification in each candidate timing")
    parser.add_argument("--full-fallback-witness", action="store_true",
                        help="run one broad verification with the full local timeout, without a receipt")
    options = parser.parse_args()
    if options.samples < 1 or options.oracle_timeout < 1:
        parser.error("samples and oracle-timeout must be positive")
    if options.incremental and options.case not in prepush_shadow_fixture.CASES:
        parser.error("--incremental requires a known representative edit case")
    if (options.build_first or options.verify_targets) and not options.incremental:
        parser.error("--build-first and --verify-targets require --incremental")
    if options.full_fallback_witness and (options.incremental or options.full_oracle):
        parser.error("--full-fallback-witness cannot be combined with timing or oracle modes")
    try:
        os.environ["PREPUSH_BASE_REF"] = prepush.git_output(
            ["rev-parse", "--verify", f"{options.base}^{{commit}}"],
        ).decode("ascii").strip()
        # Budget overrides for release-scale changes cannot qualify as evidence
        # about the normal gate. Keep both variants on the same measured budget.
        os.environ.pop("PREPUSH_TIMEOUT_SECONDS", None)
        os.environ.pop("PREPUSH_MAX_ELAPSED_SECONDS", None)
        if options.full_fallback_witness:
            result = replay_full_fallback(options.case, os.environ["PREPUSH_BASE_REF"])
        elif options.incremental:
            result = replay_incremental(options.case, os.environ["PREPUSH_BASE_REF"],
                                        options.samples, options.oracle_timeout, build_first=options.build_first,
                                        verify_targets=options.verify_targets)
        else:
            result = replay(options.case, os.environ["PREPUSH_BASE_REF"], options.samples,
                            options.full_oracle, options.oracle_timeout)
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as err:
        parser.exit(2, f"shadow replay unavailable: {err}\n")
    print(json.dumps(result, sort_keys=True))
    return int(options.full_fallback_witness and result["result"]["status"] != "pass")


if __name__ == "__main__":
    raise SystemExit(main())
