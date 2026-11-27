#!/usr/bin/env python3
"""Run a paired warm-cache shadow experiment; never issue a push receipt."""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import subprocess
import sys
import tempfile
import time

from scripts import prepush, prepush_shadow


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


def candidate_sample():
    results = []
    started = time.monotonic()
    result = prepush._run_gate(shadow_select=prepush_shadow.report, shadow_result=results.append)
    sample = results.pop()
    sample["elapsedMs"] = round((time.monotonic() - started) * 1000)
    if result:
        sample["status"] = "fail"
    return sample


def go_oracle(timeout):
    command = ["go", "test", "-json", "-count=1", "./..."]
    status, output = prepush.run(command, timeout)
    failed = set()
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
        elif event.get("Action") == "run" and event.get("Test"):
            ran.add(event["Package"])
    return {
        "status": "pass" if status == 0 and not failed and ran else "fail",
        "failedPackages": sorted(failed),
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
        observed = pair["candidate"].pop("selection")
        for field in ("scope", "paths", "goTests", "goBuilds", "omittedLegacyPackagesBeforeFallback"):
            if observed[field] != selection[field]:
                raise ValueError(f"shadow selection drifted in sample {index + 1}: {field}")
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", required=True, help="representative edit class being replayed")
    parser.add_argument("--base", default=prepush.resolve_base())
    parser.add_argument("--samples", type=int, default=MIN_SAMPLES)
    parser.add_argument("--full-oracle", action="store_true", help="also run all Go tests uncached")
    parser.add_argument("--oracle-timeout", type=int, default=600)
    options = parser.parse_args()
    if options.samples < 1 or options.oracle_timeout < 1:
        parser.error("samples and oracle-timeout must be positive")
    try:
        os.environ["PREPUSH_BASE_REF"] = prepush.git_output(
            ["rev-parse", "--verify", f"{options.base}^{{commit}}"],
        ).decode("ascii").strip()
        # Budget overrides for release-scale changes cannot qualify as evidence
        # about the normal gate. Keep both variants on the same measured budget.
        os.environ.pop("PREPUSH_TIMEOUT_SECONDS", None)
        os.environ.pop("PREPUSH_MAX_ELAPSED_SECONDS", None)
        result = replay(options.case, os.environ["PREPUSH_BASE_REF"], options.samples,
                        options.full_oracle, options.oracle_timeout)
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as err:
        parser.exit(2, f"shadow replay unavailable: {err}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
