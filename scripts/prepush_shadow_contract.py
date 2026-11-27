#!/usr/bin/env python3
"""Verify a synthetic regression selects and fails the known downstream contract."""

import argparse
import json
import sys

from scripts import prepush, prepush_shadow, prepush_shadow_fixture


def exact_outcome(output, package, test, action, diagnostic=""):
    outcomes = set()
    messages = []
    for line in output.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if not isinstance(event, dict) or event.get("Package") != package:
            continue
        name = event.get("Test")
        if name is None or name == test:
            outcomes.add((name, event.get("Action")))
        if name == test and event.get("Action") == "output":
            messages.append(event.get("Output", ""))
    return ((None, action) in outcomes and (test, action) in outcomes
            and diagnostic in "".join(messages))


def witness(case, timeout=900):
    (path, _, _), consumer, test, diagnostic = prepush_shadow_fixture.CONTRACTS[case]
    if prepush.git_output(["status", "--porcelain"]):
        raise ValueError("contract witness requires a clean disposable checkout")
    package = "fugue" + consumer[1:]
    base = prepush.git_output(["rev-parse", "HEAD"]).decode("ascii").strip()
    command = ["go", "test", "-json", "-count=1", "-run", f"^{test}$", consumer]
    status, before = prepush.run(command, timeout)
    if status != 0 or not exact_outcome(before, package, test, "pass"):
        raise ValueError(f"exact downstream contract did not pass before the edit:\n{before}")

    prepush_shadow_fixture.prepare(case)
    # Compare plans before executing the isolated contract. Full fallback or a
    # command that could reuse a cached result cannot prove the declared edge.
    snapshot = prepush_shadow.snapshot(base)
    plans = {target or "native": prepush_shadow.report(base, target=target)
             for target in (None, "linux/amd64", "linux/arm64")}
    for profile, selection in plans.items():
        if (selection["scope"] != "dependency-closure"
                or selection["paths"] != [path]
                or selection["omittedLegacyPackagesBeforeFallback"]
                or not any(consumer in cmd and "-count=1" in cmd for cmd in selection["goTests"])):
            raise ValueError(f"{profile} did not select the exact uncached downstream contract: {selection}")

    status, after = prepush.run(command, timeout)
    if status != 1 or not exact_outcome(after, package, test, "fail", diagnostic):
        raise ValueError(f"edit did not fail the intended downstream contract:\n{after}")
    if snapshot != prepush_shadow.snapshot(base):
        raise ValueError("source changed while executing the contract witness")
    return {
        "apiVersion": "prepush.fugue.dev/contract-witness-v1",
        "kind": "ShadowContractWitness",
        "productionAuthorization": False,
        "case": case,
        "sourceSnapshot": snapshot,
        "consumer": consumer,
        "test": test,
        "expectedDiagnostic": diagnostic,
        "command": command,
        "baseline": {"status": "pass", "events": before.splitlines()},
        "regression": {"status": "failed-as-expected", "events": after.splitlines()},
        "plans": plans,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=prepush_shadow_fixture.CONTRACTS, required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(witness(args.case), sort_keys=True))
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"shadow contract witness failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
