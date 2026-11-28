"""Run dependency-scoped local checks; never issue a production receipt."""

import json
import os
import subprocess
import sys
import time

from scripts import prepush, prepush_shadow

FULL_VERIFICATION_TIMEOUT_SECONDS = 360


def main() -> int:
    if any(name in os.environ for name in ("CI", "GITHUB_ACTIONS", "PREPUSH_RECEIPT")):
        print("prepush-local is for local use only; CI must run make prepush", file=sys.stderr)
        return 2

    started = time.monotonic()
    results = []
    try:
        requested_base = os.environ.get("PREPUSH_BASE_REF", "").strip()
        if requested_base:
            status, _ = prepush.run(["git", "rev-parse", "--verify", "--end-of-options",
                                     requested_base + "^{commit}"], 5)
            if status:
                raise ValueError("PREPUSH_BASE_REF does not resolve to a commit")
        base = prepush.resolve_base()
        initial = prepush_shadow.snapshot(base)

        def select(selected_base):
            if selected_base != base:
                raise RuntimeError("verification base changed")
            try:
                return prepush_shadow.conservative_report(base)
            except (OSError, TypeError, ValueError, KeyError, RuntimeError, subprocess.CalledProcessError):
                return prepush_shadow.full("native and artifact profiles could not be verified")

        code = prepush._run_gate(
            shadow_select=select, shadow_result=results.append, shadow_build_first=True,
            shadow_full_timeout=FULL_VERIFICATION_TIMEOUT_SECONDS,
        )
        result = results.pop()
        final = prepush_shadow.snapshot(base)
        elapsed = round((time.monotonic() - started) * 1000)
        result["elapsedMs"] = elapsed
        if initial != final:
            result["status"] = "fail"
            result["diagnostic"] = "source changed during local verification; rerun the checks"
            code = 1
        elif code == 0 and elapsed >= result["elapsedLimitMs"]:
            result["status"] = "fail"
            result["diagnostic"] = "local verification exceeded the elapsed-time budget"
            code = 1
    except (OSError, TypeError, ValueError, KeyError, RuntimeError, subprocess.CalledProcessError) as err:
        print(f"prepush-local could not verify this diff: {err}", file=sys.stderr)
        return 2
    print(json.dumps({
        "kind": "LocalVerification",
        "productionAuthorization": False,
        "sourceSnapshot": initial,
        **result,
    }, sort_keys=True))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
