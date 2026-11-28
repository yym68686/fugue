"""Verify sample retention stays active across replacement of all API Pods."""

import argparse
from datetime import datetime
import json
from pathlib import Path
import time

from scripts.apply_observation_sample_retention_handoff import (
    all_active, COLLECTOR_EXPRESSION, parse_release, read_progress, verify_policy,
)
from scripts.apply_observation_writer_handoff import verify_collector
from scripts.provision_observation_worker_database import Provision, ProvisionError
from scripts.verify_observation_writer_handoff import verify_workload
from scripts.verify_storage_audit_api_rollout import api_target


def advanced(progress, previous):
    return all(
        row["transferAt"] == previous[target]["transferAt"]
        and row["completedPasses"] > previous[target]["completedPasses"]
        and datetime.fromisoformat(row["completedAt"]) > datetime.fromisoformat(previous[target]["completedAt"])
        for target, row in progress.items()
    )


def continuity(provision, policy, scrape, worker_target, api, previous=None, wait_seconds=120):
    primary = provision.find_primary()
    policy_identity = verify_policy(provision, policy)
    worker = verify_workload(provision, worker_target)
    serving = verify_workload(provision, api)
    if previous and (
        previous["primary"] != list(primary)
        or previous["policy"] != list(policy_identity)
        or previous["worker"] != [list(pod) for pod in worker[2]]
        or {tuple(pod) for pod in previous["api"]} & set(serving[2])
    ):
        raise ProvisionError("sample worker, policy or database changed, or a preceding API pod is still serving")
    deadline = time.monotonic() + wait_seconds
    while True:
        progress = read_progress(provision, primary[0])
        if not all_active(progress):
            raise ProvisionError("sample retention owner or database fence changed")
        if all(row["completedAt"] and row["completedPasses"] > 0 for row in progress.values()) and (
            previous is None or advanced(progress, previous["progress"])
        ):
            break
        if time.monotonic() >= deadline:
            raise ProvisionError("not all sample retention targets committed progress across the API rollout")
        time.sleep(5)
    verify_collector(provision, scrape, [pod for pod, _ in worker[2]], COLLECTOR_EXPRESSION)
    if (provision.find_primary() != primary or verify_policy(provision, policy) != policy_identity
            or verify_workload(provision, worker_target) != worker or verify_workload(provision, api) != serving):
        raise ProvisionError("sample or API runtime identity changed while observing retention")
    return {"primary": primary, "policy": policy_identity, "worker": worker[2], "api": serving[2], "progress": progress}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("database", "activation", "policy", "scrape"):
        parser.add_argument(name, type=Path)
    parser.add_argument("--api-intent", type=Path, required=True)
    parser.add_argument("--api-receipt", type=Path)
    parser.add_argument("--before", type=Path)
    args = parser.parse_args()
    if (args.before is None) != (args.api_receipt is None):
        parser.error("the final observation requires both the baseline and the exact API receipt")
    try:
        release = parse_release(json.loads(args.activation.read_text()))
        intent = json.loads(args.api_intent.read_text())
        receipt = json.loads(args.api_receipt.read_text()) if args.api_receipt else None
        result = continuity(Provision(json.loads(args.database.read_text())), json.loads(args.policy.read_text()),
                            json.loads(args.scrape.read_text()), release["worker"], api_target(release, intent, receipt),
                            json.loads(args.before.read_text()) if args.before else None)
        print(json.dumps(result, sort_keys=True))
    except ProvisionError as err:
        parser.exit(1, f"sample retention rollout verification stopped: {err}\n")
    except (AttributeError, KeyError, TypeError, ValueError, OSError):
        parser.exit(1, "sample retention rollout verification received missing or malformed evidence\n")


if __name__ == "__main__":
    main()
