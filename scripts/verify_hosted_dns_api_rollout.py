"""Confirm the independent Hosted DNS executor stayed active across API cleanup."""

import argparse
from datetime import datetime
import json
from pathlib import Path
import time

from scripts.apply_hosted_dns_writer_handoff import observe, parse_declaration, scrape_observed
from scripts.provision_hosted_dns_worker import HostedDNSProvision
from scripts.provision_observation_worker_database import ProvisionError
from scripts.verify_hosted_dns_worker import verify_template
from scripts.verify_observation_writer_handoff import verify_workload
from scripts.verify_storage_audit_api_rollout import api_target
from scripts.verify_storage_audit_worker import prometheus_pods


def continuity(provision, declaration, scrape, api, previous=None, wait_seconds=180):
    primary = provision.find_primary()
    worker_target = declaration["worker"]
    worker = verify_workload(provision, worker_target, verify_template=verify_template)
    pod = worker[2][0]
    serving = verify_workload(provision, api)
    collectors = prometheus_pods(provision, scrape)
    if previous is not None and (
        previous["primary"] != list(primary)
        or previous["workerPod"] != list(pod)
        or {uid for _, uid in previous["apiPods"]} & {uid for _, uid in serving[2]}
    ):
        raise ProvisionError("Hosted DNS worker or database changed, or a preceding API Pod is still serving")

    deadline = time.monotonic() + wait_seconds
    while True:
        state = observe(provision, primary[0])
        if state["owner"] != "worker" or not state["transferAt"] or (
            previous is not None and state["transferAt"] != previous["transferAt"]
        ):
            raise ProvisionError("Hosted DNS writer ownership changed during API cleanup")
        completed = state["completedAt"]
        threshold = state["transferAt"] if previous is None else previous["completedAt"]
        if (completed and datetime.fromisoformat(completed) > datetime.fromisoformat(threshold)
                and all(scrape_observed(provision, collector, pod, "worker", completed) for collector in collectors)):
            break
        if time.monotonic() >= deadline:
            raise ProvisionError("no independently observed Hosted DNS progress across the API rollout")
        time.sleep(5)

    if (provision.find_primary() != primary or verify_workload(provision, api) != serving
            or verify_workload(provision, worker_target, verify_template=verify_template) != worker
            or prometheus_pods(provision, scrape) != collectors):
        raise ProvisionError("runtime identity changed while verifying Hosted DNS progress")
    return {**state, "primary": primary, "workerPod": pod, "apiPods": serving[2], "prometheusPods": collectors}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("database", "handoff", "scrape"):
        parser.add_argument(name, type=Path)
    parser.add_argument("--api-intent", type=Path, required=True)
    parser.add_argument("--api-receipt", type=Path)
    parser.add_argument("--before", type=Path)
    args = parser.parse_args()
    if (args.before is None) != (args.api_receipt is None):
        parser.error("final observation requires both the baseline and the exact API receipt")
    try:
        provision = HostedDNSProvision(json.loads(args.database.read_text()))
        declaration = parse_declaration(json.loads(args.handoff.read_text()))
        intent = json.loads(args.api_intent.read_text())
        receipt = json.loads(args.api_receipt.read_text()) if args.api_receipt else None
        if receipt is None and api_target(declaration, intent, None) != declaration["api"]:
            raise ProvisionError("cleanup does not follow the API that was verified at handoff")
        if receipt is not None and receipt["immutableRef"].split("@", 1)[-1] != receipt["topDigest"]:
            raise ProvisionError("cleanup receipt disagrees with the API artifact digest")
        result = continuity(provision, declaration, json.loads(args.scrape.read_text()),
                            api_target(declaration, intent, receipt),
                            json.loads(args.before.read_text()) if args.before else None)
        print(json.dumps(result, sort_keys=True))
    except ProvisionError as err:
        parser.exit(1, f"Hosted DNS API cleanup observation stopped: {err}\n")
    except (AttributeError, KeyError, TypeError, ValueError, OSError):
        parser.exit(1, "Hosted DNS API cleanup observation received malformed evidence\n")


if __name__ == "__main__":
    main()
