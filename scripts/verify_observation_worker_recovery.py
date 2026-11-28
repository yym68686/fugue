"""Observe durable work on an already deployed observation worker; never apply changes."""

import argparse
from datetime import datetime
import json
from pathlib import Path
import time

from scripts.apply_observation_sample_retention_handoff import (
    all_active, COLLECTOR_EXPRESSION as SAMPLE_METRICS, read_progress as sample_progress, verify_policy,
)
from scripts.apply_observation_writer_handoff import verify_collector, verify_committed_progress
from scripts.apply_quality_evidence_retention_handoff import (
    COLLECTOR_EXPRESSION as EVIDENCE_METRICS, read_progress as evidence_progress,
)
from scripts.provision_observation_worker_database import Provision, ProvisionError
from scripts.verify_observation_rollback_floor import checked_rollback
from scripts.verify_observation_writer_handoff import (
    COMMIT, IMAGE, NAME, matches, observe_database, verify_workload,
)


def parse_request(data):
    if not isinstance(data, dict) or data.keys() != {"worker", "notBefore"}:
        raise ValueError("invalid recovery observation declaration")
    worker = data["worker"]
    if (not isinstance(worker, dict) or worker.keys() != {"deployment", "container", "image", "revision", "replicas"}
            or not matches(worker["deployment"], NAME) or worker["container"] != "observation-worker"
            or not matches(worker["image"], IMAGE) or not matches(worker["revision"], COMMIT)
            or type(worker["replicas"]) is not int or worker["replicas"] != 1):
        raise ValueError("invalid recovery worker identity")
    cutoff = datetime.fromisoformat(data["notBefore"])
    if cutoff.tzinfo is None:
        raise ValueError("recovery cutoff must include a UTC offset")
    return worker, cutoff


def maintenance_active(samples, evidence):
    return all_active(samples) and (
        evidence["owner"] == "worker" and evidence["fence"] is True and evidence["transferAt"]
    )


def maintenance_advanced(now, before):
    return (now["transferAt"] == before["transferAt"]
            and now["completedPasses"] > before["completedPasses"]
            and now["completedAt"] is not None and before["completedAt"] is not None
            and datetime.fromisoformat(now["completedAt"]) > datetime.fromisoformat(before["completedAt"]))


def verify_recovery(provision, worker, cutoff, scrape, policy, *, retention_wait_seconds=90):
    live = verify_workload(provision, worker)
    rollback = checked_rollback(provision, "observation-worker", worker)
    primary = provision.find_primary()
    policy_identity = verify_policy(provision, policy)
    before = observe_database(provision, primary[0])
    samples = sample_progress(provision, primary[0])
    evidence = evidence_progress(provision, primary[0])
    if before["owner"] != "worker" or not maintenance_active(samples, evidence):
        raise ProvisionError("durable observation ownership or retention fence is missing")
    # Demand fresh commits in this observation, not just a current Ready Pod
    # and old durable counters from an earlier release.
    observed = max(time.time(), cutoff.timestamp())
    rollup = verify_committed_progress(provision, {"worker": worker}, primary, before, observed)
    deadline = time.monotonic() + retention_wait_seconds
    while True:
        last_samples = sample_progress(provision, primary[0])
        last_evidence = evidence_progress(provision, primary[0])
        if not maintenance_active(last_samples, last_evidence):
            raise ProvisionError("durable observation ownership or retention fence changed")
        if (all(maintenance_advanced(row, samples[key]) for key, row in last_samples.items())
                and maintenance_advanced(last_evidence, evidence)):
            break
        if time.monotonic() >= deadline:
            raise ProvisionError("not all retention lanes advanced under the same durable ownership")
        time.sleep(5)
    if rollup["podUID"] != live[2][0][1]:
        raise ProvisionError("rollup success came from a different Pod")
    for expression in (None, SAMPLE_METRICS, EVIDENCE_METRICS):
        verify_collector(provision, scrape, [name for name, _ in live[2]], expression)
    if (provision.find_primary() != primary or verify_workload(provision, worker) != live
            or checked_rollback(provision, "observation-worker", worker) != rollback
            or verify_policy(provision, policy) != policy_identity):
        raise ProvisionError("database, worker, policy or rollback changed during observation")
    return {"worker": worker, "rollback": {"revision": rollback[0], "image": rollback[1]},
            "rollup": {"before": before, "after": rollup},
            "sampleRetention": {"before": samples, "after": last_samples},
            "qualityEvidence": {"before": evidence, "after": last_evidence},
            "ownershipChanged": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("database", "observation", "scrape", "policy"):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    try:
        worker, cutoff = parse_request(json.loads(args.observation.read_text()))
        result = verify_recovery(Provision(json.loads(args.database.read_text())), worker, cutoff,
                                 json.loads(args.scrape.read_text()), json.loads(args.policy.read_text()))
    except ProvisionError as err:
        parser.exit(1, f"observation worker recovery check stopped: {err}\n")
    except (KeyError, TypeError, ValueError, OSError, AttributeError):
        parser.exit(1, "observation worker recovery check received missing or malformed evidence\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
