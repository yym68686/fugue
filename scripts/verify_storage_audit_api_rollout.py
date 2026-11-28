"""Collect read-only storage audit continuity evidence around legacy API cleanup."""

import argparse
from datetime import datetime
import json
from pathlib import Path
import time

from scripts.provision_observation_worker_database import ProvisionError
from scripts.provision_storage_lifecycle_audit import StorageAuditProvision
from scripts.verify_observation_writer_handoff import COMMIT, IMAGE, matches, verify_workload
from scripts.verify_storage_audit_activation import (
    activated_status, api_audit_metrics_retired, has_committed_sample, parse_declaration,
    verify_worker_template,
)
from scripts.verify_storage_audit_worker import prometheus_pods, scrape_observed


def continuity(provision, policy, scrape, declaration, verify_api, previous=None, wait_seconds=120):
    provision.verify_published_policy(policy)
    primary = provision.find_primary()
    secret_uid, _ = provision.credential(provision.get("secret", provision.config["secret"]))
    worker = verify_workload(provision, declaration["worker"], verify_template=lambda template, component:
                             verify_worker_template(template, component, provision.config["secret"]))
    api = verify_api()
    if previous and (previous["workerPod"] != list(worker[2][0]) or
                     previous["primary"] != list(primary) or
                     {tuple(pod) for pod in previous["apiPods"]} & set(api[2])):
        raise ProvisionError("audit worker changed or a preceding API pod is still serving")
    collectors = prometheus_pods(provision, scrape)
    deadline = time.monotonic() + wait_seconds
    while True:
        state = activated_status(provision, primary[0], declaration, secret_uid)
        advanced = previous is None or (
            state["completedPasses"] > previous["completedPasses"] and state["sampleAt"] and
            datetime.fromisoformat(state["sampleAt"]) > datetime.fromisoformat(previous["sampleAt"])
        )
        if (advanced and has_committed_sample(state) and
                all(api_audit_metrics_retired(provision, pod) for pod in api[2]) and
                all(scrape_observed(provision, pod, worker[2][0], "worker", published=True) for pod in collectors)):
            break
        if time.monotonic() >= deadline:
            raise ProvisionError("independent audit progress did not converge during the API rollout")
        time.sleep(5)
    if (verify_api() != api or provision.find_primary() != primary or
            verify_workload(provision, declaration["worker"]) != worker or
            prometheus_pods(provision, scrape) != collectors):
        raise ProvisionError("API, audit worker or database changed while auditing continuity")
    provision.verify_published_policy(policy)
    return {**state, "primary": primary, "workerPod": worker[2][0], "apiPods": api[2], "prometheusPods": collectors}


def api_target(declaration, intent, receipt):
    workload = declaration["api"]
    repository = workload["image"].split("@", 1)[0]
    previous_revision = intent["expectedPreviousOciRevision"]
    previous_image = repository + "@" + intent["expectedPreviousImageDigest"]
    if (intent["apiVersion"] != "release.fugue.dev/v2" or intent["kind"] != "ProductionComponentIntent"
            or intent["component"] != "api" or intent["expectedPreviousPresent"] is not True
            or not matches(previous_revision, COMMIT) or not matches(previous_image, IMAGE)
            or not matches(intent["expectedPreviousConfigSha"], COMMIT)
            or not matches(intent["expectedPreviousManifestSha"], COMMIT)):
        raise ProvisionError("API release intent has no verified predecessor identity")
    if receipt is None:
        return {**workload, "revision": previous_revision, "image": previous_image}
    revision, image = receipt["sourceSha"], receipt["immutableRef"]
    if (receipt["apiVersion"] != "release.fugue.dev/v2" or
            receipt["kind"] != "ImmutableComponentArtifactReceipt" or receipt["component"] != "api" or
            not matches(revision, COMMIT) or not matches(image, IMAGE) or
            receipt["configSha"] != revision or receipt["ociRevision"] != revision or
            image.split("@", 1)[0] != repository or revision == previous_revision):
        raise ProvisionError("API artifact receipt is outside the cleanup rollout")
    return {**workload, "revision": revision, "image": image}


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
        policy = json.loads(args.policy.read_text())
        provision = StorageAuditProvision(json.loads(args.database.read_text()), policy)
        declaration = parse_declaration(json.loads(args.activation.read_text()))
        intent = json.loads(args.api_intent.read_text())
        receipt = json.loads(args.api_receipt.read_text()) if args.api_receipt else None
        target = api_target(declaration, intent, receipt)
        result = continuity(provision, policy, json.loads(args.scrape.read_text()), declaration,
                            lambda: verify_workload(provision, target),
                            json.loads(args.before.read_text()) if args.before else None)
        print(json.dumps(result, sort_keys=True))
    except ProvisionError as err:
        parser.exit(1, f"storage audit continuity verification stopped: {err}\n")
    except (AttributeError, KeyError, TypeError, ValueError, OSError):
        parser.exit(1, "storage audit continuity verification received missing or malformed evidence\n")


if __name__ == "__main__":
    main()
