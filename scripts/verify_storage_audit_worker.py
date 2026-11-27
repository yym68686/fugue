"""Read-only evidence that the exact storage audit release is independently scraped."""

import argparse
import json
from pathlib import Path
import time
from urllib.parse import urlencode

from scripts.provision_observation_worker_database import NAME, UID, ProvisionError
from scripts.provision_storage_lifecycle_audit import StorageAuditProvision
from scripts.verify_observation_writer_handoff import COMMIT, IMAGE, controlled_by, matches, verify_workload

COMPONENT = "storage-audit-worker"
JOB = "fugue-storage-audit-worker"
DEPLOYMENT = "fugue-storage-audit-worker"


def release_component(receipt, head):
    if (not matches(head, COMMIT) or receipt.get("apiVersion") != "release.fugue.dev/v2" or
            receipt.get("kind") != "ImmutableComponentArtifactReceipt" or receipt.get("component") != COMPONENT or
            any(receipt.get(field) != head for field in ("sourceSha", "configSha", "ociRevision")) or
            not matches(receipt.get("immutableRef"), IMAGE)):
        raise ProvisionError("storage audit artifact is not bound to the current release")
    return {"deployment": DEPLOYMENT, "container": COMPONENT, "image": receipt["immutableRef"], "revision": head, "replicas": 1}


def database_status(provision, primary, standby):
    raw = provision.sql(primary, f"""
BEGIN READ ONLY;
SET LOCAL statement_timeout = '3s';
{provision.identity_sql()}
SELECT json_build_object('owner', owner, 'policyDigest', policy_sha256,
    'completedPasses', completed_passes, 'failed', last_run_failed, 'hasSample', sample IS NOT NULL)
FROM fugue_storage_audit.lifecycle_status_v1;
COMMIT;
""")
    identity, record = raw.splitlines()
    state = json.loads(record)
    if (identity != "1" or state["owner"] not in ("api", "worker") or state["policyDigest"] != provision.policy_digest or
            type(state["completedPasses"]) is not int or state["completedPasses"] < 0 or state["failed"] is not False):
        raise ProvisionError("storage audit progress or policy does not match the release")
    if standby and state["owner"] != "api":
        raise ProvisionError("the initial storage audit release is not in standby")
    if state["owner"] == "api" and (state["completedPasses"] != 0 or state["hasSample"] is not False):
        raise ProvisionError("a standby storage audit executor has published a sample")
    return state


def prometheus_pods(provision, policy):
    if policy["namespace"] != provision.config["namespace"] or not matches(policy["deployment"], NAME):
        raise ProvisionError("invalid Prometheus workload identity")
    deployment = provision.get("deployment", policy["deployment"])
    selector = deployment["spec"]["selector"]
    if set(selector) != {"matchLabels"} or not selector["matchLabels"]:
        raise ProvisionError("Prometheus selector cannot be matched exactly")
    selector = ",".join(f"{k}={v}" for k, v in sorted(selector["matchLabels"].items()))
    # Explicitly bind the query to the owning Deployment, not any Pod
    # advertising the same discovery labels.
    def objects(kind):
        return json.loads(provision.kube("get", kind, "-l", selector, "-o", "json"))["items"]

    sets = {item["metadata"]["uid"] for item in objects("replicasets")
            if controlled_by(item, "Deployment", deployment["metadata"]["uid"])}
    ready = sorted(pod["metadata"]["name"] for pod in objects("pods")
                   if not pod["metadata"].get("deletionTimestamp") and
                   any(controlled_by(pod, "ReplicaSet", uid) for uid in sets) and
                   any(c.get("type") == "Ready" and c.get("status") == "True" for c in pod["status"].get("conditions", [])))
    if not ready or len(ready) != deployment["spec"]["replicas"] or not all(matches(name, NAME) for name in ready):
        raise ProvisionError("no exact Ready Prometheus population to verify storage audit")
    return ready


def scrape_observed(provision, prometheus, worker, owner):
    namespace = provision.config["namespace"]
    name, uid = worker
    if not all(matches(n, NAME) for n in (namespace, prometheus, name)) or not matches(uid, UID):
        raise ProvisionError("invalid storage audit scrape identity")
    labels = f'{{job="{JOB}",namespace="{namespace}",pod="{name}",pod_uid="{uid}"}}'
    match = " and on(job,namespace,pod,pod_uid) "
    predicate = [f"(up{labels} == 1)", f"(time() - timestamp(up{labels}) < 75)",
                 f"(fugue_storage_audit_progress_available{labels} == 1)",
                 f"(fugue_storage_audit_executor_owner{labels} == {int(owner == 'worker')})"]
    if owner == "api":
        predicate.append(f"(fugue_storage_audit_committed_passes{labels} == 0)")
    path = f"/api/v1/namespaces/{namespace}/pods/{prometheus}:9090/proxy/api/v1/query?"
    response = json.loads(provision.kube("get", "--raw", path + urlencode({"query": match.join(predicate)})))
    data = response.get("data", {})
    if response.get("status") != "success" or data.get("resultType") != "vector":
        raise ProvisionError("storage audit Prometheus query did not return a vector")
    results = data["result"]
    return (len(results) == 1 and results[0]["value"][1] == "1" and
            all(results[0]["metric"].get(k) == v for k, v in
                (("job", JOB), ("namespace", namespace), ("pod", name), ("pod_uid", uid))))


def witness(provision, policy_document, scrape_policy, component, *, standby, deadline_seconds=100):
    provision.verify_published_policy(policy_document)
    worker = verify_workload(provision, component)
    primary = provision.find_primary()
    before = database_status(provision, primary[0], standby)
    prometheus = prometheus_pods(provision, scrape_policy)
    worker_pod = worker[2][0]
    deadline = time.monotonic() + deadline_seconds
    pending = prometheus
    while pending:
        pending = [pod for pod in pending if not scrape_observed(provision, pod, worker_pod, before["owner"])]
        if pending:
            if time.monotonic() >= deadline:
                raise ProvisionError("the exact audit worker did not appear in Prometheus with the expected owner")
            time.sleep(4)
    if (verify_workload(provision, component) != worker or provision.find_primary() != primary or
            prometheus_pods(provision, scrape_policy) != prometheus):
        raise ProvisionError("the storage audit workload or database primary changed while being observed")
    provision.verify_published_policy(policy_document)
    after = database_status(provision, primary[0], standby)
    if after["owner"] != before["owner"] or after["completedPasses"] < before["completedPasses"]:
        raise ProvisionError("storage audit ownership or progress changed unexpectedly")
    return {"revision": component["revision"], "image": component["image"], "workerPod": worker_pod,
            "prometheusPods": prometheus, **after}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("policy", type=Path)
    parser.add_argument("scrape_policy", type=Path)
    parser.add_argument("artifact_receipt", type=Path)
    parser.add_argument("revision")
    parser.add_argument("--standby", action="store_true")
    args = parser.parse_args()
    try:
        policy = json.loads(args.policy.read_text())
        provision = StorageAuditProvision(json.loads(args.database.read_text()), policy)
        component = release_component(json.loads(args.artifact_receipt.read_text()), args.revision)
        print(json.dumps(witness(provision, policy, json.loads(args.scrape_policy.read_text()),
                                 component, standby=args.standby), sort_keys=True))
    except ProvisionError as err:
        parser.exit(1, f"storage audit observation refused: {err}\n")
    except (AttributeError, KeyError, TypeError, ValueError, OSError):
        parser.exit(1, "storage audit observation received missing or malformed evidence\n")


if __name__ == "__main__":
    main()
