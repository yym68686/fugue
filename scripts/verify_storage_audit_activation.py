"""Read-only admission: prove the running storage-audit writers and rollback floors."""

import argparse
import hashlib
import json
import re
import time
from datetime import datetime
from pathlib import Path

from scripts.apply_quality_evidence_retention_handoff import check_source
from scripts.provision_observation_worker_database import NAME, UID, ProvisionError
from scripts.provision_storage_lifecycle_audit import StorageAuditProvision
from scripts.verify_observation_rollback_floor import checked_rollback
from scripts.verify_observation_writer_handoff import COMMIT, IMAGE, RELEASE_SCHEMA, load_release, matches, verify_workload
from scripts.verify_storage_audit_worker import COMPONENT, DEPLOYMENT, database_status, prometheus_pods, scrape_observed, witness


SCHEMA = "fugue.storage-audit-activation/v1"
API = "fugue-fugue-api", "api", 2
WORKER = DEPLOYMENT, COMPONENT, 1
FLAG = "FUGUE_STORAGE_LIFECYCLE_HANDOFF_ENABLED"
API_PATHS = (
    "cmd/fugue-api/main.go", "internal/store/storage_lifecycle.go",
    "internal/store/storage_lifecycle_handoff.go", "internal/store/storage_lifecycle_metrics.go",
    "internal/store/storage_policy.go", "internal/storageaudit/observation.go", "internal/storageaudit/metrics.go",
)
WORKER_PATHS = ("Dockerfile.storage-audit-worker", "cmd/fugue-storage-audit-worker", "internal/storageaudit")


def declaration_digest(declaration):
    return hashlib.sha256(json.dumps(declaration, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def parse_declaration(raw):
    targets = {"api", "worker", "apiRollback", "workerRollback"}
    if (not isinstance(raw, dict) or raw.keys() != {"schema", "policyDigest", *targets}
            or raw["schema"] != SCHEMA or not matches(raw["policyDigest"], r"[a-f0-9]{64}")):
        raise ProvisionError("invalid storage audit activation declaration")
    checked = {"schema": RELEASE_SCHEMA, **{name: raw[name] for name in ("api", "worker")}}
    load_release(checked)
    for name, identity in (("api", API), ("worker", WORKER)):
        if tuple(raw[name][key] for key in ("deployment", "container", "replicas")) != identity:
            raise ProvisionError("activation declaration does not name the dedicated executors")
        rollback = raw[name + "Rollback"]
        if name == "worker" and rollback is None:
            continue  # The initial deployment's pinned rollback is an empty resource set.
        if (not isinstance(rollback, dict) or rollback.keys() != {"revision", "image"}
                or not matches(rollback["revision"], COMMIT) or not matches(rollback["image"], IMAGE)
                or rollback["image"].split("@", 1)[0] != raw[name]["image"].split("@", 1)[0]
                or rollback["image"] == raw[name]["image"] or rollback["revision"] == raw[name]["revision"]):
            raise ProvisionError("activation needs a distinct, pinned rollback of each executor")
    return raw


def named_container(template, workload):
    containers = [item for item in template["spec"]["containers"] if item.get("name") == workload["container"]]
    if len(containers) != 1:
        raise ProvisionError("expected one exact executor container")
    return containers[0]


def verify_api_template(template, workload):
    container = named_container(template, workload)
    values = [env for env in container.get("env", []) if env.get("name") == FLAG]
    if values != [{"name": FLAG, "value": "true"}]:
        raise ProvisionError("live or rollback API does not literally enable the storage audit fence")


def verify_worker_template(template, workload, secret):
    spec = template["spec"]
    container = named_container(template, workload)
    values = [env.get("valueFrom", {}).get("secretKeyRef", {}) for env in container.get("env", [])
              if env.get("name") == "FUGUE_STORAGE_AUDIT_DATABASE_URL"]
    if (spec.get("automountServiceAccountToken") is not False or len(values) != 1
            or values[0].get("name") != secret or values[0].get("key") != "database-url"
            or values[0].get("optional", False) is not False or container.get("envFrom")):
        raise ProvisionError("live or rollback worker lacks the restricted database identity")


def verify_executors(provision, declaration, api_floor, worker_floor):
    if not matches(api_floor, COMMIT) or not matches(worker_floor, COMMIT):
        raise ProvisionError("tested API and worker source floors are required")
    def worker_template(template, workload):
        verify_worker_template(template, workload, provision.config["secret"])

    identities = {}
    for name, component, template, floor, paths in (
        ("api", "api", verify_api_template, api_floor, API_PATHS),
        ("worker", COMPONENT, worker_template, worker_floor, WORKER_PATHS),
    ):
        workload = declaration[name]
        target = verify_workload(provision, workload, verify_template=template)
        pinned = declaration[name + "Rollback"]
        rollback = checked_rollback(provision, component, workload, verify_template=template, allow_absent=pinned is None)
        if rollback != (None if pinned is None else (pinned["revision"], pinned["image"])):
            raise ProvisionError(f"current production {name} rollback differs from the activation declaration")
        check_source(floor, workload["revision"], paths)
        if rollback is not None:
            check_source(floor, rollback[0], paths)
        identities[name] = target
    return identities


def preflight(provision, declaration, policy, scrape, api_floor, worker_floor):
    if declaration["policyDigest"] != provision.policy_digest:
        raise ProvisionError("storage audit declaration and policy do not match")
    executors = verify_executors(provision, declaration, api_floor, worker_floor)
    primary = provision.find_primary()
    secret_uid, _ = provision.credential(provision.get("secret", provision.config["secret"]))
    receipts = witness(provision, policy, scrape, declaration["worker"], standby=True)
    if (receipts["workerPod"] != executors["worker"][2][0]
            or verify_executors(provision, declaration, api_floor, worker_floor) != executors
            or provision.find_primary() != primary
            or provision.credential(provision.get("secret", provision.config["secret"]))[0] != secret_uid):
        raise ProvisionError("execution or credential identity changed during storage audit admission")
    state = database_status(provision, primary[0], standby=True)
    return {"declarationSha256": declaration_digest(declaration), "apiPods": executors["api"][2],
            "workerPod": receipts["workerPod"], "prometheusPods": receipts["prometheusPods"],
            "primary": primary[0], "primaryUid": primary[1], "secretUid": secret_uid, **state}


def activated_status(provision, primary, declaration, secret_uid):
    raw = provision.sql(primary, f"""
BEGIN READ ONLY;
SET LOCAL statement_timeout = '3s';
{provision.identity_sql()}
SELECT json_build_object('owner', s.owner, 'policyDigest', s.policy_sha256,
    'completedPasses', s.completed_passes, 'failed', s.last_run_failed,
    'samplePolicyDigest', s.sample->>'policy_sha256', 'sampleAt', s.sample->>'observed_at',
    'activatedAt', a.activated_at, 'approvalSha256', a.approval_sha256,
    'activationPolicyDigest', a.policy_sha256, 'workerSecretUid', a.worker_secret_uid)
FROM fugue_storage_audit.lifecycle_status_v1 AS s
JOIN fugue_storage_audit.lifecycle_activation AS a ON a.singleton;
COMMIT;
""")
    lines = raw.splitlines()
    if len(lines) != 2 or lines[0] != "1":
        raise ProvisionError("storage audit activation has no durable witness")
    value = json.loads(lines[1])
    if (value["owner"] != "worker" or value["policyDigest"] != provision.policy_digest
            or value["activationPolicyDigest"] != provision.policy_digest
            or value["approvalSha256"] != declaration_digest(declaration) or value["workerSecretUid"] != secret_uid
            or type(value["completedPasses"]) is not int or value["completedPasses"] < 0
            or type(value["failed"]) is not bool):
        raise ProvisionError("storage audit activation no longer belongs to the declared worker; do not reset ownership")
    return value


def has_committed_sample(value):
    if (value["failed"] or not value["completedPasses"] or value["samplePolicyDigest"] != value["policyDigest"]
            or not value["sampleAt"]):
        return False
    sampled, activated = (datetime.fromisoformat(value[key]) for key in ("sampleAt", "activatedAt"))
    if sampled.tzinfo is None or activated.tzinfo is None or sampled < activated:
        raise ProvisionError("worker sample predates durable ownership or lacks its time zone")
    return True


def api_audit_metrics_retired(provision, pod):
    name, uid = pod
    namespace = provision.config["namespace"]
    if not matches(namespace, NAME) or not matches(name, NAME) or not matches(uid, UID):
        raise ProvisionError("cannot identify the live API metrics endpoint")
    path = f"/api/v1/namespaces/{namespace}/pods/{name}:9090/proxy/metrics"
    text = provision.kube("get", "--raw", path)
    if not re.search(r'(?m)^fugue_component_info\{[^\n}]*\bcomponent="api"[^\n}]*\}\s+1(?:\.0+)?\s*$', text):
        raise ProvisionError("expected API metric identity is missing")
    return not re.search(r"(?m)^(?:# (?:HELP|TYPE) )?fugue_storage_lifecycle_", text)


def verify_committed_activation(provision, declaration, scrape, api_floor, worker_floor, wait_seconds=240):
    if declaration["policyDigest"] != provision.policy_digest:
        raise ProvisionError("storage audit declaration and policy do not match")
    executors = verify_executors(provision, declaration, api_floor, worker_floor)
    primary = provision.find_primary()
    secret_uid, _ = provision.credential(provision.get("secret", provision.config["secret"]))
    collectors = prometheus_pods(provision, scrape)
    worker = executors["worker"][2][0]
    deadline = time.monotonic() + wait_seconds
    while True:
        state = activated_status(provision, primary[0], declaration, secret_uid)
        if (has_committed_sample(state)
                and all(api_audit_metrics_retired(provision, pod) for pod in executors["api"][2])
                and all(scrape_observed(provision, collector, worker, "worker", published=True) for collector in collectors)):
            break
        if time.monotonic() >= deadline:
            raise ProvisionError("storage audit sample or metrics have not converged; do not reset ownership")
        time.sleep(5)
    if (verify_executors(provision, declaration, api_floor, worker_floor) != executors
            or provision.find_primary() != primary or prometheus_pods(provision, scrape) != collectors
            or provision.credential(provision.get("secret", provision.config["secret"]))[0] != secret_uid):
        raise ProvisionError("execution identity changed after the handoff; do not reset ownership")
    return {"apiPods": executors["api"][2], "workerPod": worker, "prometheusPods": collectors, **state}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ("database", "declaration", "policy", "scrape"):
        parser.add_argument(field, type=Path)
    parser.add_argument("api_floor")
    parser.add_argument("worker_floor")
    parser.add_argument("--activated", action="store_true")
    args = parser.parse_args()
    try:
        policy = json.loads(args.policy.read_text())
        provision = StorageAuditProvision(json.loads(args.database.read_text()), policy)
        declaration = parse_declaration(json.loads(args.declaration.read_text()))
        scrape = json.loads(args.scrape.read_text())
        if args.activated:
            provision.verify_published_policy(policy)
            result = verify_committed_activation(provision, declaration, scrape, args.api_floor, args.worker_floor)
            provision.verify_published_policy(policy)
        else:
            result = preflight(provision, declaration, policy, scrape, args.api_floor, args.worker_floor)
        print(json.dumps(result, sort_keys=True))
    except ProvisionError as err:
        parser.exit(1, f"storage audit activation admission refused: {err}\n")
    except (AttributeError, KeyError, TypeError, ValueError, OSError):
        parser.exit(1, "storage audit activation admission received missing or malformed evidence\n")


if __name__ == "__main__":
    main()
