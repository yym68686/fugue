"""Transfer sample retention after proving the serving and rollback artifacts."""

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import time

from scripts.apply_observation_writer_handoff import verify_collector
from scripts.apply_quality_evidence_retention_handoff import check_workload
from scripts.provision_observation_sample_retention import FENCE, sample_role_binding_sql, transaction_body
from scripts.provision_observation_worker_database import Provision, ProvisionError
from scripts.publish_storage_policy import manifest as storage_policy_manifest
from scripts.verify_observation_writer_handoff import COMMIT, RELEASE_SCHEMA, load_release, matches


SCHEMA = "fugue.observation-sample-retention-handoff/v1"
CHECKPOINT = "observation/v1/" + hashlib.sha256(b"observation-sample-retention-writer/v1").hexdigest()
ACTIVATION = FENCE.with_name("observation_sample_retention_activate.sql")
API_PATHS = ("cmd/fugue-api/main.go",) + tuple("internal/store/" + path + ".go" for path in (
    "observation_retention", "edge_network", "edge_performance", "resource_usage",
))
WORKER_PATHS = (
    "cmd/fugue-observation-worker/main.go", "cmd/fugue-observation-worker/sample_retention.go",
    "internal/store/observation_sample_retention_worker.go", "deploy/releases/observation-worker/resources.json",
)
TARGETS = {"network", "performance", "resource-usage"}
COLLECTOR_EXPRESSION = (
    'up{job="fugue-observation-sample-retention"} == 1 and on(pod) '
    '(fugue_observation_sample_retention_storage_available{job="fugue-observation-sample-retention"} == 1) and on(pod) '
    '(fugue_observation_sample_retention_policy_valid{job="fugue-observation-sample-retention"} == 1) and on(pod) '
    '(time() - timestamp(fugue_observation_sample_retention_storage_available{job="fugue-observation-sample-retention"}) < 90)'
)


def parse_release(value):
    floors = {"apiForwardFloor", "apiRollbackFloor", "workerForwardFloor", "workerRollbackFloor"}
    if (not isinstance(value, dict) or value.keys() != {"schema", "api", "worker", *floors}
            or value["schema"] != SCHEMA or not all(matches(value[x], COMMIT) for x in floors)):
        raise ProvisionError("invalid sample retention handoff declaration")
    return {**load_release({"schema": RELEASE_SCHEMA, "api": value["api"], "worker": value["worker"]}),
            **{field: value[field] for field in floors}}


def activation_sql(provision, secret_uid, source):
    role = provision.config["role"]
    return f"""BEGIN;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '250ms';
{sample_role_binding_sql(provision, secret_uid)}
SELECT 1 / CASE WHEN
    has_function_privilege('{role}', 'fugue_observation_maintenance.prune_samples_v1(text,boolean,integer,integer,integer)', 'EXECUTE')
    AND has_table_privilege('{role}', 'fugue_observation_maintenance.sample_worker_progress', 'SELECT')
    THEN 1 ELSE 0 END;
{transaction_body(source)}
COMMIT;
"""


def read_progress(provision, primary):
    lines = provision.sql(primary, f"""
BEGIN READ ONLY;
SET LOCAL statement_timeout = '3s';
{provision.identity_sql()}
SELECT json_build_object('target', target, 'owner', owner, 'completedAt', completed_at,
    'completedPasses', completed_passes, 'totalDeleted', total_deleted, 'fence', fence_enforced,
    'transferAt', (SELECT updated_at FROM public.fugue_meta WHERE key = '{CHECKPOINT}'))
FROM fugue_observation_maintenance.sample_worker_progress ORDER BY target;
COMMIT;
""").splitlines()
    if len(lines) != len(TARGETS) + 1 or lines[0] != "1":
        raise ProvisionError("missing durable sample retention progress")
    rows = [json.loads(line) for line in lines[1:]]
    if {row["target"] for row in rows} != TARGETS:
        raise ProvisionError("unexpected durable sample retention targets")
    return {row["target"]: row for row in rows}


def verify_policy(provision, declaration):
    desired = storage_policy_manifest(declaration)
    key = "observation-retention.json"
    if desired["metadata"]["namespace"] != provision.config["namespace"] or key not in desired["data"]:
        raise ProvisionError("sample policy declaration is outside the bound namespace or missing")
    live = provision.get("configmap", desired["metadata"]["name"])
    identity = live["metadata"]
    if live["data"].get(key) != desired["data"][key] or identity.get("deletionTimestamp"):
        raise ProvisionError("running sample retention policy differs from the declaration")
    return identity["uid"], identity["resourceVersion"]


def all_active(rows):
    return all(row["owner"] == "worker" and row["fence"] is True and row["transferAt"] for row in rows.values())


def all_standby(rows):
    return all(row["owner"] == "api" and row["fence"] is False and row["transferAt"] is None
               and row["completedPasses"] == 0 and row["totalDeleted"] == 0
               and row["completedAt"] is None for row in rows.values())


def apply(provision, release, scrape, policy, source, wait_seconds=240):
    api_spec = (provision, "api", release["api"], release["apiForwardFloor"], release["apiRollbackFloor"], API_PATHS)
    worker_spec = (provision, "observation-worker", release["worker"], release["workerForwardFloor"], release["workerRollbackFloor"], WORKER_PATHS)
    api, worker = check_workload(*api_spec), check_workload(*worker_spec)
    pods = [pod for pod, _ in worker[0][2]]
    verify_collector(provision, scrape, pods, COLLECTOR_EXPRESSION)
    config_identity = verify_policy(provision, policy)
    primary = provision.find_primary()
    secret_uid, _ = provision.credential(provision.get("secret", provision.config["secret"]))
    before = read_progress(provision, primary[0])
    if not all_standby(before) and not all_active(before):
        raise ProvisionError("sample retention is outside the supported forward-only transition")
    if (check_workload(*api_spec) != api or check_workload(*worker_spec) != worker
            or verify_policy(provision, policy) != config_identity or provision.find_primary() != primary):
        raise ProvisionError("workloads, policy, database or rollback changed during admission")
    provision.sql(primary[0], activation_sql(provision, secret_uid, source))
    print("sample retention writer and all three fences committed; checking durable progress", flush=True)
    deadline = time.monotonic() + wait_seconds
    while True:
        if provision.find_primary() != primary:
            raise ProvisionError("database primary changed after handoff; do not reset ownership")
        after = read_progress(provision, primary[0])
        if not all_active(after):
            raise ProvisionError("sample retention handoff is not durable; do not reset ownership")
        if all(row["completedPasses"] > before[target]["completedPasses"] and row["completedAt"]
               and datetime.fromisoformat(row["completedAt"]) >= datetime.fromisoformat(row["transferAt"])
               for target, row in after.items()):
            verify_collector(provision, scrape, pods, COLLECTOR_EXPRESSION)
            return after
        if time.monotonic() >= deadline:
            raise ProvisionError("not all sample targets committed a new batch; do not reset ownership")
        time.sleep(10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("handoff", type=Path)
    parser.add_argument("scrape", type=Path)
    parser.add_argument("policy", type=Path)
    parser.add_argument("--apply", action="store_true", required=True)
    args = parser.parse_args()
    try:
        provision = Provision(json.loads(args.database.read_text()))
        release = parse_release(json.loads(args.handoff.read_text()))
        scrape = json.loads(args.scrape.read_text())
        policy = json.loads(args.policy.read_text())
        print(json.dumps(apply(provision, release, scrape, policy, ACTIVATION.read_text())))
    except ProvisionError as err:
        parser.exit(1, f"sample retention handoff stopped: {err}\n")
    except (KeyError, TypeError, ValueError, OSError, subprocess.SubprocessError):
        parser.exit(1, "sample retention handoff stopped on missing or malformed evidence\n")


if __name__ == "__main__":
    main()
