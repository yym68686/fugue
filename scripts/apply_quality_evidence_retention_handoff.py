"""Transfer one verified quality-evidence timer; never re-authorize the API."""

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import time

from scripts.apply_observation_writer_handoff import verify_collector
from scripts.provision_observation_worker_database import Provision, ProvisionError
from scripts.provision_quality_evidence_retention import INSTALLER, worker_binding_sql
from scripts.verify_observation_rollback_floor import checked_rollback
from scripts.verify_observation_writer_handoff import COMMIT, RELEASE_SCHEMA, load_release, matches, verify_workload


SCHEMA = "fugue.quality-evidence-retention-handoff/v1"
CHECKPOINT = "observation/v1/" + hashlib.sha256(b"platform-quality-evidence-retention-writer/v1").hexdigest()
ACTIVATION = INSTALLER.with_name("platform_quality_evidence_retention_activate.sql")
FENCE = "fugue_quality_evidence_delete_fence_v1"
COLLECTOR_EXPRESSION = (
    'up{job="fugue-quality-evidence-retention"} == 1 and on(pod) '
    '(fugue_quality_evidence_retention_storage_available{job="fugue-quality-evidence-retention"} == 1) and on(pod) '
    '(time() - timestamp(fugue_quality_evidence_retention_storage_available{job="fugue-quality-evidence-retention"}) < 90)'
)
API_PATHS = ("internal/store/platform_quality_evidence.go",)
WORKER_PATHS = (
    "cmd/fugue-observation-worker/main.go", "cmd/fugue-observation-worker/evidence_retention.go",
    "internal/store/platform_quality_evidence_retention_worker.go", "deploy/releases/observation-worker/resources.json",
)


def parse_release(value):
    if (not isinstance(value, dict) or value.keys() != {"schema", "api", "worker", "apiFloor", "workerFloor"}
            or value["schema"] != SCHEMA
            or not all(matches(value[x], COMMIT) for x in ("apiFloor", "workerFloor"))):
        raise ProvisionError("invalid quality evidence handoff declaration")
    return {**load_release({"schema": RELEASE_SCHEMA, "api": value["api"], "worker": value["worker"]}),
            "apiFloor": value["apiFloor"], "workerFloor": value["workerFloor"]}


def check_source(floor, source, paths):
    for args in (("merge-base", "--is-ancestor", floor, source),
                 ("diff", "--quiet", floor, source, "--", *paths)):
        if subprocess.run(["git", *args], capture_output=True, timeout=10).returncode != 0:
            raise ProvisionError("forward or rollback artifact does not retain the tested timer boundary")


def check_workload(provision, component, workload, floor, paths):
    target = verify_workload(provision, workload)
    rollback = checked_rollback(provision, component, workload)
    for source in (workload["revision"], rollback[0]):
        check_source(floor, source, paths)
    return target, rollback


def activation_sql(provision, secret_uid, body):
    if not body.startswith("BEGIN;\n") or not body.endswith("\nCOMMIT;\n") or body.count("\nCOMMIT;\n") != 1:
        raise ProvisionError("invalid quality evidence handoff transaction")
    statements = body.removeprefix("BEGIN;\n")
    role = provision.config["role"]
    return f"""
BEGIN;
SET LOCAL statement_timeout = '8s';
{worker_binding_sql(provision, secret_uid)}
SELECT 1 / CASE WHEN
    has_function_privilege('{role}', 'fugue_observation_maintenance.prune_quality_evidence_v1()', 'EXECUTE')
    AND has_table_privilege('{role}', 'fugue_observation_maintenance.quality_evidence_worker_progress', 'SELECT')
    THEN 1 ELSE 0 END;
{statements}
"""


def read_progress(provision, primary):
    lines = provision.sql(primary, f"""
BEGIN READ ONLY;
SET LOCAL statement_timeout = '3s';
{provision.identity_sql()}
SELECT json_build_object('owner', progress.owner, 'completedAt', progress.completed_at,
    'completedPasses', progress.completed_passes,
    'transferAt', (SELECT updated_at FROM public.fugue_meta WHERE key = '{CHECKPOINT}'),
    'fence', EXISTS (SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'public.fugue_platform_quality_evidence'::regclass
          AND tgname = '{FENCE}' AND tgenabled = 'A' AND tgtype = 10
          AND tgfoid = 'fugue_observation_maintenance.fence_legacy_quality_evidence_delete_v1()'::regprocedure))
FROM fugue_observation_maintenance.quality_evidence_worker_progress progress;
COMMIT;
""").splitlines()
    if len(lines) != 2:
        raise ProvisionError("missing durable maintenance progress")
    return json.loads(lines[-1])


def apply(provision, release, scrape, body, wait_seconds=240):
    api = check_workload(provision, "api", release["api"], release["apiFloor"], API_PATHS)
    worker = check_workload(provision, "observation-worker", release["worker"], release["workerFloor"], WORKER_PATHS)
    pods = [pod for pod, _ in worker[0][2]]
    verify_collector(provision, scrape, pods, COLLECTOR_EXPRESSION)
    primary = provision.find_primary()
    secret_uid, _ = provision.credential(provision.get("secret", provision.config["secret"]))
    before = read_progress(provision, primary[0])
    if not (before["owner"] == "api" and not before["fence"] and before["completedPasses"] == 0) and not (
            before["owner"] == "worker" and before["fence"] and before["transferAt"]):
        raise ProvisionError("quality evidence writer is outside the supported forward-only transition")
    if (check_workload(provision, "api", release["api"], release["apiFloor"], API_PATHS) != api
            or check_workload(provision, "observation-worker", release["worker"], release["workerFloor"], WORKER_PATHS) != worker
            or provision.find_primary() != primary):
        raise ProvisionError("workloads, primary or expected rollback changed during admission")
    provision.sql(primary[0], activation_sql(provision, secret_uid, body))
    print("quality evidence timer owner and fence committed; checking durable progress", flush=True)
    deadline = time.monotonic() + wait_seconds
    while True:
        if provision.find_primary() != primary:
            raise ProvisionError("database primary changed after handoff; do not reset ownership")
        after = read_progress(provision, primary[0])
        if after["owner"] != "worker" or not after["fence"] or not after["transferAt"]:
            raise ProvisionError("quality evidence handoff not durable; do not reset ownership")
        if (after["completedPasses"] > 0 and after["completedAt"]
                and datetime.fromisoformat(after["completedAt"]) >= datetime.fromisoformat(after["transferAt"])):
            return after
        if time.monotonic() >= deadline:
            raise ProvisionError("worker has not committed a retention pass; do not reset ownership")
        time.sleep(10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("handoff", type=Path)
    parser.add_argument("scrape", type=Path)
    parser.add_argument("--apply", action="store_true", required=True)
    args = parser.parse_args()
    try:
        provision = Provision(json.loads(args.database.read_text()))
        release = parse_release(json.loads(args.handoff.read_text()))
        scrape = json.loads(args.scrape.read_text())
        print(json.dumps(apply(provision, release, scrape, ACTIVATION.read_text())))
    except ProvisionError as err:
        parser.exit(1, f"quality evidence handoff stopped: {err}\n")
    except (KeyError, TypeError, ValueError, OSError, subprocess.SubprocessError):
        parser.exit(1, "quality evidence handoff stopped on missing or malformed evidence\n")


if __name__ == "__main__":
    main()
