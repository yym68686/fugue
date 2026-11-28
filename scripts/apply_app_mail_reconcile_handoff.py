"""Transfer application mail to the settled Controller, always forward-only."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import sys
import time

from scripts.provision_observation_worker_database import Provision, ProvisionError
from scripts.verify_app_mail_provider_runtime import REPO, observe_provider_runtime
from scripts.verify_observation_rollback_floor import checked_rollback
from scripts.verify_observation_writer_handoff import (
    COMMIT, RELEASE_SCHEMA, load_release, matches, verify_workload,
)


SCHEMA = "fugue.app-mail-reconcile-handoff/v1"
ACTIVATION = Path("internal/schemamigrate/app_mail_reconcile_activate.sql")
OWNER = "app-mail-reconcile/owner/v1"
SEED = "app-mail-reconcile/seed/v1"
COMMON = ("internal/appmail", "internal/store/app_mail.go",
          "internal/store/app_mail_reconcile.go", "internal/store/postgres.go")
API_PATHS = COMMON + ("cmd/fugue-api/main.go", "internal/api/app_mail.go", "deploy/releases/api/deployment.json")
CONTROLLER_PATHS = COMMON + (
    "cmd/fugue-controller/main.go", "internal/config/config.go",
    "internal/controller/app_mail.go", "internal/controller/controller.go",
    "internal/controller/metrics.go", "deploy/releases/controller/resources.json",
)


def parse_release(value, *, standby=False):
    floors = {"apiForwardFloor", "apiRollbackFloor", "controllerForwardFloor", "controllerRollbackFloor"}
    properties = {"schema", "api", "controller", *floors}
    if not standby:
        properties.add("provider")
    if (not isinstance(value, dict) or value.keys() != properties
            or value["schema"] != SCHEMA or not all(matches(value[key], COMMIT) for key in floors)):
        raise ProvisionError("invalid application mail handoff declaration")
    workloads = load_release({"schema": RELEASE_SCHEMA, "api": value["api"], "worker": value["controller"]})
    if workloads["api"]["container"] != "api" or workloads["worker"]["container"] != "controller":
        raise ProvisionError("application mail deployment roles do not match")
    if not standby:
        provider = value["provider"]
        if (not isinstance(provider, dict) or provider.keys() != {"repository", "revision", "imageDigest"}
                or not matches(provider["repository"], REPO) or not matches(provider["revision"], COMMIT)
                or not matches(provider["imageDigest"], r"sha256:[a-f0-9]{64}")):
            raise ProvisionError("application mail handoff requires a pinned, reviewed provider artifact")
    return value


def verify_source(floor, revision, paths):
    for args in (("merge-base", "--is-ancestor", floor, revision),
                 ("diff", "--quiet", floor, revision, "--", *paths)):
        if subprocess.run(["git", *args], capture_output=True, timeout=10).returncode != 0:
            raise ProvisionError("a forward or rollback image lacks the tested application-mail boundary")


def runtime_snapshot(provision, release):
    result = {}
    for component, paths in (("api", API_PATHS), ("controller", CONTROLLER_PATHS)):
        workload = release[component]
        live = verify_workload(provision, workload)
        rollback = checked_rollback(provision, component, workload)
        verify_source(release[component + "ForwardFloor"], workload["revision"], paths)
        verify_source(release[component + "RollbackFloor"], rollback[0], paths)
        result[component] = (live, rollback)
    return result


def numeric_metric(metrics, name):
    observations = re.findall(r"(?m)^" + re.escape(name) + r" ([0-9]+(?:\.[0-9]+)?)$", metrics)
    if len(observations) != 1:
        raise ProvisionError("controller has not exposed its application mail scheduler metrics")
    return float(observations[0])


def check_leader(provision, workload, pods, owner, now=None):
    now = datetime.now(timezone.utc) if now is None else now
    lease = provision.get("lease", workload["deployment"])
    holder = lease["spec"]["holderIdentity"]
    renew = datetime.fromisoformat(lease["spec"]["renewTime"].replace("Z", "+00:00"))
    duration = lease["spec"]["leaseDurationSeconds"]
    if (lease["metadata"].get("deletionTimestamp") or holder not in dict(pods)
            or type(duration) is not int or not 1 <= duration <= 60
            or not -5 <= (now - renew).total_seconds() < duration):
        raise ProvisionError("controller has no fresh leader on the admitted deployment")
    namespace = provision.config["namespace"]
    path = f"/api/v1/namespaces/{namespace}/pods/{holder}:9090/proxy/metrics"
    metrics = provision.kube("get", "--raw", path)
    attempt_at = numeric_metric(metrics, "fugue_controller_app_mail_last_check_timestamp_seconds")
    if (numeric_metric(metrics, "fugue_controller_app_mail_seed_complete") != 1
            or numeric_metric(metrics, "fugue_controller_app_mail_provider_config_ready") != 1
            or numeric_metric(metrics, "fugue_controller_app_mail_executor_owner") != int(owner == "controller")
            or not -5 <= now.timestamp() - attempt_at < 65):
        raise ProvisionError("controller leader is not freshly ready for the persisted mail owner")
    if owner == "api":
        provider_read = numeric_metric(metrics, "fugue_controller_app_mail_provider_read_timestamp_seconds")
        if not -5 <= now.timestamp() - provider_read < 90:
            raise ProvisionError("controller standby has no fresh authenticated read of an exact mail binding")
    return lease["metadata"]["uid"], holder, dict(pods)[holder]


def read_progress(provision, primary):
    lines = provision.sql(primary, f"""
BEGIN READ ONLY;
SET LOCAL statement_timeout = '3s';
{provision.identity_sql()}
WITH marker AS (
    SELECT coalesce((SELECT value FROM public.fugue_meta WHERE key='{OWNER}'),'api') AS owner,
        (SELECT updated_at FROM public.fugue_meta WHERE key='{OWNER}' AND value='controller') AS activated,
        EXISTS(SELECT 1 FROM public.fugue_meta WHERE key='{SEED}' AND value::jsonb->>'complete'='true') AS seeded
), summary AS (
    SELECT count(*) FILTER(WHERE binding.body->>'state'='active') AS active,
        count(*) FILTER(WHERE work.app_id IS NULL) AS missing,
        count(*) FILTER(WHERE binding.body->>'state'='active' AND work.last_result='synced'
            AND work.last_success_at>=marker.activated) AS confirmed
    FROM public.fugue_app_mail_bindings binding
    LEFT JOIN public.fugue_app_mail_reconcile work
        ON work.app_id=binding.app_id AND work.generation=binding.generation CROSS JOIN marker
)
SELECT json_build_object('owner',marker.owner,'activated',marker.activated,
    'seeded',marker.seeded,'active',summary.active,'missing',summary.missing,'confirmed',summary.confirmed)
FROM marker CROSS JOIN summary;
COMMIT;
""").splitlines()
    if len(lines) != 2 or lines[0] != "1":
        raise ProvisionError("control-plane PostgreSQL did not return application mail progress")
    value = json.loads(lines[1])
    if (value["owner"] not in {"api", "controller"} or not value["seeded"]
            or (value["owner"] == "controller") != bool(value["activated"])):
        raise ProvisionError("application mail owner or seed is outside the forward-only transition")
    return value


def activation_sql(provision, source):
    if (not source.startswith("BEGIN;\n") or not source.endswith("\nCOMMIT;\n")
            or source.count("\nCOMMIT;\n") != 1):
        raise ProvisionError("application mail activation is not one checked transaction")
    return "BEGIN;\nSET LOCAL statement_timeout = '5s';\n" + provision.identity_sql() + "\n" + source.removeprefix("BEGIN;\n")


def verify_provider(provision, primary, expected):
    observed, witness = observe_provider_runtime(provision, primary)
    if any(observed[key] != value for key, value in expected.items()):
        raise ProvisionError("running mail provider does not match the reviewed artifact")
    return observed, witness


def handoff(provision, release, source, *, apply=False, wait_seconds=180):
    release = parse_release(release)
    runtime = runtime_snapshot(provision, release)
    primary = provision.find_primary()
    before = read_progress(provision, primary[0])
    if before["active"] < 1:
        raise ProvisionError("no existing active binding can prove the application mail handoff")
    leader = check_leader(provision, release["controller"], runtime["controller"][0][2], before["owner"])
    provider = verify_provider(provision, primary[0], release["provider"])
    if not apply:
        return {"database": before, "controllerLeader": leader[1], "applied": False}
    if (runtime_snapshot(provision, release) != runtime or provision.find_primary() != primary
            or check_leader(provision, release["controller"], runtime["controller"][0][2], before["owner"]) != leader
            or verify_provider(provision, primary[0], release["provider"]) != provider):
        raise ProvisionError("live, rollback, leader, primary or provider evidence changed before handoff")
    if before["owner"] == "api":
        provision.sql(primary[0], activation_sql(provision, source))
        print("application mail ownership committed; waiting for actual provider results", file=sys.stderr, flush=True)
    deadline = time.monotonic() + wait_seconds
    while True:
        if provision.find_primary() != primary:
            raise ProvisionError("database primary changed after handoff; do not reset the owner")
        current = read_progress(provision, primary[0])
        if current["owner"] != "controller":
            raise ProvisionError("controller ownership is not durable; do not reset the owner")
        if current["active"] > 0 and current["confirmed"] == current["active"]:
            current_runtime = runtime_snapshot(provision, release)
            confirmed_leader = check_leader(provision, release["controller"], current_runtime["controller"][0][2], "controller")
            provider, _ = verify_provider(provision, primary[0], release["provider"])
            return {"database": current, "controllerLeader": confirmed_leader[1],
                    "providerRuntime": provider, "applied": True}
        if time.monotonic() >= deadline:
            raise ProvisionError("provider results did not advance after handoff; do not reset the owner")
        time.sleep(5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("handoff", type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        # Reuse the declared CNPG identity only. This transition does not
        # read, create, grant or mount the observation worker credential.
        provision = Provision(json.loads(args.database.read_text()))
        declaration = parse_release(json.loads(args.handoff.read_text()))
        print(json.dumps(handoff(provision, declaration, ACTIVATION.read_text(), apply=args.apply)))
    except ProvisionError as err:
        parser.exit(1, f"application mail handoff refused: {err}\n")
    except (KeyError, TypeError, ValueError, OSError, subprocess.SubprocessError):
        parser.exit(1, "application mail handoff stopped on missing or malformed evidence\n")


if __name__ == "__main__":
    main()
