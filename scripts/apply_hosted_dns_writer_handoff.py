"""Transfer hosted DNS derived answers after checking both rollback targets."""

import argparse
from datetime import datetime
import json
from pathlib import Path
import time
from urllib.parse import urlencode

from scripts.apply_quality_evidence_retention_handoff import check_source
from scripts.provision_hosted_dns_worker import HostedDNSProvision, ROLE, SCHEMA as ROLE_SCHEMA
from scripts.provision_observation_worker_database import NAME, ProvisionError, ProvisionLockBusy, UID
from scripts.verify_hosted_dns_worker import COMPONENT, DEPLOYMENT, REPOSITORY, verify_template
from scripts.verify_observation_rollback_floor import checked_rollback
from scripts.verify_observation_writer_handoff import COMMIT, RELEASE_SCHEMA, load_release, matches, verify_workload
from scripts.verify_storage_audit_worker import prometheus_pods


SCHEMA = "fugue.hosted-dns-writer-handoff/v1"
KEY = "hosted-dns-flatten/v1/writer"
API_PATHS = (
    "cmd/fugue-api/main.go", "internal/api/dns_observations.go", "internal/api/hosted_dns.go",
    "internal/api/hosted_dns_refresh.go", "internal/api/hosted_dns_resolver.go",
    "internal/store/hosted_dns.go", "internal/store/hosted_dns_pg.go",
    "internal/store/hosted_dns_flatten_writer.go", "internal/hosteddns",
)
WORKER_PATHS = ("Dockerfile.hosted-dns-worker", "cmd/fugue-hosted-dns-worker", "internal/hosteddns",
                "internal/schemamigrate/hosted_dns_flatten_worker.sql", "deploy/releases/hosted-dns-worker/resources.json")


def scrape_observed(provision, collector, pod, owner, completed_after=None):
    namespace = provision.config["namespace"]
    name, uid = pod
    if (not all(matches(n, NAME) for n in (namespace, collector, name)) or not matches(uid, UID)
            or owner not in ("api", "worker") or (owner == "api" and completed_after is not None)):
        raise ProvisionError("invalid hosted DNS scrape identity")
    identity = {"job": DEPLOYMENT, "namespace": namespace, "pod": name, "pod_uid": uid}
    selector = ",".join(f'{k}="{v}"' for k, v in identity.items())
    labels = "{" + selector + "}"
    match = " on(job,namespace,pod,pod_uid) "
    expression = (" and" + match).join((
        f"(up{labels} == 1)", f"(time() - timestamp(up{labels}) < 75)",
        f"(fugue_hosted_dns_worker_storage_available{labels} == 1)",
        f"(time() - timestamp(fugue_hosted_dns_worker_storage_available{labels}) < 75)",
        f'(fugue_hosted_dns_owner{{{selector},owner="{owner}"}} == 1)',
    ))
    completed = "fugue_hosted_dns_last_completed_at_seconds" + labels
    if owner == "api":
        expression += " unless" + match + completed
    elif completed_after is not None:
        expression += " and" + match + f"({completed} >= {int(datetime.fromisoformat(completed_after).timestamp())})"
    path = f"/api/v1/namespaces/{namespace}/pods/{collector}:9090/proxy/api/v1/query?"
    response = json.loads(provision.kube("get", "--raw", path + urlencode({"query": expression})))
    data = response.get("data", {})
    if response.get("status") != "success" or data.get("resultType") != "vector":
        raise ProvisionError("hosted DNS Prometheus query did not return a vector")
    results = data["result"]
    return (len(results) == 1 and results[0]["value"][1] == "1" and
            all(results[0]["metric"].get(k) == v for k, v in identity.items()))


def parse_declaration(raw):
    floors = {"apiFloor", "workerFloor"}
    if (not isinstance(raw, dict) or raw.keys() != {"schema", "api", "worker", *floors}
            or raw["schema"] != SCHEMA or not all(matches(raw[key], COMMIT) for key in floors)):
        raise ProvisionError("invalid hosted DNS authority declaration")
    release = load_release({"schema": RELEASE_SCHEMA, "api": raw["api"], "worker": raw["worker"]})
    for name, identity in (("api", ("fugue-fugue-api", "api", 2, "ghcr.io/yym68686/fugue-api")),
                           ("worker", (DEPLOYMENT, COMPONENT, 1, REPOSITORY))):
        workload = release[name]
        if (*[workload[key] for key in ("deployment", "container", "replicas")],
                workload["image"].split("@", 1)[0]) != identity:
            raise ProvisionError("hosted DNS authority declaration names another executor")
    return raw


def check_executors(provision, release):
    result = {}
    for name, component, paths, template in (
        ("api", "api", API_PATHS, None),
        ("worker", COMPONENT, WORKER_PATHS, verify_template),
    ):
        workload = release[name]
        live = verify_workload(provision, workload, verify_template=template)
        rollback = checked_rollback(provision, component, workload, verify_template=template)
        floor = release[name + "Floor"]
        if not matches(floor, COMMIT) or not rollback:
            raise ProvisionError("hosted DNS needs two compatible immutable releases per executor")
        for source in (workload["revision"], rollback[0]):
            check_source(floor, source, paths)
        result[name] = live, rollback
    return result


def observe(provision, primary):
    lines = provision.sql(primary, f"""
BEGIN READ ONLY;
SET LOCAL statement_timeout = '3s';
{provision.identity_sql()}
SELECT json_build_object('owner', p.owner, 'completedAt', p.completed_at, 'updated', p.updated,
    'transferAt', (SELECT updated_at FROM public.fugue_meta WHERE key = '{KEY}' AND value = 'worker'))
FROM fugue_hosted_dns_maintenance.progress_v1(NULL,NULL) p;
COMMIT;
""").splitlines()
    if len(lines) != 2 or lines[0] != "1":
        raise ProvisionError("missing hosted DNS ownership evidence")
    state = json.loads(lines[1])
    if ((state["owner"] == "api" and all(state[k] is None for k in ("completedAt", "transferAt", "updated")))
            or (state["owner"] == "worker" and state["transferAt"])):
        return state
    raise ProvisionError("unexpected hosted DNS writer or progress")


def activation_sql(provision, secret_uid):
    if not matches(secret_uid, UID):
        raise ProvisionError("invalid hosted DNS Secret UID")
    return f"""
BEGIN ISOLATION LEVEL READ COMMITTED;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '250ms';
{provision.identity_sql()}
DO $activate$
BEGIN
    IF NOT pg_try_advisory_xact_lock(hashtextextended('{KEY}',0)) THEN
        RAISE EXCEPTION 'hosted DNS writer is busy' USING ERRCODE = '55P03';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles r WHERE r.rolname = '{ROLE}' AND r.rolcanlogin AND r.rolconnlimit = 6
          AND NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolinherit OR r.rolreplication OR r.rolbypassrls)
          AND shobj_description(r.oid, 'pg_authid') = '{ROLE_SCHEMA}:{secret_uid}'
          AND NOT EXISTS (SELECT 1 FROM pg_auth_members m WHERE m.member = r.oid OR m.roleid = r.oid)
    ) OR fugue_hosted_dns_maintenance.writer_v1() <> 'api'
      OR NOT has_schema_privilege('{ROLE}', 'fugue_hosted_dns_maintenance', 'USAGE')
      OR has_schema_privilege('{ROLE}', 'fugue_hosted_dns_maintenance', 'CREATE')
      OR EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND left(c.relname, 6) = 'fugue_' AND c.relkind IN ('r','p','v','m','f')
          AND (has_any_column_privilege('{ROLE}', c.oid, 'SELECT,INSERT,UPDATE,REFERENCES')
               OR has_table_privilege('{ROLE}', c.oid, 'DELETE,TRUNCATE,TRIGGER'))
      )
      OR NOT has_function_privilege('{ROLE}', 'fugue_hosted_dns_maintenance.progress_v1(integer,text)', 'EXECUTE')
      OR NOT has_function_privilege('{ROLE}', 'fugue_hosted_dns_maintenance.records_v1(text)', 'EXECUTE')
      OR NOT has_function_privilege('{ROLE}', 'fugue_hosted_dns_maintenance.commit_v1(text,text,text,text,timestamptz,timestamptz,jsonb,jsonb,text,text,timestamptz,text,text)', 'EXECUTE') THEN
        RAISE EXCEPTION 'hosted DNS authority prerequisites have changed';
    END IF;
    INSERT INTO public.fugue_meta (key,value,updated_at) VALUES ('{KEY}','worker',clock_timestamp())
    ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at
    WHERE fugue_meta.value = 'api';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'hosted DNS writer ownership changed';
    END IF;
END
$activate$;
COMMIT;
"""


def apply(provision, release, scrape, wait_seconds=180):
    executors = check_executors(provision, release)
    pod = executors["worker"][0][2][0]
    collectors = prometheus_pods(provision, scrape)
    primary = provision.find_primary()
    secret = lambda: provision.credential(provision.get("secret", provision.config["secret"]))[0]
    secret_uid = secret()
    for delay in (2, 5, 11, None):
        if (check_executors(provision, release) != executors or provision.find_primary() != primary
                or secret() != secret_uid or prometheus_pods(provision, scrape) != collectors):
            raise ProvisionError("hosted DNS executor, primary or credential changed during admission")
        before = observe(provision, primary[0])
        if not all(scrape_observed(provision, collector, pod, before["owner"]) for collector in collectors):
            raise ProvisionError("independent monitoring has not observed the exact hosted DNS worker and owner")
        if before["owner"] == "worker":
            break
        try:
            provision.sql(primary[0], activation_sql(provision, secret_uid), busy_messages=())
        except ProvisionLockBusy:
            if delay is None:
                raise ProvisionError("hosted DNS writer remained busy during bounded admission") from None
            time.sleep(delay)
        else:
            break
    deadline = time.monotonic() + wait_seconds
    while True:
        if provision.find_primary() != primary:
            raise ProvisionError("primary changed after hosted DNS transfer; preserve the worker owner")
        after = observe(provision, primary[0])
        if after["owner"] != "worker":
            raise ProvisionError("hosted DNS transfer did not remain durable")
        threshold = before["completedAt"] or after["transferAt"]
        if (after["completedAt"] and datetime.fromisoformat(after["completedAt"]) > datetime.fromisoformat(threshold)
                and all(scrape_observed(provision, collector, pod, "worker", after["completedAt"])
                        for collector in collectors)):
            if check_executors(provision, release) != executors or prometheus_pods(provision, scrape) != collectors:
                raise ProvisionError("executor changed while confirming hosted DNS progress")
            return {"primary": primary, "workerPod": pod, "prometheusPods": collectors, **after}
        if time.monotonic() >= deadline:
            raise ProvisionError("a new DNS pass was not independently confirmed; preserve the worker owner")
        time.sleep(5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("database", "handoff", "scrape"):
        parser.add_argument(name, type=Path)
    parser.add_argument("--apply", action="store_true", required=True)
    args = parser.parse_args()
    try:
        provision = HostedDNSProvision(json.loads(args.database.read_text()))
        release = parse_declaration(json.loads(args.handoff.read_text()))
        print(json.dumps(apply(provision, release, json.loads(args.scrape.read_text())), sort_keys=True))
    except ProvisionError as err:
        parser.exit(1, f"hosted DNS writer handoff stopped: {err}\n")
    except (AttributeError, KeyError, OSError, TypeError, ValueError):
        parser.exit(1, "hosted DNS writer handoff stopped on invalid evidence\n")


if __name__ == "__main__":
    main()
