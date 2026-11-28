"""Verify the storage audit capability on a disposable private PostgreSQL."""

import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import select
import socket as net
import subprocess as sp
from tempfile import TemporaryDirectory
import time
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from scripts.provision_observation_worker_database import scram_verifier
from scripts.provision_storage_lifecycle_audit import SCHEMA, StorageAuditProvision
from scripts.publish_storage_audit_policy import policy_update_sql
from scripts.storage_audit_activation_transaction import activation_sql
from scripts.verify_storage_audit_activation import activated_status, declaration_digest, has_committed_sample

INSTALL = Path("internal/schemamigrate/storage_lifecycle_audit.sql")
WORKER_ROLE = "fugue_storage_audit_fixture"
FIXTURE = """
CREATE ROLE fixture_storage_api LOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE;
CREATE TABLE public.fugue_source_uploads(id text PRIMARY KEY, tenant_id text NOT NULL,
    created_at timestamptz NOT NULL, size_bytes bigint NOT NULL, archive_bytes bytea NOT NULL);
CREATE TABLE public.fugue_apps(id text PRIMARY KEY, tenant_id text NOT NULL, source_json jsonb);
CREATE TABLE public.fugue_operations(id text PRIMARY KEY, tenant_id text NOT NULL, desired_source_json jsonb);
CREATE TABLE public.fugue_app_database_import_jobs(id text PRIMARY KEY, tenant_id text NOT NULL, source_upload_id text);
CREATE TABLE public.fugue_unrelated_fixture(id text PRIMARY KEY);
INSERT INTO public.fugue_source_uploads VALUES
    ('a','tenant-one',now()-interval '100 days',100,'synthetic-secret-body'),
    ('b','tenant-one',now(),200,'synthetic-body'),('c','tenant-one',now(),300,'synthetic-body'),
    ('d','tenant-one',now(),400,'synthetic-body');
INSERT INTO public.fugue_apps VALUES
    ('app','tenant-one','{"origin_source":{"upload_id":"b"}}'),
    ('other','tenant-two','{"upload_id":"a"}');
INSERT INTO public.fugue_operations VALUES ('op','tenant-one','{"desired_source":{"upload_id":"c"}}');
INSERT INTO public.fugue_app_database_import_jobs VALUES ('job','tenant-one','d');
ALTER TABLE public.fugue_source_uploads OWNER TO fixture_storage_api;
ALTER TABLE public.fugue_apps OWNER TO fixture_storage_api;
ALTER TABLE public.fugue_operations OWNER TO fixture_storage_api;
ALTER TABLE public.fugue_app_database_import_jobs OWNER TO fixture_storage_api;
"""


def verify(condition, message):
    if not condition:
        raise RuntimeError(message)


def witness_worker(binary, socket, verify_passes, *, database_user=WORKER_ROLE, refused=False,
                   policy_unavailable=False, while_ready=None):
    with net.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    address = f"127.0.0.1:{port}"
    env = {**os.environ, "FUGUE_STORAGE_AUDIT_BIND_ADDR": address, "FUGUE_STORAGE_AUDIT_DATABASE_URL":
           f"host={socket} port=5432 user={database_user} dbname=postgres sslmode=disable"}
    if refused:
        stopped = sp.run([binary], env=env, capture_output=True, text=True, timeout=10)
        verify(stopped.returncode != 0 and "not an isolated worker" in stopped.stderr,
               "the worker did not reject the broad database identity")
        return
    with sp.Popen([binary], env=env, stdout=sp.DEVNULL, stderr=sp.PIPE, text=True) as process:
        try:
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                verify(process.poll() is None, f"worker exited before readiness: {process.stderr.read() if process.poll() is not None else ''}")
                try:
                    probe = "healthz" if policy_unavailable else "readyz"
                    with urlopen(f"http://{address}/{probe}", timeout=0.25) as ready:
                        if ready.status == 200:
                            break
                except (URLError, TimeoutError):
                    time.sleep(0.05)
            else:
                raise RuntimeError("worker did not report readiness after completing its bounded database checks")
            with urlopen(f"http://{address}/metrics", timeout=1) as response:
                metrics = response.read().decode()
            if policy_unavailable:
                try:
                    with urlopen(f"http://{address}/readyz", timeout=1):
                        raise RuntimeError("worker reported ready for an invalid policy")
                except HTTPError as exc:
                    verify(exc.code == 503, "invalid policy readiness did not fail closed")
                verify("fugue_storage_audit_progress_available 0\n" in metrics,
                       "worker reported invalid policy as available")
            else:
                verify(f"fugue_storage_audit_committed_passes {verify_passes}\n" in metrics
                   and "fugue_storage_audit_executor_owner 1\n" in metrics
                   and 'fugue_storage_lifecycle_free_percent_threshold{level="warning"} 20.000000\n' in metrics
                   and 'fugue_storage_lifecycle_free_percent_threshold{level="pressure"} 15.000000\n' in metrics
                   and 'fugue_storage_lifecycle_free_percent_threshold{level="critical"} 10.000000\n' in metrics
                   and "fugue_storage_lifecycle_audit_enabled 1.000000\n" in metrics
                   and "fugue_storage_lifecycle_interval_seconds 300.000000\n" in metrics
                   and "fugue_storage_lifecycle_last_success_timestamp_seconds " in metrics
                   and "fugue_storage_source_page_rows" in metrics
                   and "tenant-one" not in metrics, "worker metrics changed audit thresholds or leaked progress details")
                if while_ready is not None:
                    while_ready(address)
        finally:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=12)
            except sp.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
                raise
        verify(process.returncode == 0, "worker did not drain after a normal termination")


def wait_worker_status(address, path, expected, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urlopen(f"http://{address}/{path}", timeout=1) as response:
                observed = response.status
        except HTTPError as error:
            observed = error.code
        if observed == expected:
            return
        time.sleep(0.15)
    raise RuntimeError(f"worker {path} did not reach HTTP {expected}")


def rehearse(socket, worker_binary, *, client=("psql",), client_socket=None):
    psql = [*client, "-XAtqw", "-v", "ON_ERROR_STOP=1", "-h", client_socket or socket,
            "-p", "5432", "-d", "postgres", "-U"]
    command = psql + ["postgres"]

    def sql(body, *, refused=None, worker=False, reader=False):
        role = WORKER_ROLE if worker else "fixture_storage_api" if reader else "postgres"
        result = sp.run(psql + [role],
                        input=body, text=True, capture_output=True, timeout=10)
        if refused is None:
            verify(result.returncode == 0, result.stderr)
        else:
            verify(result.returncode != 0 and refused in result.stderr, f"{body}\nexpected {refused}: {result.stderr}")
        return result.stdout.strip()

    function = "SELECT fugue_storage_audit.sample_lifecycle_v1();"

    def run_pass(transaction="COMMIT"):
        return sql("BEGIN ISOLATION LEVEL REPEATABLE READ; " + function + transaction + ";", worker=True)

    def status():
        state = sql("SELECT row_to_json(s) FROM fugue_storage_audit.lifecycle_status_v1 s;", worker=True)
        verify("tenant-one" not in state and "synthetic" not in state and "source_cursor" not in state,
               "audit status exposed restricted metadata")
        return json.loads(state)

    def due():
        sql("UPDATE fugue_storage_audit.lifecycle_progress SET last_attempt_at=clock_timestamp()-interval '301 seconds';")

    effective = (datetime.now(timezone.utc) - timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")
    policy = {
        "version": 1, "mode": "audit", "effective_from": effective, "interval_seconds": 300,
        "source_page_rows": 2, "history_days": 7, "minimum_verified_versions": 3,
        "unused_upload_hours": 24, "unreferenced_upload_days": 30,
        "warning_free_percent": 20, "pressure_free_percent": 15, "critical_free_percent": 10,
    }

    def policy_assignment(value):
        raw = json.dumps(value, separators=(",", ":"))
        digest = sha256(raw.encode()).hexdigest()
        return f"mode='{value['mode']}', policy_sha256='{digest}', policy_document='{raw}'::jsonb", digest

    initial, original_digest = policy_assignment(policy)

    sql(FIXTURE)
    sql(INSTALL.read_text())
    sql(function, reader=True, refused="permission denied")
    verify(sql("SELECT owner FROM fugue_storage_audit.lifecycle_status_v1;") == "api",
           "installation activated an executor")
    sql(f"""INSERT INTO fugue_storage_audit.lifecycle_control
        VALUES(true,'worker','audit','{original_digest}','{effective}',300,2,'{json.dumps(policy)}');""",
        refused="storage audit must start with legacy owner")
    config = {"schema": SCHEMA, "namespace": "test-system", "cluster": "test-postgres",
              "clusterUID": "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0",
              "systemID": sql("SELECT system_identifier::text FROM pg_control_system();"),
              "database": "postgres", "role": WORKER_ROLE, "apiRole": "fixture_storage_api", "secret": "audit-database"}
    policy_document = {"namespace": "test-system", "name": "test-policy", "storageLifecycle": policy,
                       "policy": {"version": 1, "compiler_input_chunks": False, "checkpoint_pages": False,
                                  "database_max_open": 0, "database_max_idle": 0, "database_idle_seconds": 0}}

    class FixtureProvision(StorageAuditProvision):
        def sql(self, primary, body):
            verify(primary == "test-primary", "rehearsal addressed a different database")
            return sql(body)

    provision = FixtureProvision(config, policy_document)
    secret_uid = "b0b0b0b0-b0b0-b0b0-b0b0-b0b0b0b0b0b0"
    verifier = scram_verifier("a" * 64)
    standby_install = StorageAuditProvision(config, policy_document, require_standby=True)
    wrong_api = StorageAuditProvision(dict(config, apiRole="postgres"), policy_document)
    sql(wrong_api.role_sql(secret_uid, verifier), refused="does not own the expected application schema")
    sql(standby_install.role_sql(secret_uid, verifier))
    sql(provision.role_sql("c0c0c0c0-c0c0-c0c0-c0c0-c0c0c0c0c0c0", verifier),
        refused="not isolated and owned by this Secret")
    sql(provision.role_sql(secret_uid, verifier))
    verify(status()["policy_sha256"] == original_digest and status()["sample"] is None,
           "standby failed to bind the same policy without sampling")
    sql(function, reader=True, refused="permission denied")
    sql("SELECT source_cursor FROM fugue_storage_audit.lifecycle_progress;", reader=True, refused="permission denied")
    for statement in (
        "SELECT archive_bytes FROM public.fugue_source_uploads",
        "SELECT source_cursor FROM fugue_storage_audit.lifecycle_progress",
        "UPDATE public.fugue_source_uploads SET size_bytes=0",
        "UPDATE fugue_storage_audit.lifecycle_control SET owner='worker'",
        "DELETE FROM fugue_storage_audit.lifecycle_progress",
        "SELECT fugue_storage_audit.zero_temp_v1()",
        "SET temp_file_limit=10",
        "SET ROLE fugue_storage_audit_definer",
    ):
        sql(statement + ";", worker=True, refused="permission denied")
    verify(sql("""
SELECT NOT has_column_privilege('fugue_storage_audit_definer', 'public.fugue_source_uploads', 'archive_bytes', 'SELECT')
   AND NOT has_any_column_privilege('fugue_storage_audit_definer', 'public.fugue_source_uploads', 'UPDATE')
   AND NOT has_table_privilege('fugue_storage_audit_definer', 'public.fugue_source_uploads', 'INSERT,DELETE')
   AND NOT rolcanlogin AND NOT rolsuper AND NOT rolbypassrls
FROM pg_roles WHERE rolname='fugue_storage_audit_definer';
""") == "t", "definer gained upload body access or business write authority")
    verify(run_pass() == "standby" and status()["completed_passes"] == 0, "standby executed a pass")
    disabled, _ = policy_assignment({**policy, "mode": "disabled"})
    declaration = {"disposableFixture": True, "policyDigest": provision.policy_digest}
    approval = declaration_digest(declaration)
    transfer = activation_sql(provision, secret_uid, approval)
    with sp.Popen(psql + ["fixture_storage_api"], stdin=sp.PIPE, stdout=sp.PIPE, text=True) as legacy:
        try:
            legacy.stdin.write("SELECT pg_try_advisory_lock_shared(hashtextextended('storage-lifecycle-audit/owner/v1',0));"
                               " SELECT owner FROM fugue_storage_audit.lifecycle_status_v1;\n")
            legacy.stdin.flush()
            verify(legacy.stdout.readline().strip() == "t" and legacy.stdout.readline().strip() == "api",
                   "the API could not read the owner under its session lock")
            sql(f"UPDATE fugue_storage_audit.lifecycle_control SET owner='worker',{disabled};",
                refused="storage audit owner fence is busy")
            sql(transfer, refused="lock timeout")
        finally:
            legacy.stdin.close()
            verify(legacy.wait(timeout=5) == 0, "the legacy lock holder failed")
    sql(activation_sql(provision, "c0c0c0c0-c0c0-c0c0-c0c0-c0c0c0c0c0c0", approval),
        refused="roles no longer match")
    verify(status()["owner"] == "api" and status()["completed_passes"] == 0,
           "failed activation did not roll back the owner")
    activation = json.loads(sql(transfer).splitlines()[-1])
    verify(activation["policyDigest"] == original_digest and activation["approvalSha256"] == approval,
           "activation did not persist its approved policy and source")
    verify(json.loads(sql(transfer).splitlines()[-1]) == activation, "activation retry changed its durable witness")
    verify(not has_committed_sample(activated_status(provision, "test-primary", declaration, secret_uid)),
           "ownership alone was accepted as a completed worker pass")
    sql(activation_sql(provision, secret_uid, "2" * 64), refused="different handoff witness")
    sql("SELECT * FROM fugue_storage_audit.lifecycle_activation;", reader=True, refused="permission denied")
    sql("UPDATE fugue_storage_audit.lifecycle_activation SET approval_sha256=repeat('3',64);",
        worker=True, refused="permission denied")
    verify(sql("SELECT owner FROM fugue_storage_audit.lifecycle_status_v1;", reader=True) == "worker",
           "an old compatible API could not see the committed handoff")
    sql(f"UPDATE fugue_storage_audit.lifecycle_control SET {disabled};")
    verify(run_pass() == "disabled", "disabled policy ran a pass")
    for statement in (
        "UPDATE fugue_storage_audit.lifecycle_control SET owner='api';",
        "DELETE FROM fugue_storage_audit.lifecycle_control;",
        "TRUNCATE fugue_storage_audit.lifecycle_control;",
    ):
        sql(statement, refused="active storage audit owner cannot be reset")
    sql(f"UPDATE fugue_storage_audit.lifecycle_control SET {initial};")
    sql(function, worker=True, refused="requires REPEATABLE READ")

    with sp.Popen(command, stdin=sp.PIPE, stdout=sp.PIPE, text=True) as lock:
        try:
            lock.stdin.write("BEGIN; SELECT pg_advisory_xact_lock(hashtextextended('storage-lifecycle-audit/owner/v1',0)); SELECT 'held';\n")
            lock.stdin.flush()
            verify(lock.stdout.readline().strip() == "" and lock.stdout.readline().strip() == "held",
                   "the old worker did not acquire the shared writer fence")
            verify(run_pass() == "busy" and status()["completed_passes"] == 0, "two workers advanced the same pass")
        finally:
            lock.stdin.close()
            verify(lock.wait(timeout=5) == 0, "the lock holder failed")
    verify(run_pass("ROLLBACK") == "sampled" and status()["sample"] is None,
           "a pass escaped rollback before its cursor committed")
    verify(run_pass() == "sampled", "first executor did not sample")
    first = status()
    verify(datetime.fromisoformat(first["sample"]["observed_at"]) >= datetime.fromisoformat(activation["activatedAt"]),
           "first durable worker sample predated the exact handoff")
    verify(has_committed_sample(activated_status(provision, "test-primary", declaration, secret_uid)),
           "cutover observation could not verify the committed worker pass")
    page = first["sample"]["source_page"]
    verify(first["completed_passes"] == 1 and page["rows"] == 2 and page["declared_bytes"] == 300
           and page["unknown_rows"] == 1 and page["observed_referenced_rows"] == 1 and not page["page_at_end"]
           and page["older_than_30_days_declared_bytes"] == 100 and page["predating_contract_rows"] == 1,
           "wrong bounded first page, reference evidence, or sentinel")
    verify(run_pass() == "not_due" and status() == first, "an early retry advanced durable state")
    verify(first["sample"]["database_bytes"] > 0 and len(first["sample"]["relations"]) == 11,
           "the fixed relation allowlist did not produce metrics")

    sql("UPDATE public.fugue_source_uploads SET size_bytes=-1 WHERE id='c';")
    due()
    verify(run_pass() == "read_failed", "invalid source metadata was published")
    failed = status()
    verify(failed["last_run_failed"] and failed["failure_count"] == 1 and failed["completed_passes"] == 1
           and failed["sample"] == first["sample"], "failure changed the last good page")
    sql("UPDATE public.fugue_source_uploads SET size_bytes=300 WHERE id='c';")
    due()
    verify(run_pass() == "sampled", "replacement executor did not resume")
    second = status()
    page = second["sample"]["source_page"]
    verify(second["completed_passes"] == 2 and not second["last_run_failed"] and page["page_at_end"]
           and page["declared_bytes"] == 700 and page["observed_referenced_rows"] == 2
           and not page["references_complete"] and not page["retirement_authorized"] and page["eligible_rows"] == 0,
           "resume skipped the sentinel or conferred retirement authority")
    sql(INSTALL.read_text())
    verify(status() == second, "reinstall reset the active owner or last sample")
    sql(provision.role_sql(secret_uid, verifier))
    verify(status() == second, "standby retry reset the active owner or last sample")
    sql(standby_install.role_sql(secret_uid, verifier), refused="cannot touch an active owner")
    verify(status() == second, "initial release replay was able to change active audit state")
    paused_document = {**policy_document, "storageLifecycle": {**policy, "mode": "disabled"}}
    pause = StorageAuditProvision(config, paused_document)
    dry_run = sql(policy_update_sql(pause, policy_document, apply=False))
    verify(dry_run.splitlines()[-1] == "worker:updated" and status() == second,
           "policy dry run modified the active executor")
    pause_commit = sql(policy_update_sql(pause, policy_document, apply=True))
    verify(pause_commit.splitlines()[-1] == "worker:updated" and run_pass() == "disabled"
           and status()["owner"] == "worker" and status()["sample"] == second["sample"],
           "pausing the active policy discarded progress or reopened the legacy executor")
    verify(sql(policy_update_sql(pause, policy_document, apply=True)).splitlines()[-1] == "worker:unchanged",
           "repeating policy publication changed an already committed policy")
    next_document = {**policy_document, "storageLifecycle": {**policy, "history_days": 8}}
    replacement = StorageAuditProvision(config, next_document)
    sql(policy_update_sql(replacement, policy_document, apply=True), refused="neither the live predecessor")
    changed, rolled_digest = policy_assignment(next_document["storageLifecycle"])
    resumed = sql(policy_update_sql(replacement, paused_document, apply=True))
    verify(resumed.splitlines()[-1] == "worker:updated", "resuming the audit missed the committed policy")
    sql(provision.role_sql(secret_uid, verifier), refused="already exists with a different revision")
    verify(status()["policy_sha256"] == rolled_digest and status()["owner"] == "worker",
           "attempt to provision old policy changed the active worker or policy")
    verify(run_pass() == "sampled", "policy change did not start a fresh page")
    verify(status()["sample"]["policy_sha256"] == rolled_digest
           and status()["sample"]["source_page"]["declared_bytes"] == 300,
           "policy change reused a prior cursor or stale sample")
    verify(sql("SELECT count(*) || ':' || sum(size_bytes) FROM public.fugue_source_uploads;") == "4:1000",
           "audit mutated or removed a business row")
    if worker_binary is not None:
        witness_worker(worker_binary, socket, 0, database_user="postgres", refused=True)
        sql(f"GRANT SELECT (archive_bytes) ON public.fugue_source_uploads TO {WORKER_ROLE};")
        witness_worker(worker_binary, socket, 0, refused=True)
        sql(f"REVOKE SELECT (archive_bytes) ON public.fugue_source_uploads FROM {WORKER_ROLE};")
        sql(f"GRANT SELECT ON public.fugue_unrelated_fixture TO {WORKER_ROLE};")
        witness_worker(worker_binary, socket, 0, refused=True)
        sql(f"REVOKE SELECT ON public.fugue_unrelated_fixture FROM {WORKER_ROLE};")
        due()
        sql("UPDATE fugue_storage_audit.lifecycle_control SET policy_sha256=repeat('a',64);")
        witness_worker(worker_binary, socket, 3, policy_unavailable=True)
        verify(status()["completed_passes"] == 3, "invalid policy advanced durable progress")
        sql(f"UPDATE fugue_storage_audit.lifecycle_control SET {changed};")
        witness_worker(worker_binary, socket, 4)
        witness_worker(worker_binary, socket, 4)
        verify(status()["completed_passes"] == 4 and status()["sample"]["source_page"]["declared_bytes"] == 700,
               "replacement worker duplicated or skipped a committed page")

        def unavailable_dependency(address):
            unchanged = status()
            uploads = "SELECT json_agg(u ORDER BY id)::text FROM public.fugue_source_uploads u;"
            business = sql(uploads, reader=True)
            with sp.Popen(command, stdin=sp.PIPE, stdout=sp.PIPE, text=True) as locker:
                try:
                    locker.stdin.write(
                        "BEGIN; LOCK TABLE fugue_storage_audit.lifecycle_control IN ACCESS EXCLUSIVE MODE; "
                        "SELECT 'audit-control-locked';\n"
                    )
                    locker.stdin.flush()
                    readable, _, _ = select.select([locker.stdout], [], [], 6)
                    verify(readable and locker.stdout.readline().strip() == "audit-control-locked",
                           "could not lock the disposable worker dependency")
                    wait_worker_status(address, "readyz", 503, 25)
                    wait_worker_status(address, "healthz", 200, 2)
                    with urlopen(f"http://{address}/metrics", timeout=1) as response:
                        metrics = response.read().decode()
                    verify("fugue_storage_audit_progress_available 0\n" in metrics and
                           "fugue_storage_audit_committed_passes 4\n" in metrics,
                           "timed-out dependency hid or advanced the last committed sample")
                    verify(sql(uploads, reader=True) == business,
                           "worker dependency timeout blocked or changed the API's business rows")
                finally:
                    locker.stdin.close()  # EOF rolls back the fixture lock.
                    try:
                        locker.wait(timeout=8)
                    except sp.TimeoutExpired:
                        locker.kill()
                        locker.wait(timeout=3)
            verify(status() == unchanged, "timed-out worker advanced the durable audit or changed its owner")
            due()
            wait_worker_status(address, "readyz", 200, 25)
            recovered = status()
            verify(recovered["owner"] == unchanged["owner"] and
                   recovered["policy_sha256"] == unchanged["policy_sha256"] and
                   recovered["completed_passes"] == unchanged["completed_passes"] + 1 and
                   sql(uploads, reader=True) == business,
                   "worker did not recover exactly one owned page without changing business rows")

        witness_worker(worker_binary, socket, 4, while_ready=unavailable_dependency)
        print("verified worker binary: restricted credentials, durable restart and isolated dependency timeout/recovery")
    print("verified storage lifecycle audit: separate roles, bounded aggregate, durable retry/resume, no business writes")


@contextmanager
def pinned_postgres(image, scratch):
    """Keep the host's Python and test binary outside the pinned PG image."""
    if not re.fullmatch(r"[a-z0-9][a-z0-9./:_-]+@sha256:[a-f0-9]{64}", image):
        raise ValueError("PostgreSQL rehearsal requires an immutable image digest")
    # Directory access is private on the host. The container's postgres UID
    # and the host test binary only share the Unix socket, not data or code.
    socket = scratch / "socket"
    socket.mkdir(mode=0o777)
    socket.chmod(0o777)
    mount = "/audit-socket"
    name = "fugue-" + scratch.name
    label = "fugue.pro/storage-audit-rehearsal"
    try:
        started = sp.run([
            "docker", "run", "--rm", "--detach", "--name", name, "--label", f"{label}={name}",
            "--network", "none", "--user", "26:26", "--tmpfs", "/tmp:rw,mode=1777",
            "--mount", f"type=bind,source={socket},target={mount}", "--entrypoint", "bash", image,
            "-euc", 'initdb -D /tmp/audit-pg -U postgres -A trust --no-locale >/dev/null; '
            'exec postgres -D /tmp/audit-pg -h "" -k "$1" -c statement_timeout=4000 '
            '-c unix_socket_permissions=0777', "bash", mount,
        ], check=True, capture_output=True, text=True, timeout=100)
        container = started.stdout.strip()
        verify(re.fullmatch(r"[a-f0-9]{64}", container), "could not bind the disposable PostgreSQL container")
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            status = sp.run(["docker", "exec", container, "pg_isready", "-h", mount, "-U", "postgres"],
                            capture_output=True, timeout=5)
            if status.returncode == 0:
                break
            time.sleep(0.15)
        else:
            raise RuntimeError("the pinned disposable PostgreSQL did not become ready")
        yield str(socket), ("docker", "exec", "-i", container, "psql"), mount
    finally:
        # Use the unique invocation label even if the client lost its answer
        # creating the container. Never remove anything else by name alone.
        target = sp.run(["docker", "ps", "-aq", "--no-trunc", "--filter", f"name=^/{name}$",
                         "--filter", f"label={label}={name}"], capture_output=True, text=True, timeout=10)
        container = target.stdout.strip()
        if target.returncode == 0 and re.fullmatch(r"[a-f0-9]{64}", container):
            sp.run(["docker", "rm", "--force", container], check=True, capture_output=True, timeout=15)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", type=Path, help="also exercise this built storage audit worker")
    parser.add_argument("--postgres-image", help="rehearse with a pinned PostgreSQL image in a networkless container")
    args = parser.parse_args()
    if args.postgres_image:
        # Avoid Docker mount parser characters and long Unix socket paths from
        # runner-specific TEMP directories. mkdtemp still owns all cleanup.
        with TemporaryDirectory(prefix="storage-audit.", dir="/tmp") as scratch:
            with pinned_postgres(args.postgres_image, Path(scratch)) as (socket, client, client_socket):
                rehearse(socket, args.worker.resolve() if args.worker else None, client=client, client_socket=client_socket)
        return
    with TemporaryDirectory(prefix="storage-audit.") as socket:
        data = socket + "/db"
        sp.run(["initdb", "-D", data, "-U", "postgres", "-A", "trust", "--no-locale"], check=True,
               stdout=sp.DEVNULL, timeout=20)
        sp.run(["pg_ctl", "-D", data, "-l", socket + "/pg.log", "-o",
                f"-h '' -k {socket} -p 5432 -c statement_timeout=4000", "-w", "start"],
               check=True, stdout=sp.DEVNULL, timeout=15)
        try:
            rehearse(socket, args.worker.resolve() if args.worker else None)
        finally:
            sp.run(["pg_ctl", "-D", data, "-m", "fast", "-w", "stop"],
                   check=True, stdout=sp.DEVNULL, timeout=15)


if __name__ == "__main__":
    main()
