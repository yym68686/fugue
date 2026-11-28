"""Exercise the exact Hosted DNS transfer transaction on disposable PostgreSQL."""

from pathlib import Path
import os
import select
import subprocess
from tempfile import TemporaryDirectory

from scripts.apply_hosted_dns_writer_handoff import KEY, activation_sql
from scripts.provision_hosted_dns_worker import HostedDNSProvision, ROLE, SCHEMA
from scripts.provision_observation_worker_database import ProvisionError, ProvisionLockBusy, classify_sql_failure

UID = "00000000-0000-4000-8000-000000000001"
DATABASE = "fugue_test_hosted_dns_handoff"


def rehearse(socket, env):
    psql = ["psql", "-XAtqw", "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose", "-h", socket,
            "-p", "5432", "-U", "postgres"]

    def sql(body, *, database=DATABASE, expected=None):
        result = subprocess.run([*psql, "-d", database], input=body, capture_output=True, text=True, timeout=8, env=env)
        if expected is not None:
            if result.returncode == 0 or expected not in result.stderr:
                raise RuntimeError(f"expected PostgreSQL error {expected}: {result.stderr}")
        elif result.returncode:
            raise RuntimeError(result.stderr)
        return result.stdout.strip(), result.stderr

    system_id, _ = sql(f"CREATE DATABASE {DATABASE}; SELECT system_identifier FROM pg_control_system();", database="postgres")
    try:
        fixture = """
CREATE TABLE public.fugue_meta (key text PRIMARY KEY,value text NOT NULL,updated_at timestamptz DEFAULT clock_timestamp());
CREATE TABLE public.fugue_dns_zones (id text,tenant_id text,project_id text,status text);
CREATE TABLE public.fugue_dns_records (id text,zone_id text,tenant_id text,type text,values_json jsonb,status text,
  flatten_mode text,flatten_target text,flatten_ipv4_policy text,flatten_ipv6_policy text,flatten_fallback_policy text,
  flatten_status text,flattened_a_json jsonb,flattened_aaaa_json jsonb,last_resolved_at timestamptz,
  resolve_error text,last_message text,created_at timestamptz,updated_at timestamptz);
"""
        sql(fixture + Path("internal/schemamigrate/hosted_dns_flatten_worker.sql").read_text())
        sql(f"""CREATE ROLE {ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 6;
COMMENT ON ROLE {ROLE} IS '{SCHEMA}:{UID}';
GRANT USAGE ON SCHEMA fugue_hosted_dns_maintenance TO {ROLE};
GRANT EXECUTE ON FUNCTION fugue_hosted_dns_maintenance.progress_v1(integer,text),
    fugue_hosted_dns_maintenance.records_v1(text),
    fugue_hosted_dns_maintenance.commit_v1(text,text,text,text,timestamptz,timestamptz,jsonb,jsonb,text,text,timestamptz,text,text) TO {ROLE};
""")
        provision = HostedDNSProvision.__new__(HostedDNSProvision)
        provision.config = {"systemID": system_id, "database": DATABASE}
        activation = activation_sql(provision, UID)
        with subprocess.Popen([*psql, "-d", DATABASE], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              text=True, bufsize=1, env=env) as legacy:
            try:
                legacy.stdin.write(f"BEGIN; SELECT 'locked' FROM (SELECT pg_advisory_xact_lock_shared(hashtextextended('{KEY}',0))) hold;\n")
                legacy.stdin.flush()
                if not select.select([legacy.stdout], [], [], 5)[0] or legacy.stdout.readline().strip() != "locked":
                    raise RuntimeError("legacy writer did not hold the shared transaction lock")
                _, failure = sql(activation, expected="55P03")
                if not isinstance(classify_sql_failure(failure, ()), ProvisionLockBusy):
                    raise RuntimeError("actual PostgreSQL lock conflict is not recognized as retryable")
                if sql("SELECT fugue_hosted_dns_maintenance.writer_v1();")[0] != "api":
                    raise RuntimeError("failed transfer changed the owner")
            finally:
                legacy.stdin.close()  # Disconnect aborts the legacy transaction and releases its lock.
                try:
                    legacy.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    legacy.kill()
                    legacy.wait(timeout=5)

        sql(f"GRANT SELECT ON public.fugue_meta TO {ROLE};")
        _, failure = sql(activation, expected="authority prerequisites have changed")
        if isinstance(classify_sql_failure(failure, ()), ProvisionLockBusy):
            raise RuntimeError("an invalid authorization was considered retryable")
        sql(f"REVOKE SELECT ON public.fugue_meta FROM {ROLE};")
        sql(activation)
        row = sql("SELECT owner, completed_at IS NULL FROM fugue_hosted_dns_maintenance.progress_v1(NULL,NULL);")[0]
        if row != "worker|t":
            raise RuntimeError("successful transfer did not durably change only the owner")
        sql(activation, expected="authority prerequisites have changed")
        sql(f"SET ROLE {ROLE}; SELECT owner FROM fugue_hosted_dns_maintenance.progress_v1(0,'');")
        if sql("SELECT owner, completed_at IS NOT NULL FROM fugue_hosted_dns_maintenance.progress_v1(NULL,NULL);")[0] != "worker|t":
            raise RuntimeError("worker cannot advance after the transfer")
    finally:
        sql(f"DROP DATABASE {DATABASE};", database="postgres")
        sql(f"DROP ROLE IF EXISTS {ROLE};", database="postgres")


def disposable_rehearsal():
    # A private Unix socket and a freshly initialized cluster. Never select a
    # database from ambient psql settings or open a listening TCP socket.
    env = {key: value for key, value in os.environ.items() if not key.startswith("PG")}
    with TemporaryDirectory(prefix="fugue-dns-", dir="/tmp") as socket:
        database = str(Path(socket) / "db")

        def run(*command):
            subprocess.run(command, check=True, stdout=subprocess.DEVNULL, timeout=20, env=env)

        run("initdb", "-D", database, "-U", "postgres", "-A", "trust", "--no-locale")
        try:
            run("pg_ctl", "-D", database, "-l", str(Path(socket) / "postgres.log"), "-o",
                f"-c listen_addresses='' -k {socket} -p 5432", "-w", "start")
            rehearse(socket, env)
        finally:
            if (Path(database) / "postmaster.pid").is_file():
                run("pg_ctl", "-D", database, "-m", "fast", "-w", "stop")


if __name__ == "__main__":
    try:
        disposable_rehearsal()
        print("Hosted DNS transfer: lock contention, least privilege, and durable worker advancement verified")
    except (ProvisionError, RuntimeError, OSError, subprocess.SubprocessError) as err:
        raise SystemExit(f"Hosted DNS rehearsal failed: {err}") from None
