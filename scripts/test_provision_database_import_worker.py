import os
import re
import subprocess
import unittest
from urllib.parse import urlsplit, urlunsplit

from scripts.provision_database_import_worker import DEFINER, ROLE, SCHEMA, SECRET, ImportProvision
from scripts.provision_observation_worker_database import ProvisionError, scram_verifier


CONFIG = {
    "schema": SCHEMA, "namespace": "fixture", "cluster": "test-postgres",
    "clusterUID": "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0", "systemID": "1234567890123456789",
    "database": "fugue_test_import_standby", "role": ROLE, "secret": SECRET,
}
SECRET_UID = "b0b0b0b0-b0b0-b0b0-b0b0-b0b0b0b0b0b0"
OTHER_UID = "c0c0c0c0-c0c0-c0c0-c0c0-c0c0c0c0c0c0"


class ProvisionImportWorkerTest(unittest.TestCase):
    def test_rejects_wrong_declaration_and_invalid_binding(self):
        with self.assertRaises(ProvisionError):
            ImportProvision({**CONFIG, "role": "postgres"})
        with self.assertRaises(ProvisionError):
            ImportProvision(CONFIG).role_sql("invalid-uid", "not-a-verifier")

    def test_real_postgres_installs_and_confirms_without_taking_ownership(self):
        dsn = os.environ.get("FUGUE_IMPORT_STANDBY_TEST_DSN", "")
        if not dsn:
            self.skipTest("requires a disposable PostgreSQL instance")
        address = urlsplit(dsn)
        self.assertEqual(("postgresql", "127.0.0.1", "/fugue_test_import_standby", "sslmode=disable", ""),
                         (address.scheme, address.hostname, address.path, address.query, address.fragment))

        def sql(body, target=dsn, success=True):
            result = subprocess.run(["psql", "-XAtqw", "-v", "ON_ERROR_STOP=1", target],
                                    input=body, capture_output=True, text=True, timeout=12)
            if success:
                self.assertEqual(0, result.returncode, result.stderr)
            else:
                self.assertNotEqual(0, result.returncode)
            return result.stdout.strip()

        self.assertEqual("t", sql(f"""SELECT NOT EXISTS (SELECT FROM pg_class WHERE relnamespace='public'::regnamespace)
AND NOT EXISTS (SELECT FROM pg_roles WHERE rolname IN ('{ROLE}','{DEFINER}'))"""))
        identity = sql("SELECT system_identifier::text FROM pg_control_system()")
        self.assertIsNotNone(re.fullmatch(r"[0-9]+", identity))
        provision = ImportProvision({**CONFIG, "systemID": identity})
        password = "a" * 64
        install = provision.role_sql(SECRET_UID, scram_verifier(password))
        sql("""
CREATE TABLE public.fugue_meta (key text PRIMARY KEY, value text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE public.fugue_tenants (id text, status text);
CREATE TABLE public.fugue_source_uploads (id text, tenant_id text, filename text, archive_bytes bytea);
CREATE TABLE public.fugue_apps (id text, tenant_id text, name text, spec_json jsonb, status_json jsonb);
CREATE TABLE public.fugue_service_bindings (id text, tenant_id text, app_id text, service_id text, created_at timestamptz);
CREATE TABLE public.fugue_backing_services (id text, tenant_id text, owner_app_id text, type text, provisioner text, status text, spec_json jsonb);
CREATE TABLE public.fugue_operations (app_id text, type text, status text);
CREATE TABLE public.fugue_app_database_import_jobs (id text, tenant_id text, app_id text, source_upload_id text,
    status text, created_at timestamptz, started_at timestamptz, completed_at timestamptz, updated_at timestamptz,
    format text, clean boolean, logs_json jsonb, result_message text, error_message text);
INSERT INTO public.fugue_app_database_import_jobs (id,status) VALUES ('fixture','pending');
""")
        sql(f"CREATE ROLE {DEFINER} NOLOGIN NOINHERIT")
        sql(install, success=False)
        self.assertEqual("t", sql(f"SELECT to_regnamespace('{ROLE}') IS NULL"))
        sql(f"DROP ROLE {DEFINER}")
        sql(install)
        sql(provision.role_sql(SECRET_UID, scram_verifier("b" * 64)))
        sql(provision.role_sql(OTHER_UID, scram_verifier("c" * 64)), success=False)
        worker_dsn = urlunsplit(address._replace(netloc=f"{ROLE}:{password}@127.0.0.1:{address.port}"))
        self.assertEqual("api|0", sql(f"SELECT {ROLE}.owner_v1(), (SELECT count(*) FROM {ROLE}.pending_v1(5))", worker_dsn))
        sql("SELECT * FROM public.fugue_app_database_import_jobs", worker_dsn, success=False)
        self.assertEqual("t", sql("SELECT NOT EXISTS (SELECT FROM public.fugue_meta) AND to_regnamespace('fugue_app_db_import_fence') IS NULL"))
        sql(f"GRANT SELECT (id) ON public.fugue_app_database_import_jobs TO {ROLE}")
        sql(install, success=False)
        sql(f"REVOKE SELECT (id) ON public.fugue_app_database_import_jobs FROM {ROLE}")
        sql("INSERT INTO public.fugue_meta (key,value) VALUES ('app-database-import/v1/writer','worker')")
        sql(install, success=False)
        self.assertEqual("worker", sql("SELECT value FROM public.fugue_meta WHERE key='app-database-import/v1/writer'"))


if __name__ == "__main__":
    unittest.main()
