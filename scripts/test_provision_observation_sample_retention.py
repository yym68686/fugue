import os
import select
import subprocess
import unittest
from unittest.mock import Mock
from urllib.parse import urlsplit

from scripts.apply_observation_sample_retention_handoff import ACTIVATION_LOCK_MESSAGES, activation_sql
from scripts.provision_observation_sample_retention import CAPABILITY, FENCE, prepare_standby, standby_sql
from scripts.provision_observation_worker_database import Provision, ProvisionError, ProvisionLockBusy, classify_sql_failure


UID = "33759d3c-def3-4490-aa6c-deb4b77cc4f0"
CONFIG = {
    "schema": "fugue.observation-worker-database/v1",
    "namespace": "fixture",
    "cluster": "example",
    "clusterUID": UID,
    "systemID": "123456789012345678",
    "database": "fugue_test",
    "role": "fugue_observation_fixture",
    "secret": "fixture-credential",
}


class SampleRetentionStandbyTest(unittest.TestCase):
    def test_grants_only_bounded_function_and_progress_without_attaching_fence(self):
        sql = standby_sql(Provision(CONFIG), UID, FENCE.read_text(), CAPABILITY.read_text())
        self.assertLess(sql.index("shobj_description(r.oid, 'pg_authid')"), sql.index("CREATE OR REPLACE FUNCTION"))
        self.assertLess(sql.index("excess sample write rights"), sql.index("CREATE OR REPLACE FUNCTION"))
        self.assertIn("excess private data rights", sql)
        self.assertNotIn("CREATE TRIGGER", sql)
        self.assertNotIn("CREATE OR REPLACE TRIGGER", sql)
        self.assertNotIn("INSERT INTO public.fugue_meta", sql)
        self.assertEqual(sql.count("\nCOMMIT;\n"), 1)
        self.assertEqual([line for line in sql.splitlines() if line.startswith("GRANT ")], [
            "GRANT USAGE ON SCHEMA fugue_observation_maintenance TO fugue_observation_fixture;",
            "GRANT EXECUTE ON FUNCTION fugue_observation_maintenance.prune_samples_v1(text,boolean,integer,integer,integer) TO fugue_observation_fixture;",
            "GRANT SELECT ON fugue_observation_maintenance.sample_worker_progress TO fugue_observation_fixture;",
        ])

    def test_refuses_incomplete_or_moved_standby(self):
        with self.assertRaises(ProvisionError):
            standby_sql(Provision(CONFIG), UID, FENCE.read_text(), CAPABILITY.read_text().removesuffix("COMMIT;\n"))
        provision = Mock()
        provision.config = CONFIG
        provision.credential.return_value = UID, "synthetic-unused-password"
        provision.find_primary.side_effect = [("previous-primary", UID), ("replacement-primary", UID)]
        with self.assertRaises(ProvisionError):
            prepare_standby(provision, FENCE.read_text(), CAPABILITY.read_text())
        provision.sql.assert_not_called()

    def test_exact_bound_standby_and_handoff_on_disposable_postgres(self):
        dsn = os.environ.get("FUGUE_SAMPLE_RETENTION_REHEARSAL_DSN", "")
        if not dsn:
            self.skipTest("disposable sample retention PostgreSQL DSN not supplied")
        address = urlsplit(dsn)
        self.assertEqual(address.hostname, "127.0.0.1")
        self.assertEqual(address.path, "/sample_retention_test")

        def run(sql, *, role="", ok=True, verbose=False):
            if role:
                sql = f"SET ROLE {role};\n" + sql
            options = ["-v", "VERBOSITY=verbose"] if verbose else []
            result = subprocess.run(["psql", "-XAtqw", "-v", "ON_ERROR_STOP=1", *options, dsn],
                                    input=sql, capture_output=True, text=True, timeout=12)
            self.assertEqual(result.returncode == 0, ok, result.stderr)
            return result.stdout.strip() if ok else result.stderr

        role = "fugue_observation_sample_fixture"
        config = {**CONFIG, "database": "sample_retention_test", "role": role,
                  "systemID": run("SELECT system_identifier FROM pg_control_system();")}
        run(f"""
CREATE ROLE {role} LOGIN NOINHERIT;
COMMENT ON ROLE {role} IS 'fugue.observation-worker-database/v1:{UID}';
CREATE ROLE sample_retention_legacy NOLOGIN;
CREATE SCHEMA fugue_observation_maintenance;
REVOKE ALL ON SCHEMA fugue_observation_maintenance FROM PUBLIC;
CREATE TABLE fugue_meta (key text PRIMARY KEY, value text NOT NULL, updated_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE fugue_platform_quality_evidence (id text PRIMARY KEY);
CREATE TABLE fugue_tenants (id text PRIMARY KEY);
CREATE TABLE fugue_audit_events (id text PRIMARY KEY);
CREATE TABLE fugue_security_audit_chain_state (id text PRIMARY KEY);
CREATE TABLE fugue_edge_network_samples (id text PRIMARY KEY, observed_at timestamptz NOT NULL);
CREATE TABLE fugue_edge_performance_samples (id text PRIMARY KEY, sampled_at timestamptz NOT NULL);
CREATE TABLE fugue_resource_usage_samples (id text PRIMARY KEY, observed_at timestamptz NOT NULL,
    tenant_id text REFERENCES fugue_tenants(id) ON DELETE CASCADE);
CREATE INDEX ON fugue_edge_network_samples (observed_at);
GRANT SELECT (observed_at) ON fugue_edge_network_samples TO {role};
GRANT SELECT, INSERT, DELETE ON fugue_edge_network_samples TO sample_retention_legacy;
INSERT INTO fugue_edge_network_samples VALUES ('expired', now()-interval '8 days'), ('protected', now());
""")
        installer = standby_sql(Provision(config), UID, FENCE.read_text(), CAPABILITY.read_text())
        for _ in range(2):
            run(installer)
        progress = "SELECT owner,completed_passes,total_deleted FROM fugue_observation_maintenance.sample_worker_progress WHERE target='network'"
        prune = "SELECT state,deleted FROM fugue_observation_maintenance.prune_samples_v1('network',true,7,10000,1)"
        self.assertEqual(run(progress, role=role), "api|0|0")
        self.assertEqual(run(prune, role=role), "standby|0")
        run(f"GRANT DELETE ON fugue_edge_network_samples TO {role}")
        self.assertIn("excess sample write rights", run(installer, ok=False))
        run(f"REVOKE DELETE ON fugue_edge_network_samples FROM {role}")

        activate = activation_sql(Provision(config), UID, FENCE.with_name("observation_sample_retention_activate.sql").read_text())
        with subprocess.Popen(["psql", "-XAtqw", "-v", "ON_ERROR_STOP=1", dsn],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as legacy:
            try:
                legacy.stdin.write("SELECT pg_advisory_lock(315609238744311::bigint) \\g /dev/null\nSELECT 'held';\n")
                legacy.stdin.flush()
                self.assertTrue(select.select([legacy.stdout], [], [], 5)[0])
                self.assertEqual(legacy.stdout.readline().strip(), "held")
                self.assertIsInstance(classify_sql_failure(run(activate, ok=False, verbose=True),
                                                           ACTIVATION_LOCK_MESSAGES), ProvisionLockBusy)
                self.assertEqual(run(progress, role=role), "api|0|0")
            finally:
                legacy.communicate(timeout=5)
        run(activate)
        run("BEGIN; DELETE FROM fugue_edge_network_samples; INSERT INTO fugue_edge_network_samples VALUES ('fresh',now()); COMMIT;",
            role="sample_retention_legacy")
        self.assertEqual(run(prune, role=role), "completed|1")
        self.assertEqual(run(progress, role=role), "worker|1|1")
        self.assertEqual(run("SELECT id FROM fugue_edge_network_samples ORDER BY id;"), "fresh\nprotected")


if __name__ == "__main__":
    unittest.main()
