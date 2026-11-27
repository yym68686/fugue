import os
import subprocess
import unittest
from unittest.mock import Mock
from urllib.parse import urlsplit

from scripts.apply_quality_evidence_retention_handoff import activation_sql
from scripts.provision_observation_worker_database import Provision, ProvisionError
from scripts.provision_quality_evidence_retention import INSTALLER, prepare_standby, standby_sql


UID = "9422dc91-0aa8-4a6b-b047-4d8ab4cf662c"
CONFIG = {
    "schema": "fugue.observation-worker-database/v1",
    "namespace": "test",
    "cluster": "example",
    "clusterUID": UID,
    "systemID": "123456789012345678",
    "database": "fugue_test",
    "role": "fugue_observation_fixture",
    "secret": "fixture-credential",
}


class QualityEvidenceStandbyTest(unittest.TestCase):
    def test_binds_role_before_installing_and_grants_only_projection_and_capability(self):
        sql = standby_sql(Provision(CONFIG), UID, INSTALLER.read_text())
        self.assertIn("shobj_description(r.oid, 'pg_authid')", sql)
        self.assertLess(sql.index("$binding$"), sql.index("CREATE SCHEMA"))
        self.assertIn("has_any_column_privilege", sql)
        grants = [line for line in sql.splitlines() if line.startswith("GRANT ")]
        self.assertEqual(grants, [
            "GRANT USAGE ON SCHEMA fugue_observation_maintenance TO fugue_observation_fixture;",
            "GRANT EXECUTE ON FUNCTION fugue_observation_maintenance.prune_quality_evidence_v1() TO fugue_observation_fixture;",
            "GRANT SELECT ON fugue_observation_maintenance.quality_evidence_worker_progress TO fugue_observation_fixture;",
        ])
        self.assertNotIn("CREATE TRIGGER", sql)
        self.assertNotIn("INSERT INTO public.fugue_meta", sql)
        self.assertEqual(sql.count("\nCOMMIT;\n"), 1)

    def test_refuses_unbound_or_interrupted_install(self):
        with self.assertRaises(ProvisionError):
            standby_sql(Provision(CONFIG), "unsafe", INSTALLER.read_text())
        with self.assertRaises(ProvisionError):
            standby_sql(Provision(CONFIG), UID, INSTALLER.read_text().removesuffix("COMMIT;\n"))
        provision = Mock()
        provision.config = CONFIG
        provision.credential.return_value = UID, "synthetic-unused-password"
        provision.find_primary.side_effect = [("old-primary", UID), ("new-primary", UID)]
        with self.assertRaises(ProvisionError):
            prepare_standby(provision, INSTALLER.read_text())
        provision.sql.assert_not_called()

    def test_exact_installer_and_handoff_on_disposable_postgres(self):
        dsn = os.environ.get("FUGUE_QUALITY_EVIDENCE_REHEARSAL_DSN", "")
        if not dsn:
            self.skipTest("disposable quality evidence PostgreSQL DSN not supplied")
        address = urlsplit(dsn)
        self.assertEqual(address.hostname, "127.0.0.1")
        self.assertEqual(address.path, "/quality_evidence_test")

        def run(sql, *, role="", ok=True):
            if role:
                sql = f"SET ROLE {role};\n" + sql
            result = subprocess.run(["psql", "-XAtqw", "-v", "ON_ERROR_STOP=1", dsn],
                                    input=sql, capture_output=True, text=True, timeout=12)
            self.assertEqual(result.returncode == 0, ok, result.stderr)
            return result.stdout.strip() if ok else result.stderr

        role = CONFIG["role"]
        config = {**CONFIG, "database": "quality_evidence_test",
                  "systemID": run("SELECT system_identifier FROM pg_control_system();")}
        run(f"""
CREATE ROLE {role} LOGIN NOINHERIT;
COMMENT ON ROLE {role} IS 'fugue.observation-worker-database/v1:{UID}';
CREATE ROLE quality_evidence_old_api NOLOGIN;
CREATE TABLE fugue_meta (key text PRIMARY KEY, value text NOT NULL, updated_at timestamptz NOT NULL);
CREATE TABLE fugue_platform_quality_evidence (content_hash text PRIMARY KEY, expires_at timestamptz NOT NULL);
CREATE INDEX evidence_expiry ON fugue_platform_quality_evidence (expires_at);
GRANT SELECT, DELETE ON fugue_platform_quality_evidence TO quality_evidence_old_api;
INSERT INTO fugue_platform_quality_evidence VALUES ('expired', clock_timestamp()-interval '1 day'),
                                                    ('preserve', clock_timestamp()+interval '1 day');
""")
        installer = standby_sql(Provision(config), UID, INSTALLER.read_text())
        run(installer)
        run(installer)
        self.assertEqual(run("SELECT owner, completed_passes FROM fugue_observation_maintenance.quality_evidence_worker_progress;",
                             role=role), "api|0")
        prune = "SELECT state, deleted FROM fugue_observation_maintenance.prune_quality_evidence_v1();"
        self.assertEqual(run(prune, role=role), "standby|0")
        run("SELECT * FROM fugue_platform_quality_evidence;", role=role, ok=False)
        run("DELETE FROM fugue_meta;", role=role, ok=False)
        activate = INSTALLER.with_name("platform_quality_evidence_retention_activate.sql").read_text()
        for _ in range(2):
            run(activation_sql(Provision(config), UID, activate))
        delete = "DELETE FROM fugue_platform_quality_evidence WHERE content_hash='expired';"
        denied = run(delete, role="quality_evidence_old_api", ok=False)
        self.assertIn("legacy periodic quality evidence deletion fenced", denied)
        run("BEGIN; SET LOCAL fugue.quality_evidence_admission='v1';" + delete + "ROLLBACK;",
            role="quality_evidence_old_api")
        self.assertEqual(run(prune, role=role), "completed|1")
        self.assertEqual(run("SELECT owner,completed_passes,total_deleted FROM fugue_observation_maintenance.quality_evidence_worker_progress;",
                             role=role), "worker|1|1")
        self.assertEqual(run("SELECT content_hash FROM fugue_platform_quality_evidence;"), "preserve")


if __name__ == "__main__":
    unittest.main()
