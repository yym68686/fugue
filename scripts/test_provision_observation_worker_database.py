import base64
import copy
import hashlib
import json
import os
import select
import subprocess
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit, urlunsplit

from scripts.observation_rollup_writer_fence import handoff_sql
from scripts.provision_observation_worker_database import BINDING, SCHEMA, Provision, ProvisionError, scram_verifier


CONFIG = {
    "schema": SCHEMA, "namespace": "test-system", "cluster": "test-postgres",
    "clusterUID": "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0", "systemID": "1234567890123456789",
    "database": "observation_test", "role": "fugue_observation_synthetic", "secret": "worker-database",
}
SECRET_UID = "b0b0b0b0-b0b0-b0b0-b0b0-b0b0b0b0b0b0"


class FakeProvision(Provision):
    def __init__(self):
        super().__init__(CONFIG)
        self.secret = None
        self.role_exists = False
        self.events = []
        self.fail_role = False

    def find_primary(self):
        return "test-postgres-1", "test-pod-uid"

    def get(self, kind, name, optional=False):
        assert kind == "secret"
        return self.secret

    def sql(self, primary, body):
        if body.startswith("SELECT EXISTS"):
            return "t" if self.role_exists else "f"
        if "CREATE ROLE" in body:
            self.events.append("role")
            if self.fail_role:
                raise ProvisionError("test incomplete attempt")
            self.role_exists = True

    def kube(self, *args, body=None):
        self.events.append("dry-run" if "--dry-run=server" in args else "secret")
        if "--dry-run=server" not in args:
            self.secret = json.loads(body)
            self.secret["metadata"]["uid"] = SECRET_UID


class ProvisionTests(unittest.TestCase):
    @patch("builtins.print")
    def test_retries_with_same_immutable_secret(self, _):
        run = FakeProvision()
        run.fail_role = True
        with self.assertRaises(ProvisionError):
            run.run()
        before = copy.deepcopy(run.secret)
        run.fail_role = False
        run.run()
        self.assertEqual(before, run.secret)
        self.assertEqual(["dry-run", "secret", "role", "role"], run.events)

    def test_never_adopts_existing_role_or_unbound_secret(self):
        run = FakeProvision()
        run.role_exists = True
        with self.assertRaisesRegex(ProvisionError, "will not adopt"):
            run.run()
        run.secret = run.secret_body("a" * 64)
        run.secret["metadata"]["uid"] = SECRET_UID
        run.secret["metadata"]["annotations"][BINDING] = "unrelated"
        with self.assertRaisesRegex(ProvisionError, "not created by"):
            run.run()
        self.assertEqual([], run.events)

    def test_rejects_wrong_database_in_existing_secret(self):
        run = FakeProvision()
        run.secret = run.secret_body("a" * 64)
        run.secret["metadata"]["uid"] = SECRET_UID
        old = base64.b64decode(run.secret["data"]["database-url"])
        run.secret["data"]["database-url"] = base64.b64encode(old.replace(b"observation_test", b"unrelated")).decode()
        with self.assertRaisesRegex(ProvisionError, "does not match"):
            run.run()

    def test_provision_contract_on_disposable_postgres(self):
        dsn = os.environ.get("FUGUE_OBSERVATION_PROVISION_TEST_DSN", "")
        if not dsn:
            self.skipTest("requires a dedicated empty local PostgreSQL fixture")
        parsed = urlsplit(dsn)
        if parsed.hostname != "127.0.0.1" or parsed.path != "/observation_test":
            self.fail("refusing non-disposable provision test target")

        def sql(body, address=dsn):
            return subprocess.run(
                ["psql", "-qXAtw", "-v", "ON_ERROR_STOP=1", address],
                input=body, text=True, capture_output=True, check=False, timeout=15,
            )
        identity = sql("SELECT system_identifier::text FROM pg_control_system();")
        self.assertEqual(0, identity.returncode)
        config = dict(CONFIG, systemID=identity.stdout.strip())
        provision = Provision(config)
        first_password = "a" * 64
        verifier = scram_verifier(first_password)
        setup = sql("""
CREATE TABLE fugue_meta (key text PRIMARY KEY, value text, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE fugue_edge_performance_samples (id text);
CREATE TABLE fugue_edge_quality_rollup_watermarks (window_name text PRIMARY KEY, window_ended_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE fugue_edge_quality_rollups (hostname text);
""")
        self.assertEqual(0, setup.returncode)
        self.assertEqual(0, sql(provision.role_sql(SECRET_UID, verifier)).returncode)
        other_uid = "c0c0c0c0-c0c0-c0c0-c0c0-c0c0c0c0c0c0"
        self.assertNotEqual(0, sql(provision.role_sql(other_uid, verifier)).returncode)
        # A retry must not replace the first password or reset any grants.
        self.assertEqual(0, sql(provision.role_sql(SECRET_UID, scram_verifier("b" * 64))).returncode)
        address = urlunsplit(parsed._replace(netloc=f'{config["role"]}:{first_password}@127.0.0.1:{parsed.port}'))
        self.assertEqual(0, sql("SELECT owner FROM fugue_edge_quality_rollup_owner;", address).returncode)
        self.assertNotEqual(0, sql("SELECT value FROM fugue_meta;", address).returncode)
        self.assertNotEqual(0, sql("UPDATE fugue_edge_quality_rollup_owner SET owner = 'worker';", address).returncode)
        self.assertNotEqual(0, sql("ALTER ROLE postgres NOLOGIN;", address).returncode)
        key = hashlib.sha256(b"edge-quality-rollup-writer/v1").hexdigest()
        owner = sql(f"INSERT INTO fugue_meta (key, value) VALUES ('observation/v1/{key}', 'test-owner'), ('other', 'confidential');")
        self.assertEqual(0, owner.returncode)
        self.assertEqual("test-owner", sql("SELECT owner FROM fugue_edge_quality_rollup_owner;", address).stdout.strip())

        transfer = handoff_sql(provision, SECRET_UID)
        refusal = sql(transfer)
        self.assertNotEqual(0, refusal.returncode)
        self.assertIn("unexpected persisted owner", refusal.stderr)
        self.assertEqual("test-owner", sql("SELECT owner FROM fugue_edge_quality_rollup_owner;", address).stdout.strip())
        old_pass = "synthetic-local-legacy-login"
        setup_old = sql(f"""
UPDATE fugue_meta SET value = '' WHERE key = 'observation/v1/{key}';
CREATE ROLE old_api_fixture LOGIN PASSWORD '{old_pass}';
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON fugue_edge_quality_rollups, fugue_edge_quality_rollup_watermarks TO old_api_fixture;
INSERT INTO fugue_edge_quality_rollup_watermarks (window_name) VALUES ('5m');
""")
        self.assertEqual(0, setup_old.returncode)
        old = urlunsplit(parsed._replace(netloc=f'old_api_fixture:{old_pass}@127.0.0.1:{parsed.port}'))
        self.assertEqual(0, sql("INSERT INTO fugue_edge_quality_rollups VALUES ('initial');", old).returncode)
        # Model a pre-compatibility API transaction that never took the named
        # advisory lock. The handoff must still fail before installing a fence.
        holder = subprocess.Popen(["psql", "-qXAtw", "-v", "ON_ERROR_STOP=1", old],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            holder.stdin.write("BEGIN;\nINSERT INTO fugue_edge_quality_rollups VALUES ('in flight');\nSELECT 'held';\n")
            holder.stdin.flush()
            self.assertTrue(select.select([holder.stdout], [], [], 5)[0])
            self.assertEqual("held", holder.stdout.readline().strip())
            refused_busy = sql(transfer)
            self.assertNotEqual(0, refused_busy.returncode)
            self.assertIn("could not obtain lock", refused_busy.stderr)
            self.assertEqual("", sql("SELECT owner FROM fugue_edge_quality_rollup_owner;", address).stdout.strip())
        finally:
            try:
                holder.communicate("ROLLBACK;\n", timeout=5)
            except subprocess.TimeoutExpired:
                holder.kill()
                holder.communicate()
        for _ in range(2):
            transferred = sql(transfer)
            self.assertEqual(0, transferred.returncode, transferred.stderr)
            identity, snapshot = transferred.stdout.strip().splitlines()
            self.assertEqual("1", identity)
            self.assertGreater(json.loads(snapshot)["cutoverUnix"], 0)
        self.assertEqual("worker", sql("SELECT owner FROM fugue_edge_quality_rollup_owner;", address).stdout.strip())
        self.assertEqual(0, sql("INSERT INTO fugue_edge_quality_rollup_watermarks SELECT '5m' WHERE false;", old).returncode)
        for change in (
            "INSERT INTO fugue_edge_quality_rollups VALUES ('duplicate');",
            "INSERT INTO fugue_edge_quality_rollup_watermarks VALUES ('older');",
            "UPDATE fugue_edge_quality_rollups SET hostname = 'rewound';",
            "DELETE FROM fugue_edge_quality_rollups;",
            "TRUNCATE fugue_edge_quality_rollups;",
        ):
            denied = sql(change, old)
            self.assertNotEqual(0, denied.returncode)
            self.assertIn("observation rollup writes belong to the worker", denied.stderr)
        self.assertEqual(0, sql("INSERT INTO fugue_edge_quality_rollup_watermarks (window_name) VALUES ('30m');", address).returncode)
        self.assertEqual(0, sql("UPDATE fugue_edge_quality_rollups SET hostname = 'continued';", address).returncode)
        self.assertEqual("continued", sql("SELECT hostname FROM fugue_edge_quality_rollups;", address).stdout.strip())


if __name__ == "__main__":
    unittest.main()
