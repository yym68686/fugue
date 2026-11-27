"""Provision the standby worker login on the declared CNPG cluster.

The Secret is immutable and created before the role. A retry can finish
provisioning that same Secret UID; it cannot adopt or rotate another login.
This command never changes the rollup writer checkpoint.
"""

import argparse
import base64
import hashlib
import hmac
import json
import re
import secrets
import subprocess
from pathlib import Path
from urllib.parse import urlsplit


SCHEMA = "fugue.observation-worker-database/v1"
BINDING = "fugue.pro/observation-database-binding"
CHECKPOINT = "observation/v1/" + hashlib.sha256(b"edge-quality-rollup-writer/v1").hexdigest()
NAME = r"[a-z][a-z0-9-]{0,62}"
IDENTIFIER = r"[a-z][a-z0-9_]{0,62}"
UID = r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}"
PASSWORD = r"[a-f0-9]{64}"


class ProvisionError(Exception):
    pass


def parse_config(raw):
    fields = {"schema", "namespace", "cluster", "clusterUID", "systemID", "database", "role", "secret"}
    if not isinstance(raw, dict) or raw.keys() != fields or raw["schema"] != SCHEMA:
        raise ProvisionError("invalid database declaration")
    for key, expression in {
        "namespace": NAME, "cluster": NAME, "secret": NAME, "clusterUID": UID,
        "systemID": r"[1-9][0-9]{9,19}", "database": IDENTIFIER, "role": IDENTIFIER,
    }.items():
        if not isinstance(raw[key], str) or not re.fullmatch(expression, raw[key]):
            raise ProvisionError("invalid database declaration identity")
    if not raw["role"].startswith("fugue_observation_"):
        raise ProvisionError("role outside the observation-worker namespace")
    return raw


def scram_verifier(password):
    salt = secrets.token_bytes(16)
    rounds = 4096
    salted = hashlib.pbkdf2_hmac("sha256", password.encode("ascii"), salt, rounds)
    client = hmac.digest(salted, b"Client Key", "sha256")
    stored = hashlib.sha256(client).digest()
    server = hmac.digest(salted, b"Server Key", "sha256")
    def encoded(binary):
        return base64.b64encode(binary).decode("ascii")
    return f"SCRAM-SHA-256${rounds}:{encoded(salt)}${encoded(stored)}:{encoded(server)}"


class Provision:
    validate_config = staticmethod(parse_config)
    binding_annotation = BINDING
    success_message = "observation database credential provisioned; writer ownership unchanged"

    def __init__(self, config, kubectl="kubectl"):
        self.config = self.validate_config(config)
        self.kubectl = kubectl
        self.service = config["cluster"] + "-rw"
        self.server = f'{self.service}.{config["namespace"]}.svc'
        self.binding = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()

    def kube(self, *args, body=None):
        try:
            result = subprocess.run(
                [self.kubectl, "--request-timeout=20s", "-n", self.config["namespace"], *args],
                input=body, capture_output=True, text=True, timeout=35,
            )
        except (OSError, subprocess.TimeoutExpired):
            raise ProvisionError("Kubernetes operation unavailable") from None
        # Raw subprocess stderr or stdout can contain the Secret or SQL.
        if result.returncode != 0:
            raise ProvisionError(f"Kubernetes operation failed: {args[0]}")
        return result.stdout.strip()

    def get(self, kind, name, optional=False):
        args = ["get", kind, name, "-o", "json"]
        if optional:
            args += ["--ignore-not-found"]
        out = self.kube(*args)
        return json.loads(out) if out else None

    def find_primary(self):
        c = self.config
        cluster = self.get("cluster.postgresql.cnpg.io", c["cluster"])
        meta = cluster["metadata"]
        status = cluster["status"]
        primary = status.get("currentPrimary")
        if (meta.get("uid") != c["clusterUID"] or meta.get("deletionTimestamp")
                or str(status.get("systemID")) != c["systemID"] or not primary
                or primary != status.get("targetPrimary") or status.get("writeService") != self.service):
            raise ProvisionError("declared CNPG cluster or ready primary did not match")
        pod = self.get("pod", primary)
        meta = pod["metadata"]
        if (meta.get("deletionTimestamp") or pod["status"].get("phase") != "Running"
                or not any(x.get("type") == "Ready" and x.get("status") == "True"
                           for x in pod["status"].get("conditions", []))
                or not any(x.get("kind") == "Cluster" and x.get("uid") == c["clusterUID"]
                           and x.get("controller") is True for x in meta.get("ownerReferences", []))):
            raise ProvisionError("CNPG primary Pod is not ready or independently bound")
        return primary, meta["uid"]

    def sql(self, primary, body):
        return self.kube(
            "exec", "-i", primary, "-c", "postgres", "--", "psql", "-XAtqw",
            "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", self.config["database"],
            body=body,
        )

    def identity_sql(self):
        return (
            "SELECT 1 / CASE WHEN NOT pg_is_in_recovery() "
            f"AND system_identifier::text = '{self.config['systemID']}' "
            f"AND current_database() = '{self.config['database']}' THEN 1 ELSE 0 END "
            "FROM pg_control_system();"
        )

    def secret_body(self, password):
        dsn = f"postgresql://{self.config['role']}:{password}@{self.server}:5432/{self.config['database']}?sslmode=require"
        return {
            "apiVersion": "v1", "kind": "Secret", "type": "Opaque", "immutable": True,
            "metadata": {"namespace": self.config["namespace"], "name": self.config["secret"],
                         "annotations": {self.binding_annotation: self.binding}},
            "data": {"database-url": base64.b64encode(dsn.encode("ascii")).decode("ascii")},
        }

    def credential(self, secret):
        meta = secret["metadata"]
        uid = meta.get("uid", "")
        if (secret.get("immutable") is not True or secret.get("type") != "Opaque"
                or meta.get("deletionTimestamp") or not re.fullmatch(UID, uid)
                or meta.get("annotations", {}).get(self.binding_annotation) != self.binding
                or set(secret.get("data", {})) != {"database-url"}):
            raise ProvisionError("existing Secret was not created by this provisioner")
        encoded = secret["data"]["database-url"]
        try:
            password = urlsplit(base64.b64decode(encoded, validate=True).decode("ascii")).password or ""
        except (ValueError, UnicodeError, TypeError):
            raise ProvisionError("existing credential is invalid") from None
        if not re.fullmatch(PASSWORD, password) or self.secret_body(password)["data"] != secret["data"]:
            raise ProvisionError("existing credential does not match the declared database")
        return uid, password

    def role_sql(self, uid, verifier):
        role = self.config["role"]
        marker = SCHEMA + ":" + uid
        # Identifiers and UID have been validated. The encoded verifier is
        # generated locally; it contains no SQL delimiters.
        return f"""
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '8s';
SET LOCAL log_statement = 'none';
SET LOCAL log_min_error_statement = 'panic';
{self.identity_sql()}
SELECT pg_advisory_xact_lock(hashtextextended('{SCHEMA}:{role}', 0));
DO $provision$
DECLARE saved pg_roles%ROWTYPE;
BEGIN
    SELECT * INTO saved FROM pg_roles WHERE rolname = '{role}';
    IF FOUND THEN
        IF shobj_description(saved.oid, 'pg_authid') IS DISTINCT FROM '{marker}'
           OR NOT saved.rolcanlogin OR saved.rolsuper OR saved.rolcreatedb OR saved.rolcreaterole
           OR saved.rolreplication OR saved.rolbypassrls OR saved.rolinherit
           OR EXISTS (SELECT 1 FROM pg_auth_members WHERE member = saved.oid) THEN
            RAISE EXCEPTION 'observation role is not owned by this Secret';
        END IF;
        RETURN;
    END IF;
    CREATE ROLE {role} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS
        CONNECTION LIMIT 12 PASSWORD '{verifier}';
    COMMENT ON ROLE {role} IS '{marker}';
    CREATE VIEW public.fugue_edge_quality_rollup_owner WITH (security_barrier = true) AS
        SELECT value AS owner FROM public.fugue_meta WHERE key = '{CHECKPOINT}';
    GRANT USAGE ON SCHEMA public TO {role};
    GRANT SELECT ON public.fugue_edge_quality_rollup_owner, public.fugue_edge_performance_samples TO {role};
    GRANT SELECT, INSERT, UPDATE ON public.fugue_edge_quality_rollup_watermarks TO {role};
    GRANT SELECT, INSERT, UPDATE, DELETE ON public.fugue_edge_quality_rollups TO {role};
END
$provision$;
COMMIT;
"""

    def run(self):
        c = self.config
        primary = self.find_primary()
        self.sql(primary[0], self.identity_sql())
        secret = self.get("secret", c["secret"], optional=True)
        if secret is None:
            role = c["role"]
            out = self.sql(primary[0], f"SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}');")
            if out != "f":
                raise ProvisionError("will not adopt a role without its original Secret")
            body = json.dumps(self.secret_body(secrets.token_hex(32)))
            self.kube("create", "--dry-run=server", "-f", "-", "-o", "name", body=body)
            self.kube("create", "-f", "-", "-o", "name", body=body)
            secret = self.get("secret", c["secret"])
        uid, password = self.credential(secret)
        if self.find_primary() != primary:
            raise ProvisionError("CNPG primary changed before provisioning")
        self.sql(primary[0], self.role_sql(uid, scram_verifier(password)))
        # The first worker readiness check validates the actual role and grants
        # over the declared write Service. Production ownership stays with API.
        print(self.success_message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("declaration", type=Path)
    args = parser.parse_args()
    try:
        Provision(json.loads(args.declaration.read_text())).run()
    except ProvisionError as err:
        parser.exit(1, f"observation database provisioning refused: {err}\n")
    except (KeyError, TypeError, ValueError, OSError):
        parser.exit(1, "observation database provisioning failed on malformed or unavailable input\n")


if __name__ == "__main__":
    main()
