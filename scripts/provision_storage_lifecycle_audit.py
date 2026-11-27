"""Install the storage audit capability and bind a standby login in CI.

This installer cannot transfer ownership or publish a changed policy. Once a
policy exists, it must match the declared policy, even on a release retry.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re

from scripts.provision_observation_worker_database import IDENTIFIER, NAME, UID, Provision, ProvisionError
from scripts.publish_storage_policy import manifest


SCHEMA = "fugue.storage-lifecycle-audit-database/v1"
ROLE_ANNOTATION = "fugue.pro/storage-lifecycle-audit-database-binding"
INSTALLER = Path(__file__).resolve().parents[1] / "internal/schemamigrate/storage_lifecycle_audit.sql"
POLICY_FIELDS = (
    "version", "mode", "effective_from", "interval_seconds", "source_page_rows", "history_days",
    "minimum_verified_versions", "unused_upload_hours", "unreferenced_upload_days", "warning_free_percent",
    "pressure_free_percent", "critical_free_percent",
)


def parse_config(raw):
    fields = {"schema", "namespace", "cluster", "clusterUID", "systemID", "database", "role", "apiRole", "secret"}
    if not isinstance(raw, dict) or set(raw) != fields or raw["schema"] != SCHEMA:
        raise ProvisionError("invalid storage audit database declaration")
    for key, expression in {
        "namespace": NAME, "cluster": NAME, "secret": NAME, "clusterUID": UID,
        "systemID": r"[1-9][0-9]{9,19}", "database": IDENTIFIER, "role": IDENTIFIER, "apiRole": IDENTIFIER,
    }.items():
        if not isinstance(raw[key], str) or not re.fullmatch(expression, raw[key]):
            raise ProvisionError("invalid storage audit database identity")
    if not raw["role"].startswith("fugue_storage_audit_") or raw["role"] in {raw["apiRole"], "fugue_storage_audit_definer"}:
        raise ProvisionError("storage audit worker must have a separate login")
    return raw


def canonical_policy(document):
    """Use the existing publisher's validation and the Go Policy.Digest order."""
    raw = manifest(document)["data"]["storage-lifecycle.json"]
    values = json.loads(raw)
    effective = datetime.fromisoformat(values["effective_from"].replace("Z", "+00:00")).astimezone(timezone.utc)
    if effective.year <= 1:
        raise ProvisionError("storage audit policy effective date is out of range")
    values["effective_from"] = effective.isoformat(timespec="seconds").replace("+00:00", "Z")
    body = json.dumps({field: values[field] for field in POLICY_FIELDS}, separators=(",", ":"))
    return values, body, hashlib.sha256(body.encode("ascii")).hexdigest()


class StorageAuditProvision(Provision):
    validate_config = staticmethod(parse_config)
    binding_annotation = ROLE_ANNOTATION
    success_message = "storage lifecycle audit standby bound; owner unchanged"

    def __init__(self, config, policy, kubectl="kubectl", *, require_standby=False):
        super().__init__(config, kubectl)
        if not isinstance(policy, dict) or policy.get("namespace") != self.config["namespace"]:
            raise ProvisionError("storage audit policy is declared for a different namespace")
        self.policy, self.policy_json, self.policy_digest = canonical_policy(policy)
        self.require_standby = require_standby

    def verify_published_policy(self, document):
        value = self.get("configmap", document["name"])
        actual = json.loads(value["data"]["storage-lifecycle.json"])
        _, _, actual_digest = canonical_policy({**document, "storageLifecycle": actual})
        if (value["metadata"].get("deletionTimestamp") or actual_digest != self.policy_digest):
            raise ProvisionError("live and declared storage lifecycle policies do not match")

    def role_sql(self, uid, verifier):
        if not re.fullmatch(UID, uid) or not re.fullmatch(r"SCRAM-SHA-256\$4096:[A-Za-z0-9+/=]+\$[A-Za-z0-9+/=]+:[A-Za-z0-9+/=]+", verifier):
            raise ProvisionError("invalid storage audit credential binding")
        installer = INSTALLER.read_text()
        opening = "-- Administrator-installed audit capability."
        if not installer.startswith(opening) or installer.count("\nBEGIN;\n") != 1 or not installer.endswith("\nCOMMIT;\n"):
            raise ProvisionError("unexpected storage audit installer transaction")
        before, inside = installer.removesuffix("COMMIT;\n").split("\nBEGIN;\n", maxsplit=1)
        role, api = self.config["role"], self.config["apiRole"]
        marker = SCHEMA + ":" + uid
        p = self.policy
        standby_check = """
    IF NOT EXISTS (SELECT 1 FROM fugue_storage_audit.lifecycle_control WHERE singleton AND owner = 'api') THEN
        RAISE EXCEPTION 'storage audit standby enrollment cannot touch an active owner';
    END IF;
""" if self.require_standby else ""
        return f"""{before}
BEGIN;
SET LOCAL lock_timeout = '250ms';
SET LOCAL statement_timeout = '5s';
SET LOCAL log_statement = 'none';
SET LOCAL log_min_error_statement = 'panic';
{self.identity_sql()}
SELECT pg_advisory_xact_lock(hashtextextended('{SCHEMA}:{role}', 0));
SELECT pg_advisory_xact_lock(hashtextextended('storage-lifecycle-audit/owner/v1', 0));
{inside}
DO $bind_standby$
DECLARE saved pg_roles%ROWTYPE;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles a
        WHERE a.rolname = '{api}' AND a.rolcanlogin AND NOT a.rolsuper
        AND (SELECT count(*) FROM pg_class c WHERE c.relowner = a.oid AND c.oid IN
            ('public.fugue_source_uploads'::regclass, 'public.fugue_apps'::regclass,
             'public.fugue_operations'::regclass, 'public.fugue_app_database_import_jobs'::regclass)) = 4
    ) THEN
        RAISE EXCEPTION 'storage audit API reader does not own the expected application schema';
    END IF;
    SELECT * INTO saved FROM pg_roles WHERE rolname = '{role}';
    IF FOUND THEN
        IF shobj_description(saved.oid, 'pg_authid') IS DISTINCT FROM '{marker}'
           OR NOT saved.rolcanlogin OR saved.rolsuper OR saved.rolcreatedb OR saved.rolcreaterole
           OR saved.rolreplication OR saved.rolbypassrls OR saved.rolinherit
           OR EXISTS (SELECT 1 FROM pg_auth_members m WHERE m.member = saved.oid OR m.roleid = saved.oid) THEN
            RAISE EXCEPTION 'storage audit login is not isolated and owned by this Secret';
        END IF;
    ELSE
        CREATE ROLE {role} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS
            CONNECTION LIMIT 5 PASSWORD '{verifier}';
        COMMENT ON ROLE {role} IS '{marker}';
    END IF;
    IF has_schema_privilege('{role}', 'fugue_storage_audit', 'CREATE') OR
       has_function_privilege('{role}', 'fugue_storage_audit.zero_temp_v1()', 'EXECUTE') THEN
        RAISE EXCEPTION 'storage audit worker has unscoped schema or helper privileges';
    END IF;
    IF has_schema_privilege('{api}', 'fugue_storage_audit', 'CREATE') OR
       has_function_privilege('{api}', 'fugue_storage_audit.sample_lifecycle_v1()', 'EXECUTE') OR
       has_function_privilege('{api}', 'fugue_storage_audit.zero_temp_v1()', 'EXECUTE') OR
       has_any_column_privilege('{api}', 'fugue_storage_audit.lifecycle_control', 'SELECT,INSERT,UPDATE,REFERENCES') OR
       has_any_column_privilege('{api}', 'fugue_storage_audit.lifecycle_progress', 'SELECT,INSERT,UPDATE,REFERENCES') OR
       has_table_privilege('{api}', 'fugue_storage_audit.lifecycle_control', 'DELETE,TRUNCATE,TRIGGER') OR
       has_table_privilege('{api}', 'fugue_storage_audit.lifecycle_progress', 'DELETE,TRUNCATE,TRIGGER') THEN
        RAISE EXCEPTION 'storage audit API already has authority beyond the redacted view';
    END IF;
    IF EXISTS (
        SELECT FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE ((n.nspname = 'public' AND left(c.relname, 6) = 'fugue_' AND c.relkind IN ('r','p','v','m','f'))
            OR (n.nspname = 'fugue_storage_audit' AND c.relname IN ('lifecycle_control','lifecycle_progress')))
          AND (has_any_column_privilege('{role}', c.oid, 'SELECT,INSERT,UPDATE,REFERENCES')
               OR has_table_privilege('{role}', c.oid, 'DELETE,TRUNCATE,TRIGGER'))
    ) THEN
        RAISE EXCEPTION 'storage audit worker has direct access to a Fugue relation';
    END IF;
END
$bind_standby$;
GRANT USAGE ON SCHEMA fugue_storage_audit TO {api}, {role};
GRANT SELECT ON fugue_storage_audit.lifecycle_status_v1 TO {api}, {role};
GRANT EXECUTE ON FUNCTION fugue_storage_audit.sample_lifecycle_v1() TO {role};
INSERT INTO fugue_storage_audit.lifecycle_control
    (singleton, owner, mode, policy_sha256, effective_from, interval_seconds, source_page_rows, policy_document)
    VALUES (true,'api','{p['mode']}','{self.policy_digest}','{p['effective_from']}',
            {p['interval_seconds']},{p['source_page_rows']},'{self.policy_json}'::jsonb)
    ON CONFLICT (singleton) DO NOTHING;
DO $policy_identity$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM fugue_storage_audit.lifecycle_control WHERE singleton
        AND policy_sha256 = '{self.policy_digest}' AND policy_document = '{self.policy_json}'::jsonb) THEN
        RAISE EXCEPTION 'storage audit policy already exists with a different revision';
    END IF;
{standby_check}
END
$policy_identity$;
COMMIT;
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("policy", type=Path)
    parser.add_argument("--apply", action="store_true", required=True)
    args = parser.parse_args()
    try:
        policy = json.loads(args.policy.read_text())
        provision = StorageAuditProvision(json.loads(args.database.read_text()), policy, require_standby=True)
        provision.verify_published_policy(policy)
        provision.run()
    except ProvisionError as err:
        parser.exit(1, f"storage audit standby refused: {err}\n")
    except (KeyError, TypeError, ValueError, OSError):
        parser.exit(1, "storage audit standby refused on malformed or unavailable input\n")


if __name__ == "__main__":
    main()
