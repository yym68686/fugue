"""Install the isolated database-import capability in standby, never activate it."""

import argparse
import hashlib
import json
from pathlib import Path
import re

from scripts.provision_observation_worker_database import (
    Provision, ProvisionError, SCHEMA as BASE_SCHEMA, UID, parse_config,
)


SCHEMA = "fugue.database-import-worker-database/v1"
ROLE = "fugue_app_db_import_worker"
DEFINER = "fugue_app_db_import_definer"
SECRET = "fugue-database-import-worker-database"
INSTALLER = Path(__file__).resolve().parents[1] / "internal/dbimportworker/app_database_import_worker_reads.sql"


def import_database_config(value):
    if (not isinstance(value, dict) or value.get("schema") != SCHEMA
            or value.get("role") != ROLE or value.get("secret") != SECRET):
        raise ProvisionError("invalid database import worker database declaration")
    parse_config({**value, "schema": BASE_SCHEMA, "role": "fugue_observation_validation"})
    return value


class ImportProvision(Provision):
    validate_config = staticmethod(import_database_config)
    binding_annotation = "fugue.pro/database-import-worker-database-binding"
    success_message = "database import worker capability provisioned in standby; API remains the executor"

    def role_sql(self, uid, verifier):
        if not re.fullmatch(UID, uid) or not re.fullmatch(r"SCRAM-SHA-256\$4096:[A-Za-z0-9+/=]+\$[A-Za-z0-9+/=]+:[A-Za-z0-9+/=]+", verifier):
            raise ProvisionError("invalid database import credential binding")
        raw = INSTALLER.read_bytes()
        installer = raw.decode("utf-8")
        if installer.count("\nBEGIN;\n") != 1 or not installer.endswith("\nCOMMIT;\n"):
            raise ProvisionError("invalid database import standby installer")
        body = installer.removesuffix("COMMIT;\n").split("\nBEGIN;\n", 1)[1]
        marker = f"{SCHEMA}:{uid}:{hashlib.sha256(raw).hexdigest()}"
        return rf"""BEGIN;
SET LOCAL statement_timeout = '8s';
SET LOCAL lock_timeout = '250ms';
SET LOCAL log_statement = 'none';
SET LOCAL log_min_error_statement = 'panic';
SET LOCAL search_path = pg_catalog;
{self.identity_sql()}
SELECT pg_advisory_xact_lock(hashtextextended('{SCHEMA}', 0));
SELECT to_regnamespace('{ROLE}') IS NULL AS needs_install \gset
\if :needs_install
DO $absent$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname IN ('{ROLE}', '{DEFINER}')) THEN
        RAISE EXCEPTION 'refusing to adopt an existing database import role';
    END IF;
END
$absent$;
CREATE ROLE {ROLE} LOGIN NOINHERIT CONNECTION LIMIT 6 PASSWORD '{verifier}';
CREATE ROLE {DEFINER} NOLOGIN NOINHERIT;
COMMENT ON ROLE {ROLE} IS '{marker}';
COMMENT ON ROLE {DEFINER} IS '{marker}';
{body}
COMMENT ON SCHEMA {ROLE} IS '{marker}';
\endif
DO $standby$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner
               WHERE n.nspname='{ROLE}' AND r.rolsuper AND obj_description(n.oid,'pg_namespace')='{marker}')
       OR 2 <> (SELECT count(*) FROM pg_roles r WHERE r.rolname IN ('{ROLE}', '{DEFINER}')
               AND shobj_description(r.oid,'pg_authid')='{marker}' AND r.rolcanlogin=(r.rolname='{ROLE}')
               AND NOT (r.rolinherit OR r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolbypassrls OR r.rolreplication)
               AND NOT EXISTS (SELECT FROM pg_auth_members m WHERE r.oid IN (m.roleid,m.member)))
       OR EXISTS (SELECT FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
               WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','f') AND
                   (has_any_column_privilege('{ROLE}',c.oid,'SELECT,INSERT,UPDATE,REFERENCES')
                       OR has_table_privilege('{ROLE}',c.oid,'DELETE,TRUNCATE,TRIGGER')))
       OR has_schema_privilege('{ROLE}','{ROLE}','CREATE')
       OR has_schema_privilege('{DEFINER}','{ROLE}','CREATE')
       OR EXISTS (SELECT FROM public.fugue_meta
               WHERE key='app-database-import/v1/writer' AND value IS DISTINCT FROM 'api')
       OR to_regnamespace('fugue_app_db_import_fence') IS NOT NULL THEN
        RAISE EXCEPTION 'database import standby identity, privileges or owner did not match';
    END IF;
END
$standby$;
COMMIT;
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("declaration", type=Path)
    parser.add_argument("--apply", action="store_true", required=True)
    args = parser.parse_args()
    try:
        ImportProvision(json.loads(args.declaration.read_text())).run()
    except ProvisionError as err:
        parser.exit(1, f"database import standby provisioning refused: {err}\n")
    except (KeyError, TypeError, ValueError, OSError):
        parser.exit(1, "database import standby provisioning failed on malformed or unavailable input\n")


if __name__ == "__main__":
    main()
