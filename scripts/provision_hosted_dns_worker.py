"""Provision the dedicated PostgreSQL login for the hosted DNS standby worker."""

import argparse
import json
from pathlib import Path
import re

from scripts.provision_observation_worker_database import Provision, ProvisionError, SCHEMA as BASE_SCHEMA, UID, parse_config


SCHEMA = "fugue.hosted-dns-worker-database/v1"
INSTALLER = Path(__file__).resolve().parents[1] / "internal/schemamigrate/hosted_dns_flatten_worker.sql"
ROLE = "fugue_hosted_dns_refresh"
SECRET = "fugue-hosted-dns-worker-database"


def hosted_dns_config(value):
    if (not isinstance(value, dict) or value.get("schema") != SCHEMA
            or value.get("role") != ROLE or value.get("secret") != SECRET):
        raise ProvisionError("invalid hosted DNS database declaration")
    parse_config({**value, "schema": BASE_SCHEMA, "role": "fugue_observation_validation"})
    return value


class HostedDNSProvision(Provision):
    validate_config = staticmethod(hosted_dns_config)
    binding_annotation = "fugue.pro/hosted-dns-database-binding"
    success_message = "hosted DNS database credential provisioned in standby"

    def role_sql(self, uid, verifier):
        if not re.fullmatch(UID, uid) or not re.fullmatch(r"SCRAM-SHA-256\$4096:[A-Za-z0-9+/=]+\$[A-Za-z0-9+/=]+:[A-Za-z0-9+/=]+", verifier):
            raise ProvisionError("invalid hosted DNS credential binding")
        installer = INSTALLER.read_text()
        if installer.count("\nBEGIN;\n") != 1 or not installer.endswith("\nCOMMIT;\n"):
            raise ProvisionError("invalid hosted DNS installer")
        body = installer.removesuffix("COMMIT;\n").split("\nBEGIN;\n", 1)[1]
        return f"""BEGIN;
SET LOCAL statement_timeout = '8s';
SET LOCAL lock_timeout = '250ms';
SET LOCAL log_statement = 'none';
SET LOCAL log_min_error_statement = 'panic';
{self.identity_sql()}
SELECT pg_advisory_xact_lock(hashtextextended('hosted-dns-flatten/v1/writer', 0));
{body}
DO $bind_standby$
DECLARE saved pg_roles%ROWTYPE;
BEGIN
    IF fugue_hosted_dns_maintenance.writer_v1() <> 'api' THEN
        RAISE EXCEPTION 'hosted DNS capability installer requires API ownership';
    END IF;
    SELECT * INTO saved FROM pg_roles WHERE rolname = '{ROLE}';
    IF FOUND THEN
        IF shobj_description(saved.oid, 'pg_authid') IS DISTINCT FROM '{SCHEMA}:{uid}'
           OR NOT saved.rolcanlogin OR saved.rolsuper OR saved.rolcreatedb OR saved.rolcreaterole
           OR saved.rolreplication OR saved.rolbypassrls OR saved.rolinherit
           OR EXISTS (SELECT 1 FROM pg_auth_members WHERE member = saved.oid OR roleid = saved.oid) THEN
            RAISE EXCEPTION 'hosted DNS database login does not match the original Secret';
        END IF;
    ELSE
        CREATE ROLE {ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS
            CONNECTION LIMIT 6 PASSWORD '{verifier}';
        COMMENT ON ROLE {ROLE} IS '{SCHEMA}:{uid}';
    END IF;
    IF has_schema_privilege('{ROLE}', 'fugue_hosted_dns_maintenance', 'CREATE') OR EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND left(c.relname, 6) = 'fugue_' AND c.relkind IN ('r','p','v','m','f')
          AND (has_any_column_privilege('{ROLE}', c.oid, 'SELECT,INSERT,UPDATE,REFERENCES')
               OR has_table_privilege('{ROLE}', c.oid, 'DELETE,TRUNCATE,TRIGGER'))
    ) THEN
        RAISE EXCEPTION 'hosted DNS worker has unscoped database privileges';
    END IF;
END
$bind_standby$;
GRANT USAGE ON SCHEMA fugue_hosted_dns_maintenance TO {ROLE};
GRANT EXECUTE ON FUNCTION
    fugue_hosted_dns_maintenance.progress_v1(integer,text),
    fugue_hosted_dns_maintenance.records_v1(text),
    fugue_hosted_dns_maintenance.commit_v1(text,text,text,text,timestamptz,timestamptz,jsonb,jsonb,text,text,timestamptz,text,text)
TO {ROLE};
COMMIT;
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("declaration", type=Path)
    parser.add_argument("--apply", action="store_true", required=True)
    args = parser.parse_args()
    try:
        HostedDNSProvision(json.loads(args.declaration.read_text())).run()
    except ProvisionError as err:
        parser.exit(1, f"hosted DNS provisioning refused: {err}\n")
    except (KeyError, OSError, TypeError, ValueError):
        parser.exit(1, "hosted DNS provisioning failed on malformed input\n")


if __name__ == "__main__":
    main()
