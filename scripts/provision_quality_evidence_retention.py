"""Install only the bounded quality-evidence expiry capability in standby.

Invoke from the declarative observation-worker release on the checked-out
source. This command never installs the delete fence or transfers ownership.
"""

import argparse
import json
from pathlib import Path

from scripts.provision_observation_worker_database import Provision, ProvisionError, SCHEMA, UID
from scripts.verify_observation_writer_handoff import matches


INSTALLER = Path("internal/schemamigrate/platform_quality_evidence_retention.sql")


def worker_binding_sql(provision, secret_uid):
    if not matches(secret_uid, UID):
        raise ProvisionError("invalid observation worker Secret UID")
    role = provision.config["role"]
    marker = SCHEMA + ":" + secret_uid
    return f"""
{provision.identity_sql()}
DO $binding$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles AS r
        WHERE r.rolname = '{role}'
          AND shobj_description(r.oid, 'pg_authid') = '{marker}'
          AND r.rolcanlogin AND NOT r.rolsuper AND NOT r.rolinherit
          AND NOT r.rolcreatedb AND NOT r.rolcreaterole
          AND NOT r.rolreplication AND NOT r.rolbypassrls
          AND NOT EXISTS (SELECT 1 FROM pg_auth_members m WHERE m.member = r.oid)
    ) THEN
        RAISE EXCEPTION 'observation worker identity is not bound';
    END IF;
    IF has_table_privilege('{role}', 'public.fugue_platform_quality_evidence',
                           'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
       OR has_table_privilege('{role}', 'public.fugue_meta',
                              'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
       OR has_any_column_privilege('{role}', 'public.fugue_platform_quality_evidence',
                                   'SELECT, INSERT, UPDATE, REFERENCES')
       OR has_any_column_privilege('{role}', 'public.fugue_meta',
                                   'SELECT, INSERT, UPDATE, REFERENCES') THEN
        RAISE EXCEPTION 'observation maintenance role already has unsafe direct table privileges';
    END IF;
END
$binding$;
"""


def standby_sql(provision, secret_uid, installer):
    opening = "-- Install through the bound database administrator."
    if not installer.startswith(opening) or installer.count("\nBEGIN;\n") != 1 or not installer.endswith("\nCOMMIT;\n"):
        raise ProvisionError("unexpected quality evidence capability transaction")
    installer = installer.removesuffix("COMMIT;\n")
    before, inside = installer.split("\nBEGIN;\n", maxsplit=1)
    role = provision.config["role"]
    return f"""{before}
BEGIN;
{worker_binding_sql(provision, secret_uid)}
{inside}
GRANT USAGE ON SCHEMA fugue_observation_maintenance TO {role};
GRANT EXECUTE ON FUNCTION fugue_observation_maintenance.prune_quality_evidence_v1() TO {role};
GRANT SELECT ON fugue_observation_maintenance.quality_evidence_worker_progress TO {role};
COMMIT;
"""


def prepare_standby(provision, installer):
    primary = provision.find_primary()
    secret = provision.get("secret", provision.config["secret"])
    secret_uid, _ = provision.credential(secret)
    if provision.find_primary() != primary:
        raise ProvisionError("database primary changed before standby install")
    provision.sql(primary[0], standby_sql(provision, secret_uid, installer))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("--apply", action="store_true", required=True)
    args = parser.parse_args()
    try:
        provision = Provision(json.loads(args.database.read_text()))
        prepare_standby(provision, INSTALLER.read_text())
        print("quality evidence expiry capability installed; owner unchanged")
    except ProvisionError as err:
        parser.exit(1, f"quality evidence standby refused: {err}\n")
    except (KeyError, TypeError, ValueError, OSError):
        parser.exit(1, "quality evidence standby refused on malformed evidence\n")


if __name__ == "__main__":
    main()
