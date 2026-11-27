"""Provision the sample retention capability in standby, without transfer."""

import argparse
import json
from pathlib import Path

from scripts.provision_observation_worker_database import Provision, ProvisionError
from scripts.provision_quality_evidence_retention import worker_binding_sql


SCHEMA_DIR = Path("internal/schemamigrate")
FENCE = SCHEMA_DIR / "observation_sample_retention_fence.sql"
CAPABILITY = SCHEMA_DIR / "observation_sample_retention.sql"


def transaction_body(source):
    if source.count("\nBEGIN;\n") != 1 or source.count("\nCOMMIT;\n") != 1 or not source.endswith("\nCOMMIT;\n"):
        raise ProvisionError("unexpected standby installer transaction")
    return source.split("\nBEGIN;\n", maxsplit=1)[1].removesuffix("COMMIT;\n")


def sample_role_binding_sql(provision, secret_uid):
    role = provision.config["role"]
    return f"""{worker_binding_sql(provision, secret_uid)}
DO $capabilities$
DECLARE
    target text;
BEGIN
    FOREACH target IN ARRAY ARRAY['public.fugue_edge_network_samples',
                                   'public.fugue_edge_performance_samples',
                                   'public.fugue_resource_usage_samples'] LOOP
        -- The existing rollup role may keep its selected network and
        -- performance columns, but it never acquires raw write authority.
        IF has_table_privilege('{role}', target, 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
           OR has_any_column_privilege('{role}', target, 'INSERT,UPDATE,REFERENCES') THEN
            RAISE EXCEPTION 'observation worker already has excess sample write rights';
        END IF;
    END LOOP;
    FOREACH target IN ARRAY ARRAY['public.fugue_tenants', 'public.fugue_audit_events',
                                   'public.fugue_security_audit_chain_state',
                                   'public.fugue_resource_usage_samples'] LOOP
        IF has_table_privilege('{role}', target, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
           OR has_any_column_privilege('{role}', target, 'SELECT,INSERT,UPDATE,REFERENCES') THEN
            RAISE EXCEPTION 'observation worker already has excess private data rights';
        END IF;
    END LOOP;
END
$capabilities$;
"""


def standby_sql(provision, secret_uid, fence, capability):
    role = provision.config["role"]
    return f"""BEGIN;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '250ms';
{sample_role_binding_sql(provision, secret_uid)}
{transaction_body(fence)}
{transaction_body(capability)}
GRANT USAGE ON SCHEMA fugue_observation_maintenance TO {role};
GRANT EXECUTE ON FUNCTION fugue_observation_maintenance.prune_samples_v1(text,boolean,integer,integer,integer) TO {role};
GRANT SELECT ON fugue_observation_maintenance.sample_worker_progress TO {role};
COMMIT;
"""


def prepare_standby(provision, fence, capability):
    primary = provision.find_primary()
    secret = provision.get("secret", provision.config["secret"])
    secret_uid, _ = provision.credential(secret)
    if provision.find_primary() != primary:
        raise ProvisionError("database primary changed before standby install")
    provision.sql(primary[0], standby_sql(provision, secret_uid, fence, capability))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("--apply", action="store_true", required=True)
    args = parser.parse_args()
    try:
        provision = Provision(json.loads(args.database.read_text()))
        prepare_standby(provision, FENCE.read_text(), CAPABILITY.read_text())
        print("sample retention standby capability installed; writer unchanged")
    except ProvisionError as err:
        parser.exit(1, f"sample retention standby refused: {err}\n")
    except (KeyError, TypeError, ValueError, OSError):
        parser.exit(1, "sample retention standby refused on malformed evidence\n")


if __name__ == "__main__":
    main()
