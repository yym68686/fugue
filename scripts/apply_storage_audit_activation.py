"""Transfer storage audit ownership through the production release lane."""

import argparse
import json
from pathlib import Path

from scripts.provision_observation_worker_database import ProvisionError
from scripts.provision_storage_lifecycle_audit import StorageAuditProvision
from scripts.storage_audit_activation_transaction import activation_sql
from scripts.verify_storage_audit_activation import (
    parse_declaration, preflight, verify_committed_activation,
)


def current_owner(provision, primary):
    result = provision.sql(primary, f"""
BEGIN READ ONLY;
SET LOCAL statement_timeout = '3s';
{provision.identity_sql()}
SELECT owner FROM fugue_storage_audit.lifecycle_status_v1;
COMMIT;
""").splitlines()
    if len(result) != 2 or result[0] != "1" or result[1] not in ("api", "worker"):
        raise ProvisionError("storage audit ownership is missing or unsupported")
    return result[1]


def apply(provision, declaration, policy, scrape, api_floor, worker_floor):
    declaration = parse_declaration(declaration)
    if declaration["policyDigest"] != provision.policy_digest:
        raise ProvisionError("storage audit declaration and policy do not match")
    provision.verify_published_policy(policy)
    primary = provision.find_primary()
    if current_owner(provision, primary[0]) == "api":
        receipt = preflight(provision, declaration, policy, scrape, api_floor, worker_floor)
        if (primary != (receipt["primary"], receipt["primaryUid"]) or provision.find_primary() != primary
                or provision.credential(provision.get("secret", provision.config["secret"]))[0] != receipt["secretUid"]):
            raise ProvisionError("database or restricted credential changed at the storage audit write boundary")
        # The transaction independently checks the previously verified identity
        # and active policy under PostgreSQL's ownership lock. Do not repeat the
        # write on a lost reply. The next run observes which owner committed.
        provision.sql(primary[0], activation_sql(provision, receipt["secretUid"], receipt["declarationSha256"]))
    result = verify_committed_activation(provision, declaration, scrape, api_floor, worker_floor)
    provision.verify_published_policy(policy)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("database", "declaration", "policy", "scrape"):
        parser.add_argument(name, type=Path)
    parser.add_argument("api_floor")
    parser.add_argument("worker_floor")
    parser.add_argument("--apply", action="store_true", required=True)
    args = parser.parse_args()
    try:
        policy = json.loads(args.policy.read_text())
        provision = StorageAuditProvision(json.loads(args.database.read_text()), policy)
        result = apply(
            provision, json.loads(args.declaration.read_text()), policy,
            json.loads(args.scrape.read_text()), args.api_floor, args.worker_floor,
        )
        print(json.dumps(result, sort_keys=True))
    except ProvisionError as err:
        parser.exit(1, f"storage audit handoff stopped: {err}\n")
    except (AttributeError, KeyError, TypeError, ValueError, OSError):
        parser.exit(1, "storage audit handoff received missing or malformed evidence\n")


if __name__ == "__main__":
    main()
