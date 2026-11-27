"""Publish only the audit policy to the installed storage audit capability.

Called by the ordinary storage policy job after its existing ConfigMap
preflight and before that ConfigMap is applied. This cannot select an executor
or touch business data. A retry after the database commit is idempotent.
"""

import argparse
import json
from pathlib import Path
import re

from scripts.provision_observation_worker_database import ProvisionError
from scripts.provision_storage_lifecycle_audit import StorageAuditProvision, canonical_policy


def policy_update_sql(provision, live_document, *, apply):
    _, live_json, live_digest = canonical_policy(live_document)
    p = provision.policy
    desired, wanted = provision.policy_json, provision.policy_digest
    if not all(re.fullmatch(r"[a-f0-9]{64}", value) for value in (live_digest, wanted)):
        raise ProvisionError("invalid audit policy identity")
    # Both documents have been fully validated and re-encoded as timestamps,
    # integers and fixed enumerations; nothing else can be interpolated.
    return f"""
BEGIN;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '350ms';
{provision.identity_sql()}
SELECT pg_advisory_xact_lock(hashtextextended('storage-lifecycle-audit/owner/v1', 0));
DO $publish_storage_audit_policy$
DECLARE previous record;
BEGIN
    IF to_regnamespace('fugue_storage_audit') IS NULL THEN
        PERFORM set_config('fugue.storage_audit_policy_outcome', 'legacy', true);
        RETURN;
    END IF;
    IF to_regclass('fugue_storage_audit.lifecycle_control') IS NULL THEN
        RAISE EXCEPTION 'storage audit capability is only partially installed';
    END IF;
    SELECT owner, policy_sha256, policy_document INTO previous
        FROM fugue_storage_audit.lifecycle_control WHERE singleton FOR UPDATE;
    IF NOT FOUND OR previous.owner NOT IN ('api', 'worker') THEN
        RAISE EXCEPTION 'storage audit has no bound executor';
    END IF;
    IF previous.policy_sha256 = '{wanted}' AND previous.policy_document = '{desired}'::jsonb THEN
        PERFORM set_config('fugue.storage_audit_policy_outcome', previous.owner || ':unchanged', true);
        RETURN;
    END IF;
    IF previous.policy_sha256 <> '{live_digest}' OR previous.policy_document <> '{live_json}'::jsonb THEN
        RAISE EXCEPTION 'stored storage audit policy is neither the live predecessor nor the approved candidate';
    END IF;
    UPDATE fugue_storage_audit.lifecycle_control SET
        mode = '{p["mode"]}', policy_sha256 = '{wanted}', effective_from = '{p["effective_from"]}',
        interval_seconds = {p["interval_seconds"]}, source_page_rows = {p["source_page_rows"]},
        policy_document = '{desired}'::jsonb
    WHERE singleton;
    PERFORM set_config('fugue.storage_audit_policy_outcome', previous.owner || ':updated', true);
END
$publish_storage_audit_policy$;
SELECT current_setting('fugue.storage_audit_policy_outcome');
{"COMMIT" if apply else "ROLLBACK"};
"""


def publish(provision, document, *, apply):
    primary = provision.find_primary()
    value = provision.get("configmap", document["name"])
    if value["metadata"].get("deletionTimestamp"):
        raise ProvisionError("storage policy ConfigMap is being deleted")
    live = {**document, "storageLifecycle": json.loads(value["data"]["storage-lifecycle.json"])}
    if provision.find_primary() != primary:
        raise ProvisionError("database changed before publishing the storage audit policy")
    result = provision.sql(primary[0], policy_update_sql(provision, live, apply=apply)).splitlines()
    if len(result) != 3 or result[0] != "1" or result[-1] not in {
            "legacy", "api:unchanged", "api:updated", "worker:unchanged", "worker:updated"}:
        raise ProvisionError("database did not confirm the storage audit policy")
    return {"candidatePolicyDigest": provision.policy_digest, "outcome": result[-1], "applied": apply}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("policy", type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        document = json.loads(args.policy.read_text())
        provision = StorageAuditProvision(json.loads(args.database.read_text()), document)
        print(json.dumps(publish(provision, document, apply=args.apply), sort_keys=True))
    except ProvisionError as err:
        parser.exit(1, f"storage audit policy refused: {err}\n")
    except (KeyError, TypeError, ValueError, OSError):
        parser.exit(1, "storage audit policy refused on missing or malformed evidence\n")


if __name__ == "__main__":
    main()
