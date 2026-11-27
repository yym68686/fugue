"""Build the single forward-only storage audit ownership transaction.

The caller must first validate the live API and worker, immutable rollback
targets and scraped standby evidence. No release or shell entrypoint is
provided here. Database checks repeat the identity and revision predicates at
the write boundary and persist a retry witness with the ownership change.
"""

import re

from scripts.provision_observation_worker_database import UID, ProvisionError
from scripts.provision_storage_lifecycle_audit import SCHEMA


def activation_sql(provision, secret_uid, approval_digest):
    if not isinstance(secret_uid, str) or not re.fullmatch(UID, secret_uid):
        raise ProvisionError("invalid storage audit activation Secret UID")
    if not isinstance(approval_digest, str) or not re.fullmatch(r"[a-f0-9]{64}", approval_digest):
        raise ProvisionError("invalid storage audit activation declaration digest")
    role, api = provision.config["role"], provision.config["apiRole"]
    marker = SCHEMA + ":" + secret_uid
    wanted, document = provision.policy_digest, provision.policy_json
    return f"""
BEGIN;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '350ms';
{provision.identity_sql()}
SELECT pg_advisory_xact_lock(hashtextextended('storage-lifecycle-audit/owner/v1', 0));
CREATE TABLE IF NOT EXISTS fugue_storage_audit.lifecycle_activation (
    singleton boolean PRIMARY KEY CHECK (singleton),
    approval_sha256 text NOT NULL CHECK (approval_sha256 ~ '^[a-f0-9]{{64}}$'),
    policy_sha256 text NOT NULL CHECK (policy_sha256 ~ '^[a-f0-9]{{64}}$'),
    worker_secret_uid uuid NOT NULL,
    activated_at timestamptz NOT NULL CHECK (isfinite(activated_at))
);
REVOKE ALL ON TABLE fugue_storage_audit.lifecycle_activation FROM PUBLIC, {api}, {role}, fugue_storage_audit_definer;
DO $activate_storage_audit$
DECLARE controlled record;
        prior record;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles AS watcher
        WHERE watcher.rolname = '{role}' AND watcher.rolcanlogin AND NOT
            (watcher.rolsuper OR watcher.rolcreatedb OR watcher.rolcreaterole OR watcher.rolreplication
             OR watcher.rolbypassrls OR watcher.rolinherit)
          AND shobj_description(watcher.oid, 'pg_authid') = '{marker}'
          AND NOT EXISTS (SELECT 1 FROM pg_auth_members m WHERE m.member = watcher.oid OR m.roleid = watcher.oid)
    ) OR NOT has_table_privilege('{role}', 'fugue_storage_audit.lifecycle_status_v1', 'SELECT')
      OR NOT has_function_privilege('{role}', 'fugue_storage_audit.sample_lifecycle_v1()', 'EXECUTE')
      OR NOT has_table_privilege('{api}', 'fugue_storage_audit.lifecycle_status_v1', 'SELECT')
      OR has_function_privilege('{api}', 'fugue_storage_audit.sample_lifecycle_v1()', 'EXECUTE')
    THEN
        RAISE EXCEPTION 'storage audit roles no longer match their approved boundaries';
    END IF;
    IF (SELECT count(*) FROM pg_trigger WHERE tgrelid = 'fugue_storage_audit.lifecycle_control'::regclass
            AND tgname IN ('storage_audit_owner_no_downgrade', 'storage_audit_owner_no_truncate')
            AND tgfoid = 'fugue_storage_audit.keep_lifecycle_owner_v1()'::regprocedure AND tgenabled = 'A') <> 2 THEN
        RAISE EXCEPTION 'storage audit owner guard is not installed and enabled';
    END IF;
    SELECT owner, mode, effective_from, policy_sha256, policy_document INTO controlled
        FROM fugue_storage_audit.lifecycle_control WHERE singleton FOR UPDATE;
    IF NOT FOUND OR controlled.policy_sha256 <> '{wanted}' OR controlled.policy_document <> '{document}'::jsonb
       OR controlled.mode <> 'audit' OR controlled.effective_from > clock_timestamp() THEN
        RAISE EXCEPTION 'storage audit policy does not match the active handoff declaration';
    END IF;
    SELECT approval_sha256, policy_sha256, worker_secret_uid::text INTO prior
        FROM fugue_storage_audit.lifecycle_activation WHERE singleton;
    IF FOUND THEN
        IF controlled.owner <> 'worker' OR prior.approval_sha256 <> '{approval_digest}'
           OR prior.policy_sha256 <> '{wanted}' OR prior.worker_secret_uid <> '{secret_uid}' THEN
            RAISE EXCEPTION 'storage audit owner already has a different handoff witness';
        END IF;
        RETURN;
    END IF;
    IF controlled.owner <> 'api' OR EXISTS (SELECT 1 FROM fugue_storage_audit.lifecycle_progress) THEN
        RAISE EXCEPTION 'storage audit is not an idle API-owned standby';
    END IF;
    INSERT INTO fugue_storage_audit.lifecycle_activation
        (singleton, approval_sha256, policy_sha256, worker_secret_uid, activated_at)
        VALUES (true, '{approval_digest}', '{wanted}', '{secret_uid}', clock_timestamp());
    UPDATE fugue_storage_audit.lifecycle_control SET owner = 'worker' WHERE singleton;
END
$activate_storage_audit$;
SELECT json_build_object('approvalSha256', approval_sha256, 'policyDigest', policy_sha256,
                         'activatedAt', activated_at)
FROM fugue_storage_audit.lifecycle_activation WHERE singleton;
COMMIT;
"""
