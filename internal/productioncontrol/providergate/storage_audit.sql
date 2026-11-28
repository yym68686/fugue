-- Storage audit policy is written here after the external writer has been
-- retired. The existing sampling capability continues using the same row
-- and advisory lock. Install the schema first, then freeze and enroll.

CREATE TABLE IF NOT EXISTS fugue_storage_audit.native_policy_freeze (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    cluster_id text NOT NULL CHECK (cluster_id ~ '^[a-z0-9][a-z0-9_-]{0,95}$'),
    baseline_digest text NOT NULL CHECK (baseline_digest ~ '^sha256:[a-f0-9]{64}$'),
    baseline_policy jsonb NOT NULL CHECK (jsonb_typeof(baseline_policy) = 'object'),
    evidence_digest text NOT NULL CHECK (evidence_digest ~ '^sha256:[a-f0-9]{64}$'),
    frozen_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS fugue_storage_audit.native_policy_authority (
    singleton boolean PRIMARY KEY DEFAULT true REFERENCES fugue_storage_audit.native_policy_freeze(singleton) CHECK (singleton),
    writer_name name NOT NULL,
    writer_oid oid NOT NULL,
    activation_evidence text NOT NULL CHECK (activation_evidence ~ '^sha256:[a-f0-9]{64}$'),
    activated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS fugue_storage_audit.native_policy_receipts (
    intent_digest text PRIMARY KEY REFERENCES fugue_control.execution_claims(intent_digest),
    approval_digest text NOT NULL REFERENCES fugue_control.approvals(digest),
    cluster_id text NOT NULL,
    fence bigint NOT NULL CHECK (fence > 0),
    writer_oid oid NOT NULL,
    writer_xid xid8 NOT NULL UNIQUE,
    previous_digest text NOT NULL CHECK (previous_digest ~ '^sha256:[a-f0-9]{64}$'),
    target_digest text NOT NULL CHECK (target_digest ~ '^sha256:[a-f0-9]{64}$' AND target_digest <> previous_digest),
    previous_policy jsonb NOT NULL,
    target_policy jsonb NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (cluster_id, fence)
);

REVOKE ALL ON fugue_storage_audit.native_policy_freeze, fugue_storage_audit.native_policy_authority,
    fugue_storage_audit.native_policy_receipts FROM PUBLIC;

CREATE OR REPLACE FUNCTION fugue_storage_audit.reject_native_policy_history_rewrite() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $body$
BEGIN
    RAISE EXCEPTION 'storage audit policy history is append only' USING ERRCODE = '55000';
END;
$body$;
REVOKE ALL ON FUNCTION fugue_storage_audit.reject_native_policy_history_rewrite() FROM PUBLIC;

DO $triggers$
DECLARE
    target name;
BEGIN
    FOREACH target IN ARRAY ARRAY['native_policy_freeze', 'native_policy_authority', 'native_policy_receipts'] LOOP
        EXECUTE format('CREATE OR REPLACE TRIGGER storage_native_policy_append_only
            BEFORE UPDATE OR DELETE OR TRUNCATE ON fugue_storage_audit.%I FOR EACH STATEMENT
            EXECUTE FUNCTION fugue_storage_audit.reject_native_policy_history_rewrite()', target);
        EXECUTE format('ALTER TABLE fugue_storage_audit.%I ENABLE ALWAYS TRIGGER storage_native_policy_append_only', target);
    END LOOP;
END;
$triggers$;

CREATE OR REPLACE FUNCTION fugue_storage_audit.native_policy_writer_isolated(candidate oid) RETURNS boolean
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $body$
    SELECT COALESCE((
        SELECT r.rolcanlogin AND NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls)
            AND NOT EXISTS (SELECT FROM pg_auth_members m WHERE m.roleid=r.oid OR m.member=r.oid)
            AND NOT (has_schema_privilege(r.oid, 'fugue_storage_audit', 'CREATE')
              OR has_schema_privilege(r.oid, 'fugue_control', 'CREATE')
              OR has_function_privilege(r.oid, 'fugue_storage_audit.sample_lifecycle_v1()', 'EXECUTE'))
            AND NOT EXISTS (
                SELECT FROM pg_class c JOIN pg_namespace ns ON ns.oid=c.relnamespace
                WHERE ns.nspname IN ('fugue_storage_audit','fugue_control') AND c.relkind IN ('r','p','v')
                  AND (c.relowner = r.oid OR has_any_column_privilege(r.oid,c.oid,'UPDATE')
                    OR has_table_privilege(r.oid,c.oid,'DELETE,TRUNCATE,TRIGGER')
                    OR (has_any_column_privilege(r.oid,c.oid,'INSERT')
                      AND (ns.nspname, c.relname) <> ('fugue_control','execution_claims')))
            )
        FROM pg_roles r WHERE r.oid = candidate
    ), false)
$body$;
REVOKE ALL ON FUNCTION fugue_storage_audit.native_policy_writer_isolated(oid) FROM PUBLIC;

CREATE OR REPLACE FUNCTION fugue_storage_audit.freeze_native_policy(cluster text, predecessor text, body jsonb, evidence text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $body$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'storage audit freeze requires a current snapshot';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('storage-lifecycle-audit/owner/v1', 0));
    IF NOT EXISTS (
        SELECT FROM fugue_storage_audit.lifecycle_control c
        JOIN fugue_storage_audit.lifecycle_activation a USING (singleton)
        WHERE c.singleton AND c.owner = 'worker' AND 'sha256:' || c.policy_sha256 = predecessor
          AND c.policy_document = body
    ) THEN
        RAISE EXCEPTION 'storage audit freeze does not match the installed worker and active policy' USING ERRCODE = '40001';
    END IF;
    INSERT INTO fugue_storage_audit.native_policy_freeze(cluster_id,baseline_digest,baseline_policy,evidence_digest)
      VALUES (cluster,predecessor,body,evidence) ON CONFLICT DO NOTHING;
    IF NOT EXISTS (SELECT FROM fugue_storage_audit.native_policy_freeze f WHERE f.singleton
        AND (f.cluster_id,f.baseline_digest,f.baseline_policy,f.evidence_digest) = (cluster,predecessor,body,evidence)) THEN
        RAISE EXCEPTION 'storage audit policy is already frozen at a different baseline' USING ERRCODE = '40001';
    END IF;
END;
$body$;
REVOKE ALL ON FUNCTION fugue_storage_audit.freeze_native_policy(text,text,jsonb,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION fugue_storage_audit.enroll_native_policy_writer(cluster text, writer name, evidence text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $body$
DECLARE
    candidate oid;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'storage audit enrollment requires a current snapshot';
    END IF;
    -- Journal and provider locks are always acquired in this order.
    PERFORM pg_advisory_xact_lock(hashtextextended('fugue.production.execution' || chr(10) || cluster ||
      chr(10) || 'maintenance/storage-audit-policy', 0));
    PERFORM pg_advisory_xact_lock(hashtextextended('storage-lifecycle-audit/owner/v1', 0));
    IF NOT EXISTS (
        SELECT FROM fugue_storage_audit.native_policy_freeze f
        JOIN fugue_control.resource_baselines b ON b.cluster_id=f.cluster_id
          AND b.resource_id='maintenance/storage-audit-policy' AND b.generation=1 AND b.verified_digest=f.baseline_digest
        JOIN fugue_storage_audit.lifecycle_control c USING (singleton)
        WHERE f.cluster_id=cluster AND b.evidence_digest=evidence AND f.evidence_digest<>evidence
          AND convert_from(b.verified_snapshot,'UTF8')::jsonb=f.baseline_policy
          AND c.owner='worker' AND 'sha256:' || c.policy_sha256=f.baseline_digest AND c.policy_document=f.baseline_policy
    ) OR EXISTS (SELECT FROM fugue_control.execution_claims c WHERE c.cluster_id=cluster
        AND c.resource_id='maintenance/storage-audit-policy') THEN
        RAISE EXCEPTION 'storage audit enrollment lacks the independently verified frozen predecessor' USING ERRCODE = '40001';
    END IF;
    SELECT oid INTO candidate FROM pg_roles WHERE rolname=writer;
    IF writer=session_user OR NOT fugue_storage_audit.native_policy_writer_isolated(candidate) THEN
        RAISE EXCEPTION 'storage audit policy writer login is not isolated' USING ERRCODE = '42501';
    END IF;
    INSERT INTO fugue_storage_audit.native_policy_authority(writer_name,writer_oid,activation_evidence)
      VALUES (writer,candidate,evidence) ON CONFLICT DO NOTHING;
    IF NOT EXISTS (SELECT FROM fugue_storage_audit.native_policy_authority a WHERE a.singleton
        AND (a.writer_name,a.writer_oid,a.activation_evidence) = (writer,candidate,evidence)) THEN
        RAISE EXCEPTION 'storage audit policy already has another enrolled writer' USING ERRCODE = '40001';
    END IF;
END;
$body$;
REVOKE ALL ON FUNCTION fugue_storage_audit.enroll_native_policy_writer(text,name,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION fugue_storage_audit.guard_native_policy_change() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $body$
BEGIN
    IF NEW IS NOT DISTINCT FROM OLD THEN
        RETURN NEW;
    END IF;
    IF current_setting('transaction_isolation') <> 'read committed' OR NOT
       pg_try_advisory_xact_lock(hashtextextended('storage-lifecycle-audit/owner/v1', 0)) THEN
        RAISE EXCEPTION 'storage audit policy boundary is busy or stale' USING ERRCODE = '40001';
    END IF;
    IF NOT EXISTS (SELECT FROM fugue_storage_audit.native_policy_freeze) THEN
        RETURN NEW;
    END IF;
    IF NEW.owner IS DISTINCT FROM OLD.owner OR NOT EXISTS (
        SELECT FROM fugue_storage_audit.native_policy_receipts r
        JOIN fugue_storage_audit.native_policy_authority a ON a.writer_oid=r.writer_oid
        JOIN pg_roles roles ON roles.oid=a.writer_oid AND roles.rolname=a.writer_name
        WHERE r.writer_xid=pg_current_xact_id() AND roles.rolname=session_user
          AND r.previous_digest='sha256:' || OLD.policy_sha256 AND r.previous_policy=OLD.policy_document
          AND r.target_digest='sha256:' || NEW.policy_sha256 AND r.target_policy=NEW.policy_document
    ) THEN
        RAISE EXCEPTION 'storage audit policy is fenced for native execution' USING ERRCODE = '42501';
    END IF;
    RETURN NEW;
END;
$body$;
REVOKE ALL ON FUNCTION fugue_storage_audit.guard_native_policy_change() FROM PUBLIC;
CREATE OR REPLACE TRIGGER storage_audit_native_policy_fence
BEFORE UPDATE ON fugue_storage_audit.lifecycle_control FOR EACH ROW
EXECUTE FUNCTION fugue_storage_audit.guard_native_policy_change();
ALTER TABLE fugue_storage_audit.lifecycle_control ENABLE ALWAYS TRIGGER storage_audit_native_policy_fence;

CREATE OR REPLACE FUNCTION fugue_storage_audit.require_native_policy_receipt() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $body$
BEGIN
    IF NOT EXISTS (SELECT FROM fugue_storage_audit.native_policy_receipts r
        WHERE r.intent_digest=NEW.intent_digest AND r.cluster_id=NEW.cluster_id AND r.approval_digest=NEW.approval_digest
          AND r.fence=NEW.fence AND r.writer_xid=pg_current_xact_id()) THEN
        RAISE EXCEPTION 'storage audit claim must commit with its policy receipt' USING ERRCODE = '40001';
    END IF;
    RETURN NEW;
END;
$body$;
REVOKE ALL ON FUNCTION fugue_storage_audit.require_native_policy_receipt() FROM PUBLIC;
DROP TRIGGER IF EXISTS storage_audit_claim_needs_receipt ON fugue_control.execution_claims;
CREATE CONSTRAINT TRIGGER storage_audit_claim_needs_receipt
AFTER INSERT ON fugue_control.execution_claims DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
WHEN (NEW.resource_id='maintenance/storage-audit-policy')
EXECUTE FUNCTION fugue_storage_audit.require_native_policy_receipt();
ALTER TABLE fugue_control.execution_claims ENABLE ALWAYS TRIGGER storage_audit_claim_needs_receipt;

CREATE OR REPLACE FUNCTION fugue_storage_audit.commit_native_policy(claim_id text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $body$
DECLARE
    authority record;
    request record;
    target jsonb;
    updated integer;
BEGIN
    SELECT f.cluster_id,a.writer_oid INTO authority
      FROM fugue_storage_audit.native_policy_freeze f JOIN fugue_storage_audit.native_policy_authority a USING (singleton)
      JOIN pg_roles r ON r.oid=a.writer_oid AND r.rolname=a.writer_name
      WHERE r.rolname=session_user AND fugue_storage_audit.native_policy_writer_isolated(r.oid);
    IF authority.writer_oid IS NULL OR current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'storage audit policy has no isolated native writer on this connection' USING ERRCODE = '42501';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('fugue.production.execution' || chr(10) || authority.cluster_id ||
      chr(10) || 'maintenance/storage-audit-policy', 0));
    PERFORM pg_advisory_xact_lock(hashtextextended('storage-lifecycle-audit/owner/v1', 0));
    SELECT c.fence,c.approval_digest,convert_from(i.envelope,'UTF8')::jsonb AS envelope,
      convert_from(i.payload,'UTF8')::jsonb AS payload
      INTO request
      FROM fugue_control.execution_claims c
      JOIN fugue_control.intents i ON i.digest=c.intent_digest
      JOIN fugue_control.approvals a ON a.digest=c.approval_digest AND a.intent_digest=c.intent_digest
      WHERE c.intent_digest=claim_id AND c.cluster_id=authority.cluster_id
        AND c.resource_id='maintenance/storage-audit-policy'
        AND c.xmin=pg_current_xact_id()::xid
        AND NOT EXISTS (SELECT FROM fugue_control.execution_results r WHERE r.intent_digest=c.intent_digest)
        AND NOT EXISTS (SELECT FROM fugue_control.execution_claims other WHERE other.cluster_id=c.cluster_id
          AND other.resource_id=c.resource_id AND (other.fence>c.fence OR
            (other.fence<c.fence AND NOT EXISTS (SELECT FROM fugue_control.execution_results r WHERE r.intent_digest=other.intent_digest))))
        AND (convert_from(a.envelope,'UTF8')::jsonb->>'issuedAtUnix')::bigint <= extract(epoch FROM clock_timestamp())
        AND (convert_from(a.envelope,'UTF8')::jsonb->>'expiresAtUnix')::bigint > extract(epoch FROM clock_timestamp());
    IF request.envelope IS NULL OR request.envelope->>'operation' IS DISTINCT FROM 'maintenance'
       OR request.payload->>'schema' IS DISTINCT FROM 'fugue.storage-audit-policy-shadow/v1'
       OR request.payload->'publishedPolicyRetired' IS DISTINCT FROM 'true'::jsonb
       OR request.envelope->>'expectedCurrentDigest' IS DISTINCT FROM request.payload#>>'{current,digest}'
       OR request.envelope->>'targetDigest' IS DISTINCT FROM request.payload#>>'{target,digest}'
       OR request.envelope->>'rollbackDigest' IS DISTINCT FROM request.payload#>>'{current,digest}'
       OR request.payload->'current' IS DISTINCT FROM request.payload->'rollback'
       OR NOT EXISTS (
           SELECT FROM fugue_storage_audit.lifecycle_activation a WHERE a.singleton
             AND a.approval_sha256=request.payload#>>'{activation,declarationDigest}'
             AND a.policy_sha256=request.payload#>>'{activation,policyDigest}'
             AND a.worker_secret_uid::text=request.payload#>>'{activation,workerSecretUid}'
             AND a.activated_at=(request.payload#>>'{activation,activatedAt}')::timestamptz)
       OR NOT EXISTS (
           SELECT FROM fugue_control.resource_baselines b
             LEFT JOIN fugue_control.execution_claims p ON p.cluster_id=b.cluster_id AND p.resource_id=b.resource_id AND p.fence=request.fence-1
             LEFT JOIN fugue_control.execution_results r ON r.intent_digest=p.intent_digest
           WHERE b.cluster_id=authority.cluster_id AND b.resource_id='maintenance/storage-audit-policy'
             AND (request.fence=1 OR r.intent_digest IS NOT NULL)
             AND COALESCE(r.verified_digest,b.verified_digest)=request.payload#>>'{current,digest}'
             AND COALESCE(r.generation,b.generation)=(request.envelope->>'expectedGeneration')::bigint)
       OR request.payload#>>'{provider,database}' IS DISTINCT FROM current_database()
       OR request.payload#>>'{provider,role}'=session_user
       OR NOT EXISTS (SELECT FROM pg_control_system() p WHERE p.system_identifier::text=request.payload#>>'{provider,systemId}')
       OR pg_is_in_recovery() THEN
        RAISE EXCEPTION 'storage audit policy has no exact open approval at this provider' USING ERRCODE = '40001';
    END IF;
    target := request.payload#>'{target,document}';
    INSERT INTO fugue_storage_audit.native_policy_receipts
      (intent_digest,approval_digest,cluster_id,fence,writer_oid,writer_xid,previous_digest,target_digest,previous_policy,target_policy)
      VALUES (claim_id,request.approval_digest,authority.cluster_id,request.fence,authority.writer_oid,pg_current_xact_id(),
        request.payload#>>'{current,digest}',request.payload#>>'{target,digest}',request.payload#>'{current,document}',target);
    UPDATE fugue_storage_audit.lifecycle_control c SET
      mode=target->>'mode', policy_sha256=substr(request.payload#>>'{target,digest}',8), effective_from=(target->>'effective_from')::timestamptz,
      interval_seconds=(target->>'interval_seconds')::int, source_page_rows=(target->>'source_page_rows')::int, policy_document=target
      WHERE c.singleton AND c.owner='worker' AND 'sha256:' || c.policy_sha256=request.payload#>>'{current,digest}'
        AND c.policy_document=request.payload#>'{current,document}';
    GET DIAGNOSTICS updated = ROW_COUNT;
    IF updated <> 1 THEN
        RAISE EXCEPTION 'storage audit provider predecessor drifted' USING ERRCODE = '40001';
    END IF;
END;
$body$;
REVOKE ALL ON FUNCTION fugue_storage_audit.commit_native_policy(text) FROM PUBLIC;
