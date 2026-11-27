// Package providergate contains provider-side enforcement, separate from the
// journal's authority records. Merely possessing a signed journal intent
// does not grant a mutation at the provider.
package providergate

// GlobalProducerFreezeSchema is installed with the platform-store schema.
// It does not freeze anything on installation. Once the global producer
// selection is frozen, even a legacy application binary using the same
// tables cannot publish, roll back, rename, or delete a policy selection.
// Other lanes (including the producer's serving traffic publications) keep
// operating. There is deliberately no transition back to legacy authority.
//
// Native authority is a separate append after freeze and independent
// baseline enrollment. A provider receipt is bound to the same transaction
// as the exact approved selection. Neither is an API-supplied flag.
const GlobalProducerFreezeSchema = `
CREATE TABLE IF NOT EXISTS fugue_native_global_producer_gate (
  singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
  frozen BOOLEAN NOT NULL DEFAULT FALSE,
  baseline_release_id TEXT,
  baseline_fence BIGINT,
  baseline_digest TEXT,
  evidence_digest TEXT,
  frozen_at TIMESTAMPTZ,
  CHECK ((NOT frozen AND baseline_release_id IS NULL AND baseline_fence IS NULL
          AND baseline_digest IS NULL AND evidence_digest IS NULL AND frozen_at IS NULL)
     OR (frozen AND baseline_release_id IS NOT NULL AND baseline_release_id <> ''
          AND baseline_fence IS NOT NULL AND baseline_fence > 0
          AND baseline_digest IS NOT NULL AND baseline_digest ~ '^sha256:[0-9a-f]{64}$'
          AND evidence_digest IS NOT NULL AND evidence_digest ~ '^sha256:[0-9a-f]{64}$'
          AND frozen_at IS NOT NULL))
);
INSERT INTO fugue_native_global_producer_gate(singleton) VALUES(TRUE) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS fugue_native_global_producer_authority (
  singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
  cluster_id TEXT NOT NULL CHECK (cluster_id ~ '^[a-z0-9][a-z0-9_-]{0,95}$'),
  writer_role TEXT NOT NULL CHECK (writer_role <> ''),
  writer_oid OID NOT NULL,
  baseline_evidence_digest TEXT NOT NULL CHECK (baseline_evidence_digest ~ '^sha256:[0-9a-f]{64}$'),
  activation_evidence_digest TEXT NOT NULL CHECK (activation_evidence_digest ~ '^sha256:[0-9a-f]{64}$'),
  activated_at TIMESTAMPTZ NOT NULL,
  CHECK (baseline_evidence_digest <> activation_evidence_digest)
);

CREATE TABLE IF NOT EXISTS fugue_native_global_producer_receipts (
  intent_digest TEXT PRIMARY KEY CHECK (intent_digest ~ '^sha256:[0-9a-f]{64}$'),
  approval_digest TEXT NOT NULL CHECK (approval_digest ~ '^sha256:[0-9a-f]{64}$'),
  claim_fence BIGINT NOT NULL CHECK (claim_fence > 0),
  cluster_id TEXT NOT NULL,
  writer_xid XID8 NOT NULL UNIQUE,
  writer_role TEXT NOT NULL,
  previous_artifact_id TEXT NOT NULL,
  previous_digest TEXT NOT NULL CHECK (previous_digest ~ '^sha256:[0-9a-f]{64}$'),
  previous_generation TEXT NOT NULL,
  previous_sequence BIGINT NOT NULL CHECK (previous_sequence > 0),
  previous_release_id TEXT NOT NULL,
  previous_fence BIGINT NOT NULL CHECK (previous_fence > 0 AND previous_fence < 9223372036854775807),
  target_artifact_id TEXT NOT NULL,
  target_digest TEXT NOT NULL CHECK (target_digest ~ '^sha256:[0-9a-f]{64}$'),
  target_generation TEXT NOT NULL,
  target_sequence BIGINT NOT NULL CHECK (target_sequence > previous_sequence),
  idempotency_key TEXT NOT NULL CHECK (idempotency_key ~ '^producer-reconfiguration/sha256:[0-9a-f]{64}$'),
  approved_shadow_digest TEXT NOT NULL CHECK (approved_shadow_digest ~ '^sha256:[0-9a-f]{64}$'),
  live_shadow_digest TEXT NOT NULL CHECK (live_shadow_digest ~ '^sha256:[0-9a-f]{64}$'),
  recorded_at TIMESTAMPTZ NOT NULL,
  CHECK (target_artifact_id <> previous_artifact_id AND target_digest <> previous_digest)
);

CREATE OR REPLACE FUNCTION fugue_native_global_producer_append_only() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  RAISE EXCEPTION 'global producer authority and execution history are append only' USING ERRCODE = '40001';
END;
$$;
REVOKE ALL ON FUNCTION fugue_native_global_producer_append_only() FROM PUBLIC;

CREATE OR REPLACE FUNCTION fugue_native_global_producer_enable() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
  boundary record;
  writer record;
  gate_owner oid;
BEGIN
  -- Gate activation is isolated from dispatch. It is not an override or a
  -- way to reopen the legacy lane. The coordinator supplies the independently
  -- verified evidence; the provider cross-checks persistent enrollment.
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'global producer activation requires a current observation' USING ERRCODE = '40001';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('fugue.production.execution' || chr(10) || NEW.cluster_id || chr(10) || 'producer/global-policy',0));
  EXECUTE format('SELECT * FROM %I.fugue_native_global_producer_gate WHERE singleton FOR UPDATE', TG_TABLE_SCHEMA) INTO boundary;
  IF boundary.frozen IS DISTINCT FROM TRUE OR NOT EXISTS (
    SELECT 1 FROM fugue_control.resource_baselines b WHERE b.cluster_id=NEW.cluster_id
      AND b.resource_id='producer/global-policy' AND b.generation=boundary.baseline_fence
      AND b.verified_digest=boundary.baseline_digest AND b.evidence_digest=NEW.baseline_evidence_digest
  ) OR EXISTS (SELECT 1 FROM fugue_control.execution_claims c WHERE c.cluster_id=NEW.cluster_id AND c.resource_id='producer/global-policy') THEN
    RAISE EXCEPTION 'global producer activation lacks exact closed baseline enrollment' USING ERRCODE = '40001';
  END IF;
  SELECT oid,rolcanlogin,rolsuper,rolcreaterole,rolreplication,rolbypassrls INTO writer FROM pg_roles WHERE rolname=NEW.writer_role;
  SELECT relowner INTO gate_owner FROM pg_class WHERE oid=format('%I.fugue_native_global_producer_gate',TG_TABLE_SCHEMA)::regclass;
  IF writer.oid IS NULL OR NOT writer.rolcanlogin OR writer.rolsuper OR writer.rolcreaterole OR writer.rolreplication
     OR writer.rolbypassrls OR NEW.writer_role=session_user OR pg_has_role(writer.oid,gate_owner,'MEMBER') THEN
    RAISE EXCEPTION 'global producer requires a separate unprivileged writer login' USING ERRCODE = '40001';
  END IF;
  NEW.writer_oid := writer.oid;
  NEW.activated_at := clock_timestamp();
  RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION fugue_native_global_producer_enable() FROM PUBLIC;

CREATE OR REPLACE FUNCTION fugue_native_global_producer_reserve() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
  authority record;
  boundary BOOLEAN;
  operation record;
  inverse BOOLEAN;
  selected BOOLEAN;
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'global producer dispatch requires a current observation' USING ERRCODE = '40001';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('fugue.production.execution' || chr(10) || NEW.cluster_id || chr(10) || 'producer/global-policy',0));
  EXECUTE format('SELECT frozen FROM %I.fugue_native_global_producer_gate WHERE singleton FOR SHARE', TG_TABLE_SCHEMA) INTO boundary;
  EXECUTE format('SELECT * FROM %I.fugue_native_global_producer_authority WHERE singleton', TG_TABLE_SCHEMA) INTO authority;
  IF boundary IS DISTINCT FROM TRUE OR authority.cluster_id IS DISTINCT FROM NEW.cluster_id OR authority.writer_role IS DISTINCT FROM session_user::text
     OR authority.writer_oid IS DISTINCT FROM (SELECT oid FROM pg_roles WHERE rolname=session_user) THEN
    RAISE EXCEPTION 'global producer has no active native writer for this login' USING ERRCODE = '40001';
  END IF;
  SELECT convert_from(i.envelope,'UTF8')::jsonb AS envelope, convert_from(i.payload,'UTF8')::jsonb AS payload,
    convert_from(a.envelope,'UTF8')::jsonb AS approval INTO operation
    FROM fugue_control.execution_claims c JOIN fugue_control.intents i ON i.digest=c.intent_digest
    JOIN fugue_control.approvals a ON a.digest=c.approval_digest AND a.intent_digest=c.intent_digest
    WHERE c.intent_digest=NEW.intent_digest AND c.approval_digest=NEW.approval_digest
      AND c.cluster_id=NEW.cluster_id AND c.resource_id='producer/global-policy' AND c.fence=NEW.claim_fence
      AND NOT EXISTS(SELECT 1 FROM fugue_control.execution_results r WHERE r.intent_digest=c.intent_digest)
      AND NOT EXISTS(SELECT 1 FROM fugue_control.execution_claims p WHERE p.cluster_id=c.cluster_id AND p.resource_id=c.resource_id AND p.fence<c.fence
        AND NOT EXISTS(SELECT 1 FROM fugue_control.execution_results r WHERE r.intent_digest=p.intent_digest))
      AND NOT EXISTS(SELECT 1 FROM fugue_control.execution_claims n WHERE n.cluster_id=c.cluster_id AND n.resource_id=c.resource_id AND n.fence>c.fence);
  IF operation.envelope IS NULL OR operation.envelope->>'operation' IS DISTINCT FROM 'dns_membership'
     OR operation.payload->>'schema' IS DISTINCT FROM 'fugue.native-global-dns-policy/v1'
     OR operation.payload#>>'{precondition,operation}' IS DISTINCT FROM 'edge_offline'
     OR operation.approval->>'intentDigest' IS DISTINCT FROM NEW.intent_digest
     OR NOT COALESCE((operation.approval->>'issuedAtUnix')::bigint <= EXTRACT(EPOCH FROM clock_timestamp())
       AND (operation.approval->>'expiresAtUnix')::bigint > EXTRACT(EPOCH FROM clock_timestamp()),FALSE) THEN
    RAISE EXCEPTION 'global producer dispatch has no open exact policy claim' USING ERRCODE = '40001';
  END IF;
  NEW.writer_xid := pg_current_xact_id();
  NEW.writer_role := session_user;
  NEW.previous_artifact_id := operation.payload#>>'{previousProducer,id}';
  NEW.previous_digest := operation.payload#>>'{previousProducer,digest}';
  NEW.previous_generation := operation.payload#>>'{previousProducer,generation}';
  NEW.previous_sequence := (operation.payload#>>'{previousProducer,sequence}')::bigint;
  NEW.previous_release_id := operation.payload#>>'{precondition,previous_policy,release_id}';
  NEW.previous_fence := (operation.payload#>>'{precondition,previous_policy,fencing_token}')::bigint;
  NEW.target_artifact_id := operation.payload#>>'{nextProducer,id}';
  NEW.target_digest := operation.payload#>>'{nextProducer,digest}';
  NEW.target_generation := operation.payload#>>'{nextProducer,generation}';
  NEW.target_sequence := (operation.payload#>>'{nextProducer,sequence}')::bigint;
  NEW.approved_shadow_digest := operation.payload->>'shadowDigest';
  NEW.recorded_at := clock_timestamp();
  IF operation.envelope->>'expectedCurrentDigest' IS DISTINCT FROM NEW.previous_digest
     OR operation.envelope->>'rollbackDigest' IS DISTINCT FROM NEW.previous_digest
     OR operation.envelope->>'targetDigest' IS DISTINCT FROM NEW.target_digest
     OR (operation.envelope->>'expectedGeneration')::bigint IS DISTINCT FROM NEW.previous_fence
     OR operation.payload#>>'{precondition,previous_policy,artifact_id}' IS DISTINCT FROM NEW.previous_artifact_id
     OR operation.payload#>>'{precondition,previous_policy,content_hash}' IS DISTINCT FROM NEW.previous_digest THEN
    RAISE EXCEPTION 'global producer dispatch intent binding mismatch' USING ERRCODE = '40001';
  END IF;
  IF (NEW.claim_fence=1 AND NOT EXISTS(SELECT 1 FROM fugue_control.resource_baselines b WHERE b.cluster_id=NEW.cluster_id
        AND b.resource_id='producer/global-policy' AND b.generation=NEW.previous_fence AND b.verified_digest=NEW.previous_digest))
     OR (NEW.claim_fence>1 AND NOT EXISTS(SELECT 1 FROM fugue_control.execution_claims p
        JOIN fugue_control.execution_results r ON r.intent_digest=p.intent_digest
        WHERE p.cluster_id=NEW.cluster_id AND p.resource_id='producer/global-policy' AND p.fence=NEW.claim_fence-1
        AND r.generation=NEW.previous_fence AND r.verified_digest=NEW.previous_digest)) THEN
    RAISE EXCEPTION 'global producer dispatch no longer has its settled journal predecessor' USING ERRCODE = '40001';
  END IF;
  IF operation.payload ? 'reverses' THEN
    EXECUTE format('SELECT EXISTS(
      SELECT 1 FROM %I.fugue_native_global_producer_receipts p
      JOIN fugue_control.execution_claims c ON c.intent_digest=p.intent_digest AND c.cluster_id=p.cluster_id
        AND c.resource_id=''producer/global-policy'' AND c.approval_digest=p.approval_digest AND c.fence=p.claim_fence
      JOIN fugue_control.execution_results e ON e.intent_digest=c.intent_digest
      JOIN fugue_control.intents i ON i.digest=c.intent_digest
      CROSS JOIN LATERAL (SELECT convert_from(i.payload,''UTF8'')::jsonb AS body) AS original
      WHERE p.intent_digest=$1#>>''{reverses,intentDigest}'' AND p.approval_digest=$1#>>''{reverses,approvalDigest}''
        AND p.cluster_id=$2 AND p.claim_fence<$3 AND p.writer_role=session_user
        AND p.target_digest=$4 AND p.previous_fence=$5-1
        AND e.outcome=''applied'' AND e.verified_digest=$4 AND e.generation=$5
        AND e.evidence_digest=$1#>>''{reverses,evidenceDigest}''
        AND original.body->''nextProducer''=$1->''previousProducer''
        AND original.body->''nextDNS''=$1->''previousDNS''
        AND original.body->''static''=$1->''static''
        AND original.body->''origin''=$1->''origin''
        AND original.body->''changedEdge''=$1->''changedEdge''
        AND original.body->''beforeHold''=$1->''afterHold''
        AND original.body->''afterHold''=$1->''beforeHold''
        AND NOT EXISTS(SELECT 1 FROM fugue_control.execution_claims lc
          LEFT JOIN fugue_control.execution_results le ON le.intent_digest=lc.intent_digest
          WHERE lc.cluster_id=p.cluster_id AND lc.resource_id=''producer/global-policy''
            AND lc.fence>p.claim_fence AND lc.fence<$3
            AND (le.outcome IS DISTINCT FROM ''not_applied''
              OR EXISTS(SELECT 1 FROM %I.fugue_native_global_producer_receipts later WHERE later.intent_digest=lc.intent_digest))))', TG_TABLE_SCHEMA, TG_TABLE_SCHEMA)
    INTO inverse USING operation.payload, NEW.cluster_id, NEW.claim_fence, NEW.previous_digest, NEW.previous_fence;
    IF inverse IS DISTINCT FROM TRUE THEN
      RAISE EXCEPTION 'global producer rollback is not bound to the exact settled inverse' USING ERRCODE = '40001';
    END IF;
  END IF;
  EXECUTE format('SELECT EXISTS (
    SELECT 1 FROM %1$I.fugue_platform_release_lanes l
      JOIN %1$I.fugue_platform_artifact_releases r ON r.id=l.active_release_id
      JOIN %1$I.fugue_platform_artifacts a ON a.id=r.artifact_id
      JOIN %1$I.fugue_platform_artifacts t ON t.id=$7
    WHERE l.lane_key=''policy_snapshot|platform-config-producer|shadow'' AND l.artifact_kind=''policy_snapshot'' AND l.scope_key=''platform-config-producer'' AND l.release_channel=''shadow'' AND NOT l.frozen
      AND r.id=$1 AND l.fencing_token=$2 AND r.fencing_token=$2 AND r.status=''active'' AND r.artifact_id=$3 AND r.generation=$5
      AND r.lane_key=l.lane_key AND r.artifact_kind=l.artifact_kind AND r.scope_key=l.scope_key AND r.release_channel=l.release_channel
      AND a.artifact_kind=l.artifact_kind AND a.scope_key=l.scope_key AND a.status=''validated'' AND a.content_hash=$4 AND a.generation=$5 AND a.generation_sequence=$6
      AND t.artifact_kind=l.artifact_kind AND t.scope_key=l.scope_key AND t.status=''validated'' AND t.content_hash=$8 AND t.generation=$9 AND t.generation_sequence=$10)', TG_TABLE_SCHEMA)
    INTO selected USING NEW.previous_release_id,NEW.previous_fence,NEW.previous_artifact_id,NEW.previous_digest,NEW.previous_generation,NEW.previous_sequence,
      NEW.target_artifact_id,NEW.target_digest,NEW.target_generation,NEW.target_sequence;
  IF selected IS DISTINCT FROM TRUE THEN
    RAISE EXCEPTION 'global producer dispatch predecessor or target drifted' USING ERRCODE = '40001';
  END IF;
  RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION fugue_native_global_producer_reserve() FROM PUBLIC;

CREATE OR REPLACE FUNCTION fugue_native_global_producer_receipt_commit() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
  selected BOOLEAN;
BEGIN
  EXECUTE format('SELECT EXISTS (
    SELECT 1 FROM %1$I.fugue_platform_release_lanes l
      JOIN %1$I.fugue_platform_artifact_releases r ON r.id=l.active_release_id
      JOIN %1$I.fugue_platform_artifacts a ON a.id=r.artifact_id
      JOIN %1$I.fugue_platform_artifact_releases old ON old.id=$1
    WHERE l.lane_key=''policy_snapshot|platform-config-producer|shadow'' AND l.artifact_kind=''policy_snapshot'' AND l.scope_key=''platform-config-producer'' AND l.release_channel=''shadow'' AND NOT l.frozen
      AND l.fencing_token=$2 AND r.fencing_token=$2 AND r.status=''active'' AND r.artifact_id=$3 AND r.generation=$5 AND r.idempotency_key=$7
      AND r.lane_key=l.lane_key AND r.artifact_kind=l.artifact_kind AND r.scope_key=l.scope_key AND r.release_channel=l.release_channel
      AND a.artifact_kind=l.artifact_kind AND a.scope_key=l.scope_key AND a.status=''validated'' AND a.content_hash=$4 AND a.generation=$5 AND a.generation_sequence=$6
      AND old.status=''superseded'' AND old.artifact_id=$8 AND old.fencing_token=$2-1 AND old.lane_key=l.lane_key
      AND NOT EXISTS(SELECT 1 FROM %1$I.fugue_platform_artifact_releases other WHERE other.lane_key=l.lane_key AND other.status=''active'' AND other.id<>r.id))', TG_TABLE_SCHEMA)
    INTO selected USING NEW.previous_release_id,NEW.previous_fence+1,NEW.target_artifact_id,NEW.target_digest,NEW.target_generation,NEW.target_sequence,NEW.idempotency_key,NEW.previous_artifact_id;
  IF selected IS DISTINCT FROM TRUE THEN
    RAISE EXCEPTION 'global producer receipt did not commit its exact approved selection' USING ERRCODE = '40001';
  END IF;
  RETURN NULL;
END;
$$;
REVOKE ALL ON FUNCTION fugue_native_global_producer_receipt_commit() FROM PUBLIC;

CREATE OR REPLACE FUNCTION fugue_native_global_producer_selection_gate() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
  affected BOOLEAN := FALSE;
  closed BOOLEAN;
  permit record;
  bound BOOLEAN;
BEGIN
  IF TG_OP = 'TRUNCATE' THEN
    affected := TRUE;
  ELSE
    IF TG_OP <> 'INSERT' THEN
      affected := OLD.artifact_kind = 'policy_snapshot' AND OLD.scope_key = 'platform-config-producer';
    END IF;
    IF TG_OP <> 'DELETE' THEN
      affected := affected OR (NEW.artifact_kind = 'policy_snapshot' AND NEW.scope_key = 'platform-config-producer');
    END IF;
  END IF;
  IF affected THEN
    -- The row always exists. Every selection mutation takes a shared row
    -- lock until commit; the freeze UPDATE acquires its exclusive row lock.
    -- A stale repeatable-read writer fails serialization rather than
    -- proceeding with a stale mode. Reads do not take this lock.
    EXECUTE format('SELECT frozen FROM %I.fugue_native_global_producer_gate WHERE singleton FOR SHARE', TG_TABLE_SCHEMA) INTO closed;
    IF closed IS DISTINCT FROM FALSE THEN
      IF TG_OP IN ('TRUNCATE','DELETE') THEN
        RAISE EXCEPTION 'global producer policy selection is frozen' USING ERRCODE = '40001';
      END IF;
      EXECUTE format('SELECT * FROM %I.fugue_native_global_producer_receipts WHERE writer_xid=pg_current_xact_id() AND writer_role=session_user', TG_TABLE_SCHEMA) INTO permit;
      IF permit.intent_digest IS NULL THEN
        RAISE EXCEPTION 'global producer policy selection is frozen' USING ERRCODE = '40001';
      END IF;
      IF NEW.artifact_kind <> 'policy_snapshot' OR NEW.scope_key <> 'platform-config-producer' OR NEW.release_channel <> 'shadow'
         OR NEW.lane_key <> 'policy_snapshot|platform-config-producer|shadow' THEN
        RAISE EXCEPTION 'global producer permit cannot select another lane' USING ERRCODE = '40001';
      END IF;
      IF TG_TABLE_NAME = 'fugue_platform_release_lanes' THEN
        IF NEW.frozen THEN
          RAISE EXCEPTION 'global producer permit cannot freeze serving policy' USING ERRCODE = '40001';
        END IF;
        -- INSERT is the speculative part of the existing lane upsert. The
        -- receipt requires that this exact lane already exists.
        IF TG_OP='UPDATE' THEN
          IF OLD.lane_key <> NEW.lane_key OR OLD.scope_key <> NEW.scope_key OR OLD.artifact_kind <> NEW.artifact_kind OR OLD.release_channel <> NEW.release_channel
             OR OLD.frozen OR OLD.active_release_id<>permit.previous_release_id OR NEW.fencing_token<>permit.previous_fence+1
             OR (to_jsonb(OLD)-'fencing_token'-'version'-'active_release_id'-'updated_at') IS DISTINCT FROM (to_jsonb(NEW)-'fencing_token'-'version'-'active_release_id'-'updated_at') THEN
            RAISE EXCEPTION 'global producer permit cannot change the publication fence' USING ERRCODE = '40001';
          END IF;
          IF OLD.fencing_token=permit.previous_fence THEN
            IF NEW.active_release_id<>OLD.active_release_id OR NEW.version<>OLD.version+1 THEN
              RAISE EXCEPTION 'global producer must advance the lane exactly once' USING ERRCODE = '40001';
            END IF;
          ELSIF OLD.fencing_token=permit.previous_fence+1 AND NEW.version=OLD.version THEN
            EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.fugue_platform_artifact_releases WHERE id=$1 AND artifact_id=$2 AND generation=$3 AND fencing_token=$4 AND lane_key=$5 AND status=''active'' AND idempotency_key=$6)', TG_TABLE_SCHEMA)
              INTO bound USING NEW.active_release_id,permit.target_artifact_id,permit.target_generation,NEW.fencing_token,NEW.lane_key,permit.idempotency_key;
            IF bound IS DISTINCT FROM TRUE THEN
              RAISE EXCEPTION 'global producer lane must select the exact new release' USING ERRCODE = '40001';
            END IF;
          ELSE
            RAISE EXCEPTION 'global producer permit cannot advance the lane again' USING ERRCODE = '40001';
          END IF;
        END IF;
      ELSIF TG_OP='INSERT' THEN
        IF NEW.artifact_id<>permit.target_artifact_id OR NEW.generation<>permit.target_generation OR NEW.fencing_token<>permit.previous_fence+1
           OR NEW.status<>'active' OR NEW.idempotency_key<>permit.idempotency_key THEN
          RAISE EXCEPTION 'global producer permit cannot select an unapproved artifact' USING ERRCODE = '40001';
        END IF;
      ELSE
        IF OLD.id<>permit.previous_release_id OR OLD.status<>'active' OR NEW.status<>'superseded'
           OR (to_jsonb(OLD)-'status'-'updated_at') IS DISTINCT FROM (to_jsonb(NEW)-'status'-'updated_at') THEN
          RAISE EXCEPTION 'global producer permit can only supersede its exact predecessor' USING ERRCODE = '40001';
        END IF;
      END IF;
    END IF;
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  IF TG_OP = 'TRUNCATE' THEN RETURN NULL; END IF;
  RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION fugue_native_global_producer_selection_gate() FROM PUBLIC;

CREATE OR REPLACE FUNCTION fugue_native_global_producer_freeze_once() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
  selected BOOLEAN;
BEGIN
  IF TG_OP <> 'UPDATE' THEN
    RAISE EXCEPTION 'global producer gate history cannot be deleted' USING ERRCODE = '40001';
  END IF;
  IF OLD.frozen OR NOT NEW.frozen OR NOT NEW.singleton THEN
    RAISE EXCEPTION 'global producer gate cannot return to legacy authority' USING ERRCODE = '40001';
  END IF;
  -- This transition must use READ COMMITTED: after waiting for pre-freeze
  -- writers we must verify the live predecessor, not a historical snapshot.
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'global producer freeze requires a current provider observation' USING ERRCODE = '40001';
  END IF;
  EXECUTE format('SELECT EXISTS (
    SELECT 1 FROM %1$I.fugue_platform_release_lanes l
      JOIN %1$I.fugue_platform_artifact_releases r ON r.id = l.active_release_id
      JOIN %1$I.fugue_platform_artifacts a ON a.id = r.artifact_id
    WHERE l.artifact_kind = ''policy_snapshot'' AND l.scope_key = ''platform-config-producer''
      AND l.release_channel = ''shadow'' AND NOT l.frozen
      AND l.active_release_id = $1 AND l.fencing_token = $2
      AND r.artifact_kind = l.artifact_kind AND r.scope_key = l.scope_key
      AND r.release_channel = l.release_channel AND r.lane_key = l.lane_key
      AND r.status = ''active'' AND r.fencing_token = l.fencing_token
      AND r.generation = a.generation AND a.artifact_kind = l.artifact_kind
      AND a.scope_key = l.scope_key AND a.status = ''validated'' AND a.content_hash = $3)', TG_TABLE_SCHEMA)
    INTO selected USING NEW.baseline_release_id, NEW.baseline_fence, NEW.baseline_digest;
  IF selected IS DISTINCT FROM TRUE THEN
    RAISE EXCEPTION 'global producer freeze predecessor changed' USING ERRCODE = '40001';
  END IF;
  NEW.frozen_at := clock_timestamp();
  RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION fugue_native_global_producer_freeze_once() FROM PUBLIC;

DROP TRIGGER IF EXISTS fugue_native_global_producer_enable ON fugue_native_global_producer_authority;
CREATE TRIGGER fugue_native_global_producer_enable BEFORE INSERT ON fugue_native_global_producer_authority
FOR EACH ROW EXECUTE FUNCTION fugue_native_global_producer_enable();
DROP TRIGGER IF EXISTS fugue_native_global_producer_authority_immutable ON fugue_native_global_producer_authority;
CREATE TRIGGER fugue_native_global_producer_authority_immutable BEFORE UPDATE OR DELETE ON fugue_native_global_producer_authority
FOR EACH ROW EXECUTE FUNCTION fugue_native_global_producer_append_only();
DROP TRIGGER IF EXISTS fugue_native_global_producer_authority_no_truncate ON fugue_native_global_producer_authority;
CREATE TRIGGER fugue_native_global_producer_authority_no_truncate BEFORE TRUNCATE ON fugue_native_global_producer_authority
FOR EACH STATEMENT EXECUTE FUNCTION fugue_native_global_producer_append_only();

DROP TRIGGER IF EXISTS fugue_native_global_producer_reserve ON fugue_native_global_producer_receipts;
CREATE TRIGGER fugue_native_global_producer_reserve BEFORE INSERT ON fugue_native_global_producer_receipts
FOR EACH ROW EXECUTE FUNCTION fugue_native_global_producer_reserve();
DROP TRIGGER IF EXISTS fugue_native_global_producer_receipt_immutable ON fugue_native_global_producer_receipts;
CREATE TRIGGER fugue_native_global_producer_receipt_immutable BEFORE UPDATE OR DELETE ON fugue_native_global_producer_receipts
FOR EACH ROW EXECUTE FUNCTION fugue_native_global_producer_append_only();
DROP TRIGGER IF EXISTS fugue_native_global_producer_receipt_no_truncate ON fugue_native_global_producer_receipts;
CREATE TRIGGER fugue_native_global_producer_receipt_no_truncate BEFORE TRUNCATE ON fugue_native_global_producer_receipts
FOR EACH STATEMENT EXECUTE FUNCTION fugue_native_global_producer_append_only();
DROP TRIGGER IF EXISTS fugue_native_global_producer_receipt_commit ON fugue_native_global_producer_receipts;
CREATE CONSTRAINT TRIGGER fugue_native_global_producer_receipt_commit AFTER INSERT ON fugue_native_global_producer_receipts
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fugue_native_global_producer_receipt_commit();

DROP TRIGGER IF EXISTS fugue_native_global_producer_freeze_once ON fugue_native_global_producer_gate;
CREATE TRIGGER fugue_native_global_producer_freeze_once BEFORE UPDATE OR DELETE ON fugue_native_global_producer_gate
FOR EACH ROW EXECUTE FUNCTION fugue_native_global_producer_freeze_once();
DROP TRIGGER IF EXISTS fugue_native_global_producer_gate_no_truncate ON fugue_native_global_producer_gate;
CREATE TRIGGER fugue_native_global_producer_gate_no_truncate BEFORE TRUNCATE ON fugue_native_global_producer_gate
FOR EACH STATEMENT EXECUTE FUNCTION fugue_native_global_producer_freeze_once();

DROP TRIGGER IF EXISTS fugue_native_global_producer_selection_gate ON fugue_platform_release_lanes;
CREATE TRIGGER fugue_native_global_producer_selection_gate BEFORE INSERT OR UPDATE OR DELETE ON fugue_platform_release_lanes
FOR EACH ROW EXECUTE FUNCTION fugue_native_global_producer_selection_gate();
DROP TRIGGER IF EXISTS fugue_native_global_producer_selection_no_truncate ON fugue_platform_release_lanes;
CREATE TRIGGER fugue_native_global_producer_selection_no_truncate BEFORE TRUNCATE ON fugue_platform_release_lanes
FOR EACH STATEMENT EXECUTE FUNCTION fugue_native_global_producer_selection_gate();
DROP TRIGGER IF EXISTS fugue_native_global_producer_selection_gate ON fugue_platform_artifact_releases;
CREATE TRIGGER fugue_native_global_producer_selection_gate BEFORE INSERT OR UPDATE OR DELETE ON fugue_platform_artifact_releases
FOR EACH ROW EXECUTE FUNCTION fugue_native_global_producer_selection_gate();
DROP TRIGGER IF EXISTS fugue_native_global_producer_selection_no_truncate ON fugue_platform_artifact_releases;
CREATE TRIGGER fugue_native_global_producer_selection_no_truncate BEFORE TRUNCATE ON fugue_platform_artifact_releases
FOR EACH STATEMENT EXECUTE FUNCTION fugue_native_global_producer_selection_gate();
`
