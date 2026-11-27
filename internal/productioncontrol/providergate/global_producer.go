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
// This is only the freeze phase. A coordinator must verify the owner-approved
// cutover and observe drainage before invoking a transition. No code calls
// that transition in production until the native writer is implemented.
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

CREATE OR REPLACE FUNCTION fugue_native_global_producer_selection_gate() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
  affected BOOLEAN := FALSE;
  closed BOOLEAN;
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
      RAISE EXCEPTION 'global producer policy selection is frozen' USING ERRCODE = '40001';
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
