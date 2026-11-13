package schemamigrate

// Historical admission state remains compatible until runtime admission is
// independently replaced. Existing enforcement is durable and cannot be
// disabled by a restart, environment flag, schema rerun, DELETE or TRUNCATE.
// The migration-only enabling implementation has been removed. This DDL grants
// neither archive routing nor write fencing nor physical retirement.
const CompilerInputHistoryAdmissionSQL = `
CREATE TABLE IF NOT EXISTS fugue_platform_input_history_admission (
 singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
 phase TEXT NOT NULL CHECK (phase IN ('capability','enforced')),
 plan_sha256 TEXT NOT NULL CHECK (plan_sha256='' OR plan_sha256 ~ '^[0-9a-f]{64}$'),
 enforced_at TIMESTAMPTZ NULL,
 CHECK ((phase='capability' AND plan_sha256='' AND enforced_at IS NULL)
     OR (phase='enforced' AND plan_sha256<>'' AND enforced_at IS NOT NULL))
);
CREATE OR REPLACE FUNCTION fugue_platform_input_history_admission_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='TRUNCATE' OR TG_OP='DELETE' THEN
  RAISE EXCEPTION 'compiler input admission state cannot be removed' USING ERRCODE='55000';
 END IF;
 IF OLD.phase='enforced' AND NEW IS DISTINCT FROM OLD THEN
  RAISE EXCEPTION 'compiler input admission enforcement is immutable' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS fugue_input_history_admission_row_guard ON fugue_platform_input_history_admission;
CREATE TRIGGER fugue_input_history_admission_row_guard BEFORE UPDATE OR DELETE ON fugue_platform_input_history_admission
 FOR EACH ROW EXECUTE FUNCTION fugue_platform_input_history_admission_guard();
DROP TRIGGER IF EXISTS fugue_input_history_admission_truncate_guard ON fugue_platform_input_history_admission;
CREATE TRIGGER fugue_input_history_admission_truncate_guard BEFORE TRUNCATE ON fugue_platform_input_history_admission
 FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_history_admission_guard();
INSERT INTO fugue_platform_input_history_admission(singleton,phase,plan_sha256)
 VALUES(TRUE,'capability','') ON CONFLICT(singleton) DO NOTHING;
`
