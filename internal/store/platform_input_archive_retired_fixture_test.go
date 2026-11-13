package store

import (
	"context"
	"database/sql"
	"testing"
)

// Historical schema fixture only. Production has no relation-retirement executor.
func installCompilerInputArchiveRetirementGuards(ctx context.Context, tx *sql.Tx) error {
	const ddl = `
CREATE OR REPLACE FUNCTION fugue_platform_input_archive_retirement_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS (SELECT 1 FROM fugue_platform_input_archive_control WHERE singleton AND retirement_started_at IS NOT NULL) THEN
  RAISE EXCEPTION 'compiler input archive is retired' USING ERRCODE='55000';
 END IF;
 IF TG_LEVEL='STATEMENT' THEN RETURN NULL; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER fugue_platform_input_archive_control_retired_row_guard BEFORE UPDATE OR DELETE ON fugue_platform_input_archive_control FOR EACH ROW EXECUTE FUNCTION fugue_platform_input_archive_retirement_guard();
CREATE TRIGGER fugue_platform_input_archive_control_retired_truncate_guard BEFORE TRUNCATE ON fugue_platform_input_archive_control FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_archive_retirement_guard();
CREATE TRIGGER fugue_platform_input_archives_retired_row_guard BEFORE INSERT OR UPDATE OR DELETE ON fugue_platform_input_archives FOR EACH ROW EXECUTE FUNCTION fugue_platform_input_archive_retirement_guard();
CREATE TRIGGER fugue_platform_input_archives_retired_truncate_guard BEFORE TRUNCATE ON fugue_platform_input_archives FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_archive_retirement_guard();
CREATE TRIGGER fugue_platform_input_archive_segments_retired_row_guard BEFORE INSERT OR UPDATE OR DELETE ON fugue_platform_input_archive_segments FOR EACH ROW EXECUTE FUNCTION fugue_platform_input_archive_retirement_guard();
CREATE TRIGGER fugue_platform_input_archive_segments_retired_truncate_guard BEFORE TRUNCATE ON fugue_platform_input_archive_segments FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_archive_retirement_guard();
CREATE TRIGGER fugue_platform_input_archive_leases_retired_row_guard BEFORE INSERT OR UPDATE OR DELETE ON fugue_platform_input_archive_leases FOR EACH ROW EXECUTE FUNCTION fugue_platform_input_archive_retirement_guard();
CREATE TRIGGER fugue_platform_input_archive_leases_retired_truncate_guard BEFORE TRUNCATE ON fugue_platform_input_archive_leases FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_archive_retirement_guard();`
	if _, err := tx.ExecContext(ctx, ddl); err != nil {
		return err
	}
	installed, err := compilerInputArchiveRetirementGuardsInstalled(ctx, tx)
	if err != nil {
		return err
	}
	if !installed {
		return ErrConflict
	}
	return nil
}

func markCompilerInputTestRelationRetired(t *testing.T, s *Store) {
	t.Helper()
	tx, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := installCompilerInputArchiveRetirementGuards(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `UPDATE fugue_platform_input_archive_control SET epoch=epoch+1,retirement_plan_sha256=repeat('a',64),retirement_reader_current=repeat('b',40),retirement_reader_rollback=repeat('c',40),retirement_archive_policy_sha256=repeat('d',64),retirement_started_at=now(),updated_at=now() WHERE singleton AND writes_fenced=true AND retirement_started_at IS NULL`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `TRUNCATE TABLE fugue_platform_input_chunks RESTRICT`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
