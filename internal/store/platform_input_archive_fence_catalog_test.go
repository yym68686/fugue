package store

import (
	"errors"
	"strings"
	"testing"
)

func TestCompilerInputWriteFenceRejectsAlteredCatalog(t *testing.T) {
	for name, mutate := range map[string]string{
		"disabled":          `ALTER TABLE fugue_platform_input_chunks DISABLE TRIGGER fugue_platform_input_chunks_archive_write_fence`,
		"replica-only":      `ALTER TABLE fugue_platform_input_chunks ENABLE REPLICA TRIGGER fugue_platform_input_chunks_archive_write_fence`,
		"function-replaced": `CREATE OR REPLACE FUNCTION fugue_platform_input_chunks_archive_write_fence() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$`,
		"function-setting":  `ALTER FUNCTION fugue_platform_input_chunks_archive_write_fence() SET search_path=public`,
		"wrong-events": `DROP TRIGGER fugue_platform_input_chunks_archive_write_fence ON fugue_platform_input_chunks;
 CREATE TRIGGER fugue_platform_input_chunks_archive_write_fence BEFORE INSERT ON fugue_platform_input_chunks FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_chunks_archive_write_fence();
 ALTER TABLE fugue_platform_input_chunks ENABLE ALWAYS TRIGGER fugue_platform_input_chunks_archive_write_fence`,
		"conditional-row": `DROP TRIGGER fugue_platform_input_chunks_archive_write_fence ON fugue_platform_input_chunks;
 CREATE TRIGGER fugue_platform_input_chunks_archive_write_fence BEFORE INSERT OR UPDATE OR DELETE ON fugue_platform_input_chunks FOR EACH ROW WHEN (false) EXECUTE FUNCTION fugue_platform_input_chunks_archive_write_fence();
 ALTER TABLE fugue_platform_input_chunks ENABLE ALWAYS TRIGGER fugue_platform_input_chunks_archive_write_fence`,
		"argument-added": `DROP TRIGGER fugue_platform_input_chunks_archive_write_fence ON fugue_platform_input_chunks;
 CREATE TRIGGER fugue_platform_input_chunks_archive_write_fence BEFORE INSERT OR UPDATE OR DELETE ON fugue_platform_input_chunks FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_chunks_archive_write_fence('unused');
 ALTER TABLE fugue_platform_input_chunks ENABLE ALWAYS TRIGGER fugue_platform_input_chunks_archive_write_fence`,
	} {
		t.Run(name, func(t *testing.T) {
			s := archiveTestStore(t)
			resetArchiveFenceFixture(t, s)
			if _, err := RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, false); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.ExecContext(t.Context(), mutate); err != nil {
				t.Fatal(err)
			}
			state, err := compilerInputArchiveWriteFenceState(t.Context(), s.db)
			if err != nil || !state.Present || state.installed() {
				t.Fatal("altered trigger reported as installed", state, err)
			}
			var nameOnly bool
			if err := s.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='fugue_platform_input_chunks'::regclass AND tgname='fugue_platform_input_chunks_archive_write_fence' AND NOT tgisinternal)`).Scan(&nameOnly); err != nil || !nameOnly {
				t.Fatal("fixture did not exercise the former false-positive check", err)
			}
			for _, disarm := range []bool{false, true} {
				report, err := RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, disarm)
				if !errors.Is(err, ErrConflict) || report.Applied || report.TriggerInstalled {
					t.Fatal("altered fence was silently accepted or replaced", disarm, report, err)
				}
			}
			plan, err := compilerInputArchiveRetirementPlanMetadata(t.Context(), s.db, "sha256:"+strings.Repeat("a", 64), true)
			if err != nil || plan.WriteTriggerInstalled || plan.CanRetire {
				t.Fatal("storage plan accepted altered writer isolation", plan.WriteTriggerInstalled, plan.CanRetire, err)
			}
			var fenced bool
			if err := s.db.QueryRow(`SELECT writes_fenced FROM fugue_platform_input_archive_control WHERE singleton`).Scan(&fenced); err != nil || !fenced {
				t.Fatal("refusal changed the durable fence", err)
			}
		})
	}
}

func TestCompilerInputWriteFenceExplicitlyUpgradesOriginTriggerAndRejectsReplicaWrites(t *testing.T) {
	s := archiveTestStore(t)
	resetArchiveFenceFixture(t, s)
	if _, err := RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`ALTER TABLE fugue_platform_input_chunks ENABLE TRIGGER fugue_platform_input_chunks_archive_write_fence`); err != nil {
		t.Fatal(err)
	}
	state, err := compilerInputArchiveWriteFenceState(t.Context(), s.db)
	if err != nil || !state.Compatible || state.installed() {
		t.Fatal("origin-only fence was not distinguished", state, err)
	}
	upgraded, err := RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, false)
	if err != nil || !upgraded.Applied || !upgraded.WritesFenced || !upgraded.TriggerInstalled {
		t.Fatal("explicit arm did not upgrade the exact original trigger", upgraded, err)
	}
	again, err := RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, false)
	if err != nil || again.Applied || !again.TriggerInstalled {
		t.Fatal("validated arm was not idempotent", again, err)
	}
	// A fresh connection models an old/reconnecting SQL writer. Even replica
	// sessions must reject each DML statement, including a zero-row mutation.
	conn, err := s.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.ExecContext(t.Context(), `SET session_replication_role=replica`); err != nil {
		t.Fatal("disposable fixture requires permission to exercise replica mode", err)
	}
	defer func() {
		if _, err := conn.ExecContext(t.Context(), `RESET session_replication_role`); err != nil {
			t.Error(err)
		}
	}()
	for _, statement := range []string{
		`UPDATE fugue_platform_input_chunks SET data=data WHERE false`,
		`DELETE FROM fugue_platform_input_chunks WHERE false`,
		`INSERT INTO fugue_platform_input_chunks SELECT * FROM fugue_platform_input_chunks WHERE false`,
	} {
		_, err := conn.ExecContext(t.Context(), statement)
		var pgerr interface{ SQLState() string }
		if !errors.As(err, &pgerr) || pgerr.SQLState() != "55000" {
			t.Fatal("replica writer bypassed the unconditional fence", statement, err)
		}
	}
}
