package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"fugue/internal/testpostgres"
)

func TestFirstDeployExecutionStartsPostgres(t *testing.T) {
	db, err := sql.Open("pgx", testpostgres.Database(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	st := &Store{db: db, dbReady: true}
	_, err = db.Exec(`
CREATE TEMPORARY TABLE fugue_release_steps (
  operation_id text NOT NULL, step_type text NOT NULL,
  status text NOT NULL, started_at timestamptz NOT NULL
);
INSERT INTO fugue_release_steps VALUES
  ('visible-retried', 'deploy_apply', 'running', '2026-11-19T10:41:00Z'),
  ('visible-retried', 'deploy_apply', 'running', '2026-11-19T10:10:05Z'),
  ('visible-retried', 'deploy_apply', 'pending', '2026-11-19T10:10:00Z'),
  ('visible-retried', 'image_import', 'running', '2026-11-19T09:10:00Z'),
  ('visible-next', 'deploy_apply', 'running', '2026-11-19T11:10:00Z'),
  ('hidden', 'deploy_apply', 'running', '2026-11-19T09:00:00Z');`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := st.FirstDeployExecutionStarts(context.Background(), []string{"visible-retried", "visible-next", "visible-no-evidence"})
	if err != nil || len(got) != 2 ||
		!got["visible-retried"].Equal(time.Date(2026, 11, 19, 10, 10, 5, 0, time.UTC)) ||
		!got["visible-next"].Equal(time.Date(2026, 11, 19, 11, 10, 0, 0, time.UTC)) {
		t.Fatalf("unexpected batch result: %v (%v)", got, err)
	}
}
