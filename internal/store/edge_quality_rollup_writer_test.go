package store

import (
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
)

func TestEdgeQualityRollupWorkerRequiresSeparateDatabaseRole(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL for disposable PostgreSQL")
	}
	if !strings.Contains(dsn, "fugue-pgtest") && !strings.Contains(dsn, "fugue_test") {
		t.Fatal("refusing a non-disposable PostgreSQL target")
	}
	ctx := t.Context()
	schema := model.NewID("worker_schema")
	role := model.NewID("rollup_writer")
	quotedSchema := quotePostgresIntegrationIdentifier(t, schema)
	quotedRole := quotePostgresIntegrationIdentifier(t, role)
	schemaDSN := postgresIntegrationURLWithSearchPath(t, dsn, schema)
	admin, err := sql.Open("pgx", schemaDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP SCHEMA " + quotedSchema + " CASCADE") })
	for _, table := range []string{"fugue_meta", "fugue_edge_performance_samples", "fugue_edge_quality_rollups", "fugue_edge_quality_rollup_watermarks"} {
		execPostgresSchemaStatementContaining(t, ctx, admin, "CREATE TABLE IF NOT EXISTS "+table+" (")
	}
	if err := schemamigrate.EnsureEdgeQualityRollupOwnerProjection(ctx, admin); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, "CREATE ROLE "+quotedRole+" LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP OWNED BY " + quotedRole)
		_, _ = admin.Exec("DROP ROLE " + quotedRole)
	})
	for _, statement := range []string{
		"GRANT USAGE ON SCHEMA " + quotedSchema + " TO " + quotedRole,
		"GRANT SELECT ON fugue_edge_quality_rollup_owner, fugue_edge_performance_samples TO " + quotedRole,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON fugue_edge_quality_rollups TO " + quotedRole,
		"GRANT SELECT, INSERT, UPDATE ON fugue_edge_quality_rollup_watermarks TO " + quotedRole,
	} {
		if _, err := admin.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, closeAdmin, err := OpenEdgeQualityRollupWorkerStore(ctx, schemaDSN); err == nil {
		_ = closeAdmin()
		t.Fatal("the owner credential passed worker readiness")
	}
	workerDSN, err := url.Parse(schemaDSN)
	if err != nil {
		t.Fatal(err)
	}
	workerDSN.User = url.User(role)
	worker, closeWorker, err := OpenEdgeQualityRollupWorkerStore(ctx, workerDSN.String())
	if err != nil {
		t.Fatal(err)
	}
	defer closeWorker()
	if progress, err := worker.ReadEdgeQualityRollupWorkerProgress(ctx); err != nil || progress.Owner != EdgeQualityRollupAPIWriter || len(progress.Watermarks) != 0 {
		t.Fatalf("unexpected standby progress: %+v %v", progress, err)
	}
	if _, err := worker.TransferEdgeQualityRollupsToWorker(ctx); err == nil {
		t.Fatal("the worker credential could transfer authority")
	}
	if _, err := worker.ReadObservationCheckpoint(ctx, EdgeQualityRollupWriterCheckpoint); err == nil {
		t.Fatal("the worker can read shared metadata directly")
	}
	checkpoint := &Store{db: admin}
	if ok, err := checkpoint.TransferEdgeQualityRollupsToWorker(ctx); err != nil || !ok {
		t.Fatalf("transfer owner: activated=%t err=%v", ok, err)
	}
	pass, err := worker.BeginEdgeQualityRollupBuild(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := pass.AcquireWriter(EdgeQualityRollupWorkerWriter); err != nil || !ok {
		pass.Rollback()
		t.Fatalf("worker could not read its owner through the projection: acquired=%t err=%v", ok, err)
	}
	end := time.Date(2026, 11, 27, 12, 45, 0, 0, time.UTC)
	if err := pass.Commit(nil, nil, map[string]time.Time{"5m": end}); err != nil {
		t.Fatal(err)
	}
	restarted, closeRestarted, err := OpenEdgeQualityRollupWorkerStore(ctx, workerDSN.String())
	if err != nil {
		t.Fatal(err)
	}
	defer closeRestarted()
	if progress, err := restarted.ReadEdgeQualityRollupWorkerProgress(ctx); err != nil || progress.Owner != EdgeQualityRollupWorkerWriter || !progress.Watermarks["5m"].Equal(end) {
		t.Fatalf("restarted worker did not read persisted progress: %+v %v", progress, err)
	}
	if _, err := admin.ExecContext(ctx, "GRANT UPDATE (value) ON fugue_meta TO "+quotedRole); err != nil {
		t.Fatal(err)
	}
	if err := worker.CheckEdgeQualityRollupWorker(ctx); err == nil {
		t.Fatal("worker readiness accepted a column-level owner write grant")
	}
}
