package productioncontrol

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"

	"fugue/internal/schemamigrate"
	"fugue/internal/testpostgres"
	"github.com/jackc/pgx/v5"
)

func TestPostgresSampleRetentionObserver(t *testing.T) {
	address := testpostgres.Database(t, "")
	db, err := sql.Open("pgx", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := t.Context()
	exec := func(command string, args ...any) {
		t.Helper()
		if _, err := db.ExecContext(ctx, command, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`REVOKE CREATE ON SCHEMA public FROM PUBLIC;
		CREATE SCHEMA fugue_observation_maintenance;
		REVOKE ALL ON SCHEMA fugue_observation_maintenance FROM PUBLIC;
		CREATE TABLE public.fugue_meta (key text PRIMARY KEY, value text, updated_at timestamptz);
		CREATE TABLE public.fugue_edge_network_samples (observed_at timestamptz);
		CREATE TABLE public.fugue_edge_performance_samples (sampled_at timestamptz);
		CREATE TABLE public.fugue_resource_usage_samples (observed_at timestamptz, tenant_id text);
		CREATE TABLE public.fugue_tenants (id text PRIMARY KEY);`)
	exec(schemamigrate.ObservationSampleRetentionFenceSQL)
	exec(schemamigrate.ObservationSampleRetentionSQL)
	role := fmt.Sprintf("fugue_sample_observer_%d", os.Getpid())
	quoted := pgx.Identifier{role}.Sanitize()
	exec(`CREATE ROLE ` + quoted + ` LOGIN`)
	t.Cleanup(func() {
		if _, err := db.Exec(`DROP OWNED BY ` + quoted + `; DROP ROLE ` + quoted); err != nil {
			t.Error(err)
		}
	})
	exec(`
		GRANT USAGE ON SCHEMA fugue_observation_maintenance TO ` + quoted + `;
		GRANT SELECT ON fugue_observation_maintenance.sample_worker_progress TO ` + quoted + `;
		GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO ` + quoted)
	parsed, err := url.Parse(address)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.User(role)
	reader, err := sql.Open("pgx", parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	p := SampleRetentionProvider{Role: role}
	if err := db.QueryRowContext(ctx, `SELECT system_identifier::text,current_database() FROM pg_control_system()`).Scan(&p.SystemID, &p.Database); err != nil {
		t.Fatal(err)
	}
	check := func(want string) SampleRetentionObservation {
		t.Helper()
		got, err := p.Observe(ctx, reader)
		if err != nil || got.State != want || got.Resource != ObservationSampleRetentionResource || len(got.Targets) != 3 {
			t.Fatalf("expected %s, got %+v (%v)", want, got, err)
		}
		return got
	}
	if _, err := p.Observe(ctx, db); err == nil {
		t.Fatal("accepted administrator as independent observer")
	}
	wrong := p
	wrong.SystemID = "1"
	if _, err := wrong.Observe(ctx, reader); err == nil {
		t.Fatal("accepted a different PostgreSQL system")
	}
	if _, err := reader.ExecContext(ctx, `SELECT value FROM public.fugue_meta`); err == nil {
		t.Fatal("observer can read the full application metadata table")
	}
	check(SampleRetentionAPIStandby)
	exec(schemamigrate.ObservationSampleRetentionActivateSQL)
	check(SampleRetentionWorkerPending)
	for _, target := range sampleRetentionTargets {
		exec(`SELECT * FROM fugue_observation_maintenance.prune_samples_v1($1,false,7,10000,1)`, target)
	}
	got := check(SampleRetentionWorkerObserved)
	for _, target := range got.Targets {
		if target.CompletedPasses != 1 || target.TotalDeleted != 0 || !target.DeleteFenced {
			t.Fatalf("unexpected synthetic worker progress: %+v", target)
		}
	}
	exec(`GRANT EXECUTE ON FUNCTION fugue_observation_maintenance.prune_samples_v1(text, boolean, int, int, int) TO ` + quoted)
	if _, err := p.Observe(ctx, reader); err == nil {
		t.Fatal("accepted observer with retention writer rights")
	}
	exec(`REVOKE EXECUTE ON FUNCTION fugue_observation_maintenance.prune_samples_v1(text, boolean, int, int, int) FROM ` + quoted)
	exec(`ALTER TABLE public.fugue_edge_performance_samples DISABLE TRIGGER fugue_sample_retention_delete_fence_v1`)
	if _, err := p.Observe(ctx, reader); err == nil {
		t.Fatal("accepted a missing delete fence on one of the three targets")
	}
}
