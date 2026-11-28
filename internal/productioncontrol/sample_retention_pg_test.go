package productioncontrol

import (
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/schemamigrate"
	"fugue/internal/testpostgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
	// Read the current projection before updating the provider view. Once
	// upgraded, baseline import additionally binds the stable transfer time.
	legacyView := strings.Replace(schemamigrate.ObservationSampleRetentionSQL,
		",\n    (SELECT updated_at FROM public.fugue_meta WHERE key = 'observation/v1/' ||\n        encode(sha256(convert_to('observation-sample-retention-writer/v1', 'UTF8')), 'hex')) AS transfer_at",
		"", 1)
	if legacyView == schemamigrate.ObservationSampleRetentionSQL {
		t.Fatal("test did not construct the previous view")
	}
	exec(legacyView)
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
	exec(schemamigrate.ObservationSampleRetentionSQL)
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
	if err := ApplySchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := ApplySchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	ownerIntent, payload, ownerKey, owners, _ := approvalFixture(t)
	j, err := NewJournal(db, owners)
	if err != nil {
		t.Fatal(err)
	}
	pub, observer, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Second).Add(time.Second)
	j.now = func() time.Time { return now }
	scope := Scope{ownerIntent.ClusterID, ObservationSampleRetentionResource}
	authority := digest([]byte("fixture revoked identity evidence"))
	draft, err := DraftSampleRetentionBaseline(scope, got, authority, "observer-a", now)
	if err != nil {
		t.Fatal(err)
	}
	draft.ExpiresAt = draft.ObservedAt + 30
	source := memoryWitnesses{}
	baseline := Baseline{scope, draft.version(), fixtureWitness(t, observer, draft, source)}
	record := func(revocation string, key ed25519.PublicKey) (bool, error) {
		return j.ImportSampleRetentionBaseline(ctx, baseline, p, reader, revocation, source, draft.KeyID, key)
	}
	if created, err := record(digest([]byte("wrong drain proof")), pub); err == nil || created {
		t.Fatal("import accepted different legacy revocation evidence")
	}
	if created, err := record(authority, owners["owner-a"].PublicKey); err == nil || created {
		t.Fatal("owner approval key authorized its own technical observation")
	}
	now = time.Unix(draft.ExpiresAt, 0)
	if created, err := record(authority, pub); err == nil || created {
		t.Fatal("import accepted expired witness")
	}
	now = time.Unix(draft.ObservedAt, 0)
	const performanceFence = `ALTER TABLE public.fugue_edge_performance_samples %s TRIGGER fugue_sample_retention_delete_fence_v1`
	exec(fmt.Sprintf(performanceFence, "DISABLE"))
	if created, err := record(authority, pub); err == nil || created {
		t.Fatal("import accepted a provider with a missing live fence")
	}
	exec(fmt.Sprintf(performanceFence, "ENABLE ALWAYS"))
	exec(`SELECT * FROM fugue_observation_maintenance.prune_samples_v1('network',false,7,10000,1)`)
	now = now.Add(2 * time.Second)
	if created, err := record(authority, pub); err != nil || !created {
		t.Fatalf("could not import live checkpoint after progress advanced: %v / %v", created, err)
	}
	if created, err := record(authority, pub); err != nil || created {
		t.Fatalf("import retry changed durable checkpoint: %v / %v", created, err)
	}
	state, err := j.ReadResource(ctx, scope)
	if err != nil || state.Baseline != baseline || state.Pending != "" {
		t.Fatalf("unexpected journal baseline: %+v: %v", state, err)
	}
	ownerIntent.ResourceID, ownerIntent.Operation = scope.ResourceID, Maintenance
	ownerIntent.Generation, ownerIntent.CurrentDigest, ownerIntent.RollbackDigest = baseline.Verified.Generation,
		baseline.Verified.Digest, baseline.Verified.Digest
	intent, err := j.RecordIntent(ctx, "operator-a", ownerIntent, payload)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := SignApproval(ownerIntent, "owner-a", ownerKey, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	approval, err := j.RecordApproval(ctx, signed)
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := j.ReserveExecution(ctx, "worker-a", intent.Digest, approval.Digest); err == nil || created {
		t.Fatal("observation-only baseline issued an execution claim")
	}
	var pgError *pgconn.PgError
	if _, err := db.ExecContext(ctx, `INSERT INTO fugue_control.execution_claims
		(cluster_id, resource_id, fence, intent_digest, approval_digest, actor) VALUES ($1,$2,1,$3,$4,'worker-a')`,
		scope.ClusterID, scope.ResourceID, intent.Digest, approval.Digest); !errors.As(err, &pgError) ||
		pgError.ConstraintName != "observation_sample_retention_no_claim" {
		t.Fatalf("database did not reject a direct execution claim: %v", err)
	}
	exec(`GRANT EXECUTE ON FUNCTION fugue_observation_maintenance.prune_samples_v1(text, boolean, int, int, int) TO ` + quoted)
	if _, err := p.Observe(ctx, reader); err == nil {
		t.Fatal("accepted observer with retention writer rights")
	}
	exec(`REVOKE EXECUTE ON FUNCTION fugue_observation_maintenance.prune_samples_v1(text, boolean, int, int, int) FROM ` + quoted)
	exec(fmt.Sprintf(performanceFence, "DISABLE"))
	if _, err := p.Observe(ctx, reader); err == nil {
		t.Fatal("accepted a missing delete fence on one of the three targets")
	}
}
