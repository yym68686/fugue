package productioncontrol

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresJournalReplayAndAtomicAudit(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_PRODUCTION_CONTROL_DATABASE_URL")
	if dsn == "" {
		t.Skip("set FUGUE_TEST_PRODUCTION_CONTROL_DATABASE_URL to an empty disposable PostgreSQL database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var existingSchema bool
	if err := db.QueryRowContext(ctx, `SELECT to_regnamespace('fugue_control') IS NOT NULL`).Scan(&existingSchema); err != nil {
		t.Fatal(err)
	}
	if existingSchema {
		t.Fatal("refusing to run isolated journal test with a pre-existing Fugue control schema")
	}
	for range 2 {
		if err := ApplySchema(ctx, db); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `
		CREATE ROLE fugue_test_control_journal NOLOGIN;
		GRANT USAGE ON SCHEMA fugue_control TO fugue_test_control_journal;
		GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA fugue_control TO fugue_test_control_journal;
	`); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	runtimeDB := stdlib.OpenDB(*cfg, stdlib.OptionAfterConnect(func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `SET ROLE fugue_test_control_journal`)
		return err
	}))
	defer runtimeDB.Close()
	for _, statement := range []string{
		`UPDATE fugue_control.audit SET actor='changed' WHERE false`,
		`DELETE FROM fugue_control.intents WHERE false`,
		`CREATE TABLE fugue_control.unapproved_ddl (id text)`,
	} {
		_, err := runtimeDB.ExecContext(ctx, statement)
		var pgError *pgconn.PgError
		if !errors.As(err, &pgError) || pgError.Code != "42501" {
			t.Fatalf("expected permission denial for %q, got %v", statement, err)
		}
	}
	intent, payload, private, trusted, now := approvalFixture(t)
	j, err := NewJournal(runtimeDB, trusted)
	if err != nil {
		t.Fatal(err)
	}
	j.now = func() time.Time { return now }
	intentDigest, err := intent.Digest()
	if err != nil {
		t.Fatal(err)
	}
	runReplay := func(name string, appendRecord func() (Recorded, error)) {
		t.Helper()
		var wg sync.WaitGroup
		var writes atomic.Int32
		for range 12 {
			wg.Go(func() {
				out, err := appendRecord()
				if err != nil || out.Digest == "" {
					t.Errorf("%s: record=%+v err=%v", name, out, err)
					return
				}
				if out.Created {
					writes.Add(1)
				}
			})
		}
		wg.Wait()
		if n := writes.Load(); n != 1 {
			t.Fatalf("%s recorded %d times, expected one persisted insertion", name, n)
		}
	}
	runReplay("intent", func() (Recorded, error) { return j.RecordIntent(ctx, "operator-a", intent, payload) })
	changed := intent
	changed.Generation++
	if _, err := j.RecordIntent(ctx, "operator-a", changed, payload); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("reused request ID with different content: %v", err)
	}
	second, err := NewJournal(runtimeDB, trusted)
	if err != nil {
		t.Fatal(err)
	}
	second.now = j.now
	stored, decodedPayload, err := second.ReadIntent(ctx, intentDigest)
	if err != nil || stored != intent || string(decodedPayload) != string(payload) {
		t.Fatalf("restart lost intent: %+v, %s, %v", stored, decodedPayload, err)
	}
	approval, err := SignApproval(intent, "owner-a", private, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	runReplay("approval", func() (Recorded, error) { return second.RecordApproval(ctx, approval) })
	for _, check := range []struct {
		query, name string
		want        int
	}{
		{`SELECT count(*) FROM fugue_control.intents`, "intent", 1},
		{`SELECT count(*) FROM fugue_control.approvals`, "approval", 1},
		{`SELECT count(*) FROM fugue_control.audit WHERE kind='intent.recorded' AND actor='operator-a'`, "intent audit", 1},
		{`SELECT count(*) FROM fugue_control.audit WHERE kind='approval.recorded' AND actor='owner-a'`, "approval audit", 1},
	} {
		var count int
		if err := db.QueryRowContext(ctx, check.query).Scan(&count); err != nil || count != check.want {
			t.Fatalf("%s: count=%d err=%v", check.name, count, err)
		}
	}
	// Independently cause the audit INSERT to fail. The intent must not survive
	// the failed transaction, even though its INSERT was accepted.
	if _, err := db.ExecContext(ctx, `ALTER TABLE fugue_control.audit
		ADD CONSTRAINT reject_test_actor CHECK (actor <> 'audit-error')`); err != nil {
		t.Fatal(err)
	}
	intent.RequestID = "request-2"
	failedDigest, err := intent.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.RecordIntent(ctx, "audit-error", intent, payload); err == nil {
		t.Fatal("audit failure did not fail the command")
	}
	if _, _, err := second.ReadIntent(ctx, failedDigest); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("unaudited intent escaped rollback: %v", err)
	}
	t.Run("execution-lifecycle", func(t *testing.T) { testExecutionLifecycle(t, ctx, j, second) })
	t.Run("global-dns-execution-boundary", func(t *testing.T) { testGlobalDNSExecutionBoundary(t, ctx, j) })

	// Recreate the v1 audit layout in this disposable database without
	// touching the transition tables. The migrator must rehydrate the audit
	// from original rows, preserving their time and actor, exactly once.
	if _, err := db.ExecContext(ctx, `
		DROP TRIGGER execution_claim_audit ON fugue_control.execution_claims;
		DROP TRIGGER execution_result_audit ON fugue_control.execution_results;
		DROP TRIGGER execution_launch_audit ON fugue_control.execution_launches;
		DROP TRIGGER participation_topology_audit ON fugue_control.participation_topologies;
		DELETE FROM fugue_control.audit WHERE kind LIKE 'execution.%';
		ALTER TABLE fugue_control.audit DROP CONSTRAINT audit_event_kind_v3;
		ALTER TABLE fugue_control.audit ADD CONSTRAINT audit_kind_check CHECK (kind IN ('intent.recorded','approval.recorded'));
	`); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := ApplySchema(ctx, db); err != nil {
			t.Fatal(err)
		}
	}
	var launches int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM fugue_control.execution_launches l
		JOIN fugue_control.audit a ON a.intent_digest=l.intent_digest AND a.kind='execution.launched'
		AND l.recorded_at=a.recorded_at AND l.actor=a.actor`).Scan(&launches); err != nil || launches != 1 {
		t.Fatalf("historical launch was not backfilled exactly: count=%d error=%v", launches, err)
	}
	t.Run("independent-witness-gate", func(t *testing.T) { testPostgresWitnessGate(t, ctx, runtimeDB) })
	t.Run("participation-topology", func(t *testing.T) { testPostgresParticipationTopology(t, ctx, db, runtimeDB) })
}
