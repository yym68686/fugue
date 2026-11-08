package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"fugue/internal/platformconfig"
)

func TestCompilerInputWriterQueueRespectsCallerDeadline(t *testing.T) {
	s := boundedContentTestStore(t)
	locker, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback()
	if _, err = locker.ExecContext(t.Context(), `SELECT pg_advisory_xact_lock(315609238744290)`); err != nil {
		t.Fatal(err)
	}
	snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: fmt.Sprintf("deadline-%d", time.Now().UnixNano()), PolicyGeneration: "policy"}
	digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.EnsurePlatformCompilerInputContext(ctx, snapshot, digest) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("queued writer ignored caller deadline", err)
		}
		failure := compilerInputWriterStats.diagnosticSnapshot()["last_failure"].(compilerInputWriterFailure)
		if failure.Phase != "writer_barrier" || failure.DeadlineSource != "parent_deadline_observed" || failure.WriterBudgetAfterBeginMS < 0 || failure.WriterBudgetAfterBarrierMS != -1 || failure.WriterStateAtFailure != "deadline_exceeded" || failure.ParentStateAtFailure != "deadline_exceeded" {
			t.Fatal("actual PostgreSQL queue deadline was not distinguished from persistence/cleanup", failure)
		}
	case <-time.After(2 * time.Second):
		_ = locker.Rollback()
		<-done
		t.Fatal("cancelled writer remained in the database queue")
	}
	var count int
	if err = s.db.QueryRowContext(t.Context(), `SELECT count(*) FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest).Scan(&count); err != nil || count != 0 {
		t.Fatal("cancelled writer persisted an input", count, err)
	}
}

func TestCompilerInputWriterSurvivesQueueBeyondLegacyDeadline(t *testing.T) {
	s := boundedContentTestStore(t)
	locker, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback()
	if _, err = locker.ExecContext(t.Context(), `SELECT pg_advisory_xact_lock(315609238744290)`); err != nil {
		t.Fatal(err)
	}
	snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: fmt.Sprintf("queued-write-%d", time.Now().UnixNano()), PolicyGeneration: "policy"}
	digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.EnsurePlatformCompilerInputContext(ctx, snapshot, digest) }()
	select {
	case err := <-done:
		t.Fatal("writer stopped before the valid queue was released", err)
	case <-time.After(31 * time.Second):
	}
	if err = locker.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal("bounded queued write failed", err)
	}
	stored, err := s.GetPlatformCompilerInput(digest)
	if err != nil {
		t.Fatal(err)
	}
	if actual, err := platformconfig.RuntimeSnapshotDigest(stored); err != nil || actual != digest {
		t.Fatal("queued write changed the verified input", err)
	}
}

func TestCompilerInputWriterDiagnosticComponentDeadline(t *testing.T) {
	s := separatedTestStore(t)
	// The guarded helper above permits only a disposable loopback fugue_test DB.
	// Delay actual component INSERT work, after the writer has acquired its lock.
	if _, err := s.db.Exec(`CREATE FUNCTION fugue_test_writer_component_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(1); RETURN NEW; END $$; CREATE TRIGGER fugue_test_writer_component_delay BEFORE INSERT ON fugue_platform_input_components FOR EACH ROW EXECUTE FUNCTION fugue_test_writer_component_delay()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := s.db.Exec(`DROP TRIGGER fugue_test_writer_component_delay ON fugue_platform_input_components; DROP FUNCTION fugue_test_writer_component_delay()`); err != nil {
			t.Error(err)
		}
	})
	snapshot := separatedFixture(t)
	// Distinct canonical evidence forces an INSERT rather than reusing a fixture.
	snapshot.DNSSelections[0].PhysicalEvidence = []byte(fmt.Sprintf(`{"synthetic_component":"%d"}`, time.Now().UnixNano()))
	digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	if err = s.EnsurePlatformCompilerInputContext(ctx, snapshot, digest); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("actual component work ignored the inherited deadline", err)
	}
	failure := compilerInputWriterStats.diagnosticSnapshot()["last_failure"].(compilerInputWriterFailure)
	if failure.Phase != "persist_verify" || failure.DeadlineSource != "parent_deadline_observed" || failure.WriterBudgetAfterBarrierMS < 0 || failure.PhaseMS["persist_verify"] == 0 || failure.PhaseMS["commit"] != 0 {
		t.Fatal("component SQL deadline was confused with queue or commit", failure)
	}
	var count int
	if err = s.db.QueryRowContext(t.Context(), `SELECT count(*) FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest).Scan(&count); err != nil || count != 0 {
		t.Fatal("failed component transaction persisted a manifest", count, err)
	}
}
