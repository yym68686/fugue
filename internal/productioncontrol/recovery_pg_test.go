package productioncontrol

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresJournalRecoveryCheckpoint(t *testing.T) {
	sourceURL, restoreURL := os.Getenv("FUGUE_TEST_RECOVERY_SOURCE_URL"), os.Getenv("FUGUE_TEST_RECOVERY_RESTORE_URL")
	if sourceURL == "" || restoreURL == "" {
		t.Skip("set both FUGUE_TEST_RECOVERY_*_URL values to separate empty disposable PostgreSQL databases")
	}
	if sourceURL == restoreURL {
		t.Fatal("restore target must differ from source")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	openEmpty := func(dsn string) *sql.DB {
		t.Helper()
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		var empty bool
		if err := db.QueryRowContext(ctx, `SELECT NOT EXISTS (
			SELECT FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
			WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%')`).Scan(&empty); err != nil || !empty {
			t.Fatalf("test requires an empty disposable database: empty=%v err=%v", empty, err)
		}
		return db
	}
	source, restored := openEmpty(sourceURL), openEmpty(restoreURL)
	if err := ApplySchema(ctx, source); err != nil {
		t.Fatal(err)
	}
	intent, payload, key, approvers, now := approvalFixture(t)
	intent.RollbackDigest = intent.CurrentDigest
	journalFor := func(db *sql.DB) *Journal {
		t.Helper()
		j, err := NewJournal(db, approvers)
		if err != nil {
			t.Fatal(err)
		}
		j.now = func() time.Time { return now }
		return j
	}
	j := journalFor(source)
	// A v1 backup is verified in its restored schema before migration. The
	// rehome admission table, even when empty, selects a separate v2 digest.
	if _, err := source.ExecContext(ctx, `DROP TRIGGER registry_rehome_claim_admission ON fugue_control.execution_claims;
		DROP TRIGGER registry_rehome_result_guard ON fugue_control.execution_results;
		DROP TABLE fugue_control.registry_rehome_applied;
		DROP TABLE fugue_control.registry_rehome_no_write;
		DROP TABLE fugue_control.registry_rehome_admissions`); err != nil {
		t.Fatal(err)
	}
	old, err := j.RecoveryCheckpoint(ctx, intent.ClusterID, "")
	if err != nil || old.Format != recoveryDomainV1 {
		t.Fatalf("legacy journal checkpoint: %s %v", old.Format, err)
	}
	if _, err := j.VerifyRecoveryCheckpoint(ctx, intent.ClusterID, old.Digest); err != nil {
		t.Fatal(err)
	}
	if err := ApplySchema(ctx, source); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ExecContext(ctx, `DROP TABLE fugue_control.registry_rehome_applied, fugue_control.registry_rehome_no_write`); err != nil {
		t.Fatal(err)
	}
	v2, err := j.RecoveryCheckpoint(ctx, intent.ClusterID, "")
	if err != nil || v2.Format != recoveryDomainV2 {
		t.Fatalf("v2 journal checkpoint: %s %v", v2.Format, err)
	}
	if _, err := j.VerifyRecoveryCheckpoint(ctx, intent.ClusterID, old.Digest); err == nil {
		t.Fatal("upgraded v2 journal matched a legacy inventory digest")
	}
	if err := ApplySchema(ctx, source); err != nil {
		t.Fatal(err)
	}
	if _, err := j.VerifyRecoveryCheckpoint(ctx, intent.ClusterID, v2.Digest); err == nil {
		t.Fatal("upgraded v4 journal matched a v2 inventory digest")
	}
	if _, err := source.ExecContext(ctx, `DROP TABLE fugue_control.registry_rehome_applied`); err != nil {
		t.Fatal(err)
	}
	v3, err := j.RecoveryCheckpoint(ctx, intent.ClusterID, "")
	if err != nil || v3.Format != recoveryDomainV3 {
		t.Fatalf("legacy v3 journal checkpoint: %s %v", v3.Format, err)
	}
	if _, err := j.VerifyRecoveryCheckpoint(ctx, intent.ClusterID, v3.Digest); err != nil {
		t.Fatal(err)
	}
	if err := ApplySchema(ctx, source); err != nil {
		t.Fatal(err)
	}
	lkg := Version{intent.Generation, intent.CurrentDigest}
	b := Baseline{Scope: intent.scope(), Verified: lkg, EvidenceDigest: digest([]byte("baseline witness"))}
	w, err := NewExecutionJournal(j, &testExecutionEvidence{enrollment: b.EvidenceDigest})
	if err != nil {
		t.Fatal(err)
	}
	if created, err := w.AnchorResource(ctx, "observer-a", b); err != nil || !created {
		t.Fatalf("anchor: %v %v", created, err)
	}
	record := func(j *Journal, i Intent) (string, string) {
		t.Helper()
		entry, err := j.RecordIntent(ctx, "operator-a", i, payload)
		if err != nil {
			t.Fatal(err)
		}
		approval, err := SignApproval(i, "owner-a", key, now, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		review, err := j.RecordApproval(ctx, approval)
		if err != nil {
			t.Fatal(err)
		}
		return entry.Digest, review.Digest
	}
	id, approval := record(j, intent)
	claim, created, err := w.ReserveExecution(ctx, "worker-a", id, approval)
	if err != nil || !created {
		t.Fatalf("reserve: %v %v", created, err)
	}
	if consumed, err := j.ReserveReleaseLaunch(ctx, "guardian-a", intent, payload, claim.Fence); err != nil || !consumed {
		t.Fatalf("launch: %v %v", consumed, err)
	}
	move := anchorRegistryRehomeFixture(t, ctx, j, key, "rehome-recovery")
	_, admission, admissionDigest := signRegistryRehomeFixture(t, move.report(1), move.observer)
	observer := move.observer.Public().(ed25519.PublicKey)
	readOnlyCheck := func(context.Context, RegistryRehomeAdmission, Submission) error { return nil }
	moveJournal, err := NewRegistryRehomeJournal(j, move.intent.scope(), observer, readOnlyCheck)
	if err != nil {
		t.Fatal(err)
	}
	moveClaim, granted, err := moveJournal.Reserve(ctx, "fixture-rehome", move.record, move.approval, admission)
	if err != nil || !granted {
		t.Fatalf("fixture rehome reservation: %v %v", granted, err)
	}
	closed := anchorRegistryRehomeFixture(t, ctx, j, key, "rehome-closed-recovery")
	admitted, closureAdmission, closureAdmissionPin := signRegistryRehomeFixture(t, closed.report(1), closed.observer)
	closureObserver := closed.observer.Public().(ed25519.PublicKey)
	closureJournal, err := NewRegistryRehomeJournal(j, closed.intent.scope(), closureObserver, readOnlyCheck)
	if err != nil {
		t.Fatal(err)
	}
	closureClaim, created, err := closureJournal.Reserve(ctx, "fixture-rehome", closed.record, closed.approval, closureAdmission)
	if err != nil || !created {
		t.Fatalf("fixture closure reservation: %v %v", created, err)
	}
	closureTime := time.Unix(admitted.ExpiresAt, 0)
	closureRaw, closurePin := signRehomeNoWriteFixture(t, rehomeNoWriteFixture(closureClaim, closureAdmissionPin, closureTime), closed.observer)
	closureWriter := journalFor(source)
	closureWriter.now = func() time.Time { return closureTime }
	settler, err := NewRegistryRehomeNoWriteJournal(closureWriter, closureClaim.Scope, closureObserver,
		func(context.Context, RegistryRehomeNoWriteEvidence) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if settled, err := settler.CloseWithoutWrite(ctx, "fixture-settler", closed.record, closureRaw); err != nil || !settled.Created {
		t.Fatalf("fixture no-write settlement: %+v %v", settled, err)
	}
	applied := anchorRegistryRehomeFixture(t, ctx, j, key, "rehome-applied-recovery")
	a, raw, pin := signRegistryRehomeFixture(t, applied.report(1), applied.observer)
	appliedObserver := applied.observer.Public().(ed25519.PublicKey)
	admissionWriter, err := NewRegistryRehomeJournal(j, applied.intent.scope(), appliedObserver, readOnlyCheck)
	if err != nil {
		t.Fatal(err)
	}
	appliedClaim, granted, err := admissionWriter.Reserve(ctx, "fixture-rehome", applied.record, applied.approval, raw)
	if err != nil || !granted {
		t.Fatalf("fixture applied reservation: %v %v", granted, err)
	}
	appliedWriter, err := NewRegistryRehomeAdmissionReader(closureWriter, appliedClaim.Scope, appliedObserver)
	if err != nil {
		t.Fatal(err)
	}
	h := func(s string) string { return digest([]byte("fixture rehome applied " + s)) }
	proof := RegistryRehomeAppliedEvidence{ClusterID: appliedClaim.ClusterID, ResourceID: appliedClaim.ResourceID,
		IntentDigest: appliedClaim.IntentDigest, ApprovalDigest: appliedClaim.ApprovalDigest, Fence: appliedClaim.Fence,
		AdmissionDigest: pin, BaselineDigest: h("baseline"), BundleDigest: h("bundle"), ReadbackDigest: h("readback"),
		ServingEvent: h("serving event"), ServingState: h("serving state"), EndpointFreeze: h("freeze"), WriterRevocation: h("revoke"),
		RequestDrain: h("drain"), BackupReadback: h("backup"), ObservedAt: a.ExpiresAt, ExpiresAt: a.ExpiresAt + 60}
	message, err := proof.SigningInput()
	if err != nil {
		t.Fatal(err)
	}
	proof.Signature = ed25519.Sign(applied.observer, message)
	appliedEvidence, evidencePin, err := EncodeRegistryRehomeAppliedEvidence(proof)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.ExecContext(ctx, `INSERT INTO fugue_control.execution_results
		(intent_digest,outcome,generation,verified_digest,evidence_digest,actor)
		VALUES ($1,'applied',$2,$3,$4,'fixture-direct')`, applied.record, applied.intent.Generation+1, proof.BaselineDigest, evidencePin); err == nil {
		t.Fatal("direct SQL settled a rehome without its dedicated receipt")
	}
	if got, err := appliedWriter.CloseApplied(ctx, "fixture-settler", applied.record, appliedEvidence,
		func(context.Context, RegistryRehomeAppliedEvidence) error { return nil }); err != nil || !got.Created || got.Digest != evidencePin {
		t.Fatalf("fixture applied settlement: %+v %v", got, err)
	}
	wrongOutcome, wrongPin := signRehomeNoWriteFixture(t, rehomeNoWriteFixture(appliedClaim, pin, closureTime), applied.observer)
	if _, err := source.ExecContext(ctx, `INSERT INTO fugue_control.registry_rehome_no_write
		(intent_digest,admission_digest,evidence_digest,envelope) VALUES ($1,$2,$3,$4)`, applied.record, pin, wrongPin, wrongOutcome); err == nil {
		t.Fatal("SQL appended a contradictory no-write receipt to an applied settlement")
	}

	exporter, err := source.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer exporter.Rollback()
	var snapshot string
	if err := exporter.QueryRowContext(ctx, `SELECT pg_export_snapshot()`).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	other := intent
	other.RequestID = "after-export"
	second, secondApproval := record(j, other)
	checkpoint, err := j.RecoveryCheckpoint(ctx, intent.ClusterID, snapshot)
	if err != nil || checkpoint.Format != recoveryDomainV4 {
		t.Fatalf("rehome checkpoint did not use v4: %s %v", checkpoint.Format, err)
	}
	live, err := j.RecoveryCheckpoint(ctx, intent.ClusterID, "")
	if err != nil || checkpoint.Digest == live.Digest {
		t.Fatalf("checkpoint did not respect export boundary: %v", err)
	}
	if _, err := j.RecoveryCheckpoint(ctx, intent.ClusterID, snapshot+"'; select 1"); err == nil {
		t.Fatal("accepted malformed snapshot identifier")
	}
	dump := filepath.Join(t.TempDir(), "journal.dump")
	run := func(bin string, args ...string) {
		t.Helper()
		if output, err := exec.CommandContext(ctx, bin, args...).CombinedOutput(); err != nil {
			t.Fatalf("%s: %v: %s", bin, err, output)
		}
	}
	run("pg_dump", "--snapshot="+snapshot, "--format=custom", "--no-owner", "--no-privileges", "--file="+dump, "--dbname="+sourceURL)
	if err := exporter.Commit(); err != nil {
		t.Fatal(err)
	}
	run("pg_restore", "--exit-on-error", "--single-transaction", "--no-owner", "--no-privileges", "--dbname="+restoreURL, dump)

	if _, err := restored.ExecContext(ctx, `CREATE ROLE fugue_recovery_fixture_reader NOLOGIN;
		GRANT USAGE ON SCHEMA fugue_control TO fugue_recovery_fixture_reader;
		GRANT SELECT ON ALL TABLES IN SCHEMA fugue_control TO fugue_recovery_fixture_reader;`); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgx.ParseConfig(restoreURL)
	if err != nil {
		t.Fatal(err)
	}
	reader := stdlib.OpenDB(*cfg, stdlib.OptionAfterConnect(func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `SET ROLE fugue_recovery_fixture_reader`)
		return err
	}))
	defer reader.Close()
	verify := journalFor(reader)
	if _, err := verify.VerifyRecoveryCheckpoint(ctx, intent.ClusterID, checkpoint.Digest); err != nil {
		t.Fatalf("read-only verifier could not match exported backup checkpoint: %v", err)
	}
	recoveredApplied, err := NewRegistryRehomeAdmissionReader(verify, appliedClaim.Scope, appliedObserver)
	if err != nil {
		t.Fatal(err)
	}
	recoveredProof, terminal, err := recoveredApplied.ReadApplied(ctx, applied.record)
	if err != nil || recoveredProof.BaselineDigest != proof.BaselineDigest || recoveredProof.BundleDigest != proof.BundleDigest ||
		terminal != (ExecutionResult{Applied, Version{applied.intent.Generation + 1, proof.BaselineDigest}, evidencePin}) {
		t.Fatalf("restored journal did not recover the applied rehome result: %+v %v", terminal, err)
	}
	readBack, err := NewRegistryRehomeNoWriteJournal(verify, closureClaim.Scope, closureObserver,
		func(context.Context, RegistryRehomeNoWriteEvidence) error {
			return errors.New("replay must not run live check")
		})
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := readBack.CloseWithoutWrite(ctx, "fixture-settler", closed.record, closureRaw); err != nil || replay.Created || replay.Digest != closurePin {
		t.Fatalf("restored read-only journal could not replay exact no-write outcome: %+v %v", replay, err)
	}
	if _, err := reader.ExecContext(ctx, `UPDATE fugue_control.intents SET request_id='unexpected' WHERE false`); err == nil {
		t.Fatal("restore verifier could write to the execution journal")
	}
	if _, err := restored.ExecContext(ctx, `ALTER TABLE fugue_control.audit ENABLE ROW LEVEL SECURITY`); err != nil {
		t.Fatal(err)
	}
	var pgError *pgconn.PgError
	if _, err := verify.VerifyRecoveryCheckpoint(ctx, intent.ClusterID, checkpoint.Digest); !errors.As(err, &pgError) || pgError.Code != "42501" {
		t.Fatalf("verification did not fail closed when row-security could hide records: %v", err)
	}
	if _, err := restored.ExecContext(ctx, `ALTER TABLE fugue_control.audit DISABLE ROW LEVEL SECURITY`); err != nil {
		t.Fatal(err)
	}
	r := journalFor(restored)
	restoredMove, err := NewRegistryRehomeJournal(r, move.intent.scope(), observer, readOnlyCheck)
	if err != nil {
		t.Fatal(err)
	}
	if _, d, err := restoredMove.ReadAdmission(ctx, moveClaim); err != nil || d != admissionDigest {
		t.Fatalf("restored rehome admission not byte exact: %s %v", d, err)
	}
	if found, created, err := restoredMove.Reserve(ctx, "fixture-rehome", move.record, move.approval, admission); err != nil || created || found != moveClaim {
		t.Fatalf("restored database granted a second rehome dispatch: %+v %v %v", found, created, err)
	}
	if _, err := r.VerifyRecoveryCheckpoint(ctx, intent.ClusterID, live.Digest); err == nil {
		t.Fatal("accepted stale backup with current journal's pinned digest")
	}
	if _, err := r.VerifyRecoveryCheckpoint(ctx, "wrong-cluster", checkpoint.Digest); err == nil {
		t.Fatal("accepted checkpoint in another cluster")
	}
	state, err := r.ReadResource(ctx, intent.scope())
	if err != nil || state.LastVerified != lkg || state.Pending != id || state.LastFence != claim.Fence {
		t.Fatalf("restored journal lost pending or verified state: %+v %v", state, err)
	}
	if consumed, err := r.ReserveReleaseLaunch(ctx, "guardian-a", intent, payload, claim.Fence); err != nil || consumed {
		t.Fatalf("restored journal reissued a consumed launch: %v %v", consumed, err)
	}
	record(r, other)
	if _, created, err := r.ReserveExecution(ctx, "worker-b", second, secondApproval); created || !errors.Is(err, ErrResourceBusy) {
		t.Fatalf("restored journal lost the pending resource fence: %v %v", created, err)
	}
	if _, err := r.VerifyRecoveryCheckpoint(ctx, intent.ClusterID, checkpoint.Digest); err == nil {
		t.Fatal("accepted modified audit and intent rows")
	}
	if _, err := restored.ExecContext(ctx, `CREATE TABLE fugue_control.future_data (id integer PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := r.RecoveryCheckpoint(ctx, intent.ClusterID, ""); err == nil {
		t.Fatal("silently omitted unrecognized journal table")
	}
}
