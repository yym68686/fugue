package productioncontrol

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/productioncontrol/providergate"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresNativeStorageAuditPolicy(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_NATIVE_STORAGE_POLICY_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires an empty disposable PostgreSQL instance")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	var empty bool
	if err := admin.QueryRowContext(ctx, `SELECT to_regnamespace('fugue_control') IS NULL
		AND to_regnamespace('fugue_storage_audit') IS NULL AND current_database()='postgres'`).Scan(&empty); err != nil || !empty {
		t.Fatalf("native storage policy test requires a disposable instance: %v", err)
	}
	run := func(query string, args ...any) {
		t.Helper()
		if _, err := admin.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	run(`
		CREATE ROLE fixture_policy_writer LOGIN NOINHERIT;
		CREATE ROLE fixture_policy_legacy LOGIN NOINHERIT;
		CREATE TABLE public.fugue_source_uploads(id text, tenant_id text, created_at timestamptz, size_bytes bigint);
		CREATE TABLE public.fugue_apps(id text, tenant_id text, source_json jsonb);
		CREATE TABLE public.fugue_operations(id text, tenant_id text, desired_source_json jsonb);
		CREATE TABLE public.fugue_app_database_import_jobs(id text, tenant_id text, source_upload_id text);
	`)
	raw, err := os.ReadFile("../schemamigrate/storage_lifecycle_audit.sql")
	if err != nil {
		t.Fatal(err)
	}
	run(string(raw))
	run(`CREATE TABLE fugue_storage_audit.lifecycle_activation (
		singleton boolean PRIMARY KEY, approval_sha256 text NOT NULL, policy_sha256 text NOT NULL,
		worker_secret_uid uuid NOT NULL, activated_at timestamptz NOT NULL);`)
	if err := ApplySchema(ctx, admin); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := providergate.ApplyStorageAuditPolicySchema(ctx, admin); err != nil {
			t.Fatal(err)
		}
	}
	run(`
		GRANT USAGE ON SCHEMA fugue_storage_audit TO fixture_policy_legacy, fixture_policy_writer;
		GRANT SELECT,UPDATE ON fugue_storage_audit.lifecycle_control TO fixture_policy_legacy;
		GRANT USAGE ON SCHEMA fugue_control TO fixture_policy_writer;
		GRANT SELECT ON fugue_control.intents, fugue_control.approvals, fugue_control.resource_baselines,
			fugue_control.execution_claims, fugue_control.execution_results TO fixture_policy_writer;
		GRANT INSERT ON fugue_control.execution_claims TO fixture_policy_writer;
		GRANT EXECUTE ON FUNCTION fugue_storage_audit.commit_native_policy(text) TO fixture_policy_writer;
	`)
	open := func(role string) *sql.DB {
		t.Helper()
		cfg, err := pgx.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		cfg.User = role
		db := stdlib.OpenDB(*cfg)
		t.Cleanup(func() { db.Close() })
		return db
	}
	writer := open("fixture_policy_writer")
	legacy := open("fixture_policy_legacy")
	plan := storageAuditShadowFixture()
	plan.PublishedPolicyRetired = true
	plan.Activation.WorkerSecretUID = "4a42e167-7be3-4abc-bac6-dc97db94216c"
	if err := admin.QueryRowContext(ctx, `SELECT system_identifier::text,current_database() FROM pg_control_system()`).Scan(
		&plan.Provider.SystemID, &plan.Provider.Database); err != nil {
		t.Fatal(err)
	}
	base, _, key, trusted, _ := approvalFixture(t)
	now := time.Now().UTC()
	cluster := base.ClusterID
	head := Version{1, plan.Current.Digest}
	body, err := json.Marshal(plan.Current.Document)
	if err != nil {
		t.Fatal(err)
	}
	databaseDigest := strings.TrimPrefix(plan.Current.Digest, "sha256:")
	run(`INSERT INTO fugue_storage_audit.lifecycle_control VALUES (true,'api','audit',$1,$2,300,64,$3::jsonb)`,
		databaseDigest, plan.Current.Document.EffectiveFrom, body)
	run(`UPDATE fugue_storage_audit.lifecycle_control SET owner='worker' WHERE singleton`)
	run(`INSERT INTO fugue_storage_audit.lifecycle_activation VALUES (true,$1,$2,$3::uuid,$4)`,
		plan.Activation.DeclarationDigest, plan.Activation.PolicyDigest, plan.Activation.WorkerSecretUID, plan.Activation.ActivatedAt)
	run(`SELECT fugue_storage_audit.freeze_native_policy($1,$2,$3::jsonb,$4)`,
		cluster, plan.Current.Digest, body, digest([]byte("fixture freeze evidence")))
	if _, err := legacy.ExecContext(ctx, `UPDATE fugue_storage_audit.lifecycle_control SET
		source_page_rows=65,policy_document=jsonb_set(policy_document,'{source_page_rows}','65') WHERE singleton`); err == nil {
		t.Fatal("legacy publisher escaped the frozen state")
	}
	intake, err := NewJournal(admin, trusted)
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := NewJournal(writer, trusted)
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{cluster, StorageAuditPolicyResource}
	observerPublic, observerKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	witnessKey := TrustedWitness{Scope: scope, PublicKey: observerPublic}
	checkpoint, err := intake.ReadFrozenStorageAuditPolicy(ctx, scope, plan.Provider.SystemID, plan.Provider.Database)
	if err != nil {
		t.Fatal(err)
	}
	revoked := digest([]byte("fixture external revocation record"))
	signedBaseline := ExecutionWitness{Kind: BaselineWitness, KeyID: "observer-a", ClusterID: cluster,
		ResourceID: StorageAuditPolicyResource, Generation: head.Generation, VerifiedDigest: head.Digest,
		ObservationDigest: checkpoint.ObservationDigest, AuthorityDigest: revoked,
		ObservedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(time.Minute).Unix()}
	input, err := signedBaseline.SigningInput()
	if err != nil {
		t.Fatal(err)
	}
	signedBaseline.Signature = ed25519.Sign(observerKey, input)
	witness, ref, err := EncodeExecutionWitness(signedBaseline)
	if err != nil {
		t.Fatal(err)
	}
	location := memoryWitnesses{ref: witness}
	baseline := Baseline{scope, head, ref}
	if created, err := intake.ImportStorageAuditPolicyBaseline(ctx, baseline, plan.Provider.SystemID,
		plan.Provider.Database, digest([]byte("another revocation record")), location, "observer-a", witnessKey); err == nil || created {
		t.Fatal("baseline witness authorized a different revocation record")
	}
	if created, err := intake.ImportStorageAuditPolicyBaseline(ctx, baseline, plan.Provider.SystemID,
		plan.Provider.Database, revoked, location, "observer-a", witnessKey); err != nil || !created {
		t.Fatalf("recording verified baseline: %v %v", created, err)
	}
	if snapshot, err := readBaselineSnapshot(ctx, admin, scope, head); err != nil || string(snapshot) != string(body) {
		t.Fatalf("baseline did not persist the verified policy: %s %v", snapshot, err)
	}
	record := func(request string, head Version, plan StorageAuditShadow) (Recorded, Recorded) {
		t.Helper()
		submission, err := DraftStorageAuditPolicy(cluster, request, head, plan)
		if err != nil {
			t.Fatal(err)
		}
		i, err := intake.RecordIntent(ctx, "operator-a", submission.Intent, submission.Payload)
		if err != nil {
			t.Fatal(err)
		}
		approval, err := SignApproval(submission.Intent, "owner-a", key, now, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		a, err := intake.RecordApproval(ctx, approval)
		if err != nil {
			t.Fatal(err)
		}
		return i, a
	}
	intent, approval := record("change", head, plan)
	verify := func(context.Context, StorageAuditShadow) error { return nil }
	if _, _, err := dispatch.DispatchStorageAuditPolicy(ctx, cluster, "policy-writer", intent.Digest, approval.Digest, verify); err == nil {
		t.Fatal("native dispatch accepted before provider authority was enrolled")
	}
	if _, err := readClaim(ctx, admin, intent.Digest); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("failed provider mutation left a journal claim: %v", err)
	}
	run(`SELECT fugue_storage_audit.enroll_native_policy_writer($1,'fixture_policy_writer',$2)`, cluster, baseline.EvidenceDigest)
	if _, err := writer.ExecContext(ctx, `INSERT INTO fugue_control.execution_claims
		(cluster_id,resource_id,fence,intent_digest,approval_digest,actor)
		VALUES ($1,'maintenance/storage-audit-policy',1,$2,$3,'policy-writer')`, cluster, intent.Digest, approval.Digest); err == nil {
		t.Fatal("writer committed a claim separately from the provider receipt")
	}
	if _, err := writer.ExecContext(ctx, `SELECT fugue_storage_audit.commit_native_policy($1)`, intent.Digest); err == nil {
		t.Fatal("provider accepted the writer without a native claim")
	}
	if _, err := writer.ExecContext(ctx, `UPDATE fugue_storage_audit.lifecycle_control SET owner='worker'`); err == nil {
		t.Fatal("native role unexpectedly has direct table update permission")
	}
	var created atomic.Int32
	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() {
			claim, added, err := dispatch.DispatchStorageAuditPolicy(ctx, cluster, "policy-writer", intent.Digest, approval.Digest, verify)
			if err != nil || claim.Fence != 1 {
				t.Errorf("native dispatch: %+v %v %v", claim, added, err)
			}
			if added {
				created.Add(1)
			}
		})
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	var receiptCount int
	var target string
	if err := admin.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM fugue_storage_audit.native_policy_receipts),
		(SELECT 'sha256:'||policy_sha256 FROM fugue_storage_audit.lifecycle_control)`).Scan(&receiptCount, &target); err != nil ||
		created.Load() != 1 || receiptCount != 1 || target != plan.Target.Digest {
		t.Fatalf("wrong dispatch result: commits=%d receipts=%d selected=%s err=%v", created.Load(), receiptCount, target, err)
	}
	state, err := dispatch.ReadResource(ctx, baseline.Scope)
	if err != nil || state.Pending != intent.Digest || state.LastVerified != head {
		t.Fatalf("write advanced the verified version before independent observation: %+v %v", state, err)
	}
	native, err := NewStorageAuditPolicyExecutionJournal(intake, baseline.Scope, location,
		map[string]TrustedWitness{"observer-a": {Scope: baseline.Scope, PublicKey: observerPublic}})
	if err != nil {
		t.Fatal(err)
	}
	settle := func(i, a Recorded, fence int64) {
		t.Helper()
		claim := Claim{baseline.Scope, fence, i.Digest, a.Digest}
		actual, err := intake.ReadStorageAuditPolicyTransition(ctx, claim)
		if err != nil {
			t.Fatal(err)
		}
		putWitness := func(observedDigest string) string {
			t.Helper()
			signed := ExecutionWitness{
				Kind: TerminalWitness, KeyID: "observer-a", ClusterID: cluster, ResourceID: StorageAuditPolicyResource,
				Generation: actual.Verified.Generation, VerifiedDigest: actual.Verified.Digest,
				ObservationDigest: observedDigest, AuthorityDigest: actual.AuthorityDigest,
				ObservedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(time.Minute).Unix(),
				IntentDigest: i.Digest, ApprovalDigest: a.Digest, Fence: fence, Outcome: Applied,
			}
			input, err := signed.SigningInput()
			if err != nil {
				t.Fatal(err)
			}
			signed.Signature = ed25519.Sign(observerKey, input)
			raw, ref, err := EncodeExecutionWitness(signed)
			if err != nil {
				t.Fatal(err)
			}
			location[ref] = raw
			return ref
		}
		invalid := ExecutionResult{Applied, actual.Verified, putWitness(digest([]byte("wrong observed policy")))}
		if created, err := native.CompleteExecution(ctx, "observer-a", claim, invalid); err == nil || created {
			t.Fatal("settlement accepted an authentic witness with a mismatched provider observation")
		}
		result := ExecutionResult{Applied, actual.Verified, putWitness(actual.ObservationDigest)}
		if created, err := native.CompleteExecution(ctx, "observer-a", claim, result); err != nil || !created {
			t.Fatalf("settlement: %v %v", created, err)
		}
		if created, err := native.CompleteExecution(ctx, "observer-a", claim, result); err != nil || created {
			t.Fatalf("settlement replay: %v %v", created, err)
		}
	}
	settle(intent, approval, 1)
	inverse := plan
	inverse.Current, inverse.Target, inverse.Rollback = plan.Target, plan.Current, plan.Target
	rollback, rollbackApproval := record("rollback", Version{2, plan.Target.Digest}, inverse)
	if claim, added, err := dispatch.DispatchStorageAuditPolicy(ctx, cluster, "policy-writer", rollback.Digest, rollbackApproval.Digest, verify); err != nil ||
		!added || claim.Fence != 2 {
		t.Fatalf("approved rollback: %+v %v %v", claim, added, err)
	}
	settle(rollback, rollbackApproval, 2)
	state, err = dispatch.ReadResource(ctx, baseline.Scope)
	if err != nil || state.Pending != "" || state.LastVerified != (Version{3, head.Digest}) {
		t.Fatalf("rollback did not restore the exact verified policy: %+v %v", state, err)
	}
}
