package providergate

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/productioncontrol"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresGlobalProducerFreeze(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_GLOBAL_PRODUCER_GATE_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires a disposable PostgreSQL database")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	admin, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	exec := func(conn *sql.Conn, query string, args ...any) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	expectCode := func(err error, code string) {
		t.Helper()
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != code {
			t.Fatalf("expected SQLSTATE %s, got %v", code, err)
		}
	}
	exec(admin, `CREATE SCHEMA fugue_gate_fixture`)
	createdRole, createdNativeRole, createdJournal := false, false, false
	t.Cleanup(func() {
		if _, err := db.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS fugue_gate_fixture CASCADE`); err != nil {
			t.Logf("fixture cleanup: %v", err)
		}
		if createdRole {
			if _, err := db.ExecContext(context.Background(), `DROP ROLE fugue_gate_fixture_writer`); err != nil {
				t.Logf("fixture role cleanup: %v", err)
			}
		}
		if createdJournal {
			if _, err := db.ExecContext(context.Background(), `DROP SCHEMA fugue_control CASCADE`); err != nil {
				t.Logf("fixture journal cleanup: %v", err)
			}
		}
		if createdNativeRole {
			if _, err := db.ExecContext(context.Background(), `DROP ROLE fugue_gate_fixture_native`); err != nil {
				t.Logf("fixture native role cleanup: %v", err)
			}
		}
	})
	exec(admin, `SET search_path TO fugue_gate_fixture`)
	exec(admin, `CREATE TABLE fugue_platform_release_lanes (
  lane_key text PRIMARY KEY, artifact_kind text NOT NULL, scope_key text NOT NULL,
  release_channel text NOT NULL, active_release_id text NOT NULL, fencing_token bigint NOT NULL,
  frozen boolean NOT NULL DEFAULT FALSE, version bigint NOT NULL DEFAULT 1);
CREATE TABLE fugue_platform_artifact_releases (
  id text PRIMARY KEY, artifact_id text NOT NULL, artifact_kind text NOT NULL, scope_key text NOT NULL,
  release_channel text NOT NULL, lane_key text NOT NULL, generation text NOT NULL,
  status text NOT NULL, fencing_token bigint NOT NULL, idempotency_key text NOT NULL DEFAULT '');
CREATE TABLE fugue_platform_artifacts (
  id text PRIMARY KEY, generation text NOT NULL, artifact_kind text NOT NULL,
  scope_key text NOT NULL, status text NOT NULL, content_hash text NOT NULL, generation_sequence bigint NOT NULL);`)
	exec(admin, GlobalProducerFreezeSchema)
	baseDigest := "sha256:" + strings.Repeat("a", 64)
	newDigest := "sha256:" + strings.Repeat("b", 64)
	evidence := "sha256:" + strings.Repeat("c", 64)
	targetDigest := "sha256:" + strings.Repeat("d", 64)
	exec(admin, `INSERT INTO fugue_platform_artifacts VALUES
  ('base','gen1','policy_snapshot','platform-config-producer','validated',$1,1),
  ('next','gen2','policy_snapshot','platform-config-producer','validated',$2,2),
  ('target','gen3','policy_snapshot','platform-config-producer','validated',$3,3)`, baseDigest, newDigest, targetDigest)
	exec(admin, `INSERT INTO fugue_platform_artifact_releases VALUES
  ('r1','base','policy_snapshot','platform-config-producer','shadow','policy_snapshot|platform-config-producer|shadow','gen1','active',1,'')`)
	exec(admin, `INSERT INTO fugue_platform_release_lanes VALUES
  ('policy_snapshot|platform-config-producer|shadow','policy_snapshot','platform-config-producer','shadow','r1',1,FALSE,1),
  ('traffic','release_set','global','full','traffic-r1',1,FALSE,1)`)
	// A writer need not have access to the gate row. Its legacy transaction
	// holds the provider fence from the first policy-changing statement.
	exec(admin, `CREATE ROLE fugue_gate_fixture_writer NOLOGIN`)
	createdRole = true
	exec(admin, `GRANT USAGE ON SCHEMA fugue_gate_fixture TO fugue_gate_fixture_writer;
GRANT SELECT,INSERT,UPDATE,DELETE ON fugue_platform_release_lanes,fugue_platform_artifact_releases TO fugue_gate_fixture_writer`)
	writer, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	exec(writer, `SET search_path TO fugue_gate_fixture`)
	exec(writer, `SET ROLE fugue_gate_fixture_writer`)
	defer writer.ExecContext(context.Background(), `RESET ROLE`)
	_, err = writer.ExecContext(ctx, `UPDATE fugue_native_global_producer_gate SET frozen=FALSE`)
	expectCode(err, "42501")
	pre, err := writer.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pre.Rollback()
	if _, err := pre.ExecContext(ctx, `UPDATE fugue_platform_artifact_releases SET status='superseded' WHERE id='r1';
INSERT INTO fugue_platform_artifact_releases VALUES ('r2','next','policy_snapshot','platform-config-producer','shadow','policy_snapshot|platform-config-producer|shadow','gen2','active',2,'');
UPDATE fugue_platform_release_lanes SET active_release_id='r2',fencing_token=2,version=2 WHERE lane_key='policy_snapshot|platform-config-producer|shadow'`); err != nil {
		t.Fatal(err)
	}
	freeze := `UPDATE fugue_native_global_producer_gate SET frozen=TRUE,baseline_release_id=$1,
baseline_fence=$2,baseline_digest=$3,evidence_digest=$4 WHERE singleton`
	exec(admin, `SET lock_timeout='100ms'`)
	_, err = admin.ExecContext(ctx, freeze, "r1", 1, baseDigest, evidence)
	expectCode(err, "55P03") // in-flight writer has not drained
	if err := pre.Commit(); err != nil {
		t.Fatal(err)
	}
	_, err = admin.ExecContext(ctx, freeze, "r1", 1, baseDigest, evidence)
	expectCode(err, "40001") // actual selection changed while waiting
	// Old repeatable-read snapshot must also fail closed after the freeze,
	// not resume with the legacy mode observed before the transition.
	stale, err := writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer stale.Rollback()
	var fence int64
	if err := stale.QueryRowContext(ctx, `SELECT fencing_token FROM fugue_platform_release_lanes WHERE lane_key='policy_snapshot|platform-config-producer|shadow'`).Scan(&fence); err != nil || fence != 2 {
		t.Fatalf("unexpected fixture fence %d: %v", fence, err)
	}
	exec(admin, freeze, "r2", 2, newDigest, evidence)
	_, err = stale.ExecContext(ctx, `UPDATE fugue_platform_release_lanes SET fencing_token=3 WHERE lane_key='policy_snapshot|platform-config-producer|shadow'`)
	expectCode(err, "40001")
	if err := stale.Rollback(); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`UPDATE fugue_platform_release_lanes SET fencing_token=3 WHERE lane_key='policy_snapshot|platform-config-producer|shadow'`,
		`UPDATE fugue_platform_release_lanes SET scope_key='other' WHERE lane_key='policy_snapshot|platform-config-producer|shadow'`,
		`DELETE FROM fugue_platform_release_lanes WHERE lane_key='policy_snapshot|platform-config-producer|shadow'`,
		`INSERT INTO fugue_platform_release_lanes VALUES ('alt','policy_snapshot','platform-config-producer','full','r3',1,FALSE,1)`,
		`UPDATE fugue_platform_artifact_releases SET status='superseded' WHERE id='r2'`,
		`DELETE FROM fugue_platform_artifact_releases WHERE id='r2'`,
		`INSERT INTO fugue_platform_artifact_releases VALUES ('r3','base','policy_snapshot','platform-config-producer','shadow','policy_snapshot|platform-config-producer|shadow','gen1','active',3,'')`,
	} {
		_, err := writer.ExecContext(ctx, query)
		expectCode(err, "40001")
	}
	// Existing serving traffic and provider reads remain available; the
	// freeze is not a stop-all or rollback of the running traffic state.
	exec(writer, `UPDATE fugue_platform_release_lanes SET fencing_token=2 WHERE lane_key='traffic'`)
	var active string
	if err := writer.QueryRowContext(ctx, `SELECT active_release_id FROM fugue_platform_release_lanes WHERE lane_key='policy_snapshot|platform-config-producer|shadow'`).Scan(&active); err != nil || active != "r2" {
		t.Fatalf("freeze changed selected policy: %q %v", active, err)
	}
	for _, query := range []string{
		`UPDATE fugue_native_global_producer_gate SET frozen=FALSE`,
		`DELETE FROM fugue_native_global_producer_gate`,
		`TRUNCATE fugue_native_global_producer_gate`,
		`TRUNCATE fugue_platform_release_lanes`,
		`TRUNCATE fugue_platform_artifact_releases`,
	} {
		_, err := admin.ExecContext(ctx, query)
		expectCode(err, "40001")
	}
	exec(admin, GlobalProducerFreezeSchema) // rerunning bootstrap never reopens the gate
	var closed bool
	if err := admin.QueryRowContext(ctx, `SELECT frozen FROM fugue_native_global_producer_gate WHERE singleton`).Scan(&closed); err != nil || !closed {
		t.Fatalf("schema bootstrap reopened the producer gate: %v, %v", closed, err)
	}
	// Enroll a disposable journal and prove the provider validates the
	// separate login, claim and exact selected predecessor in the same DB.
	var existing bool
	if err := admin.QueryRowContext(ctx, `SELECT to_regnamespace('fugue_control') IS NOT NULL`).Scan(&existing); err != nil || existing {
		t.Fatalf("requires a disposable database without a journal: %v", err)
	}
	if err := productioncontrol.ApplySchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	createdJournal = true
	exec(admin, `CREATE ROLE fugue_gate_fixture_native LOGIN NOINHERIT`)
	createdNativeRole = true
	exec(admin, `GRANT USAGE ON SCHEMA fugue_gate_fixture TO fugue_gate_fixture_native;
GRANT SELECT,INSERT,UPDATE ON fugue_platform_release_lanes,fugue_platform_artifact_releases TO fugue_gate_fixture_native;
GRANT SELECT,INSERT ON fugue_native_global_producer_receipts TO fugue_gate_fixture_native;
GRANT SELECT,INSERT ON fugue_native_global_producer_receipts TO fugue_gate_fixture_writer`)
	public, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	j, err := productioncontrol.NewJournal(db, productioncontrol.Approvers{"owner": {ClusterID: "fixture", PublicKey: public}})
	if err != nil {
		t.Fatal(err)
	}
	w, err := productioncontrol.NewExecutionJournal(j, fixtureGateEvidence{})
	if err != nil {
		t.Fatal(err)
	}
	baselineEvidence := "sha256:" + strings.Repeat("e", 64)
	activationEvidence := "sha256:" + strings.Repeat("f", 64)
	scope := productioncontrol.Scope{ClusterID: "fixture", ResourceID: productioncontrol.GlobalProducerPolicyResource}
	if _, err := w.AnchorResource(ctx, "observer", productioncontrol.Baseline{Scope: scope, Verified: productioncontrol.Version{Generation: 2, Digest: newDigest}, EvidenceDigest: baselineEvidence}); err != nil {
		t.Fatal(err)
	}
	activate := `INSERT INTO fugue_native_global_producer_authority(cluster_id,writer_role,baseline_evidence_digest,activation_evidence_digest) VALUES('fixture',$1,$2,$3)`
	_, err = admin.ExecContext(ctx, activate, "fugue_gate_fixture_writer", baselineEvidence, activationEvidence)
	expectCode(err, "40001") // a group/non-login legacy role is not native authority
	exec(admin, activate, "fugue_gate_fixture_native", baselineEvidence, activationEvidence)

	// The real journal verifies the owner's signature. The provider receipt
	// separately derives its exact publication fields from that recorded claim.
	payload, err := json.Marshal(map[string]any{
		"schema":           "fugue.native-global-dns-policy/v1",
		"shadowDigest":     evidence,
		"previousProducer": map[string]any{"id": "next", "digest": newDigest, "generation": "gen2", "sequence": 2},
		"nextProducer":     map[string]any{"id": "target", "digest": targetDigest, "generation": "gen3", "sequence": 3},
		"precondition":     map[string]any{"operation": "edge_offline", "previous_policy": map[string]any{"artifact_id": "next", "content_hash": newDigest, "release_id": "r2", "fencing_token": 2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := productioncontrol.NewIntent("fixture", productioncontrol.GlobalProducerPolicyResource, "request", productioncontrol.DNSMembership, 2, newDigest, targetDigest, newDigest, payload)
	if err != nil {
		t.Fatal(err)
	}
	recorded, err := j.RecordIntent(ctx, "owner", intent, payload)
	if err != nil {
		t.Fatal(err)
	}
	approval, err := productioncontrol.SignApproval(intent, "owner", private, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	approved, err := j.RecordApproval(ctx, approval)
	if err != nil {
		t.Fatal(err)
	}
	claim, created, err := j.ReserveExecution(ctx, "native-writer", recorded.Digest, approved.Digest)
	if err != nil || !created {
		t.Fatalf("reserve claim: %v %v", created, err)
	}
	reserve := `INSERT INTO fugue_native_global_producer_receipts(intent_digest,approval_digest,claim_fence,cluster_id,idempotency_key,live_shadow_digest) VALUES ($1,$2,$3,$4,$5,$6)`
	key := "producer-reconfiguration/" + targetDigest
	_, err = writer.ExecContext(ctx, reserve, claim.IntentDigest, claim.ApprovalDigest, claim.Fence, claim.ClusterID, key, activationEvidence)
	expectCode(err, "40001") // old identity cannot spend even a real approved claim
	native, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	exec(native, `SET search_path TO fugue_gate_fixture`)
	exec(native, `SET SESSION AUTHORIZATION fugue_gate_fixture_native`)
	defer native.ExecContext(context.Background(), `RESET SESSION AUTHORIZATION`)
	_, err = native.ExecContext(ctx, `UPDATE fugue_platform_release_lanes SET fencing_token=3 WHERE lane_key='policy_snapshot|platform-config-producer|shadow'`)
	expectCode(err, "40001") // the native login alone is insufficient
	_, err = native.ExecContext(ctx, reserve, claim.IntentDigest, claim.ApprovalDigest, claim.Fence, claim.ClusterID, key, activationEvidence)
	expectCode(err, "40001") // a receipt without its exact mutation cannot commit
	tx, err := native.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, reserve, claim.IntentDigest, claim.ApprovalDigest, claim.Fence, claim.ClusterID, key, activationEvidence); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE fugue_platform_release_lanes SET fencing_token=3,version=3 WHERE lane_key='policy_snapshot|platform-config-producer|shadow';
UPDATE fugue_platform_artifact_releases SET status='superseded' WHERE id='r2'`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_platform_artifact_releases VALUES ('r3','target','policy_snapshot','platform-config-producer','shadow','policy_snapshot|platform-config-producer|shadow','gen3','active',3,$1)`, key); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE fugue_platform_release_lanes SET active_release_id='r3' WHERE lane_key='policy_snapshot|platform-config-producer|shadow'`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRowContext(ctx, `SELECT active_release_id,fencing_token FROM fugue_platform_release_lanes WHERE lane_key='policy_snapshot|platform-config-producer|shadow'`).Scan(&active, &fence); err != nil || active != "r3" || fence != 3 {
		t.Fatalf("wrong native selection: %q %d %v", active, fence, err)
	}
	var receiptCount int
	if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM fugue_native_global_producer_receipts WHERE intent_digest=$1 AND target_digest=$2`, claim.IntentDigest, targetDigest).Scan(&receiptCount); err != nil || receiptCount != 1 {
		t.Fatalf("wrong receipt: %d %v", receiptCount, err)
	}
	_, err = native.ExecContext(ctx, reserve, claim.IntentDigest, claim.ApprovalDigest, claim.Fence, claim.ClusterID, key, activationEvidence)
	expectCode(err, "40001") // predecessor already advanced; no second write
	_, err = writer.ExecContext(ctx, `UPDATE fugue_platform_release_lanes SET active_release_id='r2' WHERE lane_key='policy_snapshot|platform-config-producer|shadow'`)
	expectCode(err, "40001")
}

type fixtureGateEvidence struct{}

func (fixtureGateEvidence) VerifyBaseline(context.Context, productioncontrol.Baseline) error {
	return nil
}
func (fixtureGateEvidence) VerifyTerminal(context.Context, productioncontrol.Claim, productioncontrol.Intent, productioncontrol.ExecutionResult) error {
	return nil
}
