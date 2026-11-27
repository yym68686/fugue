package providergate

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

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
	createdRole := false
	t.Cleanup(func() {
		if _, err := db.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS fugue_gate_fixture CASCADE`); err != nil {
			t.Logf("fixture cleanup: %v", err)
		}
		if createdRole {
			if _, err := db.ExecContext(context.Background(), `DROP ROLE fugue_gate_fixture_writer`); err != nil {
				t.Logf("fixture role cleanup: %v", err)
			}
		}
	})
	exec(admin, `SET search_path TO fugue_gate_fixture`)
	exec(admin, `CREATE TABLE fugue_platform_release_lanes (
  lane_key text PRIMARY KEY, artifact_kind text NOT NULL, scope_key text NOT NULL,
  release_channel text NOT NULL, active_release_id text NOT NULL, fencing_token bigint NOT NULL,
  frozen boolean NOT NULL DEFAULT FALSE);
CREATE TABLE fugue_platform_artifact_releases (
  id text PRIMARY KEY, artifact_id text NOT NULL, artifact_kind text NOT NULL, scope_key text NOT NULL,
  release_channel text NOT NULL, lane_key text NOT NULL, generation text NOT NULL,
  status text NOT NULL, fencing_token bigint NOT NULL);
CREATE TABLE fugue_platform_artifacts (
  id text PRIMARY KEY, generation text NOT NULL, artifact_kind text NOT NULL,
  scope_key text NOT NULL, status text NOT NULL, content_hash text NOT NULL);`)
	exec(admin, GlobalProducerFreezeSchema)
	baseDigest := "sha256:" + strings.Repeat("a", 64)
	newDigest := "sha256:" + strings.Repeat("b", 64)
	evidence := "sha256:" + strings.Repeat("c", 64)
	exec(admin, `INSERT INTO fugue_platform_artifacts VALUES
  ('base','gen1','policy_snapshot','platform-config-producer','validated',$1),
  ('next','gen2','policy_snapshot','platform-config-producer','validated',$2)`, baseDigest, newDigest)
	exec(admin, `INSERT INTO fugue_platform_artifact_releases VALUES
  ('r1','base','policy_snapshot','platform-config-producer','shadow','producer','gen1','active',1)`)
	exec(admin, `INSERT INTO fugue_platform_release_lanes VALUES
  ('producer','policy_snapshot','platform-config-producer','shadow','r1',1,FALSE),
  ('traffic','release_set','global','full','traffic-r1',1,FALSE)`)
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
INSERT INTO fugue_platform_artifact_releases VALUES ('r2','next','policy_snapshot','platform-config-producer','shadow','producer','gen2','active',2);
UPDATE fugue_platform_release_lanes SET active_release_id='r2',fencing_token=2 WHERE lane_key='producer'`); err != nil {
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
	if err := stale.QueryRowContext(ctx, `SELECT fencing_token FROM fugue_platform_release_lanes WHERE lane_key='producer'`).Scan(&fence); err != nil || fence != 2 {
		t.Fatalf("unexpected fixture fence %d: %v", fence, err)
	}
	exec(admin, freeze, "r2", 2, newDigest, evidence)
	_, err = stale.ExecContext(ctx, `UPDATE fugue_platform_release_lanes SET fencing_token=3 WHERE lane_key='producer'`)
	expectCode(err, "40001")
	if err := stale.Rollback(); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`UPDATE fugue_platform_release_lanes SET fencing_token=3 WHERE lane_key='producer'`,
		`UPDATE fugue_platform_release_lanes SET scope_key='other' WHERE lane_key='producer'`,
		`DELETE FROM fugue_platform_release_lanes WHERE lane_key='producer'`,
		`INSERT INTO fugue_platform_release_lanes VALUES ('alt','policy_snapshot','platform-config-producer','full','r3',1,FALSE)`,
		`UPDATE fugue_platform_artifact_releases SET status='superseded' WHERE id='r2'`,
		`DELETE FROM fugue_platform_artifact_releases WHERE id='r2'`,
		`INSERT INTO fugue_platform_artifact_releases VALUES ('r3','base','policy_snapshot','platform-config-producer','shadow','producer','gen1','active',3)`,
	} {
		_, err := writer.ExecContext(ctx, query)
		expectCode(err, "40001")
	}
	// Existing serving traffic and provider reads remain available; the
	// freeze is not a stop-all or rollback of the running traffic state.
	exec(writer, `UPDATE fugue_platform_release_lanes SET fencing_token=2 WHERE lane_key='traffic'`)
	var active string
	if err := writer.QueryRowContext(ctx, `SELECT active_release_id FROM fugue_platform_release_lanes WHERE lane_key='producer'`).Scan(&active); err != nil || active != "r2" {
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
}
