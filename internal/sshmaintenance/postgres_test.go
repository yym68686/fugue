package sshmaintenance

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresRewrapPreservesIdentityAndRequiresExactPlan(test *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_SSH_MAINTENANCE_DATABASE_URL")
	if dsn == "" {
		test.Skip("requires explicit disposable loopback identity-maintenance database")
	}
	address, err := url.Parse(dsn)
	if err != nil || address.Hostname() != "127.0.0.1" || address.Path != "/fugue_test_ssh_maintenance" {
		test.Fatal("maintenance test requires an isolated loopback database")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		test.Fatal(err)
	}
	defer database.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var present sql.NullString
	if err := database.QueryRowContext(ctx, `SELECT to_regclass('public.fugue_app_ssh_identities')::text`).Scan(&present); err != nil || present.Valid {
		test.Fatal("test database must have no preexisting identity inventory")
	}
	if _, err := database.ExecContext(ctx, `CREATE TABLE fugue_app_ssh_identity_anchors(app_id text PRIMARY KEY,tenant_id text NOT NULL,identity_id text UNIQUE NOT NULL,public_body jsonb NOT NULL);
CREATE TABLE fugue_app_ssh_identities(app_id text PRIMARY KEY,tenant_id text NOT NULL,identity_id text UNIQUE NOT NULL,body jsonb NOT NULL);
CREATE TABLE fugue_audit_events(id text PRIMARY KEY,tenant_id text,actor_type text NOT NULL,actor_id text NOT NULL,action text NOT NULL,target_type text NOT NULL,target_id text NOT NULL,metadata_json jsonb,created_at timestamptz NOT NULL)`); err != nil {
		test.Fatal(err)
	}
	defer database.Exec(`DROP TABLE fugue_app_ssh_identities,fugue_app_ssh_identity_anchors,fugue_audit_events`)
	backup, keyring := fixture(test)
	retained := backup.Identities[0]
	anchor, _ := json.Marshal(retained.Anchor)
	record, _ := json.Marshal(retained.Record)
	if _, err := database.ExecContext(ctx, `INSERT INTO fugue_app_ssh_identity_anchors VALUES ($1,$2,$3,$4::jsonb)`, retained.Anchor.AppID, retained.Anchor.TenantID, retained.Anchor.ID, string(anchor)); err != nil {
		test.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `INSERT INTO fugue_app_ssh_identities VALUES ($1,$2,$3,$4::jsonb)`, retained.Anchor.AppID, retained.Anchor.TenantID, retained.Anchor.ID, string(record)); err != nil {
		test.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	transaction, err := database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		test.Fatal(err)
	}
	current, err := ReadInventory(ctx, transaction, keyring)
	transaction.Rollback()
	if err != nil {
		test.Fatal(err)
	}
	plan, err := BuildPlan(current, backup, keyring, digest)
	if err != nil {
		test.Fatal(err)
	}
	wrong := plan
	wrong.Entries = append([]Entry{}, plan.Entries...)
	wrong.Entries[0].Identity.TenantID = "foreign"
	if _, err := Apply(ctx, database, keyring, digest, backup, wrong, "fixture-operator"); err == nil {
		test.Fatal("changed review plan was accepted")
	}
	if changed, err := Apply(ctx, database, keyring, digest, backup, plan, "fixture-operator"); err != nil || changed != 1 {
		test.Fatal("valid storage rewrap did not commit", changed, err)
	}
	var updatedRaw, anchorRaw []byte
	if err := database.QueryRowContext(ctx, `SELECT body FROM fugue_app_ssh_identities`).Scan(&updatedRaw); err != nil {
		test.Fatal(err)
	}
	if err := database.QueryRowContext(ctx, `SELECT public_body FROM fugue_app_ssh_identity_anchors`).Scan(&anchorRaw); err != nil {
		test.Fatal(err)
	}
	var updated model.AppSSHIdentityRecord
	var unchanged model.AppSSHIdentity
	if Decode(updatedRaw, &updated) != nil || Decode(anchorRaw, &unchanged) != nil || updated.Identity != retained.Anchor || unchanged != retained.Anchor || updated.EncryptionKeyID != "storage-new" || updated.Ciphertext == retained.Record.Ciphertext {
		test.Fatal("rewrap changed logical identity or retained old encryption")
	}
	if _, err := keyring.Signer(updated); err != nil {
		test.Fatal("rewrapped record cannot be decrypted")
	}
	if _, err := Apply(ctx, database, keyring, digest, backup, plan, "fixture-operator"); err == nil {
		test.Fatal("stale plan was silently replayed")
	}
	var auditCount int
	if err := database.QueryRowContext(ctx, `SELECT count(*) FROM fugue_audit_events WHERE action='ssh.identity.rewrapped' AND actor_id='fixture-operator'`).Scan(&auditCount); err != nil || auditCount != 1 {
		test.Fatal("rewrap lacks its transactionally bound audit", auditCount, err)
	}
	if _, err := database.ExecContext(ctx, `DELETE FROM fugue_app_ssh_identities`); err != nil {
		test.Fatal(err)
	}
	transaction, err = database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		test.Fatal(err)
	}
	_, err = ReadInventory(ctx, transaction, keyring)
	transaction.Rollback()
	if err == nil {
		test.Fatal("missing encrypted identity with retained ownership was accepted")
	}
}
