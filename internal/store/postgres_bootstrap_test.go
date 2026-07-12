package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestApplyPostgresSchemaTxSkipsDDLWhenFingerprintMatches(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()

	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta(postgresMetaSchemaStatement)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT value FROM fugue_meta WHERE key = $1`)).
		WithArgs(postgresSchemaFingerprintMetaKey).
		WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(postgresSchemaFingerprint()))

	applied, err := s.applyPostgresSchemaTx(context.Background(), tx)
	if err != nil {
		t.Fatalf("applyPostgresSchemaTx: %v", err)
	}
	if applied {
		t.Fatal("expected schema bootstrap to skip DDL when fingerprint matches")
	}

	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestApplyPostgresSchemaTxAppliesDDLAndStoresFingerprintWhenMissing(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()

	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta(postgresMetaSchemaStatement)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT value FROM fugue_meta WHERE key = $1`)).
		WithArgs(postgresSchemaFingerprintMetaKey).
		WillReturnRows(sqlmock.NewRows([]string{"value"}))
	mock.ExpectExec(regexp.QuoteMeta(`SELECT set_config('lock_timeout', $1, true)`)).
		WithArgs(formatPostgresDuration(postgresBootstrapLockTimeout)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	for _, stmt := range postgresSchemaStatements[1:] {
		mock.ExpectExec(regexp.QuoteMeta(stmt)).
			WillReturnResult(sqlmock.NewResult(0, 0))
	}
	mock.ExpectExec(regexp.QuoteMeta(`
INSERT INTO fugue_meta (key, value, updated_at)
VALUES ($1, $2, NOW())
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value, updated_at = NOW()
`)).
		WithArgs(postgresSchemaFingerprintMetaKey, postgresSchemaFingerprint()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	applied, err := s.applyPostgresSchemaTx(context.Background(), tx)
	if err != nil {
		t.Fatalf("applyPostgresSchemaTx: %v", err)
	}
	if !applied {
		t.Fatal("expected schema bootstrap to apply DDL when fingerprint is missing")
	}

	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestIsRetryableBootstrapError(t *testing.T) {
	t.Parallel()

	retryable := fmt.Errorf("wrap: %w", &pgconn.PgError{Code: "40P01"})
	if !isRetryableBootstrapError(retryable) {
		t.Fatal("expected deadlock to be retryable during bootstrap")
	}

	nonRetryable := fmt.Errorf("wrap: %w", &pgconn.PgError{Code: "23505"})
	if isRetryableBootstrapError(nonRetryable) {
		t.Fatal("expected unique violation to stay non-retryable during bootstrap")
	}

	if isRetryableBootstrapError(sql.ErrNoRows) {
		t.Fatal("expected non-postgres errors to stay non-retryable")
	}
}

func TestPostgresBootstrapTimeoutCoversRollingStartupContention(t *testing.T) {
	t.Parallel()

	if postgresBootstrapTimeout < 5*time.Minute {
		t.Fatalf("postgres bootstrap timeout should cover rolling startup lock contention, got %s", postgresBootstrapTimeout)
	}
	if postgresPingTimeout+postgresBootstrapTimeout > 6*time.Minute {
		t.Fatalf("postgres bootstrap and ping timeout should fit within the default API startup probe window, got ping=%s bootstrap=%s", postgresPingTimeout, postgresBootstrapTimeout)
	}
}

func TestPostgresSchemaIncludesOperationLookupIndexes(t *testing.T) {
	t.Parallel()

	schema := strings.Join(postgresSchemaStatements, "\n")
	for _, indexName := range []string{
		"idx_fugue_operations_app_created_at",
		"idx_fugue_operations_app_status",
		"idx_fugue_operations_tenant_created_at",
		"idx_fugue_operations_tenant_app_status",
		"idx_fugue_operations_oom_right_sizing_event",
	} {
		if !strings.Contains(schema, indexName) {
			t.Fatalf("postgres schema is missing %s", indexName)
		}
	}
}

func TestPostgresSchemaIncludesBoundedAppPaginationIndexes(t *testing.T) {
	t.Parallel()

	schema := strings.Join(postgresSchemaStatements, "\n")
	for _, indexName := range []string{
		"idx_fugue_apps_created_id_desc",
		"idx_fugue_apps_updated_id_desc",
		"idx_fugue_apps_name_id_ci",
		"idx_fugue_apps_tenant_project_created_id_desc",
		"idx_fugue_apps_phase_created_id_desc",
		"idx_fugue_app_domains_verified_app_hostname_ci",
	} {
		if !strings.Contains(schema, indexName) {
			t.Fatalf("postgres schema is missing pagination index %s", indexName)
		}
	}
}

func TestPostgresSchemaIncludesExpectedConsumerSetPersistence(t *testing.T) {
	t.Parallel()

	schema := strings.Join(postgresSchemaStatements, "\n")
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS fugue_platform_expected_consumer_sets",
		"idx_fugue_platform_expected_consumers_release_revision",
		"idx_fugue_platform_expected_consumers_artifact_release",
		"idx_fugue_platform_expected_consumers_kind_scope",
	} {
		if !strings.Contains(schema, required) {
			t.Fatalf("postgres schema is missing %s", required)
		}
	}
}

func TestPostgresSchemaIncludesPlatformConsumerHeartbeatEnvelope(t *testing.T) {
	t.Parallel()

	schema := strings.Join(postgresSchemaStatements, "\n")
	for _, required := range []string{
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS credential_id",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS release_set_id",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS expected_consumer_set_id",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS fencing_token",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS protocol_version",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS schema_version",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS sequence",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS issued_at",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS nonce",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS generation_sequence",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS evidence_hash",
		"ALTER TABLE fugue_platform_consumer_instances ADD COLUMN IF NOT EXISTS identity_verified",
		"idx_fugue_platform_consumers_release_set",
	} {
		if !strings.Contains(schema, required) {
			t.Fatalf("postgres schema is missing %s", required)
		}
	}
}

func TestPostgresSchemaIncludesPlatformLKGHistoryRetention(t *testing.T) {
	t.Parallel()

	schema := strings.Join(postgresSchemaStatements, "\n")
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS fugue_platform_lkg_snapshot_history",
		"DROP CONSTRAINT IF EXISTS fugue_platform_lkg_snapshot_h_artifact_kind_scope_key_gener_key",
		"idx_fugue_platform_lkg_history_kind_scope_sequence",
		"idx_fugue_platform_lkg_history_kind_scope_verified",
		"FROM fugue_platform_lkg_snapshots",
		"ON CONFLICT (id) DO NOTHING",
	} {
		if !strings.Contains(schema, required) {
			t.Fatalf("postgres schema is missing %s", required)
		}
	}
	if strings.Contains(schema, "UNIQUE (artifact_kind, scope_key, generation)") {
		t.Fatal("PostgreSQL LKG history must allow a rollback generation to be re-verified as a new immutable event")
	}
}

func TestPostgresSchemaAppliesToLiveTestDatabase(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL to run live Postgres schema integration test")
	}
	if !strings.Contains(databaseURL, "fugue-pgtest") && !strings.Contains(databaseURL, "fugue_test") {
		t.Fatalf("refusing to run schema integration test against non-test database URL %q", databaseURL)
	}
	s := New("", databaseURL)
	if err := s.Init(); err != nil {
		t.Fatalf("init store against live test database: %v", err)
	}
}
