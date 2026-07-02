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
