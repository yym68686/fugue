package store

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"testing"

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

func TestPostgresSchemaIncludesOperationLookupIndexes(t *testing.T) {
	t.Parallel()

	schema := strings.Join(postgresSchemaStatements, "\n")
	for _, indexName := range []string{
		"idx_fugue_operations_app_created_at",
		"idx_fugue_operations_tenant_created_at",
	} {
		if !strings.Contains(schema, indexName) {
			t.Fatalf("postgres schema is missing %s", indexName)
		}
	}
}
