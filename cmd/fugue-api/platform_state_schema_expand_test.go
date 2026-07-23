package main

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

const platformStateSchemaInspectQueryPattern = `(?s)SELECT attribute\.attname,.*WHERE attribute\.attrelid = to_regclass\('fugue_platform_consumer_instances'\)::oid`

func exactPlatformStateSchemaRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}).
		AddRow("observation_evidence_hash", "text", "NO", "''::text").
		AddRow("observation_window_started_at", "timestamp with time zone", "YES", "").
		AddRow("observation_window_heartbeat_count", "bigint", "NO", "0")
}

func TestPlatformStateSchemaExpandLive(t *testing.T) {
	databaseURL := os.Getenv("FUGUE_SCHEMA_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set FUGUE_SCHEMA_TEST_DATABASE_URL to run the live schema expand test")
	}
	if err := runPlatformStateSchemaExpand(context.Background(), databaseURL); err != nil {
		t.Fatalf("first schema expand: %v", err)
	}
	if err := runPlatformStateSchemaExpand(context.Background(), databaseURL); err != nil {
		t.Fatalf("idempotent schema expand: %v", err)
	}
}

func TestPlatformStateSchemaExpandRequiredForConfiguredPostgresStore(t *testing.T) {
	for _, databaseURL := range []string{
		"postgresql://user:password@db.example:5432/fugue?sslmode=require",
		" host=db.example port=5432 user=fugue dbname=fugue ",
	} {
		if !platformStateSchemaExpandRequired(databaseURL) {
			t.Fatalf("configured PostgreSQL store %q did not require schema expand", databaseURL)
		}
	}
	for _, databaseURL := range []string{"", " \t\n "} {
		if platformStateSchemaExpandRequired(databaseURL) {
			t.Fatalf("file store %q unexpectedly required PostgreSQL schema expand", databaseURL)
		}
	}
}

func TestRunPlatformStateSchemaExpandRejectsNonPostgres(t *testing.T) {
	if err := runPlatformStateSchemaExpand(context.Background(), "external"); err == nil {
		t.Fatal("non-PostgreSQL database URL was accepted")
	}
}

func TestNormalizePlatformStateDatabaseURLMatchesStoreDSNCompatibility(t *testing.T) {
	for _, input := range []string{
		"  postgresql://user:password@db.example:5432/fugue?sslmode=require\n",
		"\thost=db.example port=5432 user=fugue password=secret dbname=fugue sslmode=require  ",
	} {
		got, err := normalizePlatformStateDatabaseURL(input)
		if err != nil {
			t.Fatalf("normalize %q: %v", input, err)
		}
		if got != strings.TrimSpace(input) {
			t.Fatalf("normalized DSN = %q, want trimmed input", got)
		}
	}
}

func TestWaitForPlatformStateBootstrapTable(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT to_regclass('fugue_platform_consumer_instances')::oid`)).
		WillReturnRows(sqlmock.NewRows([]string{"oid"}).AddRow(int64(42)))
	if err := waitForPlatformStateBootstrapTable(context.Background(), database, time.Millisecond); err != nil {
		t.Fatalf("waitForPlatformStateBootstrapTable: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestApplyPlatformStateSchemaExpandIsLockedAndTransactional(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`SELECT set_config('lock_timeout', $1, true)`)).
		WithArgs(platformStateSchemaExpandLockLimit.String()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock($1)`)).
		WithArgs(platformStateSchemaExpandLockID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(platformStateSchemaInspectQueryPattern).
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}))
	mock.ExpectExec(regexp.QuoteMeta(platformStateSchemaExpandSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(platformStateSchemaInspectQueryPattern).WillReturnRows(exactPlatformStateSchemaRows())
	mock.ExpectCommit()
	if err := applyPlatformStateSchemaExpand(context.Background(), database); err != nil {
		t.Fatalf("applyPlatformStateSchemaExpand: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestApplyPlatformStateSchemaExpandSkipsDDLWhenAlreadyExact(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`SELECT set_config('lock_timeout', $1, true)`)).
		WithArgs(platformStateSchemaExpandLockLimit.String()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock($1)`)).
		WithArgs(platformStateSchemaExpandLockID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(platformStateSchemaInspectQueryPattern).WillReturnRows(exactPlatformStateSchemaRows())
	mock.ExpectCommit()
	if err := applyPlatformStateSchemaExpand(context.Background(), database); err != nil {
		t.Fatalf("applyPlatformStateSchemaExpand: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestApplyPlatformStateSchemaExpandRejectsWrongExistingShapeBeforeDDL(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`SELECT set_config('lock_timeout', $1, true)`)).
		WithArgs(platformStateSchemaExpandLockLimit.String()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock($1)`)).
		WithArgs(platformStateSchemaExpandLockID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(platformStateSchemaInspectQueryPattern).WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}).
			AddRow("observation_evidence_hash", "text", "YES", "''::text"),
	)
	mock.ExpectRollback()
	if err := applyPlatformStateSchemaExpand(context.Background(), database); err == nil {
		t.Fatal("wrong existing schema shape was accepted")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyPlatformStateSchemaExpandRequiresExactShapes(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mock.ExpectQuery(platformStateSchemaInspectQueryPattern).WillReturnRows(exactPlatformStateSchemaRows())
	if err := verifyPlatformStateSchemaExpand(context.Background(), database); err != nil {
		t.Fatalf("verifyPlatformStateSchemaExpand: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
