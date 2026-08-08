package schemamigrate

import (
	"context"
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

const platformStateInspectQueryPattern = `(?s)SELECT attribute\.attname,.*WHERE attribute\.attrelid = to_regclass\('fugue_platform_consumer_instances'\)::oid`

func exactPlatformStateRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}).
		AddRow("observation_evidence_hash", "text", "NO", "''::text").
		AddRow("observation_window_started_at", "timestamp with time zone", "YES", "").
		AddRow("observation_window_heartbeat_count", "bigint", "NO", "0")
}

func TestPlatformStateMigrationLive(t *testing.T) {
	databaseURL := os.Getenv("FUGUE_SCHEMA_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set FUGUE_SCHEMA_TEST_DATABASE_URL to run the live schema migration test")
	}
	if err := MigratePlatformState(context.Background(), databaseURL); err != nil {
		t.Fatalf("first schema migration: %v", err)
	}
	if err := MigratePlatformState(context.Background(), databaseURL); err != nil {
		t.Fatalf("idempotent schema migration: %v", err)
	}
}

func TestNormalizeDatabaseURLMatchesStoreDSNCompatibility(t *testing.T) {
	for _, input := range []string{
		"  postgresql://user:password@db.example:5432/fugue?sslmode=require\n",
		"\thost=db.example port=5432 user=fugue password=secret dbname=fugue sslmode=require  ",
	} {
		got, err := normalizeDatabaseURL(input)
		if err != nil {
			t.Fatalf("normalize %q: %v", input, err)
		}
		if got != strings.TrimSpace(input) {
			t.Fatalf("normalized DSN = %q, want trimmed input", got)
		}
	}
	for _, input := range []string{"", "   ", "://invalid"} {
		if _, err := normalizeDatabaseURL(input); err == nil {
			t.Fatalf("invalid database URL %q was accepted", input)
		}
	}
}

func TestWaitForPlatformStateTable(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT to_regclass('fugue_platform_consumer_instances')::oid`)).
		WillReturnRows(sqlmock.NewRows([]string{"oid"}).AddRow(int64(42)))
	if err := waitForPlatformStateTable(context.Background(), database, time.Millisecond); err != nil {
		t.Fatalf("waitForPlatformStateTable: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestApplyPlatformStateIsLockedTransactionalAndIdempotent(t *testing.T) {
	for _, tc := range []struct {
		name     string
		existing *sqlmock.Rows
		apply    bool
	}{
		{name: "missing", existing: sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}), apply: true},
		{name: "exact", existing: exactPlatformStateRows()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			mock.ExpectBegin()
			mock.ExpectExec(regexp.QuoteMeta(`SELECT set_config('lock_timeout', $1, true)`)).
				WithArgs(platformStateLockLimit.String()).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock($1)`)).
				WithArgs(platformStateLockID).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery(platformStateInspectQueryPattern).WillReturnRows(tc.existing)
			if tc.apply {
				mock.ExpectExec(regexp.QuoteMeta(platformStateSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery(platformStateInspectQueryPattern).WillReturnRows(exactPlatformStateRows())
			}
			mock.ExpectCommit()
			if err := applyPlatformState(context.Background(), database); err != nil {
				t.Fatalf("applyPlatformState: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestVerifyPlatformStateReportsMissingMigrationWithoutDDL(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mock.ExpectQuery(platformStateInspectQueryPattern).
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}))
	err = verifyPlatformState(context.Background(), database)
	if !errors.Is(err, ErrPlatformStateMigrationRequired) {
		t.Fatalf("verifyPlatformState error = %v, want migration required", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestApplyPlatformStateRejectsWrongExistingShape(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`SELECT set_config('lock_timeout', $1, true)`)).
		WithArgs(platformStateLockLimit.String()).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock($1)`)).
		WithArgs(platformStateLockID).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(platformStateInspectQueryPattern).WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}).
			AddRow("observation_evidence_hash", "text", "YES", "''::text"),
	)
	mock.ExpectRollback()
	if err := applyPlatformState(context.Background(), database); err == nil {
		t.Fatal("wrong existing schema shape was accepted")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
