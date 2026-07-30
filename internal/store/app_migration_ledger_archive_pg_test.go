package store

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresMigrationLedgerArchiveHasNoParentForeignKey(t *testing.T) {
	t.Parallel()
	found := false
	for _, statement := range postgresSchemaStatements {
		if !containsAllStoreStrings(statement, "CREATE TABLE IF NOT EXISTS fugue_app_migration_ledgers", "ledger_json JSONB") {
			continue
		}
		found = true
		if containsAllStoreStrings(statement, "REFERENCES fugue_apps") ||
			containsAllStoreStrings(statement, "REFERENCES fugue_operations") ||
			containsAllStoreStrings(statement, "REFERENCES fugue_tenants") {
			t.Fatalf("migration archive must not cascade with product parents: %s", statement)
		}
	}
	if !found {
		t.Fatal("migration ledger archive schema statement is missing")
	}
}

func TestPostgresRecordsMigrationLedgerArchiveBeforeRetentionPrune(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	now := time.Now().UTC()
	ledger := model.NormalizeAppMigrationLedger(model.AppMigrationLedger{
		ID: "migration-archive", TenantID: "tenant-archive", AppID: "app-archive", OperationID: "op-archive",
		NewRuntimeID: "runtime-new", NewClusterID: "cluster-new", ObservedAt: now,
	}, now)
	mock.ExpectBegin()
	mock.ExpectExec(`(?s)INSERT INTO fugue_app_migration_ledgers`).
		WithArgs(
			ledger.ID, ledger.TenantID, ledger.ProjectID, ledger.AppID, ledger.OperationID,
			ledger.ObservedAt, ledger.UpdatedAt, ledger.RetainUntil, sqlmock.AnyArg(), ledger.CreatedAt,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`(?s)DELETE FROM fugue_app_migration_ledgers.*retain_until`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	s := &Store{db: db, databaseURL: "postgres://test"}
	if err := s.pgRecordAppMigrationLedgerArchive(ledger); err != nil {
		t.Fatalf("record migration archive: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("archive SQL mismatch: %v", err)
	}
}

func containsAllStoreStrings(value string, needles ...string) bool {
	for _, needle := range needles {
		if !strings.Contains(value, needle) {
			return false
		}
	}
	return true
}
