package store

import (
	"encoding/json"
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

func TestPostgresLatestMigrationLedgerArchiveIsBoundedPerOperation(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	createdAt := time.Now().UTC().Add(-48 * time.Hour)
	collectedAt := time.Now().UTC()
	ledger := model.AppMigrationLedger{
		ID:          "migration-latest",
		TenantID:    "tenant-archive",
		AppID:       "app-archive",
		OperationID: "op-archive",
		CreatedAt:   createdAt.Add(24 * time.Hour),
		UpdatedAt:   collectedAt,
	}
	payload, err := json.Marshal(ledger)
	if err != nil {
		t.Fatalf("marshal ledger: %v", err)
	}
	mock.ExpectQuery(`(?s)WITH latest AS \(.*SELECT DISTINCT ON \(operation_id\).*earliest AS \(.*MIN\(created_at\).*GROUP BY operation_id.*JOIN earliest USING \(operation_id\)`).
		WillReturnRows(sqlmock.NewRows([]string{"operation_id", "tenant_id", "app_id", "ledger_json", "collected_at", "created_at"}).
			AddRow(ledger.OperationID, ledger.TenantID, ledger.AppID, payload, collectedAt, createdAt))
	s := &Store{db: db, databaseURL: "postgres://test"}
	got, err := s.pgLatestAppMigrationLedgerArchiveByOperation()
	if err != nil {
		t.Fatalf("latest migration archive: %v", err)
	}
	archive, ok := got[ledger.OperationID]
	if !ok || archive.ledger.ID != ledger.ID || !archive.createdAt.Equal(createdAt) {
		t.Fatalf("unexpected bounded archive result: %+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("archive SQL mismatch: %v", err)
	}
}

func TestPostgresLatestMigrationLedgerArchiveFailsClosedOnMalformedPayload(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	createdAt := time.Now().UTC().Add(-48 * time.Hour)
	collectedAt := time.Now().UTC()
	mock.ExpectQuery(`(?s)WITH latest AS \(.*SELECT DISTINCT ON \(operation_id\).*JOIN earliest USING \(operation_id\)`).
		WillReturnRows(sqlmock.NewRows([]string{"operation_id", "tenant_id", "app_id", "ledger_json", "collected_at", "created_at"}).
			AddRow("op-malformed", "tenant-malformed", "app-malformed", []byte(`{"schema_version":[]}`), collectedAt, createdAt))
	s := &Store{db: db, databaseURL: "postgres://test"}
	got, err := s.pgLatestAppMigrationLedgerArchiveByOperation()
	if err != nil {
		t.Fatalf("latest migration archive: %v", err)
	}
	archive, ok := got["op-malformed"]
	if !ok || archive.ledger.AppID != "app-malformed" ||
		archive.ledger.CutoverStatus != model.AppMigrationCutoverBlocked ||
		!archive.ledger.OldArtifactsProtected {
		t.Fatalf("malformed archive did not fail closed: %+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("archive SQL mismatch: %v", err)
	}
}

func TestPostgresLatestMigrationLedgerForOperationUsesBoundedNewestFirstQuery(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	collectedAt := time.Now().UTC()
	ledger := model.AppMigrationLedger{
		ID: "migration-valid", TenantID: "tenant-archive", AppID: "app-archive", OperationID: "op-archive",
	}
	payload, err := json.Marshal(ledger)
	if err != nil {
		t.Fatalf("marshal ledger: %v", err)
	}
	mock.ExpectQuery(`(?s)FROM fugue_app_migration_ledgers.*WHERE operation_id = \$1.*ORDER BY collected_at DESC, id DESC.*LIMIT 1000`).
		WithArgs(ledger.OperationID).
		WillReturnRows(sqlmock.NewRows([]string{"ledger_json", "collected_at"}).
			AddRow([]byte(`{"schema_version":[]}`), collectedAt.Add(time.Second)).
			AddRow(payload, collectedAt))
	s := &Store{db: db, databaseURL: "postgres://test"}
	got, found, err := s.LatestAppMigrationLedger(ledger.OperationID)
	if err != nil || !found || got.ID != ledger.ID {
		t.Fatalf("bounded latest migration ledger: found=%v ledger=%+v err=%v", found, got, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("latest operation ledger SQL mismatch: %v", err)
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
