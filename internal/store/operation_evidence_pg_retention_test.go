package store

import (
	"context"
	"testing"
	"time"

	"fugue/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresOrdinaryEvidencePrunePreservesNinetyDayMigrationLedger(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock database: %v", err)
	}
	defer db.Close()
	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}

	for _, scope := range []struct {
		value string
		limit int
	}{
		{value: "op-retention", limit: operationEvidenceRetentionLimitPerOperation},
		{value: "app-retention", limit: operationEvidenceRetentionLimitPerApp},
	} {
		mock.ExpectExec(`(?s)DELETE FROM fugue_operation_evidence.*evidence_type IN`).
			WithArgs(scope.value, sqlmock.AnyArg(), scope.limit,
				model.OperationEvidenceTypeMigrationStarted,
				model.OperationEvidenceTypeMigrationCompleted,
				model.OperationEvidenceTypeMigrationFailed).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`(?s)DELETE FROM fugue_operation_evidence.*evidence_type NOT IN`).
			WithArgs(scope.value, sqlmock.AnyArg(), scope.limit,
				model.OperationEvidenceTypeMigrationStarted,
				model.OperationEvidenceTypeMigrationCompleted,
				model.OperationEvidenceTypeMigrationFailed).
			WillReturnResult(sqlmock.NewResult(0, 0))
	}

	if err := pruneOperationEvidenceTx(context.Background(), tx, model.OperationEvidence{
		TenantID:    "tenant-retention",
		AppID:       "app-retention",
		OperationID: "op-retention",
		Type:        model.OperationEvidenceTypeRolloutProgress,
		CollectedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("prune ordinary evidence: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ordinary evidence did not use migration-aware retention SQL: %v", err)
	}
	_ = tx.Rollback()
}
