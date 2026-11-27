package store

import (
	"context"
	"time"

	"fugue/internal/model"
	"fugue/internal/operationview/contract"
)

// OperationViews hands out only the Store-owned read capabilities. Do not
// embed Store or couple this adapter to a consumer's interface method set.
func (s *Store) OperationViews() operationViews {
	return operationViews{store: s}
}

type operationViews struct{ store *Store }

func (r operationViews) GetOperation(id string) (model.Operation, error) {
	return r.store.GetOperation(id)
}

func (r operationViews) GetApp(id string) (model.App, error) {
	return r.store.GetApp(id)
}

func (r operationViews) ListOperationEvidence(filter model.OperationEvidenceFilter) ([]model.OperationEvidence, error) {
	return r.store.ListOperationEvidence(filter)
}

func (r operationViews) FirstDeployExecutionStarts(ctx context.Context, ids []string) (map[string]time.Time, error) {
	return r.store.FirstDeployExecutionStarts(ctx, ids)
}

func (r operationViews) ListOperationsFiltered(tenantID string, platformAdmin bool, filter contract.Filter) ([]model.Operation, error) {
	return r.store.ListOperationsFiltered(tenantID, platformAdmin, filter)
}

func (r operationViews) ListOperationSummariesFiltered(tenantID string, platformAdmin bool, filter contract.Filter) ([]model.Operation, error) {
	return r.store.ListOperationSummariesFiltered(tenantID, platformAdmin, filter)
}
