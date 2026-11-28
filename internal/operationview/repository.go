package operationview

import (
	"context"
	"time"

	"fugue/internal/model"
)

// Repository is the read capability consumed by operation projections.
type Repository interface {
	GetOperation(string) (model.Operation, error)
	GetApp(string) (model.App, error)
	ListOperationEvidence(model.OperationEvidenceFilter) ([]model.OperationEvidence, error)
	FirstDeployExecutionStarts(context.Context, []string) (map[string]time.Time, error)
	ListOperationsFiltered(string, bool, Filter) ([]model.Operation, error)
	ListOperationSummariesFiltered(string, bool, Filter) ([]model.Operation, error)
}
