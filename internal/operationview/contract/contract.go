// Package contract declares the read capability shared by the operation
// projection and its Store adapter. It does not depend on projection policy.
package contract

import (
	"context"
	"time"

	"fugue/internal/model"
)

type Filter struct {
	TenantID  string
	AppID     string
	ProjectID string
	ServiceID string
	ActorID   string
	Types     []string
	Statuses  []string
	Limit     int
}

type Repository interface {
	GetOperation(string) (model.Operation, error)
	GetApp(string) (model.App, error)
	ListOperationEvidence(model.OperationEvidenceFilter) ([]model.OperationEvidence, error)
	FirstDeployExecutionStarts(context.Context, []string) (map[string]time.Time, error)
	ListOperationsFiltered(string, bool, Filter) ([]model.Operation, error)
	ListOperationSummariesFiltered(string, bool, Filter) ([]model.Operation, error)
}
