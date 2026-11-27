// Package operationview owns authorization and read-side projection of
// durable operations. It cannot mutate an operation or its associated app.
package operationview

import (
	"context"
	"errors"
	"strings"
	"time"

	"fugue/internal/model"
)

var ErrNotVisible = errors.New("operation is not visible to this principal")

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

func List(reads Repository, principal model.Principal, filter Filter, includeDesired bool) ([]model.Operation, error) {
	if !principal.IsPlatformAdmin() && filter.TenantID != "" && filter.TenantID != principal.TenantID {
		return nil, ErrNotVisible
	}
	project, err := ScopeProject(principal, filter.ProjectID)
	if err != nil {
		return nil, err
	}
	filter.ProjectID = project
	if includeDesired {
		return reads.ListOperationsFiltered(principal.TenantID, principal.IsPlatformAdmin(), filter)
	}
	return reads.ListOperationSummariesFiltered(principal.TenantID, principal.IsPlatformAdmin(), filter)
}

// LoadAuthorized is the shared authorization boundary for every
// operation-derived API view. Operations do not carry a project ID, so a
// project-scoped principal must be confined through the operation's App.
func LoadAuthorized(reads Repository, principal model.Principal, operationID string) (model.Operation, error) {
	op, err := reads.GetOperation(strings.TrimSpace(operationID))
	if err != nil {
		return model.Operation{}, err
	}
	if principal.IsPlatformAdmin() {
		return op, nil
	}
	if strings.TrimSpace(op.TenantID) != strings.TrimSpace(principal.TenantID) {
		return model.Operation{}, ErrNotVisible
	}
	if strings.TrimSpace(principal.ProjectID) == "" {
		return op, nil
	}
	if strings.TrimSpace(op.AppID) == "" {
		return model.Operation{}, ErrNotVisible
	}
	app, err := reads.GetApp(op.AppID)
	if err != nil || (strings.TrimSpace(app.TenantID) != "" && app.TenantID != principal.TenantID) || !principal.AllowsProject(app.ProjectID) {
		return model.Operation{}, ErrNotVisible
	}
	return op, nil
}

func ScopeProject(principal model.Principal, requested string) (string, error) {
	requested = strings.TrimSpace(requested)
	projectID := strings.TrimSpace(principal.ProjectID)
	if principal.IsPlatformAdmin() || projectID == "" {
		return requested, nil
	}
	if requested != "" && requested != projectID {
		return "", ErrNotVisible
	}
	return projectID, nil
}

// Apply timing evidence only to response copies, never to an operation that
// may be used to fence a controller write. The SQL implementation batches the
// authorized page rather than looking up each operation independently.
func ProjectFirstDeployStarts(ctx context.Context, reads Repository, ops []model.Operation) error {
	ids := make([]string, 0, len(ops))
	for _, op := range ops {
		if op.Type == model.OperationTypeDeploy && op.ExecutionMode == model.ExecutionModeManaged {
			ids = append(ids, op.ID)
		}
	}
	starts, err := reads.FirstDeployExecutionStarts(ctx, ids)
	if err != nil {
		return err
	}
	for i := range ops {
		op := &ops[i]
		at := starts[op.ID]
		if !at.IsZero() && !at.Before(op.CreatedAt) && (op.StartedAt == nil || at.Before(*op.StartedAt)) &&
			(op.CompletedAt == nil || !at.After(*op.CompletedAt)) {
			op.StartedAt = &at
		}
	}
	return nil
}

// AttachQueuedDeploy is read-only and optional. Failure to enrich must
// not change an operation's status or block access during an evidence outage.
func AttachQueuedDeploy(reads Repository, op *model.Operation) {
	if op.Type != model.OperationTypeImport || op.Status != model.OperationStatusCompleted {
		return
	}
	evidence, err := reads.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: op.TenantID, OperationID: op.ID, Types: []string{"deploy_queued"}, Limit: 8})
	if err != nil {
		return
	}
	for _, item := range evidence {
		id, _ := item.Payload["queued_deploy_operation_id"].(string)
		if id == "" || item.OperationID != op.ID {
			continue
		}
		child, err := reads.GetOperation(id)
		if err == nil && child.TenantID == op.TenantID && child.AppID == op.AppID && child.Type == model.OperationTypeDeploy {
			op.QueuedDeployOperationID = id
			return
		}
	}
}
