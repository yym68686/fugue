// Package operationcancel defines the operation cancellation policy. A caller
// must apply the transition to the current row inside its write transaction.
package operationcancel

import (
	"errors"
	"strings"
	"time"

	"fugue/internal/model"
)

var ErrNotVisible = errors.New("operation cancellation is outside the caller's write scope")

const DefaultMessage = "operation canceled before execution"

type Writer interface {
	Cancel(id, message string) (model.Operation, error)
}

func MayRequest(principal model.Principal) bool {
	return principal.IsPlatformAdmin() || principal.HasScope("app.write") || principal.HasScope("project.write")
}

func AllowsTenant(principal model.Principal, operation model.Operation) bool {
	if principal.IsPlatformAdmin() {
		return true
	}
	tenantID := strings.TrimSpace(principal.TenantID)
	return tenantID != "" && tenantID == strings.TrimSpace(operation.TenantID)
}

func AllowsApp(principal model.Principal, app model.App) bool {
	return principal.IsPlatformAdmin() ||
		(strings.TrimSpace(app.TenantID) == strings.TrimSpace(principal.TenantID) && principal.AllowsProject(app.ProjectID))
}

func Transition(operation model.Operation, message string, now time.Time) (model.Operation, bool) {
	// A running deploy can be superseded; other in-flight operations may
	// already have made changes that cannot be undone by cancellation.
	if operation.Status != model.OperationStatusPending &&
		!(operation.Status == model.OperationStatusRunning && operation.Type == model.OperationTypeDeploy) {
		return operation, false
	}
	if message = strings.TrimSpace(message); message == "" {
		message = DefaultMessage
	}
	operation.Status = model.OperationStatusCanceled
	operation.UpdatedAt = now
	operation.CompletedAt = &now
	operation.ResultMessage = message
	return operation, true
}
