package controller

import (
	"context"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/storagerecovery"
)

// Authority belongs to the authenticated operation, never to tenant-editable
// app fields. Automated bootstrap actors are deliberately not root operators.
func (s *Service) administratorMigrationOperation(op model.Operation) bool {
	switch op.Type {
	case model.OperationTypeMigrate, model.OperationTypeDatabaseSwitchover,
		model.OperationTypeDatabaseLocalize, storagerecovery.OperationType:
	default:
		return false
	}
	if op.RequestedByType == model.ActorTypeBootstrap {
		return op.RequestedByID == "bootstrap-admin"
	}
	if op.RequestedByType != model.ActorTypeAPIKey || s == nil || s.Store == nil {
		return false
	}
	key, err := s.Store.GetAPIKey(op.RequestedByID)
	if err != nil || key.Status != model.APIKeyStatusActive || key.DisabledAt != nil {
		return false
	}
	for _, scope := range key.Scopes {
		if scope == "platform.admin" || scope == "*" {
			return true
		}
	}
	return false
}

func (s *Service) activeAdministratorMigration(ctx context.Context, app model.App) (model.Operation, bool) {
	if s == nil || s.Store == nil || app.ID == "" || app.TenantID == "" {
		return model.Operation{}, false
	}
	valid := func(op model.Operation) bool {
		return op.AppID == app.ID && op.TenantID == app.TenantID &&
			(op.Status == model.OperationStatusPending || op.Status == model.OperationStatusRunning || op.Status == model.OperationStatusWaitingAgent) &&
			s.administratorMigrationOperation(op)
	}
	apply := managedAppApplySourceFromContext(ctx)
	if apply.Source == managedAppApplySourceOperation && apply.OperationID != "" {
		op, err := s.Store.GetOperation(apply.OperationID)
		return op, err == nil && valid(op)
	}
	// Background reconciliation resumes the same durable operation, not an
	// app annotation that could grant permanent maintenance privileges.
	ops, err := s.Store.ListOperationsByApp(app.TenantID, false, app.ID)
	if err == nil {
		for _, op := range ops {
			if valid(op) {
				return op, true
			}
		}
	}
	return model.Operation{}, false
}

type adminMigrationApplyKey struct{}
type adminMigrationApplyIdentity struct{ appID, tenantID string }

func (s *Service) withAdministratorMigrationApply(ctx context.Context, app model.App) context.Context {
	if _, ok := s.activeAdministratorMigration(ctx, app); ok {
		return context.WithValue(ctx, adminMigrationApplyKey{}, adminMigrationApplyIdentity{app.ID, app.TenantID})
	}
	return ctx
}

func deploymentRequiresZeroDowntimeForApply(ctx context.Context, object map[string]any) bool {
	identity, ok := ctx.Value(adminMigrationApplyKey{}).(adminMigrationApplyIdentity)
	labels := objectStringMapValue(nestedObjectValue(object, "metadata", "labels"))
	if ok && identity.appID != "" && identity.tenantID != "" &&
		strings.TrimSpace(labels[runtime.FugueLabelAppID]) == identity.appID &&
		strings.TrimSpace(labels[runtime.FugueLabelTenantID]) == identity.tenantID {
		return false
	}
	return deploymentRequiresZeroDowntime(object)
}
