package store

import (
	"strings"
	"time"

	"fugue/internal/model"
)

// normalizeManagedPostgresResizeTarget requires a complete resource envelope.
// Zero values are not accepted because ResourceSpec cannot distinguish an
// omitted limit from a request to remove an existing limit, and Kubernetes
// cannot safely remove an existing request or limit through Pod /resize.
func normalizeManagedPostgresResizeTarget(target *model.ResourceSpec) (*model.ResourceSpec, error) {
	if target == nil {
		return nil, ErrInvalidInput
	}
	normalized, err := normalizeWorkloadResources(target, model.ResourceSpec{})
	if err != nil {
		return nil, err
	}
	if normalized.CPUMilliCores <= 0 || normalized.MemoryMebibytes <= 0 ||
		normalized.CPULimitMilliCores <= 0 || normalized.MemoryLimitMebibytes <= 0 {
		return nil, ErrInvalidInput
	}
	return normalized, nil
}

// prepareManagedPostgresResizeOperation narrows an untrusted operation request
// to one app-owned managed PostgreSQL backing service and one complete runtime
// resource target. All caller-supplied AppSpec and source fields are discarded
// so the operation cannot smuggle an app deploy or a CNPG bootstrap change.
func prepareManagedPostgresResizeOperation(app model.App, op *model.Operation) error {
	if op == nil || strings.TrimSpace(op.ServiceID) == "" ||
		op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil {
		return ErrInvalidInput
	}
	targetResources, err := normalizeManagedPostgresResizeTarget(op.DesiredSpec.Postgres.RuntimeResources)
	if err != nil {
		return err
	}
	target, err := ManagedPostgresOperationTargetForApp(app, op.ServiceID)
	if err != nil || target == nil || target.Service == nil ||
		strings.TrimSpace(target.ServiceID) == "" || !target.AppOwned {
		return ErrInvalidInput
	}
	if target.Postgres.Suspended {
		return ErrConflict
	}

	desiredSpec := cloneAppSpec(&app.Spec)
	if desiredSpec == nil {
		return ErrInvalidInput
	}
	postgres := model.CloneAppPostgresSpec(&target.Postgres)
	postgres.RuntimeResources = model.CloneResourceSpec(targetResources)
	if err := normalizePostgresSpecResources(postgres); err != nil {
		return err
	}
	if err := validateManagedPostgresSpecForAppName(app.Name, postgres); err != nil {
		return err
	}
	desiredSpec.Postgres = postgres

	runtimeID := strings.TrimSpace(postgres.RuntimeID)
	if runtimeID == "" {
		runtimeID = strings.TrimSpace(app.Spec.RuntimeID)
	}
	if runtimeID == "" {
		return ErrInvalidInput
	}
	op.ServiceID = strings.TrimSpace(target.ServiceID)
	op.SourceRuntimeID = runtimeID
	op.TargetRuntimeID = runtimeID
	op.DesiredReplicas = nil
	op.DesiredSpec = desiredSpec
	op.DesiredSource = nil
	op.DesiredOriginSource = nil
	return nil
}

func validateManagedPostgresResizeTargetState(state *model.State, app model.App, serviceID string) error {
	if state == nil {
		return ErrInvalidInput
	}
	serviceID = strings.TrimSpace(serviceID)
	serviceIndex := findBackingService(state, serviceID)
	if serviceID == "" || serviceIndex < 0 {
		return ErrInvalidInput
	}
	service := state.BackingServices[serviceIndex]
	if service.TenantID != app.TenantID || service.ProjectID != app.ProjectID ||
		!isManagedPostgresService(service) || service.Spec.Postgres == nil {
		return ErrInvalidInput
	}
	if strings.TrimSpace(service.OwnerAppID) != strings.TrimSpace(app.ID) {
		return ErrConflict
	}
	if service.Spec.Postgres.Suspended {
		return ErrConflict
	}
	bindingCount := 0
	for _, binding := range state.ServiceBindings {
		if strings.TrimSpace(binding.ServiceID) != serviceID {
			continue
		}
		bindingCount++
		if strings.TrimSpace(binding.AppID) != strings.TrimSpace(app.ID) || binding.TenantID != app.TenantID {
			return ErrConflict
		}
	}
	if bindingCount != 1 {
		return ErrConflict
	}
	return nil
}

func applyManagedPostgresResizeState(state *model.State, app *model.App, op *model.Operation) error {
	if state == nil || app == nil || op == nil || op.Type != model.OperationTypeDatabaseResize ||
		op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil {
		return ErrInvalidInput
	}
	targetResources, err := normalizeManagedPostgresResizeTarget(op.DesiredSpec.Postgres.RuntimeResources)
	if err != nil {
		return err
	}
	if err := validateManagedPostgresResizeTargetState(state, *app, op.ServiceID); err != nil {
		return err
	}
	serviceIndex := findBackingService(state, strings.TrimSpace(op.ServiceID))
	service := cloneBackingService(state.BackingServices[serviceIndex])
	postgres := model.CloneAppPostgresSpec(service.Spec.Postgres)
	postgres.RuntimeResources = model.CloneResourceSpec(targetResources)
	service.Spec.Postgres = postgres
	service.UpdatedAt = time.Now().UTC()
	state.BackingServices[serviceIndex] = service
	return nil
}

func hasActiveAppDatabaseRestoreRunForManagedPostgres(state *model.State, app model.App) bool {
	if state == nil || strings.TrimSpace(app.ID) == "" {
		return false
	}
	for _, run := range state.BackupRestoreRuns {
		run = model.NormalizeBackupRestoreRun(run)
		if strings.TrimSpace(run.AppID) != strings.TrimSpace(app.ID) {
			continue
		}
		if run.Status == model.BackupRestoreStatusPlanned || run.Status == model.BackupRestoreStatusRunning {
			return true
		}
	}
	return false
}
