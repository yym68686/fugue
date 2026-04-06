package store

import (
	"strings"

	"fugue/internal/model"
)

// FailoverDesiredSpec preserves the current managed postgres placement while
// moving the app runtime to a new target. Without this pinning, a postgres
// spec that implicitly followed app.Spec.RuntimeID would be reinterpreted onto
// the failover target runtime.
func FailoverDesiredSpec(app model.App, targetRuntimeID string) *model.AppSpec {
	targetRuntimeID = strings.TrimSpace(targetRuntimeID)
	if targetRuntimeID == "" {
		return nil
	}

	next := cloneAppSpec(&app.Spec)
	if next == nil {
		return nil
	}
	next.RuntimeID = targetRuntimeID
	next.Postgres = nil

	currentDatabase := OwnedManagedPostgresSpec(app)
	if currentDatabase == nil {
		return next
	}

	postgres := *currentDatabase
	if currentDatabase.Resources != nil {
		resources := *currentDatabase.Resources
		postgres.Resources = &resources
	}
	next.Postgres = &postgres
	return next
}

func operationAppliesDesiredSpecBackingServices(op model.Operation) bool {
	if op.DesiredSpec == nil {
		return false
	}
	switch op.Type {
	case model.OperationTypeDeploy, model.OperationTypeDatabaseSwitchover, model.OperationTypeFailover:
		return true
	default:
		return false
	}
}

func applyCompletedFailoverToAppModel(app *model.App, op *model.Operation) error {
	if strings.TrimSpace(op.TargetRuntimeID) == "" {
		return ErrInvalidInput
	}
	if op.DesiredSpec != nil {
		app.Spec = *op.DesiredSpec
	} else {
		app.Spec.RuntimeID = op.TargetRuntimeID
	}
	app.Spec.RuntimeID = op.TargetRuntimeID
	app.Status.Phase = "failed-over"
	app.Status.CurrentRuntimeID = op.TargetRuntimeID
	app.Status.CurrentReplicas = app.Spec.Replicas
	if op.ExecutionMode != model.ExecutionModeManaged {
		app.Status.CurrentReleaseStartedAt = nil
		app.Status.CurrentReleaseReadyAt = nil
	}
	return nil
}
