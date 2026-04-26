package store

import (
	"strings"

	"fugue/internal/model"
)

// FailoverDesiredSpec moves the app to the failover target and consumes the
// configured app failover so the service re-enters an explicit manual re-arm
// state. Managed postgres continuity is only consumed when it was actually
// targeting the same runtime; otherwise the current placement stays pinned.
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
	next.Failover = nil
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
	if shouldConsumeManagedPostgresFailover(*currentDatabase, targetRuntimeID) {
		postgres.RuntimeID = targetRuntimeID
		postgres.FailoverTargetRuntimeID = ""
		postgres.Instances = 1
		postgres.SynchronousReplicas = 0
		postgres.PrimaryPlacementPendingRebalance = false
	}
	next.Postgres = &postgres
	return next
}

func shouldConsumeManagedPostgresFailover(spec model.AppPostgresSpec, targetRuntimeID string) bool {
	return strings.TrimSpace(spec.FailoverTargetRuntimeID) == strings.TrimSpace(targetRuntimeID)
}

func operationAppliesDesiredSpecBackingServices(op model.Operation) bool {
	if op.DesiredSpec == nil {
		return false
	}
	switch op.Type {
	case model.OperationTypeDeploy, model.OperationTypeMigrate, model.OperationTypeDatabaseSwitchover, model.OperationTypeDatabaseLocalize, model.OperationTypeFailover:
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

func applyCompletedMigrateToAppModel(app *model.App, op *model.Operation) error {
	if strings.TrimSpace(op.TargetRuntimeID) == "" {
		return ErrInvalidInput
	}
	if op.DesiredSpec != nil {
		app.Spec = *op.DesiredSpec
	} else {
		app.Spec.RuntimeID = op.TargetRuntimeID
	}
	app.Spec.RuntimeID = op.TargetRuntimeID
	if app.Route != nil {
		if model.AppExposesPublicService(app.Spec) {
			app.Route.ServicePort = model.AppPublicServicePort(app.Spec)
		} else {
			app.Route = nil
		}
	}
	applyOperationSourceStateToApp(app, *op)
	app.Status.Phase = "migrated"
	app.Status.CurrentRuntimeID = op.TargetRuntimeID
	app.Status.CurrentReplicas = app.Spec.Replicas
	if op.ExecutionMode != model.ExecutionModeManaged {
		app.Status.CurrentReleaseStartedAt = nil
		app.Status.CurrentReleaseReadyAt = nil
	}
	return nil
}
