package store

import (
	"bytes"
	"encoding/json"
	"fugue/internal/model"
	"time"
)

// Finalization fences one execution attempt. Progress timestamps may advance,
// but a requeue, cancellation, changed intent or newer configuration invalidates
// a previously observed result. This is internal, not an API-supplied assertion.
type operationFinalizationGuard struct {
	Operation            model.Operation
	App                  *model.App
	AttemptOnly          bool
	ConstraintPolicyOnly bool
}

func validateOperationFinalization(op model.Operation, app model.App, guard *operationFinalizationGuard) error {
	if guard == nil {
		return nil
	}
	expected := guard.Operation
	if op.ID != expected.ID || op.AppID != expected.AppID || op.TenantID != expected.TenantID || op.Type != expected.Type ||
		op.ExecutionMode != model.ExecutionModeManaged || op.Status != model.OperationStatusRunning ||
		!sameOperationStartedAt(op.StartedAt, expected.StartedAt) {
		return ErrConflict
	}
	if !guard.AttemptOnly && (!finalizationIntentEqual(op.DesiredSpec, expected.DesiredSpec) ||
		!finalizationIntentEqual(op.ConfigBaseSpec, expected.ConfigBaseSpec) || !finalizationIntentEqual(op.DesiredSource, expected.DesiredSource) ||
		!finalizationIntentEqual(op.DesiredOriginSource, expected.DesiredOriginSource)) {
		return ErrConflict
	}
	if previous := guard.App; previous != nil && (!finalizationIntentEqual(app.Spec, previous.Spec) ||
		!finalizationIntentEqual(model.AppBuildSource(app), model.AppBuildSource(*previous)) ||
		!finalizationIntentEqual(model.AppOriginSource(app), model.AppOriginSource(*previous)) ||
		!finalizationIntentEqual(app.Route, previous.Route) || !finalizationIntentEqual(bindingIntent(app.Bindings), bindingIntent(previous.Bindings)) ||
		!finalizationIntentEqual(backingServiceIntent(app.BackingServices), backingServiceIntent(previous.BackingServices)) || app.Name != previous.Name) {
		return ErrConflict
	}
	if guard.ConstraintPolicyOnly && (guard.App == nil || !model.RolloutConstraintPolicyOnlyChange(app.Spec, op)) {
		return ErrConflict
	}
	return nil
}

// CompleteRolloutConstraintPolicyOperation changes durable policy without
// claiming that an image, workload, traffic bundle or runtime has converged.
func (s *Store) CompleteRolloutConstraintPolicyOperation(expected model.Operation, current model.App, message string) (model.Operation, error) {
	return s.completeOperationGuarded(expected.ID, "", "", message, nil, nil, nil,
		&operationFinalizationGuard{Operation: expected, App: &current, ConstraintPolicyOnly: true})
}

func applyRolloutConstraintPolicy(app *model.App, op model.Operation) {
	app.Spec.Continuity = model.CloneAppContinuityPolicy(op.DesiredSpec.Continuity)
	app.UpdatedAt = op.UpdatedAt
	app.Status.LastOperationID, app.Status.LastMessage = op.ID, op.ResultMessage
	// Runtime phase, replicas, readiness timestamps and prior failure history
	// belong to actual runtime observations, not configuration publication.
}

func operationOnlyChangesRolloutConstraints(app model.App, op model.Operation) bool {
	if model.RolloutConstraintPolicyOnlyChange(app.Spec, op) {
		return true
	}
	// A later code release can change the app between enqueue and claim. The
	// immutable baseline still identifies a policy intent; execution rebases and
	// verifies it against current configuration before committing anything.
	return op.ConfigBaseSpec != nil && model.RolloutConstraintPolicyOnlyChange(*op.ConfigBaseSpec, op)
}

// CompleteManagedOperationGuarded commits only the observed attempt. expectedApp
// additionally fences configuration when recovering from runtime evidence.
func (s *Store) CompleteManagedOperationGuarded(expected model.Operation, expectedApp *model.App, manifestPath, message string, desiredSpec *model.AppSpec, desiredSource, desiredOriginSource *model.AppSource) (model.Operation, error) {
	return s.completeOperationGuarded(expected.ID, "", manifestPath, message, desiredSpec, desiredSource, desiredOriginSource, &operationFinalizationGuard{Operation: expected, App: expectedApp})
}
func (s *Store) FailManagedOperationGuarded(expected model.Operation, message string) (model.Operation, error) {
	return s.failOperationGuarded(expected.ID, message, &operationFinalizationGuard{Operation: expected, AttemptOnly: true})
}

// RequeueManagedOperationGuarded prevents a worker that lost leadership from
// requeueing a newer execution attempt after the next leader has claimed it.
func (s *Store) RequeueManagedOperationGuarded(expected model.Operation, message string) (model.Operation, error) {
	return s.requeueManagedOperationGuarded(expected.ID, message, &operationFinalizationGuard{Operation: expected, AttemptOnly: true})
}

// Preview the same merge used at activation, without changing persisted intent.
// Recovery can then compare it with runtime facts before a guarded commit.
func RebasedManagedDeploy(op model.Operation, current model.App) (model.Operation, error) {
	candidate := cloneOperation(op)
	if err := rebaseImportDeployConfiguration(&candidate, current, candidate.ConfigBaseSpec); err != nil {
		return model.Operation{}, err
	}
	return candidate, nil
}

func backingServiceIntent(services []model.BackingService) map[string]model.BackingServiceSpec {
	intent := make(map[string]model.BackingServiceSpec, len(services))
	for _, service := range services {
		intent[service.ID] = service.Spec
	}
	return intent
}

func sameOperationStartedAt(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	// PostgreSQL timestamptz stores microseconds; claim responses can still carry
	// the pre-serialization nanoseconds. Compare durable precision and instants.
	return a.Truncate(time.Microsecond).Equal(b.Truncate(time.Microsecond))
}

func finalizationIntentEqual(a, b any) bool {
	left, e1 := json.Marshal(a)
	right, e2 := json.Marshal(b)
	return e1 == nil && e2 == nil && bytes.Equal(left, right)
}
func bindingIntent(bindings []model.ServiceBinding) map[string]model.ServiceBinding {
	intent := make(map[string]model.ServiceBinding, len(bindings))
	for _, binding := range bindings {
		binding.CreatedAt = time.Time{}
		binding.UpdatedAt = time.Time{}
		intent[binding.ID] = binding
	}
	return intent
}
