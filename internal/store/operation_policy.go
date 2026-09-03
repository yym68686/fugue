package store

import (
	"reflect"
	"strings"

	"fugue/internal/model"
)

type operationCreatePolicy struct {
	RejectActiveDeployForApp      bool
	RejectNoopDeploy              bool
	ReuseActiveImageRebuildForApp bool
	RejectActiveImportForApp      bool
}

type operationCreateOutcome struct {
	Decision                AutoscalingDeployDecision `json:"decision"`
	ExistingOperationID     string                    `json:"existing_operation_id,omitempty"`
	ReusedExistingOperation bool
}

// OperationCreateResult reports whether an operation was inserted by the
// current call. A false Created value with a non-empty returned operation is
// an idempotent reuse of an existing operation.
type OperationCreateResult struct {
	Created bool `json:"created"`
}

type AutoscalingDeployDecision string

const (
	AutoscalingDeployDecisionQueued             AutoscalingDeployDecision = "queued_operation"
	AutoscalingDeployDecisionActiveDeployExists AutoscalingDeployDecision = "active_deploy_exists"
	AutoscalingDeployDecisionAlreadyCurrent     AutoscalingDeployDecision = "already_current"
)

type AutoscalingDeployOutcome struct {
	Decision            AutoscalingDeployDecision `json:"decision"`
	ExistingOperationID string                    `json:"existing_operation_id,omitempty"`
}

func (s *Store) CreateAutoscalingDeployOperation(op model.Operation) (model.Operation, AutoscalingDeployOutcome, error) {
	if !isAutoRightSizingDeployOperation(op) {
		return model.Operation{}, AutoscalingDeployOutcome{}, ErrInvalidInput
	}
	created, outcome, err := s.createOperationWithPolicy(op, operationCreatePolicy{
		RejectActiveDeployForApp: true,
		RejectNoopDeploy:         true,
	})
	if err != nil {
		return model.Operation{}, AutoscalingDeployOutcome{}, err
	}
	if outcome.Decision == "" {
		outcome.Decision = AutoscalingDeployDecisionQueued
	}
	return created, AutoscalingDeployOutcome{
		Decision:            outcome.Decision,
		ExistingOperationID: strings.TrimSpace(outcome.ExistingOperationID),
	}, nil
}

// CreateImageRebuildOperation atomically reuses an already active controller
// image rebuild for the same app. Background reconciliation can run on more
// than one controller replica, so a read-then-create check in the controller
// would otherwise submit duplicate builder jobs.
func (s *Store) CreateImageRebuildOperation(op model.Operation) (model.Operation, OperationCreateResult, error) {
	if op.Type != model.OperationTypeImport ||
		strings.TrimSpace(op.RequestedByType) != model.ActorTypeSystem ||
		strings.TrimSpace(op.RequestedByID) != model.OperationRequestedByImageRebuild {
		return model.Operation{}, OperationCreateResult{}, ErrInvalidInput
	}
	created, outcome, err := s.createOperationWithPolicy(op, operationCreatePolicy{
		ReuseActiveImageRebuildForApp: true,
		RejectActiveImportForApp:      true,
	})
	if err != nil {
		return model.Operation{}, OperationCreateResult{}, err
	}
	return created, OperationCreateResult{
		Created: strings.TrimSpace(created.ID) != "" && !outcome.ReusedExistingOperation,
	}, nil
}

func isAutoRightSizingDeployOperation(op model.Operation) bool {
	if op.Type != model.OperationTypeDeploy ||
		strings.TrimSpace(op.RequestedByType) != model.ActorTypeSystem {
		return false
	}
	switch strings.TrimSpace(op.RequestedByID) {
	case model.OperationRequestedByRightSizing, model.OperationRequestedByRightSizingDownscale:
		return true
	default:
		return false
	}
}

func deployOperationDesiredStateAlreadyCurrent(op model.Operation, app model.App) bool {
	if op.Type != model.OperationTypeDeploy || op.DesiredSpec == nil {
		return false
	}
	if !reflect.DeepEqual(*op.DesiredSpec, app.Spec) {
		return false
	}
	if op.DesiredSource != nil && !operationAppSourcesEqual(op.DesiredSource, model.AppBuildSource(app)) {
		return false
	}
	if op.DesiredOriginSource != nil && !operationAppSourcesEqual(op.DesiredOriginSource, model.AppOriginSource(app)) {
		return false
	}
	return true
}

func operationAppSourcesEqual(left, right *model.AppSource) bool {
	return reflect.DeepEqual(cloneAppSource(left), cloneAppSource(right))
}
