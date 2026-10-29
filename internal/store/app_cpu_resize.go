package store

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"fugue/internal/model"
)

// CPU resize owns only the app container's CPU request, never its deployment
// template, source, environment, limits, memory or backing services.
func prepareAppCPUResizeOperation(app model.App, op *model.Operation) error {
	if op.DesiredSpec == nil || op.DesiredSpec.Resources == nil || app.Spec.Resources == nil ||
		app.Spec.Resources.CPUMilliCores <= 0 || op.DesiredSpec.Resources.CPUMilliCores <= 0 ||
		app.Spec.Replicas <= 0 {
		return ErrInvalidInput
	}
	cpu := op.DesiredSpec.Resources.CPUMilliCores
	restoring := app.Spec.CPURequestTemplateMilliCores != nil && cpu == app.Spec.Resources.CPUMilliCores
	if !restoring && app.Status.CurrentReplicas != app.Spec.Replicas {
		return ErrInvalidInput
	}
	if app.Spec.Resources.CPULimitMilliCores > 0 && cpu > app.Spec.Resources.CPULimitMilliCores {
		return ErrInvalidInput
	}
	desired := cloneAppSpec(&app.Spec)
	desired.Resources = model.CloneResourceSpec(app.Spec.Resources)
	desired.Resources.CPUMilliCores = cpu
	op.DesiredSpec = desired
	op.ConfigBaseSpec = cloneAppSpec(&app.Spec)
	op.ServiceID = ""
	op.DesiredReplicas = nil
	op.DesiredSource, op.DesiredOriginSource = nil, nil
	op.SourceRuntimeID, op.TargetRuntimeID = app.Spec.RuntimeID, app.Spec.RuntimeID
	return nil
}

func applyAppCPUResize(app *model.App, op *model.Operation) error {
	if op.DesiredSpec == nil || op.DesiredSpec.Resources == nil || op.ConfigBaseSpec == nil ||
		model.AppSpecSHA256(app.Spec) != model.AppSpecSHA256(*op.ConfigBaseSpec) {
		return ErrConflict
	}
	if app.Spec.CPURequestTemplateMilliCores == nil {
		cpu := app.Spec.Resources.CPUMilliCores
		app.Spec.CPURequestTemplateMilliCores = &cpu
	}
	app.Spec.Resources = model.CloneResourceSpec(app.Spec.Resources)
	app.Spec.Resources.CPUMilliCores = op.DesiredSpec.Resources.CPUMilliCores
	app.UpdatedAt = time.Now().UTC()
	return nil
}

func activeAppCPUResize(ops []model.Operation, appID string) bool {
	for _, op := range ops {
		if op.AppID == appID && op.Type == model.OperationTypeAppCPUResize &&
			(op.Status == model.OperationStatusPending || op.Status == model.OperationStatusRunning || op.Status == model.OperationStatusWaitingAgent) {
			return true
		}
	}
	return false
}

func (s *Store) pgHasActiveAppCPUResizeTx(ctx context.Context, tx *sql.Tx, appID string) (bool, error) {
	var active bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_operations WHERE app_id=$1 AND type=$2 AND status IN ('pending','running','waiting-agent'))`, appID, model.OperationTypeAppCPUResize).Scan(&active)
	return active, err
}

// Only normal deployments fold a verified runtime request into a new template.
// Reject a fabricated server-owned snapshot on all other caller-provided specs.
func reconcileAppCPUResizeSnapshot(op *model.Operation, app model.App) error {
	if op.DesiredSpec == nil || op.Type == model.OperationTypeAppCPUResize {
		return nil
	}
	if op.Type == model.OperationTypeDeploy || op.Type == model.OperationTypeImport {
		op.DesiredSpec.CPURequestTemplateMilliCores = nil
		return nil
	}
	if p := op.DesiredSpec.CPURequestTemplateMilliCores; p != nil &&
		(app.Spec.CPURequestTemplateMilliCores == nil || *p != *app.Spec.CPURequestTemplateMilliCores) {
		return ErrInvalidInput
	}
	if app.Spec.CPURequestTemplateMilliCores != nil {
		cpu := *app.Spec.CPURequestTemplateMilliCores
		op.DesiredSpec.CPURequestTemplateMilliCores = &cpu
	}
	return nil
}

func (s *Store) CreateAppCPUResizeOperation(op model.Operation, baseline model.AppSpec) (model.Operation, error) {
	if op.Type != model.OperationTypeAppCPUResize || strings.TrimSpace(op.AppID) == "" {
		return model.Operation{}, ErrInvalidInput
	}
	created, _, err := s.createOperationWithPolicy(op, operationCreatePolicy{ExpectedAppSpecHash: model.AppSpecSHA256(baseline)})
	return created, err
}
