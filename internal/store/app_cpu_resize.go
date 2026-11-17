package store

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"fugue/internal/model"
)

// App request resize owns exactly one app-container request, never the release
// template, source, environment, limits or backing services.
func prepareAppRequestResizeOperation(app model.App, op *model.Operation) error {
	if op.DesiredSpec == nil || op.DesiredSpec.Resources == nil || app.Spec.Resources == nil ||
		!model.IsAppRequestResize(op.Type) ||
		app.Spec.Replicas <= 0 {
		return ErrInvalidInput
	}
	key := appRequestResizeField(op.Type)
	current, target := appRequestResizeValue(app.Spec.Resources, key), appRequestResizeValue(op.DesiredSpec.Resources, key)
	if current <= 0 || target <= 0 {
		return ErrInvalidInput
	}
	restoring := appRequestResizeTemplateValue(app.Spec, op.Type) != nil && target == current
	if !restoring && app.Status.CurrentReplicas != app.Spec.Replicas {
		return ErrInvalidInput
	}
	if limit := appRequestResizeLimit(app.Spec.Resources, key); limit > 0 && target > limit {
		return ErrInvalidInput
	}
	desired := cloneAppSpec(&app.Spec)
	desired.Resources = model.CloneResourceSpec(app.Spec.Resources)
	setAppRequestResizeValue(desired.Resources, key, target)
	op.DesiredSpec = desired
	op.ConfigBaseSpec = cloneAppSpec(&app.Spec)
	op.ServiceID = ""
	op.DesiredReplicas = nil
	op.DesiredSource, op.DesiredOriginSource = nil, nil
	op.SourceRuntimeID, op.TargetRuntimeID = app.Spec.RuntimeID, app.Spec.RuntimeID
	return nil
}

func applyAppRequestResize(app *model.App, op *model.Operation) error {
	if op.DesiredSpec == nil || op.DesiredSpec.Resources == nil || op.ConfigBaseSpec == nil ||
		model.AppSpecSHA256(app.Spec) != model.AppSpecSHA256(*op.ConfigBaseSpec) {
		return ErrConflict
	}
	key := appRequestResizeField(op.Type)
	if appRequestResizeTemplateValue(app.Spec, op.Type) == nil {
		setAppRequestResizeTemplate(app, op.Type, appRequestResizeValue(app.Spec.Resources, key))
	}
	app.Spec.Resources = model.CloneResourceSpec(app.Spec.Resources)
	setAppRequestResizeValue(app.Spec.Resources, key, appRequestResizeValue(op.DesiredSpec.Resources, key))
	app.UpdatedAt = time.Now().UTC()
	return nil
}

func activeAppRequestResize(ops []model.Operation, appID string) bool {
	for _, op := range ops {
		if op.AppID == appID && model.IsAppRequestResize(op.Type) &&
			(op.Status == model.OperationStatusPending || op.Status == model.OperationStatusRunning || op.Status == model.OperationStatusWaitingAgent) {
			return true
		}
	}
	return false
}

func (s *Store) pgHasActiveAppRequestResizeTx(ctx context.Context, tx *sql.Tx, appID string) (bool, error) {
	var active bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_operations WHERE app_id=$1 AND type IN ($2,$3) AND status IN ('pending','running','waiting-agent'))`, appID, model.OperationTypeAppCPUResize, model.OperationTypeAppMemoryResize).Scan(&active)
	return active, err
}

// Only normal deployments fold a verified runtime request into a new template.
// Reject a fabricated server-owned snapshot on all other caller-provided specs.
func reconcileAppRequestResizeSnapshot(op *model.Operation, app model.App) error {
	if op.DesiredSpec != nil {
		if op.DesiredSpec.WorkloadGeneration != "" && op.DesiredSpec.WorkloadGeneration != app.Spec.WorkloadGeneration {
			return ErrInvalidInput
		}
		op.DesiredSpec.WorkloadGeneration = app.Spec.WorkloadGeneration
		if app.Spec.WorkloadGeneration != "" && op.DesiredSpec.EphemeralPaths == nil {
			op.DesiredSpec.EphemeralPaths = append([]string(nil), app.Spec.EphemeralPaths...)
		}
		if err := model.ValidateRecoveryPaths(op.DesiredSpec.EphemeralPaths, *op.DesiredSpec); err != nil {
			return ErrInvalidInput
		}
	}
	if op.DesiredSpec != nil && op.DesiredSpec.NodeLoss == nil {
		op.DesiredSpec.NodeLoss = model.CloneAppNodeLossPolicy(app.Spec.NodeLoss)
	}
	if op.DesiredSpec == nil || model.IsAppRequestResize(op.Type) {
		return nil
	}
	if op.Type == model.OperationTypeDeploy || op.Type == model.OperationTypeImport {
		op.DesiredSpec.CPURequestTemplateMilliCores = nil
		op.DesiredSpec.MemoryRequestTemplateMebibytes = nil
		return nil
	}
	if p := op.DesiredSpec.CPURequestTemplateMilliCores; p != nil &&
		(app.Spec.CPURequestTemplateMilliCores == nil || *p != *app.Spec.CPURequestTemplateMilliCores) {
		return ErrInvalidInput
	}
	if p := op.DesiredSpec.MemoryRequestTemplateMebibytes; p != nil &&
		(app.Spec.MemoryRequestTemplateMebibytes == nil || *p != *app.Spec.MemoryRequestTemplateMebibytes) {
		return ErrInvalidInput
	}
	if app.Spec.CPURequestTemplateMilliCores != nil {
		cpu := *app.Spec.CPURequestTemplateMilliCores
		op.DesiredSpec.CPURequestTemplateMilliCores = &cpu
	}
	if app.Spec.MemoryRequestTemplateMebibytes != nil {
		memory := *app.Spec.MemoryRequestTemplateMebibytes
		op.DesiredSpec.MemoryRequestTemplateMebibytes = &memory
	}
	return nil
}

func appRequestResizeField(operationType string) string {
	if operationType == model.OperationTypeAppMemoryResize {
		return "memory"
	}
	return "cpu"
}

func appRequestResizeValue(resources *model.ResourceSpec, key string) int64 {
	if resources == nil {
		return 0
	}
	if key == "memory" {
		return resources.MemoryMebibytes
	}
	return resources.CPUMilliCores
}
func setAppRequestResizeValue(resources *model.ResourceSpec, key string, value int64) {
	if key == "memory" {
		resources.MemoryMebibytes = value
	} else {
		resources.CPUMilliCores = value
	}
}
func appRequestResizeLimit(resources *model.ResourceSpec, key string) int64 {
	if key == "memory" {
		return resources.MemoryLimitMebibytes
	}
	return resources.CPULimitMilliCores
}
func appRequestResizeTemplateValue(spec model.AppSpec, operationType string) *int64 {
	if operationType == model.OperationTypeAppMemoryResize {
		return spec.MemoryRequestTemplateMebibytes
	}
	return spec.CPURequestTemplateMilliCores
}
func setAppRequestResizeTemplate(app *model.App, operationType string, value int64) {
	if operationType == model.OperationTypeAppMemoryResize {
		app.Spec.MemoryRequestTemplateMebibytes = &value
	} else {
		app.Spec.CPURequestTemplateMilliCores = &value
	}
}

func (s *Store) CreateAppRequestResizeOperation(op model.Operation, baseline model.AppSpec) (model.Operation, error) {
	if !model.IsAppRequestResize(op.Type) || strings.TrimSpace(op.AppID) == "" {
		return model.Operation{}, ErrInvalidInput
	}
	created, _, err := s.createOperationWithPolicy(op, operationCreatePolicy{ExpectedAppSpecHash: model.AppSpecSHA256(baseline)})
	return created, err
}
