package store

import (
	"sort"
	"strings"

	"fugue/internal/model"
)

func hasInFlightOperationForApp(ops []model.Operation, appID string) bool {
	for _, op := range ops {
		if op.AppID != appID {
			continue
		}
		switch op.Status {
		case model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent:
			return true
		}
	}
	return false
}

func activeOperationsForLifecycleTarget(ops []model.Operation, appID, serviceID string) []model.Operation {
	appID = strings.TrimSpace(appID)
	serviceID = strings.TrimSpace(serviceID)
	active := make([]model.Operation, 0, 1)
	for _, op := range ops {
		if !isActiveOperationStatus(op.Status) {
			continue
		}
		if strings.TrimSpace(op.AppID) != appID && strings.TrimSpace(op.ServiceID) != serviceID {
			continue
		}
		active = append(active, op)
	}
	sortActiveOperations(active)
	return active
}

func managedPostgresLifecycleRetryMatches(existing, candidate model.Operation) bool {
	if !isManagedPostgresLifecycleOperationType(candidate.Type) ||
		!isActiveOperationStatus(existing.Status) ||
		strings.TrimSpace(existing.TenantID) != strings.TrimSpace(candidate.TenantID) ||
		strings.TrimSpace(existing.AppID) != strings.TrimSpace(candidate.AppID) ||
		strings.TrimSpace(existing.ServiceID) != strings.TrimSpace(candidate.ServiceID) ||
		existing.Type != candidate.Type {
		return false
	}
	if existing.DesiredSpec == nil || existing.DesiredSpec.Postgres == nil ||
		candidate.DesiredSpec == nil || candidate.DesiredSpec.Postgres == nil {
		return false
	}
	wantSuspended := existing.Type == model.OperationTypeDatabaseSuspend
	if existing.DesiredSpec.Postgres.Suspended != wantSuspended ||
		candidate.DesiredSpec.Postgres.Suspended != wantSuspended {
		return false
	}
	existingRuntimeID := strings.TrimSpace(existing.DesiredSpec.Postgres.RuntimeID)
	candidateRuntimeID := strings.TrimSpace(candidate.DesiredSpec.Postgres.RuntimeID)
	existingServiceName := strings.TrimSpace(existing.DesiredSpec.Postgres.ServiceName)
	candidateServiceName := strings.TrimSpace(candidate.DesiredSpec.Postgres.ServiceName)
	if existingRuntimeID == "" || existingRuntimeID != candidateRuntimeID ||
		existingServiceName == "" || existingServiceName != candidateServiceName {
		return false
	}
	existingSourceRuntimeID := strings.TrimSpace(existing.SourceRuntimeID)
	existingTargetRuntimeID := strings.TrimSpace(existing.TargetRuntimeID)
	candidateSourceRuntimeID := strings.TrimSpace(candidate.SourceRuntimeID)
	candidateTargetRuntimeID := strings.TrimSpace(candidate.TargetRuntimeID)
	return existingSourceRuntimeID != "" &&
		existingTargetRuntimeID != "" &&
		existingSourceRuntimeID == candidateSourceRuntimeID &&
		existingTargetRuntimeID == candidateTargetRuntimeID &&
		existingSourceRuntimeID == existingRuntimeID &&
		existingTargetRuntimeID == existingRuntimeID
}

func cloneOperation(op model.Operation) model.Operation {
	op.DesiredSpec = cloneAppSpec(op.DesiredSpec)
	op.DesiredSource = cloneAppSource(op.DesiredSource)
	op.DesiredOriginSource = cloneAppSource(op.DesiredOriginSource)
	op.ControllerTimingSegments = cloneOperationControllerTimingSegments(op.ControllerTimingSegments)
	if op.DesiredReplicas != nil {
		value := *op.DesiredReplicas
		op.DesiredReplicas = &value
	}
	if op.StartedAt != nil {
		value := *op.StartedAt
		op.StartedAt = &value
	}
	if op.CompletedAt != nil {
		value := *op.CompletedAt
		op.CompletedAt = &value
	}
	return op
}

func hasInFlightManagedPostgresLifecycleForApp(ops []model.Operation, appID string) bool {
	for _, op := range ops {
		if op.AppID != appID || !isManagedPostgresLifecycleOperationType(op.Type) || !isActiveOperationStatus(op.Status) {
			continue
		}
		return true
	}
	return false
}

func hasInFlightManagedPostgresLifecycleForService(ops []model.Operation, serviceID string) bool {
	for _, op := range ops {
		if op.ServiceID != serviceID || !isManagedPostgresLifecycleOperationType(op.Type) || !isActiveOperationStatus(op.Status) {
			continue
		}
		return true
	}
	return false
}

func isManagedPostgresLifecycleOperationType(operationType string) bool {
	return operationType == model.OperationTypeDatabaseSuspend || operationType == model.OperationTypeDatabaseResume
}

func firstActiveDeployOperationForApp(ops []model.Operation, appID string) (model.Operation, bool) {
	active := make([]model.Operation, 0, 1)
	for _, op := range ops {
		if op.AppID != appID || op.Type != model.OperationTypeDeploy || !isActiveOperationStatus(op.Status) {
			continue
		}
		active = append(active, op)
	}
	if len(active) == 0 {
		return model.Operation{}, false
	}
	sortActiveOperations(active)
	return active[0], true
}

func isActiveOperationStatus(status string) bool {
	switch status {
	case model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent:
		return true
	default:
		return false
	}
}

func sortActiveOperations(ops []model.Operation) {
	sort.Slice(ops, func(i, j int) bool {
		if ops[i].CreatedAt.Equal(ops[j].CreatedAt) {
			return ops[i].ID < ops[j].ID
		}
		return ops[i].CreatedAt.Before(ops[j].CreatedAt)
	})
}

func operationCreatedBefore(left, right model.Operation) bool {
	if left.CreatedAt.Equal(right.CreatedAt) {
		return left.ID < right.ID
	}
	return left.CreatedAt.Before(right.CreatedAt)
}
