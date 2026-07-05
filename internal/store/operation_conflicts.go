package store

import (
	"sort"

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
