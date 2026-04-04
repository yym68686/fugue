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
