package store

import "fugue/internal/model"

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
