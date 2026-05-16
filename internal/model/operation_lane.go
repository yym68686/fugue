package model

import "strings"

const (
	OperationControllerLaneForegroundImport   = "foreground-import"
	OperationControllerLaneForegroundActivate = "foreground-activate"
	OperationControllerLaneGitHubSyncImport   = "github-sync-import"
	OperationControllerLaneGitHubSyncActivate = "github-sync-activate"
	OperationControllerLaneUnknown            = "unknown"
)

func OperationControllerLaneName(op Operation) string {
	requestedByBackgroundController := operationRequestedByBackgroundController(op.RequestedByID)
	opType := strings.TrimSpace(op.Type)
	switch {
	case opType == OperationTypeImport && !requestedByBackgroundController:
		return OperationControllerLaneForegroundImport
	case opType != OperationTypeImport && !requestedByBackgroundController:
		return OperationControllerLaneForegroundActivate
	case opType == OperationTypeImport && requestedByBackgroundController:
		return OperationControllerLaneGitHubSyncImport
	case opType != OperationTypeImport && requestedByBackgroundController:
		return OperationControllerLaneGitHubSyncActivate
	default:
		return OperationControllerLaneUnknown
	}
}

func operationRequestedByBackgroundController(requestedByID string) bool {
	switch strings.TrimSpace(requestedByID) {
	case OperationRequestedByGitHubSyncController, OperationRequestedByImageTracking:
		return true
	default:
		return false
	}
}

func OperationOccupiesControllerWorker(op Operation) bool {
	return strings.TrimSpace(op.Status) == OperationStatusRunning && strings.TrimSpace(op.ExecutionMode) == ExecutionModeManaged
}
