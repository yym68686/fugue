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
	requestedByGitHubSync := strings.TrimSpace(op.RequestedByID) == OperationRequestedByGitHubSyncController
	opType := strings.TrimSpace(op.Type)
	switch {
	case opType == OperationTypeImport && !requestedByGitHubSync:
		return OperationControllerLaneForegroundImport
	case opType != OperationTypeImport && !requestedByGitHubSync:
		return OperationControllerLaneForegroundActivate
	case opType == OperationTypeImport && requestedByGitHubSync:
		return OperationControllerLaneGitHubSyncImport
	case opType != OperationTypeImport && requestedByGitHubSync:
		return OperationControllerLaneGitHubSyncActivate
	default:
		return OperationControllerLaneUnknown
	}
}

func OperationOccupiesControllerWorker(op Operation) bool {
	return strings.TrimSpace(op.Status) == OperationStatusRunning && strings.TrimSpace(op.ExecutionMode) == ExecutionModeManaged
}
