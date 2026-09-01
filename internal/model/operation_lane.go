package model

import "strings"

const (
	OperationControllerLaneForegroundImport      = "foreground-import"
	OperationControllerLaneForegroundActivate    = "foreground-activate"
	OperationControllerLaneGitHubSyncImport      = "github-sync-import"
	OperationControllerLaneGitHubSyncActivate    = "github-sync-activate"
	OperationControllerLaneUnknown               = "unknown"
	OperationResultDeployImageReplicationPending = "deploy image replication is pending"
)

// OperationWaitingForImageReplication reports the recoverable prerequisite
// state persisted when a deploy cannot proceed until its target image cache
// replica is ready.
func OperationWaitingForImageReplication(op Operation) bool {
	return strings.TrimSpace(op.Status) == OperationStatusPending &&
		strings.HasPrefix(strings.TrimSpace(op.ResultMessage), OperationResultDeployImageReplicationPending)
}

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
