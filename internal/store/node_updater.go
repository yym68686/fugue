package store

import (
	"fmt"
	"fugue/internal/storagerecovery"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/noderesourcepolicy"
)

const nodeUpdateTaskInventoryMaxWait = 30 * time.Minute
const nodeUpdateTaskPruneMaxWait = 10 * time.Minute
const nodeUpdateTaskHostPolicyMaxWait = 10 * time.Minute

func (s *Store) EnrollNodeUpdater(secret, nodeName, endpoint string, labels map[string]string, machineName, machineFingerprint, updaterVersion, joinScriptVersion string, capabilities []string) (model.NodeUpdater, string, error) {
	key, machine, runtimeObj, err := s.BootstrapClusterAttachment(secret, nodeName, endpoint, labels, machineName, machineFingerprint)
	if err != nil {
		return model.NodeUpdater{}, "", err
	}
	runtimeID := strings.TrimSpace(machine.RuntimeID)
	if runtimeObj != nil {
		runtimeID = runtimeObj.ID
	}
	return s.pgUpsertNodeUpdater(key, machine, runtimeID, updaterVersion, joinScriptVersion, capabilities)
}

func (s *Store) AuthenticateNodeUpdater(secret string) (model.NodeUpdater, model.Principal, error) {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return model.NodeUpdater{}, model.Principal{}, ErrInvalidInput
	}
	return s.pgAuthenticateNodeUpdater(secret)
}

func (s *Store) UpdateNodeUpdaterHeartbeat(updaterID string, heartbeat model.NodeUpdater) (model.NodeUpdater, error) {
	updaterID = strings.TrimSpace(updaterID)
	if updaterID == "" {
		return model.NodeUpdater{}, ErrInvalidInput
	}
	return s.pgUpdateNodeUpdaterHeartbeat(updaterID, heartbeat)
}

func (s *Store) ListNodeUpdaters(tenantID string, platformAdmin bool) ([]model.NodeUpdater, error) {
	return s.pgListNodeUpdaters(tenantID, platformAdmin)
}

func (s *Store) NodeUpdaterTargetSupportsTask(updaterID, clusterNodeName, runtimeID, taskType string) (bool, error) {
	taskType = normalizeNodeUpdateTaskType(taskType)
	if taskType == "" {
		return false, ErrInvalidInput
	}
	return s.pgNodeUpdaterTargetSupportsTask(updaterID, clusterNodeName, runtimeID, taskType)
}

func (s *Store) CreateNodeUpdateTask(principal model.Principal, updaterID, clusterNodeName, runtimeID, taskType string, payload map[string]string) (model.NodeUpdateTask, error) {
	taskType = normalizeNodeUpdateTaskType(taskType)
	if taskType == noderesourcepolicy.TaskType {
		if !principal.IsPlatformAdmin() {
			return model.NodeUpdateTask{}, fmt.Errorf("%w: node resource policy requires platform administrator permission", ErrInvalidInput)
		}
		if _, err := noderesourcepolicy.Parse(payload["policy_json"]); err != nil {
			return model.NodeUpdateTask{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
		}
		if payload["dry_run"] != "" && payload["dry_run"] != "true" && payload["dry_run"] != "false" {
			return model.NodeUpdateTask{}, fmt.Errorf("%w: dry_run must be true or false", ErrInvalidInput)
		}
	}
	if taskType == model.NodeUpdateTaskTypeRefreshJoinConfig && strings.TrimSpace(payload["pod_capacity_mode"]) != "" {
		mode := strings.TrimSpace(payload["pod_capacity_mode"])
		if !principal.IsPlatformAdmin() || (mode != "resources" && mode != "default") {
			return model.NodeUpdateTask{}, fmt.Errorf("%w: pod capacity requires platform admin and resources/default mode", ErrInvalidInput)
		}
		if payload["dry_run"] != "true" && payload["allow_restart"] != "true" {
			return model.NodeUpdateTask{}, fmt.Errorf("%w: pod capacity requires allow_restart=true outside dry-run", ErrInvalidInput)
		}
	}
	if taskType == storagerecovery.ExpandPoolTask && !principal.IsPlatformAdmin() {
		return model.NodeUpdateTask{}, fmt.Errorf("%w: pool expansion requires platform administrator permission", ErrInvalidInput)
	}
	if taskType == "" {
		return model.NodeUpdateTask{}, ErrInvalidInput
	}
	return s.pgCreateNodeUpdateTask(principal, updaterID, clusterNodeName, runtimeID, taskType, payload)
}

func (s *Store) ListNodeUpdateTasks(tenantID string, platformAdmin bool, updaterID, status string) ([]model.NodeUpdateTask, error) {
	return s.ListNodeUpdateTasksWithOptions(tenantID, platformAdmin, updaterID, status, NodeUpdateTaskListOptions{})
}

type NodeUpdateTaskListOptions struct {
	TaskID  string
	Limit   int
	Summary bool
}

func (s *Store) ListNodeUpdateTasksWithOptions(tenantID string, platformAdmin bool, updaterID, status string, options NodeUpdateTaskListOptions) ([]model.NodeUpdateTask, error) {
	if options.Limit < 0 || options.Limit > 1000 {
		return nil, ErrInvalidInput
	}
	options.TaskID = strings.TrimSpace(options.TaskID)
	return s.pgListNodeUpdateTasks(tenantID, platformAdmin, updaterID, status, options)
}

func (s *Store) ListPendingNodeUpdateTasks(updaterID string, limit int) ([]model.NodeUpdateTask, error) {
	return s.pgListPendingNodeUpdateTasks(updaterID, limit)
}

func (s *Store) GetNodeUpdateTaskForUpdater(taskID, updaterID string) (model.NodeUpdateTask, error) {
	return s.pgGetNodeUpdateTaskForUpdaterPublic(taskID, updaterID)
}

func (s *Store) FailStaleRunningNodeUpdateTasks(updaterID string, staleAfter time.Duration) (int, error) {
	return s.pgFailStaleRunningNodeUpdateTasks(updaterID, staleAfter)
}

func (s *Store) ClaimNodeUpdateTask(taskID, updaterID string) (model.NodeUpdateTask, error) {
	return s.pgClaimNodeUpdateTask(taskID, updaterID)
}

func (s *Store) AppendNodeUpdateTaskLog(taskID, updaterID, message string) (model.NodeUpdateTask, error) {
	return s.pgAppendNodeUpdateTaskLog(taskID, updaterID, message)
}

func (s *Store) FailNodeUpdateTask(taskID, updaterID, message, errorMessage string) (model.NodeUpdateTask, error) {
	return s.pgFailNodeUpdateTask(taskID, updaterID, message, errorMessage)
}

func (s *Store) CancelNodeUpdateTask(taskID, updaterID, message string) (model.NodeUpdateTask, error) {
	return s.pgCancelNodeUpdateTask(taskID, updaterID, message)
}

func (s *Store) CompleteNodeUpdateTask(taskID, updaterID, status, message, errorMessage string) (model.NodeUpdateTask, error) {
	return s.pgCompleteNodeUpdateTask(taskID, updaterID, status, message, errorMessage)
}

func staleNodeUpdateTaskResultMessage() string {
	return "node update task timed out"
}

func staleNodeUpdateTaskErrorMessage(staleAfter time.Duration) string {
	return fmt.Sprintf("node update task did not finish within %s; marking failed so updater can continue", staleAfter.Round(time.Second))
}

func nodeUpdaterPrincipal(updater model.NodeUpdater) model.Principal {
	return model.Principal{
		ActorType: model.ActorTypeNodeUpdater,
		ActorID:   strings.TrimSpace(updater.ID),
		TenantID:  strings.TrimSpace(updater.TenantID),
		Scopes: map[string]struct{}{
			"node.update": {},
		},
	}
}

func normalizeNodeUpdateTaskType(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case model.NodeUpdateTaskTypeRefreshJoinConfig:
		return model.NodeUpdateTaskTypeRefreshJoinConfig
	case model.NodeUpdateTaskTypeUpgradeK3SAgent:
		return model.NodeUpdateTaskTypeUpgradeK3SAgent
	case model.NodeUpdateTaskTypeUpgradeUpdater:
		return model.NodeUpdateTaskTypeUpgradeUpdater
	case model.NodeUpdateTaskTypeRestartK3SAgent:
		return model.NodeUpdateTaskTypeRestartK3SAgent
	case model.NodeUpdateTaskTypeDiagnoseNode:
		return model.NodeUpdateTaskTypeDiagnoseNode
	case model.NodeUpdateTaskTypeInstallNFSClient:
		return model.NodeUpdateTaskTypeInstallNFSClient
	case model.NodeUpdateTaskTypePrepullSystemImages:
		return model.NodeUpdateTaskTypePrepullSystemImages
	case model.NodeUpdateTaskTypePrepullAppImages:
		return model.NodeUpdateTaskTypePrepullAppImages
	case model.NodeUpdateTaskTypeReplicateAppImage:
		return model.NodeUpdateTaskTypeReplicateAppImage
	case model.NodeUpdateTaskTypeVerifyImageCache:
		return model.NodeUpdateTaskTypeVerifyImageCache
	case model.NodeUpdateTaskTypePruneImageCache:
		return model.NodeUpdateTaskTypePruneImageCache
	case model.NodeUpdateTaskTypeReportImageCache:
		return model.NodeUpdateTaskTypeReportImageCache
	case model.NodeUpdateTaskTypeReportLocalPV:
		return model.NodeUpdateTaskTypeReportLocalPV
	case storagerecovery.ExpandPoolTask:
		return storagerecovery.ExpandPoolTask
	case model.NodeUpdateTaskTypeDecommissionLocalPV:
		return model.NodeUpdateTaskTypeDecommissionLocalPV
	case model.NodeUpdateTaskTypeVerifySystemdEscape:
		return model.NodeUpdateTaskTypeVerifySystemdEscape
	case model.NodeUpdateTaskTypeRepairManagedIPTables:
		return model.NodeUpdateTaskTypeRepairManagedIPTables
	case model.NodeUpdateTaskTypeRefreshDesiredState:
		return model.NodeUpdateTaskTypeRefreshDesiredState
	case model.NodeUpdateTaskTypeReloadLKGBundle:
		return model.NodeUpdateTaskTypeReloadLKGBundle
	case model.NodeUpdateTaskTypeRestartStatelessNodeService:
		return model.NodeUpdateTaskTypeRestartStatelessNodeService
	case model.NodeUpdateTaskTypeRunDeepHealth:
		return model.NodeUpdateTaskTypeRunDeepHealth
	case model.NodeUpdateTaskTypeReconcileHostZRAM:
		return model.NodeUpdateTaskTypeReconcileHostZRAM
	case noderesourcepolicy.TaskType:
		return noderesourcepolicy.TaskType
	case model.NodeUpdateTaskTypeReconcileHostJournaldPolicy:
		return model.NodeUpdateTaskTypeReconcileHostJournaldPolicy
	default:
		return ""
	}
}

func nodeUpdaterSupportsTask(updater model.NodeUpdater, taskType string) bool {
	taskType = normalizeNodeUpdateTaskType(taskType)
	if taskType == "" {
		return false
	}
	if taskType == model.NodeUpdateTaskTypeUpgradeUpdater {
		return true
	}
	capabilities := normalizeStringList(updater.Capabilities)
	if len(capabilities) == 0 {
		return legacyNodeUpdaterSupportsTask(taskType)
	}
	for _, capability := range capabilities {
		if capability == taskType {
			return true
		}
	}
	return false
}

func legacyNodeUpdaterSupportsTask(taskType string) bool {
	switch taskType {
	case model.NodeUpdateTaskTypeRestartK3SAgent,
		model.NodeUpdateTaskTypeUpgradeK3SAgent,
		model.NodeUpdateTaskTypeUpgradeUpdater,
		model.NodeUpdateTaskTypeDiagnoseNode:
		return true
	default:
		return false
	}
}

func normalizeNodeUpdateTaskStatus(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case model.NodeUpdateTaskStatusPending:
		return model.NodeUpdateTaskStatusPending
	case model.NodeUpdateTaskStatusRunning:
		return model.NodeUpdateTaskStatusRunning
	case model.NodeUpdateTaskStatusCompleted:
		return model.NodeUpdateTaskStatusCompleted
	case model.NodeUpdateTaskStatusFailed:
		return model.NodeUpdateTaskStatusFailed
	case model.NodeUpdateTaskStatusCanceled:
		return model.NodeUpdateTaskStatusCanceled
	default:
		return ""
	}
}

func normalizeTerminalNodeUpdateTaskStatus(raw string) string {
	switch normalizeNodeUpdateTaskStatus(raw) {
	case model.NodeUpdateTaskStatusCompleted:
		return model.NodeUpdateTaskStatusCompleted
	case model.NodeUpdateTaskStatusFailed:
		return model.NodeUpdateTaskStatusFailed
	default:
		return ""
	}
}

func normalizeStringList(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
