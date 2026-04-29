package store

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) EnrollNodeUpdater(secret, nodeName, endpoint string, labels map[string]string, machineName, machineFingerprint, updaterVersion, joinScriptVersion string, capabilities []string) (model.NodeUpdater, string, error) {
	key, machine, runtimeObj, err := s.BootstrapClusterAttachment(secret, nodeName, endpoint, labels, machineName, machineFingerprint)
	if err != nil {
		return model.NodeUpdater{}, "", err
	}
	runtimeID := strings.TrimSpace(machine.RuntimeID)
	if runtimeObj != nil {
		runtimeID = runtimeObj.ID
	}
	if s.usingDatabase() {
		return s.pgUpsertNodeUpdater(key, machine, runtimeID, updaterVersion, joinScriptVersion, capabilities)
	}

	token := model.NewSecret("fugue_nu")
	now := time.Now().UTC()
	updater := model.NodeUpdater{
		ID:                model.NewID("nodeupdater"),
		TenantID:          strings.TrimSpace(machine.TenantID),
		NodeKeyID:         strings.TrimSpace(key.ID),
		MachineID:         strings.TrimSpace(machine.ID),
		RuntimeID:         runtimeID,
		ClusterNodeName:   strings.TrimSpace(machine.ClusterNodeName),
		Status:            model.NodeUpdaterStatusActive,
		TokenPrefix:       model.SecretPrefix(token),
		TokenHash:         model.HashSecret(token),
		Labels:            cloneMap(machine.Labels),
		Capabilities:      normalizeStringList(capabilities),
		UpdaterVersion:    strings.TrimSpace(updaterVersion),
		JoinScriptVersion: strings.TrimSpace(joinScriptVersion),
		LastSeenAt:        &now,
		LastHeartbeatAt:   &now,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	err = s.withLockedState(true, func(state *model.State) error {
		index := findNodeUpdaterByMachine(state, updater.MachineID)
		if index < 0 {
			index = findNodeUpdaterByClusterNode(state, updater.NodeKeyID, updater.ClusterNodeName)
		}
		if index >= 0 {
			updater.ID = state.NodeUpdaters[index].ID
			updater.CreatedAt = state.NodeUpdaters[index].CreatedAt
			state.NodeUpdaters[index] = updater
			return nil
		}
		state.NodeUpdaters = append(state.NodeUpdaters, updater)
		return nil
	})
	if err != nil {
		return model.NodeUpdater{}, "", err
	}
	return redactNodeUpdater(updater), token, nil
}

func (s *Store) AuthenticateNodeUpdater(secret string) (model.NodeUpdater, model.Principal, error) {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return model.NodeUpdater{}, model.Principal{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgAuthenticateNodeUpdater(secret)
	}

	var updater model.NodeUpdater
	err := s.withLockedState(true, func(state *model.State) error {
		hash := model.HashSecret(secret)
		for idx := range state.NodeUpdaters {
			if state.NodeUpdaters[idx].TokenHash != hash {
				continue
			}
			if state.NodeUpdaters[idx].Status == model.NodeUpdaterStatusRevoked {
				return ErrConflict
			}
			now := time.Now().UTC()
			state.NodeUpdaters[idx].LastSeenAt = &now
			state.NodeUpdaters[idx].UpdatedAt = now
			updater = state.NodeUpdaters[idx]
			return nil
		}
		return ErrNotFound
	})
	if err != nil {
		return model.NodeUpdater{}, model.Principal{}, err
	}
	return redactNodeUpdater(updater), nodeUpdaterPrincipal(updater), nil
}

func (s *Store) UpdateNodeUpdaterHeartbeat(updaterID string, heartbeat model.NodeUpdater) (model.NodeUpdater, error) {
	updaterID = strings.TrimSpace(updaterID)
	if updaterID == "" {
		return model.NodeUpdater{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpdateNodeUpdaterHeartbeat(updaterID, heartbeat)
	}

	var updater model.NodeUpdater
	err := s.withLockedState(true, func(state *model.State) error {
		index := findNodeUpdater(state, updaterID)
		if index < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		current := state.NodeUpdaters[index]
		current.Labels = cloneMap(heartbeat.Labels)
		current.Capabilities = normalizeStringList(heartbeat.Capabilities)
		current.UpdaterVersion = strings.TrimSpace(heartbeat.UpdaterVersion)
		current.JoinScriptVersion = strings.TrimSpace(heartbeat.JoinScriptVersion)
		current.K3SVersion = strings.TrimSpace(heartbeat.K3SVersion)
		current.OS = strings.TrimSpace(heartbeat.OS)
		current.Arch = strings.TrimSpace(heartbeat.Arch)
		current.LastError = strings.TrimSpace(heartbeat.LastError)
		current.LastSeenAt = &now
		current.LastHeartbeatAt = &now
		current.UpdatedAt = now
		state.NodeUpdaters[index] = current
		updater = current
		return nil
	})
	if err != nil {
		return model.NodeUpdater{}, err
	}
	return redactNodeUpdater(updater), nil
}

func (s *Store) ListNodeUpdaters(tenantID string, platformAdmin bool) ([]model.NodeUpdater, error) {
	if s.usingDatabase() {
		return s.pgListNodeUpdaters(tenantID, platformAdmin)
	}
	tenantID = strings.TrimSpace(tenantID)
	updaters := []model.NodeUpdater{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, updater := range state.NodeUpdaters {
			if !platformAdmin && strings.TrimSpace(updater.TenantID) != tenantID {
				continue
			}
			updaters = append(updaters, redactNodeUpdater(updater))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(updaters, func(i, j int) bool {
		return updaters[i].UpdatedAt.After(updaters[j].UpdatedAt)
	})
	return updaters, nil
}

func (s *Store) CreateNodeUpdateTask(principal model.Principal, updaterID, clusterNodeName, runtimeID, taskType string, payload map[string]string) (model.NodeUpdateTask, error) {
	taskType = normalizeNodeUpdateTaskType(taskType)
	if taskType == "" {
		return model.NodeUpdateTask{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateNodeUpdateTask(principal, updaterID, clusterNodeName, runtimeID, taskType, payload)
	}

	var task model.NodeUpdateTask
	err := s.withLockedState(true, func(state *model.State) error {
		updater, err := findNodeUpdaterTarget(state, updaterID, clusterNodeName, runtimeID)
		if err != nil {
			return err
		}
		if !principal.IsPlatformAdmin() && strings.TrimSpace(updater.TenantID) != strings.TrimSpace(principal.TenantID) {
			return ErrNotFound
		}
		now := time.Now().UTC()
		task = model.NodeUpdateTask{
			ID:              model.NewID("nodeupdate"),
			TenantID:        strings.TrimSpace(updater.TenantID),
			NodeUpdaterID:   updater.ID,
			MachineID:       updater.MachineID,
			RuntimeID:       updater.RuntimeID,
			NodeKeyID:       updater.NodeKeyID,
			ClusterNodeName: updater.ClusterNodeName,
			Type:            taskType,
			Status:          model.NodeUpdateTaskStatusPending,
			Payload:         cloneMap(payload),
			RequestedByType: principal.ActorType,
			RequestedByID:   principal.ActorID,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		state.NodeUpdateTasks = append(state.NodeUpdateTasks, task)
		return nil
	})
	if err != nil {
		return model.NodeUpdateTask{}, err
	}
	return redactNodeUpdateTask(task), nil
}

func (s *Store) ListNodeUpdateTasks(tenantID string, platformAdmin bool, updaterID, status string) ([]model.NodeUpdateTask, error) {
	if s.usingDatabase() {
		return s.pgListNodeUpdateTasks(tenantID, platformAdmin, updaterID, status)
	}
	tenantID = strings.TrimSpace(tenantID)
	updaterID = strings.TrimSpace(updaterID)
	status = normalizeNodeUpdateTaskStatus(status)
	tasks := []model.NodeUpdateTask{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, task := range state.NodeUpdateTasks {
			if !platformAdmin && strings.TrimSpace(task.TenantID) != tenantID {
				continue
			}
			if updaterID != "" && strings.TrimSpace(task.NodeUpdaterID) != updaterID {
				continue
			}
			if status != "" && strings.TrimSpace(task.Status) != status {
				continue
			}
			tasks = append(tasks, redactNodeUpdateTask(task))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].CreatedAt.After(tasks[j].CreatedAt)
	})
	return tasks, nil
}

func (s *Store) ListPendingNodeUpdateTasks(updaterID string, limit int) ([]model.NodeUpdateTask, error) {
	if s.usingDatabase() {
		return s.pgListPendingNodeUpdateTasks(updaterID, limit)
	}
	updaterID = strings.TrimSpace(updaterID)
	if updaterID == "" {
		return nil, ErrInvalidInput
	}
	if limit <= 0 {
		limit = 10
	}
	tasks := []model.NodeUpdateTask{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, task := range state.NodeUpdateTasks {
			if strings.TrimSpace(task.NodeUpdaterID) != updaterID || task.Status != model.NodeUpdateTaskStatusPending {
				continue
			}
			tasks = append(tasks, redactNodeUpdateTask(task))
			if len(tasks) >= limit {
				break
			}
		}
		return nil
	})
	return tasks, err
}

func (s *Store) ClaimNodeUpdateTask(taskID, updaterID string) (model.NodeUpdateTask, error) {
	if s.usingDatabase() {
		return s.pgClaimNodeUpdateTask(taskID, updaterID)
	}
	var task model.NodeUpdateTask
	err := s.withLockedState(true, func(state *model.State) error {
		index := findNodeUpdateTask(state, taskID)
		if index < 0 {
			return ErrNotFound
		}
		if strings.TrimSpace(state.NodeUpdateTasks[index].NodeUpdaterID) != strings.TrimSpace(updaterID) {
			return ErrNotFound
		}
		switch state.NodeUpdateTasks[index].Status {
		case model.NodeUpdateTaskStatusPending:
			now := time.Now().UTC()
			state.NodeUpdateTasks[index].Status = model.NodeUpdateTaskStatusRunning
			state.NodeUpdateTasks[index].ClaimedAt = &now
			state.NodeUpdateTasks[index].UpdatedAt = now
		case model.NodeUpdateTaskStatusRunning:
		default:
			return ErrConflict
		}
		task = state.NodeUpdateTasks[index]
		return nil
	})
	if err != nil {
		return model.NodeUpdateTask{}, err
	}
	return redactNodeUpdateTask(task), nil
}

func (s *Store) AppendNodeUpdateTaskLog(taskID, updaterID, message string) (model.NodeUpdateTask, error) {
	if s.usingDatabase() {
		return s.pgAppendNodeUpdateTaskLog(taskID, updaterID, message)
	}
	var task model.NodeUpdateTask
	err := s.withLockedState(true, func(state *model.State) error {
		index := findNodeUpdateTask(state, taskID)
		if index < 0 {
			return ErrNotFound
		}
		if strings.TrimSpace(state.NodeUpdateTasks[index].NodeUpdaterID) != strings.TrimSpace(updaterID) {
			return ErrNotFound
		}
		now := time.Now().UTC()
		state.NodeUpdateTasks[index].Logs = append(state.NodeUpdateTasks[index].Logs, model.NodeUpdateTaskLog{
			At:      now,
			Message: strings.TrimSpace(message),
		})
		state.NodeUpdateTasks[index].UpdatedAt = now
		task = state.NodeUpdateTasks[index]
		return nil
	})
	if err != nil {
		return model.NodeUpdateTask{}, err
	}
	return redactNodeUpdateTask(task), nil
}

func (s *Store) CompleteNodeUpdateTask(taskID, updaterID, status, message, errorMessage string) (model.NodeUpdateTask, error) {
	if s.usingDatabase() {
		return s.pgCompleteNodeUpdateTask(taskID, updaterID, status, message, errorMessage)
	}
	var task model.NodeUpdateTask
	err := s.withLockedState(true, func(state *model.State) error {
		index := findNodeUpdateTask(state, taskID)
		if index < 0 {
			return ErrNotFound
		}
		if strings.TrimSpace(state.NodeUpdateTasks[index].NodeUpdaterID) != strings.TrimSpace(updaterID) {
			return ErrNotFound
		}
		if state.NodeUpdateTasks[index].Status != model.NodeUpdateTaskStatusRunning {
			return ErrConflict
		}
		normalizedStatus := normalizeTerminalNodeUpdateTaskStatus(status)
		if normalizedStatus == "" {
			return ErrInvalidInput
		}
		now := time.Now().UTC()
		state.NodeUpdateTasks[index].Status = normalizedStatus
		state.NodeUpdateTasks[index].ResultMessage = strings.TrimSpace(message)
		state.NodeUpdateTasks[index].ErrorMessage = strings.TrimSpace(errorMessage)
		state.NodeUpdateTasks[index].CompletedAt = &now
		state.NodeUpdateTasks[index].UpdatedAt = now
		task = state.NodeUpdateTasks[index]
		return nil
	})
	if err != nil {
		return model.NodeUpdateTask{}, err
	}
	return redactNodeUpdateTask(task), nil
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
	default:
		return ""
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

func findNodeUpdater(state *model.State, id string) int {
	id = strings.TrimSpace(id)
	for idx := range state.NodeUpdaters {
		if strings.TrimSpace(state.NodeUpdaters[idx].ID) == id {
			return idx
		}
	}
	return -1
}

func findNodeUpdaterByMachine(state *model.State, machineID string) int {
	machineID = strings.TrimSpace(machineID)
	if machineID == "" {
		return -1
	}
	for idx := range state.NodeUpdaters {
		if strings.TrimSpace(state.NodeUpdaters[idx].MachineID) == machineID {
			return idx
		}
	}
	return -1
}

func findNodeUpdaterByClusterNode(state *model.State, nodeKeyID, clusterNodeName string) int {
	nodeKeyID = strings.TrimSpace(nodeKeyID)
	clusterNodeName = strings.TrimSpace(clusterNodeName)
	if clusterNodeName == "" {
		return -1
	}
	for idx := range state.NodeUpdaters {
		if strings.TrimSpace(state.NodeUpdaters[idx].ClusterNodeName) != clusterNodeName {
			continue
		}
		if nodeKeyID == "" || strings.TrimSpace(state.NodeUpdaters[idx].NodeKeyID) == nodeKeyID {
			return idx
		}
	}
	return -1
}

func findNodeUpdaterTarget(state *model.State, updaterID, clusterNodeName, runtimeID string) (model.NodeUpdater, error) {
	if index := findNodeUpdater(state, updaterID); index >= 0 {
		return state.NodeUpdaters[index], nil
	}
	clusterNodeName = strings.TrimSpace(clusterNodeName)
	runtimeID = strings.TrimSpace(runtimeID)
	for _, updater := range state.NodeUpdaters {
		if clusterNodeName != "" && strings.TrimSpace(updater.ClusterNodeName) == clusterNodeName {
			return updater, nil
		}
		if runtimeID != "" && strings.TrimSpace(updater.RuntimeID) == runtimeID {
			return updater, nil
		}
	}
	return model.NodeUpdater{}, ErrNotFound
}

func findNodeUpdateTask(state *model.State, id string) int {
	id = strings.TrimSpace(id)
	for idx := range state.NodeUpdateTasks {
		if strings.TrimSpace(state.NodeUpdateTasks[idx].ID) == id {
			return idx
		}
	}
	return -1
}
