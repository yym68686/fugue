package store

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

func defaultMachinePolicyForScope(scope string) model.MachinePolicy {
	return model.MachinePolicy{
		AllowBuilds:             false,
		BuildTier:               model.MachineBuildTierMedium,
		AllowSharedPool:         false,
		DesiredControlPlaneRole: model.MachineControlPlaneRoleNone,
	}
}

func seedMachinePolicyFromLabels(scope string, labels map[string]string) model.MachinePolicy {
	policy := defaultMachinePolicyForScope(scope)
	if len(labels) == 0 {
		return policy
	}

	if strings.EqualFold(
		strings.TrimSpace(labels[runtimepkg.BuildNodeLabelKey]),
		runtimepkg.BuildNodeLabelValue,
	) {
		policy.AllowBuilds = true
	}
	if rawTier, ok := labels[runtimepkg.BuildTierLabelKey]; ok {
		if buildTier := model.NormalizeMachineBuildTier(rawTier); buildTier != "" {
			policy.BuildTier = buildTier
		}
	}
	if strings.EqualFold(
		strings.TrimSpace(labels[runtimepkg.SharedPoolLabelKey]),
		runtimepkg.SharedPoolLabelValue,
	) {
		policy.AllowSharedPool = true
	}
	if role := model.NormalizeMachineControlPlaneRole(
		strings.TrimSpace(labels[runtimepkg.ControlPlaneDesiredRoleKey]),
	); role != "" {
		policy.DesiredControlPlaneRole = role
	}
	return policy
}

func normalizeMachinePolicy(scope string, policy model.MachinePolicy) model.MachinePolicy {
	normalized := defaultMachinePolicyForScope(scope)
	normalized.AllowBuilds = policy.AllowBuilds
	if buildTier := model.NormalizeMachineBuildTier(policy.BuildTier); buildTier != "" {
		normalized.BuildTier = buildTier
	}
	normalized.AllowSharedPool = policy.AllowSharedPool
	if role := model.NormalizeMachineControlPlaneRole(policy.DesiredControlPlaneRole); role != "" {
		normalized.DesiredControlPlaneRole = role
	}
	return normalized
}

func normalizeMachineForRead(machine *model.Machine) {
	if machine == nil {
		return
	}
	scope := model.NormalizeMachineScope(machine.Scope)
	if scope == "" {
		scope = model.MachineScopeTenantRuntime
	}
	machine.Scope = scope
	machine.Policy = normalizeMachinePolicy(scope, machine.Policy)
}

func machinePolicyFromRuntime(runtime model.Runtime, existing *model.Machine) model.MachinePolicy {
	policy := seedMachinePolicyFromLabels(model.MachineScopeTenantRuntime, runtime.Labels)
	if existing != nil {
		policy = existing.Policy
	}
	policy.AllowSharedPool = model.NormalizeRuntimePoolMode(runtime.Type, runtime.PoolMode) == model.RuntimePoolModeInternalShared
	return normalizeMachinePolicy(model.MachineScopeTenantRuntime, policy)
}

func machineFromRuntime(runtime model.Runtime, existing *model.Machine, now time.Time) model.Machine {
	backfillRuntimeMetadata(&runtime, model.Machine{})
	machine := model.Machine{
		ID:                model.NewID("machine"),
		TenantID:          runtime.TenantID,
		Name:              normalizedMachineName(runtime.MachineName, runtime.Name, runtime.Endpoint),
		Scope:             model.MachineScopeTenantRuntime,
		ConnectionMode:    runtime.ConnectionMode,
		Status:            runtime.Status,
		Endpoint:          strings.TrimSpace(runtime.Endpoint),
		Labels:            cloneMap(runtime.Labels),
		NodeKeyID:         strings.TrimSpace(runtime.NodeKeyID),
		RuntimeID:         runtime.ID,
		RuntimeName:       runtime.Name,
		ClusterNodeName:   strings.TrimSpace(runtime.ClusterNodeName),
		FingerprintPrefix: strings.TrimSpace(runtime.FingerprintPrefix),
		FingerprintHash:   strings.TrimSpace(runtime.FingerprintHash),
		Policy:            machinePolicyFromRuntime(runtime, existing),
		CreatedAt:         runtime.CreatedAt,
		UpdatedAt:         now,
	}
	if machine.ConnectionMode == "" {
		machine.ConnectionMode = runtimeConnectionMode(runtime.Type)
	}
	switch {
	case runtime.LastSeenAt != nil:
		t := runtime.LastSeenAt.UTC()
		machine.LastSeenAt = &t
	case runtime.LastHeartbeatAt != nil:
		t := runtime.LastHeartbeatAt.UTC()
		machine.LastSeenAt = &t
	}
	if existing != nil {
		machine.ID = existing.ID
		if !existing.CreatedAt.IsZero() {
			machine.CreatedAt = existing.CreatedAt
		}
		if existing.TenantID != "" && machine.TenantID == "" {
			machine.TenantID = existing.TenantID
		}
	}
	normalizeMachineForRead(&machine)
	return machine
}

func buildPlatformMachineRecord(nodeKeyID, nodeName, endpoint string, labels map[string]string, machineName, machineFingerprint string, existing *model.Machine, now time.Time) (model.Machine, error) {
	normalizedNodeName, err := normalizeClusterNodeName(nodeName)
	if err != nil {
		return model.Machine{}, err
	}
	machineName = normalizedMachineName(machineName, normalizedNodeName, endpoint)
	machineFingerprint = normalizedMachineFingerprint(machineFingerprint, machineName, normalizedNodeName, endpoint)

	machine := model.Machine{
		ID:                model.NewID("machine"),
		Name:              machineName,
		Scope:             model.MachineScopePlatformNode,
		ConnectionMode:    model.MachineConnectionModeCluster,
		Status:            model.RuntimeStatusActive,
		Endpoint:          strings.TrimSpace(endpoint),
		Labels:            cloneMap(labels),
		NodeKeyID:         strings.TrimSpace(nodeKeyID),
		ClusterNodeName:   normalizedNodeName,
		FingerprintPrefix: model.SecretPrefix(machineFingerprint),
		FingerprintHash:   model.HashSecret(machineFingerprint),
		Policy:            seedMachinePolicyFromLabels(model.MachineScopePlatformNode, labels),
		CreatedAt:         now,
		UpdatedAt:         now,
		LastSeenAt:        &now,
	}
	if existing != nil {
		machine.ID = existing.ID
		if !existing.CreatedAt.IsZero() {
			machine.CreatedAt = existing.CreatedAt
		}
		if machine.NodeKeyID == "" {
			machine.NodeKeyID = strings.TrimSpace(existing.NodeKeyID)
		}
		if machine.Endpoint == "" {
			machine.Endpoint = strings.TrimSpace(existing.Endpoint)
		}
		machine.Policy = normalizeMachinePolicy(model.MachineScopePlatformNode, existing.Policy)
	}
	normalizeMachineForRead(&machine)
	return machine, nil
}

func buildPlatformMachine(key model.NodeKey, nodeName, endpoint string, labels map[string]string, machineName, machineFingerprint string, existing *model.Machine, now time.Time) (model.Machine, error) {
	return buildPlatformMachineRecord(key.ID, nodeName, endpoint, labels, machineName, machineFingerprint, existing, now)
}

func findMachine(state *model.State, id string) int {
	id = strings.TrimSpace(id)
	if id == "" {
		return -1
	}
	for idx, machine := range state.Machines {
		if machine.ID == id {
			return idx
		}
	}
	return -1
}

func findMachineByRuntimeID(state *model.State, runtimeID string) int {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return -1
	}
	for idx, machine := range state.Machines {
		if strings.TrimSpace(machine.RuntimeID) == runtimeID {
			return idx
		}
	}
	return -1
}

func findMachineByClusterNodeName(state *model.State, nodeName string) int {
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" {
		return -1
	}
	for idx, machine := range state.Machines {
		if strings.EqualFold(strings.TrimSpace(machine.ClusterNodeName), nodeName) {
			return idx
		}
	}
	return -1
}

func findPlatformMachineByClusterNodeName(state *model.State, nodeName string) int {
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" {
		return -1
	}
	for idx, machine := range state.Machines {
		if model.NormalizeMachineScope(machine.Scope) != model.MachineScopePlatformNode {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(machine.ClusterNodeName), nodeName) {
			continue
		}
		return idx
	}
	return -1
}

func findMachineByFingerprintHash(state *model.State, tenantID, fingerprintHash string) int {
	fingerprintHash = strings.TrimSpace(fingerprintHash)
	if fingerprintHash == "" {
		return -1
	}
	tenantID = strings.TrimSpace(tenantID)
	for idx, machine := range state.Machines {
		if strings.TrimSpace(machine.FingerprintHash) != fingerprintHash {
			continue
		}
		if strings.TrimSpace(machine.TenantID) == tenantID {
			return idx
		}
	}
	return -1
}

func findRuntimeByClusterNodeName(state *model.State, nodeName string) int {
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" {
		return -1
	}
	for idx, runtime := range state.Runtimes {
		if strings.EqualFold(strings.TrimSpace(runtime.ClusterNodeName), nodeName) {
			return idx
		}
	}
	return -1
}

func upsertMachineForRuntimeState(state *model.State, runtime model.Runtime, now time.Time) model.Machine {
	existingIndex := findMachineByRuntimeID(state, runtime.ID)
	if existingIndex < 0 {
		existingIndex = findMachineByFingerprintHash(state, runtime.TenantID, runtime.FingerprintHash)
	}
	var existing *model.Machine
	if existingIndex >= 0 {
		existing = &state.Machines[existingIndex]
	}
	machine := machineFromRuntime(runtime, existing, now)
	if existingIndex >= 0 {
		state.Machines[existingIndex] = machine
	} else {
		state.Machines = append(state.Machines, machine)
	}
	return machine
}

func upsertPlatformMachineState(state *model.State, nodeKeyID, nodeName, endpoint string, labels map[string]string, machineName, machineFingerprint string, now time.Time) (model.Machine, error) {
	normalizedNodeName, err := normalizeClusterNodeName(nodeName)
	if err != nil {
		return model.Machine{}, err
	}
	machineFingerprint = normalizedMachineFingerprint(machineFingerprint, machineName, normalizedNodeName, endpoint)
	fingerprintHash := model.HashSecret(machineFingerprint)

	existingIndex := findPlatformMachineByClusterNodeName(state, normalizedNodeName)
	if existingIndex < 0 {
		existingIndex = findMachineByFingerprintHash(state, "", fingerprintHash)
	}
	if existingIndex < 0 && strings.TrimSpace(nodeKeyID) != "" {
		for idx, machine := range state.Machines {
			if strings.TrimSpace(machine.NodeKeyID) != strings.TrimSpace(nodeKeyID) {
				continue
			}
			if model.NormalizeMachineScope(machine.Scope) != model.MachineScopePlatformNode {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(machine.ClusterNodeName), normalizedNodeName) {
				existingIndex = idx
				break
			}
		}
	}

	var existing *model.Machine
	if existingIndex >= 0 {
		existing = &state.Machines[existingIndex]
	}
	machine, err := buildPlatformMachineRecord(nodeKeyID, normalizedNodeName, endpoint, labels, machineName, machineFingerprint, existing, now)
	if err != nil {
		return model.Machine{}, err
	}
	if existingIndex >= 0 {
		state.Machines[existingIndex] = machine
	} else {
		state.Machines = append(state.Machines, machine)
	}
	return machine, nil
}

func (s *Store) ListMachines(tenantID string, platformAdmin bool) ([]model.Machine, error) {
	if s.usingDatabase() {
		return s.pgListMachines(tenantID, platformAdmin)
	}
	var machines []model.Machine
	err := s.withLockedState(false, func(state *model.State) error {
		for _, machine := range state.Machines {
			normalizeMachineForRead(&machine)
			if platformAdmin || strings.TrimSpace(machine.TenantID) == strings.TrimSpace(tenantID) {
				machines = append(machines, machine)
			}
		}
		sort.Slice(machines, func(i, j int) bool {
			return machines[i].CreatedAt.Before(machines[j].CreatedAt)
		})
		return nil
	})
	return machines, err
}

func (s *Store) ListMachinesByNodeKey(nodeKeyID, tenantID string, platformAdmin bool) ([]model.Machine, error) {
	nodeKeyID = strings.TrimSpace(nodeKeyID)
	if nodeKeyID == "" {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgListMachinesByNodeKey(nodeKeyID, tenantID, platformAdmin)
	}
	var machines []model.Machine
	err := s.withLockedState(false, func(state *model.State) error {
		for _, machine := range state.Machines {
			if strings.TrimSpace(machine.NodeKeyID) != nodeKeyID {
				continue
			}
			normalizeMachineForRead(&machine)
			if platformAdmin || strings.TrimSpace(machine.TenantID) == strings.TrimSpace(tenantID) {
				machines = append(machines, machine)
			}
		}
		sort.Slice(machines, func(i, j int) bool {
			return machines[i].CreatedAt.Before(machines[j].CreatedAt)
		})
		return nil
	})
	return machines, err
}

func (s *Store) GetMachineByClusterNodeName(name string) (model.Machine, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return model.Machine{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetMachineByClusterNodeName(name)
	}
	var machine model.Machine
	err := s.withLockedState(false, func(state *model.State) error {
		ensureRuntimeMetadata(state)
		if idx := findMachineByClusterNodeName(state, name); idx >= 0 {
			machine = state.Machines[idx]
			normalizeMachineForRead(&machine)
			return nil
		}
		runtimeIndex := findRuntimeByClusterNodeName(state, name)
		if runtimeIndex < 0 {
			return ErrNotFound
		}
		machine = machineFromRuntime(state.Runtimes[runtimeIndex], nil, state.Runtimes[runtimeIndex].UpdatedAt)
		return nil
	})
	return machine, err
}

func (s *Store) EnsurePlatformMachineForClusterNode(nodeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.Machine, error) {
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" {
		return model.Machine{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgEnsurePlatformMachineForClusterNode(nodeName, endpoint, labels, machineName, machineFingerprint)
	}
	var machine model.Machine
	err := s.withLockedState(true, func(state *model.State) error {
		var err error
		machine, err = upsertPlatformMachineState(state, "", nodeName, endpoint, labels, machineName, machineFingerprint, time.Now().UTC())
		return err
	})
	return machine, err
}

func (s *Store) EnsureMachineForRuntime(runtimeID string) (model.Machine, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return model.Machine{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgEnsureMachineForRuntime(runtimeID)
	}
	var machine model.Machine
	err := s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)
		runtimeIndex := findRuntime(state, runtimeID)
		if runtimeIndex < 0 {
			return ErrNotFound
		}
		runtimeObj := state.Runtimes[runtimeIndex]
		if runtimeObj.Type != model.RuntimeTypeManagedOwned && runtimeObj.Type != model.RuntimeTypeExternalOwned {
			return ErrConflict
		}
		machine = upsertMachineForRuntimeState(state, runtimeObj, time.Now().UTC())
		return nil
	})
	return machine, err
}

func (s *Store) SetMachinePolicyByClusterNodeName(name string, policy model.MachinePolicy) (model.Machine, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return model.Machine{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgSetMachinePolicyByClusterNodeName(name, policy)
	}
	var machine model.Machine
	err := s.withLockedState(true, func(state *model.State) error {
		ensureRuntimeMetadata(state)
		now := time.Now().UTC()
		machineIndex := findMachineByClusterNodeName(state, name)
		if machineIndex < 0 {
			runtimeIndex := findRuntimeByClusterNodeName(state, name)
			if runtimeIndex < 0 {
				return ErrNotFound
			}
			machine = upsertMachineForRuntimeState(state, state.Runtimes[runtimeIndex], now)
			machineIndex = findMachine(state, machine.ID)
			if machineIndex < 0 {
				return ErrNotFound
			}
		}
		next := state.Machines[machineIndex]
		next.Policy = normalizeMachinePolicy(next.Scope, policy)
		next.UpdatedAt = now
		state.Machines[machineIndex] = next
		machine = next
		return nil
	})
	return machine, err
}

func (s *Store) attachPlatformClusterMachine(key model.NodeKey, nodeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.Machine, error) {
	if s.usingDatabase() {
		return s.pgAttachPlatformClusterMachine(key, nodeName, endpoint, labels, machineName, machineFingerprint)
	}
	var machine model.Machine
	err := s.withLockedState(true, func(state *model.State) error {
		var err error
		machine, err = upsertPlatformMachineState(state, key.ID, nodeName, endpoint, labels, machineName, machineFingerprint, time.Now().UTC())
		return err
	})
	return machine, err
}

func (s *Store) BootstrapClusterAttachment(secret, nodeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.NodeKey, model.Machine, *model.Runtime, error) {
	key, err := s.AuthenticateNodeKey(secret)
	if err != nil {
		return model.NodeKey{}, model.Machine{}, nil, err
	}
	if key.Scope == model.NodeKeyScopePlatformNode {
		machine, err := s.attachPlatformClusterMachine(key, nodeName, endpoint, labels, machineName, machineFingerprint)
		if err != nil {
			return model.NodeKey{}, model.Machine{}, nil, err
		}
		return key, machine, nil, nil
	}
	key, runtimeObj, err := s.BootstrapClusterNode(secret, nodeName, endpoint, labels, machineName, machineFingerprint)
	if err != nil {
		return model.NodeKey{}, model.Machine{}, nil, err
	}
	machine, err := s.EnsureMachineForRuntime(runtimeObj.ID)
	if err != nil {
		return model.NodeKey{}, model.Machine{}, nil, err
	}
	return key, machine, &runtimeObj, nil
}
