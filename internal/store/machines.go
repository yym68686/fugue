package store

import (
	"strings"
	"time"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/model/edgecohort"
	runtimepkg "fugue/internal/runtime"
)

const kubernetesControlPlaneRoleLabelKey = "node-role.kubernetes.io/control-plane"

func defaultMachinePolicyForScope(scope string) model.MachinePolicy {
	policy := model.MachinePolicy{
		AllowBuilds:             false,
		AllowSharedPool:         false,
		DesiredControlPlaneRole: model.MachineControlPlaneRoleNone,
	}
	if model.NormalizeMachineScope(scope) == model.MachineScopeTenantRuntime {
		policy.AllowAppRuntime = true
	}
	return policy
}

func machinePolicyEquals(a, b model.MachinePolicy) bool {
	return a.AllowAppRuntime == b.AllowAppRuntime &&
		a.AllowBuilds == b.AllowBuilds &&
		a.AllowSharedPool == b.AllowSharedPool &&
		a.AllowEdge == b.AllowEdge &&
		a.AllowDNS == b.AllowDNS &&
		a.AllowInternalMaintenance == b.AllowInternalMaintenance &&
		a.DesiredControlPlaneRole == b.DesiredControlPlaneRole
}

func seedMachinePolicyFromLabels(scope string, labels map[string]string) model.MachinePolicy {
	policy := defaultMachinePolicyForScope(scope)
	if len(labels) == 0 {
		return policy
	}

	if strings.EqualFold(
		strings.TrimSpace(labels[runtimepkg.BuildNodeLabelKey]),
		runtimepkg.BuildNodeLabelValue,
	) || strings.EqualFold(
		strings.TrimSpace(labels[runtimepkg.BuilderRoleLabelKey]),
		runtimepkg.NodeRoleLabelValue,
	) {
		policy.AllowBuilds = true
	}
	if strings.EqualFold(
		strings.TrimSpace(labels[runtimepkg.SharedPoolLabelKey]),
		runtimepkg.SharedPoolLabelValue,
	) {
		policy.AllowSharedPool = true
		policy.AllowBuilds = true
		policy.AllowAppRuntime = true
	}
	if strings.EqualFold(strings.TrimSpace(labels[runtimepkg.AppRuntimeRoleLabelKey]), runtimepkg.NodeRoleLabelValue) {
		policy.AllowAppRuntime = true
	}
	if strings.EqualFold(strings.TrimSpace(labels[runtimepkg.EdgeRoleLabelKey]), runtimepkg.NodeRoleLabelValue) {
		policy.AllowEdge = true
	}
	if strings.EqualFold(strings.TrimSpace(labels[runtimepkg.DNSRoleLabelKey]), runtimepkg.NodeRoleLabelValue) {
		policy.AllowDNS = true
	}
	if strings.EqualFold(strings.TrimSpace(labels[runtimepkg.InternalMaintenanceLabelKey]), runtimepkg.NodeRoleLabelValue) {
		policy.AllowInternalMaintenance = true
	}
	if rawRole, ok := labels[runtimepkg.ControlPlaneDesiredRoleKey]; ok {
		if role := model.NormalizeMachineControlPlaneRole(strings.TrimSpace(rawRole)); role != "" {
			policy.DesiredControlPlaneRole = role
		}
	} else if model.NormalizeMachineScope(scope) == model.MachineScopePlatformNode {
		if _, ok := labels[kubernetesControlPlaneRoleLabelKey]; ok {
			policy.DesiredControlPlaneRole = model.MachineControlPlaneRoleMember
		}
	}
	return policy
}

func normalizeMachinePolicy(scope string, policy model.MachinePolicy) model.MachinePolicy {
	normalized := defaultMachinePolicyForScope(scope)
	normalized.AllowAppRuntime = policy.AllowAppRuntime
	normalized.AllowBuilds = policy.AllowBuilds
	normalized.AllowSharedPool = policy.AllowSharedPool
	normalized.AllowEdge = policy.AllowEdge
	normalized.AllowDNS = policy.AllowDNS
	normalized.AllowInternalMaintenance = policy.AllowInternalMaintenance
	if normalized.AllowSharedPool {
		normalized.AllowAppRuntime = true
	}
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

// NeedsLegacyPlatformMachinePolicyBackfill reports whether an existing
// platform-node machine record still carries the old bootstrap seed defaults
// and can be safely refreshed from the current live node labels.
func NeedsLegacyPlatformMachinePolicyBackfill(machine model.Machine, labels map[string]string) bool {
	if model.NormalizeMachineScope(machine.Scope) != model.MachineScopePlatformNode {
		return false
	}
	if strings.TrimSpace(machine.TenantID) != "" || strings.TrimSpace(machine.RuntimeID) != "" {
		return false
	}
	if strings.TrimSpace(machine.NodeKeyID) != "" {
		return false
	}
	if machine.CreatedAt.IsZero() || machine.UpdatedAt.IsZero() || !machine.CreatedAt.Equal(machine.UpdatedAt) {
		return false
	}

	legacyPolicy := normalizeMachinePolicy(model.MachineScopePlatformNode, machine.Policy)
	defaultPolicy := defaultMachinePolicyForScope(model.MachineScopePlatformNode)
	if !machinePolicyEquals(legacyPolicy, defaultPolicy) {
		return false
	}

	seededPolicy := seedMachinePolicyFromLabels(model.MachineScopePlatformNode, labels)
	return !machinePolicyEquals(seededPolicy, legacyPolicy)
}

func machinePolicyFromRuntime(runtime model.Runtime, existing *model.Machine) model.MachinePolicy {
	if existing != nil {
		policy := existing.Policy
		policy.AllowSharedPool = model.NormalizeRuntimePoolMode(runtime.Type, runtime.PoolMode) == model.RuntimePoolModeInternalShared
		if policy.AllowSharedPool {
			policy.AllowAppRuntime = true
		}
		// Sharing application capacity does not grant build permission. The
		// saved role must survive runtime heartbeats and pool-mode refreshes.
		return normalizeMachinePolicy(model.MachineScopeTenantRuntime, policy)
	}

	policy := seedMachinePolicyFromLabels(model.MachineScopeTenantRuntime, runtime.Labels)
	if labelsRequestDedicatedTenantRuntimePolicy(runtime.Labels) {
		policy.AllowAppRuntime = false
	}
	policy.AllowSharedPool = model.NormalizeRuntimePoolMode(runtime.Type, runtime.PoolMode) == model.RuntimePoolModeInternalShared
	if policy.AllowSharedPool {
		policy.AllowAppRuntime = true
		policy.AllowBuilds = true
	}
	return normalizeMachinePolicy(model.MachineScopeTenantRuntime, policy)
}

func labelsRequestDedicatedTenantRuntimePolicy(labels map[string]string) bool {
	if len(labels) == 0 {
		return false
	}
	hasDedicatedRole := strings.EqualFold(strings.TrimSpace(labels[runtimepkg.EdgeRoleLabelKey]), runtimepkg.NodeRoleLabelValue) ||
		strings.EqualFold(strings.TrimSpace(labels[runtimepkg.DNSRoleLabelKey]), runtimepkg.NodeRoleLabelValue) ||
		strings.EqualFold(strings.TrimSpace(labels[runtimepkg.InternalMaintenanceLabelKey]), runtimepkg.NodeRoleLabelValue)
	if !hasDedicatedRole {
		return false
	}
	hasAppRuntimeRole := strings.EqualFold(strings.TrimSpace(labels[runtimepkg.AppRuntimeRoleLabelKey]), runtimepkg.NodeRoleLabelValue)
	hasBuildRole := strings.EqualFold(strings.TrimSpace(labels[runtimepkg.BuildNodeLabelKey]), runtimepkg.BuildNodeLabelValue) ||
		strings.EqualFold(strings.TrimSpace(labels[runtimepkg.BuilderRoleLabelKey]), runtimepkg.NodeRoleLabelValue)
	hasSharedPoolRole := strings.EqualFold(strings.TrimSpace(labels[runtimepkg.SharedPoolLabelKey]), runtimepkg.SharedPoolLabelValue)
	return !hasAppRuntimeRole && !hasBuildRole && !hasSharedPoolRole
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
		preserveMachineEdgeEnrollment(&machine, *existing)
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
		if NeedsLegacyPlatformMachinePolicyBackfill(*existing, labels) {
			machine.Policy = seedMachinePolicyFromLabels(model.MachineScopePlatformNode, labels)
		} else {
			machine.Policy = normalizeMachinePolicy(model.MachineScopePlatformNode, existing.Policy)
		}
		preserveMachineEdgeEnrollment(&machine, *existing)
	}
	normalizeMachineForRead(&machine)
	return machine, nil
}

func (s *Store) ListMachines(tenantID string, platformAdmin bool) ([]model.Machine, error) {
	return s.pgListMachines(tenantID, platformAdmin)
}

func (s *Store) ListMachinesByNodeKey(nodeKeyID, tenantID string, platformAdmin bool) ([]model.Machine, error) {
	nodeKeyID = strings.TrimSpace(nodeKeyID)
	if nodeKeyID == "" {
		return nil, ErrInvalidInput
	}
	return s.pgListMachinesByNodeKey(nodeKeyID, tenantID, platformAdmin)
}

func (s *Store) GetMachineByClusterNodeName(name string) (model.Machine, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return model.Machine{}, ErrInvalidInput
	}
	return s.pgGetMachineByClusterNodeName(name)
}

func (s *Store) EnsurePlatformMachineForClusterNode(nodeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.Machine, error) {
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" {
		return model.Machine{}, ErrInvalidInput
	}
	return s.pgEnsurePlatformMachineForClusterNode(nodeName, endpoint, labels, machineName, machineFingerprint)
}

func (s *Store) EnsureMachineForRuntime(runtimeID string) (model.Machine, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return model.Machine{}, ErrInvalidInput
	}
	return s.pgEnsureMachineForRuntime(runtimeID)
}

func (s *Store) SetMachinePolicyByClusterNodeName(name string, policy model.MachinePolicy) (model.Machine, error) {
	return s.setMachinePolicyByClusterNodeName(name, policy, "")
}

// SetMachineEdgePolicyByClusterNodeName records explicit enrollment intent
// together with its role policy. This does not publish serving configuration.
func (s *Store) SetMachineEdgePolicyByClusterNodeName(name string, policy model.MachinePolicy, groupID string) (model.Machine, error) {
	if !policy.AllowEdge || !edgetopology.ValidAuthorityID(groupID) {
		return model.Machine{}, ErrInvalidInput
	}
	return s.setMachinePolicyByClusterNodeName(name, policy, groupID)
}

func (s *Store) SetMachineEdgeEnrollment(name string, policy model.MachinePolicy, enrollment model.EdgeNodeEnrollment) (model.Machine, error) {
	if err := enrollment.Validate(name, enrollment.EdgeGroupID, time.Now().UTC()); err != nil || !policy.AllowEdge {
		return model.Machine{}, ErrInvalidInput
	}
	return s.pgSetMachinePolicyWithEdgeEnrollment(name, policy, enrollment.EdgeGroupID, &enrollment)
}

func (s *Store) setMachinePolicyByClusterNodeName(name string, policy model.MachinePolicy, edgeGroupID string) (model.Machine, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return model.Machine{}, ErrInvalidInput
	}
	return s.pgSetMachinePolicyWithEdgeGroup(name, policy, edgeGroupID)
}

func applyMachineEdgeEnrollment(machine *model.Machine, groupID string) error {
	if groupID == "" {
		return nil
	}
	if previous := machine.Labels[runtimepkg.EdgeGroupIDLabelKey]; previous != "" && previous != groupID {
		return ErrInvalidInput
	}
	machine.Labels = cloneMap(machine.Labels)
	if machine.Labels == nil {
		machine.Labels = map[string]string{}
	}
	machine.Labels[runtimepkg.EdgeGroupIDLabelKey] = groupID
	machine.Labels[runtimepkg.EdgeWorkloadLabelKey] = runtimepkg.EdgeWorkloadDynamicValue
	if machine.Labels[model.MachineEdgeEnrollmentLabel] == "" || machine.Labels[edgecohort.PhaseLabel] != edgecohort.Admitted {
		machine.Labels[edgecohort.PhaseLabel] = edgecohort.Preparing
	}
	return nil
}

// Runtime discovery updates observations, not an administrator's enrollment
// binding. Rejoining or changing pool scope must retain this explicit intent.
func preserveMachineEdgeEnrollment(machine *model.Machine, previous model.Machine) {
	groupID := previous.Labels[runtimepkg.EdgeGroupIDLabelKey]
	if !edgetopology.ValidAuthorityID(groupID) || previous.Labels[runtimepkg.EdgeWorkloadLabelKey] != runtimepkg.EdgeWorkloadDynamicValue {
		return
	}
	if machine.Labels == nil {
		machine.Labels = map[string]string{}
	}
	machine.Labels[runtimepkg.EdgeGroupIDLabelKey] = groupID
	machine.Labels[runtimepkg.EdgeWorkloadLabelKey] = runtimepkg.EdgeWorkloadDynamicValue
	if intent := previous.Labels[model.MachineEdgeEnrollmentLabel]; intent != "" {
		machine.Labels[model.MachineEdgeEnrollmentLabel] = intent
		machine.Labels[edgecohort.PhaseLabel] = previous.Labels[edgecohort.PhaseLabel]
	}
}

func (s *Store) attachPlatformClusterMachine(key model.NodeKey, nodeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.Machine, error) {
	return s.pgAttachPlatformClusterMachine(key, nodeName, endpoint, labels, machineName, machineFingerprint)
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
