package api

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

func buildVisibleClusterNodesFromResolved(
	principal model.Principal,
	resolvedSnapshots []resolvedClusterNodeSnapshot,
	runtimes []model.Runtime,
	machines []model.Machine,
	managedSharedRuntime model.Runtime,
	defaultSharedDisplayRegion string,
) []model.ClusterNode {
	runtimeByClusterNode := buildRuntimeByClusterNodeIndex(runtimes)
	resolvedSnapshots = collapseClusterNodeSnapshots(resolvedSnapshots, runtimeByClusterNode)

	machineByRuntimeID, machineByClusterNode := buildMachineIndexes(machines)
	filtered := make([]model.ClusterNode, 0, len(resolvedSnapshots))
	sharedSnapshots := make([]resolvedClusterNodeSnapshot, 0, len(resolvedSnapshots))

	for _, resolved := range resolvedSnapshots {
		snapshot := resolved.snapshot
		node := snapshot.node
		workloads := resolved.workloads
		runtimeObj, ok := runtimeByClusterNode[node.Name]
		var runtimeForNode *model.Runtime
		if ok {
			runtimeForNode = &runtimeObj
		}

		node.PublicIP = resolveClusterNodePublicIP(node, runtimeForNode)
		if ok {
			node.RuntimeID = runtimeObj.ID
			node.TenantID = runtimeObj.TenantID
		} else if snapshot.runtimeID != "" {
			node.RuntimeID = snapshot.runtimeID
		}
		node.Workloads = workloads

		if principal.IsPlatformAdmin() {
			if machine, ok := selectMachineForSnapshot(snapshot, runtimeForNode, machineByRuntimeID, machineByClusterNode); ok {
				node.Machine = buildClusterNodeMachineView(machine)
				node.Policy = buildClusterNodePolicyView(snapshot, &machine, runtimeForNode)
			} else {
				node.Policy = buildClusterNodePolicyView(snapshot, nil, runtimeForNode)
			}
		}

		if principal.IsPlatformAdmin() || ok {
			filtered = append(filtered, node)
			continue
		}
		if !snapshot.sharedPool && snapshot.runtimeID != "" && !strings.EqualFold(snapshot.runtimeID, tenantSharedRuntimeID) {
			continue
		}
		sharedSnapshots = append(sharedSnapshots, resolvedClusterNodeSnapshot{
			snapshot:  resolved.snapshot,
			workloads: workloads,
		})
	}

	if !principal.IsPlatformAdmin() {
		if sharedNode, ok := buildTenantSharedClusterNode(sharedSnapshots, managedSharedRuntime, defaultSharedDisplayRegion); ok {
			filtered = append(filtered, sharedNode)
		}
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].CreatedAt != nil && filtered[j].CreatedAt != nil && !filtered[i].CreatedAt.Equal(*filtered[j].CreatedAt) {
			return filtered[i].CreatedAt.Before(*filtered[j].CreatedAt)
		}
		return filtered[i].Name < filtered[j].Name
	})
	return filtered
}

func buildRuntimeByClusterNodeIndex(runtimes []model.Runtime) map[string]model.Runtime {
	runtimeByClusterNode := make(map[string]model.Runtime, len(runtimes))
	for _, runtimeObj := range runtimes {
		name := strings.TrimSpace(runtimeObj.ClusterNodeName)
		if name == "" {
			continue
		}
		if existing, ok := runtimeByClusterNode[name]; ok && existing.UpdatedAt.After(runtimeObj.UpdatedAt) {
			continue
		}
		runtimeByClusterNode[name] = runtimeObj
	}
	return runtimeByClusterNode
}

func buildMachineIndexes(machines []model.Machine) (map[string]model.Machine, map[string]model.Machine) {
	machineByRuntimeID := make(map[string]model.Machine, len(machines))
	machineByClusterNode := make(map[string]model.Machine, len(machines))
	for _, machine := range machines {
		if runtimeID := strings.TrimSpace(machine.RuntimeID); runtimeID != "" {
			if existing, ok := machineByRuntimeID[runtimeID]; ok && existing.UpdatedAt.After(machine.UpdatedAt) {
				// Keep the freshest machine projection for this runtime.
			} else {
				machineByRuntimeID[runtimeID] = machine
			}
		}
		if nodeName := strings.TrimSpace(machine.ClusterNodeName); nodeName != "" {
			if existing, ok := machineByClusterNode[nodeName]; ok && existing.UpdatedAt.After(machine.UpdatedAt) {
				continue
			}
			machineByClusterNode[nodeName] = machine
		}
	}
	return machineByRuntimeID, machineByClusterNode
}

func selectMachineForSnapshot(
	snapshot clusterNodeSnapshot,
	runtimeObj *model.Runtime,
	machineByRuntimeID map[string]model.Machine,
	machineByClusterNode map[string]model.Machine,
) (model.Machine, bool) {
	if runtimeObj != nil {
		if machine, ok := machineByRuntimeID[runtimeObj.ID]; ok {
			return machine, true
		}
	}
	if machine, ok := machineByClusterNode[snapshot.node.Name]; ok {
		return machine, true
	}
	if runtimeObj != nil {
		return buildRuntimeSnapshotMachine(*runtimeObj), true
	}
	return model.Machine{}, false
}

func buildRuntimeSnapshotMachine(runtimeObj model.Runtime) model.Machine {
	now := runtimeObj.UpdatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return model.Machine{
		TenantID:        runtimeObj.TenantID,
		Name:            strings.TrimSpace(runtimeObj.MachineName),
		Scope:           model.MachineScopeTenantRuntime,
		ConnectionMode:  runtimeObj.ConnectionMode,
		Status:          runtimeObj.Status,
		Endpoint:        strings.TrimSpace(runtimeObj.Endpoint),
		Labels:          cloneStringMap(runtimeObj.Labels),
		NodeKeyID:       strings.TrimSpace(runtimeObj.NodeKeyID),
		RuntimeID:       runtimeObj.ID,
		RuntimeName:     runtimeObj.Name,
		ClusterNodeName: strings.TrimSpace(runtimeObj.ClusterNodeName),
		Policy: model.MachinePolicy{
			AllowBuilds:             false,
			AllowSharedPool:         model.NormalizeRuntimePoolMode(runtimeObj.Type, runtimeObj.PoolMode) == model.RuntimePoolModeInternalShared,
			DesiredControlPlaneRole: model.MachineControlPlaneRoleNone,
		},
		CreatedAt: runtimeObj.CreatedAt,
		UpdatedAt: now,
	}
}

func buildClusterNodeMachineView(machine model.Machine) *model.ClusterNodeMachine {
	return &model.ClusterNodeMachine{
		ID:             machine.ID,
		Scope:          machine.Scope,
		ConnectionMode: machine.ConnectionMode,
		Status:         machine.Status,
		TenantID:       machine.TenantID,
		RuntimeID:      machine.RuntimeID,
		NodeKeyID:      machine.NodeKeyID,
	}
}

func machineHasSavedPolicy(machine *model.Machine) bool {
	return machine != nil && strings.TrimSpace(machine.ID) != ""
}

func buildClusterNodePolicyView(snapshot clusterNodeSnapshot, machine *model.Machine, runtimeObj *model.Runtime) *model.ClusterNodePolicy {
	effectiveBuilds := effectiveBuildPolicy(snapshot)
	effectiveSharedPool := snapshot.sharedPool
	desiredBuilds := effectiveBuilds
	desiredSharedPool := effectiveSharedPool
	desiredRole := desiredControlPlaneRole(snapshot, machine)
	nodeMode := strings.TrimSpace(firstNodeLabel(snapshot.labels, runtimepkg.NodeModeLabelKey))
	hasSavedPolicy := machineHasSavedPolicy(machine)

	if hasSavedPolicy {
		desiredBuilds = machine.Policy.AllowBuilds
		desiredSharedPool = machine.Policy.AllowSharedPool
	}
	if runtimeObj != nil && !hasSavedPolicy {
		desiredSharedPool = model.NormalizeRuntimePoolMode(runtimeObj.Type, runtimeObj.PoolMode) == model.RuntimePoolModeInternalShared
		if nodeMode == "" {
			nodeMode = strings.TrimSpace(runtimeObj.Type)
		}
	}

	return &model.ClusterNodePolicy{
		AllowBuilds:               desiredBuilds,
		AllowSharedPool:           desiredSharedPool,
		NodeMode:                  nodeMode,
		DesiredControlPlaneRole:   desiredRole,
		EffectiveBuilds:           effectiveBuilds,
		EffectiveSharedPool:       effectiveSharedPool,
		EffectiveControlPlaneRole: effectiveControlPlaneRole(snapshot, desiredRole),
	}
}

func effectiveBuildPolicy(snapshot clusterNodeSnapshot) bool {
	return strings.EqualFold(firstNodeLabel(snapshot.labels, runtimepkg.BuildNodeLabelKey), runtimepkg.BuildNodeLabelValue)
}

func desiredControlPlaneRole(snapshot clusterNodeSnapshot, machine *model.Machine) string {
	if machineHasSavedPolicy(machine) {
		if role := model.NormalizeMachineControlPlaneRole(machine.Policy.DesiredControlPlaneRole); role != "" {
			return role
		}
	}
	if role := model.NormalizeMachineControlPlaneRole(firstNodeLabel(snapshot.labels, runtimepkg.ControlPlaneDesiredRoleKey)); role != "" {
		return role
	}
	return model.MachineControlPlaneRoleNone
}

func effectiveControlPlaneRole(snapshot clusterNodeSnapshot, desiredRole string) string {
	for _, role := range snapshot.node.Roles {
		if strings.EqualFold(strings.TrimSpace(role), "control-plane") {
			return model.MachineControlPlaneRoleMember
		}
	}
	switch model.NormalizeMachineControlPlaneRole(desiredRole) {
	case model.MachineControlPlaneRoleCandidate, model.MachineControlPlaneRoleMember:
		return model.MachineControlPlaneRoleCandidate
	default:
		return model.MachineControlPlaneRoleNone
	}
}
