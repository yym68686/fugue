package api

import (
	"context"
	"net/http"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

const legacyBuildTierLabelKey = "fugue.io/build-tier"

type setClusterNodePolicyRequest struct {
	AllowAppRuntime          *bool   `json:"allow_app_runtime"`
	AllowBuilds              *bool   `json:"allow_builds"`
	AllowSharedPool          *bool   `json:"allow_shared_pool"`
	AllowEdge                *bool   `json:"allow_edge"`
	AllowDNS                 *bool   `json:"allow_dns"`
	AllowInternalMaintenance *bool   `json:"allow_internal_maintenance"`
	DesiredControlPlaneRole  *string `json:"desired_control_plane_role"`
}

func (s *Server) handleSetClusterNodePolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can manage cluster node policy")
		return
	}

	nodeName := strings.TrimSpace(r.PathValue("name"))
	if nodeName == "" {
		httpx.WriteError(w, http.StatusBadRequest, "cluster node name is required")
		return
	}

	var req setClusterNodePolicyRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !req.hasUpdates() {
		httpx.WriteError(w, http.StatusBadRequest, "at least one policy field must be provided")
		return
	}

	machine, err := s.store.GetMachineByClusterNodeName(nodeName)
	if err == store.ErrNotFound {
		machine, err = s.ensurePlatformMachineByClusterNodeName(r.Context(), nodeName)
	}
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	runtimeObj, err := s.loadClusterNodePolicyRuntime(machine)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	if req.AllowSharedPool != nil && runtimeObj != nil {
		if runtimeObj.Type != model.RuntimeTypeManagedOwned || strings.TrimSpace(runtimeObj.TenantID) == "" {
			httpx.WriteError(w, http.StatusBadRequest, "shared pool can only be managed on managed-owned cluster runtimes")
			return
		}
		desiredPoolMode := model.RuntimePoolModeDedicated
		if *req.AllowSharedPool {
			desiredPoolMode = model.RuntimePoolModeInternalShared
		}
		updatedRuntime, err := s.store.SetRuntimePoolMode(runtimeObj.ID, desiredPoolMode)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		runtimeObj = &updatedRuntime
		machine, err = s.store.GetMachineByClusterNodeName(nodeName)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
	}

	nextPolicy, err := req.mergeInto(machine.Policy, runtimeObj)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	machine, err = s.store.SetMachinePolicyByClusterNodeName(nodeName, nextPolicy)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	nodeReconciled, reconcileErr := s.reconcileClusterNodePolicy(r.Context(), machine, runtimeObj)
	if req.AllowSharedPool != nil {
		s.trySyncManagedSharedLocationRuntimes(r.Context())
	}
	if nodeReconciled {
		s.clusterNodeInventoryCache.clear(clusterNodeInventoryCacheKey)
		if _, err := s.refreshClusterNodeInventory(r.Context()); err != nil && s.log != nil {
			s.log.Printf("cluster node policy refresh %s: %v", nodeName, err)
		}
	}

	clusterNode, err := s.loadManagedClusterNodeView(r.Context(), principal, nodeName)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	metadata := map[string]string{
		"cluster_node_name":          nodeName,
		"allow_app_runtime":          boolString(machine.Policy.AllowAppRuntime),
		"allow_builds":               boolString(machine.Policy.AllowBuilds),
		"allow_shared_pool":          boolString(machine.Policy.AllowSharedPool),
		"allow_edge":                 boolString(machine.Policy.AllowEdge),
		"allow_dns":                  boolString(machine.Policy.AllowDNS),
		"allow_internal_maintenance": boolString(machine.Policy.AllowInternalMaintenance),
		"desired_control_plane_role": machine.Policy.DesiredControlPlaneRole,
	}
	if clusterNode.Policy != nil {
		metadata["effective_control_plane_role"] = clusterNode.Policy.EffectiveControlPlaneRole
	}
	if runtimeObj != nil {
		metadata["runtime_id"] = runtimeObj.ID
	}
	if reconcileErr != "" {
		metadata["reconcile_error"] = reconcileErr
	}
	s.appendAudit(principal, "cluster.node.policy", "cluster_node", nodeName, machine.TenantID, metadata)

	response := map[string]any{
		"cluster_node":    clusterNode,
		"node_reconciled": nodeReconciled,
	}
	if reconcileErr != "" {
		response["reconcile_error"] = reconcileErr
	}
	httpx.WriteJSON(w, http.StatusOK, response)
}

func (req setClusterNodePolicyRequest) hasUpdates() bool {
	return req.AllowAppRuntime != nil ||
		req.AllowBuilds != nil ||
		req.AllowSharedPool != nil ||
		req.AllowEdge != nil ||
		req.AllowDNS != nil ||
		req.AllowInternalMaintenance != nil ||
		req.DesiredControlPlaneRole != nil
}

func (req setClusterNodePolicyRequest) mergeInto(current model.MachinePolicy, runtimeObj *model.Runtime) (model.MachinePolicy, error) {
	next := current
	if req.AllowAppRuntime != nil {
		next.AllowAppRuntime = *req.AllowAppRuntime
	}
	if req.AllowBuilds != nil {
		next.AllowBuilds = *req.AllowBuilds
	}
	if req.AllowSharedPool != nil && runtimeObj == nil {
		next.AllowSharedPool = *req.AllowSharedPool
	}
	if req.AllowEdge != nil {
		next.AllowEdge = *req.AllowEdge
	}
	if req.AllowDNS != nil {
		next.AllowDNS = *req.AllowDNS
	}
	if req.AllowInternalMaintenance != nil {
		next.AllowInternalMaintenance = *req.AllowInternalMaintenance
	}
	if req.DesiredControlPlaneRole != nil {
		role := model.MachineControlPlaneRoleNone
		if trimmed := strings.TrimSpace(*req.DesiredControlPlaneRole); trimmed != "" {
			role = model.NormalizeMachineControlPlaneRole(trimmed)
			if role == "" {
				return model.MachinePolicy{}, store.ErrInvalidInput
			}
		}
		next.DesiredControlPlaneRole = role
	}
	return next, nil
}

func (s *Server) loadClusterNodePolicyRuntime(machine model.Machine) (*model.Runtime, error) {
	runtimeID := strings.TrimSpace(machine.RuntimeID)
	if runtimeID == "" {
		return nil, nil
	}
	runtimeObj, err := s.store.GetRuntime(runtimeID)
	if err != nil {
		if err == store.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &runtimeObj, nil
}

func (s *Server) reconcileClusterNodePolicy(ctx context.Context, machine model.Machine, runtimeObj *model.Runtime) (bool, string) {
	nodeReconciled := false
	var reconcileErr string

	if runtimeObj != nil {
		reconciled, err := s.reconcileRuntimeClusterNode(ctx, *runtimeObj)
		if err != nil {
			reconcileErr = err.Error()
		} else if reconciled {
			nodeReconciled = true
		}
	}

	reconciled, err := s.reconcileMachineClusterNode(ctx, machine, runtimeObj)
	if err != nil {
		if reconcileErr == "" {
			reconcileErr = err.Error()
		} else if s.log != nil {
			s.log.Printf("cluster node policy reconcile %s: %v", machine.ClusterNodeName, err)
		}
	} else if reconciled {
		nodeReconciled = true
	}

	return nodeReconciled, reconcileErr
}

func (s *Server) reconcileMachineClusterNode(ctx context.Context, machine model.Machine, runtimeObj *model.Runtime) (bool, error) {
	nodeName := strings.TrimSpace(machine.ClusterNodeName)
	if nodeName == "" {
		if runtimeObj != nil {
			nodeName = strings.TrimSpace(runtimeObj.ClusterNodeName)
		}
	}
	if nodeName == "" {
		return false, nil
	}

	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	client, err := clientFactory()
	if err != nil {
		return false, err
	}
	return client.reconcileMachineNode(ctx, nodeName, machine, runtimeObj)
}

func (c *clusterNodeClient) reconcileMachineNode(ctx context.Context, nodeName string, machine model.Machine, runtimeObj *model.Runtime) (bool, error) {
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" {
		return false, nil
	}

	node, err := c.getNode(ctx, nodeName)
	if err != nil {
		if isKubernetesNodeNotFound(err) {
			return false, nil
		}
		return false, err
	}

	patch, changed := buildMachineNodeMergePatch(node, machine, runtimeObj)
	if !changed {
		return false, nil
	}
	if err := c.patchNode(ctx, nodeName, patch); err != nil {
		if isKubernetesNodeNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func buildMachineNodeMergePatch(node kubeNode, machine model.Machine, runtimeObj *model.Runtime) (map[string]any, bool) {
	labelsPatch, labelsChanged := buildMachineNodeLabelsPatch(node, machine, runtimeObj)
	taints, taintsChanged := buildMachineNodeTaints(node.Spec.Taints, nodeSchedulingHealthy(node), machine)
	if !labelsChanged && !taintsChanged {
		return nil, false
	}
	patch := map[string]any{}
	if labelsChanged {
		patch["metadata"] = map[string]any{
			"labels": labelsPatch,
		}
	}
	if taintsChanged {
		patch["spec"] = map[string]any{"taints": taints}
	}
	return patch, true
}

func buildMachineNodeLabelsPatch(node kubeNode, machine model.Machine, runtimeObj *model.Runtime) (map[string]any, bool) {
	current := node.Metadata.Labels
	desired := machineJoinLabelMap(machine)
	if runtimeObj == nil && machine.Policy.AllowSharedPool {
		desired[runtimepkg.SharedPoolLabelKey] = runtimepkg.SharedPoolLabelValue
	}
	applyNodeHealthLabels(desired, nodeSchedulingHealthy(node))

	keys := []string{
		runtimepkg.NodeKeyIDLabelKey,
		runtimepkg.MachineIDLabelKey,
		runtimepkg.MachineScopeLabelKey,
		runtimepkg.AppRuntimeRoleLabelKey,
		runtimepkg.BuilderRoleLabelKey,
		runtimepkg.BuildNodeLabelKey,
		runtimepkg.EdgeRoleLabelKey,
		runtimepkg.DNSRoleLabelKey,
		runtimepkg.InternalMaintenanceLabelKey,
		legacyBuildTierLabelKey,
		runtimepkg.ControlPlaneDesiredRoleKey,
		runtimepkg.NodeSchedulableLabelKey,
		runtimepkg.NodeHealthLabelKey,
	}
	if runtimeObj == nil {
		keys = append(keys, runtimepkg.SharedPoolLabelKey)
	}

	patch := map[string]any{}
	changed := false
	for _, key := range keys {
		currentValue, hasCurrent := "", false
		if current != nil {
			currentValue, hasCurrent = current[key]
		}
		desiredValue, hasDesired := desired[key]
		switch {
		case hasDesired && strings.TrimSpace(desiredValue) != "":
			patch[key] = desiredValue
			if !hasCurrent || currentValue != desiredValue {
				changed = true
			}
		case hasCurrent:
			patch[key] = nil
			changed = true
		}
	}
	return patch, changed
}

func buildMachineNodeTaints(current []kubeNodeTaint, healthy bool, machine model.Machine) ([]kubeNodeTaint, bool) {
	normalizedCurrent := normalizeKubeNodeTaints(current)
	desiredDedicatedTaint, hasDesiredDedicatedTaint := desiredMachineDedicatedTaint(machine.Policy)
	desiredHealthTaint, hasDesiredHealthTaint := desiredNodeHealthTaint(healthy)

	next := make([]kubeNodeTaint, 0, len(normalizedCurrent)+2)
	dedicatedPresent := false
	healthPresent := false
	for _, taint := range normalizedCurrent {
		switch taint.Key {
		case runtimepkg.DedicatedTaintKey:
			if hasDesiredDedicatedTaint && !dedicatedPresent && kubeNodeTaintEqual(taint, desiredDedicatedTaint) {
				next = append(next, desiredDedicatedTaint)
				dedicatedPresent = true
			}
		case runtimepkg.NodeUnhealthyTaintKey:
			if hasDesiredHealthTaint && !healthPresent && kubeNodeTaintEqual(taint, desiredHealthTaint) {
				next = append(next, desiredHealthTaint)
				healthPresent = true
			}
		default:
			next = append(next, taint)
		}
	}
	if hasDesiredDedicatedTaint && !dedicatedPresent {
		next = append(next, desiredDedicatedTaint)
	}
	if hasDesiredHealthTaint && !healthPresent {
		next = append(next, desiredHealthTaint)
	}

	return next, !kubeNodeTaintSlicesEqual(normalizedCurrent, next)
}

func machineDedicatedTaintValue(policy model.MachinePolicy) (string, bool) {
	switch {
	case policy.AllowEdge:
		return runtimepkg.DedicatedEdgeValue, true
	case policy.AllowDNS:
		return runtimepkg.DedicatedDNSValue, true
	case policy.AllowInternalMaintenance:
		return runtimepkg.DedicatedInternalValue, true
	default:
		return "", false
	}
}

func desiredMachineDedicatedTaint(policy model.MachinePolicy) (kubeNodeTaint, bool) {
	value, ok := machineDedicatedTaintValue(policy)
	if !ok {
		return kubeNodeTaint{}, false
	}
	return kubeNodeTaint{
		Key:    runtimepkg.DedicatedTaintKey,
		Value:  value,
		Effect: "NoSchedule",
	}, true
}

func desiredNodeHealthTaint(healthy bool) (kubeNodeTaint, bool) {
	if healthy {
		return kubeNodeTaint{}, false
	}
	return kubeNodeTaint{
		Key:    runtimepkg.NodeUnhealthyTaintKey,
		Value:  runtimepkg.NodeUnhealthyTaintValue,
		Effect: "NoSchedule",
	}, true
}

func applyNodeHealthLabels(labels map[string]string, healthy bool) {
	if labels == nil {
		return
	}
	if healthy {
		labels[runtimepkg.NodeSchedulableLabelKey] = "true"
		labels[runtimepkg.NodeHealthLabelKey] = runtimepkg.NodeHealthReadyValue
		return
	}
	labels[runtimepkg.NodeSchedulableLabelKey] = "false"
	labels[runtimepkg.NodeHealthLabelKey] = runtimepkg.NodeHealthBlockedValue
}

func nodeSchedulingHealthy(node kubeNode) bool {
	return kubeNodeConditionStatus(node, clusterNodeConditionReady) == "true" &&
		kubeNodeConditionStatus(node, clusterNodeConditionDisk) != "true"
}

func kubeNodeConditionStatus(node kubeNode, conditionType string) string {
	for _, condition := range node.Status.Conditions {
		if strings.EqualFold(strings.TrimSpace(condition.Type), conditionType) {
			return normalizeKubeConditionStatus(condition.Status)
		}
	}
	return "unknown"
}

func (s *Server) loadManagedClusterNodeView(ctx context.Context, principal model.Principal, nodeName string) (model.ClusterNode, error) {
	snapshots, err := s.loadClusterNodeInventory(ctx)
	if err != nil {
		return model.ClusterNode{}, err
	}

	runtimes, err := s.store.ListNodes(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		return model.ClusterNode{}, err
	}
	machines, err := s.store.ListMachines(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		return model.ClusterNode{}, err
	}
	if principal.IsPlatformAdmin() {
		machines, err = s.ensureBootstrapControlPlaneMachines(snapshots, runtimes, machines)
		if err != nil {
			return model.ClusterNode{}, err
		}
	}
	apps, err := s.store.ListApps(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		return model.ClusterNode{}, err
	}
	services, err := s.store.ListBackingServices(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		return model.ClusterNode{}, err
	}
	managedSharedRuntime, err := s.store.GetRuntime(tenantSharedRuntimeID)
	if err != nil {
		return model.ClusterNode{}, err
	}
	_, defaultSharedDisplayRegion, _ := selectDefaultManagedSharedLocation(snapshots)

	workloadResolver := newClusterWorkloadResolver(apps, services)
	resolvedSnapshots := make([]resolvedClusterNodeSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		resolvedSnapshots = append(resolvedSnapshots, resolvedClusterNodeSnapshot{
			snapshot:  snapshot,
			workloads: workloadResolver.resolve(snapshot.pods),
		})
	}
	nodes := buildVisibleClusterNodesFromResolved(
		principal,
		resolvedSnapshots,
		runtimes,
		machines,
		managedSharedRuntime,
		defaultSharedDisplayRegion,
	)
	for _, node := range nodes {
		if strings.EqualFold(node.Name, nodeName) {
			return node, nil
		}
	}
	return model.ClusterNode{}, store.ErrNotFound
}

func (s *Server) ensurePlatformMachineByClusterNodeName(ctx context.Context, nodeName string) (model.Machine, error) {
	snapshots, err := s.loadClusterNodeInventory(ctx)
	if err != nil {
		return model.Machine{}, err
	}
	for _, snapshot := range snapshots {
		if !strings.EqualFold(strings.TrimSpace(snapshot.node.Name), strings.TrimSpace(nodeName)) {
			continue
		}
		return s.store.EnsurePlatformMachineForClusterNode(
			snapshot.node.Name,
			bootstrapControlPlaneMachineEndpoint(snapshot),
			snapshot.labels,
			bootstrapControlPlaneMachineName(snapshot),
			bootstrapControlPlaneMachineFingerprint(snapshot),
		)
	}
	return model.Machine{}, store.ErrNotFound
}
