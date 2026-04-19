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

type setClusterNodePolicyRequest struct {
	AllowBuilds             *bool   `json:"allow_builds"`
	BuildTier               *string `json:"build_tier"`
	AllowSharedPool         *bool   `json:"allow_shared_pool"`
	DesiredControlPlaneRole *string `json:"desired_control_plane_role"`
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

	clusterNode, err := s.loadManagedClusterNodeView(r.Context(), principal, nodeName)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	metadata := map[string]string{
		"cluster_node_name":          nodeName,
		"allow_builds":               boolString(machine.Policy.AllowBuilds),
		"build_tier":                 machine.Policy.BuildTier,
		"allow_shared_pool":          boolString(machine.Policy.AllowSharedPool),
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
	return req.AllowBuilds != nil ||
		req.BuildTier != nil ||
		req.AllowSharedPool != nil ||
		req.DesiredControlPlaneRole != nil
}

func (req setClusterNodePolicyRequest) mergeInto(current model.MachinePolicy, runtimeObj *model.Runtime) (model.MachinePolicy, error) {
	next := current
	if req.AllowBuilds != nil {
		next.AllowBuilds = *req.AllowBuilds
	}
	if req.BuildTier != nil {
		buildTier := model.NormalizeMachineBuildTier(strings.TrimSpace(*req.BuildTier))
		if buildTier == "" {
			return model.MachinePolicy{}, store.ErrInvalidInput
		}
		next.BuildTier = buildTier
	}
	if req.AllowSharedPool != nil && runtimeObj == nil {
		next.AllowSharedPool = *req.AllowSharedPool
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

	patch, changed := buildMachineNodeMergePatch(node.Metadata.Labels, machine, runtimeObj)
	if !changed {
		return true, nil
	}
	if err := c.patchNode(ctx, nodeName, patch); err != nil {
		if isKubernetesNodeNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func buildMachineNodeMergePatch(current map[string]string, machine model.Machine, runtimeObj *model.Runtime) (map[string]any, bool) {
	labelsPatch, changed := buildMachineNodeLabelsPatch(current, machine, runtimeObj)
	if !changed {
		return nil, false
	}
	return map[string]any{
		"metadata": map[string]any{
			"labels": labelsPatch,
		},
	}, true
}

func buildMachineNodeLabelsPatch(current map[string]string, machine model.Machine, runtimeObj *model.Runtime) (map[string]any, bool) {
	desired := machineJoinLabelMap(machine)
	if runtimeObj == nil && machine.Policy.AllowSharedPool {
		desired[runtimepkg.SharedPoolLabelKey] = runtimepkg.SharedPoolLabelValue
	}

	keys := []string{
		runtimepkg.NodeKeyIDLabelKey,
		runtimepkg.MachineIDLabelKey,
		runtimepkg.MachineScopeLabelKey,
		runtimepkg.BuildNodeLabelKey,
		runtimepkg.BuildTierLabelKey,
		runtimepkg.ControlPlaneDesiredRoleKey,
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
