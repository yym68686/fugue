package api

import (
	"context"
	"net/http"
	"sort"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) handleListClusterNodePolicies(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can inspect cluster node policy")
		return
	}
	statuses, err := s.loadClusterNodePolicyStatuses(r.Context(), principal)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"node_policies": statuses})
}

func (s *Server) handleGetClusterNodePolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can inspect cluster node policy")
		return
	}
	nodeName := strings.TrimSpace(r.PathValue("name"))
	if nodeName == "" {
		httpx.WriteError(w, http.StatusBadRequest, "cluster node name is required")
		return
	}
	statuses, err := s.loadClusterNodePolicyStatuses(r.Context(), principal)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	for _, status := range statuses {
		if strings.EqualFold(status.NodeName, nodeName) {
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"node_policy": status})
			return
		}
	}
	s.writeStoreError(w, store.ErrNotFound)
}

func (s *Server) handleGetClusterNodePolicyStatus(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can inspect cluster node policy")
		return
	}
	statuses, err := s.loadClusterNodePolicyStatuses(r.Context(), principal)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"summary":       summarizeClusterNodePolicyStatuses(statuses),
		"node_policies": statuses,
	})
}

func (s *Server) loadClusterNodePolicyStatuses(ctx context.Context, principal model.Principal) ([]model.ClusterNodePolicyStatus, error) {
	snapshots, err := s.loadClusterNodeInventory(ctx)
	if err != nil {
		return nil, err
	}
	runtimes, err := s.store.ListNodes(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		return nil, err
	}
	machines, err := s.store.ListMachines(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		return nil, err
	}
	if principal.IsPlatformAdmin() {
		machines, err = s.ensureBootstrapControlPlaneMachines(snapshots, runtimes, machines)
		if err != nil {
			return nil, err
		}
	}

	runtimeByClusterNode := buildRuntimeByClusterNodeIndex(runtimes)
	machineByRuntimeID, machineByClusterNode := buildMachineIndexes(machines)
	statuses := make([]model.ClusterNodePolicyStatus, 0, len(snapshots))
	for _, snapshot := range snapshots {
		runtimeObj, ok := runtimeByClusterNode[snapshot.node.Name]
		var runtimeForNode *model.Runtime
		if ok {
			runtimeForNode = &runtimeObj
		}
		machine, hasMachine := selectMachineForSnapshot(snapshot, runtimeForNode, machineByRuntimeID, machineByClusterNode)
		var machineForNode *model.Machine
		if hasMachine {
			machineForNode = &machine
		}
		policy := buildClusterNodePolicyView(snapshot, machineForNode, runtimeForNode)
		statuses = append(statuses, buildClusterNodePolicyStatus(snapshot, machineForNode, runtimeForNode, policy))
	}
	sort.Slice(statuses, func(i, j int) bool {
		return strings.Compare(statuses[i].NodeName, statuses[j].NodeName) < 0
	})
	return statuses, nil
}

func buildClusterNodePolicyStatus(snapshot clusterNodeSnapshot, machine *model.Machine, runtimeObj *model.Runtime, policy *model.ClusterNodePolicy) model.ClusterNodePolicyStatus {
	ready := clusterNodeConditionIsTrue(snapshot.node, clusterNodeConditionReady)
	diskPressure := clusterNodeConditionIsTrue(snapshot.node, clusterNodeConditionDisk)
	reasons := clusterNodePolicyReconcileReasons(snapshot, machine, runtimeObj)
	out := model.ClusterNodePolicyStatus{
		NodeName:         snapshot.node.Name,
		Policy:           policy,
		Labels:           cloneStringMap(snapshot.labels),
		Taints:           clusterNodePolicyTaintViews(snapshot.taints),
		Conditions:       cloneClusterNodeConditions(snapshot.node.Conditions),
		Ready:            ready,
		DiskPressure:     diskPressure,
		NodeSchedulable:  clusterNodeSnapshotSchedulable(snapshot),
		Reconciled:       len(reasons) == 0,
		ReconcileReasons: reasons,
	}
	if runtimeObj != nil {
		out.RuntimeID = runtimeObj.ID
		out.TenantID = runtimeObj.TenantID
	} else if strings.TrimSpace(snapshot.runtimeID) != "" {
		out.RuntimeID = strings.TrimSpace(snapshot.runtimeID)
	}
	if machineHasSavedPolicy(machine) {
		out.MachineID = machine.ID
		if strings.TrimSpace(out.RuntimeID) == "" {
			out.RuntimeID = strings.TrimSpace(machine.RuntimeID)
		}
		if strings.TrimSpace(out.TenantID) == "" {
			out.TenantID = strings.TrimSpace(machine.TenantID)
		}
	}
	return out
}

func clusterNodePolicyReconcileReasons(snapshot clusterNodeSnapshot, machine *model.Machine, runtimeObj *model.Runtime) []string {
	healthy := clusterNodeSnapshotSchedulable(snapshot)
	reasons := []string{}
	if runtimeObj != nil {
		if _, changed := buildRuntimeNodeLabelsPatchForHealth(snapshot.labels, healthy, *runtimeObj); changed {
			reasons = append(reasons, "runtime labels drift from desired policy")
		}
		if _, changed := buildRuntimeNodeTaintsForHealth(snapshot.taints, healthy, *runtimeObj); changed {
			reasons = append(reasons, "runtime taints drift from desired policy")
		}
	}
	if machineHasSavedPolicy(machine) {
		node := kubeNode{}
		node.Metadata.Labels = snapshot.labels
		if _, changed := buildMachineNodeLabelsPatch(node, *machine, runtimeObj); changed {
			reasons = append(reasons, "node policy labels drift from desired policy")
		}
		if _, changed := buildMachineNodeTaints(snapshot.taints, healthy, *machine); changed {
			reasons = append(reasons, "node policy taints drift from desired policy")
		}
	}
	return reasons
}

func clusterNodePolicyTaintViews(taints []kubeNodeTaint) []model.ClusterNodeTaint {
	if len(taints) == 0 {
		return nil
	}
	out := make([]model.ClusterNodeTaint, 0, len(taints))
	for _, taint := range normalizeKubeNodeTaints(taints) {
		out = append(out, model.ClusterNodeTaint{
			Key:    strings.TrimSpace(taint.Key),
			Value:  strings.TrimSpace(taint.Value),
			Effect: strings.TrimSpace(taint.Effect),
		})
	}
	return out
}

func cloneClusterNodeConditions(in map[string]model.ClusterNodeCondition) map[string]model.ClusterNodeCondition {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]model.ClusterNodeCondition, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func summarizeClusterNodePolicyStatuses(statuses []model.ClusterNodePolicyStatus) model.ClusterNodePolicyStatusSummary {
	summary := model.ClusterNodePolicyStatusSummary{Total: len(statuses)}
	for _, status := range statuses {
		if status.Reconciled {
			summary.Reconciled++
		} else {
			summary.Drifted++
		}
		if status.Ready {
			summary.Ready++
		}
		if status.DiskPressure {
			summary.DiskPressure++
		}
		if !status.NodeSchedulable {
			summary.BlockedByHealth++
		}
	}
	return summary
}
