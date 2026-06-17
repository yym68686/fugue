package api

import (
	"testing"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

func TestBuildClusterNodePolicyViewFallsBackToLiveStateForSyntheticRuntimeMachine(t *testing.T) {
	t.Parallel()

	snapshot := clusterNodeSnapshot{
		sharedPool: false,
		labels: map[string]string{
			runtimepkg.BuildNodeLabelKey:          runtimepkg.BuildNodeLabelValue,
			runtimepkg.ControlPlaneDesiredRoleKey: model.MachineControlPlaneRoleCandidate,
		},
	}
	runtimeObj := model.Runtime{
		ID:              "runtime_123",
		TenantID:        "tenant_123",
		Name:            "builder-1",
		Type:            model.RuntimeTypeManagedOwned,
		PoolMode:        model.RuntimePoolModeInternalShared,
		Status:          model.RuntimeStatusActive,
		ClusterNodeName: "builder-1",
		UpdatedAt:       time.Date(2026, time.April, 20, 0, 0, 0, 0, time.UTC),
	}
	syntheticMachine := buildRuntimeSnapshotMachine(runtimeObj)

	if syntheticMachine.ID != "" {
		t.Fatalf("expected synthetic runtime machine to have no persisted id, got %q", syntheticMachine.ID)
	}

	policy := buildClusterNodePolicyView(snapshot, &syntheticMachine, &runtimeObj)
	if policy == nil {
		t.Fatal("expected cluster node policy")
	}
	if !policy.AllowBuilds {
		t.Fatalf("expected desired builds to fall back to live state, got %#v", policy)
	}
	if !policy.AllowSharedPool {
		t.Fatalf("expected desired shared-pool to follow runtime pool mode, got %#v", policy)
	}
	if policy.DesiredControlPlaneRole != model.MachineControlPlaneRoleCandidate {
		t.Fatalf("expected desired control-plane role %q, got %q", model.MachineControlPlaneRoleCandidate, policy.DesiredControlPlaneRole)
	}
}

func TestBuildClusterNodePolicyViewReportsDedicatedModeDrift(t *testing.T) {
	t.Parallel()

	snapshot := clusterNodeSnapshot{
		labels: map[string]string{
			runtimepkg.AppRuntimeRoleLabelKey: runtimepkg.NodeRoleLabelValue,
			runtimepkg.EdgeRoleLabelKey:       runtimepkg.NodeRoleLabelValue,
			runtimepkg.DNSRoleLabelKey:        runtimepkg.NodeRoleLabelValue,
		},
		taints: []kubeNodeTaint{{
			Key:    runtimepkg.DedicatedTaintKey,
			Value:  runtimepkg.DedicatedEdgeValue,
			Effect: "NoSchedule",
		}},
		node: model.ClusterNode{
			Conditions: map[string]model.ClusterNodeCondition{
				clusterNodeConditionReady: {Status: "true"},
				clusterNodeConditionDisk:  {Status: "false"},
			},
		},
	}
	machine := model.Machine{
		ID: "machine_mixed_edge",
		Policy: model.MachinePolicy{
			AllowAppRuntime: true,
			AllowEdge:       true,
			AllowDNS:        true,
		},
	}

	policy := buildClusterNodePolicyView(snapshot, &machine, nil)
	if policy == nil {
		t.Fatal("expected cluster node policy")
	}
	if policy.DedicatedMode != model.MachineDedicatedModeNone {
		t.Fatalf("expected mixed desired policy to be non-dedicated, got %#v", policy)
	}
	if policy.EffectiveDedicatedMode != model.MachineDedicatedModeEdge {
		t.Fatalf("expected live taint to report dedicated edge drift, got %#v", policy)
	}
}

func TestBuildClusterNodePolicyViewSavedEdgePolicyOverridesRuntimeIdentityFallback(t *testing.T) {
	t.Parallel()

	snapshot := clusterNodeSnapshot{
		labels: map[string]string{
			runtimepkg.RuntimeIDLabelKey:  "runtime_edge",
			runtimepkg.TenantIDLabelKey:   "tenant_edge",
			runtimepkg.NodeModeLabelKey:   model.RuntimeTypeManagedOwned,
			runtimepkg.EdgeRoleLabelKey:   runtimepkg.NodeRoleLabelValue,
			runtimepkg.NodeHealthLabelKey: runtimepkg.NodeHealthReadyValue,
		},
	}
	runtimeObj := model.Runtime{
		ID:              "runtime_edge",
		TenantID:        "tenant_edge",
		Type:            model.RuntimeTypeManagedOwned,
		PoolMode:        model.RuntimePoolModeDedicated,
		ClusterNodeName: "edge-1",
	}
	machine := model.Machine{
		ID:        "machine_edge",
		RuntimeID: runtimeObj.ID,
		Policy: model.MachinePolicy{
			AllowAppRuntime: false,
			AllowEdge:       true,
		},
	}

	policy := buildClusterNodePolicyView(snapshot, &machine, &runtimeObj)
	if policy == nil {
		t.Fatal("expected cluster node policy")
	}
	if policy.EffectiveAppRuntime {
		t.Fatalf("expected saved edge-only policy to suppress runtime identity app fallback, got %#v", policy)
	}
	if !policy.EffectiveEdge {
		t.Fatalf("expected live edge role to remain effective, got %#v", policy)
	}
	if policy.AllowAppRuntime {
		t.Fatalf("expected desired app runtime disabled, got %#v", policy)
	}
}
