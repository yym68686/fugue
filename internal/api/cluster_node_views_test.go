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
