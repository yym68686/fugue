package controller

import (
	"testing"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

func TestNodeEligibleForAppImageReplicationSkipsEdgeDNSOfflineAndPressure(t *testing.T) {
	svc := &Service{}
	eligibility := imageReplicationEligibility{
		runtimeByID: map[string]model.Runtime{
			"runtime-offline": {ID: "runtime-offline", Status: model.RuntimeStatusOffline},
			"runtime-active":  {ID: "runtime-active", Status: model.RuntimeStatusActive},
		},
		machineByID: map[string]model.Machine{
			"edge-machine": {ID: "edge-machine", Policy: model.MachinePolicy{AllowEdge: true}},
			"app-machine":  {ID: "app-machine", Policy: model.MachinePolicy{AllowAppRuntime: true}},
			"cp-machine":   {ID: "cp-machine", Policy: model.MachinePolicy{DesiredControlPlaneRole: model.MachineControlPlaneRoleMember}},
		},
		machineByNode: map[string]model.Machine{},
		pressureByTarget: map[string]struct{}{
			"cluster:pressure-node": {},
		},
	}
	cases := []struct {
		name     string
		updater  model.NodeUpdater
		priority string
		want     string
	}{
		{
			name:     "edge only",
			updater:  model.NodeUpdater{Status: model.NodeUpdaterStatusActive, MachineID: "edge-machine", RuntimeID: "runtime-active", ClusterNodeName: "edge-node", Labels: map[string]string{runtimepkg.EdgeRoleLabelKey: runtimepkg.NodeRoleLabelValue}},
			priority: model.ImageReplicationPriorityRepair,
			want:     "edge_or_dns_only",
		},
		{
			name:     "offline runtime",
			updater:  model.NodeUpdater{Status: model.NodeUpdaterStatusActive, MachineID: "app-machine", RuntimeID: "runtime-offline", ClusterNodeName: "offline-node"},
			priority: model.ImageReplicationPriorityRepair,
			want:     "runtime_not_active",
		},
		{
			name:     "pressure repair",
			updater:  model.NodeUpdater{Status: model.NodeUpdaterStatusActive, MachineID: "app-machine", RuntimeID: "runtime-active", ClusterNodeName: "pressure-node"},
			priority: model.ImageReplicationPriorityRepair,
			want:     "filesystem_pressure",
		},
		{
			name:     "control plane only",
			updater:  model.NodeUpdater{Status: model.NodeUpdaterStatusActive, MachineID: "cp-machine", RuntimeID: "runtime-active", ClusterNodeName: "cp-node"},
			priority: model.ImageReplicationPriorityRepair,
			want:     "control_plane_only",
		},
		{
			name:     "app runtime allowed",
			updater:  model.NodeUpdater{Status: model.NodeUpdaterStatusActive, MachineID: "app-machine", RuntimeID: "runtime-active", ClusterNodeName: "app-node", Labels: map[string]string{runtimepkg.AppRuntimeRoleLabelKey: runtimepkg.NodeRoleLabelValue}},
			priority: model.ImageReplicationPriorityRepair,
			want:     "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, reason := svc.nodeEligibleForAppImageReplication(tc.updater, tc.priority, deployImageTarget{}, eligibility)
			if tc.want == "" {
				if !ok || reason != "" {
					t.Fatalf("expected eligible, got ok=%t reason=%q", ok, reason)
				}
				return
			}
			if ok || reason != tc.want {
				t.Fatalf("expected ineligible reason %q, got ok=%t reason=%q", tc.want, ok, reason)
			}
		})
	}
}

func TestImageTargetReplicaCountTreatsLegacyTwoAsDefaultOne(t *testing.T) {
	svc := &Service{}
	image := model.Image{RequiredReplicaCount: 2, MinAvailableReplicaCount: 2}
	if got := svc.imageTargetReplicaCount(image); got != 1 {
		t.Fatalf("target replicas = %d, want 1", got)
	}
	image.RequiredReplicaCount = 3
	if got := svc.imageTargetReplicaCount(image); got != 3 {
		t.Fatalf("explicit larger target replicas = %d, want 3", got)
	}
}
