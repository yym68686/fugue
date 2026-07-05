package controller

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
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

func TestCancelObsoletePendingNodeImageUpdateTasksSkipsEdgeBacklog(t *testing.T) {
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	_, secret, err := stateStore.CreateScopedNodeKey("", "platform", model.NodeKeyScopePlatformNode)
	if err != nil {
		t.Fatalf("create platform key: %v", err)
	}
	edgeUpdater, _, err := stateStore.EnrollNodeUpdater(
		secret,
		"edge-only",
		"https://edge-only.example.com",
		map[string]string{runtimepkg.EdgeRoleLabelKey: runtimepkg.NodeRoleLabelValue},
		"edge-only",
		"edge-fingerprint",
		"v1",
		"join-v1",
		[]string{"heartbeat", "tasks", model.NodeUpdateTaskTypePrepullAppImages},
	)
	if err != nil {
		t.Fatalf("enroll edge updater: %v", err)
	}
	appUpdater, _, err := stateStore.EnrollNodeUpdater(
		secret,
		"app-node",
		"https://app-node.example.com",
		map[string]string{runtimepkg.AppRuntimeRoleLabelKey: runtimepkg.NodeRoleLabelValue},
		"app-node",
		"app-fingerprint",
		"v1",
		"join-v1",
		[]string{"heartbeat", "tasks", model.NodeUpdateTaskTypePrepullAppImages},
	)
	if err != nil {
		t.Fatalf("enroll app updater: %v", err)
	}

	admin := model.Principal{ActorType: model.ActorTypeSystem, ActorID: "test", Scopes: map[string]struct{}{"platform.admin": {}}}
	edgeRepair, err := stateStore.CreateNodeUpdateTask(admin, edgeUpdater.ID, "", "", model.NodeUpdateTaskTypePrepullAppImages, map[string]string{
		"priority":  model.ImageReplicationPriorityRepair,
		"image_ref": "registry.fugue.internal:5000/fugue-apps/demo:old",
	})
	if err != nil {
		t.Fatalf("create edge repair prepull: %v", err)
	}
	edgeDeploy, err := stateStore.CreateNodeUpdateTask(admin, edgeUpdater.ID, "", "", model.NodeUpdateTaskTypePrepullAppImages, map[string]string{
		"priority":  model.ImageReplicationPriorityDeployBlocking,
		"image_ref": "registry.fugue.internal:5000/fugue-apps/demo:deploy",
	})
	if err != nil {
		t.Fatalf("create edge deploy prepull: %v", err)
	}
	appRepair, err := stateStore.CreateNodeUpdateTask(admin, appUpdater.ID, "", "", model.NodeUpdateTaskTypePrepullAppImages, map[string]string{
		"priority":  model.ImageReplicationPriorityRepair,
		"image_ref": "registry.fugue.internal:5000/fugue-apps/demo:current",
	})
	if err != nil {
		t.Fatalf("create app repair prepull: %v", err)
	}

	svc := &Service{Store: stateStore}
	eligibility, err := svc.loadImageReplicationEligibility()
	if err != nil {
		t.Fatalf("load eligibility: %v", err)
	}
	if err := svc.cancelObsoletePendingNodeImageUpdateTasks(context.Background(), eligibility); err != nil {
		t.Fatalf("cancel obsolete pending node update tasks: %v", err)
	}

	tasks, err := stateStore.ListNodeUpdateTasks("", true, "", "")
	if err != nil {
		t.Fatalf("list node tasks: %v", err)
	}
	byID := map[string]model.NodeUpdateTask{}
	for _, task := range tasks {
		byID[task.ID] = task
	}
	if got := byID[edgeRepair.ID]; got.Status != model.NodeUpdateTaskStatusCanceled || !strings.Contains(got.ResultMessage, "edge_or_dns_only") {
		t.Fatalf("expected edge repair task canceled as edge_or_dns_only, got %+v", got)
	}
	if got := byID[edgeDeploy.ID]; got.Status != model.NodeUpdateTaskStatusPending {
		t.Fatalf("expected deploy-blocking edge task preserved, got %+v", got)
	}
	if got := byID[appRepair.ID]; got.Status != model.NodeUpdateTaskStatusPending {
		t.Fatalf("expected app repair task preserved, got %+v", got)
	}
}
