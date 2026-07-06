package controller

import (
	"context"
	"path/filepath"
	"testing"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestReconcileNodeUpdaterVersionsSchedulesUpgradeOnce(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Node Updater Upgrade Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	for _, item := range []struct {
		node    string
		version string
	}{
		{"worker-old", "v12"},
		{"worker-current", model.NodeUpdaterCurrentVersion},
		{"worker-future", "v99"},
	} {
		if _, _, err := stateStore.EnrollNodeUpdater(nodeSecret, item.node, "https://"+item.node+".example.com", nil, "machine-"+item.node, "fingerprint-"+item.node, item.version, "join-v10", []string{"heartbeat", "tasks", model.NodeUpdateTaskTypeUpgradeUpdater}); err != nil {
			t.Fatalf("enroll updater %s: %v", item.node, err)
		}
	}
	svc := &Service{Store: stateStore}
	if err := svc.reconcileNodeUpdaterVersions(context.Background()); err != nil {
		t.Fatalf("reconcile node updater versions: %v", err)
	}
	if err := svc.reconcileNodeUpdaterVersions(context.Background()); err != nil {
		t.Fatalf("second reconcile node updater versions: %v", err)
	}
	tasks, err := stateStore.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusPending)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected one upgrade task, got %+v", tasks)
	}
	task := tasks[0]
	if task.Type != model.NodeUpdateTaskTypeUpgradeUpdater || task.ClusterNodeName != "worker-old" {
		t.Fatalf("unexpected upgrade task: %+v", task)
	}
	if task.Payload["target_version"] != model.NodeUpdaterCurrentVersion || task.RequestedByID != "fugue-controller/node-updater-upgrade" {
		t.Fatalf("unexpected upgrade payload/requester: %+v", task)
	}
}

func TestControllerNodeUpdaterNeedsUpgrade(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		current string
		target  string
		want    bool
	}{
		{name: "older", current: "v12", target: "v13", want: true},
		{name: "same", current: "v13", target: "v13", want: false},
		{name: "future", current: "v14", target: "v13", want: false},
		{name: "empty", current: "", target: "v13", want: true},
		{name: "unknown", current: "legacy", target: "v13", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := controllerNodeUpdaterNeedsUpgrade(tt.current, tt.target); got != tt.want {
				t.Fatalf("needs upgrade = %v, want %v", got, tt.want)
			}
		})
	}
}
