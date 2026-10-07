package controller

import (
	"context"
	"testing"

	"fugue/internal/config"
	"fugue/internal/model"
)

func TestScheduleLocalPVInventoryReportsRequiresCapabilityAndDeduplicates(t *testing.T) {
	t.Parallel()

	stateStore, nodeSecret := newImageCacheControllerTestStore(t)
	for _, item := range []struct {
		node         string
		capabilities []string
	}{
		{"worker-1", []string{"heartbeat", "tasks", model.NodeUpdateTaskTypeReportLocalPV}},
		{"worker-2", []string{"heartbeat", "tasks"}},
	} {
		if _, _, err := stateStore.EnrollNodeUpdater(
			nodeSecret,
			item.node,
			"https://"+item.node+".example.com",
			nil,
			item.node,
			"fingerprint-"+item.node,
			"v10",
			"join-v10",
			item.capabilities,
		); err != nil {
			t.Fatalf("enroll updater %s: %v", item.node, err)
		}
	}

	svc := &Service{
		Store:  stateStore,
		Config: config.ControllerConfig{LocalPVInventoryEnabled: true},
	}
	if err := svc.scheduleLocalPVInventoryReports(context.Background()); err != nil {
		t.Fatalf("schedule inventory: %v", err)
	}
	if err := svc.scheduleLocalPVInventoryReports(context.Background()); err != nil {
		t.Fatalf("reschedule inventory: %v", err)
	}

	tasks, err := stateStore.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusPending)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}

	if len(tasks) != 2 {
		t.Fatalf("expected inventory and protocol upgrade, got %+v", tasks)
	}
	kinds := map[string]int{}
	for _, task := range tasks {
		if task.ClusterNodeName != "worker-1" {
			t.Fatalf("upgraded a node without inventory capability: %+v", task)
		}
		kinds[task.Type]++
	}
	if kinds[model.NodeUpdateTaskTypeReportLocalPV] != 1 || kinds[model.NodeUpdateTaskTypeUpgradeUpdater] != 1 {
		t.Fatalf("missing or duplicate inventory/upgrade task: %+v", kinds)
	}

}

func TestControllerLocalPVInventoryHasStorageDoesNotInventVolumeGroup(t *testing.T) {
	t.Parallel()

	if controllerLocalPVInventoryHasStorage(model.LocalPVInventory{
		VGName:         "fugue-vg",
		ImagePath:      "/var/lib/fugue/lvm-localpv/fugue-vg.img",
		ImageSizeBytes: 32 << 30,
		UnsafeReasons:  []string{"loop_device_missing", "lvm_tools_unavailable_or_vg_missing"},
	}) {
		t.Fatal("an unattached backing file without observed storage must not emit capacity metrics")
	}
	if !controllerLocalPVInventoryHasStorage(model.LocalPVInventory{
		VGName:      "fugue-vg",
		PVSizeBytes: 96 << 30,
	}) {
		t.Fatal("an observed LocalPV physical-volume size must emit capacity metrics")
	}
	if !controllerLocalPVInventoryHasStorage(model.LocalPVInventory{
		VGName:     "fugue-vg",
		LoopDevice: "/dev/loop0",
	}) {
		t.Fatal("an attached LocalPV backing device with unavailable volume-group capacity must emit fail-closed capacity metrics")
	}
}
