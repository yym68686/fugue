package store

import (
	"fugue/internal/model"
	"fugue/internal/storagerecovery"
	"testing"
	"time"
)

func TestStorageRescueCannotStarveBehindFreshInventoryOrPruning(t *testing.T) {
	now := time.Now()
	tasks := []model.NodeUpdateTask{
		{ID: "prune", Type: model.NodeUpdateTaskTypePruneImageCache, CreatedAt: now.Add(-time.Hour)},
		{ID: "inventory", Type: model.NodeUpdateTaskTypeReportLocalPV, CreatedAt: now.Add(-time.Minute)},
		{ID: "rescue", Type: storagerecovery.ExpandPoolTask, CreatedAt: now},
		{ID: "upgrade", Type: model.NodeUpdateTaskTypeUpgradeUpdater, CreatedAt: now},
	}
	sortNodeUpdateTasksForDelivery(tasks)
	for i, want := range []string{"upgrade", "rescue", "prune", "inventory"} {
		if tasks[i].ID != want {
			t.Fatalf("position %d: %s, want %s", i, tasks[i].ID, want)
		}
	}
}

func TestAgedPruneCannotStarveBehindReplenishedInventory(t *testing.T) {
	now := time.Now()
	tasks := []model.NodeUpdateTask{
		{ID: "fresh-inventory", Type: model.NodeUpdateTaskTypeReportImageCache, CreatedAt: now.Add(-time.Minute)},
		{ID: "aged-prune", Type: model.NodeUpdateTaskTypePruneImageCache, CreatedAt: now.Add(-11 * time.Minute)},
		{ID: "fresh-prune", Type: model.NodeUpdateTaskTypePruneImageCache, CreatedAt: now},
		{ID: "overdue-inventory", Type: model.NodeUpdateTaskTypeReportLocalPV, CreatedAt: now.Add(-31 * time.Minute)},
		{ID: "deploy", Type: model.NodeUpdateTaskTypeReplicateAppImage, CreatedAt: now, Payload: map[string]string{"priority": model.ImageReplicationPriorityDeployBlocking}},
	}
	sortNodeUpdateTasksForDelivery(tasks)
	for i, want := range []string{"overdue-inventory", "deploy", "aged-prune", "fresh-inventory", "fresh-prune"} {
		if tasks[i].ID != want {
			t.Fatalf("%d: %s want %s", i, tasks[i].ID, want)
		}
	}
}
