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
	for i, want := range []string{"upgrade", "rescue", "inventory", "prune"} {
		if tasks[i].ID != want {
			t.Fatalf("position %d: %s, want %s", i, tasks[i].ID, want)
		}
	}
}
