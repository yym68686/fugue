package store

import (
	"fugue/internal/model"
	"fugue/internal/noderesourcepolicy"
	"testing"
	"time"
)

func TestResourceRecoveryIsNotStarvedByPeriodicInventory(t *testing.T) {
	now := time.Now().UTC()
	tasks := []model.NodeUpdateTask{
		{ID: "inventory", Type: model.NodeUpdateTaskTypeReportLocalPV, CreatedAt: now.Add(-time.Minute)},
		{ID: "capacity", Type: noderesourcepolicy.TaskType, CreatedAt: now.Add(-11 * time.Minute)},
		{ID: "upgrade", Type: model.NodeUpdateTaskTypeUpgradeUpdater, CreatedAt: now},
	}
	sortNodeUpdateTasksForDelivery(tasks)
	for i, id := range []string{"upgrade", "capacity", "inventory"} {
		if tasks[i].ID != id {
			t.Fatal("capacity recovery starved", tasks)
		}
	}
}
