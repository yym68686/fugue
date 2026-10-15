package store

import (
	"fugue/internal/model"
	"fugue/internal/noderesourcepolicy"
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

func TestHostPolicyRecoveryCannotStarveBehindReplenishedInventory(t *testing.T) {
	now := time.Now().UTC()
	for _, taskType := range []string{noderesourcepolicy.TaskType, model.NodeUpdateTaskTypeReconcileHostJournaldPolicy} {
		t.Run(taskType, func(t *testing.T) {
			policy := model.NodeUpdateTask{ID: "policy", Type: taskType, CreatedAt: now.Add(-10 * time.Minute)}
			freshPriority, agedPriority := 4, 3
			wantOrder := []string{"upgrade", "overdue-inventory", "rescue", "deploy", "policy", "fresh-inventory"}
			if taskType == noderesourcepolicy.TaskType {
				freshPriority, agedPriority = 1, 1
				wantOrder = []string{"upgrade", "overdue-inventory", "policy", "rescue", "deploy", "fresh-inventory"}
			}
			if got := nodeUpdateTaskDeliveryPriority(policy, now.Add(-time.Nanosecond)); got != freshPriority {
				t.Fatal("fresh policy changed its priority", got)
			}
			if got := nodeUpdateTaskDeliveryPriority(policy, now); got != agedPriority {
				t.Fatal("waiting policy was not promoted at the boundary", got)
			}
			tasks := []model.NodeUpdateTask{
				{ID: "fresh-inventory", Type: model.NodeUpdateTaskTypeReportImageCache, CreatedAt: now.Add(-time.Minute)},
				policy,
				{ID: "upgrade", Type: model.NodeUpdateTaskTypeUpgradeUpdater, CreatedAt: now},
				{ID: "rescue", Type: storagerecovery.ExpandPoolTask, CreatedAt: now},
				{ID: "overdue-inventory", Type: model.NodeUpdateTaskTypeReportLocalPV, CreatedAt: now.Add(-31 * time.Minute)},
				{ID: "deploy", Type: model.NodeUpdateTaskTypeReplicateAppImage, CreatedAt: now, Payload: map[string]string{"priority": model.ImageReplicationPriorityDeployBlocking}},
			}
			sortNodeUpdateTasksForDelivery(tasks)
			for i, want := range wantOrder {
				if tasks[i].ID != want {
					t.Fatalf("%d: %s want %s", i, tasks[i].ID, want)
				}
			}
			policy.CreatedAt = time.Time{}
			if got := nodeUpdateTaskDeliveryPriority(policy, now); got != freshPriority {
				t.Fatal("missing creation time authorized priority promotion", got)
			}
		})
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
