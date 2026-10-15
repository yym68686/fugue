package store

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestJournaldPolicyDeliveryMakesProgressBehindReplenishedInventory(t *testing.T) {
	now := time.Now().UTC()
	policy := model.NodeUpdateTask{ID: "policy", Type: model.NodeUpdateTaskTypeReconcileHostJournaldPolicy, CreatedAt: now.Add(-time.Minute)}
	for cycle := 0; cycle < 4; cycle++ {
		inventory := model.NodeUpdateTask{ID: "inventory", Type: model.NodeUpdateTaskTypeReportLocalPV, CreatedAt: now}
		tasks := []model.NodeUpdateTask{policy, inventory}
		sortNodeUpdateTasksForDelivery(tasks)
		want := "inventory"
		if cycle >= 2 {
			want = "policy"
		}
		if tasks[0].ID != want {
			t.Fatalf("poll %d: got %s, want %s", cycle, tasks[0].ID, want)
		}
		policy.CreatedAt = policy.CreatedAt.Add(-5 * time.Minute)
	}
}
