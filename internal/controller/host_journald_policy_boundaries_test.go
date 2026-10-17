package controller

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

// Exercise the scheduler through its projected store read. An updated task
// record must not renew the terminal-time cooldown, and a new policy must not
// inherit the old policy's cooldown or successful reconciliation interval.
func TestHostJournaldProjectedStatePreservesReconciliationBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name            string
		status          string
		age             time.Duration
		omitCompletedAt bool
		changedPolicy   bool
		manualActor     bool
		wantPending     int
	}{
		{"failure inside cooldown", model.NodeUpdateTaskStatusFailed, 29 * time.Minute, false, false, false, 0},
		{"failure after cooldown despite newer metadata", model.NodeUpdateTaskStatusFailed, 31 * time.Minute, false, false, false, 1},
		{"success inside interval", model.NodeUpdateTaskStatusCompleted, 23 * time.Hour, false, false, false, 0},
		{"success after interval despite newer metadata", model.NodeUpdateTaskStatusCompleted, 25 * time.Hour, false, false, false, 1},
		{"failure timestamp fallback inside cooldown", model.NodeUpdateTaskStatusFailed, 29 * time.Minute, true, false, false, 0},
		{"failure timestamp fallback after cooldown", model.NodeUpdateTaskStatusFailed, 31 * time.Minute, true, false, false, 1},
		{"success timestamp fallback inside interval", model.NodeUpdateTaskStatusCompleted, 23 * time.Hour, true, false, false, 0},
		{"success timestamp fallback after interval", model.NodeUpdateTaskStatusCompleted, 25 * time.Hour, true, false, false, 1},
		{"changed policy after recent failure", model.NodeUpdateTaskStatusFailed, time.Minute, false, true, false, 1},
		{"changed policy after recent success", model.NodeUpdateTaskStatusCompleted, time.Minute, false, true, false, 1},
		{"manual failure does not enter scheduler cooldown", model.NodeUpdateTaskStatusFailed, time.Minute, false, false, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stateStore, nodeSecret, path := newImageCacheControllerTestStoreWithPath(t)
			updater, _, err := stateStore.EnrollNodeUpdater(nodeSecret, "worker", "https://worker.example", nil, "worker", "fingerprint", model.NodeUpdaterCurrentVersion, "join-v36", []string{"heartbeat", "tasks", model.NodeUpdateTaskTypeReconcileHostJournaldPolicy})
			if err != nil {
				t.Fatal(err)
			}
			svc := &Service{Store: stateStore, Config: config.ControllerConfig{
				HostJournaldPolicyEnabled:           true,
				HostJournaldMaxRetentionSec:         "30day",
				HostJournaldSystemMaxUse:            "1G",
				HostJournaldPolicyReconcileInterval: 24 * time.Hour,
			}}
			if err := svc.scheduleHostJournaldPolicyReconciliation(context.Background()); err != nil {
				t.Fatal(err)
			}
			tasks, err := stateStore.ListNodeUpdateTasks("", true, updater.ID, model.NodeUpdateTaskStatusPending)
			if err != nil || len(tasks) != 1 {
				t.Fatalf("initial task: %d %v", len(tasks), err)
			}
			original := tasks[0]
			if _, err := stateStore.ClaimNodeUpdateTask(original.ID, updater.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := stateStore.CompleteNodeUpdateTask(original.ID, updater.ID, tc.status, "terminal", ""); err != nil {
				t.Fatal(err)
			}
			// Move only this isolated fixture's terminal timestamps. No sleeps or
			// production clock overrides are needed to cross the real intervals.
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var state model.State
			if err := json.Unmarshal(raw, &state); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			terminal := now.Add(-tc.age)
			found := false
			for i := range state.NodeUpdateTasks {
				task := &state.NodeUpdateTasks[i]
				if task.ID != original.ID {
					continue
				}
				found = true
				created, claimed := terminal.Add(-2*time.Minute), terminal.Add(-time.Minute)
				task.CreatedAt, task.ClaimedAt, task.CompletedAt, task.UpdatedAt = created, &claimed, &terminal, now
				if tc.omitCompletedAt {
					task.CompletedAt, task.UpdatedAt = nil, terminal
				}
				if tc.manualActor {
					task.RequestedByID = "manual-operator"
				}
			}
			if !found {
				t.Fatal("fixture task missing")
			}
			raw, err = json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if tc.changedPolicy {
				svc.Config.HostJournaldSystemMaxUse = "2G"
			}
			for i := 0; i < 2; i++ {
				if err := svc.scheduleHostJournaldPolicyReconciliation(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			pending, err := stateStore.ListNodeUpdateTasks("", true, updater.ID, model.NodeUpdateTaskStatusPending)
			if err != nil || len(pending) != tc.wantPending {
				t.Fatalf("pending after reconciliation: got %d want %d, error %v", len(pending), tc.wantPending, err)
			}
			if len(pending) == 1 {
				if pending[0].ID == original.ID || pending[0].RequestedByID != hostJournaldPolicyActorID {
					t.Fatal("reconciliation reused terminal task or changed scheduler ownership")
				}
				if (pending[0].Payload["policy_hash"] != original.Payload["policy_hash"]) != tc.changedPolicy {
					t.Fatal("policy identity did not follow desired configuration")
				}
			}
		})
	}
}
