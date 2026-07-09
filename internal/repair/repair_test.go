package repair

import (
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/localwal"
	"fugue/internal/model"
)

func TestEvaluateRepairCooldownMaxAttemptsAndSelfQuarantine(t *testing.T) {
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	policy := DefaultPolicy(ActionEdgeWorkerRestart)
	decision := Evaluate(policy, State{}, "edge-1", now, false)
	if decision.Allowed || decision.Reason != "feature_flag_disabled" {
		t.Fatalf("expected disabled flag block, got %+v", decision)
	}
	state := State{}
	AppendAttempt(&state, "edge-1", model.EdgeRepairAttempt{
		Action:      ActionEdgeWorkerRestart,
		SafetyClass: policy.SafetyClass,
		Attempt:     1,
		Status:      "failed",
		StartedAt:   now.Add(-time.Minute),
		FinishedAt:  now.Add(-30 * time.Second),
	})
	cooldown := Evaluate(policy, state, "edge-1", now, true)
	if cooldown.Allowed || cooldown.Reason != "repair_cooldown_active" || cooldown.CooldownRemaining <= 0 {
		t.Fatalf("expected cooldown block, got %+v", cooldown)
	}
	for i := 2; i <= 3; i++ {
		AppendAttempt(&state, "edge-1", model.EdgeRepairAttempt{
			Action:      ActionEdgeWorkerRestart,
			SafetyClass: policy.SafetyClass,
			Attempt:     i,
			Status:      "failed",
			StartedAt:   now.Add(time.Duration(i) * -10 * time.Minute),
			FinishedAt:  now.Add(time.Duration(i) * -9 * time.Minute),
		})
	}
	maxed := Evaluate(policy, state, "edge-1", now, true)
	if maxed.Allowed || maxed.Reason != "repair_max_attempts_exceeded" || !maxed.SelfQuarantine {
		t.Fatalf("expected max-attempt self quarantine, got %+v", maxed)
	}
}

func TestRecordRepairWAL(t *testing.T) {
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "repair.wal")
	attempt := model.EdgeRepairAttempt{
		Action:      ActionRouteBundleReload,
		SafetyClass: model.EdgeRepairSafetyL2LocalReload,
		Attempt:     1,
		Status:      "success",
		Message:     "reloaded LKG",
		StartedAt:   now,
		Evidence:    map[string]string{"generation": "routegen_1"},
	}
	if err := RecordWAL(path, "node-a", "edge-1", attempt, nil); err != nil {
		t.Fatalf("record wal: %v", err)
	}
	records, err := localwal.ReadAll(path)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	if len(records) != 1 || records[0].Action != "repair_action" || records[0].Evidence["action"] != ActionRouteBundleReload {
		t.Fatalf("unexpected records: %+v", records)
	}
}
