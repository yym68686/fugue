package store

import (
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/pgretire"
)

func TestSourceRetirementEvidenceSurvivesDiagnosticPruning(t *testing.T) {
	now := time.Now()
	old := now.Add(-365 * 24 * time.Hour)
	items := []model.OperationEvidence{{ID: "intent", Type: pgretire.IntentEvidence, CollectedAt: old}, {ID: "verified", Type: pgretire.VerifiedEvidence, CollectedAt: old}, {ID: "ordinary", Type: "rollout_progress", CollectedAt: old}}
	got := retainOperationEvidenceScope(items, func(model.OperationEvidence) bool { return true }, now, 1)
	if len(got) != 2 {
		t.Fatalf("safety evidence was evicted: %+v", got)
	}
	for _, evidence := range got {
		if !operationEvidenceMatchesFilter(evidence, model.OperationEvidenceFilter{PlatformAdmin: true}) {
			t.Fatal("safety evidence hidden from recovery")
		}
	}
}
