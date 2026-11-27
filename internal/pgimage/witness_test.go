package pgimage

import (
	"maps"
	"testing"

	"fugue/internal/model"
)

func TestCompletionProofPreservesDurableWitness(t *testing.T) {
	for _, budget := range []int{0, 10} {
		name := "default"
		if budget != 0 {
			name = "fast-drain"
		}
		t.Run(name, func(t *testing.T) {
			op := model.Operation{DesiredSpec: &model.AppSpec{Postgres: &model.AppPostgresSpec{
				Image: "target", ImageMigrationDrainBudgetSeconds: budget,
			}}}
			intent := map[string]any{"cluster_uid": "cluster", "system_id": "system", "pod_uid": "old", "target_image": "target", "future_field": "original"}
			verified := maps.Clone(intent)
			verified["verified_pod_uid"] = "new"
			if budget != 0 {
				for _, payload := range []map[string]any{intent, verified} {
					payload["drain"] = map[string]any{"budget_seconds": float64(budget)}
				}
				verified["drain_restored"] = true
			}
			baseline := maps.Clone(intent)
			proofs := map[string]map[string]any{BaselineEvidence: baseline, IntentEvidence: intent, VerifiedEvidence: verified}
			if !ValidCompletionProof(op, proofs) {
				t.Fatal("compatible durable witness did not verify")
			}
			if got := verified["verified_pod_uid"]; got != "new" {
				t.Fatalf("verification mutated the supplied evidence: %v", got)
			}
			delete(verified, "future_field")
			if ValidCompletionProof(op, proofs) {
				t.Fatal("dropped witness field allowed completion")
			}
			verified["future_field"] = "original"
			verified["verified_pod_uid"] = "old"
			if ValidCompletionProof(op, proofs) {
				t.Fatal("original primary allowed completion")
			}
			verified["verified_pod_uid"] = "new"
			if budget != 0 {
				delete(verified, "drain_restored")
				if ValidCompletionProof(op, proofs) {
					t.Fatal("fast drain completed without restoration evidence")
				}
				verified["drain_restored"] = true
			}
			baseline["system_id"] = "foreign"
			if ValidCompletionProof(op, proofs) {
				t.Fatal("different intent and baseline allowed completion")
			}
		})
	}
}
