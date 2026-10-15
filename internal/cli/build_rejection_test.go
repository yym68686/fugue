package cli

import (
	"testing"
	"time"
)

func TestDeploymentResultRetainsPreflightResourceRejection(t *testing.T) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fact, ok := decodePublicBuildAttempt(map[string]any{
		"attempt": 1, "outcome": "rejected", "started_at": now, "finished_at": now,
		"causes":           []string{"builder_memory_policy_exhausted", "arbitrary-private-reason"},
		"missing_evidence": []string{}, "memory_limit_bytes": int64(6 << 30),
	})
	if !ok || fact.Outcome != "rejected" || len(fact.Causes) != 1 || fact.Causes[0] != "builder_memory_policy_exhausted" || len(fact.MissingEvidence) != 0 {
		t.Fatal("CLI discarded or misclassified a preflight rejection", fact, ok)
	}
}
