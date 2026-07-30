package runtime

import (
	"slices"
	"testing"

	"fugue/internal/model"
)

func TestAgentMigrationImageEvidenceRequiresCurrentTaskPull(t *testing.T) {
	t.Parallel()
	status, result, invariants := agentMigrationImageReplicationEvidence(1, "registry.example/app:v1", "registry.example/app:v1", false)
	if status != model.AppMigrationEvidenceMissing || result == "" || !slices.Contains(invariants, "image_replication_unverified") {
		t.Fatalf("migration image without a current pull must be blocked: status=%q result=%q invariants=%v", status, result, invariants)
	}

	status, result, invariants = agentMigrationImageReplicationEvidence(1, "registry.example/app:v1", "registry.example/app:v1", true)
	if status != model.AppMigrationEvidenceVerified || result == "" || len(invariants) != 0 {
		t.Fatalf("current pull plus matching target Deployment must verify: status=%q result=%q invariants=%v", status, result, invariants)
	}
}

func TestAgentMigrationImageEvidenceRejectsTemplateMismatchAfterPull(t *testing.T) {
	t.Parallel()
	status, _, invariants := agentMigrationImageReplicationEvidence(1, "registry.example/app:v2", "registry.example/app:v1", true)
	if status != model.AppMigrationEvidenceMissing || !slices.Contains(invariants, "image_missing") {
		t.Fatalf("a successful pull cannot excuse a stale Deployment image: status=%q invariants=%v", status, invariants)
	}
}

func TestAgentMigrationReplicaCountExcludesPreviousRevision(t *testing.T) {
	t.Parallel()
	if got := minAgentMigrationReplicaCount(0, 1, 1); got != 0 {
		t.Fatalf("ready replicas from a previous revision counted as current: %d", got)
	}
	if got := minAgentMigrationReplicaCount(1, 1, 1); got != 1 {
		t.Fatalf("current updated/ready/available replicas = %d, want 1", got)
	}
}

func TestAgentMigrationGenerationEvidenceDoesNotCompareDifferentObjects(t *testing.T) {
	t.Parallel()
	generation, observedGeneration, invariants := agentMigrationGenerationEvidence(2, 2, 11, 11)
	// ManagedApp and Deployment generations are independent counters. A max/min
	// comparison across them would incorrectly turn these two current objects
	// into generation=11/observedGeneration=2 and block every cutover.
	if generation != 2 || observedGeneration != 2 || len(invariants) != 0 {
		t.Fatalf("independent current generations were combined incorrectly: generation=%d observed=%d invariants=%v", generation, observedGeneration, invariants)
	}

	_, _, invariants = agentMigrationGenerationEvidence(2, 1, 11, 11)
	if !slices.Contains(invariants, "generation_not_observed") {
		t.Fatalf("stale ManagedApp generation was not rejected: %v", invariants)
	}
	_, _, invariants = agentMigrationGenerationEvidence(2, 2, 11, 10)
	if !slices.Contains(invariants, "deployment_generation_not_observed") {
		t.Fatalf("stale Deployment generation was not rejected: %v", invariants)
	}
}
