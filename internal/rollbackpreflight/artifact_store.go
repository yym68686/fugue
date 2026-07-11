package rollbackpreflight

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

type ArtifactStoreInventory struct {
	Artifacts    []model.PlatformArtifact
	Contents     []model.PlatformArtifactContent
	LKGSnapshots []model.PlatformLKGSnapshot
	SourceID     string
	ObservedAt   time.Time
	ValidFor     time.Duration
}

type ArtifactStoreObservation struct {
	Evidence platformsafety.RollbackAssetEvidence
	Detail   string
}

// CollectArtifactStoreObservations verifies rollback generations against
// content-addressed artifact rows and signed LKG snapshots already loaded by a
// caller. It deliberately performs no store, filesystem, or network I/O.
func CollectArtifactStoreObservations(
	requirements []platformsafety.RollbackAssetRequirement,
	inventory ArtifactStoreInventory,
	keyring bundleauth.Keyring,
) []ArtifactStoreObservation {
	observedAt := inventory.ObservedAt.UTC()
	defaultExpiresAt := time.Time{}
	if !observedAt.IsZero() && inventory.ValidFor > 0 {
		defaultExpiresAt = observedAt.Add(inventory.ValidFor)
	}
	observations := make([]ArtifactStoreObservation, 0, len(requirements))
	for _, requirement := range requirements {
		kind := normalizeKind(requirement.Kind)
		if kind == platformsafety.RollbackAssetKindImageDigest || !artifactStoreKind(kind) {
			continue
		}
		observation := ArtifactStoreObservation{Evidence: platformsafety.RollbackAssetEvidence{
			Kind:       kind,
			ScopeKey:   strings.TrimSpace(requirement.ScopeKey),
			Reference:  strings.TrimSpace(requirement.Reference),
			Identity:   strings.TrimSpace(requirement.Identity),
			State:      model.InvariantEvidenceStateUnknown,
			SourceID:   strings.TrimSpace(inventory.SourceID),
			ObservedAt: observedAt,
			ExpiresAt:  defaultExpiresAt,
		}}
		if observation.Evidence.SourceID == "" {
			observation.Detail = "artifact store source identity is missing"
			observations = append(observations, observation)
			continue
		}
		if observedAt.IsZero() {
			observation.Detail = "artifact store observation time is missing"
			observations = append(observations, observation)
			continue
		}
		if inventory.ValidFor <= 0 {
			observation.Detail = "artifact store evidence validity must be positive"
			observations = append(observations, observation)
			continue
		}

		artifacts := matchingArtifacts(inventory.Artifacts, requirement)
		switch len(artifacts) {
		case 0:
			observation.Detail = "pinned artifact generation is missing"
			observations = append(observations, observation)
			continue
		case 1:
		default:
			observation.Evidence.State = model.InvariantEvidenceStateFail
			observation.Detail = "multiple artifacts match the pinned kind, scope, and generation"
			observations = append(observations, observation)
			continue
		}
		artifact := artifacts[0]
		contents := matchingContents(inventory.Contents, artifact.ContentHash)
		switch len(contents) {
		case 0:
			observation.Detail = "content-addressed artifact payload is missing"
			observations = append(observations, observation)
			continue
		case 1:
		default:
			observation.Evidence.State = model.InvariantEvidenceStateFail
			observation.Detail = "content-addressed artifact payload is duplicated"
			observations = append(observations, observation)
			continue
		}
		if err := verifyContent(artifact, contents[0], keyring); err != nil {
			observation.Evidence.State = model.InvariantEvidenceStateFail
			observation.Detail = err.Error()
			observations = append(observations, observation)
			continue
		}

		snapshots := matchingLKGSnapshots(inventory.LKGSnapshots, artifact)
		if len(snapshots) == 0 {
			observation.Detail = "verified LKG snapshot for the pinned generation is missing"
			observations = append(observations, observation)
			continue
		}
		snapshot := newestLKGSnapshot(snapshots)
		decision := platformsafety.EvaluatePlatformLKGSnapshot(snapshot, artifact, keyring, observedAt)
		if !decision.Pass {
			observation.Evidence.State = model.InvariantEvidenceStateFail
			if decisionOnlyExpired(decision) {
				observation.Evidence.State = model.InvariantEvidenceStateStale
				observation.Evidence.ExpiresAt = snapshot.ExpiresAt.UTC()
			}
			observation.Detail = decisionDetail(decision)
			observations = append(observations, observation)
			continue
		}

		observation.Evidence.State = model.InvariantEvidenceStatePass
		if snapshot.ExpiresAt.Before(observation.Evidence.ExpiresAt) {
			observation.Evidence.ExpiresAt = snapshot.ExpiresAt.UTC()
		}
		observation.Detail = "content-addressed artifact and signed verified LKG snapshot are available"
		observations = append(observations, observation)
	}
	return observations
}

func Evidence(observations []ArtifactStoreObservation) []platformsafety.RollbackAssetEvidence {
	evidence := make([]platformsafety.RollbackAssetEvidence, 0, len(observations))
	for _, observation := range observations {
		evidence = append(evidence, observation.Evidence)
	}
	return evidence
}

func artifactStoreKind(kind string) bool {
	switch normalizeKind(kind) {
	case platformsafety.RollbackAssetKindCaddyConfig,
		platformsafety.RollbackAssetKindDNSBundle,
		platformsafety.RollbackAssetKindEdgeRouteBundle,
		platformsafety.RollbackAssetKindNodeDesiredState:
		return true
	default:
		return false
	}
}

func matchingArtifacts(artifacts []model.PlatformArtifact, requirement platformsafety.RollbackAssetRequirement) []model.PlatformArtifact {
	kind := normalizeKind(requirement.Kind)
	scopeKey := strings.TrimSpace(requirement.ScopeKey)
	generation := strings.TrimSpace(requirement.Identity)
	matches := make([]model.PlatformArtifact, 0, 1)
	for _, artifact := range artifacts {
		if normalizeKind(artifact.ArtifactKind) == kind &&
			strings.TrimSpace(artifact.ScopeKey) == scopeKey &&
			strings.TrimSpace(artifact.Generation) == generation {
			matches = append(matches, artifact)
		}
	}
	return matches
}

func matchingContents(contents []model.PlatformArtifactContent, contentHash string) []model.PlatformArtifactContent {
	contentHash = strings.TrimSpace(contentHash)
	matches := make([]model.PlatformArtifactContent, 0, 1)
	for _, content := range contents {
		if strings.EqualFold(strings.TrimSpace(content.ContentHash), contentHash) {
			matches = append(matches, content)
		}
	}
	return matches
}

func verifyContent(artifact model.PlatformArtifact, content model.PlatformArtifactContent, keyring bundleauth.Keyring) error {
	raw, err := json.Marshal(content.Content)
	if err != nil {
		return fmt.Errorf("content-addressed artifact payload is not serializable: %w", err)
	}
	if !strings.EqualFold(strings.TrimSpace(content.ContentHash), strings.TrimSpace(artifact.ContentHash)) ||
		content.SizeBytes != int64(len(raw)) {
		return fmt.Errorf("content-addressed artifact hash or size does not match the pinned artifact")
	}
	storedArtifact := artifact
	storedArtifact.Content = content.Content
	if decision := platformsafety.EvaluateArtifactIntegrity(storedArtifact, keyring); !decision.Pass {
		return fmt.Errorf("content-addressed artifact failed integrity verification: %s", decisionDetail(decision))
	}
	return nil
}

func matchingLKGSnapshots(snapshots []model.PlatformLKGSnapshot, artifact model.PlatformArtifact) []model.PlatformLKGSnapshot {
	matches := make([]model.PlatformLKGSnapshot, 0, 1)
	for _, snapshot := range snapshots {
		if snapshot.ArtifactID == artifact.ID &&
			normalizeKind(snapshot.ArtifactKind) == normalizeKind(artifact.ArtifactKind) &&
			strings.TrimSpace(snapshot.ScopeKey) == strings.TrimSpace(artifact.ScopeKey) &&
			strings.TrimSpace(snapshot.Generation) == strings.TrimSpace(artifact.Generation) {
			matches = append(matches, snapshot)
		}
	}
	return matches
}

func newestLKGSnapshot(snapshots []model.PlatformLKGSnapshot) model.PlatformLKGSnapshot {
	latest := snapshots[0]
	for _, candidate := range snapshots[1:] {
		if candidate.UpdatedAt.After(latest.UpdatedAt) ||
			(candidate.UpdatedAt.Equal(latest.UpdatedAt) && candidate.CreatedAt.After(latest.CreatedAt)) ||
			(candidate.UpdatedAt.Equal(latest.UpdatedAt) && candidate.CreatedAt.Equal(latest.CreatedAt) && candidate.ID > latest.ID) {
			latest = candidate
		}
	}
	return latest
}

func decisionOnlyExpired(decision platformsafety.Decision) bool {
	if len(decision.Violations) == 0 {
		return false
	}
	for _, violation := range decision.Violations {
		if violation.Invariant != platformsafety.InvariantLKGNotExpired {
			return false
		}
	}
	return true
}

func decisionDetail(decision platformsafety.Decision) string {
	parts := make([]string, 0, len(decision.Violations))
	for _, violation := range decision.Violations {
		parts = append(parts, violation.Invariant+": "+violation.Message)
	}
	if len(parts) == 0 {
		return "rollback asset verification failed without a violation detail"
	}
	return strings.Join(parts, "; ")
}

func normalizeKind(kind string) string {
	return strings.ToLower(strings.TrimSpace(kind))
}
