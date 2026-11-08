package store

import (
	"context"
	"fmt"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// GetPlatformArtifactRuntimeInput reads only the exact signed operational
// input. It neither restores archives nor falls back to original history.
// Callers still consume CheckPlatformInputRead for their complete source set
// and recheck source identity before performing any business mutation.
func (s *Store) GetPlatformArtifactRuntimeInput(a model.PlatformArtifact) (platformconfig.RuntimeSnapshot, error) {
	return s.GetPlatformArtifactRuntimeInputContext(context.Background(), a)
}

func (s *Store) GetPlatformArtifactRuntimeInputContext(ctx context.Context, a model.PlatformArtifact) (platformconfig.RuntimeSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return platformconfig.RuntimeSnapshot{}, err
	}
	fail := func() (platformconfig.RuntimeSnapshot, error) {
		return platformconfig.RuntimeSnapshot{}, fmt.Errorf("runtime input contract or lineage invalid: %w", ErrConflict)
	}
	if s.VerifyPlatformArtifactIntegrity(a) != nil {
		return fail()
	}
	digest, err := platformconfig.ArtifactRuntimeInputDigest(a)
	if err != nil {
		return fail()
	}
	if digest == "" {
		return platformconfig.RuntimeSnapshot{}, ErrNotFound
	}
	snapshot, err := s.GetPlatformCompilerInputContext(ctx, digest)
	if err != nil {
		return platformconfig.RuntimeSnapshot{}, err
	}
	if snapshot.IntentGeneration != a.Metadata["intent_generation"] || snapshot.PolicyGeneration != a.Metadata["policy_generation"] {
		return fail()
	}
	if a.Metadata["runtime_input_digest"] != "" {
		compactDigest, err := platformconfig.RuntimeSnapshotDigest(platformconfig.ConfigurationRuntimeInput(snapshot))
		if ctx.Err() != nil {
			return platformconfig.RuntimeSnapshot{}, ctx.Err()
		}
		if err != nil || compactDigest != digest {
			return fail()
		}
	}
	return snapshot, nil
}
