package store

import (
	"context"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
)

// The verified publication ledger is durable across restarts, LKG expiry and
// later operator publications. A changed current baseline cannot reset a bound.
func hasVerifiedProducerPublication(state *model.State, scope, policyRelease string) bool {
	parents := map[string]bool{}
	for _, a := range state.PlatformArtifacts {
		if a.ArtifactKind == model.PlatformArtifactKindReleaseSet && a.ScopeKey == scope && a.Metadata[platformproducer.PolicyReleaseMetadata] == policyRelease {
			parents[a.ID] = true
		}
	}
	for _, r := range state.PlatformArtifactReleases {
		if parents[r.ArtifactID] && r.ScopeKey == scope && r.ArtifactKind == model.PlatformArtifactKindReleaseSet && r.ReleaseChannel == "full" && producerOwnsPublication(r) && r.VerificationState == model.PlatformArtifactVerificationStateVerified && r.VerifiedLKGGeneration == r.Generation {
			return true
		}
	}
	return false
}

func pgHasVerifiedProducerPublication(ctx context.Context, db platformStateDB, scope, policyRelease string) (bool, error) {
	var found bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (
SELECT 1 FROM fugue_platform_artifacts a JOIN fugue_platform_artifact_releases r ON r.artifact_id=a.id
WHERE a.artifact_kind=$1 AND a.scope_key=$2 AND a.metadata_json->>$3=$4
AND r.artifact_kind=$1 AND r.scope_key=$2 AND r.release_channel='full'
AND r.released_by_type=$5 AND r.released_by_id=$6 AND r.verification_state=$7
AND r.verified_lkg_generation=r.generation)`, model.PlatformArtifactKindReleaseSet, scope, platformproducer.PolicyReleaseMetadata, policyRelease, model.ActorTypeBootstrap, platformproducer.Actor, model.PlatformArtifactVerificationStateVerified).Scan(&found)
	return found, err
}

func (s *Store) HasVerifiedProducerPublication(scope, policyRelease string) (bool, error) {
	if scope == "" || policyRelease == "" {
		return false, ErrInvalidInput
	}
	if s.db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return pgHasVerifiedProducerPublication(ctx, s.db, scope, policyRelease)
	}
	var found bool
	err := s.withLockedState(false, func(state *model.State) error {
		found = hasVerifiedProducerPublication(state, scope, policyRelease)
		return nil
	})
	return found, err
}
