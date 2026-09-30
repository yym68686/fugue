package store

import (
	"context"
	"fmt"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/cellpublication"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

// Called while the publication transaction holds its authority locks. A DNS
// publication cannot endorse a stale Cell fence read before that transaction.
func validateCellDNSReferencesInState(state *model.State, child model.PlatformArtifact, keys bundleauth.Keyring) error {
	_, err := cellpublication.VerifyDNSArtifact(child, keys)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrConflict, err)
	}
	fail := func() error { return fmt.Errorf("%w: referenced Cell publication is no longer selected", ErrConflict) }
	dependencies, err := platformconfig.DNSRouteDependencies(child)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrConflict, err)
	}
	for _, dependency := range dependencies {
		var selected *model.PlatformArtifactRelease
		for _, r := range state.PlatformArtifactReleases {
			if r.ScopeKey != dependency.Parent.ScopeKey || r.ArtifactKind != model.PlatformArtifactKindReleaseSet || r.Status != model.PlatformArtifactReleaseStatusActive || (r.ReleaseChannel != "gray" && r.ReleaseChannel != "full") {
				continue
			}
			lane, ok := platformReleaseLaneByKey(state.PlatformReleaseLanes, r.LaneKey)
			if !ok || lane.Frozen || lane.ActiveReleaseID != r.ID || lane.FencingToken != r.FencingToken {
				return fail()
			}
			index := platformArtifactIndex(state.PlatformArtifacts, r.ArtifactID)
			if index < 0 {
				return fail()
			}
			if r.ReleaseChannel == "gray" {
				groups, err := platformconfig.ResolveTrafficCanary(state.PlatformArtifacts[index], r.CanaryRuleRef)
				if err != nil {
					return fail()
				}
				if !platformconfig.TrafficCanaryContains(groups, dependency.GroupID) {
					continue
				}
			}
			if selected != nil && selected.ReleasedAt.Equal(r.ReleasedAt) && selected.ID != r.ID {
				return fail()
			}
			if selected == nil || r.ReleasedAt.After(selected.ReleasedAt) {
				copy := r
				selected = &copy
			}
		}
		if selected == nil || selected.ID != dependency.ReleaseID || selected.ArtifactID != dependency.Parent.ID || selected.Generation != dependency.Parent.Generation || selected.FencingToken != dependency.FencingToken || selected.ReleaseChannel != dependency.ReleaseChannel || selected.CanaryRuleRef != dependency.CanaryRuleRef {
			return fail()
		}
		if dependency.RequireVerified && (selected.VerificationState != model.PlatformArtifactVerificationStateVerified || selected.VerifiedLKGGeneration != dependency.Parent.Generation) {
			return fail()
		}
		for _, embedded := range dependency.Artifacts {
			index := platformArtifactIndex(state.PlatformArtifacts, embedded.ID)
			if index < 0 {
				return fail()
			}
			a := state.PlatformArtifacts[index]
			if a.ContentHash != embedded.ContentHash || a.GenerationSequence != embedded.GenerationSequence || a.ScopeKey != embedded.ScopeKey || a.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(a, keys).Pass {
				return fail()
			}
		}
	}
	return nil
}

// ValidateDNSPublicationReferences checks one coherent read snapshot during
// compilation. Serving publication repeats these same checks under its own
// transaction; compiling immutable content is never a serving grant.
func (s *Store) ValidateDNSPublicationReferences(child model.PlatformArtifact) error {
	dependencies, err := platformconfig.DNSRouteDependencies(child)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrConflict, err)
	}
	if len(dependencies) == 0 {
		return nil
	}
	keys := s.platformArtifactSigningKeyring()
	if s.db == nil {
		return s.withLockedState(false, func(state *model.State) error { return validateCellDNSReferencesInState(state, child, keys) })
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	state := &model.State{PlatformArtifacts: []model.PlatformArtifact{child}}
	parent := model.PlatformArtifact{ScopeKey: child.ScopeKey, Content: map[string]any{"publication_role": platformconfig.PublicationRoleCellDNS}}
	if err := s.pgLoadCellDNSReferences(ctx, tx, parent, state); err != nil {
		return err
	}
	return validateCellDNSReferencesInState(state, child, keys)
}
