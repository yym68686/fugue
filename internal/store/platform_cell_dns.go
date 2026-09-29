package store

import (
	"fmt"

	"fugue/internal/bundleauth"
	"fugue/internal/cellpublication"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

// Called while the publication transaction holds its authority locks. A DNS
// publication cannot endorse a stale Cell fence read before that transaction.
func validateCellDNSReferencesInState(state *model.State, child model.PlatformArtifact, keys bundleauth.Keyring) error {
	pubs, err := cellpublication.VerifyDNSArtifact(child, keys)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrConflict, err)
	}
	fail := func() error { return fmt.Errorf("%w: referenced Cell publication is no longer selected", ErrConflict) }
	for _, p := range pubs {
		ref := p.Reference
		var selected *model.PlatformArtifactRelease
		for _, r := range state.PlatformArtifactReleases {
			if r.ScopeKey != p.Parent.ScopeKey || r.ArtifactKind != model.PlatformArtifactKindReleaseSet || r.Status != model.PlatformArtifactReleaseStatusActive || (r.ReleaseChannel != "gray" && r.ReleaseChannel != "full") {
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
				if !platformconfig.TrafficCanaryContains(groups, ref.AuthorityCellID) {
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
		if selected == nil || selected.ID != ref.ReleaseID || selected.ArtifactID != ref.ReleaseSetID || selected.Generation != p.Parent.Generation || selected.FencingToken != ref.FencingToken || selected.ReleaseChannel != ref.ReleaseChannel || selected.CanaryRuleRef != ref.CanaryRuleRef {
			return fail()
		}
		for _, embedded := range []model.PlatformArtifact{p.Parent, p.Route, p.TLS} {
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
