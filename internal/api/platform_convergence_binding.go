package api

import (
	"fmt"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func (s *Server) platformConvergenceBinding(set model.PlatformExpectedConsumerSet) *platformcontrol.ConsumerReleaseBinding {
	if set.ReleaseSetID == "" || set.ArtifactReleaseID == "" {
		return nil
	}
	parent, err := s.store.GetPlatformArtifact(set.ReleaseSetID)
	if err != nil || parent.ArtifactKind != model.PlatformArtifactKindReleaseSet || parent.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(parent) != nil || !s.validateReleaseSetReferences(parent).Pass {
		return nil
	}
	release, err := s.store.GetPlatformArtifactRelease(set.ArtifactReleaseID)
	if err != nil || release.ArtifactID != parent.ID || release.ArtifactKind != parent.ArtifactKind || release.Generation != parent.Generation || release.ScopeKey != parent.ScopeKey || release.ScopeKey != set.ScopeKey || release.Status != model.PlatformArtifactReleaseStatusActive || release.FencingToken <= 0 {
		return nil
	}
	current, active, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, release.ScopeKey, release.ReleaseChannel)
	if err != nil || !found || current.ID != parent.ID || active.ID != release.ID || active.FencingToken != release.FencingToken {
		return nil
	}
	newest, err := s.latestActiveReleaseSetPublication(parent)
	if err != nil || newest.ID != release.ID {
		return nil
	}
	child, err := s.consumerAssignmentChild(parent, set.ArtifactKind)
	if err != nil || child.Status != model.PlatformArtifactStatusValidated || child.ScopeKey != set.ScopeKey || child.Generation != set.ExpectedGeneration || child.GenerationSequence <= 0 || s.store.VerifyPlatformArtifactIntegrity(child) != nil {
		return nil
	}
	return &platformcontrol.ConsumerReleaseBinding{ReleaseSetID: parent.ID, ArtifactReleaseID: release.ID, ArtifactKind: child.ArtifactKind, ScopeKey: child.ScopeKey, Generation: child.Generation, FencingToken: release.FencingToken, GenerationSequence: child.GenerationSequence}
}

// A full promotion must account for every member of the newest active release
// of this exact ReleaseSet. Fence counters belong to release lanes, so their
// numeric values cannot order publications in different channels.
func (s *Server) currentReleaseSetExpectations(parent model.PlatformArtifact) ([]model.PlatformExpectedConsumerSet, error) {
	if parent.ArtifactKind != model.PlatformArtifactKindReleaseSet || parent.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(parent) != nil || !s.validateReleaseSetReferences(parent).Pass {
		return nil, fmt.Errorf("release set references or integrity unavailable")
	}
	chosen, err := s.latestActiveReleaseSetPublication(parent)
	if err != nil {
		return nil, err
	}
	raw, ok := parent.Content["artifact_kinds"].([]any)
	if !ok || len(raw) == 0 {
		return nil, fmt.Errorf("release set members unavailable")
	}
	sets := make([]model.PlatformExpectedConsumerSet, 0, len(raw))
	for _, v := range raw {
		kind, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("release set member kind invalid")
		}
		items, err := s.store.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{ReleaseSetID: parent.ID, ArtifactReleaseID: chosen.ID, ArtifactKind: kind, ScopeKey: parent.ScopeKey})
		if err != nil {
			return nil, err
		}
		var latest *model.PlatformExpectedConsumerSet
		for i := range items {
			if latest == nil || items[i].Revision > latest.Revision {
				latest = &items[i]
			}
		}
		if latest == nil {
			return nil, fmt.Errorf("release set expected consumers missing for %s", kind)
		}
		if !latest.RequiresConsumers || s.platformConvergenceBinding(*latest) == nil {
			return nil, fmt.Errorf("release set expected consumer authority unavailable for %s", kind)
		}
		sets = append(sets, *latest)
	}
	return sets, nil
}

func (s *Server) latestActiveReleaseSetPublication(parent model.PlatformArtifact) (*model.PlatformArtifactRelease, error) {
	var chosen *model.PlatformArtifactRelease
	for _, channel := range []string{model.PlatformArtifactReleaseChannelShadow, model.PlatformArtifactReleaseChannelGray, model.PlatformArtifactReleaseChannelFull} {
		current, release, found, err := s.store.GetActivePlatformArtifact(parent.ArtifactKind, parent.ScopeKey, channel)
		if err != nil {
			return nil, err
		}
		if !found || current.ID != parent.ID {
			continue
		}
		if release.Status != model.PlatformArtifactReleaseStatusActive || release.ArtifactID != parent.ID || release.Generation != parent.Generation || release.FencingToken <= 0 || release.ReleasedAt.IsZero() {
			return nil, fmt.Errorf("release set active authority is inconsistent")
		}
		if chosen != nil && release.ReleasedAt.Equal(chosen.ReleasedAt) && release.ID != chosen.ID {
			return nil, fmt.Errorf("release publication order is ambiguous")
		}
		if chosen == nil || release.ReleasedAt.After(chosen.ReleasedAt) {
			copy := release
			chosen = &copy
		}
	}
	if chosen == nil {
		return nil, fmt.Errorf("release set has no active consumer release")
	}
	return chosen, nil
}
