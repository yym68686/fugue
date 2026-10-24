package api

import (
	"fugue/internal/model"
	"sort"
	"strings"
)

// Only explicit shadow convergence can consult the isolated observation lane.
// Legacy initial-enrollment receipts remain readable when no new receipt exists.
func (s *Server) platformConsumersForConvergence(set model.PlatformExpectedConsumerSet) ([]model.PlatformConsumerInstance, error) {
	serving, err := s.store.ListPlatformConsumers(set.ArtifactKind, set.ScopeKey)
	if err != nil || set.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || !strings.HasPrefix(set.ScopeKey, "authority-cell:") || set.ArtifactReleaseID == "" {
		return serving, err
	}
	release, err := s.store.GetPlatformArtifactRelease(set.ArtifactReleaseID)
	if err != nil {
		return nil, err
	}
	if release.ReleaseChannel != "shadow" {
		return serving, nil
	}
	shadow, err := s.store.ListPlatformShadowConsumers(set.ArtifactKind, set.ScopeKey)
	if err != nil {
		return nil, err
	}
	byID := map[string]model.PlatformConsumerInstance{}
	for _, v := range serving {
		byID[v.ConsumerID] = v
	}
	for _, v := range shadow {
		if v.ExpectedConsumerSetID == set.ID && v.ReleaseSetID == set.ReleaseSetID && v.FencingToken == release.FencingToken {
			byID[v.ConsumerID] = v
		}
	}
	out := make([]model.PlatformConsumerInstance, 0, len(byID))
	for _, v := range byID {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ConsumerID < out[j].ConsumerID })
	return out, nil
}
