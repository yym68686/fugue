package api

import (
	"errors"
	"regexp"
	"sort"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeartifact"
	"fugue/internal/trafficbinding"
)

var trafficSourceGroup = regexp.MustCompile(edgetopology.AuthorityIDPattern)
var errTrafficSourceChanged = errors.New("traffic release changed during route observation")

// A release selection is authoritative even when it cannot yet be consumed.
// Returning an error preserves the consumer's serving artifact instead of
// reinterpreting a broken or unprepared release as permission to read business
// tables. The bootstrap fallback is consulted only when no release selects us.
func (s *Server) edgeRouteIntentSnapshotFromTrafficRelease(group string) (model.EdgeRouteIntentSnapshot, bool, error) {
	return s.edgeRouteIntentSnapshotFromTrafficReleaseWithReader(group, newConsumerArtifactReader(s.store.GetPlatformArtifact))
}

func (s *Server) edgeRouteIntentSnapshotFromTrafficReleaseWithReader(group string, readArtifact func(string) (model.PlatformArtifact, error)) (model.EdgeRouteIntentSnapshot, bool, error) {
	return s.edgeRouteIntentSnapshotFromTrafficScope(group, trafficRouteScope(group), readArtifact)
}

func (s *Server) edgeRouteIntentSnapshotFromTrafficScope(group, scope string, readArtifact func(string) (model.PlatformArtifact, error)) (model.EdgeRouteIntentSnapshot, bool, error) {
	for attempt := 0; ; attempt++ {
		snapshot, found, err := s.observeTrafficRouteSource(group, scope, readArtifact)
		if !errors.Is(err, errTrafficSourceChanged) || attempt == 2 {
			return snapshot, found, err
		}
	}
}

func (s *Server) observeTrafficRouteSource(group, scope string, readArtifact func(string) (model.PlatformArtifact, error)) (model.EdgeRouteIntentSnapshot, bool, error) {
	defer s.observeOperation("traffic-source")()
	parent, release, found, err := s.selectTrafficRouteReleaseInScope(group, scope)
	if err != nil || !found {
		return model.EdgeRouteIntentSnapshot{}, found, err
	}
	fail := func() (model.EdgeRouteIntentSnapshot, bool, error) {
		return model.EdgeRouteIntentSnapshot{}, true, errors.New("traffic release route source is unavailable")
	}
	if !validateReleaseSetReferences(parent, readArtifact).Pass {
		return fail()
	}
	var child model.PlatformArtifact
	var latest model.PlatformExpectedConsumerSet
	// Every member of the signed publication role must be prepared. An ordinary
	// traffic release still requires route, DNS and TLS membership.
	kinds, err := platformconfig.ValidateReleaseComposition(parent)
	if err != nil {
		return fail()
	}
	for _, kind := range kinds {
		artifact, err := consumerAssignmentChild(parent, kind, readArtifact)
		if err != nil || artifact.ScopeKey != parent.ScopeKey || s.store.VerifyPlatformArtifactIntegrity(artifact) != nil {
			return fail()
		}
		sets, err := s.store.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{ReleaseSetID: parent.ID, ArtifactReleaseID: release.ID, ArtifactKind: kind, ScopeKey: parent.ScopeKey})
		if err != nil {
			return fail()
		}
		var set model.PlatformExpectedConsumerSet
		for _, candidate := range sets {
			if candidate.Revision > set.Revision {
				set = candidate
			}
		}
		if set.ID == "" || set.ExpectedGeneration != artifact.Generation {
			return fail()
		}
		if platformcontrol.ValidateDeclaredTrafficConsumerSet(parent, set) != nil {
			return fail()
		}
		component := model.PlatformConsumerComponentEdgeWorker
		if kind == model.PlatformArtifactKindDNSAnswerBundle {
			component = model.PlatformConsumerComponentDNSServer
		}
		member := false
		for _, e := range platformcontrol.ProjectExpectedConsumerOwners(set).Consumers {
			if e.Cohort == group && e.Required && e.Component == component && e.ArtifactKind == kind && e.ScopeKey == parent.ScopeKey && e.ExpectedGeneration == artifact.Generation {
				member = true
				break
			}
		}
		if !member {
			return fail()
		}
		if kind == model.PlatformArtifactKindEdgeRouteBundle {
			child, latest = artifact, set
		}
	}
	a := model.PlatformConsumerAssignment{ExpectedConsumerSetID: latest.ID, Revision: latest.Revision, ArtifactReleaseID: release.ID, ReleaseSetID: parent.ID, ArtifactID: child.ID, ArtifactKind: child.ArtifactKind, ScopeKey: parent.ScopeKey, ExpectedGeneration: child.Generation, ContentHash: child.ContentHash, GenerationSequence: child.GenerationSequence, FencingToken: release.FencingToken, ReleaseChannel: release.ReleaseChannel}
	snapshot, err := routeartifact.ProjectRelease(parent, child, a, release, s.bundleKeyring())
	if err != nil || trafficbinding.ValidateGroup(snapshot.TrafficRelease, group, true) != nil {
		return fail()
	}
	// Publication/rollback can supersede an assignment while its artifacts
	// are read. Never return a mixed release snapshot to the executor.
	current, active, found, err := s.selectTrafficRouteReleaseInScope(group, scope)
	if err != nil || !found || active.Status != model.PlatformArtifactReleaseStatusActive {
		return fail()
	}
	if current.ID != parent.ID || current.ContentHash != parent.ContentHash || active.ID != release.ID || active.FencingToken != release.FencingToken || active.CanaryRuleRef != release.CanaryRuleRef {
		return model.EdgeRouteIntentSnapshot{}, true, errTrafficSourceChanged
	}
	return snapshot, true, nil
}

// A newer full publication supersedes an older gray lane. Newer canaries
// override full only in their signed cohort; timestamps order cross-lane
// publication, whereas each lane retains its own independent fencing token.
func (s *Server) selectTrafficRouteRelease(group string) (model.PlatformArtifact, model.PlatformArtifactRelease, bool, error) {
	return s.selectTrafficRouteReleaseInScope(group, trafficRouteScope(group))
}

func trafficRouteScope(group string) string {
	if cell := platformcontrol.ConsumerAuthorityID(group); cell != "" {
		return platformconfig.AuthorityCellScope(cell)
	}
	return "global"
}

func (s *Server) selectTrafficRouteReleaseInScope(group, scope string) (model.PlatformArtifact, model.PlatformArtifactRelease, bool, error) {
	type candidate struct {
		parent  model.PlatformArtifact
		release model.PlatformArtifactRelease
	}
	candidates := []candidate{}
	fail := func() (model.PlatformArtifact, model.PlatformArtifactRelease, bool, error) {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, true, errors.New("traffic route release selection invalid")
	}
	if group == "" || len(group) > 128 || !trafficSourceGroup.MatchString(group) || (scope != "global" && scope != trafficRouteScope(group)) {
		return fail()
	}
	for _, channel := range []string{model.PlatformArtifactReleaseChannelFull, model.PlatformArtifactReleaseChannelGray} {
		parent, release, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, scope, channel)
		if err != nil {
			return fail()
		}
		if found {
			candidates = append(candidates, candidate{parent, release})
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].release.ReleasedAt.After(candidates[j].release.ReleasedAt) })
	for _, c := range candidates {
		parent, release := c.parent, c.release
		if parent.ScopeKey != scope || release.ScopeKey != scope || parent.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(parent) != nil || release.ReleasedAt.IsZero() {
			return fail()
		}
		if scope != "global" {
			topology, err := platformconfig.TrafficConsumersFromRelease(parent)
			if err != nil || topology == nil || topology.AuthorityCellID != group {
				return fail()
			}
		}
		if release.ReleaseChannel == model.PlatformArtifactReleaseChannelGray {
			groups, err := platformconfig.ResolveTrafficCanary(parent, release.CanaryRuleRef)
			if err != nil {
				return fail()
			}
			if !platformconfig.TrafficCanaryContains(groups, group) {
				continue
			}
		}
		return parent, release, true, nil
	}
	return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, false, nil
}
