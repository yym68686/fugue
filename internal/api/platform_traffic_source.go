package api

import (
	"errors"
	"regexp"
	"sort"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeartifact"
	"fugue/internal/trafficbinding"
)

var trafficSourceGroup = regexp.MustCompile(`^edge-group-[a-z0-9]+(?:-[a-z0-9]+)*$`)

// A release selection is authoritative even when it cannot yet be consumed.
// Returning an error preserves the consumer's serving artifact instead of
// reinterpreting a broken or unprepared release as permission to read business
// tables. The bootstrap fallback is consulted only when no release selects us.
func (s *Server) edgeRouteIntentSnapshotFromTrafficRelease(group string) (model.EdgeRouteIntentSnapshot, bool, error) {
	parent, release, found, err := s.selectTrafficRouteRelease(group)
	if err != nil || !found {
		return model.EdgeRouteIntentSnapshot{}, found, err
	}
	fail := func() (model.EdgeRouteIntentSnapshot, bool, error) {
		return model.EdgeRouteIntentSnapshot{}, true, errors.New("traffic release route source is unavailable")
	}
	if !s.validateReleaseSetReferences(parent).Pass {
		return fail()
	}
	var child model.PlatformArtifact
	var latest model.PlatformExpectedConsumerSet
	// Read all three prepared topologies; a partially completed preparation
	// cannot activate only the route member of a traffic release.
	for _, kind := range []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindCaddyRouteConfig} {
		artifact, err := s.consumerAssignmentChild(parent, kind)
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
	current, active, found, err := s.selectTrafficRouteRelease(group)
	if err != nil || !found || current.ID != parent.ID || current.ContentHash != parent.ContentHash || active.ID != release.ID || active.FencingToken != release.FencingToken || active.CanaryRuleRef != release.CanaryRuleRef || active.Status != model.PlatformArtifactReleaseStatusActive {
		return fail()
	}
	return snapshot, true, nil
}

// A newer full publication supersedes an older gray lane. Newer canaries
// override full only in their signed cohort; timestamps order cross-lane
// publication, whereas each lane retains its own independent fencing token.
func (s *Server) selectTrafficRouteRelease(group string) (model.PlatformArtifact, model.PlatformArtifactRelease, bool, error) {
	type candidate struct {
		parent  model.PlatformArtifact
		release model.PlatformArtifactRelease
	}
	candidates := []candidate{}
	fail := func() (model.PlatformArtifact, model.PlatformArtifactRelease, bool, error) {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, true, errors.New("traffic route release selection invalid")
	}
	for _, channel := range []string{model.PlatformArtifactReleaseChannelFull, model.PlatformArtifactReleaseChannelGray} {
		parent, release, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", channel)
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
		if group == "" || len(group) > 128 || !trafficSourceGroup.MatchString(group) || parent.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(parent) != nil || release.ReleasedAt.IsZero() {
			return fail()
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
