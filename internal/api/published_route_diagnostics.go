package api

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"slices"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

var errMixedRoutePublications = errors.New("groups select different traffic publications; specify edge_group_id")
var errPublishedRouteDiagnostics = errors.New("published traffic artifacts or prepared membership unavailable")

type publishedRouteExplainResponse struct {
	model.RouteExplainResponse
	TrafficRelease *model.TrafficReleaseBinding `json:"traffic_release"`
}

type publishedRouteServingModesResponse struct {
	model.RouteServingModeListResponse
	TrafficRelease    *model.TrafficReleaseBinding `json:"traffic_release"`
	HealthyEdgeGroups map[string]bool              `json:"healthy_edge_groups"`
}

func routeDiagnosticGroup(w http.ResponseWriter, r *http.Request) (string, bool) {
	values, present := r.URL.Query()["edge_group_id"]
	if !present {
		return "", true
	}
	if len(values) != 1 || len(values[0]) > 128 || !trafficSourceGroup.MatchString(values[0]) {
		httpx.WriteError(w, http.StatusBadRequest, "one canonical edge_group_id is required")
		return "", false
	}
	return values[0], true
}

func writeRouteDiagnosticError(w http.ResponseWriter, err error) {
	if errors.Is(err, errMixedRoutePublications) {
		httpx.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	httpx.WriteError(w, http.StatusServiceUnavailable, errPublishedRouteDiagnostics.Error())
}

// Enumerate prepared ownership, not live business routes or process defaults.
// An older gray lane cannot reintroduce groups removed by a newer full release.
func (s *Server) publishedRouteDiagnosticGroups() ([]string, error) {
	full, fullRelease, haveFull, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", model.PlatformArtifactReleaseChannelFull)
	if err != nil {
		return nil, err
	}
	gray, grayRelease, haveGray, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", model.PlatformArtifactReleaseChannelGray)
	if err != nil {
		return nil, err
	}
	type publication struct {
		parent  model.PlatformArtifact
		release model.PlatformArtifactRelease
	}
	parents := []publication{}
	if haveFull {
		parents = append(parents, publication{full, fullRelease})
	}
	if haveGray && (!haveFull || grayRelease.ReleasedAt.After(fullRelease.ReleasedAt)) {
		parents = append(parents, publication{gray, grayRelease})
	}
	groups := []string{}
	for _, selected := range parents {
		parent := selected.parent
		var canary []string
		if selected.release.ReleaseChannel == model.PlatformArtifactReleaseChannelGray {
			canary, err = platformconfig.ResolveTrafficCanary(parent, selected.release.CanaryRuleRef)
			if err != nil {
				return nil, err
			}
		}
		sets, err := s.routeDiagnosticExpectations(parent, selected.release.ID)
		if err != nil {
			return nil, err
		}
		for _, set := range sets {
			if set.ArtifactKind != model.PlatformArtifactKindEdgeRouteBundle {
				continue
			}
			for _, member := range platformcontrol.ProjectExpectedConsumerOwners(set).Consumers {
				if canary != nil && !platformconfig.TrafficCanaryContains(canary, member.Cohort) {
					continue
				}
				if member.Required {
					if !trafficSourceGroup.MatchString(member.Cohort) {
						return nil, errPublishedRouteDiagnostics
					}
					groups = append(groups, member.Cohort)
				}
			}
		}
	}
	if len(groups) == 0 {
		return nil, errPublishedRouteDiagnostics
	}
	return uniqueSortedStrings(groups), nil
}

func (s *Server) routeDiagnosticExpectations(parent model.PlatformArtifact, releaseID string) ([]model.PlatformExpectedConsumerSet, error) {
	sets := []model.PlatformExpectedConsumerSet{}
	for _, kind := range []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindCaddyRouteConfig} {
		candidates, err := s.store.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{ReleaseSetID: parent.ID, ArtifactReleaseID: releaseID, ArtifactKind: kind, ScopeKey: parent.ScopeKey})
		if err != nil {
			return nil, err
		}
		var latest model.PlatformExpectedConsumerSet
		for _, candidate := range candidates {
			if candidate.Revision > latest.Revision {
				latest = candidate
			}
		}
		if latest.ID == "" || !latest.RequiresConsumers {
			return nil, errPublishedRouteDiagnostics
		}
		sets = append(sets, latest)
	}
	return sets, nil
}

func (s *Server) publishedRouteDiagnostics(ctx context.Context, group string) (model.EdgeRouteIntentSnapshot, map[string]bool, error) {
	fail := func(err error) (model.EdgeRouteIntentSnapshot, map[string]bool, error) {
		return model.EdgeRouteIntentSnapshot{}, nil, err
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	groups := []string{group}
	if group == "" {
		var err error
		groups, err = s.publishedRouteDiagnosticGroups()
		if err != nil {
			return fail(err)
		}
	}
	read := newConsumerArtifactReader(s.store.GetPlatformArtifact)
	var snapshot model.EdgeRouteIntentSnapshot
	for _, g := range groups {
		current, found, err := s.edgeRouteIntentSnapshotFromTrafficReleaseWithReader(g, read)
		if err != nil || !found || current.TrafficRelease == nil {
			return fail(errPublishedRouteDiagnostics)
		}
		if snapshot.TrafficRelease != nil && !reflect.DeepEqual(snapshot.TrafficRelease, current.TrafficRelease) {
			return fail(errMixedRoutePublications)
		}
		snapshot = current
	}
	parent, err := read(snapshot.TrafficRelease.ReleaseSetID)
	if err != nil {
		return fail(err)
	}
	sets, err := s.routeDiagnosticExpectations(parent, snapshot.TrafficRelease.ReleaseID)
	if err != nil || len(sets) != 3 {
		return fail(errPublishedRouteDiagnostics)
	}
	healthy := map[string]bool{}
	for _, g := range groups {
		healthy[g] = true
	}
	for _, set := range sets {
		child, err := consumerAssignmentChild(parent, set.ArtifactKind, read)
		if err != nil || !platformsafety.EvaluateArtifactIntegrity(child, s.bundleKeyring()).Pass || !reflect.DeepEqual(platformconfig.LineageFromArtifact(child), platformconfig.LineageFromArtifact(parent)) || set.ArtifactReleaseID != snapshot.TrafficRelease.ReleaseID || set.ExpectedGeneration != child.Generation {
			return fail(errPublishedRouteDiagnostics)
		}
		b := snapshot.TrafficRelease
		binding := &platformcontrol.ConsumerReleaseBinding{ReleaseChannel: b.ReleaseChannel, CanaryEdgeGroups: b.EdgeGroupIDs, ReleaseSetID: parent.ID, ArtifactReleaseID: b.ReleaseID, ArtifactKind: child.ArtifactKind, ScopeKey: child.ScopeKey, Generation: child.Generation, FencingToken: b.FencingToken, GenerationSequence: child.GenerationSequence}
		consumers, err := s.store.ListPlatformConsumers(set.ArtifactKind, set.ScopeKey)
		if err != nil {
			return fail(err)
		}
		status := platformcontrol.EvaluateConsumerConvergence(set, consumers, time.Now().UTC(), binding)
		counts := map[string]int{}
		for _, a := range status.Assessments {
			g := a.Expected.Cohort
			if _, selected := healthy[g]; !selected || !a.Required {
				continue
			}
			counts[g]++
			healthy[g] = healthy[g] && a.State == model.InvariantEvidenceStatePass
		}
		for g := range healthy {
			healthy[g] = healthy[g] && counts[g] > 0 && len(status.UnexpectedConsumers) == 0
		}
	}
	for _, g := range groups {
		current, release, found, err := s.selectTrafficRouteRelease(g)
		if err != nil || !found || current.ID != parent.ID || current.ContentHash != parent.ContentHash || release.ID != snapshot.TrafficRelease.ReleaseID || release.FencingToken != snapshot.TrafficRelease.FencingToken {
			return fail(errPublishedRouteDiagnostics)
		}
	}
	latest, err := s.routeDiagnosticExpectations(parent, snapshot.TrafficRelease.ReleaseID)
	if err != nil || !reflect.DeepEqual(sets, latest) {
		return fail(errPublishedRouteDiagnostics)
	}
	if group == "" {
		latestGroups, err := s.publishedRouteDiagnosticGroups()
		if err != nil || !slices.Equal(groups, latestGroups) {
			return fail(errPublishedRouteDiagnostics)
		}
	}
	return snapshot, healthy, ctx.Err()
}

func publishedRouteDiagnosticBindings(snapshot model.EdgeRouteIntentSnapshot, healthy map[string]bool) []model.EdgeRouteBinding {
	bindings := edgeRouteIntentDiagnosticBindings(snapshot.Routes)
	for i := range bindings {
		r := &bindings[i]
		if r.Status != model.EdgeRouteStatusActive {
			continue
		}
		ready := false
		for group, ok := range healthy {
			ready = ready || ok && (r.PolicyEdgeGroupID == "" || r.PolicyEdgeGroupID == group) && !slices.Contains(r.ExcludedEdgeGroupIDs, group)
		}
		if !ready {
			r.Status = model.EdgeRouteStatusUnavailable
			r.StatusReason = "published traffic consumers have not provided fresh matching serving evidence"
			r.UpstreamURL, r.Upstreams = "", nil
		}
	}
	return bindings
}
