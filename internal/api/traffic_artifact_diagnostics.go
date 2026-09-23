package api

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
	"fugue/internal/routeartifact"
)

type trafficArtifactDiagnostic struct {
	Parents, Releases, Digests            []string
	Routes, DNSRecords, RequiredConsumers int
}

func (d trafficArtifactDiagnostic) evidence() map[string]string {
	return map[string]string{"guardian": "bundle-rollout", "validation_path": "published_traffic_artifact_and_consumer_facts", "release_set_ids": strings.Join(d.Parents, ","), "release_ids": strings.Join(d.Releases, ","), "artifact_digests": strings.Join(d.Digests, ","), "required_consumers": fmt.Sprint(d.RequiredConsumers)}
}

// Diagnostics inspect the actual published configuration and trusted facts.
// They never compile business state or advance an artifact/recovery pointer.
func (s *Server) inspectPublishedTrafficArtifacts(ctx context.Context, zone string, owners map[string]string) (trafficArtifactDiagnostic, error) {
	out := trafficArtifactDiagnostic{}
	fail := func(message string) (trafficArtifactDiagnostic, error) {
		return out, fmt.Errorf("published traffic diagnostics: %s", message)
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	groups := []string{}
	for _, g := range owners {
		groups = append(groups, g)
	}
	groups = uniqueSortedStrings(groups)
	if len(groups) == 0 {
		return fail("declared consumer groups unavailable")
	}
	type selection struct {
		group   string
		parent  model.PlatformArtifact
		release model.PlatformArtifactRelease
	}
	selected := []selection{}
	seen := map[string]bool{}
	for _, group := range groups {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		parent, release, found, err := s.selectTrafficRouteRelease(group)
		if err != nil || !found {
			return fail("current serving publication unavailable")
		}
		selected = append(selected, selection{group, parent, release})
		read := newConsumerArtifactReader(s.store.GetPlatformArtifact)
		if !platformsafety.EvaluateArtifactIntegrity(parent, s.bundleKeyring()).Pass || !validateReleaseSetReferences(parent, read).Pass {
			return fail("parent integrity or member references invalid")
		}
		members := map[string]model.PlatformArtifact{}
		for _, kind := range []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindCaddyRouteConfig} {
			child, err := consumerAssignmentChild(parent, kind, read)
			if err != nil || !platformsafety.EvaluateArtifactIntegrity(child, s.bundleKeyring()).Pass || !reflect.DeepEqual(platformconfig.LineageFromArtifact(parent), platformconfig.LineageFromArtifact(child)) {
				return fail("member integrity or lineage invalid")
			}
			members[kind] = child
		}
		route, err := routeartifact.Project(members[model.PlatformArtifactKindEdgeRouteBundle])
		if err != nil {
			return fail("route artifact schema invalid")
		}
		for node, owner := range owners {
			if owner != group {
				continue
			}
			records, viewGroup, err := platformDNSArtifactView(members[model.PlatformArtifactKindDNSAnswerBundle], node, zone)
			if err != nil || viewGroup != group {
				return fail("declared DNS consumer zone view unavailable")
			}
			out.DNSRecords += len(records)
		}
		if seen[release.ID] {
			continue
		}
		seen[release.ID] = true
		out.Parents = append(out.Parents, parent.ID)
		out.Releases = append(out.Releases, release.ID)
		out.Routes += len(route.Routes)
		for _, kind := range []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindCaddyRouteConfig} {
			out.Digests = append(out.Digests, kind+"="+members[kind].ContentHash)
		}
		sets, err := s.currentReleaseSetExpectations(parent)
		if err != nil || len(sets) != 3 {
			return fail("exact prepared consumer sets unavailable")
		}
		for _, set := range sets {
			if set.ArtifactReleaseID != release.ID {
				return fail("prepared consumer publication changed")
			}
			consumers, err := s.store.ListPlatformConsumers(set.ArtifactKind, set.ScopeKey)
			if err != nil {
				return fail("consumer observations unavailable")
			}
			binding := s.platformConvergenceBindingWithReader(set, read)
			if binding == nil {
				return fail("consumer publication binding invalid")
			}
			status := s.evaluateLiveConsumerConvergence(ctx, platformcontrol.ProjectExpectedConsumerOwners(set), consumers, binding)
			if !status.Pass || status.RequiredExpected == 0 {
				return fail("required consumers have not converged for " + set.ArtifactKind)
			}
			out.RequiredConsumers += status.RequiredPassing
		}
	}
	// A promotion can occur while the read is in progress. Do not return a
	// mixture of publication identities as a passing observation.
	for _, prior := range selected {
		parent, release, found, err := s.selectTrafficRouteRelease(prior.group)
		if err != nil || !found || parent.ID != prior.parent.ID || parent.ContentHash != prior.parent.ContentHash || release.ID != prior.release.ID || release.FencingToken != prior.release.FencingToken {
			return fail("publication changed during observation")
		}
	}
	sort.Strings(out.Parents)
	sort.Strings(out.Releases)
	sort.Strings(out.Digests)
	return out, ctx.Err()
}
