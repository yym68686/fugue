package api

import (
	"net/http"
	"testing"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeartifact"
)

func TestConsumerBindingReuseRetainsFreshTopologyTrustAndPublication(t *testing.T) {
	state, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	compile := func(generation string) platformConfigCompileResponse {
		t.Helper()
		response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, platformConfigCompileRequest{
			Intent: platformconfig.PlatformIntent{Generation: generation, Scope: "global", Routes: []platformconfig.RouteIntent{{Hostname: "binding.example.test", UpstreamURL: "http://origin:8080", Enabled: true}}},
			Policy: platformconfig.PolicySnapshot{Generation: "policy", Scope: "global", TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "first", EdgeGroupIDs: []string{"edge-group-test-a"}}}},
		})
		if response.Code != http.StatusCreated {
			t.Fatal(response.Code, response.Body.String())
		}
		var compiled platformConfigCompileResponse
		mustDecodeJSON(t, response, &compiled)
		return compiled
	}
	principal := model.Principal{ActorType: "test", ActorID: "test"}
	publish := func(compiled platformConfigCompileResponse) model.PlatformArtifactRelease {
		t.Helper()
		_, release, _, _, err := state.ReleasePlatformArtifact(compiled.ReleaseArtifact.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "gray", CanaryRuleRef: "cohort=first", IdempotencyKey: compiled.ReleaseArtifact.ID}, principal)
		if err != nil {
			t.Fatal(err)
		}
		return release
	}
	var revision int64
	prepare := func(compiled platformConfigCompileResponse, release model.PlatformArtifactRelease, group string) {
		t.Helper()
		topology := platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{ID: "edge-a", EdgeGroupID: group}}, DNSNodes: []model.DNSNode{{ID: "dns-a", PhysicalNodeID: "dns-a", EdgeGroupID: group, Zone: "example.test"}}}
		for _, child := range []model.PlatformArtifact{compiled.RouteArtifact, compiled.DNSArtifact, compiled.TLSArtifact} {
			revision++
			set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: compiled.ReleaseArtifact.ID, ArtifactReleaseID: release.ID, ArtifactKind: child.ArtifactKind, ScopeKey: "global", Generation: child.Generation, Revision: revision, Topology: topology})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := state.CreatePlatformExpectedConsumerSet(set); err != nil {
				t.Fatal(err)
			}
		}
	}
	compiled := compile("first")
	release := publish(compiled)
	prepare(compiled, release, "edge-group-test-a")
	claims := platformcontrol.PlatformComponentIdentityClaims{Component: model.PlatformConsumerComponentEdgeWorker, NodeID: "edge-a", ScopeKey: "global", ArtifactKinds: []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig}}
	readArtifact := newConsumerArtifactReader(state.GetPlatformArtifact)
	items, err := server.resolvePlatformConsumerAssignmentsWithReader(claims, readArtifact)
	if err != nil || len(items) != 2 {
		t.Fatal("fixture must own two kinds", items, err)
	}
	readBinding := routeartifact.NewRequestBindingReader()
	for _, item := range items {
		ok, err := server.consumerAssignmentIsSelectedWithBindingReader(item, "edge-group-test-a", readArtifact, readBinding)
		if err != nil || !ok {
			t.Fatal("valid selection failed", ok, err)
		}
	}
	// Reusing projection work cannot retain a group removed by newer topology.
	prepare(compiled, release, "edge-group-test-b")
	if ok, err := server.consumerAssignmentIsSelectedWithBindingReader(items[0], "edge-group-test-a", readArtifact, readBinding); ok || err == nil {
		t.Fatal("reused binding bypassed newer topology", ok, err)
	}
	prepare(compiled, release, "edge-group-test-a")
	if ok, err := server.consumerAssignmentIsSelectedWithBindingReader(items[0], "edge-group-test-a", readArtifact, readBinding); err != nil || !ok {
		t.Fatal("restored topology rejected", ok, err)
	}
	// Server trust can change independently of immutable store observations.
	server.bundleRevokedKeyIDs = append(server.bundleRevokedKeyIDs, compiled.RouteArtifact.Provenance.KeyID)
	if ok, err := server.consumerAssignmentIsSelectedWithBindingReader(items[0], "edge-group-test-a", readArtifact, readBinding); ok || err == nil {
		t.Fatal("reused binding bypassed revoked signing key", ok, err)
	}
	server.bundleRevokedKeyIDs = nil
	if ok, err := server.consumerAssignmentIsSelectedWithBindingReader(items[0], "edge-group-test-a", readArtifact, readBinding); err != nil || !ok {
		t.Fatal("restored trust rejected", ok, err)
	}
	// Publish while returning a reused binding. The existing final authority
	// reread must discard it and retry against the newly prepared publication.
	next := compile("next")
	var nextRelease model.PlatformArtifactRelease
	project := func(parent, child model.PlatformArtifact, assignment model.PlatformConsumerAssignment, released model.PlatformArtifactRelease, keys bundleauth.Keyring) (*model.TrafficReleaseBinding, error) {
		binding, err := readBinding(parent, child, assignment, released, keys)
		if nextRelease.ID == "" {
			nextRelease = publish(next)
			prepare(next, nextRelease, "edge-group-test-a")
		}
		return binding, err
	}
	if ok, err := server.consumerAssignmentIsSelectedWithBindingReader(items[0], "edge-group-test-a", readArtifact, project); err != nil || ok {
		t.Fatal("superseded assignment accepted during observation", ok, err)
	}
	items, err = server.resolvePlatformConsumerAssignmentsWithReader(claims, readArtifact)
	if err != nil || len(items) != 2 {
		t.Fatal("next assignments unavailable", items, err)
	}
	for _, item := range items {
		if item.Release.ID != nextRelease.ID {
			t.Fatal("old release survived resolution")
		}
		if ok, err := server.consumerAssignmentIsSelectedWithBindingReader(item, "edge-group-test-a", readArtifact, readBinding); err != nil || !ok {
			t.Fatal("new publication rejected", ok, err)
		}
	}
}
