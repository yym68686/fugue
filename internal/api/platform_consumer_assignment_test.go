package api

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestPlatformConsumerAssignmentUsesActiveReleaseAndVerifiedIdentity(t *testing.T) {
	t.Parallel()
	state, server, tenant, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	keyring := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "assignment-key", Keys: map[string]string{"assignment-key": "synthetic-assignment-key"}}
	server.auth.PlatformComponentIdentityKeyring = keyring
	issue := func(component, node, scope, kind string) string {
		t.Helper()
		token, err := platformcontrol.IssuePlatformComponentIdentity(keyring, platformcontrol.PlatformComponentIdentityClaims{CredentialID: "assignment-reader", Component: component, NodeID: node, ScopeKey: scope, ArtifactKinds: []string{kind}}, time.Now().UTC(), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	token := issue(model.PlatformConsumerComponentEdgeWorker, "assignment-node", "global", model.PlatformArtifactKindEdgeRouteBundle)
	compile := func(generation string) platformConfigCompileResponse {
		t.Helper()
		response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, map[string]any{
			"intent": map[string]any{"generation": generation, "scope": "global", "routes": []any{map[string]any{"hostname": "assignment.example.test", "upstream_url": "http://origin:8080", "enabled": true}}},
			"policy": map[string]any{"generation": "assignment-policy", "scope": "global", "minimum_healthy_edges": 1, "max_stale_seconds": 86400, "dependency_order": []string{"route", "tls", "dns"}},
		})
		if response.Code != http.StatusCreated {
			t.Fatalf("compile: %d %s", response.Code, response.Body.String())
		}
		var body platformConfigCompileResponse
		mustDecodeJSON(t, response, &body)
		return body
	}
	release := func(artifact model.PlatformArtifact) model.PlatformArtifactRelease {
		t.Helper()
		response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+artifact.ID+"/release", admin, model.PlatformArtifactReleaseRequest{ReleaseChannel: model.PlatformArtifactReleaseChannelShadow, Reason: "assignment test", IdempotencyKey: artifact.ID})
		if response.Code != http.StatusOK {
			t.Fatalf("release: %d %s", response.Code, response.Body.String())
		}
		var body model.PlatformArtifactReleaseResponse
		mustDecodeJSON(t, response, &body)
		return body.Release
	}
	buildSet := func(compiled platformConfigCompileResponse, released model.PlatformArtifactRelease, revision int64, node string) model.PlatformExpectedConsumerSet {
		t.Helper()
		set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: compiled.ReleaseArtifact.ID, ArtifactReleaseID: released.ID, ArtifactKind: compiled.RouteArtifact.ArtifactKind, Scope: compiled.RouteArtifact.Scope, ScopeKey: compiled.RouteArtifact.ScopeKey, Generation: compiled.RouteArtifact.Generation, Revision: revision, PreparedAt: time.Now().UTC(), Topology: platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{ID: node, EdgeGroupID: "assignment-group", Country: "US"}}}})
		if err != nil {
			t.Fatal(err)
		}
		return set
	}
	persist := func(set model.PlatformExpectedConsumerSet) {
		t.Helper()
		if _, err := state.CreatePlatformExpectedConsumerSet(set); err != nil {
			t.Fatal(err)
		}
	}
	get := func(credential string, code int) model.PlatformConsumerAssignmentResponse {
		t.Helper()
		response := performJSONRequest(t, server, http.MethodGet, "/v1/platform-state/consumers/assignment", credential, nil)
		if response.Code != code {
			t.Fatalf("assignment: wanted %d, got %d %s", code, response.Code, response.Body.String())
		}
		var body model.PlatformConsumerAssignmentResponse
		if code == http.StatusOK {
			mustDecodeJSON(t, response, &body)
		}
		return body
	}
	compiled := compile("assignment-intent-one")
	released := release(compiled.ReleaseArtifact)
	set := buildSet(compiled, released, 1, "assignment-node")
	persist(set)
	// Newer unrelated topology must not evict the assignment from a global limit.
	for i := 0; i < 205; i++ {
		noise := buildSet(compiled, released, int64(i+2), "unrelated-node")
		noise.ID = fmt.Sprintf("noise-%d", i)
		noise.ReleaseSetID = "unrelated-release-set"
		persist(noise)
	}
	body := get(token, http.StatusOK)
	if len(body.Assignments) != 1 {
		t.Fatalf("expected one authorized assignment: %+v", body)
	}
	assignment := body.Assignments[0]
	if assignment.ExpectedConsumerSetID != set.ID || assignment.ReleaseSetID != compiled.ReleaseArtifact.ID || assignment.ArtifactID != compiled.RouteArtifact.ID || assignment.ContentHash != compiled.RouteArtifact.ContentHash || assignment.GenerationSequence != compiled.RouteArtifact.GenerationSequence || assignment.ExpectedGeneration != compiled.RouteArtifact.Generation || assignment.FencingToken != released.FencingToken || assignment.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow {
		t.Fatalf("unbound assignment: %+v", assignment)
	}
	consumers, err := state.ListPlatformConsumers(compiled.RouteArtifact.ArtifactKind, "global")
	if err != nil || len(consumers) != 0 {
		t.Fatalf("assignment must not write runtime facts: %+v %v", consumers, err)
	}
	get(tenant, http.StatusUnauthorized)
	get(admin, http.StatusUnauthorized)
	get(issue(model.PlatformConsumerComponentEdgeWorker, "other-node", "global", compiled.RouteArtifact.ArtifactKind), http.StatusNotFound)
	get(issue(model.PlatformConsumerComponentDNSServer, "assignment-node", "global", compiled.RouteArtifact.ArtifactKind), http.StatusNotFound)
	get(issue(model.PlatformConsumerComponentEdgeWorker, "assignment-node", "other-scope", compiled.RouteArtifact.ArtifactKind), http.StatusNotFound)
	get(issue(model.PlatformConsumerComponentEdgeWorker, "assignment-node", "global", model.PlatformArtifactKindDNSAnswerBundle), http.StatusNotFound)
	// Superseded release records cannot continue assigning their artifact.
	next := compile("assignment-intent-two")
	nextRelease := release(next.ReleaseArtifact)
	get(token, http.StatusNotFound)
	nextSet := buildSet(next, nextRelease, 1, "assignment-node")
	persist(nextSet)
	body = get(token, http.StatusOK)
	if len(body.Assignments) != 1 || body.Assignments[0].ArtifactReleaseID != nextRelease.ID || body.Assignments[0].FencingToken <= released.FencingToken {
		t.Fatalf("stale release assignment: %+v", body)
	}
	// Removing this node in the latest revision must not resurrect revision 1.
	persist(buildSet(next, nextRelease, 2, "replacement-node"))
	get(token, http.StatusNotFound)
	// A matching identity with a stale child generation must fail closed.
	invalid := buildSet(next, nextRelease, 3, "assignment-node")
	invalid.ExpectedGeneration = "incorrect-generation"
	for i := range invalid.Consumers {
		invalid.Consumers[i].ExpectedGeneration = invalid.ExpectedGeneration
	}
	persist(invalid)
	get(token, http.StatusServiceUnavailable)
	// Preparation cannot associate this ReleaseSet with its predecessor's release.
	rejected := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/release-set/prepare-consumers", admin, map[string]any{"release_set_id": next.ReleaseArtifact.ID, "artifact_release_id": released.ID})
	if rejected.Code != http.StatusConflict {
		t.Fatalf("unrelated release binding: %d %s", rejected.Code, rejected.Body.String())
	}
}
