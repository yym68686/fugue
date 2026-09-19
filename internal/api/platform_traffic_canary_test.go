package api

import (
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"net/http"
	"testing"
	"time"
)

func TestTrafficCanaryAssignmentsDownloadAndHeartbeatAreGroupBound(t *testing.T) {
	state, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, map[string]any{
		"intent": map[string]any{"generation": "intent", "scope": "global", "routes": []any{map[string]any{"hostname": "app.example.test", "upstream_url": "http://origin:8080", "enabled": true}}},
		"policy": map[string]any{"generation": "policy", "scope": "global", "traffic_rollout_cohorts": []any{map[string]any{"id": "first", "edge_group_ids": []string{"edge-group-a"}}, map[string]any{"id": "complete", "edge_group_ids": []string{"edge-group-a", "edge-group-b"}}}},
	})
	if response.Code != 201 {
		t.Fatal(response.Code, response.Body.String())
	}
	var compiled platformConfigCompileResponse
	mustDecodeJSON(t, response, &compiled)
	_, released, _, _, err := state.ReleasePlatformArtifact(compiled.ReleaseArtifact.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "gray", CanaryRuleRef: "cohort=first"}, model.Principal{ActorType: "test", ActorID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	topology := platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{ID: "node-a", EdgeGroupID: "edge-group-a"}, {ID: "node-b", EdgeGroupID: "edge-group-b"}}, DNSNodes: []model.DNSNode{{ID: "dns-a", PhysicalNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test"}, {ID: "dns-b", PhysicalNodeID: "dns-b", EdgeGroupID: "edge-group-b", Zone: "example.test"}}}
	keyring := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "key", Keys: map[string]string{"key": "synthetic-canary-identity-secret"}}
	server.auth.PlatformComponentIdentityKeyring = keyring
	for i, child := range []model.PlatformArtifact{compiled.RouteArtifact, compiled.TLSArtifact, compiled.DNSArtifact} {
		set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: compiled.ReleaseArtifact.ID, ArtifactReleaseID: released.ID, ArtifactKind: child.ArtifactKind, ScopeKey: "global", Generation: child.Generation, Revision: int64(i + 1), PreparedAt: time.Now().UTC(), Topology: topology})
		if err != nil {
			t.Fatal(err)
		}
		if set.RequiredCardinality != 2 {
			t.Fatal("expected topology shrank")
		}
		if _, err = state.CreatePlatformExpectedConsumerSet(set); err != nil {
			t.Fatal(err)
		}
		for _, expected := range set.Consumers {
			claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: "reader", Component: expected.Component, NodeID: expected.NodeID, ScopeKey: "global", ArtifactKinds: []string{child.ArtifactKind}}
			token, err := platformcontrol.IssuePlatformComponentIdentity(keyring, claims, time.Now().UTC(), time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			selected := expected.Cohort == "edge-group-a"
			code := 404
			if selected {
				code = 200
			}
			assigned := performJSONRequest(t, server, http.MethodGet, "/v1/platform-state/consumers/assignment", token, nil)
			if assigned.Code != code {
				t.Fatal("assignment escaped cohort", expected.ConsumerID, assigned.Code, assigned.Body.String())
			}
			downloaded := performJSONRequest(t, server, http.MethodGet, "/v1/platform-state/consumers/artifacts/"+child.ID+"?expected_consumer_set_id="+set.ID, token, nil)
			if downloaded.Code != code {
				t.Fatal("download escaped cohort", expected.ConsumerID, downloaded.Code, downloaded.Body.String())
			}
			parent := performJSONRequest(t, server, http.MethodGet, "/v1/platform-state/consumers/artifacts/"+compiled.ReleaseArtifact.ID+"?expected_consumer_set_id="+set.ID, token, nil)
			if parent.Code != code {
				t.Fatal("parent download escaped cohort", expected.ConsumerID, parent.Code, parent.Body.String())
			}
			if selected {
				var reply consumerArtifactLookup
				mustDecodeJSON(t, parent, &reply)
				if reply.Artifact.ID != compiled.ReleaseArtifact.ID || reply.Assignment.ArtifactID != child.ID || reply.Release.ID != released.ID {
					t.Fatal("parent response lost child authorization")
				}
			}
			foreign := performJSONRequest(t, server, http.MethodGet, "/v1/platform-state/consumers/artifacts/"+compiled.ReleaseArtifact.ID+"?expected_consumer_set_id=foreign", token, nil)
			if foreign.Code != 404 {
				t.Fatal("parent accepted foreign expected set", foreign.Code)
			}
			if !selected {
				heartbeat := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: expected.ConsumerID, Component: expected.Component, NodeID: expected.NodeID, ArtifactKind: child.ArtifactKind, ScopeKey: "global", ExpectedConsumerSetID: set.ID, ReleaseSetID: compiled.ReleaseArtifact.ID, FencingToken: released.FencingToken, GenerationSequence: child.GenerationSequence}
				result := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/trusted-heartbeat", token, heartbeat)
				if result.Code != 409 {
					t.Fatal("unselected gray receipt admitted", result.Code, result.Body.String())
				}
			}
		}
		// An unselected group still receives its explicitly published shadow
		// channel. Gray selection must not hide independent assignments.
		_, shadow, _, _, err := state.ReleasePlatformArtifact(compiled.ReleaseArtifact.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", IdempotencyKey: "retained-shadow"}, model.Principal{ActorType: "test", ActorID: "test"})
		if err != nil {
			t.Fatal(err)
		}
		shadowSet, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: compiled.ReleaseArtifact.ID, ArtifactReleaseID: shadow.ID, ArtifactKind: child.ArtifactKind, ScopeKey: "global", Generation: child.Generation, Revision: int64(i + 10), Topology: topology})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = state.CreatePlatformExpectedConsumerSet(shadowSet); err != nil {
			t.Fatal(err)
		}
		unselected := set.Consumers[1]
		token, err := platformcontrol.IssuePlatformComponentIdentity(keyring, platformcontrol.PlatformComponentIdentityClaims{CredentialID: "reader", Component: unselected.Component, NodeID: unselected.NodeID, ScopeKey: "global", ArtifactKinds: []string{child.ArtifactKind}}, time.Now().UTC(), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		result := performJSONRequest(t, server, http.MethodGet, "/v1/platform-state/consumers/assignment", token, nil)
		var assignments model.PlatformConsumerAssignmentResponse
		mustDecodeJSON(t, result, &assignments)
		if result.Code != 200 || len(assignments.Assignments) != 1 || assignments.Assignments[0].ReleaseChannel != "shadow" {
			t.Fatal("gray selection removed unselected shadow channel", result.Code, result.Body.String())
		}
		stored, err := state.GetPlatformExpectedConsumerSet(set.ID)
		if err != nil || len(stored.Consumers) != 2 {
			t.Fatal("assignment changed immutable topology")
		}
	}
}
