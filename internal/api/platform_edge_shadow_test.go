package api

import (
	"net/http"
	"reflect"
	"testing"
	"time"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
)

func TestEdgeShadowHTTPPreservesRegisteredLKGAndSelectsExactCandidate(t *testing.T) {
	for _, cell := range []string{"", "cell-test"} {
		t.Run("authority="+cell, func(t *testing.T) { testEdgeShadowHTTP(t, cell) })
	}
}

func testEdgeShadowHTTP(t *testing.T, cell string) {
	state, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	ring := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "identity", Keys: map[string]string{"identity": "synthetic-edge-shadow-key"}}
	server.auth.PlatformComponentIdentityKeyring = ring
	server.heartbeatAuditKeyring = trustedHeartbeatAuditTestKeyring()
	now := time.Now().UTC()
	scope, group := "global", "edge-group-a"
	if cell != "" {
		scope, group = platformconfig.AuthorityCellScope(cell), cell
	}
	if _, _, err := state.CreateEdgeNodeToken(model.EdgeNode{ID: "edge-shadow", EdgeGroupID: group, LastHeartbeatAt: &now, LastSeenAt: &now, Healthy: true, Status: model.EdgeHealthHealthy}); err != nil {
		t.Fatal(err)
	}
	request := platformConfigCompileRequest{Intent: platformconfig.PlatformIntent{Generation: "candidate-intent", Scope: scope, Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", UpstreamURL: "http://origin:8080", Enabled: true}}}, Policy: platformconfig.PolicySnapshot{Generation: "candidate-policy", Scope: scope}}
	if cell != "" {
		request.Intent.AuthorityCellID, request.Policy.AuthorityCellID = cell, cell
		request.Intent.PublicationRole, request.Policy.PublicationRole = platformconfig.PublicationRoleCellRoutes, platformconfig.PublicationRoleCellRoutes
		request.Intent.EdgeTopology = &edgetopology.Intent{SchemaVersion: edgetopology.SchemaVersion, Cells: []edgetopology.AuthorityCell{{ID: cell}}, Pools: []edgetopology.ServingPool{{ID: "pool-test"}}, Edges: []edgetopology.Edge{{ID: "edge-shadow", AuthorityCellID: cell, ServingPoolIDs: []string{"pool-test"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "edge-shadow"}}}}
		request.Policy.TLSReadiness = &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 10, ProbeTimeoutSeconds: 1, FactFreshnessSeconds: 60, MaxConcurrency: 2, MaxProbes: 10}
		topology, err := platformconfig.TrafficConsumerTopologyFromIntent(request.Intent)
		if err != nil {
			t.Fatal(err)
		}
		request.Policy.ConsumerTopologyDigest, _ = platformconfig.Digest(topology)
	}
	r := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, request)
	if r.Code != http.StatusCreated {
		t.Fatal(r.Code, r.Body.String())
	}
	var compiled platformConfigCompileResponse
	mustDecodeJSON(t, r, &compiled)
	r = performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+compiled.ReleaseArtifact.ID+"/release", admin, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", IdempotencyKey: "edge-shadow"})
	if r.Code != http.StatusOK {
		t.Fatal(r.Code, r.Body.String())
	}
	var published model.PlatformArtifactReleaseResponse
	mustDecodeJSON(t, r, &published)
	r = performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/release-set/prepare-consumers", admin, map[string]any{"release_set_id": compiled.ReleaseArtifact.ID, "artifact_release_id": published.Release.ID})
	if r.Code != http.StatusCreated {
		t.Fatal(r.Code, r.Body.String())
	}
	var body struct {
		Sets []model.PlatformExpectedConsumerSet `json:"expected_consumer_sets"`
	}
	mustDecodeJSON(t, r, &body)
	for _, set := range body.Sets {
		if set.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle {
			continue
		}
		t.Run(set.ArtifactKind, func(t *testing.T) {
			member := set.Consumers[0]
			_, err := state.UpsertPlatformConsumerHeartbeat(model.PlatformConsumerHeartbeatRequest{ConsumerID: member.ConsumerID, Component: member.Component, NodeID: member.NodeID, ArtifactKind: set.ArtifactKind, ScopeKey: set.ScopeKey, LKGGeneration: "registered-original-group", ActualGeneration: "registered-original-group", ApplyStatus: "failed", ProbeStatus: "failed"})
			if err != nil {
				t.Fatal(err)
			}
			before, err := state.ListPlatformConsumers(set.ArtifactKind, set.ScopeKey)
			if err != nil {
				t.Fatal(err)
			}
			child, err := server.consumerAssignmentChild(compiled.ReleaseArtifact, set.ArtifactKind)
			if err != nil {
				t.Fatal(err)
			}
			claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: "credential", Component: member.Component, NodeID: member.NodeID, AuthorityID: member.AuthorityID, ScopeKey: set.ScopeKey, ArtifactKinds: []string{set.ArtifactKind}}
			token, err := platformcontrol.IssuePlatformComponentIdentity(ring, claims, now, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			claims, err = platformcontrol.ParsePlatformComponentIdentity(ring, token, now)
			if err != nil {
				t.Fatal(err)
			}
			h, err := platformcontrol.BindPlatformConsumerHeartbeatToExpectedSet(claims, set, platformcontrol.PlatformConsumerHeartbeatEnvelope{Sequence: 1, GenerationSequence: child.GenerationSequence, FencingToken: published.Release.FencingToken, IssuedAt: now, Nonce: model.NewID("nonce"), ProtocolVersion: "v1", SchemaVersion: "v1", CandidateGeneration: set.ExpectedGeneration, ApplyStatus: "staged", ProbeStatus: "shadow_validated"})
			if err != nil {
				t.Fatal(err)
			}
			h.CompatibilityCapabilities = []string{platformcontrol.TrafficReleaseCapabilityV1}
			h.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
			if err != nil {
				t.Fatal(err)
			}
			accepted := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/shadow-heartbeat", token, h)
			if accepted.Code != http.StatusOK {
				t.Fatal(accepted.Code, accepted.Body.String())
			}
			current, err := state.ListPlatformConsumers(set.ArtifactKind, set.ScopeKey)
			if err != nil || !reflect.DeepEqual(before, current) {
				t.Fatal("candidate replaced original serving/LKG registration", err)
			}
			selected, err := server.platformConsumersForConvergence(set)
			if err != nil || len(selected) != 1 || selected[0].ExpectedConsumerSetID != set.ID || selected[0].ActualGeneration != "" || selected[0].LKGGeneration != "" {
				t.Fatal("exact shadow selection differs", selected, err)
			}
			foreign := set
			foreign.ID += "-another-expectation"
			selected, err = server.platformConsumersForConvergence(foreign)
			if err != nil || !reflect.DeepEqual(selected, before) {
				t.Fatal("shadow leaked into another expectation", selected, err)
			}
			replay := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/shadow-heartbeat", token, h)
			if replay.Code == http.StatusOK {
				t.Fatal("replayed shadow advanced cursor")
			}
		})
	}
}
