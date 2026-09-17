package api

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestConsumerMembershipRetainsSilentEdgesAndBlocksPartialConvergence(t *testing.T) {
	state, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	now := time.Now().UTC()
	stale := now.Add(-24 * time.Hour)
	nodes := []model.EdgeNode{
		{ID: "edge-current", EdgeGroupID: "group-a", Healthy: true, Status: model.EdgeHealthHealthy, LastHeartbeatAt: &now, LastSeenAt: &now},
		{ID: "edge-silent", EdgeGroupID: "group-a", Healthy: false, Status: model.EdgeHealthUnknown, LastHeartbeatAt: &stale, LastSeenAt: &stale},
	}
	for _, node := range nodes {
		if _, _, err := state.CreateEdgeNodeToken(node); err != nil {
			t.Fatal(err)
		}
	}
	set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: "membership-release", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, Generation: "generation", ScopeKey: "global", Topology: platformcontrol.ExpectedConsumerTopology{EdgeNodes: nodes}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = state.CreatePlatformExpectedConsumerSet(set); err != nil {
		t.Fatal(err)
	}
	claims := platformcontrol.PlatformComponentIdentityClaims{Version: "v1", CredentialID: "credential", TokenID: "token", IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(time.Minute).Unix(), Component: "edge-worker", NodeID: "edge-current", ScopeKey: "global", ArtifactKinds: []string{set.ArtifactKind}}
	heartbeat, err := platformcontrol.BindPlatformConsumerHeartbeatToExpectedSet(claims, set, platformcontrol.PlatformConsumerHeartbeatEnvelope{Sequence: 1, GenerationSequence: 1, FencingToken: 1, IssuedAt: now, Nonce: strings.Repeat("a", 32), ProtocolVersion: "v1", SchemaVersion: "v1", ActualGeneration: "generation", LKGGeneration: "lkg", ApplyStatus: "applied", ProbeStatus: "passed"})
	if err != nil {
		t.Fatal(err)
	}
	heartbeat.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(heartbeat)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = state.AcceptTrustedPlatformConsumerHeartbeat(claims, set.ID, heartbeat, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
		t.Fatal(err)
	}
	response := performJSONRequest(t, server, http.MethodGet, "/v1/admin/platform-state/convergence?release_set_id="+set.ReleaseSetID, admin, nil)
	if response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	var body struct {
		Convergence []model.PlatformConsumerConvergenceStatus `json:"convergence"`
	}
	mustDecodeJSON(t, response, &body)
	if len(body.Convergence) != 1 {
		t.Fatal(body)
	}
	// This fixture has no real artifact/release record, so its observed
	// heartbeat remains unknown under the authoritative binding gate.
	status := body.Convergence[0]
	if status.RequiredExpected != 2 || status.RequiredObserved != 1 || status.RequiredPassing != 0 || status.Pass {
		t.Fatal("silent edge disappeared from required membership", status)
	}
	gate := server.validateReleaseSetConvergence(model.PlatformArtifact{ID: set.ReleaseSetID})
	if gate.Pass {
		t.Fatal("partial cohort passed full promotion", gate)
	}
	// The same membership source feeds initial prepare, independently of health.
	topology, err := server.platformConsumerTopology(context.Background(), model.Principal{})
	if err != nil || len(topology.EdgeNodes) != 2 {
		t.Fatal("prepare topology discarded silent edge", topology, err)
	}
	// Only authoritative removal, rather than silence, can remove an expectation.
	projected := platformcontrol.ProjectExpectedConsumerSetToTopology(set, platformcontrol.ExpectedConsumerTopology{EdgeNodes: activeEdgeNodesForPolicy(nodes, []model.ClusterNodePolicyStatus{{NodeName: "edge-current"}})})
	if projected.RequiredCardinality != 1 {
		t.Fatal("authoritative removal ignored", projected)
	}
	stored, err := state.GetPlatformExpectedConsumerSet(set.ID)
	if err != nil || stored.RequiredCardinality != 2 {
		t.Fatal("immutable expected set changed", err)
	}
}
