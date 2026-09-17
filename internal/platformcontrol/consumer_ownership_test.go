package platformcontrol

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestTLSConsumerOwnerAssignmentBindingAndConvergence(t *testing.T) {
	now := time.Now().UTC()
	topology := ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{ID: "node-a", EdgeGroupID: "group-a"}}}
	set, err := BuildExpectedConsumerSet(ExpectedConsumerSetBuildRequest{ReleaseSetID: "release-set", ArtifactReleaseID: "release", ArtifactKind: model.PlatformArtifactKindCaddyRouteConfig, ScopeKey: "global", Generation: "tls-generation", Topology: topology})
	if err != nil || len(set.Consumers) != 1 || set.Consumers[0].Component != model.PlatformConsumerComponentEdgeWorker {
		t.Fatal(set, err)
	}
	set.Consumers[0].Component, set.Consumers[0].ConsumerID = model.PlatformConsumerComponentCaddyEdgeFront, "caddy-edge-front:node-a"
	before, _ := json.Marshal(set)
	claims := PlatformComponentIdentityClaims{Version: "v1", CredentialID: "credential", TokenID: "token", IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(time.Minute).Unix(), Component: "edge-worker", NodeID: "node-a", ScopeKey: "global", ArtifactKinds: []string{set.ArtifactKind}}
	h, err := BindPlatformConsumerHeartbeatToExpectedSet(claims, set, PlatformConsumerHeartbeatEnvelope{})
	if err != nil || h.ConsumerID != "edge-worker:node-a" || h.ExpectedConsumerSetID != set.ID {
		t.Fatal(h, err)
	}
	for _, mutate := range []func(*PlatformComponentIdentityClaims){
		func(c *PlatformComponentIdentityClaims) { c.Component = "caddy-edge-front" },
		func(c *PlatformComponentIdentityClaims) { c.NodeID = "node-b" },
		func(c *PlatformComponentIdentityClaims) { c.ScopeKey = "other" },
		func(c *PlatformComponentIdentityClaims) {
			c.ArtifactKinds = []string{model.PlatformArtifactKindEdgeRouteBundle}
		},
	} {
		bad := claims
		mutate(&bad)
		if _, err := BindPlatformConsumerHeartbeatToExpectedSet(bad, set, PlatformConsumerHeartbeatEnvelope{}); err == nil {
			t.Fatal("foreign owner accepted", bad)
		}
	}
	projected := ProjectExpectedConsumerSetToTopology(set, topology)
	if projected.ID != set.ID || projected.RequiredCardinality != 1 || projected.Consumers[0].ConsumerID != "edge-worker:node-a" {
		t.Fatal(projected)
	}
	status := EvaluateConsumerConvergence(projected, []model.PlatformConsumerInstance{{ConsumerID: "caddy-edge-front:node-a", Component: "caddy-edge-front", NodeID: "node-a", ArtifactKind: set.ArtifactKind, ScopeKey: set.ScopeKey, ExpectedConsumerSetID: set.ID, IdentityVerified: true, ActualGeneration: set.ExpectedGeneration, ApplyStatus: "applied", ProbeStatus: "passed"}}, now)
	if status.Pass || status.RequiredPassing != 0 {
		t.Fatal("old Front evidence passed", status)
	}
	// CLI callers evaluate the immutable set without a live topology read.
	direct := EvaluateConsumerConvergence(set, nil, now)
	if len(direct.Assessments) != 1 || direct.Assessments[0].ConsumerID != "edge-worker:node-a" {
		t.Fatal("direct convergence skipped the owner projection", direct)
	}
	after, _ := json.Marshal(set)
	if string(before) != string(after) {
		t.Fatal("immutable TLS expectations changed")
	}
}
