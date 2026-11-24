package platformcontrol

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestWorkerReplacementEnrollmentUsesOnlyFreshExactIdentity(t *testing.T) {
	now := time.Now().UTC()
	legacy := platformComponentTestClaims()
	legacy.NodeID, legacy.ScopeKey, legacy.CredentialID = "node-a", "global", "kubernetes:test:worker:pod-uid"
	legacy.ArtifactKinds = []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig}
	replacement := legacy
	replacement.WorkerEnrollmentID = strings.Repeat("a", 32)
	replacement.WorkerFrontServiceUID = "11111111-1111-1111-1111-111111111111"
	token, err := IssuePlatformComponentIdentity(platformComponentTestKeyring(), replacement, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err = ParsePlatformComponentIdentity(platformComponentTestKeyring(), token, now)
	if err != nil || replacement.ConsumerID() == legacy.ConsumerID() || replacement.WorkerFrontServiceUID != "11111111-1111-1111-1111-111111111111" {
		t.Fatal("replacement acquired legacy identity", err)
	}
	for _, kind := range replacement.ArtifactKinds {
		topology := ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{ID: replacement.NodeID, EdgeGroupID: "edge-group-a"}}, WorkerEnrollments: map[string]string{replacement.NodeID: replacement.WorkerEnrollmentID}}
		set, err := BuildExpectedConsumerSet(ExpectedConsumerSetBuildRequest{ReleaseSetID: "release-set", ArtifactKind: kind, Generation: "generation", ScopeKey: "global", Topology: topology, PreparedAt: now})
		if err != nil || len(set.Consumers) != 1 || set.Consumers[0].ConsumerID != replacement.ConsumerID() || set.Consumers[0].NodeID != replacement.NodeID {
			t.Fatal("physical and logical membership were not kept separate", set, err)
		}
		if _, err := BindPlatformConsumerHeartbeatToExpectedSet(replacement, set, PlatformConsumerHeartbeatEnvelope{}); err != nil {
			t.Fatal("replacement could not report its own expectation", err)
		}
		if _, err := BindPlatformConsumerHeartbeatToExpectedSet(legacy, set, PlatformConsumerHeartbeatEnvelope{}); err == nil {
			t.Fatal("legacy credential inherited the new membership")
		}
		if _, err := BindPlatformConsumerHeartbeatToExpectedSet(replacement, set, PlatformConsumerHeartbeatEnvelope{ConsumerID: legacy.ConsumerID()}); err == nil {
			t.Fatal("replacement could report as the legacy identity")
		}
		fact := model.PlatformConsumerInstance{ConsumerID: legacy.ConsumerID(), Component: legacy.Component, NodeID: legacy.NodeID, IdentityVerified: true, ArtifactKind: kind, ScopeKey: "global"}
		if out := EvaluateConsumerConvergence(set, []model.PlatformConsumerInstance{fact}, now); out.Pass || out.RequiredObserved != 0 {
			t.Fatal("replacement reused old evidence", out)
		}
		if projected := ProjectExpectedConsumerSetToTopology(set, topology); projected.RequiredCardinality != 1 {
			t.Fatal("enrollment lost when evaluating convergence", projected)
		}
	}
	for _, invalid := range []PlatformComponentIdentityClaims{
		{Component: replacement.Component, NodeID: replacement.NodeID, ScopeKey: "global", WorkerEnrollmentID: replacement.WorkerEnrollmentID, CredentialID: "unbound", ArtifactKinds: replacement.ArtifactKinds},
		{Component: replacement.Component, NodeID: replacement.NodeID, ScopeKey: "global", AuthorityID: "cell-a", WorkerEnrollmentID: replacement.WorkerEnrollmentID, CredentialID: replacement.CredentialID, ArtifactKinds: replacement.ArtifactKinds},
		{Component: replacement.Component, NodeID: replacement.NodeID, ScopeKey: "global", WorkerFrontServiceUID: replacement.WorkerFrontServiceUID, CredentialID: replacement.CredentialID, ArtifactKinds: replacement.ArtifactKinds},
		{Component: replacement.Component, NodeID: replacement.NodeID, ScopeKey: "global", WorkerEnrollmentID: replacement.WorkerEnrollmentID, WorkerFrontServiceUID: "not-a-service-uid", CredentialID: replacement.CredentialID, ArtifactKinds: replacement.ArtifactKinds},
	} {
		if _, err := IssuePlatformComponentIdentity(platformComponentTestKeyring(), invalid, now, time.Minute); err == nil {
			t.Fatal("replacement identity accepted a foreign scope or unbound credential")
		}
	}
}

func TestAuthorityConsumersKeepStableNodeAndSeparateExpectedOwnership(t *testing.T) {
	now := time.Now().UTC()
	topology := ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{
		{ID: "node-a", EdgeGroupID: "edge-group-old"},
		{ID: "node-a", EdgeGroupID: "cell-a"},
		{ID: "node-a", EdgeGroupID: "cell-b"},
	}}
	for _, kind := range []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig} {
		t.Run(kind, func(t *testing.T) {
			set, err := BuildExpectedConsumerSet(ExpectedConsumerSetBuildRequest{ReleaseSetID: "release-set", ArtifactKind: kind, Generation: "generation", ScopeKey: "global", PreparedAt: now, Topology: topology})
			if err != nil || len(set.Consumers) != 3 || set.RequiredCardinality != 3 {
				t.Fatal("overlapping stable node was deduplicated", set, err)
			}
			before, _ := json.Marshal(set)
			for _, authority := range []string{"", "cell-a", "cell-b"} {
				claims := platformComponentTestClaims()
				claims.NodeID, claims.AuthorityID, claims.ArtifactKinds = "node-a", authority, []string{kind}
				token, err := IssuePlatformComponentIdentity(platformComponentTestKeyring(), claims, now, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				claims, err = ParsePlatformComponentIdentity(platformComponentTestKeyring(), token, now)
				if err != nil || claims.AuthorityID != authority {
					t.Fatal(claims, err)
				}
				h, err := BindPlatformConsumerHeartbeatToExpectedSet(claims, set, PlatformConsumerHeartbeatEnvelope{})
				if err != nil || h.NodeID != "node-a" || h.ConsumerID != claims.ConsumerID() {
					t.Fatal(h, err)
				}
				for _, other := range []string{"edge-worker:node-a", "edge-worker:cell-a:node-a", "edge-worker:cell-b:node-a"} {
					if other == h.ConsumerID {
						continue
					}
					if _, err := BindPlatformConsumerHeartbeatToExpectedSet(claims, set, PlatformConsumerHeartbeatEnvelope{ConsumerID: other}); err == nil {
						t.Fatal("credential crossed authority", authority, other)
					}
				}
			}
			// Only the exact remaining authority survives projection. Historical
			// expectations remain immutable; a new cell receives no old facts.
			projected := ProjectExpectedConsumerSetToTopology(set, ExpectedConsumerTopology{EdgeNodes: topology.EdgeNodes[1:2]})
			if projected.RequiredCardinality != 1 || projected.Consumers[0].ConsumerID != "edge-worker:cell-a:node-a" || projected.Consumers[0].AuthorityID != "cell-a" {
				t.Fatal(projected)
			}
			legacy := model.PlatformConsumerInstance{ConsumerID: "edge-worker:node-a", Component: "edge-worker", NodeID: "node-a", ArtifactKind: kind, ScopeKey: "global", IdentityVerified: true}
			convergence := EvaluateConsumerConvergence(projected, []model.PlatformConsumerInstance{legacy}, now)
			if convergence.RequiredObserved != 0 || convergence.Pass {
				t.Fatal("legacy evidence became neutral evidence", convergence)
			}
			after, _ := json.Marshal(set)
			if string(before) != string(after) {
				t.Fatal("projection mutated immutable set")
			}
		})
	}
}

func TestScopedConsumerRejectsInvalidOrUnboundAuthority(t *testing.T) {
	now := time.Now().UTC()
	for _, authority := range []string{"cell-", "cell-A", " cell-a", "cell-a/other", "cell-a:other", "edge-group-a"} {
		claims := platformComponentTestClaims()
		claims.AuthorityID = authority
		if _, err := IssuePlatformComponentIdentity(platformComponentTestKeyring(), claims, now, time.Minute); err == nil {
			t.Fatal("invalid authority accepted", authority)
		}
	}
	claims := platformComponentTestClaims()
	claims.AuthorityID, claims.Component = "cell-a", model.PlatformConsumerComponentNodeUpdater
	if _, err := IssuePlatformComponentIdentity(platformComponentTestKeyring(), claims, now, time.Minute); err == nil {
		t.Fatal("unrelated node component acquired cell authority")
	}
	claims.Component, claims.NodeID = model.PlatformConsumerComponentEdgeWorker, "other:node"
	if _, err := IssuePlatformComponentIdentity(platformComponentTestKeyring(), claims, now, time.Minute); err == nil {
		t.Fatal("ambiguous consumer ID accepted")
	}
	legacy := PlatformComponentIdentityClaims{Component: "edge-worker", NodeID: "node-a"}
	if ExpectedConsumerIdentityMatches(model.PlatformExpectedConsumer{ConsumerID: "edge-worker:node-a", Component: "edge-worker", NodeID: "node-a", Cohort: "cell-a"}, legacy) {
		t.Fatal("legacy representation authorized a neutral cohort")
	}
	dns := PlatformComponentIdentityClaims{Component: "dns-server", NodeID: "node-a"}
	if ExpectedConsumerIdentityMatches(model.PlatformExpectedConsumer{ConsumerID: "dns-server:node-a", Component: "dns-server", NodeID: "node-a", Cohort: "cell-a"}, dns) {
		t.Fatal("legacy DNS identity acquired a neutral receipt")
	}
}

func TestScopedTLSOwnerProjectionPreservesAuthority(t *testing.T) {
	set := model.PlatformExpectedConsumerSet{ArtifactKind: model.PlatformArtifactKindCaddyRouteConfig, ScopeKey: "global", Consumers: []model.PlatformExpectedConsumer{{Component: "caddy-edge-front", NodeID: "node-a", AuthorityID: "cell-a", ConsumerID: "caddy-edge-front:cell-a:node-a", Cohort: "cell-a", ArtifactKind: model.PlatformArtifactKindCaddyRouteConfig, ScopeKey: "global"}}}
	projected := ProjectExpectedConsumerOwners(set)
	if projected.Consumers[0].ConsumerID != "edge-worker:cell-a:node-a" || projected.Consumers[0].AuthorityID != "cell-a" {
		t.Fatal(projected)
	}
	set.Consumers[0].Cohort = "cell-b"
	if ProjectExpectedConsumerOwners(set).Consumers[0].Component != "caddy-edge-front" {
		t.Fatal("foreign authority owner adopted")
	}
}
