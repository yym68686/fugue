package platformcontrol

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDNSConsumerTopologyUsesPhysicalProcessWithoutInventingHealth(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	nodes := []model.DNSNode{
		{ID: "node-a", EdgeGroupID: "edge-group-a", Zone: "one.example", Healthy: false},
		{ID: "zone-two", PhysicalNodeID: "node-a", EdgeGroupID: "edge-group-a", Zone: "two.example", Healthy: true},
		{ID: "zone-three", PhysicalNodeID: "node-a", EdgeGroupID: "edge-group-a", Zone: "three.example", Healthy: true},
		{ID: "node-b", PhysicalNodeID: "node-b", EdgeGroupID: "edge-group-b", Zone: "one.example"},
	}
	req := ExpectedConsumerSetBuildRequest{ReleaseSetID: "release", ArtifactReleaseID: "release-shadow", ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ScopeKey: "global", Generation: "dns-generation", PreparedAt: now, Topology: ExpectedConsumerTopology{DNSNodes: nodes}}
	set := mustBuildExpectedConsumerSet(t, req)
	if len(set.Consumers) != 2 || set.RequiredCardinality != 2 || set.Consumers[0].ConsumerID != "dns-server:node-a" || set.Consumers[1].ConsumerID != "dns-server:node-b" {
		t.Fatal("zone telemetry became independent consumers", set)
	}
	if status := EvaluateConsumerConvergence(set, nil, now); status.Pass || status.RequiredExpected != 2 {
		t.Fatal("physical unhealthy process lost its required evidence", status)
	}
	req.Topology.DNSNodes = []model.DNSNode{nodes[3], nodes[2], nodes[1], nodes[0]}
	reversed := mustBuildExpectedConsumerSet(t, req)
	if !reflect.DeepEqual(set, reversed) {
		t.Fatal("DNS zone ordering changed expected identity")
	}
	legacy := set
	legacy.Consumers = append([]model.PlatformExpectedConsumer(nil), set.Consumers...)
	for _, id := range []string{"zone-two", "zone-three", "retired-node"} {
		row := set.Consumers[0]
		row.NodeID, row.ConsumerID = id, "dns-server:"+id
		legacy.Consumers = append(legacy.Consumers, row)
	}
	legacy.RequiredCardinality = 5
	before, _ := json.Marshal(legacy)
	projected := ProjectExpectedConsumerSetToTopology(legacy, req.Topology)
	after, _ := json.Marshal(legacy)
	if string(before) != string(after) || projected.ID != legacy.ID || projected.TopologyRevision != legacy.TopologyRevision || projected.RequiredCardinality != 2 {
		t.Fatal("live projection changed immutable lineage or failed to attribute aliases")
	}
	binding := &ConsumerReleaseBinding{ReleaseSetID: set.ReleaseSetID, ArtifactReleaseID: set.ArtifactReleaseID, ArtifactKind: set.ArtifactKind, ScopeKey: set.ScopeKey, Generation: set.ExpectedGeneration, FencingToken: 4, GenerationSequence: 7}
	observed := []model.PlatformConsumerInstance{boundPassingConsumer(set, set.Consumers[0], now), boundPassingConsumer(set, set.Consumers[1], now)}
	if status := EvaluateConsumerConvergence(projected, observed, now, binding); !status.Pass || status.RequiredPassing != 2 {
		t.Fatal("physical process evidence did not satisfy owned zones", status)
	}
	observed[0].ConsumerID, observed[0].NodeID = "dns-server:zone-two", "zone-two"
	if status := EvaluateConsumerConvergence(projected, observed, now, binding); status.Pass {
		t.Fatal("a zone alias fabricated physical process health")
	}
	observed[0] = boundPassingConsumer(set, set.Consumers[0], now.Add(-5*time.Minute))
	if status := EvaluateConsumerConvergence(projected, observed, now, binding); status.Pass {
		t.Fatal("stale physical process heartbeat accepted")
	}
	empty := ProjectExpectedConsumerSetToTopology(legacy, ExpectedConsumerTopology{})
	if status := EvaluateConsumerConvergence(empty, nil, now); status.Pass || status.State != model.InvariantEvidenceStateUnknown || !empty.RequiresConsumers {
		t.Fatal("empty DNS topology became convergence", status)
	}
	if status := EvaluateConsumerConvergence(empty, observed, now); status.Pass {
		t.Fatal("unexpected old observations revived empty DNS topology", status)
	}
	// A still-declared zone with no parent telemetry continues requiring that
	// physical process; absence of its base row is not decommission evidence.
	req.Topology.DNSNodes = []model.DNSNode{nodes[1]}
	orphan := mustBuildExpectedConsumerSet(t, req)
	if len(orphan.Consumers) != 1 || orphan.Consumers[0].NodeID != "node-a" || !orphan.Consumers[0].Required {
		t.Fatal("missing parent telemetry dropped required process")
	}
}

func TestDNSConsumerTopologyRejectsAmbiguousOwnership(t *testing.T) {
	for _, nodes := range [][]model.DNSNode{
		{{ID: ""}},
		{{ID: "zone", PhysicalNodeID: "a"}, {ID: "zone", PhysicalNodeID: "b"}},
		{{ID: "a", EdgeGroupID: "group-a"}, {ID: "zone", PhysicalNodeID: "a", EdgeGroupID: "group-b"}},
		{{ID: "a", PhysicalNodeID: "b"}, {ID: "zone", PhysicalNodeID: "a"}},
	} {
		req := ExpectedConsumerSetBuildRequest{ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, Generation: "dns-generation", Topology: ExpectedConsumerTopology{DNSNodes: nodes}}
		if _, err := BuildExpectedConsumerSet(req); err == nil {
			t.Fatal("ambiguous physical DNS identity accepted")
		}
		projected := ProjectExpectedConsumerSetToTopology(model.PlatformExpectedConsumerSet{ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, RequiresConsumers: true}, req.Topology)
		if status := EvaluateConsumerConvergence(projected, nil, time.Now()); status.Pass || status.State != model.InvariantEvidenceStateUnknown {
			t.Fatal("ambiguous topology was reported as verified", status)
		}
		// An unrelated DNS inventory fault must not block the independent
		// Edge route expectation. Only artifacts with DNS consumers use it.
		req.ArtifactKind = model.PlatformArtifactKindEdgeRouteBundle
		req.Topology.EdgeNodes = []model.EdgeNode{{ID: "edge-a"}}
		edgeSet := mustBuildExpectedConsumerSet(t, req)
		if len(ProjectExpectedConsumerSetToTopology(edgeSet, req.Topology).Consumers) != 1 {
			t.Fatal("DNS metadata fault invalidated independent Edge topology")
		}
	}
}

func TestDNSAuthoritiesSharePhysicalNodeWithoutSharingReceipts(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	var nodes []model.DNSNode
	for _, group := range []string{"edge-group-old", "cell-a", "cell-b"} {
		nodes = append(nodes, model.DNSNode{ID: "node-a", EdgeGroupID: group}, model.DNSNode{ID: "zone-alias", PhysicalNodeID: "node-a", EdgeGroupID: group})
	}
	req := ExpectedConsumerSetBuildRequest{ReleaseSetID: "parent", ArtifactReleaseID: "shadow", ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ScopeKey: "global", Generation: "generation", PreparedAt: now, Topology: ExpectedConsumerTopology{DNSNodes: nodes}}
	set := mustBuildExpectedConsumerSet(t, req)
	if len(set.Consumers) != 3 || set.RequiredCardinality != 3 {
		t.Fatal("authorities or aliases changed physical process cardinality", set)
	}
	before, _ := json.Marshal(set)
	for _, authority := range []string{"", "cell-a", "cell-b"} {
		claims := platformComponentTestClaims()
		claims.Component, claims.NodeID, claims.AuthorityID = model.PlatformConsumerComponentDNSServer, "node-a", authority
		claims.CredentialID, claims.ArtifactKinds = "kubernetes:test-system:dns-account:pod-a", []string{set.ArtifactKind}
		token, err := IssuePlatformComponentIdentity(platformComponentTestKeyring(), claims, now, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		claims, err = ParsePlatformComponentIdentity(platformComponentTestKeyring(), token, now)
		if err != nil || claims.AuthorityID != authority {
			t.Fatal(claims, err)
		}
		h, err := BindPlatformConsumerHeartbeatToExpectedSet(claims, set, PlatformConsumerHeartbeatEnvelope{})
		if err != nil || h.ConsumerID != claims.ConsumerID() || h.NodeID != "node-a" {
			t.Fatal(h, err)
		}
		for _, expected := range set.Consumers {
			if expected.ConsumerID == claims.ConsumerID() {
				if !ExpectedConsumerIdentityMatches(expected, claims) || expected.NodeID != "node-a" || (authority != "" && expected.FailureDomain != "node:node-a") {
					t.Fatal("physical ownership changed", expected)
				}
				continue
			}
			if ExpectedConsumerIdentityMatches(expected, claims) {
				t.Fatal("identity crossed authority")
			}
			if _, err := BindPlatformConsumerHeartbeatToExpectedSet(claims, set, PlatformConsumerHeartbeatEnvelope{ConsumerID: expected.ConsumerID}); err == nil {
				t.Fatal("foreign authority receipt accepted")
			}
		}
	}
	projected := ProjectExpectedConsumerSetToTopology(set, ExpectedConsumerTopology{DNSNodes: nodes[2:4]})
	if projected.RequiredCardinality != 1 || len(projected.Consumers) != 1 || projected.Consumers[0].ConsumerID != "dns-server:cell-a:node-a" {
		t.Fatal("projection moved an authority", projected)
	}
	binding := &ConsumerReleaseBinding{ReleaseSetID: set.ReleaseSetID, ArtifactReleaseID: set.ArtifactReleaseID, ArtifactKind: set.ArtifactKind, ScopeKey: set.ScopeKey, Generation: set.ExpectedGeneration, FencingToken: 4, GenerationSequence: 7}
	for _, expected := range set.Consumers {
		status := EvaluateConsumerConvergence(projected, []model.PlatformConsumerInstance{boundPassingConsumer(set, expected, now)}, now, binding)
		if expected.AuthorityID == "cell-a" {
			if !status.Pass || status.RequiredPassing != 1 {
				t.Fatal(status)
			}
		} else if status.Pass || status.RequiredObserved != 0 {
			t.Fatal("other authority supplied current health", status)
		}
	}
	after, _ := json.Marshal(set)
	if string(before) != string(after) {
		t.Fatal("projection mutated immutable set")
	}
	for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
	if reversed := mustBuildExpectedConsumerSet(t, req); !reflect.DeepEqual(set, reversed) {
		t.Fatal("input order changed authority identity")
	}
}

func TestDNSAuthorityAliasesCannotMoveBetweenCells(t *testing.T) {
	topology := ExpectedConsumerTopology{DNSNodes: []model.DNSNode{
		{ID: "shared-zone-alias", PhysicalNodeID: "node-a", EdgeGroupID: "cell-a"},
		{ID: "shared-zone-alias", PhysicalNodeID: "node-b", EdgeGroupID: "cell-b"},
	}}
	req := ExpectedConsumerSetBuildRequest{ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, Generation: "generation", PreparedAt: time.Now().UTC(), Topology: topology}
	set := mustBuildExpectedConsumerSet(t, req)
	for i := range set.Consumers {
		set.Consumers[i].NodeID = "shared-zone-alias"
		set.Consumers[i].ConsumerID, _ = PlatformConsumerID(model.PlatformConsumerComponentDNSServer, "shared-zone-alias", set.Consumers[i].AuthorityID)
	}
	before, _ := json.Marshal(set)
	projected := ProjectExpectedConsumerSetToTopology(set, topology)
	if len(projected.Consumers) != 2 || projected.Consumers[0].ConsumerID != "dns-server:cell-a:node-a" || projected.Consumers[1].ConsumerID != "dns-server:cell-b:node-b" {
		t.Fatal("alias crossed cell boundary", projected)
	}
	after, _ := json.Marshal(set)
	if string(before) != string(after) {
		t.Fatal("alias projection changed immutable set")
	}
	set.Consumers[0].ConsumerID = "dns-server:cell-b:shared-zone-alias"
	projected = ProjectExpectedConsumerSetToTopology(set, topology)
	if len(projected.Consumers) != 1 || projected.Consumers[0].AuthorityID != "cell-b" {
		t.Fatal("alias projection repaired a forged authority identity", projected)
	}
	for _, nodes := range [][]model.DNSNode{
		{{ID: "node-a", EdgeGroupID: "cell-A"}},
		{{ID: "other:node", EdgeGroupID: "cell-a"}},
		{{ID: "alias", PhysicalNodeID: "node-a", EdgeGroupID: "cell-a"}, {ID: "alias", PhysicalNodeID: "node-b", EdgeGroupID: "cell-a"}},
	} {
		req.Topology.DNSNodes = nodes
		if _, err := BuildExpectedConsumerSet(req); err == nil {
			t.Fatal("invalid scoped ownership accepted", nodes)
		}
	}
}

func TestScopedDNSIdentityRequiresPodCredential(t *testing.T) {
	claims := platformComponentTestClaims()
	claims.Component, claims.NodeID, claims.AuthorityID = model.PlatformConsumerComponentDNSServer, "node-a", "cell-a"
	claims.ArtifactKinds = []string{model.PlatformArtifactKindDNSAnswerBundle}
	for _, credential := range []string{"shared-secret", "kubernetes:namespace:account", "kubernetes::account:pod", "kubernetes:namespace::pod", "kubernetes:namespace:account:", "kubernetes:namespace:account:pod:other"} {
		claims.CredentialID = credential
		if _, err := IssuePlatformComponentIdentity(platformComponentTestKeyring(), claims, time.Now(), time.Minute); err == nil {
			t.Fatal("non-Pod-bound scoped DNS identity issued", credential)
		}
	}
}
