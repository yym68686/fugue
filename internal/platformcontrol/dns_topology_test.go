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
