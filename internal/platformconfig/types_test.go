package platformconfig

import (
	"strings"
	"testing"
)

func TestCompileIsDeterministicAndCarriesLineage(t *testing.T) {
	request := CompileRequest{
		Intent: PlatformIntent{
			Generation: "intent-0001",
			Routes:     []RouteIntent{{Hostname: "b.example", UpstreamURL: "http://b", Enabled: true}, {Hostname: "a.example", UpstreamURL: "http://a", Enabled: true}},
		},
		Policy: PolicySnapshot{
			Generation:          "policy-0001",
			MinimumHealthyEdges: 2,
			DependencyOrder:     []string{"route", "tls", "dns"},
		},
	}
	first, err := Compile(request)
	if err != nil {
		t.Fatalf("compile first: %v", err)
	}
	second, err := Compile(request)
	if err != nil {
		t.Fatalf("compile second: %v", err)
	}
	firstDigest, err := Digest(first.RouteArtifact.Content)
	if err != nil {
		t.Fatalf("digest first: %v", err)
	}
	secondDigest, err := Digest(second.RouteArtifact.Content)
	if err != nil {
		t.Fatalf("digest second: %v", err)
	}
	if firstDigest != secondDigest {
		t.Fatalf("route compiler is not deterministic: %s != %s", firstDigest, secondDigest)
	}
	if first.Lineage.IntentDigest == "" || first.Lineage.PolicyDigest == "" || first.Lineage.CompilerVersion == "" {
		t.Fatalf("lineage is incomplete: %+v", first.Lineage)
	}
	if first.Lineage.IntentGeneration != request.Intent.Generation || first.Lineage.PolicyGeneration != request.Policy.Generation {
		t.Fatalf("lineage generation binding is incomplete: %+v", first.Lineage)
	}
	if first.RouteArtifact.Metadata["intent_digest"] != first.Lineage.IntentDigest {
		t.Fatalf("route artifact did not retain intent lineage: %+v", first.RouteArtifact.Metadata)
	}
	if len(first.ReleaseSet.Dependencies) != 2 || first.ReleaseSet.Dependencies[0].Relation != "requires" {
		t.Fatalf("release set dependency graph is incomplete: %+v", first.ReleaseSet.Dependencies)
	}
	if !strings.HasPrefix(first.ReleaseSet.Generation, "release-") {
		t.Fatalf("unexpected release set generation: %s", first.ReleaseSet.Generation)
	}
}

func TestCompileRejectsDuplicateRoutesAndInvalidDependencyOrder(t *testing.T) {
	_, err := Compile(CompileRequest{
		Intent: PlatformIntent{Generation: "intent-0001", Routes: []RouteIntent{{Hostname: "a", UpstreamURL: "http://a"}, {Hostname: "a", UpstreamURL: "http://b"}}},
		Policy: PolicySnapshot{Generation: "policy-0001"},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate route") {
		t.Fatalf("expected duplicate route rejection, got %v", err)
	}
	_, err = Compile(CompileRequest{
		Intent: PlatformIntent{Generation: "intent-0001"},
		Policy: PolicySnapshot{Generation: "policy-0001", DependencyOrder: []string{"route", "route"}},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate node") {
		t.Fatalf("expected dependency rejection, got %v", err)
	}
}

func TestCompileRejectsRuntimeSnapshotGenerationDrift(t *testing.T) {
	_, err := Compile(CompileRequest{
		Intent:          PlatformIntent{Generation: "intent-0001"},
		Policy:          PolicySnapshot{Generation: "policy-0001"},
		RuntimeSnapshot: RuntimeSnapshot{IntentGeneration: "intent-old", PolicyGeneration: "policy-0001", Facts: map[string]any{"edges": 2}},
	})
	if err == nil || !strings.Contains(err.Error(), "runtime snapshot generations") {
		t.Fatalf("expected runtime snapshot generation mismatch, got %v", err)
	}
}

func TestCompileRejectsConstraintGraphCyclesAndUnknownNodes(t *testing.T) {
	base := CompileRequest{
		Intent: PlatformIntent{Generation: "intent-graph-0001"},
		Policy: PolicySnapshot{
			Generation: "policy-graph-0001",
			ConstraintGraph: ConstraintGraph{
				Nodes: []string{"route", "tls", "dns"},
				Edges: []ConstraintEdge{{From: "route", To: "tls", Relation: "requires"}, {From: "tls", To: "dns", Relation: "before"}},
			},
		},
	}
	if _, err := Compile(base); err != nil {
		t.Fatalf("valid constraint graph rejected: %v", err)
	}
	base.Policy.ConstraintGraph.Edges = append(base.Policy.ConstraintGraph.Edges, ConstraintEdge{From: "dns", To: "route", Relation: "requires"})
	if _, err := Compile(base); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("expected graph cycle rejection, got %v", err)
	}
	base.Policy.ConstraintGraph.Edges = []ConstraintEdge{{From: "route", To: "unknown", Relation: "requires"}}
	if _, err := Compile(base); err == nil || !strings.Contains(err.Error(), "unknown node") {
		t.Fatalf("expected unknown node rejection, got %v", err)
	}
}
