package platformconfig

import (
	"strings"
	"testing"
	"time"
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

func TestCompileFixedRuntimeSnapshotIsStableAcrossWallClockChanges(t *testing.T) {
	request := CompileRequest{
		Intent:          PlatformIntent{Generation: "intent-fixed-snapshot"},
		Policy:          PolicySnapshot{Generation: "policy-fixed-snapshot"},
		RuntimeSnapshot: RuntimeSnapshot{IntentGeneration: "intent-fixed-snapshot", PolicyGeneration: "policy-fixed-snapshot", Facts: map[string]any{"healthy_edges": 2}},
		CreatedAt:       time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	first, err := Compile(request)
	if err != nil {
		t.Fatalf("compile first: %v", err)
	}
	request.CreatedAt = request.CreatedAt.Add(24 * time.Hour)
	second, err := Compile(request)
	if err != nil {
		t.Fatalf("compile second: %v", err)
	}
	firstDigest, err := Digest(first.RouteArtifact.Content)
	if err != nil {
		t.Fatalf("first digest: %v", err)
	}
	secondDigest, err := Digest(second.RouteArtifact.Content)
	if err != nil {
		t.Fatalf("second digest: %v", err)
	}
	if firstDigest != secondDigest {
		t.Fatalf("fixed runtime snapshot produced different artifact digest: %s != %s", firstDigest, secondDigest)
	}
}

func TestImportEnvironmentBuildsAuditableIntent(t *testing.T) {
	result, err := ImportEnvironment(map[string]string{
		"FUGUE_PLATFORM_ROUTES_JSON":    `[{"hostname":"App.Example.","upstream_url":"http://app:8080","enabled":true}]`,
		"FUGUE_DNS_STATIC_RECORDS_JSON": `[{"name":"App.Example.","type":"A","values":["203.0.113.10"],"ttl":60}]`,
		"FUGUE_BUNDLE_SIGNING_KEY":      "must-not-be-imported",
	}, "env-import-1")
	if err != nil {
		t.Fatalf("import environment: %v", err)
	}
	if result.SourceDigest == "" || len(result.ImportedKeys) != 2 || len(result.Intent.Routes) != 1 || len(result.Intent.DNS) != 1 {
		t.Fatalf("unexpected import result: %+v", result)
	}
	if result.Intent.Routes[0].Hostname != "app.example" || result.Intent.DNS[0].Type != "A" {
		t.Fatalf("environment values were not normalized: %+v", result.Intent)
	}
}

func TestImportEnvironmentAcceptsLegacyEnvelopesAndPreservesRouteState(t *testing.T) {
	result, err := ImportEnvironment(map[string]string{
		"FUGUE_PLATFORM_ROUTES_JSON":    `{"routes":[{"hostname":"Active.Example.","upstream_url":"http://active:8080"},{"hostname":"Disabled.Example.","upstream_url":"http://disabled:8080","status":"disabled","edge_group_id":"edge-group-country-us"}]}`,
		"FUGUE_DNS_STATIC_RECORDS_JSON": `{"records":[{"name":"Active.Example.","type":"A","values":["203.0.113.10"],"ttl":60}]}`,
	}, "env-envelope-1")
	if err != nil {
		t.Fatalf("import legacy envelopes: %v", err)
	}
	if len(result.Intent.Routes) != 2 || len(result.Intent.DNS) != 1 {
		t.Fatalf("legacy envelope records were lost: %+v", result.Intent)
	}
	if !result.Intent.Routes[0].Enabled || result.Intent.Routes[1].Enabled || result.Intent.Routes[1].EdgeGroupID != "edge-group-country-us" {
		t.Fatalf("legacy route state was not preserved: %+v", result.Intent.Routes)
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
