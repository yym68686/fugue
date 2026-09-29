package edge

import (
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

func TestWorkerExplicitCellScopeChecksCompiledRoutesAndForeignArtifacts(t *testing.T) {
	scope := platformconfig.AuthorityCellScope("cell-a")
	now := time.Now().UTC()
	intent := platformconfig.PlatformIntent{AuthorityCellID: "cell-a", Scope: scope, Generation: "intent", EdgeTopology: &edgetopology.Intent{SchemaVersion: edgetopology.SchemaVersion, Cells: []edgetopology.AuthorityCell{{ID: "cell-a"}}, Pools: []edgetopology.ServingPool{{ID: "pool-a"}}, Edges: []edgetopology.Edge{{ID: "edge-a", AuthorityCellID: "cell-a", ServingPoolIDs: []string{"pool-a"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "edge-a"}}}}, DNSConsumers: []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "cell-a", Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60}}, Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", UpstreamURL: "http://origin:8080", Enabled: true}}}
	topology, err := platformconfig.TrafficConsumerTopologyFromIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := platformconfig.Digest(topology)
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: intent, Policy: platformconfig.PolicySnapshot{AuthorityCellID: "cell-a", ConsumerTopologyDigest: digest, Scope: scope, Generation: "policy"}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now, DNSConsumers: []platformconfig.DNSConsumerObservation{{NodeID: "dns-a", EdgeGroupID: "cell-a", ObservedAt: now, A: []string{"8.8.8.8"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	a := compiled.RouteArtifact
	a.ID, a.ScopeKey, a.Status, a.GenerationSequence = "routes", scope, model.PlatformArtifactStatusValidated, 1
	a.ContentHash, err = platformconfig.Digest(a.Content)
	if err != nil {
		t.Fatal(err)
	}
	a, err = platformsafety.SignPlatformArtifact(a, bundleauth.NewKeyring("scope-fixture-key", "key", "", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	assignment := model.PlatformConsumerAssignment{ExpectedConsumerSetID: "set", ReleaseSetID: "parent", ArtifactReleaseID: "release", ArtifactID: a.ID, ArtifactKind: a.ArtifactKind, ScopeKey: scope, ExpectedGeneration: a.Generation, ContentHash: a.ContentHash, GenerationSequence: 1, FencingToken: 1, ReleaseChannel: "shadow"}
	release := model.PlatformArtifactRelease{ID: "release", ArtifactID: "parent", ArtifactKind: model.PlatformArtifactKindReleaseSet, ScopeKey: scope, Generation: a.Metadata["release_set_generation"], ReleaseChannel: "shadow", Status: model.PlatformArtifactReleaseStatusActive, FencingToken: 1}
	s := NewService(config.EdgeConfig{EdgeID: "edge-a", EdgeGroupID: "cell-a", PlatformScopeKey: scope, BundleSigningKey: "scope-fixture-key", BundleSigningKeyID: "key"}, nil)
	if _, err := s.verifyPlatformRouteCandidate(a, assignment, release); err != nil {
		t.Fatal("complete signed cell routes rejected", err)
	}
	for _, other := range []string{"global", "authority-cell:cell-b"} {
		s.Config.PlatformScopeKey = other
		if _, err := s.verifyPlatformRouteCandidate(a, assignment, release); err == nil {
			t.Fatal("foreign scope artifact accepted")
		}
	}
	s.Config.PlatformScopeKey = scope
	routes := a.Content["routes"].([]any)
	routes[0].(map[string]any)["service_port"] = 65536
	a.ContentHash, _ = platformconfig.Digest(a.Content)
	a, err = platformsafety.SignPlatformArtifact(a, bundleauth.NewKeyring("scope-fixture-key", "key", "", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	assignment.ContentHash = a.ContentHash
	if _, err := s.verifyPlatformRouteCandidate(a, assignment, release); err == nil {
		t.Fatal("scope support bypassed route validation")
	}
}
