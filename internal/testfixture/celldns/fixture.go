// Package celldns supplies synthetic signed publications for compiler,
// consumer, API and store contract tests. It has no production callers.
package celldns

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

func Keys() bundleauth.Keyring {
	return bundleauth.NewKeyring("synthetic-cell-artifact-signing-key", "test", "", "", nil)
}

func Sign(t testing.TB, a model.PlatformArtifact, id string, sequence int64) model.PlatformArtifact {
	t.Helper()
	a.ID, a.ScopeKey, a.GenerationSequence, a.Status = id, a.Scope.Key, sequence, model.PlatformArtifactStatusValidated
	a.ContentHash, _ = platformconfig.Digest(a.Content)
	var err error
	a, err = platformsafety.SignPlatformArtifact(a, Keys())
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func Request(t testing.TB) platformconfig.CompileRequest {
	t.Helper()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	probe := &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 10, ProbeTimeoutSeconds: 2, FactFreshnessSeconds: 60, MaxConcurrency: 4, MaxProbes: 1024}
	topology := &edgetopology.Intent{SchemaVersion: edgetopology.SchemaVersion, Pools: []edgetopology.ServingPool{{ID: "pool-public"}}}
	publications := []platformconfig.CellRoutePublicationInput{}
	endpoints := []platformconfig.DNSEdgeEndpoint{}
	candidates := []platformconfig.DNSSelectionCandidate{}
	for i, suffix := range []string{"a", "b"} {
		cell, node, ip := "cell-"+suffix, "edge-"+suffix, fmt.Sprintf("93.184.216.%d", 34+i)
		edge := edgetopology.Edge{ID: node, AuthorityCellID: cell, ServingPoolIDs: []string{"pool-public"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"provider": "provider-" + suffix, "host": node}}
		topology.Cells = append(topology.Cells, edgetopology.AuthorityCell{ID: cell})
		topology.Edges = append(topology.Edges, edge)
		intent := platformconfig.PlatformIntent{PublicationRole: platformconfig.PublicationRoleCellRoutes, SchemaVersion: platformconfig.SchemaVersion, AuthorityCellID: cell, Scope: platformconfig.AuthorityCellScope(cell), Generation: "routes-" + suffix, EdgeTopology: &edgetopology.Intent{SchemaVersion: edgetopology.SchemaVersion, Cells: []edgetopology.AuthorityCell{{ID: cell}}, Pools: topology.Pools, Edges: []edgetopology.Edge{edge}}, Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", AppID: "app-a", TenantID: "tenant-a", UpstreamURL: "http://origin-" + suffix + ":8080", Enabled: true, RoutePolicy: model.EdgeRoutePolicyEnabled}}, TLS: []platformconfig.TLSIntent{{Hostname: "app.example.test", Policy: "auto"}}}
		members, err := platformconfig.TrafficConsumerTopologyFromIntent(intent)
		if err != nil {
			t.Fatal(err)
		}
		digest, _ := platformconfig.Digest(members)
		policy := platformconfig.PolicySnapshot{PublicationRole: intent.PublicationRole, AuthorityCellID: cell, Scope: intent.Scope, Generation: "routes-policy-" + suffix, ConsumerTopologyDigest: digest, TLSReadiness: probe, MinimumHealthyEdges: 2, MaxStaleSeconds: 3600, TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "complete", EdgeGroupIDs: []string{cell}}}}
		compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: intent, Policy: policy, CreatedAt: now})
		if err != nil {
			t.Fatal(err)
		}
		route := Sign(t, compiled.RouteArtifact, "route-"+suffix, 1)
		tls := Sign(t, compiled.TLSArtifact, "tls-"+suffix, 1)
		parent := Sign(t, platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet, []string{route.ID, tls.ID}, now), "parent-"+suffix, 1)
		release := model.PlatformArtifactRelease{ID: "release-" + suffix, ArtifactID: parent.ID, ArtifactKind: parent.ArtifactKind, ScopeKey: parent.ScopeKey, Generation: parent.Generation, ReleaseChannel: "full", FencingToken: 1, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: now, LaneKey: platformsafety.ReleaseLaneKey(parent.ArtifactKind, parent.ScopeKey, "full")}
		ref := platformconfig.CellRoutePublicationReference{AuthorityCellID: cell, ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, ReleaseID: release.ID, ReleaseChannel: release.ReleaseChannel, FencingToken: release.FencingToken, RouteArtifactID: route.ID, RouteArtifactDigest: route.ContentHash, TLSArtifactID: tls.ID, TLSArtifactDigest: tls.ContentHash}
		publications = append(publications, platformconfig.CellRoutePublicationInput{Reference: ref, Parent: parent, Route: route, TLS: tls})
		endpoints = append(endpoints, platformconfig.DNSEdgeEndpoint{EdgeID: node, EdgeGroupID: cell, A: []string{ip}, ObservedAt: now})
		candidates = append(candidates, platformconfig.DNSSelectionCandidate{IP: ip, EdgeID: node, EdgeGroupID: cell, Weight: 100})
	}
	intent := platformconfig.PlatformIntent{PublicationRole: platformconfig.PublicationRoleCellDNS, SchemaVersion: platformconfig.SchemaVersion, AuthorityCellID: "cell-dns", Scope: platformconfig.AuthorityCellScope("cell-dns"), Generation: "dns-intent", EdgeTopology: topology, DNSConsumers: []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "cell-dns", Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60}}, DNS: []platformconfig.DNSIntent{{Hostname: "app.example.test", AppID: "app-a", TenantID: "tenant-a", Type: "FUGUE_APP", Values: []string{"app-a"}, TTL: 60, Application: &platformconfig.DNSApplicationIntent{IPv4Policy: "ipv4_only", IPv6Policy: "ipv4_only", TTLPolicy: "record", FallbackPolicy: "fail_closed"}}}}
	for _, p := range publications {
		intent.CellRoutePublications = append(intent.CellRoutePublications, p.Reference)
	}
	members, err := platformconfig.TrafficConsumerTopologyFromIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := platformconfig.Digest(members)
	policy := platformconfig.PolicySnapshot{PublicationRole: intent.PublicationRole, AuthorityCellID: intent.AuthorityCellID, Scope: intent.Scope, Generation: "dns-policy", ConsumerTopologyDigest: digest, MinimumHealthyEdges: 1, MaxStaleSeconds: 3600, DNSReadiness: probe, DNSPlacementMode: platformconfig.DNSPlacementConsumerReadiness, DNSQueryPolicy: &platformconfig.DNSQueryPolicy{RankingMode: "disabled", PreferenceMode: "runtime_locality", MinimumTTLSeconds: 10, MaximumTTLSeconds: 60}, TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "complete", EdgeGroupIDs: []string{"cell-dns"}}}, DNSAuthorities: []platformconfig.DNSAuthorityPolicy{{NodeID: "dns-a", Zone: "example.test", Nameservers: []string{"ns.example.test"}, TTLSeconds: 60, RefreshSeconds: 300, RetrySeconds: 60, ExpireSeconds: 3600}}, DNSClientPolicies: []platformconfig.DNSClientPolicy{{NodeID: "dns-a"}}, DNSAnswerRules: []platformconfig.DNSAnswerRule{{NodeID: "dns-a", Hostname: "app.example.test", Type: "A", SelectionMode: "global", TTLSeconds: 60}}, EdgeSelectionConstraints: []platformconfig.EdgeSelectionConstraint{{TenantID: "tenant-a", Hostname: "app.example.test", AllowedPoolIDs: []string{"pool-public"}, RequiredCapabilities: []string{"http", "tls"}, MinCandidates: 2, MinDistinctCells: 2, MinDistinctDomains: map[string]int{"provider": 2}, FactMaxAgeSeconds: 30}}}
	return platformconfig.CompileRequest{Intent: intent, Policy: policy, CreatedAt: now, CellRoutePublications: publications, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now, DNSEdgeEndpoints: endpoints, DNSConsumers: []platformconfig.DNSConsumerObservation{{NodeID: "dns-a", EdgeGroupID: "cell-dns", ObservedAt: now, A: []string{"8.8.8.8"}}}, DNSSelections: []platformconfig.DNSSelectionObservation{{NodeID: "dns-a", Hostname: "app.example.test", Type: "A", SourceGeneration: "selection-input", SourceDigest: "sha256:" + strings.Repeat("a", 64), ObservedAt: now, Candidates: candidates}}}}
}

func Compile(t testing.TB, req platformconfig.CompileRequest) platformconfig.CompileResult {
	t.Helper()
	c, err := platformconfig.Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	c.DNSArtifact = Sign(t, c.DNSArtifact, "dns-child", 1)
	c.ReleaseArtifact = Sign(t, platformconfig.BuildReleaseSetArtifact(c.ReleaseSet, []string{c.DNSArtifact.ID}, req.CreatedAt), "dns-parent", 1)
	return c
}

func Publication(p platformconfig.CellRoutePublicationInput) model.PlatformArtifactRelease {
	r := p.Reference
	return model.PlatformArtifactRelease{ID: r.ReleaseID, ArtifactID: p.Parent.ID, ArtifactKind: p.Parent.ArtifactKind, Scope: p.Parent.Scope, ScopeKey: p.Parent.ScopeKey, Generation: p.Parent.Generation, ReleaseChannel: r.ReleaseChannel, CanaryRuleRef: r.CanaryRuleRef, FencingToken: r.FencingToken, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: p.Parent.CreatedAt, LaneKey: platformsafety.ReleaseLaneKey(p.Parent.ArtifactKind, p.Parent.ScopeKey, r.ReleaseChannel)}
}
