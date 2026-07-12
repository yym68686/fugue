package rollbackpreflight

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestBuildEdgeRouteBundleArtifactIsStableAcrossRefreshesAndNodes(t *testing.T) {
	bundle := semanticRouteBundleFixture()
	first, err := BuildEdgeRouteBundleArtifact(bundle)
	if err != nil {
		t.Fatalf("build first route artifact: %v", err)
	}

	refreshed := cloneRouteBundle(t, bundle)
	refreshed.EdgeID = "edge-b"
	refreshed.GeneratedAt = refreshed.GeneratedAt.Add(30 * time.Minute)
	refreshed.ValidUntil = refreshed.ValidUntil.Add(30 * time.Minute)
	refreshed.PreviousGeneration = "routegen_previous"
	refreshed.Issuer = "rotated-issuer"
	refreshed.KeyID = "rotated-key"
	refreshed.Signature = "rotated-signature"
	refreshed.Signatures = []model.BundleSignature{{
		SchemaVersion: model.BundleSchemaVersionV1,
		Issuer:        "rotated-issuer",
		KeyID:         "rotated-key",
		Signature:     "rotated-signature",
		GeneratedAt:   refreshed.GeneratedAt,
		ValidUntil:    refreshed.ValidUntil,
	}}
	refreshed.Routes[0].CreatedAt = refreshed.Routes[0].CreatedAt.Add(time.Hour)
	refreshed.Routes[0].UpdatedAt = refreshed.Routes[0].UpdatedAt.Add(time.Hour)
	second, err := BuildEdgeRouteBundleArtifact(refreshed)
	if err != nil {
		t.Fatalf("build refreshed route artifact: %v", err)
	}

	if !reflect.DeepEqual(first.Content, second.Content) {
		t.Fatalf("refresh-only fields changed semantic content:\nfirst=%#v\nsecond=%#v", first.Content, second.Content)
	}
	if first.Generation != second.Generation {
		t.Fatalf("refresh changed generation: first=%s second=%s", first.Generation, second.Generation)
	}
	if first.Scope.EdgeID != "edge-a" || second.Scope.EdgeID != "edge-b" {
		t.Fatalf("node-specific scope was not preserved: first=%#v second=%#v", first.Scope, second.Scope)
	}
	if first.ArtifactKind != model.PlatformArtifactKindEdgeRouteBundle || first.Status != model.PlatformArtifactStatusDraft {
		t.Fatalf("unexpected route artifact metadata: %#v", first)
	}
}

func TestBuildEdgeRouteBundleArtifactRejectsGenerationDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*model.EdgeRouteBundle)
		want   string
	}{
		{
			name: "missing scope",
			mutate: func(bundle *model.EdgeRouteBundle) {
				bundle.EdgeID = ""
			},
			want: "requires edge_id and edge_group_id",
		},
		{
			name: "unsupported schema",
			mutate: func(bundle *model.EdgeRouteBundle) {
				bundle.SchemaVersion = "2.0"
			},
			want: "unsupported bundle schema version",
		},
		{
			name: "missing generation",
			mutate: func(bundle *model.EdgeRouteBundle) {
				bundle.Generation = ""
			},
			want: "version and generation are required",
		},
		{
			name: "version generation conflict",
			mutate: func(bundle *model.EdgeRouteBundle) {
				bundle.Generation = "routegen_other"
			},
			want: "does not match generation",
		},
		{
			name: "serving material changed",
			mutate: func(bundle *model.EdgeRouteBundle) {
				bundle.Routes[0].Upstreams[0].UpstreamURL = "http://changed.default.svc.cluster.local:9000"
				bundle.Routes[0].RouteGeneration = ""
			},
			want: "does not match semantic generation",
		},
		{
			name: "request body policy changed",
			mutate: func(bundle *model.EdgeRouteBundle) {
				bundle.Routes[0].RequestBodyPolicies[0].MaxBytes++
				bundle.Routes[0].RouteGeneration = ""
			},
			want: "does not match semantic generation",
		},
		{
			name: "route generation conflict",
			mutate: func(bundle *model.EdgeRouteBundle) {
				bundle.Routes[0].RouteGeneration = "routegen_wrong"
			},
			want: "route 0 generation mismatch",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bundle := semanticRouteBundleFixture()
			test.mutate(&bundle)
			if _, err := BuildEdgeRouteBundleArtifact(bundle); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("BuildEdgeRouteBundleArtifact() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestBuildDNSAnswerBundleArtifactIsStableAcrossRefreshesAndNodes(t *testing.T) {
	bundle := semanticDNSBundleFixture()
	first, err := BuildDNSAnswerBundleArtifact(bundle)
	if err != nil {
		t.Fatalf("build first DNS artifact: %v", err)
	}

	refreshed := cloneDNSBundle(t, bundle)
	refreshed.DNSNodeID = "dns-b"
	refreshed.Zone = ".EXAMPLE.TEST."
	refreshed.GeneratedAt = refreshed.GeneratedAt.Add(30 * time.Minute)
	refreshed.ValidUntil = refreshed.ValidUntil.Add(30 * time.Minute)
	refreshed.PreviousGeneration = "dnsgen_previous"
	refreshed.Issuer = "rotated-issuer"
	refreshed.KeyID = "rotated-key"
	refreshed.Signature = "rotated-signature"
	refreshed.Signatures = nil
	second, err := BuildDNSAnswerBundleArtifact(refreshed)
	if err != nil {
		t.Fatalf("build refreshed DNS artifact: %v", err)
	}

	if !reflect.DeepEqual(first.Content, second.Content) {
		t.Fatalf("refresh-only fields changed DNS semantic content:\nfirst=%#v\nsecond=%#v", first.Content, second.Content)
	}
	if first.Scope.NodeID != "dns-a" || second.Scope.NodeID != "dns-b" || second.Scope.Hostname != "example.test" {
		t.Fatalf("DNS node and zone scope was not preserved: first=%#v second=%#v", first.Scope, second.Scope)
	}
	if first.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || first.Generation != second.Generation {
		t.Fatalf("unexpected DNS artifact metadata: first=%#v second=%#v", first, second)
	}
}

func TestBuildDNSAnswerBundleArtifactRejectsGenerationDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*model.EdgeDNSBundle)
		want   string
	}{
		{
			name: "missing scope",
			mutate: func(bundle *model.EdgeDNSBundle) {
				bundle.DNSNodeID = ""
			},
			want: "requires dns_node_id, edge_group_id, and zone",
		},
		{
			name: "unsupported schema",
			mutate: func(bundle *model.EdgeDNSBundle) {
				bundle.SchemaVersion = "2.0"
			},
			want: "unsupported bundle schema version",
		},
		{
			name: "missing version",
			mutate: func(bundle *model.EdgeDNSBundle) {
				bundle.Version = ""
			},
			want: "version and generation are required",
		},
		{
			name: "serving material changed",
			mutate: func(bundle *model.EdgeDNSBundle) {
				bundle.Records[0].Candidates[0].Weight++
				bundle.Records[0].RecordGeneration = ""
			},
			want: "does not match semantic generation",
		},
		{
			name: "record generation conflict",
			mutate: func(bundle *model.EdgeDNSBundle) {
				bundle.Records[0].RecordGeneration = "dnsgen_wrong"
			},
			want: "record 0 generation mismatch",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bundle := semanticDNSBundleFixture()
			test.mutate(&bundle)
			if _, err := BuildDNSAnswerBundleArtifact(bundle); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("BuildDNSAnswerBundleArtifact() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func semanticRouteBundleFixture() model.EdgeRouteBundle {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	exclusionExpiry := now.Add(2 * time.Hour)
	route := model.EdgeRouteBinding{
		Hostname:             "api.example.test",
		PathPrefix:           "/v1",
		RouteKind:            model.EdgeRouteKindCustomDomain,
		AppID:                "app-1",
		TenantID:             "tenant-1",
		RuntimeID:            "runtime-1",
		RuntimeType:          "kubernetes",
		RuntimeEdgeGroupID:   "runtime-group-a",
		RuntimeClusterNode:   "worker-a",
		RuntimeEdgeGroup:     "runtime-group-a",
		SelectedEdgeGroup:    "edge-group-a",
		EdgeGroupID:          "edge-group-a",
		FallbackEdgeGroupID:  "edge-group-b",
		PolicyEdgeGroupID:    "edge-group-a",
		ExcludedEdgeIDs:      []string{"edge-c"},
		ExcludedEdgeGroupIDs: []string{"edge-group-c"},
		ExclusionReason:      "operator policy",
		ExclusionExpiresAt:   &exclusionExpiry,
		MinHealthyEdgeNodes:  1,
		HealthyEdgeNodeCount: 2,
		EdgeRedundancyStatus: "ready",
		EdgeRedundancyReason: "two nodes",
		RoutePolicy:          model.EdgeRoutePolicyEnabled,
		SelectionReason:      "quality",
		FallbackReason:       "regional fallback",
		UpstreamKind:         model.EdgeRouteUpstreamKindKubernetesService,
		UpstreamScope:        model.EdgeRouteUpstreamScopeCluster,
		UpstreamURL:          "http://stable.default.svc.cluster.local:8080",
		Upstreams: []model.EdgeRouteUpstream{{
			Role:                 "stable",
			ReleaseID:            "release-1",
			Weight:               100,
			UpstreamKind:         model.EdgeRouteUpstreamKindKubernetesService,
			UpstreamScope:        model.EdgeRouteUpstreamScopeCluster,
			UpstreamURL:          "http://stable.default.svc.cluster.local:8080",
			ServicePort:          8080,
			RuntimeID:            "runtime-1",
			DeploymentGeneration: "deploy-1",
			Status:               "ready",
			StatusReason:         "probe passed",
		}},
		ServicePort:          8080,
		TLSPolicy:            model.EdgeRouteTLSPolicyCustomDomain,
		CachePolicyID:        "cache-1",
		CacheNamespace:       "app-1",
		DeploymentGeneration: "deploy-1",
		RequestBodyPolicies: []model.EdgeRequestBodyPolicy{{
			Name:              "responses-upload",
			Methods:           []string{"POST"},
			Paths:             []string{"/v1/responses"},
			MaxBytes:          64 << 20,
			TimeoutSeconds:    600,
			MaxConcurrent:     32,
			RetryAfterSeconds: 5,
		}},
		Streaming:    true,
		Status:       model.EdgeRouteStatusActive,
		StatusReason: "ready",
		CreatedAt:    now.Add(-time.Hour),
		UpdatedAt:    now,
	}
	route.RouteGeneration = routeGeneration(edgeRouteVersionMaterialFromBinding(route))
	bundle := model.EdgeRouteBundle{
		SchemaVersion: model.BundleSchemaVersionV1,
		GeneratedAt:   now,
		ValidUntil:    now.Add(time.Hour),
		Issuer:        model.BundleIssuerFugue,
		KeyID:         "key-a",
		Signature:     "signature-a",
		EdgeID:        "edge-a",
		EdgeGroupID:   "edge-group-a",
		Routes:        []model.EdgeRouteBinding{route},
		TLSAllowlist: []model.EdgeTLSAllowlistEntry{{
			Hostname:  "api.example.test",
			AppID:     "app-1",
			TenantID:  "tenant-1",
			Status:    "ready",
			TLSStatus: model.EdgeTLSStatusReady,
		}},
		CachePolicies: []model.CachePolicy{{
			ID:                          "cache-1",
			Kind:                        "static",
			HostnameScope:               "api.example.test",
			PathPatterns:                []string{"/assets/*"},
			MethodAllowlist:             []string{"GET", "HEAD"},
			StatusAllowlist:             []int{200, 206},
			TTLSeconds:                  3600,
			StaleWhileRevalidateSeconds: 60,
			BrowserCacheControl:         "public, max-age=60",
			EdgeCacheControl:            "public, max-age=3600",
			BypassOnAuthorization:       true,
			BypassOnCookie:              true,
			VaryAllowlist:               []string{"Accept-Encoding"},
			PurgeMode:                   "generation",
		}},
	}
	bundle.Version = generationForMaterial("routegen_", edgeRouteBundleGenerationMaterial{
		Routes:        []edgeRouteVersionMaterial{edgeRouteVersionMaterialFromBinding(route)},
		TLSAllowlist:  bundle.TLSAllowlist,
		CachePolicies: bundle.CachePolicies,
	})
	bundle.Generation = bundle.Version
	return bundle
}

func semanticDNSBundleFixture() model.EdgeDNSBundle {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	record := model.EdgeDNSRecord{
		Name:                "api.example.test",
		Type:                "A",
		Values:              []string{"192.0.2.10", "192.0.2.11"},
		TTL:                 60,
		RecordKind:          model.EdgeDNSRecordKindCustomDomainTarget,
		AppID:               "app-1",
		TenantID:            "tenant-1",
		EdgeGroupID:         "edge-group-a",
		FallbackEdgeGroupID: "edge-group-b",
		Status:              "ready",
		StatusReason:        "route ready",
		AnswerPolicy: model.DNSAnswerPolicy{
			PolicyKind:                "quality",
			AllowedEdgeGroups:         []string{"edge-group-a", "edge-group-b"},
			PreferredEdgeGroups:       []string{"edge-group-a"},
			FallbackEdgeGroups:        []string{"edge-group-b"},
			TTLSeconds:                60,
			ECSEnabled:                true,
			HealthRequired:            true,
			RouteReadyRequired:        true,
			ExplorationPercent:        5,
			SwitchCooldownSec:         120,
			RankingVersion:            "rank-v1",
			RankingScope:              "global",
			Region:                    "us-west",
			Country:                   "US",
			Priority:                  10,
			Weight:                    100,
			Reason:                    "healthy",
			SelectedEdgeGroupID:       "edge-group-a",
			ShadowSelectedEdgeGroupID: "edge-group-b",
			ShadowReason:              "exploration",
		},
		Candidates: []model.EdgeDNSAnswerCandidate{{
			IP:                "192.0.2.10",
			EdgeID:            "edge-a",
			EdgeGroupID:       "edge-group-a",
			Region:            "us-west",
			Country:           "US",
			WorkloadMode:      "edge",
			CanaryState:       model.EdgeCanaryStateActive,
			CanaryWeight:      100,
			PublicProbeStatus: model.EdgePublicProbeStatusPassing,
			ServingGeneration: "routegen_current",
			LKGGeneration:     "routegen_previous",
			CacheStatus:       "fresh",
			DNSEligible:       true,
			Priority:          10,
			Weight:            100,
			Reason:            "healthy",
			TrafficClass:      "api",
			Score:             0.91,
			ScoreBreakdown:    map[string]float64{"latency": 0.9},
			Healthy:           true,
			RouteReady:        true,
			TLSReady:          true,
		}},
		ScopedCandidates: []model.EdgeDNSScopedAnswerCandidates{{
			ScopeKey:            "country=US",
			Country:             "US",
			Region:              "us-west",
			ASN:                 "64500",
			PolicyKind:          "quality",
			Reason:              "regional quality",
			SelectedEdgeGroupID: "edge-group-a",
			CooldownUntil:       now.Add(time.Minute),
			Candidates: []model.EdgeDNSAnswerCandidate{{
				IP:          "192.0.2.11",
				EdgeID:      "edge-b",
				EdgeGroupID: "edge-group-b",
				Weight:      10,
				Healthy:     true,
				RouteReady:  true,
				TLSReady:    true,
			}},
		}},
	}
	record.RecordGeneration = generationForMaterial("dnsgen_", edgeDNSRecordVersionMaterialFromRecord(record))
	bundle := model.EdgeDNSBundle{
		SchemaVersion: model.BundleSchemaVersionV1,
		GeneratedAt:   now,
		ValidUntil:    now.Add(time.Hour),
		Issuer:        model.BundleIssuerFugue,
		KeyID:         "key-a",
		Signature:     "signature-a",
		DNSNodeID:     "dns-a",
		EdgeGroupID:   "edge-group-a",
		Zone:          "example.test",
		Records:       []model.EdgeDNSRecord{record},
	}
	bundle.Version = generationForMaterial("dnsgen_", edgeDNSBundleGenerationMaterial{
		Zone:    bundle.Zone,
		Records: []edgeDNSRecordVersionMaterial{edgeDNSRecordVersionMaterialFromRecord(record)},
	})
	bundle.Generation = bundle.Version
	return bundle
}

func cloneRouteBundle(t *testing.T, bundle model.EdgeRouteBundle) model.EdgeRouteBundle {
	t.Helper()
	raw, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("marshal route bundle clone: %v", err)
	}
	var cloned model.EdgeRouteBundle
	if err := json.Unmarshal(raw, &cloned); err != nil {
		t.Fatalf("unmarshal route bundle clone: %v", err)
	}
	return cloned
}

func cloneDNSBundle(t *testing.T, bundle model.EdgeDNSBundle) model.EdgeDNSBundle {
	t.Helper()
	raw, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("marshal DNS bundle clone: %v", err)
	}
	var cloned model.EdgeDNSBundle
	if err := json.Unmarshal(raw, &cloned); err != nil {
		t.Fatalf("unmarshal DNS bundle clone: %v", err)
	}
	return cloned
}
