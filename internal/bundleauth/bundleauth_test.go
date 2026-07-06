package bundleauth

import (
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestEdgeRouteBundleSignsLegacyRouteProjectionForOldEdges(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 18, 2, 45, 0, 0, time.UTC)
	keyring := NewKeyring("route-signing-key", "control-plane", "", "", nil)
	bundle := model.EdgeRouteBundle{
		Version:     "routegen_test",
		Generation:  "routegen_test",
		GeneratedAt: now,
		EdgeID:      "edge-us-1",
		EdgeGroupID: "edge-group-country-us",
		Routes: []model.EdgeRouteBinding{
			{
				Hostname:    "app.example.com",
				RouteKind:   model.EdgeRouteKindPlatform,
				AppID:       "app_1",
				TenantID:    "tenant_1",
				RuntimeID:   "runtime_1",
				EdgeGroupID: "edge-group-country-us",
				ExcludedEdgeIDs: []string{
					"edge-de-1",
				},
				ExcludedEdgeGroupIDs: []string{
					"edge-group-country-de",
				},
				ExclusionReason: "slow-upload",
				RoutePolicy:     model.EdgeRoutePolicyEnabled,
				UpstreamKind:    model.EdgeRouteUpstreamKindKubernetesService,
				UpstreamScope:   model.EdgeRouteUpstreamScopeCluster,
				UpstreamURL:     "http://stable.example.svc.cluster.local:80",
				Upstreams: []model.EdgeRouteUpstream{
					{
						Role:         model.AppReleaseRoleStable,
						ReleaseID:    "release_stable",
						Weight:       90,
						UpstreamURL:  "http://stable.example.svc.cluster.local:80",
						RuntimeID:    "runtime_1",
						ServicePort:  80,
						UpstreamKind: model.EdgeRouteUpstreamKindKubernetesService,
					},
					{
						Role:         model.AppReleaseRoleCandidate,
						ReleaseID:    "release_candidate",
						Weight:       10,
						UpstreamURL:  "http://candidate.example.svc.cluster.local:80",
						RuntimeID:    "runtime_1",
						ServicePort:  80,
						UpstreamKind: model.EdgeRouteUpstreamKindKubernetesService,
					},
				},
				ServicePort:     80,
				TLSPolicy:       model.EdgeRouteTLSPolicyPlatform,
				Streaming:       true,
				Status:          model.EdgeRouteStatusActive,
				RouteGeneration: "route_binding_test",
				CreatedAt:       now,
				UpdatedAt:       now,
			},
		},
	}

	signed := SignEdgeRouteBundleWithKeyring(bundle, keyring, time.Hour)
	if len(signed.Signatures) < 2 {
		t.Fatalf("expected current and legacy route signatures, got %+v", signed.Signatures)
	}
	if err := VerifyEdgeRouteBundleWithKeyring(signed, keyring, now); err != nil {
		t.Fatalf("verify signed bundle with current route model: %v", err)
	}

	legacyDecoded := signed
	legacyDecoded.Routes = append([]model.EdgeRouteBinding(nil), signed.Routes...)
	for idx := range legacyDecoded.Routes {
		legacyDecoded.Routes[idx].Upstreams = nil
		legacyDecoded.Routes[idx].ExcludedEdgeIDs = nil
		legacyDecoded.Routes[idx].ExcludedEdgeGroupIDs = nil
		legacyDecoded.Routes[idx].ExclusionReason = ""
		legacyDecoded.Routes[idx].ExclusionExpiresAt = nil
	}
	if err := VerifyEdgeRouteBundleWithKeyring(legacyDecoded, keyring, now); err != nil {
		t.Fatalf("verify signed bundle after old edge drops unknown upstreams: %v", err)
	}

	tampered := signed
	tampered.Routes = append([]model.EdgeRouteBinding(nil), signed.Routes...)
	tampered.Routes[0].Upstreams = append([]model.EdgeRouteUpstream(nil), signed.Routes[0].Upstreams...)
	tampered.Routes[0].Upstreams[1].UpstreamURL = "http://attacker.invalid:80"
	if err := VerifyEdgeRouteBundleWithKeyring(tampered, keyring, now); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("expected current verifier to reject tampered upstreams, got %v", err)
	}
}

func TestEdgeDNSBundleSignsLegacyCandidateProjectionForOldDNSNodes(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	keyring := NewKeyring("dns-signing-key", "control-plane", "", "", nil)
	bundle := model.EdgeDNSBundle{
		Version:       "dnsgen_test",
		Generation:    "dnsgen_test",
		GeneratedAt:   now,
		DNSNodeID:     "dns-us-1",
		EdgeGroupID:   "edge-group-country-us",
		Zone:          "fugue.pro",
		SchemaVersion: model.BundleSchemaVersionV1,
		Records: []model.EdgeDNSRecord{
			{
				Name:             "api.example.com.",
				Type:             "A",
				Values:           []string{"203.0.113.10"},
				TTL:              30,
				RecordKind:       "platform",
				AppID:            "app_1",
				TenantID:         "tenant_1",
				EdgeGroupID:      "edge-group-country-us",
				Status:           "active",
				RecordGeneration: "record_test",
				Candidates: []model.EdgeDNSAnswerCandidate{
					{
						IP:                "203.0.113.10",
						EdgeID:            "edge-us-1",
						EdgeGroupID:       "edge-group-country-us",
						Region:            "us-west",
						Country:           "US",
						WorkloadMode:      "dynamic",
						CanaryState:       "canary",
						CanaryWeight:      1,
						PublicProbeStatus: "pass",
						DNSEligible:       true,
						Priority:          10,
						Weight:            100,
						Reason:            "dynamic_canary",
						TrafficClass:      "default",
						Score:             0.95,
						ScoreBreakdown: map[string]float64{
							"ttfb": 0.8,
						},
						Healthy:    true,
						RouteReady: true,
						TLSReady:   true,
					},
				},
				ScopedCandidates: []model.EdgeDNSScopedAnswerCandidates{
					{
						ScopeKey:            "country:CN",
						Country:             "CN",
						PolicyKind:          "quality",
						Reason:              "best_quality",
						SelectedEdgeGroupID: "edge-group-country-us",
						Candidates: []model.EdgeDNSAnswerCandidate{
							{
								IP:                "203.0.113.10",
								EdgeID:            "edge-us-1",
								EdgeGroupID:       "edge-group-country-us",
								WorkloadMode:      "dynamic",
								CanaryState:       "canary",
								CanaryWeight:      1,
								PublicProbeStatus: "pass",
								DNSEligible:       true,
								Priority:          1,
								Weight:            100,
								Reason:            "scoped_dynamic_canary",
								Healthy:           true,
								RouteReady:        true,
								TLSReady:          true,
							},
						},
					},
				},
			},
		},
	}

	signed := SignEdgeDNSBundleWithKeyring(bundle, keyring, time.Hour)
	if len(signed.Signatures) < 2 {
		t.Fatalf("expected current and legacy DNS signatures, got %+v", signed.Signatures)
	}
	if err := VerifyEdgeDNSBundleWithKeyring(signed, keyring, now); err != nil {
		t.Fatalf("verify signed bundle with current DNS model: %v", err)
	}

	legacyDecoded := signed
	legacyDecoded.Records = append([]model.EdgeDNSRecord(nil), signed.Records...)
	for recordIdx := range legacyDecoded.Records {
		legacyDecoded.Records[recordIdx].Candidates = cloneLegacyEdgeDNSAnswerCandidates(signed.Records[recordIdx].Candidates)
		legacyDecoded.Records[recordIdx].ScopedCandidates = cloneLegacyEdgeDNSScopedAnswerCandidates(signed.Records[recordIdx].ScopedCandidates)
	}
	if err := VerifyEdgeDNSBundleWithKeyring(legacyDecoded, keyring, now); err != nil {
		t.Fatalf("verify signed bundle after old DNS drops unknown candidate fields: %v", err)
	}

	tampered := signed
	tampered.Records = append([]model.EdgeDNSRecord(nil), signed.Records...)
	tampered.Records[0].Candidates = append([]model.EdgeDNSAnswerCandidate(nil), signed.Records[0].Candidates...)
	tampered.Records[0].Candidates[0].PublicProbeStatus = "fail"
	if err := VerifyEdgeDNSBundleWithKeyring(tampered, keyring, now); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("expected current verifier to reject tampered dynamic DNS candidate fields, got %v", err)
	}
}
