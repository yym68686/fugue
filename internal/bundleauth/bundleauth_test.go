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
				Hostname:      "app.example.com",
				RouteKind:     model.EdgeRouteKindPlatform,
				AppID:         "app_1",
				TenantID:      "tenant_1",
				RuntimeID:     "runtime_1",
				EdgeGroupID:   "edge-group-country-us",
				RoutePolicy:   model.EdgeRoutePolicyEnabled,
				UpstreamKind:  model.EdgeRouteUpstreamKindKubernetesService,
				UpstreamScope: model.EdgeRouteUpstreamScopeCluster,
				UpstreamURL:   "http://stable.example.svc.cluster.local:80",
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
