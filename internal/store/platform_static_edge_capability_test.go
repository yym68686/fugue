package store

import (
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestStaticEdgeCapabilityDoesNotEnrollRouteAndTLSWorkers(t *testing.T) {
	availability := model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"static-a", "edge-a"}, RecoverySuccesses: 3, RecoveryMinSeconds: 30}
	policy := platformconfig.PolicySnapshot{
		StaticEdgeRoutes: []platformconfig.StaticEdgeDNSRoute{{AppID: "app-a", TenantID: "tenant-a", Hostname: "app.example.test", Routing: model.AppStaticEdgeRouting{
			Version: model.StaticEdgeDNSVersion, Availability: availability,
			Candidates: []model.StaticEdgeDNSCandidate{{RegistrationID: "registration-a", EdgeID: "static-a", Addresses: []string{"1.1.1.1"}, ProbePath: "/health", RouteDigest: "sha256:" + strings.Repeat("c", 64), StatusCode: 200}},
		}}},
		DNSQueryPolicy: &platformconfig.DNSQueryPolicy{RankingMode: "active", PreferenceMode: "runtime_locality", MinimumTTLSeconds: 60, MaximumTTLSeconds: 120, AvailabilityRoutes: []platformconfig.DNSAvailabilityRoute{{Hostname: "app.example.test", Policy: availability}}},
	}
	for _, kind := range []string{model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig} {
		t.Run(kind, func(t *testing.T) {
			// Compiled children all retain the shared signed policy. Its presence
			// must not enroll route/TLS workers as independent DNS proof consumers.
			required, err := physicalCapabilitiesRequired(model.PlatformArtifact{ArtifactKind: kind, Content: map[string]any{"policy": policy}})
			if err != nil {
				t.Fatal(err)
			}
			if required.staticEdge != (kind == model.PlatformArtifactKindDNSAnswerBundle) {
				t.Fatalf("static endpoint protocol requirement crosses consumer boundary: kind=%s required=%t", kind, required.staticEdge)
			}
			if required.staticEdgePolicy != (kind != model.PlatformArtifactKindDNSAnswerBundle) {
				t.Fatalf("shared static policy decoding is not required of route/TLS consumers: kind=%s required=%t", kind, required.staticEdgePolicy)
			}
			if !required.availability {
				t.Fatal("independent static proof scoping removed the existing availability requirement")
			}
		})
	}
	policy.StaticEdgeRoutes = nil
	required, err := physicalCapabilitiesRequired(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, Content: map[string]any{"policy": policy}})
	if err != nil || required.staticEdge || required.staticEdgePolicy || !required.availability {
		t.Fatalf("ordinary managed availability requires an unrelated static protocol: %+v %v", required, err)
	}
}
