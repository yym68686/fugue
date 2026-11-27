package api

import (
	"net/http"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDiscoveryPlatformRoutesFailClosedWithoutVerifiedTrafficArtifact(t *testing.T) {
	_, s, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	s.platformRoutes = []model.PlatformRoute{{Hostname: "ambient.example.test", UpstreamURL: "http://ambient:8080"}}
	routes, err := s.publishedDiscoveryPlatformRoutes(t.Context())
	if err != nil || len(routes) != 0 {
		t.Fatal("ambient routes accepted", routes, err)
	}
	response := performJSONRequest(t, s, http.MethodGet, "/v1/discovery/bundle", "", nil)
	if response.Code != 200 || strings.Contains(response.Body.String(), "ambient.example.test") {
		t.Fatal("bootstrap topology blocked or ambient routes exposed", response.Code)
	}
}

func TestDiscoverySummaryPreservesStaticSemanticsAndExcludesPrivateRoutes(t *testing.T) {
	routes := []platformconfig.RouteIntent{
		{Hostname: "platform.example.test", UpstreamURL: "http://platform:8080", Enabled: true, Kind: "control-plane-api", EdgeGroupMode: "pinned", EdgeGroupID: "edge-group-a", TLSPolicy: "platform", RoutePolicy: "edge_enabled", TTL: 180},
		{Hostname: "disabled.example.test", UpstreamURL: "http://disabled:8080", Enabled: false, Kind: "platform", EdgeGroupMode: "region_aware", TTL: 300},
		{Hostname: "tenant.example.test", UpstreamURL: "http://tenant-private:8080", Enabled: true, AppID: "synthetic-app", TenantID: "synthetic-tenant"},
		{Hostname: "platform.example.test", PathPrefix: "/private", UpstreamURL: "http://private-path:8080", Enabled: true, AppID: "synthetic-app", TenantID: "synthetic-tenant"},
		{Hostname: "owned.example.test", UpstreamURL: "http://overridden:8080", Enabled: true, AppID: "synthetic-app", TenantID: "synthetic-tenant"},
	}
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Scope: "global", Generation: "summary", Routes: routes}, Policy: platformconfig.PolicySnapshot{Scope: "global", Generation: "policy"}})
	if err != nil {
		t.Fatal(err)
	}
	declared := []model.PlatformRoute{{Hostname: "platform.example.test"}, {Hostname: "disabled.example.test"}, {Hostname: "owned.example.test"}}
	result, err := discoveryPlatformRouteSummary(compiled.RouteArtifact, declared)
	if err != nil || len(result) != 2 {
		t.Fatal(result, err)
	}
	if result[0].Hostname != "disabled.example.test" || result[0].Status != "disabled" || result[0].TTL != 300 || result[0].EdgeGroupMode != "region_aware" {
		t.Fatal("disabled entry lost semantics", result[0])
	}
	if result[1].Hostname != "platform.example.test" || result[1].UpstreamURL != "http://platform:8080" || result[1].EdgeGroupMode != "pinned" || result[1].EdgeGroupID != "edge-group-a" || result[1].TTL != 180 || result[1].TLSPolicy != "platform" {
		t.Fatal("platform summary is lossy", result[1])
	}
	weighted := routes[0]
	weighted.Upstreams = []platformconfig.UpstreamIntent{{UpstreamURL: "http://first:8080", Weight: 60}, {UpstreamURL: "http://second:8080", Weight: 40}}
	compiled, err = platformconfig.Compile(platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Scope: "global", Generation: "weighted", Routes: []platformconfig.RouteIntent{weighted}}, Policy: platformconfig.PolicySnapshot{Scope: "global", Generation: "policy"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := discoveryPlatformRouteSummary(compiled.RouteArtifact, declared); err == nil {
		t.Fatal("weighted route collapsed to arbitrary origin")
	}
}
