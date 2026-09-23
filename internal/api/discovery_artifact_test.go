package api

import (
	"encoding/json"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
	"fugue/internal/store"
)

func TestDiscoveryPlatformRoutesFailClosedWithoutVerifiedTrafficArtifact(t *testing.T) {
	_, s, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	s.platformRoutes = []model.PlatformRoute{{Hostname: "ambient.example.test", UpstreamURL: "http://ambient:8080"}}
	routes, err := s.publishedDiscoveryPlatformRoutes()
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

func TestPublicDiscoveryVerifiesPinnedPlatformMembershipAndEveryMember(t *testing.T) {
	path := t.TempDir() + "/state.json"
	state := store.New(path)
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	s := NewServer(state, auth.New(state, ""), nil, ServerConfig{BundleSigningKey: "synthetic-discovery-key", BundleSigningKeyID: "discovery-key"})
	now := time.Now().UTC()
	if _, err := state.UpdateDNSHeartbeat(model.DNSNode{ID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test", PublicIPv4: "8.8.8.8", LastSeenAt: &now}); err != nil {
		t.Fatal(err)
	}
	seedVerifiedDNSDelegationFixture(t, s, "example.test", platformconfig.RouteIntent{Hostname: "private.example.test", UpstreamURL: "http://tenant-private:8080", Enabled: false, AppID: "synthetic-app", TenantID: "synthetic-tenant"})
	s.platformRoutes = []model.PlatformRoute{{Hostname: "ambient.example.test", UpstreamURL: "http://ambient:8080"}}
	// A new valid base draft is not the verified parent source.
	createTestStaticIntent(t, s, "unpublished-newer-base", "http://newer:8080")
	response := performJSONRequest(t, s, http.MethodGet, "/v1/discovery/bundle", "", nil)
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	var bundle model.DiscoveryBundle
	mustDecodeJSON(t, response, &bundle)
	if len(bundle.PlatformRoutes) != 1 || bundle.PlatformRoutes[0].Hostname != "disabled.example.test" || strings.Contains(response.Body.String(), "tenant-private") || strings.Contains(response.Body.String(), "ambient.example") || strings.Contains(response.Body.String(), "newer:8080") {
		t.Fatal("anonymous discovery leaked unrelated routes", bundle.PlatformRoutes)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Use the disposable file store to model persisted corruption. It is never
	// published, and every read must reject without re-signing the bad input.
	for _, scenario := range []string{"route signature", "DNS signature", "TLS signature", "base signature", "base digest", "member lineage", "missing child", "LKG signature"} {
		t.Run(scenario, func(t *testing.T) {
			var st model.State
			if err := json.Unmarshal(raw, &st); err != nil {
				t.Fatal(err)
			}
			parent, _, _ := s.verifiedPlatformArtifactForScope(model.PlatformArtifactKindReleaseSet, "global")
			for i := range st.PlatformArtifacts {
				a := &st.PlatformArtifacts[i]
				if scenario == "route signature" && a.ArtifactKind == model.PlatformArtifactKindEdgeRouteBundle || scenario == "DNS signature" && a.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle || scenario == "TLS signature" && a.ArtifactKind == model.PlatformArtifactKindCaddyRouteConfig || scenario == "base signature" && a.ID == parent.Metadata["producer_static_intent_id"] {
					a.Provenance.Signature = "corrupt"
				}
				if scenario == "base digest" && a.ID == parent.Metadata["producer_static_intent_id"] {
					a.Content["generation"] = "changed-base"
					signed, e := platformsafety.SignPlatformArtifact(*a, s.bundleKeyring())
					if e != nil {
						t.Fatal(e)
					}
					*a = signed
				}
				if scenario == "member lineage" && a.ArtifactKind == model.PlatformArtifactKindEdgeRouteBundle {
					a.Metadata["compiler_version"] = "foreign-compiler"
					signed, e := platformsafety.SignPlatformArtifact(*a, s.bundleKeyring())
					if e != nil {
						t.Fatal(e)
					}
					*a = signed
				}
				if scenario == "missing child" && a.ArtifactKind == model.PlatformArtifactKindEdgeRouteBundle {
					a.ID = "missing"
				}
			}
			if scenario == "LKG signature" {
				for i := range st.PlatformLKGSnapshots {
					st.PlatformLKGSnapshots[i].SnapshotProvenance.Signature = "corrupt"
				}
			}
			damaged, _ := json.Marshal(st)
			if err := os.WriteFile(path, damaged, 0600); err != nil {
				t.Fatal(err)
			}
			r := performJSONRequest(t, s, http.MethodGet, "/v1/discovery/bundle", "", nil)
			if r.Code != 503 || r.Header().Get("ETag") != "" || strings.Contains(r.Body.String(), "platform_routes") {
				t.Fatal("bad source signed into public discovery", r.Code, r.Body.String())
			}
			after, _ := os.ReadFile(path)
			if !reflect.DeepEqual(damaged, after) {
				t.Fatal("query rewrote configuration")
			}
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
		})
	}
}
