package api

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestImportedEnvironmentMatchesLegacyRouteAndDNSProjection(t *testing.T) {
	routesJSON := `{"routes":[
		{"hostname":"api.example","kind":"control-plane-api","upstream_url":"http://api:8080","upstream_kind":"kubernetes-service","upstream_scope":"cluster","tls_policy":"platform","route_policy":"edge_enabled","edge_group_mode":"region_aware","ttl":60},
		{"hostname":"pinned.example","kind":"control-plane-mesh","upstream_url":"http://mesh:80","route_policy":"edge_canary","edge_group_mode":"pinned","edge_group_id":"edge-group-test-a"},
		{"hostname":"disabled.example","upstream_url":"http://disabled:80","status":"disabled"},
		{"hostname":"unavailable.example","upstream_url":"http://unavailable:80","status":"unavailable","status_reason":"configured maintenance"},
		{"hostname":"origin.example","upstream_url":"http://origin:80","route_policy":"route_a_only"}
	]}`
	dnsJSON := `{"records":[
		{"name":"api.example","type":"A","values":["192.0.2.1","192.0.2.2"],"ttl":60},
		{"name":"example","type":"MX","values":["10 mail.example."],"ttl":300,"record_kind":"hosted","status":"active"},
		{"name":"example","type":"TXT","values":["v=spf1 -all"],"ttl":300},
		{"name":"example","type":"NS","values":["NS.Example."],"ttl":3600}
	]}`
	imported, err := platformconfig.ImportEnvironment(map[string]string{"FUGUE_PLATFORM_ROUTES_JSON": routesJSON, "FUGUE_DNS_STATIC_RECORDS_JSON": dnsJSON}, "migration-comparison")
	if err != nil {
		t.Fatal(err)
	}
	_, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, platformConfigCompileRequest{
		Intent: imported.Intent, Policy: platformconfig.PolicySnapshot{Generation: "migration-policy", MinimumHealthyEdges: 2},
	})
	if response.Code != http.StatusCreated {
		t.Fatalf("compile: %d %s", response.Code, response.Body.String())
	}
	var compiled platformConfigCompileResponse
	mustDecodeJSON(t, response, &compiled)
	seedVerifiedPlatformArtifactAPI(t, server, admin, compiled.RouteArtifact.ID)
	snapshot, found, err := server.edgeRouteIntentSnapshotFromVerifiedArtifact()
	if err != nil || !found {
		t.Fatalf("verified projection: found=%v err=%v", found, err)
	}
	byHost := map[string]model.EdgeRouteIntent{}
	for _, route := range snapshot.Routes {
		byHost[route.Hostname] = route
	}
	legacy := parsePlatformRoutes(routesJSON, nil)
	if len(legacy) != len(snapshot.Routes) {
		t.Fatal("route inventory changed")
	}
	for _, route := range legacy {
		want := edgeRouteIntentFromPlatformRoute(route)
		want.MinHealthyEdgeNodes = 2 // Explicit changeable policy applies to both paths.
		if route.Status == model.EdgeRouteStatusDisabled {
			want.RoutePolicy = model.EdgeRoutePolicyRouteAOnly
		}
		want.Generation = edgeRouteIntentGeneration(want)
		if got := byHost[route.Hostname]; !reflect.DeepEqual(got, want) {
			t.Fatalf("route semantics changed for %s:\ngot %+v\nwant %+v", route.Hostname, got, want)
		}
	}
	for _, route := range imported.Intent.Routes {
		if route.Hostname == "api.example" && (route.Kind != "control-plane-api" || route.EdgeGroupMode != "region_aware" || route.TTL != 60) {
			t.Fatalf("lost platform metadata: %+v", route)
		}
	}
	legacyDNS := parseEdgeDNSStaticRecords(dnsJSON, nil)
	raw, err := json.Marshal(compiled.DNSArtifact.Content["records"])
	if err != nil {
		t.Fatal(err)
	}
	var records []platformconfig.DNSIntent
	if err := json.Unmarshal(raw, &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != len(legacyDNS) {
		t.Fatal("DNS inventory changed")
	}
	for _, expected := range legacyDNS {
		found := false
		for _, got := range records {
			if got.Hostname == expected.Name && got.Type == expected.Type {
				found = true
				if !reflect.DeepEqual(got.Values, expected.Values) || got.TTL != expected.TTL || got.RecordKind != expected.RecordKind || got.Status != expected.Status {
					t.Fatalf("DNS semantics changed: %+v != %+v", got, expected)
				}
			}
		}
		if !found {
			t.Fatalf("lost DNS record: %+v", expected)
		}
	}
	// Compiler lineage must remain replayable with the imported snapshot.
	first, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: imported.Intent, Policy: platformconfig.PolicySnapshot{Generation: "policy"}, CreatedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	second, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: imported.Intent, Policy: platformconfig.PolicySnapshot{Generation: "policy"}, CreatedAt: time.Now().Add(time.Hour)})
	if err != nil || !reflect.DeepEqual(first.RouteArtifact.Content, second.RouteArtifact.Content) {
		t.Fatal("compiler replay changed imported route content")
	}
}
