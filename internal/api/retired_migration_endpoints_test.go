package api

import (
	"net/http"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestRetiredMigrationEndpointsNeverReadOrPublishLegacyConfiguration(t *testing.T) {
	state, s, tenant, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	s.platformRoutes = []model.PlatformRoute{{Hostname: "legacy.example.test", UpstreamURL: "http://old:8080"}}
	s.dnsStaticRecords = []model.EdgeDNSRecord{{Name: "legacy.example.test", Type: "A", Values: []string{"8.8.8.8"}, TTL: 60}}
	before, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v1/admin/platform-config/dns/compare", "/v1/admin/platform-config/routes/compare"} {
		for _, tc := range []struct {
			token  string
			status int
		}{{"", 401}, {tenant, 403}, {admin, 410}} {
			r := performJSONRequest(t, s, http.MethodGet, path+"?artifact_id=missing&node_id=missing&zone=example.test", tc.token, nil)
			if r.Code != tc.status || strings.Contains(r.Body.String(), "legacy.example.test") || r.Header().Get("ETag") != "" {
				t.Fatal(path, r.Code, r.Body.String())
			}
		}
		r := performJSONRequest(t, s, http.MethodGet, path, admin, nil)
		if r.Code != 410 {
			t.Fatal("retired endpoint still reads comparison input", path, r.Code)
		}
	}
	after, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("retired comparison mutated artifact store", err)
	}
	for _, kind := range []string{model.PlatformArtifactKindReleaseSet, model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle} {
		lkg, err := state.GetPlatformLKG(kind, "global")
		if err != nil || lkg != nil {
			t.Fatal("retired comparison created LKG", err)
		}
	}
}
