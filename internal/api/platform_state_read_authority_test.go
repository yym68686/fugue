package api

import (
	"net/http"
	"testing"
)

func TestPlatformStateReadRequiresArtifactAuthority(t *testing.T) {
	s, server, _, admin, app, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	const path = "/v1/platform-state/artifacts/release_set"
	for _, tc := range []struct {
		scope string
		want  int
	}{{"app.read", 403}, {"artifact.read", 200}} {
		_, key, err := s.CreateAPIKey(app.TenantID, tc.scope, []string{tc.scope})
		if err != nil {
			t.Fatal(err)
		}
		r := performJSONRequest(t, server, http.MethodGet, path, key, nil)
		if r.Code != tc.want {
			t.Fatalf("%s: %d", tc.scope, r.Code)
		}
	}
	if r := performJSONRequest(t, server, http.MethodGet, path, admin, nil); r.Code != 200 {
		t.Fatal(r.Code)
	}
}
