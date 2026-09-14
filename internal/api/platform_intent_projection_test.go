package api

import (
	"net/http"
	"testing"

	"fugue/internal/model"
)

func TestPlatformIntentProjectionRequiresPlatformAdmin(t *testing.T) {
	state, server, tenant, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	for _, test := range []struct {
		name, token string
		status      int
	}{{"anonymous", "", http.StatusUnauthorized}, {"tenant", tenant, http.StatusForbidden}} {
		t.Run(test.name, func(t *testing.T) {
			response := performJSONRequest(t, server, http.MethodGet, "/v1/admin/platform-config/routes/project", test.token, nil)
			if response.Code != test.status {
				t.Fatalf("expected %d got %d", test.status, response.Code)
			}
		})
	}
	before, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if err != nil {
		t.Fatal(err)
	}
	response := performJSONRequest(t, server, http.MethodGet, "/v1/admin/platform-config/routes/project", admin, nil)
	if response.Code != http.StatusOK && response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unexpected admin result %d %s", response.Code, response.Body.String())
	}
	after, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if err != nil || len(before) != len(after) {
		t.Fatal("projection modified artifacts")
	}
}
