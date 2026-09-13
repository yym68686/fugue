package api

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/store"
)

func TestPlatformPolicyLKGStorageFailureIsUnavailable(t *testing.T) {
	for name, state := range map[string]string{
		"unreadable state": "{",
		"missing artifact": `{"platform_lkg_snapshots":[{"id":"policy-lkg","artifact_kind":"policy_snapshot","scope_key":"global","artifact_id":"missing-policy"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
			path := filepath.Join(t.TempDir(), "state.json")
			if err := os.WriteFile(path, []byte(state), 0600); err != nil {
				t.Fatal(err)
			}
			// Authentication keeps its healthy store. Only the serving state
			// lookup fails, which must not be interpreted as absent policy.
			server.store = store.New(path)
			response := performJSONRequest(t, server, http.MethodGet, "http://localhost/v1/admin/platform-config/policy-lkg", admin, nil)
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("failed policy lookup must preserve the caller's LKG with 503: %d %s", response.Code, response.Body.String())
			}
			if strings.Contains(response.Body.String(), `"artifact":`) || strings.Contains(response.Body.String(), `"lkg":`) {
				t.Fatal("unusable recovery state must not be returned as a policy")
			}
		})
	}
}
