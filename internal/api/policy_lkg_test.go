package api

import (
	"net/http"
	"strings"
	"testing"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
)

func TestPlatformPolicyLKGStorageFailureIsUnavailable(t *testing.T) {
	for _, name := range []string{"failed SQL lookup", "unreadable artifact"} {
		t.Run(name, func(t *testing.T) {
			_, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
			// Authentication keeps its healthy store. Only the serving state
			// lookup fails, which must not be interpreted as absent policy.
			const fixture = "policy-lkg-failure"
			server.store = newStoreTestDatabase(t, fixture)
			db := openStoreTestSQL(t, fixture)
			if name == "failed SQL lookup" {
				if _, err := db.Exec(`ALTER TABLE fugue_platform_lkg_snapshots RENAME TO test_unavailable_lkg_snapshots`); err != nil {
					t.Fatal(err)
				}
			} else {
				server.store.ConfigurePlatformArtifactSigning(bundleauth.Keyring{PrimaryKey: "synthetic-policy-lkg-key", PrimaryKeyID: "test"})
				artifact, err := server.store.CreatePlatformArtifact(model.PlatformArtifact{
					ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{Key: "global"},
					Generation: "policy-lkg-test", Content: map[string]any{"test": true},
				})
				if err != nil {
					t.Fatal(err)
				}
				// The foreign key prevents a dangling artifact. A malformed
				// artifact body still exercises failure after LKG lookup.
				if _, err := db.Exec(`
INSERT INTO fugue_platform_lkg_snapshots (id, artifact_id, artifact_kind, scope_key, generation, content_hash, expires_at, created_at, updated_at)
VALUES ('policy-lkg', $1, $2, 'global', $3, $4, now()+interval '1 hour', now(), now())`,
					artifact.ID, artifact.ArtifactKind, artifact.Generation, artifact.ContentHash); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`UPDATE fugue_platform_artifacts SET content_json='[]'::jsonb, content_ref_hash=NULL WHERE id=$1`, artifact.ID); err != nil {
					t.Fatal(err)
				}
			}
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
