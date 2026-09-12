package api

import (
	"net/http"
	"testing"

	"fugue/internal/model"
)

func TestEdgeTLSAskUsesVerifiedTLSArtifact(t *testing.T) {
	_, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	create := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts", platformAdminKey, model.PlatformArtifactCreateRequest{
		ArtifactKind: model.PlatformArtifactKindCaddyRouteConfig,
		Scope:        model.PlatformArtifactScope{ScopeType: "global"},
		Generation:   "tls-artifact-lkg-1",
		Content: map[string]any{
			"routes": []any{map[string]any{"hostname": "artifact.fugue.pro", "upstream_url": "http://artifact:8080"}},
			"certificates": []any{map[string]any{
				"hostname": "artifact.fugue.pro", "policy": "managed",
			}},
		},
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("create TLS artifact: %d %s", create.Code, create.Body.String())
	}
	var created model.PlatformArtifactResponse
	mustDecodeJSON(t, create, &created)
	validate := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+created.Artifact.ID+"/validate", platformAdminKey, map[string]any{"dry_run": false})
	if validate.Code != http.StatusOK {
		t.Fatalf("validate TLS artifact: %d %s", validate.Code, validate.Body.String())
	}
	seedVerifiedPlatformArtifactAPI(t, server, platformAdminKey, created.Artifact.ID)
	allowed, found, err := server.edgeTLSAskFromVerifiedArtifact("artifact.fugue.pro")
	if err != nil || !found || !allowed {
		t.Fatalf("expected verified TLS artifact allow, allowed=%v found=%v err=%v", allowed, found, err)
	}
	allowed, found, err = server.edgeTLSAskFromVerifiedArtifact("missing.fugue.pro")
	if err != nil || !found || allowed {
		t.Fatalf("expected verified TLS artifact deny, allowed=%v found=%v err=%v", allowed, found, err)
	}
}
