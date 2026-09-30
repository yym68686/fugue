package api

import (
	"fugue/internal/model"
	"net/http"
	"testing"
)

func TestPlatformArtifactListAcceptsExactGenerationFilter(t *testing.T) {
	state, server, tenant, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	for _, generation := range []string{"target-generation", "later-generation"} {
		_, err := state.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPlatformIntent, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: "authority-cell:cell-filter"}, Generation: generation, Content: map[string]any{"fixture": generation}})
		if err != nil {
			t.Fatal(err)
		}
	}
	path := "/v1/admin/artifacts?kind=platform_intent&scope=authority-cell:cell-filter&generation=target-generation&limit=1"
	response := performJSONRequest(t, server, http.MethodGet, path, admin, nil)
	if response.Code != http.StatusOK {
		t.Fatal(response.Code, response.Body.String())
	}
	var result model.PlatformArtifactListResponse
	mustDecodeJSON(t, response, &result)
	if len(result.Artifacts) != 1 || result.Artifacts[0].Generation != "target-generation" || result.Artifacts[0].Content["fixture"] != "target-generation" {
		t.Fatal("generation query was ignored or full content lost")
	}
	response = performJSONRequest(t, server, http.MethodGet, path, tenant, nil)
	if response.Code != http.StatusForbidden {
		t.Fatal("generation query changed artifact read authorization", response.Code)
	}
}
