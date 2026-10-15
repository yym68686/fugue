package api

import (
	"fugue/internal/model"
	"net/http"
	"strings"
	"testing"
)

func TestHostnameLineageDefaultsToBoundedMetadata(t *testing.T) {
	st, server, tenantKey, adminKey, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	artifact, err := st.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, Generation: "lineage-fixture", Content: map[string]any{"rows": []any{map[string]any{"origin_HOSTNAME": " .Target.example. "}}, "payload": strings.Repeat("x", 1<<20)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.ValidatePlatformArtifact(artifact.ID, []model.PlatformArtifactValidationResult{{Name: "schema", Pass: true, Severity: model.RobustnessSeverityInfo}}); err != nil {
		t.Fatal(err)
	}
	path := "/v1/admin/platform-config/hostname-lineage?hostname=target.example"
	for _, test := range []struct {
		query  string
		status int
		body   bool
	}{{"", 200, false}, {"&include_content=true", 200, true}, {"&limit=201", 400, false}, {"&limit=0", 400, false}, {"&include_content=nope", 400, false}, {"&include_content=true&limit=6", 400, false}} {
		r := performJSONRequest(t, server, http.MethodGet, path+test.query, adminKey, nil)
		if r.Code != test.status {
			t.Fatalf("%s: %d %s", test.query, r.Code, r.Body.String())
		}
		if test.status != 200 {
			continue
		}
		var response struct {
			Artifacts []platformConfigLineageDependency `json:"artifacts"`
			Limit     int                               `json:"candidate_limit_per_kind"`
			Content   bool                              `json:"content_included"`
		}
		mustDecodeJSON(t, r, &response)
		if len(response.Artifacts) != 1 || response.Content != test.body || (response.Artifacts[0].Artifact.Content != nil) != test.body {
			t.Fatal("lineage content policy changed", test.query)
		}
		if !test.body && r.Body.Len() > 10000 {
			t.Fatal("large artifact leaked into metadata response")
		}
	}
	if r := performJSONRequest(t, server, http.MethodGet, path, tenantKey, nil); r.Code != http.StatusForbidden {
		t.Fatal("admin authorization changed", r.Code)
	}
}
