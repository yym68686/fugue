package api

import (
	"net/http"
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestAppListSummaryPreservesResponseAndPagination(t *testing.T) {
	_, server, key, app := setupSearchTestServer(t)
	for _, pagination := range []string{"", "&limit=1"} {
		path := "/v1/apps?include_resource_usage=false&include_live_status=false" + pagination
		fullResponse := performJSONRequest(t, server, http.MethodGet, path, key, nil)
		summaryResponse := performJSONRequest(t, server, http.MethodGet, path+"&view=summary", key, nil)
		if fullResponse.Code != 200 || summaryResponse.Code != 200 {
			t.Fatalf("list status full=%d summary=%d", fullResponse.Code, summaryResponse.Code)
		}
		var full, summary struct {
			Apps []model.App `json:"apps"`
			Page any         `json:"page_info"`
		}
		mustDecodeJSON(t, fullResponse, &full)
		mustDecodeJSON(t, summaryResponse, &summary)
		if len(full.Apps) != 1 || full.Apps[0].ID != app.ID {
			t.Fatal("fixture app missing")
		}
		full.Apps[0] = store.AppReadSummary(full.Apps[0])
		if !reflect.DeepEqual(full, summary) {
			t.Fatal("summary changed retained response data")
		}
	}
	bad := performJSONRequest(t, server, http.MethodGet, "/v1/apps?view=unknown", key, nil)
	if bad.Code != http.StatusBadRequest {
		t.Fatal("unknown view accepted")
	}
}
