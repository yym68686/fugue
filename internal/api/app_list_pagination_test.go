package api

import (
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"testing"

	"fugue/internal/model"
)

func TestListAppsOptInPaginationIsStableAndLegacyCompatible(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, firstApp := setupSearchTestServer(t)
	for _, name := range []string{"alpha", "bravo", "charlie", "delta", "echo"} {
		if _, err := stateStore.CreateApp(firstApp.TenantID, firstApp.ProjectID, name, "", model.AppSpec{
			Image:    "ghcr.io/example/" + name + ":latest",
			Ports:    []int{8080},
			Replicas: 1,
		}); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}

	legacy := performJSONRequest(t, server, http.MethodGet, "/v1/apps?include_resource_usage=false", apiKey, nil)
	if legacy.Code != http.StatusOK {
		t.Fatalf("legacy list status %d body=%s", legacy.Code, legacy.Body.String())
	}
	var legacyPayload map[string]json.RawMessage
	if err := json.Unmarshal(legacy.Body.Bytes(), &legacyPayload); err != nil {
		t.Fatalf("decode legacy response: %v", err)
	}
	if _, present := legacyPayload["page_info"]; present {
		t.Fatal("legacy unpaginated response unexpectedly includes page_info")
	}

	type response struct {
		Apps []model.App     `json:"apps"`
		Page appListPageInfo `json:"page_info"`
	}
	seen := make(map[string]struct{})
	cursor := ""
	firstNextCursor := ""
	firstPageIDs := make([]string, 0, 2)
	for {
		target := "/v1/apps?include_resource_usage=false&limit=2&sort=name_asc"
		if cursor != "" {
			target += "&cursor=" + url.QueryEscape(cursor)
		}
		recorder := performJSONRequest(t, server, http.MethodGet, target, apiKey, nil)
		if recorder.Code != http.StatusOK {
			t.Fatalf("page status %d body=%s", recorder.Code, recorder.Body.String())
		}
		var payload response
		if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
			t.Fatalf("decode page: %v", err)
		}
		if len(payload.Apps) > 2 {
			t.Fatalf("page exceeded limit: %d", len(payload.Apps))
		}
		if payload.Page.TotalItems != 6 {
			t.Fatalf("unexpected total %d", payload.Page.TotalItems)
		}
		for _, app := range payload.Apps {
			if _, duplicate := seen[app.ID]; duplicate {
				t.Fatalf("duplicate app %s", app.ID)
			}
			seen[app.ID] = struct{}{}
		}
		if cursor == "" {
			for _, app := range payload.Apps {
				firstPageIDs = append(firstPageIDs, app.ID)
			}
		}
		if !payload.Page.HasNextPage {
			break
		}
		if payload.Page.NextCursor == "" {
			t.Fatal("has_next_page without next_cursor")
		}
		if firstNextCursor == "" {
			firstNextCursor = payload.Page.NextCursor
		}
		cursor = payload.Page.NextCursor
	}
	if len(seen) != 6 {
		t.Fatalf("pagination omitted apps: got %d", len(seen))
	}
	secondRecorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps?include_resource_usage=false&limit=2&sort=name_asc&cursor="+url.QueryEscape(firstNextCursor), apiKey, nil)
	if secondRecorder.Code != http.StatusOK {
		t.Fatalf("second page status %d body=%s", secondRecorder.Code, secondRecorder.Body.String())
	}
	var secondPage response
	if err := json.Unmarshal(secondRecorder.Body.Bytes(), &secondPage); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if secondPage.Page.PreviousCursor == "" {
		t.Fatal("second page is missing previous_cursor")
	}
	previousRecorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps?include_resource_usage=false&limit=2&sort=name_asc&cursor="+url.QueryEscape(secondPage.Page.PreviousCursor), apiKey, nil)
	if previousRecorder.Code != http.StatusOK {
		t.Fatalf("previous page status %d body=%s", previousRecorder.Code, previousRecorder.Body.String())
	}
	var previousPage response
	if err := json.Unmarshal(previousRecorder.Body.Bytes(), &previousPage); err != nil {
		t.Fatalf("decode previous page: %v", err)
	}
	previousIDs := make([]string, 0, len(previousPage.Apps))
	for _, app := range previousPage.Apps {
		previousIDs = append(previousIDs, app.ID)
	}
	if !slices.Equal(previousIDs, firstPageIDs) {
		t.Fatalf("previous page changed: got %v want %v", previousIDs, firstPageIDs)
	}

	invalid := performJSONRequest(t, server, http.MethodGet, "/v1/apps?include_resource_usage=false&limit=2&sort=name_asc&q=changed&cursor="+url.QueryEscape(cursor), apiKey, nil)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("filter-bound cursor status %d body=%s", invalid.Code, invalid.Body.String())
	}
}

func TestListAppsPaginationRejectsOutOfRangeLimit(t *testing.T) {
	t.Parallel()

	_, server, apiKey, _ := setupSearchTestServer(t)
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps?limit=201", apiKey, nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", recorder.Code, recorder.Body.String())
	}
}
