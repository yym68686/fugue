package api

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestSearchResourcesFindsProjectAppAndDomain(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app := setupSearchTestServer(t)
	domain, err := stateStore.PutAppDomain(model.AppDomain{
		AppID:       app.ID,
		TenantID:    app.TenantID,
		Hostname:    "uni-api.example.com",
		Status:      model.AppDomainStatusVerified,
		DNSStatus:   model.AppDomainDNSStatusReady,
		TLSStatus:   model.AppDomainTLSStatusReady,
		RouteTarget: "uni-api-web-api.apps.example.com",
	})
	if err != nil {
		t.Fatalf("put app domain: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/search?q=uni-api&limit=20", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response model.SearchResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode search response: %v", err)
	}
	if !searchResponseHas(response, "project", "uni-api-web") {
		t.Fatalf("expected project match in %+v", response.Results)
	}
	appResult, ok := searchResponseFind(response, "app", app.ID)
	if !ok {
		t.Fatalf("expected app match in %+v", response.Results)
	}
	if want := appInternalURL(app); appResult.InternalURL != want {
		t.Fatalf("expected app internal_url %q, got %q", want, appResult.InternalURL)
	}
	if !searchResponseHas(response, "domain", domain.Hostname) {
		t.Fatalf("expected domain match in %+v", response.Results)
	}
}

func TestListAppsSupportsSearchAndProjectFilters(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app := setupSearchTestServer(t)
	_, platformKey, err := stateStore.CreateAPIKey(app.TenantID, "platform-admin", []string{"platform.admin"})
	if err != nil {
		t.Fatalf("create platform key: %v", err)
	}
	if _, err := stateStore.CreateApp(app.TenantID, app.ProjectID, "billing-worker", "", model.AppSpec{
		Image:    "ghcr.io/example/billing-worker:latest",
		Ports:    []int{8080},
		Replicas: 1,
	}); err != nil {
		t.Fatalf("create extra app: %v", err)
	}
	otherTenant, err := stateStore.CreateTenant("Other Discovery Tenant")
	if err != nil {
		t.Fatalf("create other tenant: %v", err)
	}
	otherProject, err := stateStore.CreateProject(otherTenant.ID, "uni-api-web", "")
	if err != nil {
		t.Fatalf("create other project: %v", err)
	}
	raiseManagedTestCap(t, stateStore, otherTenant.ID)
	if _, err := stateStore.CreateApp(otherTenant.ID, otherProject.ID, "uni-api-web-api", "", model.AppSpec{
		Image:    "ghcr.io/example/uni-api-web-api:latest",
		Ports:    []int{8080},
		Replicas: 1,
	}); err != nil {
		t.Fatalf("create other app: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps?tenant_id="+app.TenantID+"&project_id="+app.ProjectID+"&q=uni-api&include_live_status=false&include_resource_usage=false", platformKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Apps []model.App `json:"apps"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode app list: %v", err)
	}
	if len(response.Apps) != 1 || response.Apps[0].ID != app.ID {
		t.Fatalf("expected only filtered app %s, got %+v", app.ID, response.Apps)
	}

	tenantRecorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps?project_id="+app.ProjectID+"&q=uni-api&include_live_status=false&include_resource_usage=false", apiKey, nil)
	if tenantRecorder.Code != http.StatusOK {
		t.Fatalf("expected tenant status %d, got %d body=%s", http.StatusOK, tenantRecorder.Code, tenantRecorder.Body.String())
	}
}

func setupSearchTestServer(t *testing.T) (*store.Store, *Server, string, model.App) {
	t.Helper()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Discovery Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "uni-api-web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, stateStore, tenant.ID)
	_, apiKey, err := stateStore.CreateAPIKey(tenant.ID, "viewer", []string{"app.read"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "uni-api-web-api", "", model.AppSpec{
		Image:    "ghcr.io/example/uni-api-web-api:latest",
		Ports:    []int{8080},
		Replicas: 1,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	return stateStore, server, apiKey, app
}

func searchResponseHas(response model.SearchResponse, kind, idOrName string) bool {
	_, ok := searchResponseFind(response, kind, idOrName)
	return ok
}

func searchResponseFind(response model.SearchResponse, kind, idOrName string) (model.SearchResult, bool) {
	for _, result := range response.Results {
		if result.Kind == kind && (result.ID == idOrName || result.Name == idOrName) {
			return result, true
		}
	}
	return model.SearchResult{}, false
}
