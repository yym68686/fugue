package api

import (
	"errors"
	"net/http"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestGetAppRouteAvailabilityReportsCurrentConflictAndInvalid(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app, occupiedApp := setupAppRouteTestServer(t)

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/route/availability?hostname=demo", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var currentResponse struct {
		Availability appRouteAvailability `json:"availability"`
	}
	mustDecodeJSON(t, recorder, &currentResponse)
	if !currentResponse.Availability.Valid || !currentResponse.Availability.Available || !currentResponse.Availability.Current {
		t.Fatalf("expected current hostname to be valid/current/available, got %+v", currentResponse.Availability)
	}
	if got := currentResponse.Availability.Hostname; got != "demo.apps.example.com" {
		t.Fatalf("expected normalized hostname demo.apps.example.com, got %q", got)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/route/availability?hostname=taken.apps.example.com", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var occupiedResponse struct {
		Availability appRouteAvailability `json:"availability"`
	}
	mustDecodeJSON(t, recorder, &occupiedResponse)
	if !occupiedResponse.Availability.Valid {
		t.Fatalf("expected occupied hostname to be syntactically valid, got %+v", occupiedResponse.Availability)
	}
	if occupiedResponse.Availability.Available || occupiedResponse.Availability.Current {
		t.Fatalf("expected occupied hostname to be unavailable, got %+v", occupiedResponse.Availability)
	}
	if occupiedResponse.Availability.Hostname != occupiedApp.Route.Hostname {
		t.Fatalf("expected occupied hostname %q, got %q", occupiedApp.Route.Hostname, occupiedResponse.Availability.Hostname)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/route/availability?hostname=bad_name", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var invalidResponse struct {
		Availability appRouteAvailability `json:"availability"`
	}
	mustDecodeJSON(t, recorder, &invalidResponse)
	if invalidResponse.Availability.Valid || invalidResponse.Availability.Available {
		t.Fatalf("expected invalid hostname to be rejected, got %+v", invalidResponse.Availability)
	}
	if invalidResponse.Availability.Reason == "" {
		t.Fatal("expected invalid hostname reason")
	}
}

func TestAppRouteAvailabilityAllowsVerifiedCustomDomainPathSplit(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app, occupiedApp := setupAppRouteTestServer(t)

	domainHostname := "api2.example.com"
	domain, err := s.PutAppDomain(model.AppDomain{
		Hostname:    domainHostname,
		AppID:       app.ID,
		TenantID:    app.TenantID,
		Status:      model.AppDomainStatusVerified,
		DNSStatus:   model.AppDomainDNSStatusReady,
		TLSStatus:   model.AppDomainTLSStatusReady,
		RouteTarget: "demo.apps.example.com",
	})
	if err != nil {
		t.Fatalf("put custom domain: %v", err)
	}
	if domain.Status != model.AppDomainStatusVerified {
		t.Fatalf("expected verified custom domain, got %+v", domain)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+occupiedApp.ID+"/route/availability?hostname="+domainHostname+"&path_prefix=/v1", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var splitResponse struct {
		Availability appRouteAvailability `json:"availability"`
	}
	mustDecodeJSON(t, recorder, &splitResponse)
	if !splitResponse.Availability.Valid || !splitResponse.Availability.Available || splitResponse.Availability.Current {
		t.Fatalf("expected split route to be valid and available, got %+v", splitResponse.Availability)
	}
	if splitResponse.Availability.Hostname != domainHostname {
		t.Fatalf("expected hostname %q, got %q", domainHostname, splitResponse.Availability.Hostname)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+occupiedApp.ID+"/route/availability?hostname="+domainHostname+"&path_prefix=/", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	mustDecodeJSON(t, recorder, &splitResponse)
	if splitResponse.Availability.Valid || splitResponse.Availability.Available {
		t.Fatalf("expected root custom-domain route to be rejected for a different app, got %+v", splitResponse.Availability)
	}
	if splitResponse.Availability.Reason == "" {
		t.Fatal("expected root custom-domain route rejection reason")
	}

	recorder = performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+occupiedApp.ID+"/route", apiKey, map[string]any{
		"hostname":    domainHostname,
		"path_prefix": "/v1",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var patchResponse struct {
		App            model.App            `json:"app"`
		Availability   appRouteAvailability `json:"availability"`
		AlreadyCurrent bool                 `json:"already_current"`
	}
	mustDecodeJSON(t, recorder, &patchResponse)
	if patchResponse.AlreadyCurrent {
		t.Fatal("expected first split route patch to change hostname")
	}
	if patchResponse.App.Route == nil {
		t.Fatal("expected app route in patch response")
	}
	if got := patchResponse.App.Route.Hostname; got != domainHostname {
		t.Fatalf("expected split route hostname %q, got %q", domainHostname, got)
	}
	if got := patchResponse.App.Route.PathPrefix; got != "/v1" {
		t.Fatalf("expected split route path prefix /v1, got %q", got)
	}

	found, err := s.GetAppByRoutePrefix(domainHostname, "/v1")
	if err != nil {
		t.Fatalf("lookup split route: %v", err)
	}
	if found.ID != occupiedApp.ID {
		t.Fatalf("expected split route to resolve to app %s, got %s", occupiedApp.ID, found.ID)
	}

	persistedDomain, err := s.GetAppDomain(domainHostname)
	if err != nil {
		t.Fatalf("lookup custom domain: %v", err)
	}
	if persistedDomain.AppID != app.ID {
		t.Fatalf("expected custom domain to remain attached to frontend app %s, got %s", app.ID, persistedDomain.AppID)
	}
}

func TestPatchAppRouteUpdatesHostnameAndIsIdempotent(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app, _ := setupAppRouteTestServer(t)

	recorder := performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID+"/route", apiKey, map[string]any{
		"hostname": "fresh-name",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var patchResponse struct {
		App            model.App            `json:"app"`
		Availability   appRouteAvailability `json:"availability"`
		AlreadyCurrent bool                 `json:"already_current"`
	}
	mustDecodeJSON(t, recorder, &patchResponse)
	if patchResponse.AlreadyCurrent {
		t.Fatal("expected first route patch to change hostname")
	}
	if patchResponse.App.Route == nil {
		t.Fatal("expected app route in patch response")
	}
	if got := patchResponse.App.Route.Hostname; got != "fresh-name.apps.example.com" {
		t.Fatalf("expected updated hostname fresh-name.apps.example.com, got %q", got)
	}
	if !patchResponse.Availability.Valid || !patchResponse.Availability.Available {
		t.Fatalf("expected patched hostname availability to be valid and available, got %+v", patchResponse.Availability)
	}

	found, err := s.GetAppByHostname("fresh-name.apps.example.com")
	if err != nil {
		t.Fatalf("lookup patched hostname: %v", err)
	}
	if found.ID != app.ID {
		t.Fatalf("expected patched hostname to resolve to app %s, got %s", app.ID, found.ID)
	}
	if _, err := s.GetAppByHostname("demo.apps.example.com"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected previous hostname to be released, got %v", err)
	}

	recorder = performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID+"/route", apiKey, map[string]any{
		"hostname": "fresh-name.apps.example.com",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	mustDecodeJSON(t, recorder, &patchResponse)
	if !patchResponse.AlreadyCurrent {
		t.Fatal("expected repeated route patch to be idempotent")
	}
}

func TestAppRouteEndpointsRejectBackgroundApps(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Background Route Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	app, err := s.CreateApp(tenant.ID, project.ID, "worker", "", model.AppSpec{
		Image:       "ghcr.io/example/worker:latest",
		NetworkMode: model.AppNetworkModeBackground,
		Replicas:    1,
		RuntimeID:   "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create background app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		AppBaseDomain:   "apps.example.com",
		APIPublicDomain: "api.example.com",
	})

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/route/availability?hostname=worker", apiKey, nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID+"/route", apiKey, map[string]any{
		"hostname": "worker",
	})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
}

func setupAppRouteTestServer(t *testing.T) (*store.Store, *Server, string, model.App, model.App) {
	t.Helper()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Route Test Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	spec := model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}
	app, err := s.CreateAppWithRoute(tenant.ID, project.ID, "demo", "", spec, model.AppRoute{
		Hostname:    "demo.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://demo.apps.example.com",
		ServicePort: 8080,
	})
	if err != nil {
		t.Fatalf("create primary app: %v", err)
	}
	occupiedApp, err := s.CreateAppWithRoute(tenant.ID, project.ID, "taken", "", spec, model.AppRoute{
		Hostname:    "taken.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://taken.apps.example.com",
		ServicePort: 8080,
	})
	if err != nil {
		t.Fatalf("create occupied app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		AppBaseDomain:   "apps.example.com",
		APIPublicDomain: "api.example.com",
	})
	return s, server, apiKey, app, occupiedApp
}
