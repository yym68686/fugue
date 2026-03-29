package api

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestPutAppDomainVerifiesWithCNAMEOnly(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app, resolver := setupAppDomainTestServer(t)
	expectedTarget := server.primaryCustomDomainTarget(app)
	resolver.cname["www.example.com"] = expectedTarget + "."

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", apiKey, map[string]any{
		"hostname": "www.example.com",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var putResponse struct {
		Domain         model.AppDomain       `json:"domain"`
		Availability   appDomainAvailability `json:"availability"`
		AlreadyCurrent bool                  `json:"already_current"`
	}
	mustDecodeJSON(t, recorder, &putResponse)
	if putResponse.AlreadyCurrent {
		t.Fatal("expected new app domain to be created")
	}
	if got := putResponse.Domain.RouteTarget; got != expectedTarget {
		t.Fatalf("expected route target %q, got %q", expectedTarget, got)
	}
	if putResponse.Domain.VerificationTXTName != "" || putResponse.Domain.VerificationTXTValue != "" {
		t.Fatalf("expected CNAME-only verification, got %+v", putResponse.Domain)
	}
	if putResponse.Domain.Status != model.AppDomainStatusVerified {
		t.Fatalf("expected verified domain status, got %+v", putResponse.Domain)
	}

	found, err := s.GetAppByHostname("www.example.com")
	if err != nil {
		t.Fatalf("lookup verified custom domain: %v", err)
	}
	if found.ID != app.ID {
		t.Fatalf("expected app %s, got %s", app.ID, found.ID)
	}
}

func TestPutAppDomainVerifiesWithFlattenedTargetIPs(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app, resolver := setupAppDomainTestServer(t)
	expectedTarget := server.primaryCustomDomainTarget(app)
	edgeIP := net.ParseIP("203.0.113.10")
	resolver.ip["example.com"] = []net.IPAddr{{IP: edgeIP}}
	resolver.ip[expectedTarget] = []net.IPAddr{{IP: edgeIP}}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", apiKey, map[string]any{
		"hostname": "example.com",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var putResponse struct {
		Domain model.AppDomain `json:"domain"`
	}
	mustDecodeJSON(t, recorder, &putResponse)
	if got := putResponse.Domain.RouteTarget; got != expectedTarget {
		t.Fatalf("expected route target %q, got %q", expectedTarget, got)
	}
	if putResponse.Domain.Status != model.AppDomainStatusVerified {
		t.Fatalf("expected verified domain status, got %+v", putResponse.Domain)
	}
}

func TestPutAppDomainRequiresCNAMEBeforeCreatingClaim(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app, _ := setupAppDomainTestServer(t)
	expectedTarget := server.primaryCustomDomainTarget(app)

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", apiKey, map[string]any{
		"hostname": "www.example.com",
	})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "CNAME") || !strings.Contains(recorder.Body.String(), expectedTarget) {
		t.Fatalf("expected CNAME guidance in response, got body=%s", recorder.Body.String())
	}
	if _, err := s.GetAppDomain("www.example.com"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected unrouted hostname to remain unclaimed, got %v", err)
	}
}

func TestCustomDomainTargetStaysStableWhenAppRouteChanges(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app, resolver := setupAppDomainTestServer(t)
	expectedTarget := server.primaryCustomDomainTarget(app)
	updatedApp, err := s.UpdateAppRoute(app.ID, model.AppRoute{
		Hostname:    "renamed.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://renamed.apps.example.com",
		ServicePort: 8080,
	})
	if err != nil {
		t.Fatalf("update app route: %v", err)
	}
	if got := server.primaryCustomDomainTarget(updatedApp); got != expectedTarget {
		t.Fatalf("expected stable target %q after route change, got %q", expectedTarget, got)
	}
	resolver.cname["www.example.com"] = expectedTarget + "."

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+updatedApp.ID+"/domains", apiKey, map[string]any{
		"hostname": "www.example.com",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var putResponse struct {
		Domain model.AppDomain `json:"domain"`
	}
	mustDecodeJSON(t, recorder, &putResponse)
	if got := putResponse.Domain.RouteTarget; got != expectedTarget {
		t.Fatalf("expected stable route target %q, got %q", expectedTarget, got)
	}
}

func TestEdgeTLSAskAutoVerifiesPendingDomain(t *testing.T) {
	t.Parallel()

	s, server, _, app, resolver := setupAppDomainTestServer(t)
	if _, err := s.PutAppDomain(model.AppDomain{
		Hostname:    "www.example.com",
		AppID:       app.ID,
		TenantID:    app.TenantID,
		Status:      model.AppDomainStatusPending,
		RouteTarget: "demo.apps.example.com",
	}); err != nil {
		t.Fatalf("put pending app domain: %v", err)
	}
	resolver.cname["www.example.com"] = "demo.apps.example.com."

	req := httptest.NewRequest(http.MethodGet, "/v1/edge/tls/ask?token=edge-secret&domain=www.example.com", nil)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	domain, err := s.GetAppDomain("www.example.com")
	if err != nil {
		t.Fatalf("get app domain after ask: %v", err)
	}
	if domain.Status != model.AppDomainStatusVerified {
		t.Fatalf("expected app domain to be auto-verified, got %+v", domain)
	}
}

func setupAppDomainTestServer(t *testing.T) (*store.Store, *Server, string, model.App, *fakeAppDomainResolver) {
	t.Helper()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("App Domain Tenant")
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
	app, err := s.CreateAppWithRoute(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppRoute{
		Hostname:    "demo.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://demo.apps.example.com",
		ServicePort: 8080,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		AppBaseDomain:          "apps.example.com",
		CustomDomainBaseDomain: "cname.fugue.pro",
		APIPublicDomain:        "api.example.com",
		EdgeTLSAskToken:        "edge-secret",
	})
	resolver := &fakeAppDomainResolver{
		cname: map[string]string{},
		ip:    map[string][]net.IPAddr{},
	}
	server.dnsResolver = resolver
	return s, server, apiKey, app, resolver
}

type fakeAppDomainResolver struct {
	cname map[string]string
	ip    map[string][]net.IPAddr
}

func (f *fakeAppDomainResolver) LookupCNAME(_ context.Context, host string) (string, error) {
	if value, ok := f.cname[normalizeExternalAppDomain(host)]; ok {
		return value, nil
	}
	return "", &net.DNSError{IsNotFound: true}
}

func (f *fakeAppDomainResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	if values, ok := f.ip[normalizeExternalAppDomain(host)]; ok {
		return values, nil
	}
	return nil, &net.DNSError{IsNotFound: true}
}
