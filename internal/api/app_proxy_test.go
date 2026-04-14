package api

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

type fakeServiceResolver struct {
	ips map[string][]net.IPAddr
}

type countingServiceResolver struct {
	fakeServiceResolver
	lookupCount map[string]int
}

func (f fakeServiceResolver) LookupCNAME(context.Context, string) (string, error) {
	return "", nil
}

func (f fakeServiceResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	if addrs, ok := f.ips[host]; ok {
		return addrs, nil
	}
	return nil, &net.DNSError{IsNotFound: true}
}

func (f *countingServiceResolver) LookupCNAME(context.Context, string) (string, error) {
	return "", nil
}

func (f *countingServiceResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	if f.lookupCount == nil {
		f.lookupCount = map[string]int{}
	}
	f.lookupCount[host]++
	return f.fakeServiceResolver.LookupIPAddr(ctx, host)
}

func TestServiceURLForAppPrefersIDScopedService(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Route: &model.AppRoute{
			ServicePort: 8080,
		},
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	host := "app-demo." + namespace + ".svc.cluster.local"
	server := &Server{
		dnsResolver: fakeServiceResolver{
			ips: map[string][]net.IPAddr{
				host: {
					{IP: net.ParseIP("10.0.0.10")},
				},
			},
		},
	}

	got := server.serviceURLForApp(context.Background(), app)
	want := "http://" + host + ":8080"
	if got != want {
		t.Fatalf("expected service url %q, got %q", want, got)
	}
}

func TestServiceURLForAppFallsBackToLegacyServiceDuringMigration(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Route: &model.AppRoute{
			ServicePort: 8080,
		},
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	legacyHost := "demo." + namespace + ".svc.cluster.local"
	server := &Server{
		dnsResolver: fakeServiceResolver{
			ips: map[string][]net.IPAddr{
				legacyHost: {
					{IP: net.ParseIP("10.0.0.20")},
				},
			},
		},
	}

	got := server.serviceURLForApp(context.Background(), app)
	want := "http://" + legacyHost + ":8080"
	if got != want {
		t.Fatalf("expected legacy service url %q, got %q", want, got)
	}
}

func TestServiceURLForAppCachesResolvedServiceHost(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Route: &model.AppRoute{
			ServicePort: 8080,
		},
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	host := "app-demo." + namespace + ".svc.cluster.local"
	resolver := &countingServiceResolver{
		fakeServiceResolver: fakeServiceResolver{
			ips: map[string][]net.IPAddr{
				host: {
					{IP: net.ParseIP("10.0.0.10")},
				},
			},
		},
	}
	server := &Server{
		dnsResolver:              resolver,
		appProxyServiceHostCache: newExpiringResponseCache[string](time.Minute),
	}

	first := server.serviceURLForApp(context.Background(), app)
	second := server.serviceURLForApp(context.Background(), app)
	want := "http://" + host + ":8080"
	if first != want || second != want {
		t.Fatalf("expected cached service url %q, got first=%q second=%q", want, first, second)
	}
	if got := resolver.lookupCount[host]; got != 1 {
		t.Fatalf("expected one DNS lookup for %q, got %d", host, got)
	}
}

func TestLoadAppByHostnameCachedUsesShortTTLCache(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "apps.example.com")
	server.appProxyAppCache = newExpiringResponseCache[model.App](time.Minute)

	first, err := server.loadAppByHostnameCached("demo.apps.example.com")
	if err != nil {
		t.Fatalf("load app by hostname: %v", err)
	}
	if first.Route == nil || first.Route.ServicePort != 8080 {
		t.Fatalf("expected first lookup to use port 8080, got %+v", first.Route)
	}

	if _, err := storeState.UpdateAppRoute(app.ID, model.AppRoute{
		Hostname:    "demo.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://demo.apps.example.com",
		ServicePort: 9090,
	}); err != nil {
		t.Fatalf("update app route: %v", err)
	}

	second, err := server.loadAppByHostnameCached("demo.apps.example.com")
	if err != nil {
		t.Fatalf("reload cached app by hostname: %v", err)
	}
	if second.Route == nil || second.Route.ServicePort != 8080 {
		t.Fatalf("expected cached lookup to keep port 8080, got %+v", second.Route)
	}

	server.appProxyAppCache.clear("demo.apps.example.com")

	third, err := server.loadAppByHostnameCached("demo.apps.example.com")
	if err != nil {
		t.Fatalf("reload uncached app by hostname: %v", err)
	}
	if third.Route == nil || third.Route.ServicePort != 9090 {
		t.Fatalf("expected refreshed lookup to use port 9090, got %+v", third.Route)
	}
}

func TestMaybeHandleAppProxyUsesCustomDomainLookup(t *testing.T) {
	t.Parallel()

	storeState, server, _, platformAdminKey, app, resolver := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	expectedTarget := server.primaryCustomDomainTarget(app)
	resolver.cname["fugue.pro"] = expectedTarget + "."

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", platformAdminKey, map[string]any{
		"hostname": "fugue.pro",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected domain attach status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	loaded, err := server.loadAppByHostnameCached("fugue.pro")
	if err != nil {
		t.Fatalf("load custom-domain app by hostname: %v", err)
	}
	if loaded.ID != app.ID {
		t.Fatalf("expected custom-domain lookup to resolve app %q, got %q", app.ID, loaded.ID)
	}

	if _, err := storeState.GetAppDomain("fugue.pro"); err != nil {
		t.Fatalf("expected custom domain to be stored: %v", err)
	}
}
