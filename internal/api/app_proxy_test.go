package api

import (
	"context"
	"net"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

type fakeServiceResolver struct {
	ips map[string][]net.IPAddr
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
