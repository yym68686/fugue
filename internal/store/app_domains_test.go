package store

import (
	"errors"
	"path/filepath"
	"testing"

	"fugue/internal/model"
)

func TestVerifiedAppDomainResolvesThroughGetAppByHostname(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Custom Domains")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
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

	_, err = s.PutAppDomain(model.AppDomain{
		Hostname:             "www.example.com",
		AppID:                app.ID,
		TenantID:             tenant.ID,
		Status:               model.AppDomainStatusPending,
		VerificationTXTName:  "_fugue-challenge.www.example.com",
		VerificationTXTValue: "token",
		RouteTarget:          "api.example.com",
	})
	if err != nil {
		t.Fatalf("put pending app domain: %v", err)
	}
	if _, err := s.GetAppByHostname("www.example.com"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected pending domain lookup to fail, got %v", err)
	}

	domain, err := s.GetAppDomain("www.example.com")
	if err != nil {
		t.Fatalf("get app domain: %v", err)
	}
	domain.Status = model.AppDomainStatusVerified
	if _, err := s.PutAppDomain(domain); err != nil {
		t.Fatalf("verify app domain: %v", err)
	}

	found, err := s.GetAppByHostname("www.example.com")
	if err != nil {
		t.Fatalf("lookup verified app domain: %v", err)
	}
	if found.ID != app.ID {
		t.Fatalf("expected app %s, got %s", app.ID, found.ID)
	}
}

func TestDeleteAppDomainReleasesHostname(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Delete Domains")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
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
	if _, err := s.PutAppDomain(model.AppDomain{
		Hostname:             "www.example.com",
		AppID:                app.ID,
		TenantID:             tenant.ID,
		Status:               model.AppDomainStatusVerified,
		VerificationTXTName:  "_fugue-challenge.www.example.com",
		VerificationTXTValue: "token",
		RouteTarget:          "api.example.com",
	}); err != nil {
		t.Fatalf("put app domain: %v", err)
	}

	if _, err := s.DeleteAppDomain(app.ID, "www.example.com"); err != nil {
		t.Fatalf("delete app domain: %v", err)
	}
	if _, err := s.GetAppDomain("www.example.com"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected app domain to be deleted, got %v", err)
	}
	if _, err := s.GetAppByHostname("www.example.com"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected hostname to be released, got %v", err)
	}
}
