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

func TestPutVerifiedAppDomainDefaultsTLSPending(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("TLS Domains")
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

	domain, err := s.PutAppDomain(model.AppDomain{
		Hostname:    "www.example.com",
		AppID:       app.ID,
		TenantID:    tenant.ID,
		Status:      model.AppDomainStatusVerified,
		RouteTarget: "d-123.dns.fugue.pro",
	})
	if err != nil {
		t.Fatalf("put verified app domain: %v", err)
	}
	if domain.TLSStatus != model.AppDomainTLSStatusPending {
		t.Fatalf("expected pending TLS status, got %+v", domain)
	}
	if domain.DNSStatus != model.AppDomainDNSStatusReady {
		t.Fatalf("expected DNS status ready for a verified domain, got %+v", domain)
	}
	if domain.DNSRecordKind != model.AppDomainDNSRecordKindCNAME {
		t.Fatalf("expected default DNS record kind cname, got %+v", domain)
	}
	if domain.TLSReadyAt != nil {
		t.Fatalf("expected TLS ready timestamp to be empty, got %+v", domain)
	}
}

func TestPutAppDomainAllowsSubpathRouteOnSameHostname(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("TLS Subpath Domains")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	web, err := s.CreateAppWithRoute(tenant.ID, project.ID, "web", "", model.AppSpec{
		Image:     "ghcr.io/example/web:latest",
		Ports:     []int{3000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppRoute{
		Hostname:    "web.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://web.apps.example.com",
		ServicePort: 3000,
	})
	if err != nil {
		t.Fatalf("create web app: %v", err)
	}
	domain, err := s.PutAppDomain(model.AppDomain{
		Hostname:    "www.example.com",
		AppID:       web.ID,
		TenantID:    tenant.ID,
		Status:      model.AppDomainStatusVerified,
		RouteTarget: "d-123.dns.fugue.pro",
	})
	if err != nil {
		t.Fatalf("put verified app domain: %v", err)
	}
	if _, err := s.CreateAppWithRoute(tenant.ID, project.ID, "api", "", model.AppSpec{
		Image:     "ghcr.io/example/api:latest",
		Ports:     []int{8000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppRoute{
		Hostname:    "www.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://www.example.com/v1",
		PathPrefix:  "/v1",
		ServicePort: 8000,
	}); err != nil {
		t.Fatalf("create subpath app route: %v", err)
	}

	domain.TLSStatus = model.AppDomainTLSStatusReady
	domain.TLSLastMessage = ""
	if _, err := s.PutAppDomain(domain); err != nil {
		t.Fatalf("update app domain TLS fields with subpath route on same hostname: %v", err)
	}
}

func TestRootAppRouteRejectsVerifiedAppDomainHostname(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Root Domain Conflicts")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	web, err := s.CreateAppWithRoute(tenant.ID, project.ID, "web", "", model.AppSpec{
		Image:     "ghcr.io/example/web:latest",
		Ports:     []int{3000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppRoute{
		Hostname:    "web.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://web.apps.example.com",
		ServicePort: 3000,
	})
	if err != nil {
		t.Fatalf("create web app: %v", err)
	}
	if _, err := s.PutAppDomain(model.AppDomain{
		Hostname:    "www.example.com",
		AppID:       web.ID,
		TenantID:    tenant.ID,
		Status:      model.AppDomainStatusVerified,
		RouteTarget: "d-123.dns.fugue.pro",
	}); err != nil {
		t.Fatalf("put verified app domain: %v", err)
	}

	_, err = s.CreateAppWithRoute(tenant.ID, project.ID, "root-api", "", model.AppSpec{
		Image:     "ghcr.io/example/api:latest",
		Ports:     []int{8000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppRoute{
		Hostname:    "www.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://www.example.com",
		PathPrefix:  "/",
		ServicePort: 8000,
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict for root app route on verified custom domain hostname, got %v", err)
	}

	api, err := s.CreateAppWithRoute(tenant.ID, project.ID, "api", "", model.AppSpec{
		Image:     "ghcr.io/example/api:latest",
		Ports:     []int{8000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppRoute{
		Hostname:    "api.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://api.apps.example.com",
		ServicePort: 8000,
	})
	if err != nil {
		t.Fatalf("create api app: %v", err)
	}
	_, err = s.UpdateAppRoute(api.ID, model.AppRoute{
		Hostname:    "www.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://www.example.com",
		PathPrefix:  "/",
		ServicePort: 8000,
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict for updating root route onto verified custom domain hostname, got %v", err)
	}
}

func TestDeleteAppDomainReleasesEdgeTLSCertificate(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("TLS Domains")
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
		Hostname:    "www.example.com",
		AppID:       app.ID,
		TenantID:    tenant.ID,
		Status:      model.AppDomainStatusVerified,
		RouteTarget: "d-123.dns.fugue.pro",
	}); err != nil {
		t.Fatalf("put verified app domain: %v", err)
	}
	if _, err := s.PutEdgeTLSCertificate(model.EdgeTLSCertificate{
		Hostname:       "www.example.com",
		TenantID:       tenant.ID,
		AppID:          app.ID,
		CertificatePEM: "cert",
		PrivateKeyPEM:  "key",
	}); err != nil {
		t.Fatalf("put edge tls certificate: %v", err)
	}

	if _, err := s.DeleteAppDomain(app.ID, "www.example.com"); err != nil {
		t.Fatalf("delete app domain: %v", err)
	}
	if _, err := s.GetEdgeTLSCertificate("www.example.com"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected edge tls certificate to be deleted, got %v", err)
	}
}
