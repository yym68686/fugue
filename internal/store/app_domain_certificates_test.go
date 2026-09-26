package store

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
	"fugue/internal/tlscertificate"
)

func TestAppDomainCertificateImportCASAndEdgeReportIsolation(t *testing.T) {
	backends := map[string]string{"json": ""}
	if databaseURL := os.Getenv("FUGUE_TEST_DATABASE_URL"); databaseURL != "" {
		backends["postgres"] = databaseURL
	}
	for backend, databaseURL := range backends {
		t.Run(backend, func(t *testing.T) {
			statePath := t.TempDir() + "/store.json"
			s := New(statePath, databaseURL)
			if databaseURL != "" {
				u, err := url.Parse(databaseURL)
				if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "test") {
					t.Fatal("PostgreSQL test requires a disposable loopback test database")
				}
				db, err := sql.Open("pgx", databaseURL)
				if err != nil {
					t.Fatal(err)
				}
				s = &Store{db: db, databaseURL: databaseURL}
				ctx := context.Background()
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = s.applyPostgresSchemaTx(ctx, tx); err != nil {
					_ = tx.Rollback()
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
				for _, migrate := range []func(context.Context, string) error{
					schemamigrate.MigratePlatformState, schemamigrate.MigrateImageCacheManifestGraph,
					schemamigrate.MigrateEdgeInstanceFencing, schemamigrate.MigrateSourceUploadSessions,
				} {
					if err := migrate(ctx, databaseURL); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := s.Init(); err != nil {
				t.Fatal(err)
			}
			if s.usingDatabase() {
				t.Cleanup(func() { _ = s.db.Close() })
			}
			tenant, err := s.CreateTenant("certificate-test-" + model.NewID("tenant"))
			if err != nil {
				t.Fatal(err)
			}
			project, err := s.CreateProject(tenant.ID, "certificate-test", "")
			if err != nil {
				t.Fatal(err)
			}
			app, err := s.CreateApp(tenant.ID, project.ID, "certificate-test", "", model.AppSpec{
				Image: "ghcr.io/example/certificate-test:latest", Replicas: 1, RuntimeID: "runtime_managed_shared"})
			if err != nil {
				t.Fatal(err)
			}
			host := "certificate-" + strings.ToLower(model.NewID("test")) + ".external.test"
			_, err = s.PutAppDomain(model.AppDomain{Hostname: host, TenantID: tenant.ID, AppID: app.ID,
				Status: model.AppDomainStatusVerified, DNSMode: model.AppDomainDNSModeManual,
				DNSStatus: model.AppDomainDNSStatusReady, TLSStatus: model.AppDomainTLSStatusPending})
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			firstExpiry := now.Add(30 * 24 * time.Hour)
			first := model.EdgeTLSCertificate{Hostname: host, TenantID: tenant.ID, AppID: app.ID,
				CertificatePEM: "first-certificate", PrivateKeyPEM: "first-key", IssuerStorage: tlscertificate.ImportedIssuerStorage,
				CertificateSHA256: strings.Repeat("a", 64), NotAfter: &firstExpiry}
			stored, err := s.ImportAppDomainCertificate(first, project.ID, "", []string{host})
			if err != nil || stored.CertificateSHA256 != first.CertificateSHA256 {
				t.Fatalf("initial import: %+v %v", stored, err)
			}
			if _, err := s.ImportAppDomainCertificate(first, project.ID, "", []string{host}); err != nil {
				t.Fatalf("identical retry: %v", err)
			}
			secondExpiry := now.Add(40 * 24 * time.Hour)
			second := first
			second.CertificatePEM, second.PrivateKeyPEM = "second-certificate", "second-key"
			second.CertificateSHA256, second.NotAfter = strings.Repeat("b", 64), &secondExpiry
			if _, err := s.ImportAppDomainCertificate(second, project.ID, "", []string{host}); !errors.Is(err, ErrConflict) {
				t.Fatalf("stale fingerprint accepted: %v", err)
			}
			if _, err := s.ImportAppDomainCertificate(second, "foreign-project", first.CertificateSHA256, []string{host}); !errors.Is(err, ErrConflict) {
				t.Fatalf("foreign project accepted: %v", err)
			}
			var wg sync.WaitGroup
			errorsSeen := make(chan error, 2)
			for _, incoming := range []model.EdgeTLSCertificate{second, func() model.EdgeTLSCertificate {
				third := second
				third.CertificateSHA256 = strings.Repeat("c", 64)
				third.CertificatePEM = "third-certificate"
				return third
			}()} {
				wg.Add(1)
				go func(cert model.EdgeTLSCertificate) {
					defer wg.Done()
					_, err := s.ImportAppDomainCertificate(cert, project.ID, first.CertificateSHA256, []string{host})
					errorsSeen <- err
				}(incoming)
			}
			wg.Wait()
			close(errorsSeen)
			var successes, conflicts int
			for err := range errorsSeen {
				switch {
				case err == nil:
					successes++
				case errors.Is(err, ErrConflict):
					conflicts++
				default:
					t.Fatalf("unexpected CAS failure: %v", err)
				}
			}
			if successes != 1 || conflicts != 1 {
				t.Fatalf("CAS allowed %d winners and %d conflicts", successes, conflicts)
			}
			current, err := s.GetEdgeTLSCertificate(host)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.PutEdgeTLSCertificate(first); err != nil {
				t.Fatal(err)
			}
			after, err := s.GetEdgeTLSCertificate(host)
			if err != nil || after.CertificateSHA256 != current.CertificateSHA256 {
				t.Fatalf("edge report replaced imported certificate: %v", err)
			}
		})
	}
}
