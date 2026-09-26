package edge

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/tlscertificate"
)

func TestImportedCustomDomainTLSChangesCaddyLoadAndPrefersValidatedMaterial(t *testing.T) {
	host := "customer.external.test"
	var loadHeaders []string
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/load" || r.Method != http.MethodPost {
			t.Errorf("unexpected Caddy request %s %s", r.Method, r.URL.Path)
		}
		loadHeaders = append(loadHeaders, r.Header.Get("Cache-Control"))
		w.WriteHeader(http.StatusOK)
	}))
	defer admin.Close()
	bundle := testBundle("routegen_imported_cert")
	route := bundle.Routes[0]
	route.Hostname = host
	route.RouteKind = model.EdgeRouteKindCustomDomain
	route.TLSPolicy = model.EdgeRouteTLSPolicyCustomDomain
	bundle.Routes = []model.EdgeRouteBinding{route}
	bundle.TLSAllowlist = []model.EdgeTLSAllowlistEntry{{Hostname: host, AppID: route.AppID,
		TenantID: route.TenantID, Status: model.AppDomainStatusVerified, TLSStatus: model.AppDomainTLSStatusReady}}
	s := NewService(config.EdgeConfig{APIURL: "https://api.example.invalid", EdgeToken: "edge-secret",
		EdgeGroupID: "edge-group-default", CaddyEnabled: true, CaddyAdminURL: admin.URL,
		CaddyListenAddr: ":18443", CaddyTLSMode: caddyTLSModePublicOnDemand,
		CaddyTLSAskURL:       "http://127.0.0.1:7832/edge/tls/ask",
		CaddyProxyListenAddr: ":7833", CaddyDataDir: t.TempDir(), CaddySharedTLSEnabled: true}, log.New(io.Discard, "", 0))
	before, err := s.caddyConfigSignature(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.applyCaddyConfigOnly(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM := testCaddyTLSKeyPairAt(t, host, time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour))
	block, _ := pem.Decode([]byte(certPEM))
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	s.caddyImportedTLSRoots = roots
	imported := caddyTLSCertificateBundle{CertificatePEM: certPEM, PrivateKeyPEM: keyPEM,
		MetadataJSON: "{}", IssuerStorage: tlscertificate.ImportedIssuerStorage}
	if installed, err := s.installSharedCaddyTLSCertificate(host, imported); err != nil || !installed {
		t.Fatalf("install validated material: %t %v", installed, err)
	}
	got, err := s.readLocalCaddyTLSCertificate(host)
	if err != nil || got.IssuerStorage != tlscertificate.ImportedIssuerStorage || got.CertificatePEM != certPEM {
		t.Fatalf("read import: issuer=%s err=%v", got.IssuerStorage, err)
	}
	after, err := s.caddyConfigSignature(bundle)
	if err != nil || after == before {
		t.Fatalf("import did not alter Caddy configuration signature: %v", err)
	}
	configBody, _, err := s.buildCaddyConfig(bundle)
	if err != nil || !strings.Contains(string(configBody), tlscertificate.ImportedIssuerStorage+"/"+host+"/"+host+".crt") {
		t.Fatalf("imported certificate is absent from load_files: %v", err)
	}
	if _, err := s.applyCaddyConfigOnly(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	if len(loadHeaders) != 2 || loadHeaders[1] != "must-revalidate" {
		t.Fatalf("changed certificate did not force a real Caddy reload: %v", loadHeaders)
	}
	if _, err := s.applyCaddyConfigOnly(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	if len(loadHeaders) != 2 {
		t.Fatalf("unchanged certificate caused another Caddy reload: %v", loadHeaders)
	}
	s.mu.Lock()
	last := time.Now().Add(-caddySharedTLSRefreshInterval - time.Minute)
	s.metrics.CaddyWarmupSignature = s.caddyTLSWarmupSignature(bundle, after)
	s.metrics.CaddyWarmupAt = &last
	s.mu.Unlock()
	if !s.needsCaddyWarmup(s.caddyTLSWarmupSignature(bundle, after)) {
		t.Fatal("shared certificate refresh never recurs")
	}
}
