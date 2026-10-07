package edge

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

func sharedOnlyTLSFixture(t *testing.T) (*Service, model.EdgeRouteBundle, string) {
	t.Helper()
	host := "customer.example.test"
	bundle := testBundle("shared-cert")
	route := bundle.Routes[0]
	route.Hostname, route.RouteKind, route.TLSPolicy = host, model.EdgeRouteKindCustomDomain, model.EdgeRouteTLSPolicyCustomDomain
	bundle.Routes = []model.EdgeRouteBinding{route}
	bundle.TLSAllowlist = []model.EdgeTLSAllowlistEntry{{Hostname: host, AppID: route.AppID, TenantID: route.TenantID, Status: model.AppDomainStatusVerified, TLSStatus: model.AppDomainTLSStatusReady}}
	s := NewService(config.EdgeConfig{APIURL: "https://api.example.test", EdgeToken: "synthetic-edge", EdgeGroupID: route.EdgeGroupID, CaddyEnabled: true,
		CaddyListenAddr: "127.0.0.1:18443", CaddyProxyListenAddr: "127.0.0.1:7833",
		CaddyTLSMode: caddyTLSModeSharedOnly, CaddyDataDir: t.TempDir(), CaddySharedTLSEnabled: true}, nil)
	return s, bundle, host
}

func TestSharedOnlyTLSValidatesAndReloadsSharedCertificate(t *testing.T) {
	s, bundle, host := sharedOnlyTLSFixture(t)
	loads := 0
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/load" || r.Header.Get("Cache-Control") != "must-revalidate" {
			t.Error("certificate update requires a real Caddy reload")
		}
		loads++
	}))
	defer admin.Close()
	s.Config.CaddyAdminURL = admin.URL
	if err := s.validateConfig(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		cert, key := testCaddyTLSKeyPairAt(t, host, time.Now().Add(-60*24*time.Hour), time.Now().Add(time.Hour))
		if _, err := s.installSharedCaddyTLSCertificate(host, caddyTLSCertificateBundle{CertificatePEM: cert, PrivateKeyPEM: key, IssuerStorage: defaultCaddyIssuerStorage}); err != nil {
			t.Fatal(err)
		}
		if _, _, _, ok := s.loadedCaddyTLSFiles(host); !ok {
			t.Fatal("valid shared certificate was not loaded")
		}
		if _, err := s.applyCaddyConfigOnly(context.Background(), bundle); err != nil {
			t.Fatal(err)
		}
	}
	if loads != 2 {
		t.Fatal("shared certificate replacement did not reload Caddy", loads)
	}
	if _, err := s.applyCaddyConfigOnly(context.Background(), bundle); err != nil || loads != 2 {
		t.Fatal("unchanged shared certificate caused reload", err, loads)
	}
	certPath, _, _, _ := s.loadedCaddyTLSFiles(host)
	if err := os.WriteFile(certPath, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok := s.loadedCaddyTLSFiles(host); ok {
		t.Fatal("corrupt shared material accepted")
	}
	raw, _, err := s.buildCaddyConfig(bundle)
	if err != nil || strings.Contains(string(raw), "load_files") || strings.Contains(string(raw), "on_demand") {
		t.Fatal("missing shared certificate enabled issuance", err)
	}
	w := httptest.NewRecorder()
	s.handleTLSAsk(w, httptest.NewRequest("GET", "/edge/tls/ask?domain="+host, nil))
	if w.Code != http.StatusForbidden {
		t.Fatal("shared-only worker granted ACME permission")
	}
	s.Config.CaddySharedTLSEnabled = false
	if s.validateConfig() == nil {
		t.Fatal("shared-only mode accepted disabled synchronization")
	}
}

func TestRealCaddySharedOnlyServesAndReloadsWithoutIssuance(t *testing.T) {
	binary := os.Getenv("FUGUE_TEST_CADDY_BINARY")
	if binary == "" {
		t.Skip("set FUGUE_TEST_CADDY_BINARY for isolated real TLS validation")
	}
	s, bundle, host := sharedOnlyTLSFixture(t)
	freeAddress := func() string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		return l.Addr().String()
	}
	s.Config.CaddyListenAddr = freeAddress()
	s.Config.CaddyAdminURL = "http://" + freeAddress()
	install := func() {
		cert, key := testCaddyTLSKeyPairAt(t, host, time.Now().Add(-60*24*time.Hour), time.Now().Add(time.Hour))
		if _, err := s.installSharedCaddyTLSCertificate(host, caddyTLSCertificateBundle{CertificatePEM: cert, PrivateKeyPEM: key, IssuerStorage: defaultCaddyIssuerStorage}); err != nil {
			t.Fatal(err)
		}
	}
	install()
	raw, _, err := s.buildCaddyConfig(bundle)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	document["storage"] = map[string]any{"module": "file_system", "root": s.Config.CaddyDataDir}
	raw, _ = json.Marshal(document)
	dir := t.TempDir()
	configFile := filepath.Join(dir, "config.json")
	if err := os.WriteFile(configFile, raw, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	logFile, err := os.Create(filepath.Join(dir, "caddy.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	cmd := exec.CommandContext(ctx, binary, "run", "--config", configFile)
	cmd.Env = append(os.Environ(), "XDG_CONFIG_HOME="+dir, "XDG_DATA_HOME="+dir)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	defer func() { cancel(); _ = cmd.Wait() }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", strings.TrimPrefix(s.Config.CaddyAdminURL, "http://"), 200*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			logs, _ := os.ReadFile(logFile.Name())
			t.Fatal("Caddy did not start", string(logs))
		}
		time.Sleep(50 * time.Millisecond)
	}
	certificate := func(name string) ([32]byte, error) {
		// This isolated fixture deliberately uses self-signed certificates.
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", s.Config.CaddyListenAddr, &tls.Config{ServerName: name, InsecureSkipVerify: true})
		if err != nil {
			return [32]byte{}, err
		}
		defer conn.Close()
		return sha256.Sum256(conn.ConnectionState().PeerCertificates[0].Raw), nil
	}
	before, err := certificate(host)
	if err != nil {
		t.Fatal("near-expiry shared certificate did not serve", err)
	}
	if _, err := certificate("unconfigured.example.test"); err == nil {
		t.Fatal("unconfigured hostname acquired certificate")
	}
	install()
	if _, err := s.applyCaddyConfigOnly(ctx, bundle); err != nil {
		t.Fatal(err)
	}
	after, err := certificate(host)
	if err != nil || after == before {
		t.Fatal("real Caddy failed to reload shared certificate", err)
	}
	logs, _ := os.ReadFile(logFile.Name())
	for _, unexpected := range []string{"obtaining certificate", "renewing certificate", "renewal info", "registering account"} {
		if strings.Contains(string(logs), unexpected) {
			t.Fatalf("certificate consumer started issuance: %s", unexpected)
		}
	}
}
