package certsync

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fixtureCertificate(t *testing.T, host string, lifetime time.Duration) (string, string, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: host},
		DNSNames: []string{host}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(lifetime),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, leaf, leaf, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	privateDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privateDER})), parsed
}

func TestSynchronizerImportsOnlyValidatedNewerCertificate(t *testing.T) {
	host, appID := "customer.external.test", "app-test"
	root := t.TempDir()
	dir := filepath.Join(root, "certificates", "acme-v02.api.letsencrypt.org-directory", host)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM, leaf := fixtureCertificate(t, host, 30*24*time.Hour)
	for suffix, value := range map[string]string{"crt": certPEM, "key": keyPEM} {
		if err := os.WriteFile(filepath.Join(dir, host+"."+suffix), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	tokenPath := filepath.Join(root, "api-token")
	if err := os.WriteFile(tokenPath, []byte("test-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(leaf.Raw)
	fingerprint := hex.EncodeToString(sum[:])
	var stored metadata
	var puts int
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" || r.URL.Path != "/v1/apps/"+appID+"/domains/"+host+"/certificate" {
			t.Errorf("unexpected API identity or path: %s", r.URL.Path)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		switch r.Method {
		case http.MethodGet:
			if !stored.Present {
				stored = metadata{Hostname: host, AppID: appID}
			}
			_ = json.NewEncoder(w).Encode(stored)
		case http.MethodPut:
			puts++
			var req struct {
				CertificatePEM string `json:"certificate_pem"`
				PrivateKeyPEM  string `json:"private_key_pem"`
				ExpectedSHA    string `json:"expected_certificate_sha256"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.CertificatePEM != strings.TrimSpace(certPEM) ||
				req.PrivateKeyPEM != strings.TrimSpace(keyPEM) || req.ExpectedSHA != stored.CertificateSHA256 {
				t.Error("import body mismatched source material or current fingerprint")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			expires := leaf.NotAfter
			stored = metadata{Hostname: host, AppID: appID, Present: true, CertificateSHA256: fingerprint, NotAfter: &expires}
			_ = json.NewEncoder(w).Encode(stored)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer api.Close()
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	s := Synchronizer{Config: Config{APIURL: api.URL, APITokenFile: tokenPath, AppID: appID,
		Hostnames: []string{host}, CaddyDataDir: root, MinimumDaysLeft: 21}, Roots: roots, Client: api.Client()}
	for index, want := range []string{"imported", "unchanged", "imported", "standby_newer"} {
		if index == 2 {
			certPEM, keyPEM, leaf = fixtureCertificate(t, host, 60*24*time.Hour)
			roots.AddCert(leaf)
			for suffix, value := range map[string]string{"crt": certPEM, "key": keyPEM} {
				if err := os.WriteFile(filepath.Join(dir, host+"."+suffix), []byte(value), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			sum = sha256.Sum256(leaf.Raw)
			fingerprint = hex.EncodeToString(sum[:])
		}
		if index == 3 {
			expires := time.Now().Add(70 * 24 * time.Hour)
			stored = metadata{Hostname: host, AppID: appID, Present: true, CertificateSHA256: strings.Repeat("a", 64), NotAfter: &expires}
		}
		results, err := s.Run(t.Context())
		if err != nil || len(results) != 1 || results[0].Action != want || results[0].CertificateSHA256 != fingerprint {
			t.Fatalf("sync %d: results=%+v err=%v", index, results, err)
		}
	}
	if puts != 2 {
		t.Fatalf("expected initial and renewed certificate imports, got %d", puts)
	}
}

func TestSynchronizerRejectsUntrustedAndPrivateTokenFile(t *testing.T) {
	host := "customer.external.test"
	root := t.TempDir()
	dir := filepath.Join(root, "certificates", "issuer", host)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	cert, key, _ := fixtureCertificate(t, host, 30*24*time.Hour)
	for suffix, value := range map[string]string{"crt": cert, "key": key} {
		if err := os.WriteFile(filepath.Join(dir, host+"."+suffix), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	token := filepath.Join(root, "token")
	if err := os.WriteFile(token, []byte("test-token"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := Synchronizer{Config: Config{APIURL: "https://api.example.test", APITokenFile: token, AppID: "app-test",
		Hostnames: []string{host}, CaddyDataDir: root, MinimumDaysLeft: 21}}
	if _, err := s.Run(t.Context()); err == nil {
		t.Fatal("world-readable API token accepted")
	}
	if err := os.Chmod(token, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Run(t.Context()); err == nil || !strings.Contains(err.Error(), "trusted") {
		t.Fatalf("untrusted source accepted: %v", err)
	}
}
