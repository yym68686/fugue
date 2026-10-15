package tlsissuance

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestACMEProtocolUsesSharedProofAndPublishesMatchingCertificate(t *testing.T) {
	b := fixtureBackend()
	var mu sync.Mutex
	validated, finalized := false, false
	orders := 0
	var issued []byte
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rootTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Fixture CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(365 * 24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	rootDER, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	root, _ := x509.ParseCertificate(rootDER)
	var authority *httptest.Server
	token := strings.Repeat("a", 32)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Replay-Nonce", base64.RawURLEncoding.EncodeToString([]byte(time.Now().String())))
		w.Header().Set("Content-Type", "application/json")
		base := authority.URL
		order := func(status string) map[string]any {
			return map[string]any{"status": status, "authorizations": []string{base + "/authorization"}, "finalize": base + "/finalize", "certificate": base + "/certificate"}
		}
		switch r.URL.Path {
		case "/directory":
			json.NewEncoder(w).Encode(map[string]string{"newNonce": base + "/nonce", "newAccount": base + "/account", "newOrder": base + "/orders"})
		case "/nonce":
			w.WriteHeader(200)
		case "/account":
			w.Header().Set("Location", base+"/account/1")
			w.WriteHeader(201)
			io.WriteString(w, `{"status":"valid"}`)
		case "/orders":
			orders++
			w.Header().Set("Location", base+"/order/1")
			w.WriteHeader(201)
			json.NewEncoder(w).Encode(order("pending"))
		case "/authorization":
			status := "pending"
			if validated {
				status = "valid"
			}
			json.NewEncoder(w).Encode(map[string]any{"status": status, "identifier": map[string]string{"type": "dns", "value": "site.example.test"}, "challenges": []any{map[string]string{"type": "http-01", "status": status, "url": base + "/challenge", "token": token}}})
		case "/challenge":
			b.mu.Lock()
			state := b.states["site.example.test"]
			b.mu.Unlock()
			if state.Token != token || !strings.HasPrefix(state.KeyAuthorization, token+".") || !state.ChallengeExpires.After(time.Now()) {
				t.Error("CA accepted before shared proof publication")
			}
			validated = true
			json.NewEncoder(w).Encode(map[string]string{"type": "http-01", "status": "valid", "url": base + "/challenge", "token": token})
		case "/order/1":
			status := "pending"
			if validated {
				status = "ready"
			}
			if finalized {
				status = "valid"
			}
			json.NewEncoder(w).Encode(order(status))
		case "/finalize":
			var jws struct {
				Payload string `json:"payload"`
			}
			if err := json.NewDecoder(r.Body).Decode(&jws); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			payload, err := base64.RawURLEncoding.DecodeString(jws.Payload)
			if err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			var finalize struct {
				CSR string `json:"csr"`
			}
			json.Unmarshal(payload, &finalize)
			der, err := base64.RawURLEncoding.DecodeString(finalize.CSR)
			if err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			csr, err := x509.ParseCertificateRequest(der)
			if err != nil || csr.CheckSignature() != nil || len(csr.DNSNames) != 1 || csr.DNSNames[0] != "site.example.test" {
				t.Error("CSR identity or signature differs")
				w.WriteHeader(400)
				return
			}
			leaf := &x509.Certificate{SerialNumber: big.NewInt(2), DNSNames: csr.DNSNames, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(90 * 24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
			certDER, err := x509.CreateCertificate(rand.Reader, leaf, root, csr.PublicKey, key)
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			issued = append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootDER})...)
			finalized = true
			json.NewEncoder(w).Encode(order("valid"))
		case "/certificate":
			w.Header().Set("Content-Type", "application/pem-certificate-chain")
			w.Write(issued)
		default:
			t.Errorf("unexpected ACME request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	authority = httptest.NewTLSServer(handler)
	defer authority.Close()
	b.p.DirectoryURL = authority.URL + "/directory"
	engine := &Engine{Backend: b, Owner: "issuer", HTTPClient: authority.Client()}
	if err := engine.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if orders != 1 || !validated || !finalized || b.publishes != 1 || b.cert.SHA256 == "old-valid" {
		t.Fatal("ACME issuance incomplete", orders, validated, finalized, b.publishes)
	}
	block, _ := pem.Decode([]byte(b.cert.PEM))
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil || leaf.VerifyHostname("site.example.test") != nil {
		t.Fatal("published wrong certificate", err)
	}
	if b.states["site.example.test"].Token != "" {
		t.Fatal("finished proof remained active")
	}
}
