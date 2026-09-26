package api

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/tlscertificate"
)

func TestAppDomainCertificateImportScopeCASAndExpiry(t *testing.T) {
	state, server, ordinaryKey, _, app, resolver := setupAppDomainTestServerWithDomains(t, "example.test")
	host := "customer.external.test"
	resolver.cname[host] = server.primaryCustomDomainTarget(app) + "."
	created := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", ordinaryKey, map[string]any{"hostname": host})
	if created.Code != http.StatusOK {
		t.Fatal(created.Body.String())
	}
	_, tlsKey, err := state.CreateAPIKey(app.TenantID, "certificate-writer", []string{"app.tls.read", "app.tls.write"})
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/apps/" + app.ID + "/domains/" + host + "/certificate"
	certPEM, privatePEM, _ := generateTestTLSCertificateBundleAt(t, host, time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour))
	block, _ := pem.Decode([]byte(certPEM))
	if block == nil {
		t.Fatal("missing leaf")
	}
	roots := x509.NewCertPool()
	roots.AddCert(mustParseCertificate(t, block.Bytes))
	server.certificateImportRoots = roots
	request := map[string]any{"certificate_pem": certPEM, "private_key_pem": privatePEM, "expected_certificate_sha256": ""}
	if r := performJSONRequest(t, server, http.MethodPut, path, ordinaryKey, request); r.Code != http.StatusForbidden {
		t.Fatalf("ordinary app scope imported a certificate: %d", r.Code)
	}
	if r := performJSONRequest(t, server, http.MethodGet, path, ordinaryKey, nil); r.Code != http.StatusForbidden {
		t.Fatalf("ordinary app scope read certificate metadata: %d", r.Code)
	}
	initial := performJSONRequest(t, server, http.MethodGet, path, tlsKey, nil)
	var initialMeta appDomainCertificateMetadata
	mustDecodeJSON(t, initial, &initialMeta)
	if initial.Code != http.StatusOK || initialMeta.Present {
		t.Fatalf("unexpected initial metadata: %d %+v", initial.Code, initialMeta)
	}
	imported := performJSONRequest(t, server, http.MethodPut, path, tlsKey, request)
	var importedMeta appDomainCertificateMetadata
	mustDecodeJSON(t, imported, &importedMeta)
	if imported.Code != http.StatusOK || !importedMeta.Present || len(importedMeta.CertificateSHA256) != 64 ||
		strings.Contains(imported.Body.String(), "PRIVATE KEY") {
		t.Fatalf("invalid import response: %d %s", imported.Code, imported.Body.String())
	}
	stored, err := state.GetEdgeTLSCertificate(host)
	if err != nil || stored.IssuerStorage != tlscertificate.ImportedIssuerStorage || stored.CertificateSHA256 != importedMeta.CertificateSHA256 {
		t.Fatalf("incorrect stored material: %+v %v", stored, err)
	}
	domain, err := state.GetAppDomain(host)
	if err != nil || domain.TLSStatus != model.AppDomainTLSStatusPending {
		t.Fatalf("import asserted TLS readiness: %+v %v", domain, err)
	}
	if r := performJSONRequest(t, server, http.MethodPut, path, tlsKey, request); r.Code != http.StatusOK {
		t.Fatalf("identical retry is not idempotent: %d %s", r.Code, r.Body.String())
	}
	newCert, newKey, _ := generateTestTLSCertificateBundleAt(t, host, time.Now().Add(-time.Hour), time.Now().Add(40*24*time.Hour))
	newBlock, _ := pem.Decode([]byte(newCert))
	roots.AddCert(mustParseCertificate(t, newBlock.Bytes))
	request["certificate_pem"], request["private_key_pem"] = newCert, newKey
	if r := performJSONRequest(t, server, http.MethodPut, path, tlsKey, request); r.Code != http.StatusConflict {
		t.Fatalf("stale CAS accepted: %d %s", r.Code, r.Body.String())
	}
	request["expected_certificate_sha256"] = importedMeta.CertificateSHA256
	if r := performJSONRequest(t, server, http.MethodPut, path, tlsKey, request); r.Code != http.StatusOK {
		t.Fatalf("valid renewal rejected: %d %s", r.Code, r.Body.String())
	}
	newStored, err := state.GetEdgeTLSCertificate(host)
	if err != nil || newStored.CertificateSHA256 == stored.CertificateSHA256 {
		t.Fatalf("renewal did not replace material: %v", err)
	}
	if _, err := state.PutEdgeTLSCertificate(stored); err != nil {
		t.Fatal(err)
	}
	current, err := state.GetEdgeTLSCertificate(host)
	if err != nil || current.CertificateSHA256 != newStored.CertificateSHA256 {
		t.Fatalf("older edge report replaced a newer import: %v", err)
	}
	metadata := performJSONRequest(t, server, http.MethodGet, path, tlsKey, nil)
	if metadata.Code != http.StatusOK || strings.Contains(metadata.Body.String(), "PRIVATE KEY") || strings.Contains(metadata.Body.String(), "certificate_pem") {
		t.Fatalf("metadata leaked certificate material: %d %s", metadata.Code, metadata.Body.String())
	}
}

func TestAppDomainCertificateImportRejectsUntrustedAndWrongName(t *testing.T) {
	state, server, ordinaryKey, _, app, resolver := setupAppDomainTestServerWithDomains(t, "example.test")
	host := "customer.external.test"
	resolver.cname[host] = server.primaryCustomDomainTarget(app) + "."
	if r := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", ordinaryKey, map[string]any{"hostname": host}); r.Code != http.StatusOK {
		t.Fatal(r.Body.String())
	}
	_, tlsKey, err := state.CreateAPIKey(app.TenantID, "certificate-writer", []string{"app.tls.write"})
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/apps/" + app.ID + "/domains/" + host + "/certificate"
	otherCert, otherKey, _ := generateTestTLSCertificateBundleAt(t, "other.external.test", time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour))
	request := map[string]any{"certificate_pem": otherCert, "private_key_pem": otherKey, "expected_certificate_sha256": ""}
	if r := performJSONRequest(t, server, http.MethodPut, path, tlsKey, request); r.Code != http.StatusBadRequest {
		t.Fatalf("wrong-host certificate accepted: %d %s", r.Code, r.Body.String())
	}
	certPEM, privatePEM, _ := generateTestTLSCertificateBundleAt(t, host, time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour))
	request["certificate_pem"], request["private_key_pem"] = certPEM, privatePEM
	if r := performJSONRequest(t, server, http.MethodPut, path, tlsKey, request); r.Code != http.StatusBadRequest {
		t.Fatalf("untrusted self-signed certificate accepted: %d %s", r.Code, r.Body.String())
	}
	if r := performJSONRequest(t, server, http.MethodGet, path, tlsKey, nil); r.Code != http.StatusOK {
		t.Fatalf("write scope does not permit metadata read: %d", r.Code)
	}
	for _, names := range [][]string{{host, "foreign.external.test"}, {host, "*.external.test"}} {
		cert, key, root := testCertificateWithNames(t, names)
		trust := x509.NewCertPool()
		trust.AddCert(root)
		server.certificateImportRoots = trust
		request["certificate_pem"], request["private_key_pem"] = cert, key
		response := performJSONRequest(t, server, http.MethodPut, path, tlsKey, request)
		if response.Code != http.StatusForbidden && response.Code != http.StatusBadRequest {
			t.Fatalf("certificate with unowned or wildcard SAN accepted: names=%v status=%d", names, response.Code)
		}
	}
}

func testCertificateWithNames(t *testing.T, names []string) (string, string, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{CommonName: names[0]}, DNSNames: names, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(30 * 24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	privateDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privateDER})), mustParseCertificate(t, der)
}

func mustParseCertificate(t *testing.T, der []byte) *x509.Certificate {
	t.Helper()
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}
