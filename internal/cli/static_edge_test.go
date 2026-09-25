package cli

import (
	"bytes"
	"context"
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
	"runtime"
	"strings"
	"testing"
	"time"

	c "fugue/internal/staticedgecontract"
	m "fugue/internal/staticedgemanager"
)

func tlsFixture(t *testing.T) (staticEdgeContext, *m.TLSCredentials) {
	t.Helper()
	dir := t.TempDir()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "management test CA"}, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, e := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if e != nil {
		t.Fatal(e)
	}
	ca, _ = x509.ParseCertificate(der)
	caPath := filepath.Join(dir, "ca.pem")
	os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600)
	issue := func(name string, usage x509.ExtKeyUsage) (string, string, string) {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tpl := &x509.Certificate{SerialNumber: big.NewInt(int64(len(name) + 2)), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name}, ExtKeyUsage: []x509.ExtKeyUsage{usage}, KeyUsage: x509.KeyUsageDigitalSignature, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
		b, e := x509.CreateCertificate(rand.Reader, tpl, ca, &key.PublicKey, caKey)
		if e != nil {
			t.Fatal(e)
		}
		certPath := filepath.Join(dir, name+".pem")
		keyPath := filepath.Join(dir, name+".key")
		os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: b}), 0600)
		kb, _ := x509.MarshalPKCS8PrivateKey(key)
		os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb}), 0600)
		sum := sha256.Sum256(b)
		return certPath, keyPath, hex.EncodeToString(sum[:])
	}
	serverCert, serverKey, _ := issue("manager.example.test", x509.ExtKeyUsageServerAuth)
	clientCert, clientKey, fp := issue("operator", x509.ExtKeyUsageClientAuth)
	creds, e := m.NewCredentials(map[string]m.CredentialSlot{"a": {ServerCert: serverCert, ServerKey: serverKey, ClientCA: caPath, Grants: map[string]string{fp: "operator"}}, "b": {ServerCert: serverCert, ServerKey: serverKey, ClientCA: caPath, Grants: map[string]string{strings.Repeat("0", 64): "read"}}}, "a")
	if e != nil {
		t.Fatal(e)
	}
	return staticEdgeContext{Name: "local-test", EdgeID: "test-edge", Transport: "mtls", ServerName: "manager.example.test", CAFile: caPath, ClientCert: clientCert, ClientKey: clientKey}, creds
}
func TestStaticEdgeMTLSRequiresCorrectIdentityAndNeverCallsFugue(t *testing.T) {
	cfg, creds := tlsFixture(t)
	apiCalls := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { apiCalls++; http.Error(w, "offline", 503) }))
	defer api.Close()
	t.Setenv("FUGUE_API_URL", api.URL)
	t.Setenv("FUGUE_API_KEY", "invalid")
	t.Setenv("FUGUE_CONTEXT_FILE", filepath.Join(t.TempDir(), "bad.json"))
	os.WriteFile(os.Getenv("FUGUE_CONTEXT_FILE"), []byte("broken"), 0600)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, g, e := creds.Authorize(r)
		if e != nil || g != "operator" {
			http.Error(w, "denied", 403)
			return
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("Fugue token leaked")
		}
		var req c.Request
		json.NewDecoder(r.Body).Decode(&req)
		json.NewEncoder(w).Encode(c.Response{Schema: c.RPCSchema, RequestID: req.RequestID, EdgeID: req.EdgeID, OK: true, Status: 200, Result: &c.Observed{EdgeID: req.EdgeID, Ready: true}})
	}))
	server.TLS = creds.Config()
	server.StartTLS()
	defer server.Close()
	cfg.ManagerURL = server.URL
	t.Setenv("FUGUE_STATIC_EDGE_CONTEXT_FILE", filepath.Join(t.TempDir(), "contexts.json"))
	if e := saveStaticEdgeContexts(staticEdgeContextFile{SchemaVersion: 1, Contexts: []staticEdgeContext{cfg}}); e != nil {
		t.Fatal(e)
	}
	var out, stderr bytes.Buffer
	if e := runWithStreams([]string{"--json", "static-edge", "status", cfg.Name}, &out, &stderr); e != nil {
		t.Fatal(e, stderr.String())
	}
	if apiCalls != 0 {
		t.Fatal("Fugue API contacted")
	}
	req := c.Request{Schema: c.RPCSchema, EdgeID: cfg.EdgeID, RequestID: "probe", Operation: "status"}
	cfg.ServerName = "wrong.example.test"
	if _, e := staticEdgeCall(context.Background(), cfg, req); e == nil {
		t.Fatal("wrong server accepted")
	}
	cfg.ServerName = "manager.example.test"
	if e := creds.Select("b"); e != nil {
		t.Fatal(e)
	}
	if _, e := staticEdgeCall(context.Background(), cfg, req); e == nil {
		t.Fatal("revoked fingerprint still allowed")
	}
}
func TestStaticEdgeMTLSRejectsRedirect(t *testing.T) {
	cfg, creds := tlsFixture(t)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://elsewhere.example.test", 307)
	}))
	srv.TLS = creds.Config()
	srv.StartTLS()
	defer srv.Close()
	cfg.ManagerURL = srv.URL
	_, e := staticEdgeCall(context.Background(), cfg, c.Request{Schema: c.RPCSchema, EdgeID: cfg.EdgeID, RequestID: "x", Operation: "status"})
	if e == nil || !strings.Contains(e.Error(), "redirect") {
		t.Fatal(e)
	}
}
func TestStaticEdgeContextStrictAndSSHInjection(t *testing.T) {
	base := staticEdgeContext{Name: "example", EdgeID: "edge", Transport: "ssh", SSHHost: "alias", ManagerCommand: "/usr/local/bin/manager"}
	for _, host := range []string{"-oProxyCommand=bad", "alias;bad", "user@host", "alias\ncommand"} {
		x := base
		x.SSHHost = host
		if validateStaticEdgeContext(x) == nil {
			t.Fatal(host)
		}
	}
	base.ManagerCommand = "/bin/sh -c bad"
	if validateStaticEdgeContext(base) == nil {
		t.Fatal("shell command accepted")
	}
	p := filepath.Join(t.TempDir(), "contexts.json")
	t.Setenv("FUGUE_STATIC_EDGE_CONTEXT_FILE", p)
	for _, raw := range []string{`{"schema_version":2,"contexts":[]}`, `{"schema_version":1,"contexts":[]} {}`, `{"schema_version":1,"contexts":[],"secret":"bad"}`} {
		os.WriteFile(p, []byte(raw), 0600)
		if _, e := readStaticEdgeContexts(); e == nil {
			t.Fatal(raw)
		}
	}
}
func TestStaticEdgeSSHUsesAliasAndStructuredStdin(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX SSH fixture")
	}
	dir := t.TempDir()
	script := `#!/bin/sh
printf '%s\n' "$@" > "$TEST_SSH_ARGS"
cat > "$TEST_SSH_STDIN"
printf '%s' '{"schema":"fugue.static-edge.rpc/v1","edge_id":"test-edge","request_id":"ssh-test","ok":true,"status":200}'
`
	os.WriteFile(filepath.Join(dir, "ssh"), []byte(script), 0700)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("TEST_SSH_ARGS", filepath.Join(dir, "args"))
	t.Setenv("TEST_SSH_STDIN", filepath.Join(dir, "stdin"))
	cfg := staticEdgeContext{Name: "test", EdgeID: "test-edge", Transport: "ssh", SSHHost: "my-alias", ManagerCommand: "/usr/local/bin/manager"}
	req := c.Request{Schema: c.RPCSchema, EdgeID: cfg.EdgeID, RequestID: "ssh-test", Operation: "status"}
	if _, e := staticEdgeCall(context.Background(), cfg, req); e != nil {
		t.Fatal(e)
	}
	args, _ := os.ReadFile(os.Getenv("TEST_SSH_ARGS"))
	if !strings.Contains(string(args), "StrictHostKeyChecking=yes") || !strings.HasSuffix(string(args), "my-alias\n/usr/local/bin/manager\nssh-rpc\n") {
		t.Fatal(string(args))
	}
	raw, _ := os.ReadFile(os.Getenv("TEST_SSH_STDIN"))
	var actual c.Request
	if c.StrictJSON(raw, &actual) != nil || actual.RequestID != req.RequestID {
		t.Fatal(string(raw))
	}
}
