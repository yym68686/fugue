package cli

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestStaticEdgeBootstrapUniqueClientAndDurableIdentity(t *testing.T) {
	t.Setenv("FUGUE_STATIC_EDGE_STATE_DIR", t.TempDir())
	cli := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
	identity, e := createStaticEdgeManagementIdentity("candidate", "192.0.2.20")
	if e != nil {
		t.Fatal(e)
	}
	if e = cli.saveStaticEdgeBootstrapIdentity("candidate", &identity); e != nil {
		t.Fatal(e)
	}
	original := append([]byte(nil), identity.ClientCert...)
	again, e := createStaticEdgeManagementIdentity("candidate", "192.0.2.20")
	if e != nil {
		t.Fatal(e)
	}
	if e = cli.saveStaticEdgeBootstrapIdentity("candidate", &again); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(original, again.ClientCert) || again.CAPath == "" {
		t.Fatal("recovery replaced management identity")
	}
	for _, p := range []string{again.CAPath, again.ClientKeyPath, filepath.Join(filepath.Dir(again.CAPath), "ca.key")} {
		s, e := os.Stat(p)
		if e != nil || s.Mode().Perm() != 0600 {
			t.Fatal(p, e)
		}
	}
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	csr, e := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{CommonName: "new-edge"}}, key)
	if e != nil {
		t.Fatal(e)
	}
	cert, e := issueStaticEdgeClientCSR(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csr}), again.CAPath, filepath.Join(filepath.Dir(again.CAPath), "ca.key"))
	if e != nil {
		t.Fatal(e)
	}
	leaf, e := staticEdgeParseCertificate(cert)
	if e != nil {
		t.Fatal(e)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(again.CA)
	if _, e = leaf.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); e != nil {
		t.Fatal(e)
	}
	if _, e = leaf.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); e == nil {
		t.Fatal("business client can authenticate as server")
	}
	if _, e = issueStaticEdgeClientCSR([]byte("bad"), again.CAPath, filepath.Join(filepath.Dir(again.CAPath), "ca.key")); e == nil {
		t.Fatal("bad CSR accepted")
	}
}

func TestStaticEdgeBootstrapCaddyConfigIsLocalAndBounded(t *testing.T) {
	binary := os.Getenv("FUGUE_TEST_CADDY")
	if binary == "" {
		t.Skip("set FUGUE_TEST_CADDY for Caddy adapter test")
	}
	o := staticEdgeBootstrapOptions{EdgeID: "test-edge", Hostnames: []string{"example.test", "api.example.test", "assets.example.test"}, OriginIP: "192.0.2.40", OriginPort: 19443, OriginServerName: "origin.example.test"}
	config := staticEdgeBootstrapCaddyfile(o)
	cmd := exec.Command(binary, "adapt", "--config", "/dev/stdin", "--adapter", "caddyfile")
	cmd.Stdin = strings.NewReader(config)
	raw, e := cmd.Output()
	if e != nil {
		t.Fatal(e)
	}
	var doc struct {
		Admin struct {
			Listen string
			Config struct{ Persist bool }
		}
		Apps struct {
			HTTP struct {
				Servers map[string]struct {
					Listen []string
					Routes []json.RawMessage
				}
			}
		}
	}
	if e = json.Unmarshal(raw, &doc); e != nil {
		t.Fatal(e)
	}
	if doc.Admin.Listen != "unix//run/static-caddy/admin.sock" || doc.Admin.Config.Persist {
		t.Fatal(string(raw))
	}
	found := false
	for _, server := range doc.Apps.HTTP.Servers {
		for _, listen := range server.Listen {
			if listen == ":18480" {
				t.Fatal("internal upstream publicly exposed")
			}
			if listen == "127.0.0.1:18480" {
				found = true
				for _, route := range server.Routes {
					var r struct{ Match []map[string]json.RawMessage }
					if e = json.Unmarshal(route, &r); e != nil {
						t.Fatal(e)
					}
					for _, matcher := range r.Match {
						if _, ok := matcher["host"]; ok {
							t.Fatal("local upstream must accept preserved public Host, not only 127.0.0.1")
						}
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("local health listener absent")
	}
	if strings.Contains(config, "lb_try_duration") || strings.Contains(config, "max_fails") {
		t.Fatal("unexpected retry/health policy")
	}
	for _, forbidden := range []string{"Requires=static-caddy", "BindsTo=", "PartOf=", "ExecStop="} {
		if strings.Contains(staticEdgeManagerSystemdUnit, forbidden) {
			t.Fatal("manager may stop business process")
		}
	}
}

func TestStaticEdgeBootstrapRejectsUnsafeHostAndPaths(t *testing.T) {
	o := staticEdgeBootstrapOptions{SSHHost: "new", SourceSSH: "old", EdgeID: "edge", PublicIP: "192.0.2.1", Hostnames: []string{"example.test"}, OriginIP: "192.0.2.2", OriginPort: 19443, OriginServerName: "origin.example.test", SourceCaddy: "/usr/bin/caddy", SourceCertRoot: "/certs", SourceClientDir: "/client", CaddySHA256: strings.Repeat("a", 64), BusinessCA: "/ca.pem", BusinessCAKey: "/ca.key"}
	if e := validateStaticEdgeBootstrap(o); e != nil {
		t.Fatal(e)
	}
	o.SSHHost = "new;bad"
	if e := validateStaticEdgeBootstrap(o); e == nil {
		t.Fatal("unsafe SSH host")
	}
	o.SSHHost = "new"
	o.SourceCaddy = "/usr/bin/../bad"
	if e := validateStaticEdgeBootstrap(o); e == nil {
		t.Fatal("unsafe source path")
	}
}
