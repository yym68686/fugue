package livediagnostics

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestIndependentPublisherSignatureInteroperates(t *testing.T) {
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl is not installed")
	}
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 is not installed")
	}
	script := `import os,json,base64,hashlib
from scripts import publish_diagnostic_catalog as p
from scripts.test_publish_diagnostic_catalog import config
c=p.validate(config(),'registry.example/diagnostics@sha256:'+'a'*64)
seed=os.urandom(32);public=p.public_key(seed);key=seed+public;kid=hashlib.sha256(public).hexdigest()[:16];payload=p.canonical(c)
print(json.dumps({'envelope':{'key_id':kid,'payload':base64.b64encode(payload).decode(),'signature':base64.b64encode(p.sign(payload,key)).decode()},'keys':{kid:base64.b64encode(public).decode()}}))`
	cmd := exec.Command("python3", "-c", script)
	cmd.Dir = "../.."
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("independent publisher failed: %v %s", err, out)
	}
	var fixture struct {
		Envelope json.RawMessage   `json:"envelope"`
		Keys     map[string]string `json:"keys"`
	}
	if err := json.Unmarshal(out, &fixture); err != nil {
		t.Fatal(err)
	}
	catalog, err := VerifyCatalog(fixture.Envelope, fixture.Keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Probes) != 1 || catalog.Probes[0].ID != "sample-test" {
		t.Fatalf("unexpected verified catalog %+v", catalog)
	}
}
func TestEnvironmentCatalogMatchesRunnerProtocol(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/environments/production/diagnostics/catalog.json")
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Catalog json.RawMessage `json:"catalog"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	resolved := bytes.ReplaceAll(envelope.Catalog, []byte(`"$runner"`), []byte(`"registry.example/diagnostics@sha256:`+strings.Repeat("a", 64)+`"`))
	var catalog Catalog
	if err := DecodeStrict(resolved, &catalog); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Validate(); err != nil {
		t.Fatal(err)
	}
}
