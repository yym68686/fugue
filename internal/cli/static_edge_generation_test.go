package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	c "fugue/internal/staticedgecontract"
	"fugue/internal/staticedgemanager"
	"strings"
	"testing"
)

func sampleGeneration() staticGenerationPlan {
	return staticGenerationPlan{Schema: staticGenerationSchema, ID: "fixture-g1", EdgeID: "fixture-origin", Role: "origin", SSHHost: "fixture-node", ManagementIP: "192.0.2.8", ManagementListen: "192.0.2.8:9443", SourceCommit: strings.Repeat("a", 40), PackageSHA256: strings.Repeat("b", 64), Listeners: []string{"192.0.2.8:19444"}, CaddyConfig: json.RawMessage(`{"apps":{"http":{}}}`), Checks: map[string]staticedgemanager.Probe{"health": {URL: "http://127.0.0.1:19880/health", Status: 200}}, Assets: map[string]string{}, Preserve: []staticGenerationFence{{Unit: "previous.service", PID: 71, Config: "/etc/previous/config.json", SHA256: strings.Repeat("c", 64)}}}
}
func TestStaticGenerationRejectsUnsafeDestinationAndUnpinnedArtifacts(t *testing.T) {
	p := sampleGeneration()
	if e := p.validate(); e != nil {
		t.Fatal(e)
	}
	for _, change := range []func(*staticGenerationPlan){func(p *staticGenerationPlan) { p.ID = "../current" }, func(p *staticGenerationPlan) { p.SSHHost = "-oProxyCommand=bad" }, func(p *staticGenerationPlan) { p.Listeners = []string{":443"} }, func(p *staticGenerationPlan) { p.Assets["../caddy"] = "/tmp/fixture" }, func(p *staticGenerationPlan) { p.Preserve[0].PID = 0 }} {
		p = sampleGeneration()
		change(&p)
		if p.validate() == nil {
			t.Fatal("unsafe generation accepted")
		}
	}
	p = sampleGeneration()
	files := map[string][]byte{"caddy": []byte("fixture-caddy"), "fugue-static-edge-manager": []byte("fixture-manager"), "fugue-static-edge-observer": []byte("fixture-observer")}
	hashes := map[string]string{}
	for name, raw := range files {
		hashes[name] = strings.TrimPrefix(c.Hash(raw), "sha256:")
	}
	files["provenance.json"], _ = json.Marshal(map[string]any{"source_commit": p.SourceCommit, "files": hashes})
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, raw := range files {
		if e := tw.WriteHeader(&tar.Header{Name: name, Mode: 0755, Typeflag: tar.TypeReg, Size: int64(len(raw))}); e != nil {
			t.Fatal(e)
		}
		tw.Write(raw)
	}
	tw.Close()
	gz.Close()
	p.PackageSHA256 = strings.TrimPrefix(c.Hash(buf.Bytes()), "sha256:")
	if _, e := generationPackage(buf.Bytes(), p); e != nil {
		t.Fatal(e)
	}
	p.SourceCommit = strings.Repeat("d", 40)
	if _, e := generationPackage(buf.Bytes(), p); e == nil {
		t.Fatal("wrong source accepted")
	}
	p = sampleGeneration()
	if _, e := generationPackage(buf.Bytes(), p); e == nil {
		t.Fatal("wrong archive digest accepted")
	}
}
