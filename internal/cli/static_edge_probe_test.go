package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestStaticEdgeProbeRejectsOldEndpointWithSuccessfulStatus(t *testing.T) {
	req := staticEdgeProbeRequest{IP: "192.0.2.20", Host: "example.test", Path: "/health", Status: 200, EdgeID: "candidate"}
	if e := verifyStaticEdgeProbe(req, staticEdgeProbeResult{Status: 200, EdgeID: "old", PeerIP: req.IP}); e == nil {
		t.Fatal("intercepted old edge accepted")
	}
	if e := verifyStaticEdgeProbe(req, staticEdgeProbeResult{Status: 200, EdgeID: req.EdgeID, PeerIP: "192.0.2.10"}); e == nil {
		t.Fatal("wrong IP accepted")
	}
	if e := verifyStaticEdgeProbe(req, staticEdgeProbeResult{Status: 200, EdgeID: req.EdgeID, PeerIP: req.IP}); e != nil {
		t.Fatal(e)
	}
}

func TestStaticEdgeRemoteProbeUsesStructuredInput(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX fixture")
	}
	dir := t.TempDir()
	script := `#!/bin/sh
cat > "$PROBE_INPUT"
printf '%s' '{"status":200,"edge_id":"candidate","peer_ip":"192.0.2.20","body_bytes":2}'
`
	if e := os.WriteFile(filepath.Join(dir, "ssh"), []byte(script), 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("PROBE_INPUT", filepath.Join(dir, "input.json"))
	if e := probeStaticEdgeVerified(context.Background(), "trusted-vantage", "192.0.2.20", "example.test", "/health", 200, "candidate", time.Second); e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(os.Getenv("PROBE_INPUT"))
	if e != nil {
		t.Fatal(e)
	}
	var input staticEdgeProbeRequest
	if e = json.Unmarshal(raw, &input); e != nil {
		t.Fatal(e)
	}
	if input.IP != "192.0.2.20" || input.Host != "example.test" {
		t.Fatal(input)
	}
}

func TestStaticEdgePythonProbeParsesRealTLSHTTP(t *testing.T) {
	python, e := exec.LookPath("python3")
	if e != nil {
		t.Skip("python3 unavailable")
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "example.test" || r.URL.Path != "/health" {
			t.Error(r.Host, r.URL.Path)
		}
		w.Header().Set("X-Fugue-Static-Edge", "candidate")
		w.Write([]byte("ok"))
	}))
	defer server.Close()
	_, port, _ := net.SplitHostPort(server.Listener.Addr().String())
	// Test fixture uses the same verified HTTP parser and socket path against an
	// isolated certificate; production always uses create_default_context.
	code := strings.Replace(staticEdgePythonProbe, "ssl.create_default_context()", "ssl._create_unverified_context()", 1)
	code = strings.Replace(code, `(r["ip"],443)`, `(r["ip"],`+port+`)`, 1)
	input, _ := json.Marshal(staticEdgeProbeRequest{IP: "127.0.0.1", Host: "example.test", Path: "/health", Status: 200, EdgeID: "candidate", Timeout: 2})
	cmd := exec.Command(python, "-c", code)
	cmd.Stdin = bytes.NewReader(input)
	output, e := cmd.CombinedOutput()
	if e != nil {
		t.Fatal(e, string(output))
	}
	var result staticEdgeProbeResult
	if e = json.Unmarshal(output, &result); e != nil {
		t.Fatal(e, string(output))
	}
	if result.Status != 200 || result.EdgeID != "candidate" || result.BodyBytes != 2 {
		t.Fatal(result)
	}
}
