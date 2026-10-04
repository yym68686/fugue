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
	"reflect"
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
		w.Header().Add("Alt-Svc", `h2=":443"`)
		w.Header().Add("Alt-Svc", `h3=":443"`)
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
	if result.Status != 200 || result.EdgeID != "candidate" || result.BodyBytes != 2 || result.AltSvc != `h2=":443", h3=":443"` {
		t.Fatal(result)
	}
}

func TestStaticEdgeProbeFailsClosedOnUnverifiedHTTP3(t *testing.T) {
	req := staticEdgeProbeRequest{IP: "192.0.2.20", Host: "example.test", Path: "/health", Status: 200, EdgeID: "candidate", RequireProtocolCoverage: true}
	for _, alt := range []string{`h3=":443"; ma=2592000`, `h2=":443", h3=":31443"; ma=60`, `h3-29=":443"`} {
		result := staticEdgeProbeResult{Status: 200, EdgeID: "candidate", PeerIP: req.IP, AltSvc: alt}
		if err := verifyStaticEdgeProbe(req, result); err == nil || !strings.Contains(err.Error(), "cannot verify cached QUIC paths") {
			t.Fatalf("unverified protocol accepted: %q %v", alt, err)
		}
		req.RequireProtocolCoverage = false
		if err := verifyStaticEdgeProbe(req, result); err != nil {
			t.Fatalf("TCP recovery probe blocked: %v", err)
		}
		req.RequireProtocolCoverage = true
	}
	for _, alt := range []string{"", `clear`, `h2=":443"`} {
		if err := verifyStaticEdgeProbe(req, staticEdgeProbeResult{Status: 200, EdgeID: "candidate", PeerIP: req.IP, AltSvc: alt}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStaticEdgeHTTP3CachedPorts(t *testing.T) {
	for _, test := range []struct {
		name  string
		alt   []string
		ports []int
	}{
		{name: "origin form", alt: []string{`h3=":443"; ma=86400`}, ports: []int{443}},
		{name: "cached port survives DNS switch", alt: []string{`h3=":443"`, `h3=":31443"`}, ports: []int{443, 31443}},
		{name: "multiple alternatives", alt: []string{`h3=":31443", h3=":443"`, `h3=":443"`}, ports: []int{443, 31443}},
		{name: "candidate clear retains old cached path", alt: []string{`h3=":443"`, `clear`}, ports: []int{443}},
		{name: "explicit same host", alt: []string{`h3="example.test:443"`}, ports: []int{443}},
		{name: "no h3", alt: []string{`h2=":443"`}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ports, err := staticEdgeHTTP3Ports("example.test", test.alt...)
			if err != nil || !reflect.DeepEqual(ports, test.ports) {
				t.Fatalf("cached ports for %q = %v, %v; want %v", test.alt, ports, err, test.ports)
			}
		})
	}
	for _, alt := range []string{`h3="bad"`, `h3=":0"`, `h3=":65536"`, `h3=:443`, `h3="other.test:443"`, `h3-29=":443"`} {
		if _, err := staticEdgeHTTP3Ports("example.test", alt); err == nil {
			t.Fatalf("accepted unverified alternative %q", alt)
		}
	}
}
