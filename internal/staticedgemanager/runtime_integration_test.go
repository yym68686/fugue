package staticedgemanager

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	c "fugue/internal/staticedgecontract"
)

// Real Caddy is opt-in locally and required by the static-edge CLI release gate.
func TestRealCaddyPreservesStreamAcrossActivationAndRollback(t *testing.T) {
	binary := os.Getenv("FUGUE_TEST_CADDY")
	if binary == "" {
		t.Skip("set FUGUE_TEST_CADDY to run isolated real proxy test")
	}
	dir, e := os.MkdirTemp("", "fse-it-")
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(dir)
	release := make(chan struct{})
	sent := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stream" {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: first\n\n")
			w.(http.Flusher).Flush()
			close(sent)
			<-release
			fmt.Fprint(w, "data: last\n\n")
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer upstream.Close()
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	addr := listener.Addr().String()
	listener.Close()
	u, _ := url.Parse(upstream.URL)
	socket := filepath.Join(dir, "caddy.sock")
	file := filepath.Join(dir, "caddy.json")
	config := func(marker string) json.RawMessage {
		cfg := map[string]any{"admin": map[string]any{"listen": "unix/" + socket, "config": map[string]any{"persist": false}}, "apps": map[string]any{"http": map[string]any{"servers": map[string]any{"test": map[string]any{"listen": []string{addr}, "routes": []any{map[string]any{"handle": []any{map[string]any{"handler": "headers", "response": map[string]any{"set": map[string]any{"X-Test-Generation": []string{marker}}}}, map[string]any{"handler": "reverse_proxy", "flush_interval": -1, "upstreams": []any{map[string]string{"dial": u.Host}}}}}}}}}}}
		raw, _ := json.Marshal(cfg)
		return raw
	}
	first := config("one")
	os.WriteFile(file, first, 0600)
	log, e := os.Create(filepath.Join(dir, "caddy.log"))
	if e != nil {
		t.Fatal(e)
	}
	defer log.Close()
	cmd := exec.Command(binary, "run", "--config", file)
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.Env = append(os.Environ(), "XDG_DATA_HOME="+dir, "XDG_CONFIG_HOME="+dir)
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { cmd.Process.Signal(os.Interrupt); cmd.Wait() }()
	raw, _ := os.ReadFile(binary)
	rt, e := NewCaddyRuntime(CaddyConfig{Binary: binary, BinarySHA256: c.Hash(raw), AdminSocket: socket, ConfigFile: file, Checks: map[string]Probe{"health": {URL: "http://" + addr + "/health", Status: 200, Body: "ok"}}})
	if e != nil {
		t.Fatal(e)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, e = rt.Snapshot(context.Background())
		if e == nil {
			break
		}
		if time.Now().After(deadline) {
			b, _ := os.ReadFile(filepath.Join(dir, "caddy.log"))
			t.Fatal(e, string(b))
		}
		time.Sleep(20 * time.Millisecond)
	}
	pub, key, _ := ed25519.GenerateKey(nil)
	manager, e := New(Config{Role: "edge", EdgeID: "local-edge", StateDir: filepath.Join(dir, "state"), VerificationKeys: map[string]ed25519.PublicKey{"local": pub}, Runtime: rt})
	if e != nil {
		t.Fatal(e)
	}
	defer manager.Close()
	bundle := func(n uint64, raw json.RawMessage) c.Bundle {
		b := c.Bundle{Schema: c.SchemaV1, EdgeID: "local-edge", Role: "edge", Generation: n, Mode: "serving", CaddyConfig: raw, HealthChecks: []string{"health"}, SigningKeyID: "local"}
		c.SignBundle(&b, key, "local")
		return b
	}
	call := func(op, id string, b *c.Bundle, d string) c.Response {
		r := manager.st.Revision
		out := manager.Execute(c.Request{Schema: c.RPCSchema, EdgeID: "local-edge", RequestID: id, Operation: op, ExpectedRevision: &r, Bundle: b, TargetDigest: d}, "tester", "admin")
		if !out.OK {
			t.Fatalf("%s failed: %+v", op, out)
		}
		return out
	}
	b1 := bundle(1, first)
	call("adopt", "adopt", &b1, "")
	response, e := http.Get("http://" + addr + "/stream")
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	line, e := reader.ReadString('\n')
	if e != nil || !strings.Contains(line, "first") {
		t.Fatal(line, e)
	}
	<-sent
	b2 := bundle(2, config("two"))
	call("stage", "stage", &b2, "")
	call("activate", "activate", nil, b2.BundleDigest)
	check, e := http.Get("http://" + addr + "/health")
	if e != nil {
		t.Fatal(e)
	}
	check.Body.Close()
	if check.Header.Get("X-Test-Generation") != "two" {
		t.Fatal("new request didn't use new config")
	}
	call("rollback", "rollback", nil, b1.BundleDigest)
	close(release)
	remaining := []byte{}
	for {
		line, e = reader.ReadString('\n')
		remaining = append(remaining, line...)
		if e != nil {
			break
		}
	}
	if !strings.Contains(string(remaining), "last") {
		t.Fatalf("stream interrupted: %s", remaining)
	}
	if cmd.ProcessState != nil {
		t.Fatal("Caddy unexpectedly exited")
	}
}
