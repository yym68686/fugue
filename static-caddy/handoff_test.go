package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
	"golang.org/x/net/http2"
)

// This child runs the real Caddy command lifecycle, including process exit.
// In-process caddy.Stop alone does not reproduce a binary replacement.
func TestCaddyHandoffChild(t *testing.T) {
	config := os.Getenv("FUGUE_HANDOFF_CHILD_CONFIG")
	if config == "" {
		t.Skip("isolated process fixture")
	}
	os.Args = []string{os.Args[0], "run", "--config", config}
	main()
}

// A finite shutdown grace cannot guarantee an arbitrary in-flight stream
// survives replacement. Run with FUGUE_HANDOFF_TEST_GRACE=120s to reproduce
// the inspected production budget using only synthetic loopback requests.
func TestFiniteGraceDoesNotProveSafeBinaryHandoff(t *testing.T) {
	grace := 200 * time.Millisecond
	if value := os.Getenv("FUGUE_HANDOFF_TEST_GRACE"); value != "" {
		var err error
		grace, err = time.ParseDuration(value)
		if err != nil || grace < 100*time.Millisecond || grace > 125*time.Second {
			t.Fatal("fixture grace must be between 100ms and 125s")
		}
	}
	root, err := os.MkdirTemp("/tmp", "fugue-handoff-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "isolated-fixture"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	privateDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile := filepath.Join(root, "tls.crt"), filepath.Join(root, "tls.key")
	if err = os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}), 0600); err != nil {
		t.Fatal(err)
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			w.WriteHeader(204)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "ready\n")
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
	}))
	defer backend.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	adminSocket := filepath.Join(root, "admin.sock")
	config := map[string]any{
		"admin":   map[string]any{"listen": "unix/" + adminSocket, "config": map[string]any{"persist": false}},
		"storage": map[string]any{"module": "file_system", "root": filepath.Join(root, "storage")},
		"logging": map[string]any{"logs": map[string]any{"default": map[string]any{"writer": map[string]any{"output": "discard"}}}},
		"apps": map[string]any{
			"tls": map[string]any{"certificates": map[string]any{"load_files": []any{map[string]any{"certificate": certFile, "key": keyFile}}}},
			"http": map[string]any{"grace_period": int64(grace), "servers": map[string]any{"fixture": map[string]any{
				"listen": []string{address}, "protocols": []string{"h1", "h2", "h3"}, "tls_connection_policies": []any{map[string]any{}}, "automatic_https": map[string]any{"disable": true},
				"routes": []any{map[string]any{"handle": []any{map[string]any{"handler": "reverse_proxy", "flush_interval": -1, "upstreams": []any{map[string]any{"dial": backend.Listener.Addr().String()}}}}}},
			}}},
		},
	}
	raw, _ := json.Marshal(config)
	configFile := filepath.Join(root, "caddy.json")
	if err = os.WriteFile(configFile, raw, 0600); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), grace+20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestCaddyHandoffChild$")
	cmd.Env = append(os.Environ(), "FUGUE_HANDOFF_CHILD_CONFIG="+configFile)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }() // synthetic child only
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	tlsConfig := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	h2 := &http2.Transport{TLSClientConfig: tlsConfig.Clone()}
	h3 := &http3.Transport{TLSClientConfig: tlsConfig.Clone()}
	defer h2.CloseIdleConnections()
	defer h3.Close()
	clients := []*http.Client{{Transport: h2}, {Transport: h3}}
	ready := &http.Client{Transport: h2, Timeout: time.Second}
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, e := ready.Get("https://" + address + "/ready")
		if e == nil {
			resp.Body.Close()
			if resp.StatusCode != 204 {
				t.Fatal(resp.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("isolated Caddy readiness failed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	type result struct {
		protocol int
		err      error
		elapsed  time.Duration
	}
	finished := make(chan result, 2)
	responses := make([]*http.Response, 0, 2)
	for index, client := range clients {
		resp, e := client.Get("https://" + address + "/hold")
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		if resp.ProtoMajor != index+2 {
			t.Fatal("fixture negotiated the wrong protocol", resp.Proto)
		}
		prefix := make([]byte, 6)
		if _, e = io.ReadFull(resp.Body, prefix); e != nil || string(prefix) != "ready\n" {
			t.Fatal("fixture stream did not start")
		}
		responses = append(responses, resp)
	}
	start := time.Now()
	for _, resp := range responses {
		go func(resp *http.Response) {
			_, e := io.Copy(io.Discard, resp.Body)
			finished <- result{resp.ProtoMajor, e, time.Since(start)}
		}(resp)
	}
	adminTransport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", adminSocket)
	}}
	defer adminTransport.CloseIdleConnections()
	go func() {
		request, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/stop", nil)
		resp, _ := (&http.Client{Transport: adminTransport}).Do(request)
		if resp != nil {
			resp.Body.Close()
		}
	}()
	for range responses {
		select {
		case got := <-finished:
			if got.err == nil || got.elapsed < grace*9/10 {
				t.Fatalf("expected finite-grace termination of still-active synthetic HTTP/%d stream: err=%v elapsed=%s", got.protocol, got.err, got.elapsed)
			}
			t.Logf("synthetic HTTP/%d active stream ended with %T after %s; configured grace=%s", got.protocol, got.err, got.elapsed, grace)
		case <-ctx.Done():
			t.Fatal("isolated lifecycle test timed out")
		}
	}
}
