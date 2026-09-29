package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"github.com/caddyserver/caddy/v2"
	"golang.org/x/net/http2"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestActualCaddyConfigWithObservedHTTPTransport(t *testing.T) {
	store, socket := collector(t)
	h := observation(t, socket, true)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Request-ID", "config-fixture")
		io.Copy(w, r.Body)
	}))
	defer backend.Close()
	reserved, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	addr := reserved.Addr().String()
	reserved.Close()
	cfg := map[string]any{"admin": map[string]any{"disabled": true, "config": map[string]any{"persist": false}}, "apps": map[string]any{"http": map[string]any{"servers": map[string]any{"fixture": map[string]any{
		"listen": []string{addr}, "protocols": []string{"h1", "h2c"}, "fugue_observations": true, "automatic_https": map[string]any{"disable": true},
		"routes": []any{map[string]any{"handle": []any{map[string]any{"handler": "fugue_observation", "node_id": h.NodeID, "hop": "entry", "build": "fixture", "config_digest": "sha256:fixture", "socket": socket, "correlation_key_file": h.CorrelationKeyFile, "entry": true}, map[string]any{"handler": "reverse_proxy", "upstreams": []any{map[string]any{"dial": strings.TrimPrefix(backend.URL, "http://")}}, "transport": map[string]any{"protocol": "fugue_observed_http", "versions": []string{"1.1"}}, "flush_interval": -1}}}},
	}}}}}
	cfg["storage"] = map[string]any{"module": "file_system", "root": t.TempDir()}
	raw, e := json.Marshal(cfg)
	if e != nil {
		t.Fatal(e)
	}
	if e = caddy.Load(raw, true); e != nil {
		t.Fatal(e)
	}
	defer caddy.Stop()
	tr := &http2.Transport{AllowHTTP: true, DialTLSContext: func(ctx context.Context, network, address string, _ *tls.Config) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	resp, e := client.Post("http://"+addr, "application/octet-stream", strings.NewReader("config-body"))
	if e != nil {
		t.Fatal(e)
	}
	body, e := io.ReadAll(resp.Body)
	resp.Body.Close()
	if e != nil || string(body) != "config-body" || resp.ProtoMajor != 2 {
		t.Fatal(e, string(body), resp.Proto)
	}
	values := records(t, store, resp.Header.Get("X-Fugue-Observation-ID"))
	found := false
	for _, r := range values {
		if r.Hop == "entry" && r.Coverage.HTTP2Frames && r.HTTP2 != nil {
			found = true
		}
	}
	if !found {
		t.Fatal("real Caddy server did not install protocol hook")
	}
}
