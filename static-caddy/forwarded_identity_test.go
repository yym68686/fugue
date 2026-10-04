package main

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
)

// The public listener must replace client-supplied XFF. Every subsequent
// proxy must explicitly trust its authenticated predecessor or the original
// address is lost. These are real Caddy listeners, with IPv6 loopback standing
// in for the external client and IPv4 loopback for internal proxy peers.
func TestForwardedClientIPAcrossInternalProxyHops(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"xff": r.Header.Get("X-Forwarded-For"),
		})
	}))
	defer backend.Close()
	reserve := func(network, address string) string {
		t.Helper()
		listener, err := net.Listen(network, address)
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		return listener.Addr().String()
	}
	entry := reserve("tcp6", "[::1]:0")
	loopback := reserve("tcp4", "127.0.0.1:0")
	origin := reserve("tcp4", "127.0.0.1:0")
	server := func(address, upstream string, trusted bool) map[string]any {
		result := map[string]any{
			"listen": []string{address}, "protocols": []string{"h1"},
			"automatic_https": map[string]bool{"disable": true},
			"routes": []any{map[string]any{"handle": []any{map[string]any{
				"handler": "reverse_proxy", "flush_interval": -1,
				"upstreams": []any{map[string]any{"dial": upstream}},
			}}}},
		}
		if trusted {
			result["trusted_proxies"] = map[string]any{"source": "static", "ranges": []string{"127.0.0.1/32"}}
		}
		return result
	}
	defer caddy.Stop()
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	for _, test := range []struct {
		name            string
		loopbackTrusted bool
		originTrusted   bool
		want            string
	}{
		{"neither internal hop trusted", false, false, "127.0.0.1"},
		{"only origin fixed still loses client", false, true, "127.0.0.1, 127.0.0.1"},
		{"only loopback fixed still loses client", true, false, "127.0.0.1"},
		{"both internal hops preserve client", true, true, "::1, 127.0.0.1, 127.0.0.1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := map[string]any{
				"admin":   map[string]any{"disabled": true, "config": map[string]any{"persist": false}},
				"storage": map[string]any{"module": "file_system", "root": t.TempDir()},
				"apps": map[string]any{
					"tls": map[string]any{"disable_storage_clean": true},
					"http": map[string]any{"grace_period": 0, "servers": map[string]any{
						"entry":    server(entry, loopback, false),
						"loopback": server(loopback, origin, test.loopbackTrusted),
						"origin":   server(origin, strings.TrimPrefix(backend.URL, "http://"), test.originTrusted),
					}},
				},
			}
			raw, err := json.Marshal(config)
			if err != nil {
				t.Fatal(err)
			}
			if err := caddy.Load(raw, true); err != nil {
				t.Fatal(err)
			}
			for _, forged := range []bool{false, true} {
				request, err := http.NewRequest(http.MethodGet, "http://"+entry+"/identity", nil)
				if err != nil {
					t.Fatal(err)
				}
				if forged {
					request.Header.Add("X-Forwarded-For", "198.51.100.77")
					request.Header.Add("X-Forwarded-For", "203.0.113.88")
					request.Header.Set("X-Real-IP", "198.51.100.77")
				}
				response, err := client.Do(request)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || response.StatusCode != http.StatusOK {
					t.Fatalf("proxy request failed: status=%d err=%v", response.StatusCode, err)
				}
				var headers map[string]string
				if err := json.Unmarshal(body, &headers); err != nil {
					t.Fatal(err)
				}
				if headers["xff"] != test.want {
					t.Fatalf("forged=%t got XFF %q, want %q", forged, headers["xff"], test.want)
				}
			}
		})
	}
}
