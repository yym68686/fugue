package releaseguardian

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestRegistryRehomeHTTPDirectRead(t *testing.T) {
	var redirected atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "" || r.Header.Get("Cache-Control") != "no-cache, no-store" {
			t.Errorf("unexpected probe method or headers: %s", r.Method)
		}
		switch r.URL.Path {
		case "/healthz":
			_, _ = w.Write([]byte("ready"))
		case "/redirect":
			http.Redirect(w, r, "/forbidden", http.StatusFound)
		case "/forbidden":
			redirected.Store(true)
		case "/large":
			_, _ = w.Write([]byte("ready" + strings.Repeat("x", 64<<10)))
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("ready"))
		}
	}))
	defer server.Close()
	address := server.Listener.Addr().String()
	if got, err := readRegistryRehomeHTTP(t.Context(), address, "", "/healthz", "ready"); err != nil || got != digest([]byte("ready")) {
		t.Fatalf("expected exact body digest from direct GET: %q %v", got, err)
	}
	for _, tc := range []struct{ path, marker string }{
		{"/redirect", "ready"}, {"/large", "ready"}, {"/unavailable", "ready"}, {"/healthz", "not-present"},
	} {
		if got, err := readRegistryRehomeHTTP(t.Context(), address, "", tc.path, tc.marker); err == nil || got != "" {
			t.Errorf("probe accepted an invalid response from %q: %q %v", tc.path, got, err)
		}
	}
	if redirected.Load() {
		t.Fatal("the probe followed a redirect")
	}
}
