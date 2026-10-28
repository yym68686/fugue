package routeprobe

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/routeproof"
)

func TestProbeReusesAuthenticatedConnectionButNeverProofOrNonce(t *testing.T) {
	var connections, requests atomic.Int32
	nonces := make(chan string, 5)
	expires := time.Now().UTC().Add(time.Minute)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "example.com" || r.TLS.ServerName != "example.com" || r.Method != http.MethodHead {
			t.Error("lost HTTP/TLS target identity")
		}
		nonce := r.Header.Get(routeproof.NonceHeader)
		nonces <- nonce
		if requests.Add(1) == 3 {
			nonce = "0123456789abcdef0123456789abcdef"
		}
		for k, v := range map[string]string{
			routeproof.NonceHeader: nonce, routeproof.DigestHeader: "sha256:" + strings.Repeat("a", 64),
			routeproof.VersionHeader: "actual", routeproof.ExpiryHeader: expires.Format(time.RFC3339Nano),
			routeproof.EdgeHeader: "edge-a", routeproof.GroupHeader: "cell-a", "Cache-Control": "no-store",
		} {
			w.Header().Set(k, v)
		}
		w.WriteHeader(204)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.StartTLS()
	defer server.Close()
	pool := &probeTransportPool{limit: 2}
	transport := pool.get("example.com", "8.8.8.8")
	defer transport.CloseIdleConnections()
	transport.TLSClientConfig.RootCAs = x509.NewCertPool()
	transport.TLSClientConfig.RootCAs.AddCert(server.Certificate())
	var peer *x509.Certificate
	transport.TLSClientConfig.VerifyConnection = func(state tls.ConnectionState) error {
		peer = state.VerifiedChains[0][0]
		return nil
	}
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	for i := 0; i < 2; i++ {
		proof, err := probeWithPool(context.Background(), "example.com", "/", "8.8.8.8", "", time.Second, pool)
		if err != nil || !proof.ValidUntil.Equal(expires) || proof.CheckedAt.IsZero() {
			t.Fatal("fresh nonce-bound proof failed", proof, err)
		}
	}
	if _, err := probeWithPool(context.Background(), "example.com", "/", "8.8.8.8", "", time.Second, pool); err == nil {
		t.Fatal("reused connection accepted a replayed nonce")
	}
	// Model time crossing the certificate's validity window without closing
	// the authenticated connection. A warm connection cannot extend TLS trust.
	originalNotBefore, originalNotAfter := peer.NotBefore, peer.NotAfter
	defer func() { peer.NotBefore, peer.NotAfter = originalNotBefore, originalNotAfter }()
	peer.NotAfter = time.Now().Add(-time.Second)
	if _, err := probeWithPool(context.Background(), "example.com", "/", "8.8.8.8", "", time.Second, pool); err == nil {
		t.Fatal("pooled connection outlived its certificate")
	}
	peer.NotAfter = time.Now().Add(time.Hour)
	peer.NotBefore = time.Now().Add(time.Minute)
	if _, err := probeWithPool(context.Background(), "example.com", "/", "8.8.8.8", "", time.Second, pool); err == nil {
		t.Fatal("pooled connection bypassed certificate not-before")
	}
	if connections.Load() != 1 || requests.Load() != 5 {
		t.Fatal("connection was not reused or request/proof was cached", connections.Load(), requests.Load())
	}
	seen := map[string]bool{}
	for i := 0; i < 5; i++ {
		nonce := <-nonces
		if !routeproof.ValidNonce(nonce) || seen[nonce] {
			t.Fatal("nonce reused across observations")
		}
		seen[nonce] = true
	}
}

func TestProbePoolIsBoundedAndSeparatesAddressAndTLSIdentity(t *testing.T) {
	pool := &probeTransportPool{limit: 2}
	first := pool.get("example.com", "8.8.8.8")
	if first != pool.get("example.com", "8.8.8.8") {
		t.Fatal("exact target not reused")
	}
	second := pool.get("example.com", "9.9.9.9")
	third := pool.get("other.example.com", "9.9.9.9")
	if first == second || second == third || len(pool.entries) != 2 || pool.lru.Len() != 2 {
		t.Fatal("pool mixed physical/TLS identities or exceeded bound")
	}
	if first == pool.get("example.com", "8.8.8.8") {
		t.Fatal("evicted target remained retained")
	}
	for _, transport := range []*http.Transport{first, second, third} {
		defer transport.CloseIdleConnections()
		if transport.Proxy != nil || transport.TLSClientConfig.InsecureSkipVerify || transport.TLSClientConfig.MinVersion < tls.VersionTLS12 || transport.IdleConnTimeout <= 0 || transport.MaxResponseHeaderBytes != 16<<10 {
			t.Fatal("pool relaxed transport security or resource bounds")
		}
	}
}

func TestProbePoolBoundsConnectionAffinityAcrossBackendHandoff(t *testing.T) {
	for _, http2 := range []bool{false, true} {
		name := "http1"
		if http2 {
			name = "http2"
		}
		t.Run(name, func(t *testing.T) {
			backend := func(version string) *httptest.Server {
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if (r.ProtoMajor == 2) != http2 {
						t.Error("fixture did not negotiate the requested HTTP version", r.Proto)
					}
					for key, value := range map[string]string{
						routeproof.NonceHeader:   r.Header.Get(routeproof.NonceHeader),
						routeproof.DigestHeader:  "sha256:" + strings.Repeat("a", 64),
						routeproof.VersionHeader: version,
						routeproof.ExpiryHeader:  time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano),
						routeproof.EdgeHeader:    "edge-a", routeproof.GroupHeader: "cell-a", "Cache-Control": "no-store",
					} {
						w.Header().Set(key, value)
					}
					w.WriteHeader(http.StatusNoContent)
				}))
				server.EnableHTTP2 = http2
				server.StartTLS()
				t.Cleanup(server.Close)
				return server
			}
			old, current := backend("previous"), backend("current")
			var target atomic.Value
			target.Store(old.Listener.Addr().String())
			pool := &probeTransportPool{limit: 2}
			configured := map[*http.Transport]bool{}
			observe := func() Proof {
				transport := pool.get("example.com", "8.8.8.8")
				if !configured[transport] {
					configured[transport] = true
					t.Cleanup(transport.CloseIdleConnections)
					transport.TLSClientConfig.RootCAs = x509.NewCertPool()
					transport.TLSClientConfig.RootCAs.AddCert(old.Certificate())
					transport.TLSClientConfig.RootCAs.AddCert(current.Certificate())
					// An L4 front chooses the backend when a connection is created,
					// not for each request carried by that connection.
					transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
						return (&net.Dialer{}).DialContext(ctx, network, target.Load().(string))
					}
				}
				proof, err := probeWithPool(context.Background(), "example.com", "/", "8.8.8.8", "", time.Second, pool)
				if err != nil {
					t.Fatal(err)
				}
				return proof
			}
			if proof := observe(); proof.Version != "previous" {
				t.Fatal("initial backend was not observed", proof)
			}
			target.Store(current.Listener.Addr().String())
			if proof := observe(); proof.Version != "previous" {
				t.Fatal("fixture did not preserve connection-level affinity", proof)
			}
			// Advance only the pool entry's age. No sleeps, clock changes, or
			// changes to the proof/TLS validity or nonce requirements.
			pool.mu.Lock()
			entry := pool.entries["example.com\x008.8.8.8"]
			aged := entry.Value.(probeTransportEntry)
			aged.createdAt = time.Now().Add(-probeTransportMaxAge - time.Second)
			entry.Value = aged
			pool.mu.Unlock()
			if proof := observe(); proof.Version != "current" {
				t.Fatal("continuously reused connection hid the backend handoff beyond its maximum age", proof)
			}
			if len(pool.entries) != 1 || pool.lru.Len() != 1 {
				t.Fatal("rotation leaked pool entries")
			}
		})
	}
}
