package routeprobe

import (
	"context"
	"crypto/x509"
	"fugue/internal/platformconfig"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStaticProbeRequiresPublicTLSNonceAndExactAppRoute(t *testing.T) {
	p := platformconfig.DNSReadinessProbe{EdgeID: "static-a", EdgeGroupID: platformconfig.StaticEdgeDNSGroup("static-a"), Hostname: "example.com", Path: "/health", RouteDigest: "sha256:" + strings.Repeat("c", 64), StaticEdge: &platformconfig.StaticEdgeDNSProbe{StatusCode: 200}}
	mode := "valid"
	nonces := map[string]bool{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "HEAD" || r.Host != "example.com" || r.URL.Path != "/health" || r.TLS.ServerName != "example.com" {
			t.Error("probe lost exact target")
		}
		nonce := r.Header.Get(StaticNonceHeader)
		if nonces[nonce] || len(nonce) != 32 {
			t.Error("probe nonce reused")
		}
		nonces[nonce] = true
		w.Header().Set(StaticNonceHeader, nonce)
		w.Header().Set(StaticEdgeHeader, p.EdgeID)
		w.Header().Set(StaticRouteHeader, p.RouteDigest)
		w.Header().Set("Cache-Control", "no-store")
		switch mode {
		case "nonce":
			w.Header().Set(StaticNonceHeader, "wrong")
		case "app":
			w.Header().Set(StaticRouteHeader, "wrong")
		case "edge":
			w.Header().Set(StaticEdgeHeader, "other")
		case "cache":
			w.Header().Set("Cache-Control", "public")
		case "redirect":
			w.Header().Set("Location", "/another")
			w.WriteHeader(302)
			return
		}
		w.WriteHeader(200)
	}))
	defer server.Close()
	transport := &http.Transport{TLSClientConfig: server.Client().Transport.(*http.Transport).TLSClientConfig.Clone(), DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}}
	defer transport.CloseIdleConnections()
	// Test server's public test certificate includes example.com.
	transport.TLSClientConfig.ServerName = "example.com"
	for _, test := range []string{"valid", "nonce", "app", "edge", "cache", "redirect", "tls"} {
		t.Run(test, func(t *testing.T) {
			mode = test
			if test == "tls" {
				transport.CloseIdleConnections()
				transport.TLSClientConfig.RootCAs = x509.NewCertPool()
			}
			proof, err := probeStaticWithTransport(context.Background(), p, time.Second, time.Minute, transport)
			if test != "valid" {
				if err == nil {
					t.Fatal("invalid static proof accepted")
				}
				return
			}
			if err != nil || proof.Kind != StaticEdgeProofKind || proof.TrafficRelease != nil || proof.ValidUntil.After(proof.CheckedAt.Add(time.Minute)) {
				t.Fatal("valid static proof rejected or granted platform authority", proof, err)
			}
		})
	}
}
