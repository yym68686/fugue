package agentedge

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
)

func TestCaptureFailureIDAcceptsOnlyOneCanonicalVerified503Value(t *testing.T) {
	const id = "abcdef0123456789abcdef0123456789"
	verified := &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{}}}}
	for _, tc := range []struct {
		name   string
		status int
		scheme string
		tls    *tls.ConnectionState
		values []string
		want   string
	}{
		{"accepted", 503, "https", verified, []string{id}, id},
		{"old server", 503, "https", verified, nil, ""},
		{"duplicate", 503, "https", verified, []string{id, id}, ""},
		{"combined", 503, "https", verified, []string{id + "," + id}, ""},
		{"short", 503, "https", verified, []string{id[:31]}, ""},
		{"malformed", 503, "https", verified, []string{"Z" + id[1:]}, ""},
		{"success", 200, "https", verified, []string{id}, ""},
		{"other error", 502, "https", verified, []string{id}, ""},
		{"plain HTTP", 503, "http", nil, []string{id}, ""},
		{"unverified TLS", 503, "https", &tls.ConnectionState{}, []string{id}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.scheme+"://api.example.test/v1/agent/edge-candidates", nil)
			res := &http.Response{StatusCode: tc.status, TLS: tc.tls, Header: make(http.Header)}
			for _, value := range tc.values {
				res.Header.Add(CaptureFailureIDHeader, value)
			}
			if got := captureFailureID(res, req); got != tc.want {
				t.Fatal("unexpected diagnostic acceptance")
			}
		})
	}
}

func TestControlCaptureFailureSurvivesSuccessfulOriginFallback(t *testing.T) {
	const id = "abcdef0123456789abcdef0123456789"
	m, g, private, _, now := controlFixture(t)
	g = activateControl(t, m, g, private, now)
	var selectedCalls, originCalls atomic.Int32
	respond := func(w http.ResponseWriter, r *http.Request, calls *atomic.Int32) {
		calls.Add(1)
		if r.Header.Get(CaptureFailureIDHeader) != "" {
			t.Error("Agent echoed a diagnostic ID as a request input")
		}
		w.Header().Set(CaptureFailureIDHeader, id)
	}
	selected, selectedRoots := edgeTLSServer(t, "api.example.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respond(w, r, &selectedCalls)
		w.WriteHeader(503)
	}))
	raw := encodeGrant(t, g, private)
	origin, originRoots := edgeTLSServer(t, "api.example.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respond(w, r, &originCalls)
		_, _ = w.Write(raw)
	}))
	dial := func(serverURL string) func(context.Context, string, string) (net.Conn, error) {
		return func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(serverURL, "https://"))
		}
	}
	selectedTransport := m.selected.Transport.(*transport)
	selectedTransport.roots, selectedTransport.dial = selectedRoots, dial(selected.URL)
	selectedTransport.now = m.now
	originTransport := m.acquisition.Transport.(*http.Transport)
	originTransport.TLSClientConfig.RootCAs, originTransport.DialContext = originRoots, dial(origin.URL)
	m.fetch = m.fetchGrant
	m.refreshAt = time.Time{}
	*now = now.Add(time.Second)
	if err := m.Step(t.Context()); err != nil {
		t.Fatal("successful fallback was changed", err)
	}
	if selectedCalls.Load() != 1 || originCalls.Load() != 1 || m.CaptureFailures() != (CaptureFailureWitnesses{SelectedEdge: id}) {
		t.Fatalf("failed selected attempt not distinguished from the successful fallback: selected=%d origin=%d witnesses=%+v", selectedCalls.Load(), originCalls.Load(), m.CaptureFailures())
	}
	if err := m.Step(t.Context()); err != nil || m.CaptureFailures() != (CaptureFailureWitnesses{}) {
		t.Fatal("capture witness leaked across steps", err)
	}
}
