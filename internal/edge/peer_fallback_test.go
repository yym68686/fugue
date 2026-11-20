package edge

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

const peerTestHost = "example.com" // included in the httptest certificate

func TestEdgePeerFallbackUsesPinnedHTTP1AndIndependentTelemetry(t *testing.T) {
	t.Parallel()
	const (
		self = "192.0.2.10"
		peer = "192.0.2.20"
	)
	for _, tc := range []struct {
		name, method, header, wantSkip, wantClass string
		dnsAnswers                                []string
		peerStatus, wantStatus, wantDials         int
		untrusted                                 bool
	}{
		{name: "recovered", dnsAnswers: []string{"::ffff:" + self, "10.0.0.1", "100.64.0.1", "2001:db8::123", peer}, peerStatus: 200, wantStatus: 200, wantDials: 1, wantClass: model.PlatformErrorClassNone},
		{name: "peer_503_is_not_proof_of_app_failure", dnsAnswers: []string{peer}, peerStatus: 503, wantStatus: 503, wantDials: 1, wantClass: model.PlatformErrorClassEvidenceUnknown},
		{name: "untrusted_peer_fails_closed", dnsAnswers: []string{peer}, untrusted: true, wantStatus: 502, wantDials: 1, wantClass: model.PlatformErrorClassOriginUnavailable},
		{name: "no_distinct_peer", dnsAnswers: []string{self, "127.0.0.1", "100.64.0.1", "2001:db8::123"}, wantSkip: "no_distinct_peer", wantStatus: 502, wantClass: model.PlatformErrorClassOriginDNS},
		{name: "loop_marker", header: edgePeerFallbackHeader, wantStatus: 502, wantClass: model.PlatformErrorClassOriginDNS},
		{name: "health_proof", header: edgeHealthProbeHeader, wantStatus: 502, wantClass: model.PlatformErrorClassOriginDNS},
		{name: "no_post_replay", method: http.MethodPost, wantStatus: 502, wantClass: model.PlatformErrorClassOriginDNS},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var peerRequests, peerDials, lookups atomic.Int32
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				peerRequests.Add(1)
				if r.Proto != "HTTP/1.1" || r.TLS.NegotiatedProtocol != "http/1.1" || r.Host != peerTestHost || r.TLS.ServerName != peerTestHost {
					t.Errorf("unexpected peer request: protocol=%s alpn=%s host=%s sni=%s", r.Proto, r.TLS.NegotiatedProtocol, r.Host, r.TLS.ServerName)
				}
				if r.Header.Get(edgePeerFallbackHeader) != "1" || r.Header.Get(edgeTraceIDHeader) == "" || r.URL.RequestURI() != "/status?check=1" {
					t.Errorf("peer request lost loop protection, correlation or URL: %v %v", r.Header, r.URL)
				}
				w.WriteHeader(tc.peerStatus)
				_, _ = io.WriteString(w, "peer response")
			}))
			server.EnableHTTP2 = true
			server.TLS = &tls.Config{NextProtos: []string{"h2", "http/1.1"}}
			server.Config.ErrorLog = log.New(io.Discard, "", 0)
			server.StartTLS()
			defer server.Close()

			var logs bytes.Buffer
			service := NewService(config.EdgeConfig{PeerFallbackEnabled: true, PublicIPv4: self}, log.New(&logs, "", 0))
			defer service.closeEdgeProxyTransports()
			base := service.proxyBase.(*http.Transport)
			roots := x509.NewCertPool()
			if !tc.untrusted {
				roots.AddCert(server.Certificate())
			}
			base.TLSClientConfig.RootCAs = roots
			base.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				if address == "origin.example:80" {
					err := &net.DNSError{Err: "no such host", Name: "origin.example", IsNotFound: true}
					trace := httptrace.ContextClientTrace(ctx)
					trace.DNSStart(httptrace.DNSStartInfo{Host: "origin.example"})
					trace.DNSDone(httptrace.DNSDoneInfo{Err: err})
					return nil, err
				}
				peerDials.Add(1)
				if address != peer+":443" {
					t.Errorf("unexpected alternate dial: %s", address)
					return nil, net.ErrClosed
				}
				return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			}
			service.peerLookupIP = func(ctx context.Context, network, host string) ([]netip.Addr, error) {
				lookups.Add(1)
				if network != "ip" || host != peerTestHost+"." {
					t.Errorf("unexpected lookup: %s %s", network, host)
				}
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > edgePeerDNSBudget {
					t.Errorf("missing DNS budget: %s", deadline)
				}
				var answers []netip.Addr
				for _, answer := range tc.dnsAnswers {
					answers = append(answers, netip.MustParseAddr(answer))
				}
				return answers, nil
			}
			bundle := testBundle("routegen_peer_fallback")
			bundle.Routes[0].Hostname = peerTestHost
			bundle.Routes[0].UpstreamURL = "http://origin.example"
			bundle.Routes[0].UpstreamScope = "cluster_service"
			service.recordSyncSuccess(bundle, `"routegen_peer_fallback"`, time.Now().UTC(), false)
			method := tc.method
			if method == "" {
				method = http.MethodGet
			}
			req := httptest.NewRequest(method, "https://"+peerTestHost+"/status?check=1", nil)
			if tc.header != "" {
				req.Header.Set(tc.header, "1")
			}
			recorder := httptest.NewRecorder()
			service.ProxyHandler().ServeHTTP(recorder, req)
			if recorder.Code != tc.wantStatus || int(peerDials.Load()) != tc.wantDials {
				t.Fatalf("status=%d, peer dials=%d, body=%q\n%s", recorder.Code, peerDials.Load(), recorder.Body, logs.String())
			}
			if got := lookups.Load(); (got == 1) != (tc.dnsAnswers != nil) || got > 1 {
				t.Fatalf("unexpected lookup count %d", got)
			}
			if got := peerRequests.Load(); (got == 1) != (tc.peerStatus != 0) || got > 1 {
				t.Fatalf("unexpected peer request count %d", got)
			}
			var summary struct {
				ErrorClass string              `json:"platform_error_class"`
				Skip       string              `json:"peer_fallback_skip"`
				Mode       string              `json:"origin_resolution_mode"`
				DNSError   string              `json:"origin_dns_error"`
				Attempts   []edgeOriginAttempt `json:"origin_attempts"`
			}
			for _, line := range strings.Split(logs.String(), "\n") {
				var fact struct {
					Type    string `json:"event_type"`
					Summary string `json:"summary_json"`
				}
				if json.Unmarshal([]byte(line), &fact) == nil && fact.Type == "request_fact" {
					if err := json.Unmarshal([]byte(fact.Summary), &summary); err != nil {
						t.Fatal(err)
					}
				}
			}
			if summary.Skip != tc.wantSkip || summary.ErrorClass != tc.wantClass {
				t.Fatalf("unexpected summary: %+v", summary)
			}
			if tc.wantDials == 0 {
				if len(summary.Attempts) != 0 || summary.Mode != "cluster_service" || summary.DNSError == "" {
					t.Fatalf("primary diagnostics were lost: %+v", summary)
				}
				return
			}
			if len(summary.Attempts) != 2 || summary.Mode != "peer_https" || summary.DNSError != "" {
				t.Fatalf("attempts not isolated: %+v", summary)
			}
			primary, alternate := summary.Attempts[0], summary.Attempts[1]
			if primary.Kind != "primary" || !primary.DNSFailed || primary.Connected || primary.HTTPStatus != 0 || primary.TransportError != "dns_not_found" ||
				alternate.Kind != "peer" || alternate.PeerIP != peer || alternate.DNSFailed || alternate.HTTPStatus != tc.peerStatus {
				t.Fatalf("incorrect attempt details: %+v", summary.Attempts)
			}
			if tc.untrusted {
				if alternate.Connected || alternate.TLSErrorKind != "tls_certificate" {
					t.Fatalf("expected TLS verification failure: %+v", alternate)
				}
			} else if !alternate.Connected || alternate.TLSProtocol != "http/1.1" {
				t.Fatalf("expected connected HTTP/1.1 peer: %+v", alternate)
			}
		})
	}
}

func TestEdgeHTTP1ConfigurationReplacesInheritedALPN(t *testing.T) {
	t.Parallel()
	transport := &http.Transport{ForceAttemptHTTP2: true, TLSClientConfig: &tls.Config{NextProtos: []string{"h2", "http/1.1"}}}
	transport = transport.Clone()
	configureEdgeHTTP1(transport)
	if transport.Protocols.HTTP2() || !transport.Protocols.HTTP1() || transport.ForceAttemptHTTP2 || strings.Join(transport.TLSClientConfig.NextProtos, ",") != "http/1.1" {
		t.Fatalf("unexpected wire protocol configuration: %s %v", transport.Protocols, transport.TLSClientConfig.NextProtos)
	}
}
