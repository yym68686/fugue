package edge

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	edgePeerDNSBudget     = time.Second
	edgePeerDialBudget    = time.Second
	edgePeerTLSBudget     = 2 * time.Second
	edgePeerHeadersBudget = 3 * time.Second
)

var edgeNonPublicCGNAT = netip.MustParsePrefix("100.64.0.0/10")

func configureEdgeHTTP1(transport *http.Transport) {
	// Clone can inherit DefaultTransport's populated ALPN list without
	// copying its lazily-installed h2 handler. Align ALPN with the wire.
	transport.ForceAttemptHTTP2 = false
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetHTTP1(true)
	transport.TLSNextProto = nil
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	}
	transport.TLSClientConfig.NextProtos = []string{"http/1.1"}
}

func (s *Service) peerFallbackAllowed(r *http.Request, observed edgeProxyObservation) bool {
	if !s.Config.PeerFallbackEnabled || r == nil ||
		strings.TrimSpace(r.Header.Get(edgePeerFallbackHeader)) != "" ||
		observed.Streaming || observed.Upload || observed.HealthProbe || observed.InternalWarmup ||
		(r.Body != nil && r.Body != http.NoBody) {
		return false
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	default:
		return false
	}
}

func (s *Service) resolvePeer(ctx context.Context, host string) (netip.Addr, string) {
	dnsHost := strings.TrimSuffix(host, ".")
	if dnsHost == "" {
		return netip.Addr{}, "host_missing"
	}
	if _, err := netip.ParseAddr(dnsHost); err == nil {
		return netip.Addr{}, "host_is_ip"
	}
	self4, _ := netip.ParseAddr(strings.TrimSpace(s.Config.PublicIPv4))
	self6, _ := netip.ParseAddr(strings.TrimSpace(s.Config.PublicIPv6))
	self4, self6 = self4.Unmap(), self6.Unmap()
	if !self4.Is4() && !self6.Is6() {
		return netip.Addr{}, "self_address_unknown"
	}
	lookup := s.peerLookupIP
	if lookup == nil {
		lookup = net.DefaultResolver.LookupNetIP
	}
	dnsContext, cancel := context.WithTimeout(ctx, edgePeerDNSBudget)
	defer cancel()
	// Resolve only the already selected public route, not a destination
	// supplied by a request. DNS remains the existing peer eligibility input.
	answers, err := lookup(dnsContext, "ip", dnsHost+".")
	if err != nil {
		return netip.Addr{}, "peer_lookup_" + originTransportErrorKind(err)
	}
	for _, answer := range answers {
		ip := answer.Unmap()
		if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.Zone() != "" || edgeNonPublicCGNAT.Contains(ip) ||
			ip == self4 || ip == self6 {
			continue
		}
		// Without our own address in this family, this could still be us.
		if (ip.Is4() && self4.Is4()) || (ip.Is6() && self6.Is6()) {
			return ip, ""
		}
	}
	return netip.Addr{}, "no_distinct_peer"
}

func (s *Service) proxyPeerFallback(w http.ResponseWriter, r *http.Request, host string, route model.EdgeRouteBinding, observed *edgeProxyObservation) bool {
	if r.Context().Err() != nil {
		return false
	}
	base, ok := s.proxyBase.(*http.Transport)
	if !ok || base.DialTLS != nil || base.DialTLSContext != nil {
		observed.PeerFallbackSkip = "peer_transport_unavailable"
		return false
	}
	lookupStarted := time.Now()
	ip, skip := s.resolvePeer(r.Context(), host)
	dnsDuration := time.Since(lookupStarted)
	if skip != "" {
		observed.PeerFallbackSkip = skip
		return false
	}
	if r.Context().Err() != nil {
		return false
	}
	transport := base.Clone()
	configureEdgeHTTP1(transport)
	transport.Proxy = nil
	dial := base.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		name, port, err := net.SplitHostPort(address)
		if err != nil || name != host || port != "443" {
			return nil, fmt.Errorf("peer fallback transport destination mismatch")
		}
		ctx, cancel := context.WithTimeout(ctx, edgePeerDialBudget)
		defer cancel()
		return dial(ctx, network, net.JoinHostPort(ip.String(), port))
	}
	transport.TLSClientConfig.ServerName = host
	transport.TLSHandshakeTimeout = edgePeerTLSBudget
	transport.ResponseHeaderTimeout = edgePeerHeadersBudget
	defer transport.CloseIdleConnections()

	// Keep both Host and TLS SNI; only the socket destination is pinned.
	target := &url.URL{Scheme: "https", Host: host}
	peerRequest := r.Clone(r.Context())
	peerRequest.Header.Set(edgePeerFallbackHeader, "1")
	peerRequest.Header.Set("X-Fugue-Edge-Route-Host", host)
	peer := &edgeProxyObservation{
		Route: route, Host: host, Proxied: true, PeerFallback: true,
		EdgeRequestID: observed.EdgeRequestID, TraceID: observed.TraceID,
		RequestID: observed.RequestID, ReleaseID: observed.ReleaseID, ReleaseRole: observed.ReleaseRole,
		OriginDNS: dnsDuration,
	}
	peer.initializeOriginTrace()
	writer := newEdgeProxyObservationResponseWriter(w, observed.ReceivedAt, peer)
	observed.PeerFallback = true
	observed.FallbackHit = true
	observed.PeerAddress = ip.String()
	observed.peerObservation = peer
	proxy := newEdgeReverseProxy(host, target, route, peer, true, nil, &edgeProxyTransport{base: transport, observation: peer}, s.Config.EdgeID)
	proxy.ServeHTTP(writer, peerRequest)
	return writer.wroteHeader
}

type edgeOriginAttempt struct {
	Kind              string `json:"kind"`
	PeerIP            string `json:"peer_ip,omitempty"`
	RemoteAddr        string `json:"remote_addr,omitempty"`
	HTTPStatus        int    `json:"http_status,omitempty"`
	Connected         bool   `json:"connected"`
	DNSFailed         bool   `json:"dns_failed"`
	ErrorType         string `json:"error_type,omitempty"`
	TLSProtocol       string `json:"tls_protocol,omitempty"`
	TLSErrorKind      string `json:"tls_error_kind,omitempty"`
	TransportError    string `json:"transport_error_kind,omitempty"`
	ResponseBodyError string `json:"response_body_error_kind,omitempty"`
	DNSMillis         int64  `json:"dns_ms"`
	ConnectMillis     int64  `json:"connect_ms"`
	TTFBMillis        int64  `json:"ttfb_ms"`
	UpstreamMillis    int64  `json:"upstream_ms"`
}

func (o edgeProxyObservation) originAttempt(kind, peerIP string) edgeOriginAttempt {
	return edgeOriginAttempt{
		Kind: kind, PeerIP: peerIP, RemoteAddr: o.OriginRemoteAddr,
		HTTPStatus: o.StatusCode, Connected: o.OriginGotConn, DNSFailed: o.OriginDNSError != "",
		ErrorType: edgeObservationErrorType(o), TLSProtocol: o.OriginTLSProtocol,
		TLSErrorKind: o.OriginTLSErrorKind, TransportError: o.OriginTransportErrorKind,
		ResponseBodyError: o.OriginResponseBodyErrorKind, DNSMillis: o.OriginDNS.Milliseconds(),
		ConnectMillis: o.OriginConnect.Milliseconds(), TTFBMillis: o.OriginTTFB.Milliseconds(),
		UpstreamMillis: o.Upstream.Milliseconds(),
	}
}

// Use only the final attempt for legacy flat origin fields and attribution.
// Build this from independent snapshots at request completion, including panics
// during a response body copy; late primary callbacks cannot mutate the peer.
func (o *edgeProxyObservation) completePeerOutcome() {
	if o.peerObservation == nil {
		return
	}
	peer := o.peerObservation.originTraceSnapshot()
	primary := o.originAttempt("primary", "")
	// The handler may already have sent a synthetic 502 or marked a cancel.
	// Neither is an HTTP response received from the primary upstream.
	primary.HTTPStatus, primary.ErrorType = 0, "upstream_error"
	alternate := peer.originAttempt("peer", o.PeerAddress)
	if peer.UpstreamError != "" {
		alternate.HTTPStatus = 0 // transport failure, not a response from the peer
	}
	o.originAttempts = []edgeOriginAttempt{primary, alternate}
	o.ClientCanceled = o.ClientCanceled || peer.ClientCanceled
	if peer.StatusCode != 0 && !peer.ClientCanceled {
		o.StatusCode, o.ResponseBytes = peer.StatusCode, peer.ResponseBytes
	}
	o.RequestID, o.SSE, o.Streaming = peer.RequestID, peer.SSE, peer.Streaming
	o.TTFB, o.Upstream, o.ResponseWrite = peer.TTFB, peer.Upstream, peer.ResponseWrite
	o.OriginDNS, o.OriginDNSError = peer.OriginDNS, peer.OriginDNSError
	o.OriginConnect, o.OriginConnectError = peer.OriginConnect, peer.OriginConnectError
	o.OriginGotConn, o.OriginConnectionReused = peer.OriginGotConn, peer.OriginConnectionReused
	o.OriginConnectionWasIdle, o.OriginConnectionIdle = peer.OriginConnectionWasIdle, peer.OriginConnectionIdle
	o.OriginRemoteAddr, o.OriginLocalAddr = peer.OriginRemoteAddr, peer.OriginLocalAddr
	o.OriginWroteHeaders, o.OriginWroteRequest = peer.OriginWroteHeaders, peer.OriginWroteRequest
	o.OriginRequestWrite, o.OriginRequestWriteErr = peer.OriginRequestWrite, peer.OriginRequestWriteErr
	o.OriginTTFB, o.OriginTotal, o.UpstreamError = peer.OriginTTFB, peer.OriginTotal, peer.UpstreamError
	o.OriginTLSProtocol, o.OriginTLSErrorKind = peer.OriginTLSProtocol, peer.OriginTLSErrorKind
	o.OriginTransportErrorKind, o.OriginResponseBodyErrorKind = peer.OriginTransportErrorKind, peer.OriginResponseBodyErrorKind
	o.ResponseWriteErrorKind = peer.ResponseWriteErrorKind
}
