package edge

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"fugue/internal/edge/relayroute"
)

const relayPermitHeader = "X-Fugue-Origin-Relay-Permit"
const relayDispatchHeader = "X-Fugue-Origin-Relay-Dispatch"
const relayMaxBudgetMS = int64(24 * time.Hour / time.Millisecond)
const relayBodyReadTimeout = 30 * time.Second

type relayPermit struct {
	binding  relayroute.Binding
	source   relayroute.Endpoint
	hash     [32]byte
	request  relayRequest
	expires  time.Time
	deadline time.Time
}

type relayDispatchKey struct{ source, dispatch string }

type relayDecisionEvidence struct {
	at                  time.Time
	local, link, egress relayroute.Health
}

func (e *relayDecisionEvidence) logFields() map[string]any {
	out := map[string]any{}
	for name, health := range map[string]relayroute.Health{"direct": e.local, "peer_link": e.link, "peer_origin": e.egress} {
		if !health.At.IsZero() {
			out[name] = map[string]any{"state": health.State, "age_ms": e.at.Sub(health.At).Milliseconds(), "rounds": health.Samples}
		}
	}
	return out
}

func relayRandomID() string {
	var key [16]byte
	_, _ = rand.Read(key[:])
	return hex.EncodeToString(key[:])
}

func relayValidID(id string) bool { return relayValidHex(id, 16) }

func relayValidHex(id string, size int) bool {
	if len(id) != size*2 || strings.ToLower(id) != id {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

func stripOriginRelayHeaders(h http.Header) {
	for key := range h {
		if strings.HasPrefix(strings.ToLower(key), "x-fugue-origin-relay-") {
			delete(h, key)
		}
	}
}

func (e *edgeOriginRelay) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	p := e.current(now)
	if p == nil || r.Method != http.MethodPost || r.TLS == nil || len(r.TLS.VerifiedChains) == 0 ||
		len(r.TLS.PeerCertificates) == 0 || now.Before(r.TLS.PeerCertificates[0].NotBefore) ||
		!now.Before(r.TLS.PeerCertificates[0].NotAfter) || r.URL.RawPath != "" || r.Header.Get("Upgrade") != "" {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	var source relayroute.Endpoint
	for _, ep := range p.Endpoints {
		if relayCertificateName(r.TLS.PeerCertificates[0], ep.TLSName) {
			if source.Edge != "" {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			source = ep
		}
	}
	if source.Edge == "" || source.Edge == e.s.Config.EdgeID {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	switch r.URL.Path {
	case relayPrefix + "forward":
		e.receiveForward(w, r, p, source, now)
	case relayPrefix + "state", relayPrefix + "prepare":
		deadline := http.NewResponseController(w)
		_ = deadline.SetReadDeadline(now.Add(relayControlTimeout))
		defer deadline.SetReadDeadline(time.Time{})
		var request relayRequest
		if r.URL.RawQuery != "" || r.ContentLength < 1 || r.ContentLength > 2048 ||
			relayroute.Decode(http.MaxBytesReader(w, r.Body, 2048), &request) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		e.receiveControl(w, r, p, source, request, time.Now())
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (e *edgeOriginRelay) receiveControl(w http.ResponseWriter, r *http.Request, p *relayroute.Policy, source relayroute.Endpoint, request relayRequest, now time.Time) {
	binding, ok := p.Binding(request.Binding)
	index := e.s.currentRouteIndex()
	if !ok || request.PolicyHash != hex.EncodeToString(p.Hash[:]) ||
		binding.Source != source.Edge || binding.Peer != e.s.Config.EdgeID || !e.serving(index) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	bound, ok := e.bound(index, binding, false, now)
	if !ok || !e.samePolicy(p, now) {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	health := e.originHealth(bound.probe, now)
	reply := relayReply{Process: e.process, State: health.State, Samples: health.Samples, RTTMillis: health.RTT.Milliseconds()}
	if !health.At.IsZero() {
		reply.AgeMillis = now.Sub(health.At).Milliseconds()
	}
	e.mu.Lock()
	for id, permit := range e.permits {
		if !now.Before(permit.expires) {
			delete(e.permits, id)
		}
	}
	if now.Sub(e.sweptAt) >= time.Second {
		for key, expiry := range e.dispatches {
			if !now.Before(expiry) {
				delete(e.dispatches, key)
			}
		}
		e.sweptAt = now
	}
	reply.Capacity = max(0, p.MaxConcurrent-e.inbound-len(e.permits))
	if len(e.dispatches) >= 8192 {
		reply.Capacity = 0
	}
	if r.URL.Path == relayPrefix+"prepare" {
		key := relayDispatchKey{source.Edge, request.Dispatch}
		_, duplicate := e.dispatches[key]
		if p.Mode != "active" || request.Bytes < 1 || request.Bytes > binding.MaxBodyBytes || request.BudgetMS < 0 || request.BudgetMS > relayMaxBudgetMS ||
			!relayValidID(request.Dispatch) || !relayValidHex(request.QueryHash, sha256.Size) ||
			health.State != relayroute.Healthy || reply.Capacity <= 0 || duplicate {
			e.mu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		reply.Permit = relayRandomID()
		permit := relayPermit{binding: binding, source: source, hash: p.Hash, request: request, expires: minTime(now.Add(5*time.Second), p.Expires)}
		if request.BudgetMS > 0 {
			permit.deadline = now.Add(time.Duration(request.BudgetMS) * time.Millisecond)
			permit.expires = minTime(permit.expires, permit.deadline)
		}
		e.permits[reply.Permit] = permit
		// Retain the dispatch identity beyond the one-shot permit, even if
		// the response is lost or the signed policy changes.
		e.dispatches[key] = now.Add(15 * time.Minute)
	}
	e.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(reply)
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func (e *edgeOriginRelay) samePolicy(p *relayroute.Policy, now time.Time) bool {
	cur := e.current(now)
	return cur != nil && cur.Hash == p.Hash
}

func (e *edgeOriginRelay) consumePermit(token string, source relayroute.Endpoint, p *relayroute.Policy, r *http.Request, now time.Time) (relayPermit, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	permit, ok := e.permits[token]
	if !ok || p.Mode != "active" || permit.hash != p.Hash || permit.source != source || !now.Before(permit.expires) ||
		r.ContentLength != permit.request.Bytes || r.Header.Get(relayDispatchHeader) != permit.request.Dispatch ||
		relayQueryHash(r.URL.RawQuery) != permit.request.QueryHash || e.inbound >= p.MaxConcurrent {
		return relayPermit{}, false
	}
	delete(e.permits, token)
	e.inbound++
	return permit, true
}

func (e *edgeOriginRelay) receiveForward(w http.ResponseWriter, r *http.Request, p *relayroute.Policy, source relayroute.Endpoint, now time.Time) {
	token := r.Header.Get(relayPermitHeader)
	if !relayValidID(token) || r.ContentLength <= 0 || len(r.URL.RawQuery) > 4096 ||
		len(r.TransferEncoding) != 0 || r.Header.Get("Expect") != "" || !relayValidQuery(r.URL.RawQuery) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	permit, ok := e.consumePermit(token, source, p, r, now)
	if !ok {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	defer func() { e.mu.Lock(); e.inbound--; e.mu.Unlock() }()
	index := e.s.currentRouteIndex()
	bound, ok := e.bound(index, permit.binding, false, now)
	if !ok || !e.serving(index) || !e.samePolicy(p, time.Now()) {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if e.originHealth(bound.probe, time.Now()).State != relayroute.Healthy {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	ctx := r.Context()
	if !permit.deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, permit.deadline)
		defer cancel()
	}
	bodyDeadline := now.Add(relayBodyReadTimeout)
	if deadline, ok := ctx.Deadline(); ok {
		bodyDeadline = minTime(bodyDeadline, deadline)
	}
	controller := http.NewResponseController(w)
	if err := controller.SetReadDeadline(bodyDeadline); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	defer controller.SetReadDeadline(time.Time{})
	r = r.Clone(ctx)
	r.URL.Path = permit.binding.Path
	r.Host = permit.binding.Host
	r.GetBody = nil
	r.Body = &relayDeadlineBody{ReadCloser: http.MaxBytesReader(w, r.Body, r.ContentLength), remaining: r.ContentLength, clear: func() {
		_ = controller.SetReadDeadline(time.Time{})
	}}
	stripOriginRelayHeaders(r.Header)
	observed := edgeProxyObservation{ReceivedAt: now.UTC(), Host: permit.binding.Host, Route: bound.route, BundleVersion: bound.probe.version,
		ReleaseID: bound.upstream.ReleaseID, ReleaseRole: bound.upstream.Role, Method: r.Method, Path: permit.binding.Path,
		EdgeRequestID: edgeRequestIDFromRequest(r), TraceID: edgeTraceIDForProxy(r)}
	// Do not reenter handleProxy. It would rebalance releases, permit a
	// second relay and count B as another public request.
	proxy := e.s.newEdgeReverseProxy(permit.binding.Host, bound.target, bound.route, &observed, false, nil)
	rewrite := proxy.Rewrite
	proxy.Rewrite = func(pr *httputil.ProxyRequest) {
		rewrite(pr)
		for _, name := range []string{"X-Forwarded-For", "X-Forwarded-Proto", "X-Forwarded-Host"} {
			pr.Out.Header.Del(name)
			if v := pr.In.Header.Values(name); len(v) > 0 {
				pr.Out.Header[name] = append([]string(nil), v...)
			}
		}
		stripOriginRelayHeaders(pr.Out.Header)
		pr.Out.GetBody = nil
	}
	writer := newEdgeProxyObservationResponseWriter(w, now, &observed)
	defer func() {
		hop := observed.originTraceSnapshot()
		if e.s.Logger != nil {
			writeEdgeStructuredLog(e.s.Logger.Writer(), map[string]any{
				"event_type": "origin_relay_egress", "dispatch_id": permit.request.Dispatch, "edge_id": e.s.Config.EdgeID,
				"source_edge": source.Edge, "binding_id": permit.binding.ID, "edge_request_id": hop.EdgeRequestID,
				"trace_id": hop.TraceID, "origin_connect_ms": hop.OriginConnect.Milliseconds(),
				"origin_write_ms": hop.OriginRequestWrite.Milliseconds(), "origin_ttfb_ms": hop.OriginTTFB.Milliseconds(),
				"duration_ms": time.Since(now).Milliseconds(), "status": writer.statusCode(),
				"origin_transport_error_kind": hop.OriginTransportErrorKind, "origin_body_error_kind": hop.OriginResponseBodyErrorKind,
				"response_write_error_kind": hop.ResponseWriteErrorKind, "canceled": r.Context().Err() != nil,
			})
		}
	}()
	proxy.ServeHTTP(writer, r)
}

// tryForward may return false until the sole business dispatch is committed.
// After that point its answer is unconditionally true (or ReverseProxy aborts
// the downstream stream); it must not hand control back to the direct path.
func (e *edgeOriginRelay) tryForward(w *edgeProxyObservationResponseWriter, r *http.Request, host string, o *edgeProxyObservation) bool {
	now := time.Now()
	p := e.current(now)
	if p == nil || r.Method != http.MethodPost || !o.RequestBodyBuffered || !o.RequestBodyEOF ||
		r.ContentLength <= 0 || r.URL.RawPath != "" || len(r.Trailer) != 0 ||
		o.HealthProbe || o.InternalWarmup || o.FallbackHit || o.WebSocket || r.Context().Err() != nil {
		return false
	}
	index := e.s.currentRouteIndex()
	var local relayroute.Health
	var selected []relayroute.Candidate
	bindings := map[string]relayroute.Binding{}
	var probed bool
	for _, b := range p.Bindings {
		if b.Source != e.s.Config.EdgeID || b.Host != host || b.Path != r.URL.Path || r.ContentLength > b.MaxBodyBytes {
			continue
		}
		o.RelayMode = p.Mode
		bound, ok := e.bound(index, b, true, now)
		if !ok || o.NetworkRoute == nil || bound.probe.version != o.BundleVersion || bound.probe.digest != relayRouteDigest(*o.NetworkRoute) || bound.upstream.ReleaseID != o.ReleaseID {
			o.RelayReason = "route_binding_mismatch"
			continue
		}
		if !probed {
			local = e.originHealth(bound.probe, now)
			probed = true
		}
		peer, _ := p.Endpoint(b.Peer)
		e.mu.Lock()
		row := e.remote[b.ID]
		e.mu.Unlock()
		if row.binding != b || row.endpoint != peer || row.at.After(now) || now.Sub(row.at) >= relayRemoteTTL {
			continue
		}
		selected = append(selected, relayroute.Candidate{ID: b.Peer, Link: row.link.Health(now), Origin: row.origin, Capacity: row.capacity})
		bindings[b.Peer] = b
	}
	if !probed {
		return false
	}
	o.RelayMode = p.Mode
	o.RelayLocalState = string(local.State)
	plan := relayroute.Select(now, local, selected)
	o.RelayReason, o.RelayPeer = plan.Reason, plan.Peer
	evidence := &relayDecisionEvidence{at: now, local: local}
	for _, candidate := range selected {
		if candidate.ID == plan.Peer {
			evidence.link, evidence.egress = candidate.Link, candidate.Origin
		}
	}
	o.RelayEvidence = evidence
	if plan.Peer == "" {
		return false
	}
	if !relayValidQuery(r.URL.RawQuery) {
		o.RelayReason = "unsupported_request_target"
		return false
	}
	if !e.serving(index) {
		o.RelayReason = "source_not_serving"
		return false
	}
	if p.Mode == "shadow" {
		return false
	}
	id := relayRandomID()
	if !relayInCanary(id, p.RelayPercent) {
		o.RelayReason = "outside_canary"
		return false
	}
	var budgetMS int64
	if deadline, ok := r.Context().Deadline(); ok {
		budgetMS = time.Until(deadline).Milliseconds()
		if budgetMS <= relayControlTimeout.Milliseconds() || budgetMS > relayMaxBudgetMS {
			o.RelayReason = "unsupported_request_budget"
			return false
		}
	}
	e.mu.Lock()
	if e.outbound >= p.MaxConcurrent {
		e.mu.Unlock()
		o.RelayReason = "source_capacity"
		return false
	}
	e.outbound++
	e.mu.Unlock()
	defer func() { e.mu.Lock(); e.outbound--; e.mu.Unlock() }()
	peer, _ := p.Endpoint(plan.Peer)
	b := bindings[peer.Edge]
	started := time.Now()
	reply, err := e.control(r.Context(), peer, "prepare", relayRequest{
		Binding: b.ID, PolicyHash: hex.EncodeToString(p.Hash[:]), Dispatch: id, QueryHash: relayQueryHash(r.URL.RawQuery), Bytes: r.ContentLength, BudgetMS: budgetMS,
	})
	o.RelayPrepare = time.Since(started)
	if err != nil || !relayValidID(reply.Permit) || r.Context().Err() != nil ||
		!e.samePolicy(p, time.Now()) || !e.serving(index) {
		o.RelayReason = "prepare_unavailable"
		return false
	}
	o.RelayDispatched, o.RelayDispatchID = true, id
	e.sendForward(w, r, o, peer, reply.Permit)
	return true
}

func relayValidQuery(query string) bool {
	if len(query) > 4096 {
		return false
	}
	_, err := url.ParseQuery(query)
	return err == nil
}

type relayDeadlineBody struct {
	io.ReadCloser
	remaining int64
	clear     func()
}

func (b *relayDeadlineBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	if b.clear != nil && (b.remaining <= 0 || err != nil) {
		b.clear()
		b.clear = nil
	}
	return n, err
}

type relayResponseBody struct {
	io.ReadCloser
	trailer http.Header
}

func (b relayResponseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		stripOriginRelayHeaders(b.trailer)
	}
	return n, err
}

func relayInCanary(id string, percent int) bool {
	if percent <= 0 {
		return false
	}
	h := sha256.Sum256([]byte(id))
	return (int(h[0])*256+int(h[1]))%100 < percent
}

func (e *edgeOriginRelay) sendForward(w *edgeProxyObservationResponseWriter, r *http.Request, o *edgeProxyObservation, peer relayroute.Endpoint, permit string) {
	hop := edgeProxyObservation{}
	hop.initializeOriginTrace()
	proxy := httputil.ReverseProxy{
		Transport: &edgeProxyTransport{base: e.client(peer).Transport, observation: &hop},
		Rewrite: func(pr *httputil.ProxyRequest) {
			setEdgeXForwarded(pr)
			pr.Out.URL.Scheme = "https"
			pr.Out.URL.Host = peer.Address
			pr.Out.URL.Path = relayPrefix + "forward"
			pr.Out.URL.RawPath = ""
			// The signed request target is an exact path; preserve its
			// query byte-for-byte, including ordering and escaping.
			pr.Out.URL.RawQuery = pr.In.URL.RawQuery
			pr.Out.Host = peer.Address
			pr.Out.GetBody = nil
			// The existing upload buffer established the final seekable
			// length even if the original client used chunked framing.
			pr.Out.TransferEncoding = nil
			pr.Out.Trailer = nil
			stripOriginRelayHeaders(pr.Out.Header)
			pr.Out.Header.Del("Expect")
			for _, control := range []string{edgePeerFallbackHeader, "X-Fugue-Edge-Route-Host",
				edgeHealthProbeHeader, edgeCacheWarmupHeader, "X-Fugue-Network-Probe"} {
				pr.Out.Header.Del(control)
			}
			pr.Out.Header.Set("X-Forwarded-Host", o.Host)
			pr.Out.Header.Set(edgeRequestIDHeader, o.EdgeRequestID)
			pr.Out.Header.Set(edgeTraceIDHeader, o.TraceID)
			pr.Out.Header.Set(relayPermitHeader, permit)
			pr.Out.Header.Set(relayDispatchHeader, o.RelayDispatchID)
		},
		ModifyResponse: func(resp *http.Response) error {
			stripOriginRelayHeaders(resp.Header)
			stripOriginRelayHeaders(resp.Trailer)
			resp.Body = relayResponseBody{resp.Body, resp.Trailer}
			o.RequestID = edgeRequestIDFromHeader(resp.Header)
			if edgeResponseIsSSE(resp) {
				o.Streaming = true
				o.SSE = true
			}
			return nil
		},
		ErrorHandler: func(rw http.ResponseWriter, req *http.Request, err error) {
			o.UpstreamError = "relay dispatch failed"
			o.ClientCanceled = edgeProxyErrorIsClientCanceled(req, err)
			if o.ClientCanceled {
				rw.WriteHeader(edgeStatusClientClosedRequest)
				return
			}
			http.Error(rw, "upstream app is unavailable", http.StatusBadGateway)
		},
	}
	defer func() {
		state := hop.originTraceSnapshot()
		o.RelayConnect, o.RelayWrite, o.RelayTTFB = state.OriginConnect, state.OriginRequestWrite, state.OriginTTFB
		o.RelayError = state.OriginTransportErrorKind
		o.OriginResponseBodyErrorKind = state.OriginResponseBodyErrorKind
		o.StatusCode, o.ResponseBytes = w.statusCode(), w.bytesWritten
	}()
	proxy.ServeHTTP(w, r)
}
