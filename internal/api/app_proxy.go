package api

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

const (
	defaultAppProxyLookupCacheTTL  = 5 * time.Second
	defaultAppProxyMaxAttempts     = 4
	defaultAppProxyRetryDelay      = 100 * time.Millisecond
	defaultAppProxyReplayBodyLimit = 512 << 10
	defaultAppProxyMaxIdleConns    = 256
	defaultAppProxyMaxIdlePerHost  = 32
)

const (
	appProxyTraceIDHeader       = "X-Fugue-Trace-Id"
	appProxyEdgeRequestIDHeader = "X-Fugue-Edge-Request-Id"
)

var appProxyRequestSequence uint64

func (s *Server) maybeHandleAppProxy(w http.ResponseWriter, r *http.Request) bool {
	startedAt := time.Now()
	host := strings.TrimSpace(strings.ToLower(r.Host))
	if host == "" {
		return false
	}
	if idx := strings.Index(host, ":"); idx >= 0 {
		host = host[:idx]
	}
	if s.isReservedAppHostname(host) || isInternalControlPlaneHost(host) {
		return false
	}
	identity := appProxyRequestIdentityForProxy(r)

	app, err := s.loadAppByRouteCached(host, r.URL.Path)
	if err != nil {
		if err == store.ErrNotFound {
			if s.isAppHostname(host) {
				s.logAppProxyObservation(appProxyObservation{
					Host:       host,
					Method:     r.Method,
					Path:       safeAppProxyLogPath(r),
					StatusCode: http.StatusNotFound,
					Duration:   time.Since(startedAt),
					WebSocket:  appProxyRequestIsWebSocket(r),
					SSE:        appProxyRequestWantsSSE(r),
					RouteState: "missing",
					TraceID:    identity.TraceID,
					RequestID:  identity.RequestID,
					EdgeReqID:  identity.EdgeRequestID,
				})
				http.NotFound(w, r)
				return true
			}
			return false
		}
		s.logAppProxyObservation(appProxyObservation{
			Host:       host,
			Method:     r.Method,
			Path:       safeAppProxyLogPath(r),
			StatusCode: http.StatusInternalServerError,
			Duration:   time.Since(startedAt),
			WebSocket:  appProxyRequestIsWebSocket(r),
			SSE:        appProxyRequestWantsSSE(r),
			RouteState: "lookup-error",
			TraceID:    identity.TraceID,
			RequestID:  identity.RequestID,
			EdgeReqID:  identity.EdgeRequestID,
		})
		http.Error(w, "app lookup failed", http.StatusInternalServerError)
		return true
	}
	app = s.overlayManagedAppStatusCached(app)
	observed := appProxyObservation{
		Host:       host,
		Method:     r.Method,
		Path:       safeAppProxyLogPath(r),
		AppID:      app.ID,
		TenantID:   app.TenantID,
		RuntimeID:  appProxyRuntimeID(app),
		WebSocket:  appProxyRequestIsWebSocket(r),
		SSE:        appProxyRequestWantsSSE(r),
		RouteState: "active",
		TraceID:    identity.TraceID,
		RequestID:  identity.RequestID,
		EdgeReqID:  identity.EdgeRequestID,
	}
	if app.Spec.Replicas == 0 {
		observed.StatusCode = http.StatusServiceUnavailable
		observed.Duration = time.Since(startedAt)
		observed.RouteState = "disabled"
		s.logAppProxyObservation(observed)
		http.Error(w, "app is disabled", http.StatusServiceUnavailable)
		return true
	}
	if app.Status.CurrentReplicas == 0 {
		observed.StatusCode = http.StatusServiceUnavailable
		observed.Duration = time.Since(startedAt)
		observed.RouteState = "unavailable"
		s.logAppProxyObservation(observed)
		http.Error(w, appRouteUnavailableMessage(app), http.StatusServiceUnavailable)
		return true
	}

	target, err := url.Parse(s.serviceURLForApp(r.Context(), app))
	if err != nil {
		observed.StatusCode = http.StatusInternalServerError
		observed.Duration = time.Since(startedAt)
		observed.RouteState = "invalid-target"
		s.logAppProxyObservation(observed)
		http.Error(w, "invalid app target", http.StatusInternalServerError)
		return true
	}
	observed.Target = target.String()
	if err := prepareAppProxyRequestForRetries(r); err != nil {
		observed.StatusCode = http.StatusBadRequest
		observed.Duration = time.Since(startedAt)
		observed.RouteState = "bad-request"
		s.logAppProxyObservation(observed)
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return true
	}
	proxy := s.newAppReverseProxy(host, target, app, &observed)
	observedWriter := newAppProxyObservationResponseWriter(w)
	proxy.ServeHTTP(observedWriter, r)
	observed.StatusCode = observedWriter.statusCode()
	if !observedWriter.wroteHeader && observed.WebSocket && observed.UpstreamError == "" {
		observed.StatusCode = http.StatusSwitchingProtocols
	}
	observed.Duration = time.Since(startedAt)
	s.logAppProxyObservation(observed)
	return true
}

func (s *Server) newAppReverseProxy(host string, target *url.URL, app model.App, observed *appProxyObservation) *httputil.ReverseProxy {
	transport := newDefaultAppProxyTransport()
	if s != nil && s.appProxyTransport != nil {
		transport = s.appProxyTransport
	}
	return &httputil.ReverseProxy{
		Rewrite: func(req *httputil.ProxyRequest) {
			req.SetURL(target)
			req.SetXForwarded()
			req.Out.Host = target.Host
			req.Out.Header.Set("X-Forwarded-Host", host)
			identity := appProxyRequestIdentityForProxy(req.In)
			applyAppProxyTraceHeaders(req.Out.Header, identity)
			if observed != nil {
				observed.TraceID = identity.TraceID
				observed.RequestID = identity.RequestID
				observed.EdgeReqID = identity.EdgeRequestID
			}
		},
		Transport: transport,
		ErrorHandler: func(rw http.ResponseWriter, req *http.Request, proxyErr error) {
			if observed != nil {
				observed.UpstreamError = strings.TrimSpace(proxyErr.Error())
				observed.RouteState = "upstream-error"
			}
			if s != nil && s.log != nil {
				s.log.Printf(
					"app proxy failed app=%s host=%s target=%s method=%s path=%s: %v",
					app.ID,
					host,
					target.String(),
					req.Method,
					safeAppProxyLogPath(req),
					proxyErr,
				)
			}
			http.Error(rw, "upstream app is unavailable", http.StatusBadGateway)
		},
		ModifyResponse: func(resp *http.Response) error {
			if observed != nil && resp != nil && strings.TrimSpace(observed.RequestID) == "" {
				observed.RequestID = appProxyRequestIDFromHeader(resp.Header)
			}
			return nil
		},
	}
}

type appProxyObservation struct {
	Host          string
	Method        string
	Path          string
	AppID         string
	TenantID      string
	RuntimeID     string
	Target        string
	StatusCode    int
	Duration      time.Duration
	RouteState    string
	UpstreamError string
	WebSocket     bool
	SSE           bool
	TraceID       string
	RequestID     string
	EdgeReqID     string
}

func (s *Server) logAppProxyObservation(observed appProxyObservation) {
	if s == nil || s.log == nil {
		return
	}
	if observed.StatusCode == 0 {
		observed.StatusCode = http.StatusOK
	}
	if observed.RouteState == "" {
		observed.RouteState = "unknown"
	}
	s.log.Printf(
		"route_a_app_proxy_request host=%s app=%s tenant=%s runtime=%s method=%s path=%s status=%d duration_ms=%d target=%s route_state=%s trace_id=%s request_id=%s edge_request_id=%s upstream_error=%t websocket=%t sse=%t",
		observed.Host,
		observed.AppID,
		observed.TenantID,
		observed.RuntimeID,
		observed.Method,
		observed.Path,
		observed.StatusCode,
		observed.Duration.Milliseconds(),
		observed.Target,
		observed.RouteState,
		strings.TrimSpace(observed.TraceID),
		strings.TrimSpace(observed.RequestID),
		strings.TrimSpace(observed.EdgeReqID),
		strings.TrimSpace(observed.UpstreamError) != "",
		observed.WebSocket,
		observed.SSE,
	)
}

type appProxyTraceIdentity struct {
	TraceID       string
	RequestID     string
	EdgeRequestID string
}

func appProxyRequestIdentityForProxy(r *http.Request) appProxyTraceIdentity {
	identity := appProxyTraceIdentity{}
	if r != nil {
		identity.TraceID = appProxyTraceIDFromHeader(r.Header)
		identity.RequestID = appProxyRequestIDFromHeader(r.Header)
		identity.EdgeRequestID = strings.TrimSpace(r.Header.Get(appProxyEdgeRequestIDHeader))
	}
	if identity.TraceID == "" {
		identity.TraceID = appProxyTraceIDForProxy()
	}
	if identity.RequestID == "" {
		identity.RequestID = identity.EdgeRequestID
	}
	if identity.RequestID == "" {
		identity.RequestID = appProxyRequestIDForProxy()
	}
	return identity
}

func applyAppProxyTraceHeaders(header http.Header, identity appProxyTraceIdentity) {
	if header == nil {
		return
	}
	if requestID := strings.TrimSpace(identity.RequestID); requestID != "" {
		header.Set("X-Request-Id", requestID)
	}
	if edgeRequestID := strings.TrimSpace(identity.EdgeRequestID); edgeRequestID != "" {
		header.Set(appProxyEdgeRequestIDHeader, edgeRequestID)
	}
	traceID := strings.TrimSpace(identity.TraceID)
	if traceID == "" {
		traceID = appProxyTraceIDForProxy()
	}
	header.Set(appProxyTraceIDHeader, traceID)
	if strings.TrimSpace(header.Get("traceparent")) == "" {
		header.Set("traceparent", appProxyTraceparentForProxy(traceID, identity.RequestID))
	}
}

func appProxyTraceIDFromHeader(header http.Header) string {
	if header == nil {
		return ""
	}
	if traceID := normalizeAppProxyTraceID(header.Get(appProxyTraceIDHeader)); traceID != "" {
		return traceID
	}
	traceparent := strings.TrimSpace(header.Get("traceparent"))
	parts := strings.Split(traceparent, "-")
	if len(parts) >= 4 {
		return normalizeAppProxyTraceID(parts[1])
	}
	return ""
}

func normalizeAppProxyTraceID(raw string) string {
	traceID := strings.ToLower(strings.TrimSpace(raw))
	if len(traceID) != 32 || appProxyAllZeroHex(traceID) {
		return ""
	}
	if _, err := hex.DecodeString(traceID); err != nil {
		return ""
	}
	return traceID
}

func appProxyRequestIDFromHeader(header http.Header) string {
	if header == nil {
		return ""
	}
	for _, headerName := range []string{"X-Request-Id", "X-Request-ID", "X-Correlation-ID"} {
		if value := strings.TrimSpace(header.Get(headerName)); value != "" {
			return value
		}
	}
	return ""
}

func appProxyTraceIDForProxy() string {
	var random [16]byte
	if _, err := rand.Read(random[:]); err == nil && !appProxyAllZeroHex(hex.EncodeToString(random[:])) {
		return hex.EncodeToString(random[:])
	}
	sequence := atomic.AddUint64(&appProxyRequestSequence, 1)
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%d", time.Now().UnixNano(), sequence)))
	return hex.EncodeToString(sum[:16])
}

func appProxyRequestIDForProxy() string {
	sequence := atomic.AddUint64(&appProxyRequestSequence, 1)
	return fmt.Sprintf("app_%x_%x", time.Now().UnixNano(), sequence)
}

func appProxyTraceparentForProxy(traceID string, requestID string) string {
	traceID = normalizeAppProxyTraceID(traceID)
	if traceID == "" {
		traceID = appProxyTraceIDForProxy()
	}
	return "00-" + traceID + "-" + appProxySpanIDForProxy(requestID) + "-01"
}

func appProxySpanIDForProxy(requestID string) string {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		requestID = appProxyRequestIDForProxy()
	}
	sum := sha256.Sum256([]byte(requestID))
	spanID := hex.EncodeToString(sum[:8])
	if appProxyAllZeroHex(spanID) {
		return "0000000000000001"
	}
	return spanID
}

func appProxyAllZeroHex(value string) bool {
	if value == "" {
		return true
	}
	for _, char := range value {
		if char != '0' {
			return false
		}
	}
	return true
}

type appProxyObservationResponseWriter struct {
	http.ResponseWriter
	wroteHeader bool
	status      int
}

func newAppProxyObservationResponseWriter(w http.ResponseWriter) *appProxyObservationResponseWriter {
	return &appProxyObservationResponseWriter{ResponseWriter: w}
}

func (w *appProxyObservationResponseWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

func (w *appProxyObservationResponseWriter) WriteHeader(statusCode int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *appProxyObservationResponseWriter) Write(data []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

func (w *appProxyObservationResponseWriter) Flush() {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *appProxyObservationResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("response writer does not support hijacking")
	}
	return hijacker.Hijack()
}

func (w *appProxyObservationResponseWriter) Push(target string, opts *http.PushOptions) error {
	pusher, ok := w.ResponseWriter.(http.Pusher)
	if !ok {
		return http.ErrNotSupported
	}
	return pusher.Push(target, opts)
}

func (w *appProxyObservationResponseWriter) ReadFrom(reader io.Reader) (int64, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if readerFrom, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		return readerFrom.ReadFrom(reader)
	}
	return io.Copy(w.ResponseWriter, reader)
}

func (w *appProxyObservationResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *appProxyObservationResponseWriter) statusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func appProxyRuntimeID(app model.App) string {
	if runtimeID := strings.TrimSpace(app.Status.CurrentRuntimeID); runtimeID != "" {
		return runtimeID
	}
	return strings.TrimSpace(app.Spec.RuntimeID)
}

func safeAppProxyLogPath(r *http.Request) string {
	if r == nil || r.URL == nil {
		return ""
	}
	path := strings.TrimSpace(r.URL.EscapedPath())
	if path == "" {
		return "/"
	}
	return path
}

func appProxyRequestIsWebSocket(r *http.Request) bool {
	if r == nil {
		return false
	}
	return headerContainsToken(r.Header, "Connection", "upgrade") &&
		strings.EqualFold(strings.TrimSpace(r.Header.Get("Upgrade")), "websocket")
}

func appProxyRequestWantsSSE(r *http.Request) bool {
	if r == nil {
		return false
	}
	return headerContainsToken(r.Header, "Accept", "text/event-stream")
}

func headerContainsToken(header http.Header, key, token string) bool {
	token = strings.ToLower(strings.TrimSpace(token))
	if token == "" {
		return false
	}
	for _, value := range header.Values(key) {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

func (s *Server) loadAppByHostnameCached(host string) (model.App, error) {
	host = strings.TrimSpace(strings.ToLower(host))
	if host == "" || s == nil || s.store == nil {
		return model.App{}, store.ErrNotFound
	}

	return s.appProxyAppCache.do(host, func() (model.App, error) {
		return s.store.GetAppByHostname(host)
	})
}

func (s *Server) loadAppByRouteCached(host, requestPath string) (model.App, error) {
	host = strings.TrimSpace(strings.ToLower(host))
	requestPath = model.NormalizeAppRoutePathPrefix(requestPath)
	if host == "" || s == nil || s.store == nil {
		return model.App{}, store.ErrNotFound
	}

	cacheKey := host + "\x00" + requestPath
	return s.appProxyAppCache.do(cacheKey, func() (model.App, error) {
		return s.store.GetAppByRoute(host, requestPath)
	})
}

func (s *Server) isAppHostname(host string) bool {
	if s.isReservedAppHostname(host) {
		return false
	}
	base := strings.TrimSpace(strings.ToLower(s.appBaseDomain))
	if base == "" {
		return false
	}
	return strings.HasSuffix(host, "."+base)
}

func isInternalControlPlaneHost(host string) bool {
	host = strings.Trim(strings.TrimSpace(strings.ToLower(host)), "[]")
	if host == "" {
		return false
	}
	if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		host = strings.Trim(strings.TrimSpace(strings.ToLower(parsedHost)), "[]")
	}
	if net.ParseIP(host) != nil {
		return true
	}
	if host == "localhost" || !strings.Contains(host, ".") {
		return true
	}
	return strings.HasSuffix(host, ".svc") ||
		strings.Contains(host, ".svc.") ||
		strings.HasSuffix(host, ".svc.cluster.local")
}

func (s *Server) serviceURLForApp(ctx context.Context, app model.App) string {
	port := 80
	if app.Route != nil && app.Route.ServicePort > 0 {
		port = app.Route.ServicePort
	} else if len(app.Spec.Ports) > 0 {
		port = app.Spec.Ports[0]
	}
	return "http://" + s.serviceHostForApp(ctx, app) + ":" + strconv.Itoa(port)
}

func (s *Server) serviceHostForApp(ctx context.Context, app model.App) string {
	cacheKey := strings.TrimSpace(app.ID + "|" + app.TenantID + "|" + app.Name)
	if cacheKey == "" {
		return s.resolveServiceHostForApp(ctx, app)
	}

	resolved, err := s.appProxyServiceHostCache.do(cacheKey, func() (string, error) {
		return s.resolveServiceHostForApp(ctx, app), nil
	})
	if err != nil || strings.TrimSpace(resolved) == "" {
		return s.resolveServiceHostForApp(ctx, app)
	}
	return resolved
}

func (s *Server) resolveServiceHostForApp(ctx context.Context, app model.App) string {
	namespace := runtime.NamespaceForTenant(app.TenantID)
	primaryHost := appServiceHost(namespace, runtime.RuntimeAppServiceName(app))
	legacyHost := appServiceHost(namespace, runtime.RuntimeServiceName(app.Name))
	if legacyHost == "" || legacyHost == primaryHost {
		return primaryHost
	}
	if s.serviceHostResolves(ctx, primaryHost) {
		return primaryHost
	}
	if s.serviceHostResolves(ctx, legacyHost) {
		return legacyHost
	}
	return primaryHost
}

func (s *Server) serviceHostResolves(ctx context.Context, host string) bool {
	host = strings.TrimSpace(host)
	if host == "" || s == nil || s.dnsResolver == nil {
		return false
	}
	addrs, err := s.dnsResolver.LookupIPAddr(ctx, host)
	return err == nil && len(addrs) > 0
}

func appServiceHost(namespace, serviceName string) string {
	serviceName = strings.TrimSpace(serviceName)
	namespace = strings.TrimSpace(namespace)
	if serviceName == "" {
		return ""
	}
	if namespace == "" {
		return serviceName
	}
	return serviceName + "." + namespace + ".svc.cluster.local"
}

func newDefaultAppProxyTransport() http.RoundTripper {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		dialer := &net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}
		return appProxyRetryTransport{
			base: &http.Transport{
				Proxy:                 nil,
				DialContext:           rootedAppProxyKubernetesServiceDialContext(dialer.DialContext),
				ForceAttemptHTTP2:     false,
				MaxIdleConns:          defaultAppProxyMaxIdleConns,
				MaxIdleConnsPerHost:   defaultAppProxyMaxIdlePerHost,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: time.Second,
			},
			maxAttempts: defaultAppProxyMaxAttempts,
			retryDelay:  defaultAppProxyRetryDelay,
		}
	}
	transport := base.Clone()
	transport.Proxy = nil
	transport.DialContext = rootedAppProxyKubernetesServiceDialContext(transport.DialContext)
	transport.ForceAttemptHTTP2 = false
	transport.DisableKeepAlives = false
	transport.MaxIdleConns = defaultAppProxyMaxIdleConns
	transport.MaxIdleConnsPerHost = defaultAppProxyMaxIdlePerHost
	return appProxyRetryTransport{
		base:        transport,
		maxAttempts: defaultAppProxyMaxAttempts,
		retryDelay:  defaultAppProxyRetryDelay,
	}
}

func rootedAppProxyKubernetesServiceDialContext(next func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		return next(ctx, network, rootedAppProxyKubernetesServiceDialAddress(address))
	}
}

func rootedAppProxyKubernetesServiceDialAddress(address string) string {
	host, port, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return address
	}
	if strings.HasSuffix(strings.ToLower(host), ".svc.cluster.local") {
		host += "."
	}
	return net.JoinHostPort(host, port)
}

type appProxyRetryTransport struct {
	base        http.RoundTripper
	maxAttempts int
	retryDelay  time.Duration
}

func (t appProxyRetryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	maxAttempts := t.maxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			if err := resetAppProxyRequestBody(req); err != nil {
				return nil, lastErr
			}
			if err := sleepBeforeAppProxyRetry(req.Context(), appProxyRetryDelayForAttempt(t.retryDelay, attempt)); err != nil {
				if lastErr != nil {
					return nil, lastErr
				}
				return nil, err
			}
		}

		resp, err := base.RoundTrip(req)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if attempt == maxAttempts || !canRetryAppProxyRequest(req) || !isTransientAppProxyRoundTripError(req.Context(), err) {
			return nil, err
		}
	}
	return nil, lastErr
}

func appProxyRetryDelayForAttempt(baseDelay time.Duration, attempt int) time.Duration {
	if baseDelay <= 0 || attempt <= 1 {
		return 0
	}
	delay := baseDelay
	for i := 2; i < attempt; i++ {
		delay *= 2
	}
	return delay
}

func sleepBeforeAppProxyRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func canRetryAppProxyRequest(req *http.Request) bool {
	if req == nil {
		return false
	}
	if req.Body == nil || req.Body == http.NoBody {
		return true
	}
	return req.GetBody != nil
}

func resetAppProxyRequestBody(req *http.Request) error {
	if req == nil || req.Body == nil || req.Body == http.NoBody {
		return nil
	}
	if req.GetBody == nil {
		return errors.New("app proxy request body is not replayable")
	}
	body, err := req.GetBody()
	if err != nil {
		return err
	}
	req.Body = body
	return nil
}

func isTransientAppProxyRoundTripError(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return isTransientAppProxyRoundTripError(ctx, urlErr.Err)
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	for _, marker := range []string{
		"connect: connection refused",
		"connection refused",
		"connection reset by peer",
		"server closed idle connection",
		"unexpected eof",
		"eof",
		"no such host",
		"no route to host",
		"network is unreachable",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func prepareAppProxyRequestForRetries(req *http.Request) error {
	if req == nil || req.GetBody != nil || req.Body == nil || req.Body == http.NoBody {
		return nil
	}
	if !shouldReplayAppProxyRequestBody(req) {
		return nil
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return err
	}
	if err := req.Body.Close(); err != nil {
		return err
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	req.ContentLength = int64(len(body))
	return nil
}

func shouldReplayAppProxyRequestBody(req *http.Request) bool {
	if req == nil || req.Body == nil || req.Body == http.NoBody || req.GetBody != nil {
		return false
	}
	if req.ContentLength <= 0 || req.ContentLength > defaultAppProxyReplayBodyLimit {
		return false
	}
	path := ""
	if req.URL != nil {
		path = req.URL.Path
	}
	if isAppProxyUpgradeRequest(req) || isAppProxyStreamingRequest(req) || isAppProxyNoReplayPath(path) {
		return false
	}
	return true
}

func isAppProxyStreamingRequest(req *http.Request) bool {
	if req == nil {
		return false
	}
	for _, value := range req.Header.Values("Accept") {
		for _, part := range strings.Split(value, ",") {
			mediaType := strings.TrimSpace(part)
			if idx := strings.Index(mediaType, ";"); idx >= 0 {
				mediaType = strings.TrimSpace(mediaType[:idx])
			}
			if strings.EqualFold(mediaType, "text/event-stream") {
				return true
			}
		}
	}
	return false
}

func isAppProxyNoReplayPath(path string) bool {
	path = strings.TrimSpace(strings.ToLower(path))
	if path == "" {
		return false
	}
	if path == "/v1/responses" || strings.HasPrefix(path, "/v1/responses/") {
		return true
	}
	if path == "/v1/images" || strings.HasPrefix(path, "/v1/images/") {
		return true
	}
	if path == "/stream" || strings.HasSuffix(path, "/stream") || strings.Contains(path, "/stream/") {
		return true
	}
	return false
}

func isAppProxyUpgradeRequest(req *http.Request) bool {
	if req == nil {
		return false
	}
	if strings.TrimSpace(req.Header.Get("Upgrade")) != "" {
		return true
	}
	for _, value := range req.Header.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
				return true
			}
		}
	}
	return false
}

func appRouteUnavailableMessage(app model.App) string {
	phase := strings.TrimSpace(app.Status.Phase)
	message := strings.TrimSpace(app.Status.LastMessage)

	switch {
	case phase != "" && message != "" && !strings.EqualFold(phase, message):
		return "app is unavailable: " + phase + ": " + message
	case message != "":
		return "app is unavailable: " + message
	case phase != "":
		return "app is unavailable: " + phase
	default:
		return "app is unavailable"
	}
}
