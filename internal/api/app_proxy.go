package api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

const (
	defaultAppProxyLookupCacheTTL  = 5 * time.Second
	defaultAppProxyMaxAttempts     = 4
	defaultAppProxyRetryDelay      = 100 * time.Millisecond
	defaultAppProxyReplayBodyLimit = 8 << 20
)

func (s *Server) maybeHandleAppProxy(w http.ResponseWriter, r *http.Request) bool {
	host := strings.TrimSpace(strings.ToLower(r.Host))
	if host == "" {
		return false
	}
	if idx := strings.Index(host, ":"); idx >= 0 {
		host = host[:idx]
	}
	if s.isReservedAppHostname(host) {
		return false
	}

	app, err := s.loadAppByHostnameCached(host)
	if err != nil {
		if err == store.ErrNotFound {
			if s.isAppHostname(host) {
				http.NotFound(w, r)
				return true
			}
			return false
		}
		http.Error(w, "app lookup failed", http.StatusInternalServerError)
		return true
	}
	app = s.overlayManagedAppStatusCached(app)
	if app.Spec.Replicas == 0 {
		http.Error(w, "app is disabled", http.StatusServiceUnavailable)
		return true
	}
	if app.Status.CurrentReplicas == 0 {
		http.Error(w, appRouteUnavailableMessage(app), http.StatusServiceUnavailable)
		return true
	}

	target, err := url.Parse(s.serviceURLForApp(r.Context(), app))
	if err != nil {
		http.Error(w, "invalid app target", http.StatusInternalServerError)
		return true
	}
	if err := prepareAppProxyRequestForRetries(r); err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return true
	}
	proxy := s.newAppReverseProxy(host, target, app)
	proxy.ServeHTTP(w, r)
	return true
}

func (s *Server) newAppReverseProxy(host string, target *url.URL, app model.App) *httputil.ReverseProxy {
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
		},
		Transport: transport,
		ErrorHandler: func(rw http.ResponseWriter, req *http.Request, proxyErr error) {
			if s != nil && s.log != nil {
				s.log.Printf(
					"app proxy failed app=%s host=%s target=%s method=%s path=%s: %v",
					app.ID,
					host,
					target.String(),
					req.Method,
					req.URL.RequestURI(),
					proxyErr,
				)
			}
			http.Error(rw, "upstream app is unavailable", http.StatusBadGateway)
		},
	}
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
	primaryHost := appServiceHost(namespace, runtime.RuntimeAppResourceName(app))
	legacyHost := appServiceHost(namespace, runtime.RuntimeResourceName(app.Name))
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
		return appProxyRetryTransport{
			base: &http.Transport{
				Proxy:               nil,
				ForceAttemptHTTP2:   false,
				DisableKeepAlives:   true,
				TLSHandshakeTimeout: 10 * time.Second,
			},
			maxAttempts: defaultAppProxyMaxAttempts,
			retryDelay:  defaultAppProxyRetryDelay,
		}
	}
	transport := base.Clone()
	transport.Proxy = nil
	transport.ForceAttemptHTTP2 = false
	transport.DisableKeepAlives = true
	return appProxyRetryTransport{
		base:        transport,
		maxAttempts: defaultAppProxyMaxAttempts,
		retryDelay:  defaultAppProxyRetryDelay,
	}
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
	if isAppProxyUpgradeRequest(req) {
		return nil
	}
	if req.ContentLength <= 0 || req.ContentLength > defaultAppProxyReplayBodyLimit {
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
