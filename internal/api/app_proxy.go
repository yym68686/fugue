package api

import (
	"context"
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

const defaultAppProxyLookupCacheTTL = 5 * time.Second

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
		return &http.Transport{
			Proxy:               nil,
			ForceAttemptHTTP2:   false,
			MaxIdleConns:        100,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		}
	}
	transport := base.Clone()
	transport.Proxy = nil
	transport.ForceAttemptHTTP2 = false
	return transport
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
