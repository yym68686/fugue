package api

import (
	"context"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
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

	app, err := s.store.GetAppByHostname(host)
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
	app = s.overlayManagedAppStatus(r.Context(), app)
	if app.Spec.Replicas == 0 || app.Status.CurrentReplicas == 0 {
		http.Error(w, "app is disabled", http.StatusServiceUnavailable)
		return true
	}

	target, err := url.Parse(s.serviceURLForApp(r.Context(), app))
	if err != nil {
		http.Error(w, "invalid app target", http.StatusInternalServerError)
		return true
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = target.Host
		req.Header.Set("X-Forwarded-Host", host)
	}
	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, proxyErr error) {
		http.Error(rw, "upstream app is unavailable", http.StatusBadGateway)
	}
	proxy.ServeHTTP(w, r)
	return true
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
