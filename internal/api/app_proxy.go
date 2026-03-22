package api

import (
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
	if !s.isAppHostname(host) {
		return false
	}

	app, err := s.store.GetAppByHostname(host)
	if err != nil {
		if err == store.ErrNotFound {
			http.NotFound(w, r)
			return true
		}
		http.Error(w, "app lookup failed", http.StatusInternalServerError)
		return true
	}

	target, err := url.Parse(serviceURLForApp(app))
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
	if host == strings.TrimSpace(strings.ToLower(s.apiPublicDomain)) {
		return false
	}
	base := strings.TrimSpace(strings.ToLower(s.appBaseDomain))
	if base == "" {
		return false
	}
	return strings.HasSuffix(host, "."+base)
}

func serviceURLForApp(app model.App) string {
	port := 80
	if app.Route != nil && app.Route.ServicePort > 0 {
		port = app.Route.ServicePort
	} else if len(app.Spec.Ports) > 0 {
		port = app.Spec.Ports[0]
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	return "http://" + sanitizeProxyName(app.Name) + "." + namespace + ".svc.cluster.local:" + strconv.Itoa(port)
}

func sanitizeProxyName(name string) string {
	name = model.Slugify(name)
	if len(name) > 50 {
		return name[:50]
	}
	return name
}
