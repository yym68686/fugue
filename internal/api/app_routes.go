package api

import (
	"errors"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

var appRouteLabelPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

type appRouteAvailability struct {
	Input      string `json:"input,omitempty"`
	Label      string `json:"label,omitempty"`
	Hostname   string `json:"hostname,omitempty"`
	PathPrefix string `json:"path_prefix,omitempty"`
	BaseDomain string `json:"base_domain,omitempty"`
	PublicURL  string `json:"public_url,omitempty"`
	Valid      bool   `json:"valid"`
	Available  bool   `json:"available"`
	Current    bool   `json:"current"`
	Reason     string `json:"reason,omitempty"`
}

func (s *Server) createAppWithAutoRoute(tenantID, projectID, name, description string, spec model.AppSpec, source *model.AppSource) (model.App, error) {
	if !model.AppManagedRouteEnabled(spec) {
		if source != nil {
			return s.store.CreateImportedAppWithoutRoute(tenantID, projectID, name, description, spec, *source)
		}
		return s.store.CreateApp(tenantID, projectID, name, description, spec)
	}

	appName := strings.TrimSpace(name)
	if appName == "" {
		appName = "app"
	}
	if err := s.ensureAppNameAvailable(tenantID, projectID, appName); err != nil {
		return model.App{}, err
	}

	baseName := normalizeImportBaseName(appName)
	if baseName == "" {
		baseName = "app"
	}

	for attempt := 0; attempt < 8; attempt++ {
		candidateHost := buildAutoRouteHostname(baseName, s.appBaseDomain, attempt)
		if s.isReservedAppHostname(candidateHost) {
			continue
		}
		_, err := s.store.GetAppByHostname(candidateHost)
		if err == nil {
			continue
		}
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return model.App{}, err
		}

		route := model.AppRoute{
			Hostname:    candidateHost,
			PathPrefix:  "/",
			BaseDomain:  s.appBaseDomain,
			PublicURL:   model.AppRoutePublicURL(candidateHost, "/"),
			ServicePort: firstServicePort(spec),
		}
		var (
			app       model.App
			createErr error
		)
		if source != nil {
			app, createErr = s.store.CreateImportedApp(tenantID, projectID, appName, description, spec, *source, route)
		} else {
			app, createErr = s.store.CreateAppWithRoute(tenantID, projectID, appName, description, spec, route)
		}
		if createErr == nil {
			return app, nil
		}
		if !errors.Is(createErr, store.ErrConflict) {
			return model.App{}, createErr
		}
	}

	return model.App{}, store.ErrConflict
}

func (s *Server) handleGetAppRouteAvailability(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	if !model.AppExposesPublicService(app.Spec) {
		httpx.WriteError(w, http.StatusBadRequest, "app does not expose a public service")
		return
	}

	availability, err := s.inspectAppRouteAvailability(app, r.URL.Query().Get("hostname"), r.URL.Query().Get("path_prefix"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"availability": availability,
	})
}

func (s *Server) handlePatchAppRoute(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	if !model.AppExposesPublicService(app.Spec) {
		httpx.WriteError(w, http.StatusBadRequest, "app does not expose a public service")
		return
	}

	var req struct {
		Hostname   string `json:"hostname"`
		PathPrefix string `json:"path_prefix"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	availability, err := s.inspectAppRouteAvailability(app, req.Hostname, req.PathPrefix)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !availability.Valid {
		httpx.WriteError(w, http.StatusBadRequest, availability.Reason)
		return
	}
	if !availability.Available {
		httpx.WriteError(w, http.StatusConflict, availability.Reason)
		return
	}
	if availability.Current {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"app":             sanitizeAppForAPI(app),
			"availability":    availability,
			"already_current": true,
		})
		return
	}

	updatedApp, err := s.store.UpdateAppRoute(app.ID, s.buildManagedAppRoute(app, availability.Hostname, availability.PathPrefix))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.route.patch", "app", updatedApp.ID, updatedApp.TenantID, map[string]string{"hostname": availability.Hostname, "path_prefix": availability.PathPrefix})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"app":             sanitizeAppForAPI(updatedApp),
		"availability":    availability,
		"already_current": false,
	})
}

func (s *Server) ensureAppNameAvailable(tenantID, projectID, appName string) error {
	apps, err := s.store.ListApps(tenantID, false)
	if err != nil {
		return err
	}
	for _, existing := range apps {
		if existing.ProjectID == projectID && strings.EqualFold(strings.TrimSpace(existing.Name), appName) {
			return store.ErrConflict
		}
	}
	return nil
}

func (s *Server) inspectAppRouteAvailability(app model.App, rawHostname, rawPathPrefix string) (appRouteAvailability, error) {
	availability := appRouteAvailability{
		Input:      strings.TrimSpace(rawHostname),
		PathPrefix: normalizeRequestedAppPathPrefix(rawPathPrefix),
		BaseDomain: s.appBaseDomain,
	}
	host, label, reason, err := s.resolveRequestedAppRouteHostname(app, rawHostname, availability.PathPrefix)
	if err != nil {
		return availability, err
	}
	availability.Label = label
	availability.Hostname = host
	if host != "" {
		availability.PublicURL = model.AppRoutePublicURL(host, availability.PathPrefix)
	}
	if reason != "" {
		availability.Reason = reason
		return availability, nil
	}
	if s.isReservedAppHostname(host) {
		availability.Reason = "hostname is reserved"
		return availability, nil
	}

	availability.Valid = true
	currentHost := ""
	currentPathPrefix := "/"
	if app.Route != nil {
		currentHost = strings.TrimSpace(strings.ToLower(app.Route.Hostname))
		currentPathPrefix = model.NormalizeAppRoutePathPrefix(app.Route.PathPrefix)
	}
	if currentHost == host && currentPathPrefix == availability.PathPrefix {
		availability.Available = true
		availability.Current = true
		return availability, nil
	}

	owner, err := s.store.GetAppByRoutePrefix(host, availability.PathPrefix)
	switch {
	case err == nil:
		if owner.ID == app.ID {
			availability.Available = true
			availability.Current = true
			return availability, nil
		}
		availability.Reason = "hostname is already in use"
		return availability, nil
	case errors.Is(err, store.ErrNotFound):
		availability.Available = true
		return availability, nil
	default:
		return availability, err
	}
}

func (s *Server) resolveRequestedAppRouteHostname(app model.App, rawHostname, pathPrefix string) (hostname, label, reason string, err error) {
	rawHostname = strings.TrimSpace(strings.ToLower(rawHostname))
	pathPrefix = model.NormalizeAppRoutePathPrefix(pathPrefix)
	if rawHostname == "" {
		return "", "", "hostname is required", nil
	}

	baseDomain := strings.TrimSpace(strings.ToLower(s.appBaseDomain))
	if baseDomain != "" {
		if rawHostname == baseDomain || strings.HasSuffix(rawHostname, "."+baseDomain) || !strings.Contains(rawHostname, ".") {
			hostname, label, reason := normalizeRequestedAppHostname(rawHostname, baseDomain)
			return hostname, label, reason, nil
		}
	}

	if !strings.Contains(rawHostname, ".") {
		if baseDomain == "" {
			return "", "", "app base domain is not configured", nil
		}
		return "", "", "hostname must include a subdomain", nil
	}

	hostname, reason = s.normalizeRequestedCustomDomain(rawHostname, false)
	if reason != "" {
		return "", "", reason, nil
	}

	var domain model.AppDomain
	domain, err = s.store.GetAppDomain(hostname)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return "", "", "custom domain must be attached to an app before it can receive a route", nil
		}
		return "", "", "", err
	}
	if domain.Status != model.AppDomainStatusVerified {
		return "", "", "custom domain must be verified before it can receive a route", nil
	}

	owner, err := s.store.GetApp(domain.AppID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return "", "", "custom domain owner app was not found", nil
		}
		return "", "", "", err
	}
	if owner.TenantID != app.TenantID || owner.ProjectID != app.ProjectID {
		return "", "", "custom domain belongs to another project", nil
	}
	if domain.AppID != app.ID && pathPrefix == "/" {
		return "", "", "custom domain root is owned by another app; choose a non-root path prefix", nil
	}
	return hostname, "", "", nil
}

func normalizeRequestedAppHostname(raw, baseDomain string) (hostname, label, reason string) {
	baseDomain = strings.TrimSpace(strings.ToLower(baseDomain))
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return "", "", "hostname is required"
	}
	if baseDomain == "" {
		return "", "", "app base domain is not configured"
	}
	label = raw
	if strings.HasSuffix(raw, "."+baseDomain) {
		label = strings.TrimSuffix(raw, "."+baseDomain)
	}
	label = strings.Trim(label, ".")
	if label == "" {
		return "", "", "hostname must include a subdomain"
	}
	if strings.Contains(label, ".") {
		return "", label, "hostname must be a single label under " + baseDomain
	}
	if !appRouteLabelPattern.MatchString(label) {
		return "", label, "hostname must use lowercase letters, numbers, or hyphens, and cannot start or end with a hyphen"
	}
	return label + "." + baseDomain, label, ""
}

func normalizeRequestedAppPathPrefix(raw string) string {
	return model.NormalizeAppRoutePathPrefix(raw)
}

func buildAutoRouteHostname(baseName, baseDomain string, attempt int) string {
	hostBase := baseName
	if attempt > 0 {
		suffix := randomHostnameWord()
		maxBaseLen := 50 - len(suffix) - 1
		if maxBaseLen < 8 {
			maxBaseLen = 8
		}
		hostBase = truncateSlug(baseName, maxBaseLen) + "-" + suffix
	}
	return hostBase + "." + strings.TrimSpace(strings.ToLower(baseDomain))
}

func (s *Server) buildManagedAppRoute(app model.App, hostname, pathPrefix string) model.AppRoute {
	pathPrefix = model.NormalizeAppRoutePathPrefix(pathPrefix)
	return model.AppRoute{
		Hostname:    hostname,
		PathPrefix:  pathPrefix,
		BaseDomain:  s.appBaseDomain,
		PublicURL:   model.AppRoutePublicURL(hostname, pathPrefix),
		ServicePort: firstServicePort(app.Spec),
	}
}

func firstServicePort(spec model.AppSpec) int {
	return model.AppPublicServicePort(spec)
}

func (s *Server) isReservedAppHostname(host string) bool {
	host = strings.TrimSpace(strings.ToLower(host))
	if host == "" {
		return false
	}
	_, exists := s.reservedAppHosts[host]
	return exists
}

func registryHostFromPushBase(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return ""
	}
	if strings.Contains(raw, "://") {
		parsed, err := url.Parse(raw)
		if err == nil {
			return strings.TrimSpace(strings.ToLower(parsed.Hostname()))
		}
	}
	host := raw
	if idx := strings.Index(host, "/"); idx >= 0 {
		host = host[:idx]
	}
	if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		host = parsedHost
	}
	return strings.Trim(strings.TrimSpace(strings.ToLower(host)), "[]")
}

func reservedAppHosts(hosts ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(hosts))
	for _, host := range hosts {
		host = registryHostFromPushBase(host)
		if host == "" {
			continue
		}
		out[host] = struct{}{}
	}
	return out
}
