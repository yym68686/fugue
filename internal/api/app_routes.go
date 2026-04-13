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
	BaseDomain string `json:"base_domain,omitempty"`
	PublicURL  string `json:"public_url,omitempty"`
	Valid      bool   `json:"valid"`
	Available  bool   `json:"available"`
	Current    bool   `json:"current"`
	Reason     string `json:"reason,omitempty"`
}

func (s *Server) createAppWithAutoRoute(tenantID, projectID, name, description string, spec model.AppSpec, source *model.AppSource) (model.App, error) {
	if model.AppUsesBackgroundNetwork(spec) {
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
			BaseDomain:  s.appBaseDomain,
			PublicURL:   "https://" + candidateHost,
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

	availability, err := s.inspectAppRouteAvailability(app, r.URL.Query().Get("hostname"))
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
		Hostname string `json:"hostname"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	availability, err := s.inspectAppRouteAvailability(app, req.Hostname)
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

	updatedApp, err := s.store.UpdateAppRoute(app.ID, s.buildManagedAppRoute(app, availability.Hostname))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.route.patch", "app", updatedApp.ID, updatedApp.TenantID, map[string]string{"hostname": availability.Hostname})
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

func (s *Server) inspectAppRouteAvailability(app model.App, raw string) (appRouteAvailability, error) {
	availability := appRouteAvailability{
		Input:      strings.TrimSpace(raw),
		BaseDomain: s.appBaseDomain,
	}
	if strings.TrimSpace(s.appBaseDomain) == "" {
		availability.Reason = "app base domain is not configured"
		return availability, nil
	}

	host, label, reason := normalizeRequestedAppHostname(raw, s.appBaseDomain)
	availability.Label = label
	availability.Hostname = host
	if host != "" {
		availability.PublicURL = "https://" + host
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
	if app.Route != nil {
		currentHost = strings.TrimSpace(strings.ToLower(app.Route.Hostname))
	}
	if currentHost == host {
		availability.Available = true
		availability.Current = true
		return availability, nil
	}

	owner, err := s.store.GetAppByHostname(host)
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

func (s *Server) buildManagedAppRoute(app model.App, hostname string) model.AppRoute {
	return model.AppRoute{
		Hostname:    hostname,
		BaseDomain:  s.appBaseDomain,
		PublicURL:   "https://" + hostname,
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
