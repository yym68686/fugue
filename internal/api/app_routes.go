package api

import (
	"errors"
	"net"
	"net/url"
	"strings"

	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) createAppWithAutoRoute(tenantID, projectID, name, description string, spec model.AppSpec) (model.App, error) {
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

		app, createErr := s.store.CreateAppWithRoute(tenantID, projectID, appName, description, spec, model.AppRoute{
			Hostname:    candidateHost,
			BaseDomain:  s.appBaseDomain,
			PublicURL:   "https://" + candidateHost,
			ServicePort: firstServicePort(spec),
		})
		if createErr == nil {
			return app, nil
		}
		if !errors.Is(createErr, store.ErrConflict) {
			return model.App{}, createErr
		}
	}

	return model.App{}, store.ErrConflict
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

func firstServicePort(spec model.AppSpec) int {
	if len(spec.Ports) > 0 && spec.Ports[0] > 0 {
		return spec.Ports[0]
	}
	return 80
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
