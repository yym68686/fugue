package api

import (
	"strings"

	"fugue/internal/model"
)

type appListFilter struct {
	TenantID  string
	Query     string
	ProjectID string
	Domain    string
	Phase     string
	SourceRef string
}

func (f appListFilter) HasAny() bool {
	return strings.TrimSpace(f.TenantID) != "" ||
		strings.TrimSpace(f.Query) != "" ||
		strings.TrimSpace(f.ProjectID) != "" ||
		strings.TrimSpace(f.Domain) != "" ||
		strings.TrimSpace(f.Phase) != "" ||
		strings.TrimSpace(f.SourceRef) != ""
}

func (f appListFilter) NeedsDomainIndex() bool {
	return strings.TrimSpace(f.Query) != "" || strings.TrimSpace(f.Domain) != ""
}

func filterAppListResults(apps []model.App, filter appListFilter, domainHostsByAppID map[string][]string) []model.App {
	out := make([]model.App, 0, len(apps))
	for _, app := range apps {
		if tenantID := strings.TrimSpace(filter.TenantID); tenantID != "" && strings.TrimSpace(app.TenantID) != tenantID {
			continue
		}
		if projectID := strings.TrimSpace(filter.ProjectID); projectID != "" && strings.TrimSpace(app.ProjectID) != projectID {
			continue
		}
		if phase := strings.TrimSpace(filter.Phase); phase != "" && !strings.EqualFold(strings.TrimSpace(app.Status.Phase), phase) {
			continue
		}
		if domain := strings.TrimSpace(filter.Domain); domain != "" && !appMatchesDomainFilter(app, domain, domainHostsByAppID[strings.TrimSpace(app.ID)]) {
			continue
		}
		if sourceRef := strings.TrimSpace(filter.SourceRef); sourceRef != "" && !appMatchesSourceFilter(app, sourceRef) {
			continue
		}
		if query := strings.TrimSpace(filter.Query); query != "" && !appMatchesQueryFilter(app, query, domainHostsByAppID[strings.TrimSpace(app.ID)]) {
			continue
		}
		out = append(out, app)
	}
	return out
}

func appMatchesQueryFilter(app model.App, query string, domainHosts []string) bool {
	query = strings.TrimSpace(query)
	if query == "" {
		return true
	}
	fields := []string{
		app.ID,
		app.Name,
		app.Description,
		app.ProjectID,
		app.TenantID,
	}
	if app.Route != nil {
		fields = append(fields, app.Route.Hostname, app.Route.PublicURL, app.Route.BaseDomain, app.Route.DomainName, app.Route.EntrypointName)
	}
	fields = append(fields, domainHosts...)
	fields = append(fields, appSourceSearchFields(app.Source)...)
	fields = append(fields, appSourceSearchFields(app.OriginSource)...)
	fields = append(fields, appSourceSearchFields(app.BuildSource)...)
	return containsFoldAny(fields, query)
}

func appMatchesDomainFilter(app model.App, domain string, domainHosts []string) bool {
	domain = strings.TrimSpace(domain)
	if domain == "" {
		return true
	}
	fields := append([]string(nil), domainHosts...)
	if app.Route != nil {
		fields = append(fields, app.Route.Hostname, app.Route.PublicURL, app.Route.BaseDomain)
	}
	return containsFoldAny(fields, domain)
}

func appMatchesSourceFilter(app model.App, sourceRef string) bool {
	sourceRef = strings.TrimSpace(sourceRef)
	if sourceRef == "" {
		return true
	}
	fields := []string{}
	fields = append(fields, appSourceSearchFields(app.Source)...)
	fields = append(fields, appSourceSearchFields(app.OriginSource)...)
	fields = append(fields, appSourceSearchFields(app.BuildSource)...)
	return containsFoldAny(fields, sourceRef)
}

func appSourceSearchFields(source *model.AppSource) []string {
	if source == nil {
		return nil
	}
	return []string{
		source.Type,
		source.RepoURL,
		source.RepoBranch,
		source.ImageRef,
		source.ResolvedImageRef,
		source.UploadID,
		source.UploadFilename,
		source.ArchiveSHA256,
		source.SourceDir,
		source.BuildStrategy,
		source.CommitSHA,
		source.DockerfilePath,
		source.BuildContextDir,
		source.ImageNameSuffix,
		source.ComposeService,
		source.DetectedProvider,
		source.DetectedStack,
	}
}

func containsFoldAny(fields []string, needle string) bool {
	needle = strings.ToLower(strings.TrimSpace(needle))
	if needle == "" {
		return true
	}
	for _, field := range fields {
		if strings.Contains(strings.ToLower(strings.TrimSpace(field)), needle) {
			return true
		}
	}
	return false
}

func principalAllowsAppID(principal model.Principal, tenantID, appID string) bool {
	if principal.IsPlatformAdmin() {
		return true
	}
	if strings.TrimSpace(tenantID) != "" && strings.TrimSpace(tenantID) != strings.TrimSpace(principal.TenantID) {
		return false
	}
	return strings.TrimSpace(appID) != ""
}
