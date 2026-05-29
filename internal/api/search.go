package api

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

const (
	searchDefaultLimit = 50
	searchMaxLimit     = 200
)

func (s *Server) handleSearchResources(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		httpx.WriteError(w, http.StatusBadRequest, "q is required")
		return
	}
	limit := searchDefaultLimit
	if rawLimit := strings.TrimSpace(r.URL.Query().Get("limit")); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 {
			httpx.WriteError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		if parsed > searchMaxLimit {
			parsed = searchMaxLimit
		}
		limit = parsed
	}
	types := parseSearchTypes(r.URL.Query().Get("types"))
	response, err := s.searchResources(principal, query, types, limit)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) searchResources(principal model.Principal, query string, types map[string]bool, limit int) (model.SearchResponse, error) {
	query = strings.TrimSpace(query)
	tenants, err := s.searchVisibleTenants(principal)
	if err != nil {
		return model.SearchResponse{}, err
	}
	tenantByID := make(map[string]model.Tenant, len(tenants))
	for _, tenant := range tenants {
		tenantByID[strings.TrimSpace(tenant.ID)] = tenant
	}

	projects, err := s.searchVisibleProjects(principal)
	if err != nil {
		return model.SearchResponse{}, err
	}
	projectByID := make(map[string]model.Project, len(projects))
	for _, project := range projects {
		projectByID[strings.TrimSpace(project.ID)] = project
	}

	apps, err := s.store.ListApps(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		return model.SearchResponse{}, err
	}
	apps = filterAppsForPrincipal(principal, apps)
	appByID := make(map[string]model.App, len(apps))
	for _, app := range apps {
		appByID[strings.TrimSpace(app.ID)] = app
	}

	services, err := s.store.ListBackingServices(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		return model.SearchResponse{}, err
	}
	services = filterSearchServicesForPrincipal(principal, services, projectByID)

	domains, err := s.store.ListVerifiedAppDomains()
	if err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "not found") {
			domains = nil
		}
	}
	domains = filterSearchDomainsForPrincipal(principal, domains, appByID)

	runtimes, err := s.store.ListRuntimes(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		return model.SearchResponse{}, err
	}

	results := make([]model.SearchResult, 0)
	matchedAppIDs := map[string]bool{}
	matchedProjectIDs := map[string]bool{}

	if searchTypeEnabled(types, "tenant") && principal.IsPlatformAdmin() {
		for _, tenant := range tenants {
			if score, fields := searchScore(query, map[string]string{"id": tenant.ID, "name": tenant.Name, "slug": tenant.Slug}); score > 0 {
				results = append(results, model.SearchResult{
					Kind:          "tenant",
					ID:            tenant.ID,
					Name:          tenant.Name,
					TenantID:      tenant.ID,
					TenantName:    tenant.Name,
					Status:        tenant.Status,
					Score:         score,
					MatchedFields: fields,
				})
			}
		}
	}

	if searchTypeEnabled(types, "project") {
		for _, project := range projects {
			fields := map[string]string{"id": project.ID, "name": project.Name, "slug": project.Slug, "description": project.Description, "tenant_id": project.TenantID}
			if tenant, ok := tenantByID[project.TenantID]; ok {
				fields["tenant_name"] = tenant.Name
				fields["tenant_slug"] = tenant.Slug
			}
			if score, matched := searchScore(query, fields); score > 0 {
				matchedProjectIDs[project.ID] = true
				results = append(results, decorateSearchResultContext(model.SearchResult{
					Kind:          "project",
					ID:            project.ID,
					Name:          project.Name,
					TenantID:      project.TenantID,
					ProjectID:     project.ID,
					ProjectName:   project.Name,
					Summary:       project.Description,
					Score:         score,
					MatchedFields: matched,
				}, tenantByID, projectByID, appByID))
			}
		}
	}

	if searchTypeEnabled(types, "app") {
		domainHostsByAppID := groupDomainHostsByAppID(domains)
		for _, app := range apps {
			fields := appSearchFields(app, projectByID, tenantByID, domainHostsByAppID[strings.TrimSpace(app.ID)])
			if score, matched := searchScore(query, fields); score > 0 {
				matchedAppIDs[app.ID] = true
				matchedProjectIDs[app.ProjectID] = true
				publicURL := ""
				if app.Route != nil {
					publicURL = app.Route.PublicURL
				}
				results = append(results, decorateSearchResultContext(model.SearchResult{
					Kind:          "app",
					ID:            app.ID,
					Name:          app.Name,
					TenantID:      app.TenantID,
					ProjectID:     app.ProjectID,
					AppID:         app.ID,
					AppName:       app.Name,
					PublicURL:     publicURL,
					InternalURL:   appInternalURL(app),
					Status:        app.Status.Phase,
					RuntimeID:     firstNonEmpty(app.Spec.RuntimeID, app.Status.CurrentRuntimeID),
					Summary:       app.Description,
					Score:         score,
					MatchedFields: matched,
				}, tenantByID, projectByID, appByID))
			}
		}
	}

	if searchTypeEnabled(types, "domain") {
		for _, app := range apps {
			if app.Route == nil {
				continue
			}
			fields := map[string]string{"hostname": app.Route.Hostname, "public_url": app.Route.PublicURL, "app": app.Name}
			if score, matched := searchScore(query, fields); score > 0 {
				matchedAppIDs[app.ID] = true
				matchedProjectIDs[app.ProjectID] = true
				results = append(results, decorateSearchResultContext(model.SearchResult{
					Kind:          "domain",
					ID:            app.Route.Hostname,
					Name:          app.Route.Hostname,
					TenantID:      app.TenantID,
					ProjectID:     app.ProjectID,
					AppID:         app.ID,
					AppName:       app.Name,
					PublicURL:     app.Route.PublicURL,
					Status:        "route",
					Score:         score,
					MatchedFields: matched,
				}, tenantByID, projectByID, appByID))
			}
		}
		for _, domain := range domains {
			fields := map[string]string{"hostname": domain.Hostname, "route_target": domain.RouteTarget, "status": domain.Status, "app_id": domain.AppID}
			if score, matched := searchScore(query, fields); score > 0 {
				app := appByID[strings.TrimSpace(domain.AppID)]
				matchedAppIDs[app.ID] = true
				matchedProjectIDs[app.ProjectID] = true
				results = append(results, decorateSearchResultContext(model.SearchResult{
					Kind:          "domain",
					ID:            domain.Hostname,
					Name:          domain.Hostname,
					TenantID:      domain.TenantID,
					ProjectID:     app.ProjectID,
					AppID:         app.ID,
					AppName:       app.Name,
					Status:        domain.Status,
					Ref:           domain.RouteTarget,
					Score:         score,
					MatchedFields: matched,
				}, tenantByID, projectByID, appByID))
			}
		}
	}

	if searchTypeEnabled(types, "service") {
		for _, service := range services {
			fields := map[string]string{"id": service.ID, "name": service.Name, "type": service.Type, "status": service.Status, "owner_app_id": service.OwnerAppID}
			if project, ok := projectByID[service.ProjectID]; ok {
				fields["project_name"] = project.Name
			}
			if score, matched := searchScore(query, fields); score > 0 {
				matchedProjectIDs[service.ProjectID] = true
				if service.OwnerAppID != "" {
					matchedAppIDs[service.OwnerAppID] = true
				}
				results = append(results, decorateSearchResultContext(model.SearchResult{
					Kind:          "service",
					ID:            service.ID,
					Name:          service.Name,
					TenantID:      service.TenantID,
					ProjectID:     service.ProjectID,
					AppID:         service.OwnerAppID,
					Status:        service.Status,
					Type:          service.Type,
					Summary:       service.Description,
					Score:         score,
					MatchedFields: matched,
				}, tenantByID, projectByID, appByID))
			}
		}
	}

	if searchTypeEnabled(types, "runtime") {
		for _, runtime := range runtimes {
			fields := map[string]string{"id": runtime.ID, "name": runtime.Name, "machine_name": runtime.MachineName, "cluster_node": runtime.ClusterNodeName, "type": runtime.Type, "status": runtime.Status}
			if score, matched := searchScore(query, fields); score > 0 {
				results = append(results, decorateSearchResultContext(model.SearchResult{
					Kind:          "runtime",
					ID:            runtime.ID,
					Name:          runtime.Name,
					TenantID:      runtime.TenantID,
					Status:        runtime.Status,
					Type:          runtime.Type,
					RuntimeID:     runtime.ID,
					RuntimeName:   runtime.Name,
					Score:         score,
					MatchedFields: matched,
				}, tenantByID, projectByID, appByID))
			}
		}
	}

	if searchTypeEnabled(types, "operation") {
		operations, err := s.store.ListOperationSummariesFiltered(principal.TenantID, principal.IsPlatformAdmin(), store.OperationListFilter{Limit: limit})
		if err != nil {
			return model.SearchResponse{}, err
		}
		for _, operation := range operations {
			app := appByID[strings.TrimSpace(operation.AppID)]
			if strings.TrimSpace(operation.AppID) != "" && strings.TrimSpace(app.ID) == "" {
				continue
			}
			fields := map[string]string{"id": operation.ID, "type": operation.Type, "status": operation.Status, "app_id": operation.AppID, "app": app.Name}
			score, matched := searchScore(query, fields)
			if score <= 0 && !matchedAppIDs[operation.AppID] && !matchedProjectIDs[app.ProjectID] {
				continue
			}
			if score <= 0 {
				score = 20
				matched = []string{"related_resource"}
			}
			results = append(results, decorateSearchResultContext(model.SearchResult{
				Kind:          "operation",
				ID:            operation.ID,
				Name:          operation.ID,
				TenantID:      operation.TenantID,
				ProjectID:     app.ProjectID,
				AppID:         operation.AppID,
				AppName:       app.Name,
				Status:        operation.Status,
				Type:          operation.Type,
				RuntimeID:     firstNonEmpty(operation.TargetRuntimeID, operation.AssignedRuntimeID),
				Score:         score,
				MatchedFields: matched,
			}, tenantByID, projectByID, appByID))
		}
	}

	sort.SliceStable(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		if results[i].Kind != results[j].Kind {
			return results[i].Kind < results[j].Kind
		}
		return results[i].Name < results[j].Name
	})
	if len(results) > limit {
		results = results[:limit]
	}
	return model.SearchResponse{Query: query, Types: searchTypeList(types), Results: results, Limit: limit}, nil
}

func (s *Server) searchVisibleTenants(principal model.Principal) ([]model.Tenant, error) {
	if principal.IsPlatformAdmin() {
		return s.store.ListTenants()
	}
	if strings.TrimSpace(principal.TenantID) == "" {
		return nil, nil
	}
	tenant, err := s.store.GetTenant(principal.TenantID)
	if err != nil {
		return nil, err
	}
	return []model.Tenant{tenant}, nil
}

func (s *Server) searchVisibleProjects(principal model.Principal) ([]model.Project, error) {
	var (
		projects []model.Project
		err      error
	)
	if principal.IsPlatformAdmin() {
		projects, err = s.store.ListAllProjects()
	} else if strings.TrimSpace(principal.TenantID) != "" {
		projects, err = s.store.ListProjects(principal.TenantID)
	}
	if err != nil {
		return nil, err
	}
	return filterProjectsForPrincipal(principal, projects), nil
}

func parseSearchTypes(raw string) map[string]bool {
	out := map[string]bool{}
	for _, value := range strings.Split(raw, ",") {
		value = strings.ToLower(strings.TrimSpace(value))
		if value != "" {
			out[value] = true
		}
	}
	return out
}

func searchTypeEnabled(types map[string]bool, value string) bool {
	if len(types) == 0 {
		return true
	}
	return types[value]
}

func searchTypeList(types map[string]bool) []string {
	if len(types) == 0 {
		return nil
	}
	out := make([]string, 0, len(types))
	for value := range types {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func searchScore(query string, fields map[string]string) (int, []string) {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return 0, nil
	}
	querySlug := model.Slugify(query)
	score := 0
	matched := []string{}
	for field, value := range fields {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		lower := strings.ToLower(value)
		fieldScore := 0
		switch {
		case lower == query:
			fieldScore = 100
		case querySlug != "" && model.Slugify(value) == querySlug:
			fieldScore = 92
		case strings.Contains(lower, query):
			fieldScore = 60
		}
		if fieldScore > 0 {
			matched = append(matched, field)
			if fieldScore > score {
				score = fieldScore
			}
		}
	}
	sort.Strings(matched)
	return score, matched
}

func appSearchFields(app model.App, projectByID map[string]model.Project, tenantByID map[string]model.Tenant, domainHosts []string) map[string]string {
	fields := map[string]string{
		"id":          app.ID,
		"name":        app.Name,
		"description": app.Description,
		"tenant_id":   app.TenantID,
		"project_id":  app.ProjectID,
		"status":      app.Status.Phase,
		"runtime_id":  firstNonEmpty(app.Spec.RuntimeID, app.Status.CurrentRuntimeID),
	}
	if tenant, ok := tenantByID[app.TenantID]; ok {
		fields["tenant_name"] = tenant.Name
		fields["tenant_slug"] = tenant.Slug
	}
	if project, ok := projectByID[app.ProjectID]; ok {
		fields["project_name"] = project.Name
		fields["project_slug"] = project.Slug
	}
	if app.Route != nil {
		fields["hostname"] = app.Route.Hostname
		fields["public_url"] = app.Route.PublicURL
		fields["base_domain"] = app.Route.BaseDomain
	}
	fields["internal_url"] = appInternalURL(app)
	for index, host := range domainHosts {
		fields["domain_"+strconv.Itoa(index)] = host
	}
	for index, value := range appSourceSearchFields(app.Source) {
		fields["source_"+strconv.Itoa(index)] = value
	}
	for index, value := range appSourceSearchFields(app.OriginSource) {
		fields["origin_source_"+strconv.Itoa(index)] = value
	}
	for index, value := range appSourceSearchFields(app.BuildSource) {
		fields["build_source_"+strconv.Itoa(index)] = value
	}
	return fields
}

func appInternalURL(app model.App) string {
	service := buildAppInternalService(app)
	if service == nil || strings.TrimSpace(service.Host) == "" || service.Port <= 0 {
		return ""
	}
	return fmt.Sprintf("http://%s:%d", strings.TrimSpace(service.Host), service.Port)
}

func decorateSearchResultContext(result model.SearchResult, tenantByID map[string]model.Tenant, projectByID map[string]model.Project, appByID map[string]model.App) model.SearchResult {
	if result.TenantName == "" {
		if tenant, ok := tenantByID[strings.TrimSpace(result.TenantID)]; ok {
			result.TenantName = tenant.Name
		}
	}
	if result.ProjectName == "" {
		if project, ok := projectByID[strings.TrimSpace(result.ProjectID)]; ok {
			result.ProjectName = project.Name
			if result.TenantID == "" {
				result.TenantID = project.TenantID
			}
		}
	}
	if result.AppName == "" {
		if app, ok := appByID[strings.TrimSpace(result.AppID)]; ok {
			result.AppName = app.Name
			if result.ProjectID == "" {
				result.ProjectID = app.ProjectID
			}
			if result.TenantID == "" {
				result.TenantID = app.TenantID
			}
		}
	}
	if result.Name == "" {
		result.Name = firstNonEmpty(result.AppName, result.ProjectName, result.ID)
	}
	return result
}

func filterSearchServicesForPrincipal(principal model.Principal, services []model.BackingService, projectByID map[string]model.Project) []model.BackingService {
	out := make([]model.BackingService, 0, len(services))
	for _, service := range services {
		project, ok := projectByID[strings.TrimSpace(service.ProjectID)]
		if ok && !principalAllowsProject(principal, project) {
			continue
		}
		if !ok && !principal.IsPlatformAdmin() && strings.TrimSpace(service.TenantID) != strings.TrimSpace(principal.TenantID) {
			continue
		}
		out = append(out, service)
	}
	return out
}

func filterSearchDomainsForPrincipal(principal model.Principal, domains []model.AppDomain, appByID map[string]model.App) []model.AppDomain {
	out := make([]model.AppDomain, 0, len(domains))
	for _, domain := range domains {
		app, ok := appByID[strings.TrimSpace(domain.AppID)]
		if !ok || !principalAllowsApp(principal, app) {
			continue
		}
		out = append(out, domain)
	}
	return out
}

func groupDomainHostsByAppID(domains []model.AppDomain) map[string][]string {
	out := map[string][]string{}
	for _, domain := range domains {
		appID := strings.TrimSpace(domain.AppID)
		if appID == "" {
			continue
		}
		out[appID] = append(out[appID], strings.TrimSpace(domain.Hostname))
	}
	return out
}
