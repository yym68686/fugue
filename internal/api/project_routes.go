package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) handleGetProjectRoutes(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	project, allowed := s.loadAuthorizedProject(w, r, principal)
	if !allowed {
		return
	}
	table, err := s.projectRouteTableView(project)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"project":     project,
		"route_table": table,
	})
}

func (s *Server) handlePutProjectRoutes(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("project.write") && !principal.HasScope("app.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing project.write or app.write scope")
		return
	}
	project, allowed := s.loadAuthorizedProject(w, r, principal)
	if !allowed {
		return
	}
	var req struct {
		Domains     []model.ProjectRouteDomain     `json:"domains"`
		Entrypoints []model.ProjectRouteEntrypoint `json:"entrypoints"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	table := model.NormalizeProjectRouteTable(model.ProjectRouteTable{
		TenantID:    project.TenantID,
		ProjectID:   project.ID,
		Domains:     req.Domains,
		Entrypoints: req.Entrypoints,
	})
	apps, err := s.store.ListAppsMetadataByProjectIDs([]string{project.ID})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	bindings, err := store.CompileProjectRouteTableBindings(table, apps)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if err := s.validateProjectRouteBindings(project, bindings); err != nil {
		if errors.Is(err, store.ErrInvalidInput) || errors.Is(err, store.ErrConflict) || errors.Is(err, store.ErrNotFound) {
			s.writeStoreError(w, err)
			return
		}
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	updated, err := s.store.PutProjectRouteTable(project.ID, table)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "project.routes.put", "project", project.ID, project.TenantID, map[string]string{"route_count": fmt.Sprintf("%d", len(updated.Bindings))})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"project":     project,
		"route_table": updated,
	})
}

func (s *Server) handleDeleteProjectRoutes(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("project.write") && !principal.HasScope("app.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing project.write or app.write scope")
		return
	}
	project, allowed := s.loadAuthorizedProject(w, r, principal)
	if !allowed {
		return
	}
	deleted, err := s.store.DeleteProjectRouteTable(project.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			table, viewErr := s.projectRouteTableView(project)
			if viewErr != nil {
				s.writeStoreError(w, viewErr)
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"project":     project,
				"route_table": table,
				"deleted":     false,
			})
			return
		}
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "project.routes.delete", "project", project.ID, project.TenantID, map[string]string{"route_count": fmt.Sprintf("%d", len(deleted.Bindings))})
	table, err := s.projectRouteTableView(project)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"project":     project,
		"route_table": table,
		"deleted":     true,
	})
}

func (s *Server) projectRouteTableView(project model.Project) (model.ProjectRouteTable, error) {
	apps, err := s.store.ListAppsMetadataByProjectIDs([]string{project.ID})
	if err != nil {
		return model.ProjectRouteTable{}, err
	}
	table, err := s.store.GetProjectRouteTable(project.ID)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return model.ProjectRouteTable{}, err
		}
		table = legacyProjectRouteTable(project, apps)
	}
	table.Bindings = store.TryCompileProjectRouteTableBindings(table, apps)
	return table, nil
}

func legacyProjectRouteTable(project model.Project, apps []model.App) model.ProjectRouteTable {
	domains := make([]model.ProjectRouteDomain, 0)
	domainNamesByHost := make(map[string]string)
	entrypointByDomain := make(map[string]int)
	entrypoints := make([]model.ProjectRouteEntrypoint, 0)
	for _, app := range apps {
		if app.Route == nil || strings.TrimSpace(app.Route.Hostname) == "" {
			continue
		}
		route := model.NormalizeAppRoute(*app.Route)
		hostname := strings.TrimSpace(strings.ToLower(route.Hostname))
		domainName := domainNamesByHost[hostname]
		if domainName == "" {
			domainName = strings.TrimSpace(route.DomainName)
			if domainName == "" {
				domainName = fmt.Sprintf("legacy-%d", len(domains)+1)
			}
			domainNamesByHost[hostname] = domainName
			domains = append(domains, model.ProjectRouteDomain{
				Name:     domainName,
				Hostname: hostname,
				Host:     hostname,
				TLS:      "auto",
			})
		}
		entrypointIndex, ok := entrypointByDomain[domainName]
		if !ok {
			entrypointName := strings.TrimSpace(route.EntrypointName)
			if entrypointName == "" {
				entrypointName = domainName
			}
			entrypoints = append(entrypoints, model.ProjectRouteEntrypoint{
				Name:   entrypointName,
				Domain: domainName,
			})
			entrypointIndex = len(entrypoints) - 1
			entrypointByDomain[domainName] = entrypointIndex
		}
		entrypoints[entrypointIndex].Routes = append(entrypoints[entrypointIndex].Routes, model.ProjectRouteEntrypointRoute{
			Path:       route.PathPrefix,
			PathPrefix: route.PathPrefix,
			Service:    app.Name,
			AppID:      app.ID,
		})
	}
	return model.NormalizeProjectRouteTable(model.ProjectRouteTable{
		TenantID:    project.TenantID,
		ProjectID:   project.ID,
		Domains:     domains,
		Entrypoints: entrypoints,
		Legacy:      true,
		CreatedAt:   project.CreatedAt,
		UpdatedAt:   project.UpdatedAt,
	})
}

func (s *Server) validateProjectRouteBindings(project model.Project, bindings []model.ProjectRouteBinding) error {
	seen := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		hostname := normalizeExternalAppDomain(binding.Hostname)
		pathPrefix := model.NormalizeAppRoutePathPrefix(binding.PathPrefix)
		if hostname == "" || pathPrefix == "" || strings.TrimSpace(binding.AppID) == "" {
			return store.ErrInvalidInput
		}
		if s.isReservedAppHostname(hostname) {
			return store.ErrConflict
		}
		key := hostname + "\x00" + pathPrefix
		if _, exists := seen[key]; exists {
			return store.ErrConflict
		}
		seen[key] = struct{}{}
		if s.projectRouteHostnameUsesPlatformTLS(hostname) {
			continue
		}
		domain, err := s.store.GetAppDomain(hostname)
		if err != nil {
			return err
		}
		if domain.TenantID != project.TenantID {
			return store.ErrNotFound
		}
		owner, err := s.store.GetApp(domain.AppID)
		if err != nil {
			return err
		}
		if owner.ProjectID != project.ID {
			return store.ErrInvalidInput
		}
		if domain.Status != model.AppDomainStatusVerified {
			return fmt.Errorf("custom domain %s must be verified before it can receive project routes", hostname)
		}
	}
	return nil
}

func (s *Server) projectRouteHostnameUsesPlatformTLS(hostname string) bool {
	hostname = normalizeExternalAppDomain(hostname)
	if hostname == "" {
		return false
	}
	if s.isPlatformOwnedDomainBinding(hostname) {
		return true
	}
	baseDomain := normalizeExternalAppDomain(s.appBaseDomain)
	return baseDomain != "" && (hostname == baseDomain || strings.HasSuffix(hostname, "."+baseDomain))
}
