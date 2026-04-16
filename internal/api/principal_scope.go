package api

import (
	"strings"

	"fugue/internal/model"
)

func principalAllowsProject(principal model.Principal, project model.Project) bool {
	if principal.IsPlatformAdmin() {
		return true
	}
	if strings.TrimSpace(project.TenantID) != "" && project.TenantID != principal.TenantID {
		return false
	}
	return principal.AllowsProject(project.ID)
}

func principalAllowsApp(principal model.Principal, app model.App) bool {
	if principal.IsPlatformAdmin() {
		return true
	}
	if strings.TrimSpace(app.TenantID) != "" && app.TenantID != principal.TenantID {
		return false
	}
	return principal.AllowsProject(app.ProjectID)
}

func filterProjectsForPrincipal(principal model.Principal, projects []model.Project) []model.Project {
	if principal.IsPlatformAdmin() || strings.TrimSpace(principal.ProjectID) == "" {
		return projects
	}
	filtered := make([]model.Project, 0, len(projects))
	for _, project := range projects {
		if principalAllowsProject(principal, project) {
			filtered = append(filtered, project)
		}
	}
	return filtered
}

func filterAppsForPrincipal(principal model.Principal, apps []model.App) []model.App {
	if principal.IsPlatformAdmin() || strings.TrimSpace(principal.ProjectID) == "" {
		return apps
	}
	filtered := make([]model.App, 0, len(apps))
	for _, app := range apps {
		if principalAllowsApp(principal, app) {
			filtered = append(filtered, app)
		}
	}
	return filtered
}

func projectIDForPrincipal(principal model.Principal, requested string) string {
	requested = strings.TrimSpace(requested)
	if strings.TrimSpace(principal.ProjectID) != "" && requested == "" {
		return principal.ProjectID
	}
	return requested
}
