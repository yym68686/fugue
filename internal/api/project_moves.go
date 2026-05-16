package api

import (
	"errors"
	"net/http"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) handleMoveAppProject(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("project.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or project.write scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	var req store.AppProjectMoveOptions
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	preview := req
	preview.DryRun = true
	plan, err := s.store.MoveAppProject(app.ID, preview)
	if !s.writeProjectMoveStoreError(w, plan, err) {
		return
	}
	if !principalAllowsProjectMovePlan(principal, plan) {
		httpx.WriteError(w, http.StatusForbidden, "target project is not visible to this key")
		return
	}
	if !req.DryRun {
		plan, err = s.store.MoveAppProject(app.ID, req)
		if !s.writeProjectMoveStoreError(w, plan, err) {
			return
		}
	}
	action := "app.move_project.plan"
	if !req.DryRun {
		action = "app.move_project"
	}
	s.appendAudit(principal, action, "app", app.ID, app.TenantID, map[string]string{
		"app_id":            app.ID,
		"source_project_id": app.ProjectID,
		"target_projects":   strings.Join(projectMovePlanProjectIDs(plan.TargetProjects), ","),
		"dry_run":           boolString(req.DryRun),
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"plan": sanitizeProjectMovePlanForAPI(plan)})
}

func (s *Server) handleMoveBackingServiceProject(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("project.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or project.write scope")
		return
	}
	service, allowed := s.loadAuthorizedBackingService(w, r, principal)
	if !allowed {
		return
	}
	sourceProject, err := s.store.GetProject(service.ProjectID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principalAllowsProject(principal, sourceProject) {
		httpx.WriteError(w, http.StatusForbidden, "backing service project is not visible to this key")
		return
	}
	var req store.BackingServiceProjectMoveOptions
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	preview := req
	preview.DryRun = true
	plan, err := s.store.MoveBackingServiceProject(service.ID, preview)
	if !s.writeProjectMoveStoreError(w, plan, err) {
		return
	}
	if !principalAllowsProjectMovePlan(principal, plan) {
		httpx.WriteError(w, http.StatusForbidden, "target project is not visible to this key")
		return
	}
	if !req.DryRun {
		plan, err = s.store.MoveBackingServiceProject(service.ID, req)
		if !s.writeProjectMoveStoreError(w, plan, err) {
			return
		}
	}
	action := "backing_service.move_project.plan"
	if !req.DryRun {
		action = "backing_service.move_project"
	}
	s.appendAudit(principal, action, "backing_service", service.ID, service.TenantID, map[string]string{
		"service_id":        service.ID,
		"source_project_id": service.ProjectID,
		"target_projects":   strings.Join(projectMovePlanProjectIDs(plan.TargetProjects), ","),
		"dry_run":           boolString(req.DryRun),
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"plan": sanitizeProjectMovePlanForAPI(plan)})
}

func (s *Server) handlePlanProjectSplit(w http.ResponseWriter, r *http.Request) {
	s.handleProjectSplit(w, r, true)
}

func (s *Server) handleSplitProject(w http.ResponseWriter, r *http.Request) {
	s.handleProjectSplit(w, r, false)
}

func (s *Server) handleProjectSplit(w http.ResponseWriter, r *http.Request, dryRun bool) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("project.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing project.write scope")
		return
	}
	project, allowed := s.loadAuthorizedProject(w, r, principal)
	if !allowed {
		return
	}
	var req store.ProjectSplitOptions
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.DryRun = dryRun || req.DryRun
	preview := req
	preview.DryRun = true
	plan, err := s.store.SplitProject(project.ID, preview)
	if !s.writeProjectMoveStoreError(w, plan, err) {
		return
	}
	if !principalAllowsProjectMovePlan(principal, plan) {
		httpx.WriteError(w, http.StatusForbidden, "target project is not visible to this key")
		return
	}
	if !req.DryRun {
		plan, err = s.store.SplitProject(project.ID, req)
		if !s.writeProjectMoveStoreError(w, plan, err) {
			return
		}
	}
	action := "project.split.plan"
	if !req.DryRun {
		action = "project.split"
	}
	s.appendAudit(principal, action, "project", project.ID, project.TenantID, map[string]string{
		"source_project_id": project.ID,
		"target_projects":   strings.Join(projectMovePlanProjectIDs(plan.TargetProjects), ","),
		"dry_run":           boolString(req.DryRun),
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"plan": sanitizeProjectMovePlanForAPI(plan)})
}

func (s *Server) writeProjectMoveStoreError(w http.ResponseWriter, plan store.ProjectMovePlan, err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, store.ErrConflict) && len(plan.Blockers) > 0 {
		httpx.WriteError(w, http.StatusConflict, strings.Join(plan.Blockers, "; "))
		return false
	}
	s.writeStoreError(w, err)
	return false
}

func principalAllowsProjectMovePlan(principal model.Principal, plan store.ProjectMovePlan) bool {
	if principal.IsPlatformAdmin() {
		return true
	}
	if strings.TrimSpace(plan.SourceProject.ID) != "" && !principalAllowsProject(principal, plan.SourceProject) {
		return false
	}
	for _, project := range plan.TargetProjects {
		if !principalAllowsProject(principal, project) {
			return false
		}
	}
	for _, project := range plan.CreatedProjects {
		if !principalAllowsProject(principal, project) {
			return false
		}
	}
	return true
}

func sanitizeProjectMovePlanForAPI(plan store.ProjectMovePlan) store.ProjectMovePlan {
	out := plan
	out.Apps = sanitizeAppsForAPI(plan.Apps)
	out.BackingServices = cloneBackingServices(plan.BackingServices)
	out.Bindings = cloneServiceBindings(plan.Bindings)
	return out
}

func projectMovePlanProjectIDs(projects []model.Project) []string {
	ids := make([]string, 0, len(projects))
	for _, project := range projects {
		if strings.TrimSpace(project.ID) != "" {
			ids = append(ids, strings.TrimSpace(project.ID))
		}
	}
	return ids
}
