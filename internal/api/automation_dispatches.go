package api

import (
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

// handleListAutomationActionDispatches exposes the durable action WAL as a
// read-only view. It follows the same tenant/project boundary as intents and
// never claims, advances, or otherwise mutates a dispatch.
func (s *Server) handleListAutomationActionDispatches(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !canReadAutomations(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing automation.read or automation.write scope")
		return
	}

	limit, err := readIntQuery(r, "limit", defaultAutomationActionDispatchListLimit)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if limit < 1 || limit > maxAutomationActionDispatchListLimit {
		httpx.WriteError(w, http.StatusBadRequest, "limit must be between 1 and 1000")
		return
	}

	tenantID := principal.TenantID
	if principal.IsPlatformAdmin() {
		tenantID = strings.TrimSpace(r.URL.Query().Get("tenant_id"))
	}
	projectID := projectIDForPrincipal(principal, r.URL.Query().Get("project_id"))
	if !principal.IsPlatformAdmin() &&
		strings.TrimSpace(principal.ProjectID) != "" &&
		projectID != principal.ProjectID {
		httpx.WriteError(w, http.StatusForbidden, "project is outside the credential boundary")
		return
	}

	dispatches, err := s.store.ListAutomationActionDispatches(store.AutomationActionDispatchFilter{
		TenantID:      tenantID,
		ProjectID:     projectID,
		PolicyID:      strings.TrimSpace(r.URL.Query().Get("policy_id")),
		AppID:         strings.TrimSpace(r.URL.Query().Get("app_id")),
		Status:        strings.TrimSpace(r.URL.Query().Get("status")),
		PlatformAdmin: principal.IsPlatformAdmin(),
		Limit:         limit,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.AutomationActionDispatchListResponse{
		Dispatches:  dispatches,
		GeneratedAt: time.Now().UTC(),
	})
}

func (s *Server) handleGetAutomationActionDispatch(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !canReadAutomations(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing automation.read or automation.write scope")
		return
	}
	dispatch, err := s.store.GetAutomationActionDispatch(
		strings.TrimSpace(r.PathValue("dispatch_id")),
		principal.TenantID,
		principal.IsPlatformAdmin(),
	)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principal.IsPlatformAdmin() && !principal.AllowsProject(dispatch.ProjectID) {
		httpx.WriteError(w, http.StatusForbidden, "automation dispatch is outside the credential boundary")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.AutomationActionDispatchResponse{Dispatch: dispatch})
}

const (
	defaultAutomationActionDispatchListLimit = 200
	maxAutomationActionDispatchListLimit     = 1000
)
