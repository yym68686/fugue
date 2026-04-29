package api

import (
	"net/http"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

func (s *Server) handleListProjectRuntimeReservations(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	project, allowed := s.loadAuthorizedProject(w, r, principal)
	if !allowed {
		return
	}
	reservations, err := s.store.ListProjectRuntimeReservations(project.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"runtime_reservations": reservations})
}

func (s *Server) handleReserveProjectRuntime(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("runtime.reserve") {
		httpx.WriteError(w, http.StatusForbidden, "missing runtime.reserve scope")
		return
	}
	project, allowed := s.loadAuthorizedProject(w, r, principal)
	if !allowed {
		return
	}
	var req struct {
		RuntimeID string `json:"runtime_id"`
		Mode      string `json:"mode"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if mode := strings.TrimSpace(req.Mode); mode != "" && mode != model.ProjectRuntimeReservationModeExclusive {
		httpx.WriteError(w, http.StatusBadRequest, "unsupported runtime reservation mode")
		return
	}
	reservation, err := s.store.ReserveProjectRuntime(project.ID, req.RuntimeID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "project.runtime.reserve", "project", project.ID, project.TenantID, map[string]string{"runtime_id": reservation.RuntimeID})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"runtime_reservation": reservation})
}

func (s *Server) handleDeleteProjectRuntimeReservation(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("runtime.reserve") {
		httpx.WriteError(w, http.StatusForbidden, "missing runtime.reserve scope")
		return
	}
	project, allowed := s.loadAuthorizedProject(w, r, principal)
	if !allowed {
		return
	}
	reservation, err := s.store.DeleteProjectRuntimeReservation(project.ID, r.PathValue("runtime_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "project.runtime.unreserve", "project", project.ID, project.TenantID, map[string]string{"runtime_id": reservation.RuntimeID})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"deleted": true, "runtime_reservation": reservation})
}
