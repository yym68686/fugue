package api

import (
	"net/http"

	"fugue/internal/httpx"
)

func (s *Server) handleListBackingServices(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	services, err := s.store.ListBackingServices(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"backing_services": cloneBackingServices(services),
	})
}

func (s *Server) handleGetBackingService(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	service, err := s.store.GetBackingService(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principal.IsPlatformAdmin() && service.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "backing service is not visible to this tenant")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"backing_service": cloneBackingService(service),
	})
}

func (s *Server) handleListAppBindings(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"bindings":         cloneServiceBindings(app.Bindings),
		"backing_services": cloneBackingServices(app.BackingServices),
	})
}
