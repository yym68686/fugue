package api

import (
	"net/http"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) handleListNodeDeepHealth(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	results, err := s.store.ListNodeDeepHealthResults()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.NodeDeepHealthListResponse{
		Results:     results,
		GeneratedAt: time.Now().UTC(),
	})
}

func (s *Server) handleGetNodeDeepHealth(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	result, err := s.store.GetNodeDeepHealthResult(r.PathValue("node_updater_id"))
	if err != nil {
		if err == store.ErrNotFound {
			httpx.WriteError(w, http.StatusNotFound, "node deep health result not found")
			return
		}
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.NodeDeepHealthResponse{Result: result})
}
