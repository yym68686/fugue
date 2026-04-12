package api

import (
	"errors"
	"net/http"

	"fugue/internal/httpx"
	"fugue/internal/store"
)

func (s *Server) handleDeleteRuntime(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("runtime.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing runtime.write scope")
		return
	}

	runtimeObj, ok := s.runtimeOwner(
		w,
		principal,
		r.PathValue("id"),
		"only runtime owner can delete this server",
	)
	if !ok {
		return
	}

	deletedRuntime, err := s.store.DeleteRuntime(runtimeObj.ID)
	if err != nil {
		s.writeDeleteRuntimeError(w, err)
		return
	}

	s.appendAudit(principal, "runtime.delete", "runtime", deletedRuntime.ID, deletedRuntime.TenantID, map[string]string{
		"name": deletedRuntime.Name,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"deleted": true,
		"runtime": deletedRuntime,
	})
}

func (s *Server) writeDeleteRuntimeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		httpx.WriteError(w, http.StatusNotFound, "resource not found")
	case errors.Is(err, store.ErrConflict):
		httpx.WriteError(w, http.StatusConflict, err.Error())
	case errors.Is(err, store.ErrInvalidInput):
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
	default:
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
	}
}
