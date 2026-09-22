package api

import (
	"fugue/internal/httpx"
	"net/http"
)

func (s *Server) handleComparePlatformDNSMigration(w http.ResponseWriter, r *http.Request) {
	if !mustPrincipal(r).IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	httpx.WriteError(w, http.StatusGone, "legacy migration comparison retired; inspect published traffic diagnostics, artifact lineage and runtime facts")
}
