package api

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/auth"
	"fugue/internal/backupadapter"
	"fugue/internal/httpx"
	"fugue/internal/store"
)

// handleGetBackupRunObservation is the fixed-purpose compatibility bridge
// consumed by the isolated backup observer. It performs only bounded store
// reads and pure contract translation; it never appends audit state, claims a
// run, renews a lease, executes a backup, or touches object storage.
func (s *Server) handleGetBackupRunObservation(w http.ResponseWriter, r *http.Request) {
	claims, ok := auth.BackupObserverIdentityFromContext(r.Context())
	if !ok {
		// Generated routing must always install RequireBackupObserver. Keep a
		// fail-closed guard here so direct handler use cannot accidentally
		// expose the legacy store.
		httpx.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	runID := r.PathValue("run")
	requestedSpecDigest, ok := exactBackupObservationSpecDigest(r.URL)
	if runID == "" || strings.TrimSpace(runID) != runID || !ok {
		httpx.WriteError(w, http.StatusBadRequest, "invalid backup observation precondition")
		return
	}
	if claims.RunID != runID || claims.SpecDigest != requestedSpecDigest {
		httpx.WriteError(w, http.StatusForbidden, "backup observer identity does not own this run precondition")
		return
	}

	run, err := s.store.GetBackupRun(runID, "", true)
	if err != nil {
		writeBackupObservationReadError(w, err)
		return
	}
	if strings.TrimSpace(run.TenantID) != claims.TenantID {
		// Do not reveal whether the credential named a run owned by another
		// tenant or by the platform domain.
		httpx.WriteError(w, http.StatusNotFound, "backup observation not found")
		return
	}
	backend, err := s.store.GetBackupBackendObservation(run.BackendID, "", true)
	if err != nil {
		writeBackupObservationReadError(w, err)
		return
	}
	if backend.BackendID != strings.TrimSpace(run.BackendID) ||
		(backend.TenantID != "" && backend.TenantID != strings.TrimSpace(run.TenantID)) {
		httpx.WriteError(w, http.StatusConflict, "backup observation source is inconsistent")
		return
	}
	spec, err := backupadapter.BuildShadowSpec(run, backend.Generation)
	if err != nil {
		httpx.WriteError(w, http.StatusConflict, "backup run is not representable by the observation contract")
		return
	}
	if spec.CellKey != claims.CellKey {
		httpx.WriteError(w, http.StatusForbidden, "backup observer identity does not own this backup cell")
		return
	}
	if spec.Digest != requestedSpecDigest {
		httpx.WriteError(w, http.StatusConflict, "backup observation spec digest precondition failed")
		return
	}

	artifacts, err := s.store.ListBackupArtifacts(store.BackupArtifactFilter{
		RunID:         run.ID,
		ActiveOnly:    true,
		PlatformAdmin: true,
		// The v1 contract permits exactly one current LKG artifact. Reading
		// two is sufficient to prove ambiguity without an unbounded result.
		Limit: 2,
	})
	if err != nil {
		writeBackupObservationReadError(w, err)
		return
	}
	status, err := backupadapter.BuildShadowStatus(spec, run, artifacts, time.Now().UTC())
	if err != nil {
		httpx.WriteError(w, http.StatusConflict, "backup run status is not representable by the observation contract")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, status)
}

func exactBackupObservationSpecDigest(requestURL *url.URL) (string, bool) {
	if requestURL == nil || requestURL.RawQuery == "" {
		return "", false
	}
	query, err := url.ParseQuery(requestURL.RawQuery)
	if err != nil || len(query) != 1 || query.Encode() != requestURL.RawQuery {
		return "", false
	}
	values, ok := query["spec_digest"]
	if !ok || len(values) != 1 || values[0] == "" {
		return "", false
	}
	return values[0], true
}

func writeBackupObservationReadError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		httpx.WriteError(w, http.StatusNotFound, "backup observation not found")
	case errors.Is(err, store.ErrInvalidInput), errors.Is(err, store.ErrConflict):
		httpx.WriteError(w, http.StatusConflict, "backup observation source is inconsistent")
	default:
		w.Header().Set("Retry-After", "5")
		httpx.WriteError(w, http.StatusServiceUnavailable, "backup observation source is unavailable")
	}
}
