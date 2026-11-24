package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

func (s *Server) handleRetireRecoveredBackupArtifact(w http.ResponseWriter, r *http.Request) {
	actor := mustPrincipal(r)
	if !actor.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform administrator required")
		return
	}
	var req model.RecoveredBackupRetirementRequest
	r.Body = http.MaxBytesReader(w, r.Body, 8<<10)
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 35*time.Second)
	defer cancel()
	id := r.PathValue("id")
	req.ReplacementArtifactID = strings.TrimSpace(req.ReplacementArtifactID)
	req.Reason = strings.TrimSpace(req.Reason)
	preview := req
	preview.Confirmation = ""
	result, err := s.store.RecoveredBackupRetirement(ctx, id, preview, actor)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	execute := req.Confirmation != ""
	if len(result.Blockers) == 0 && execute && req.Confirmation != result.Confirmation {
		result.Blockers = append(result.Blockers, "confirmation does not match the current preview")
	}
	if len(result.Blockers) == 0 {
		for _, candidate := range []struct{ id, role string }{{id, "recovered backup"}, {req.ReplacementArtifactID, "replacement backup"}} {
			if err := s.verifyRecoveredBackupRetirementRemote(ctx, candidate.id); err != nil {
				// Provider errors can include repository URLs. Report the failing
				// phase without returning any repository credentials or paths.
				result.Blockers = append(result.Blockers, candidate.role+": matching completed Longhorn Backup or R2 configuration could not be verified")
			}
		}
	}
	if len(result.Blockers) > 0 {
		result.Confirmation = ""
		if execute {
			httpx.WriteJSON(w, http.StatusConflict, result)
			return
		}
	} else if execute {
		result, err = s.store.RecoveredBackupRetirement(ctx, id, req, actor)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		if !result.Retired {
			httpx.WriteJSON(w, http.StatusConflict, result)
			return
		}
	}
	httpx.WriteJSON(w, http.StatusOK, result)
}

func (s *Server) verifyRecoveredBackupRetirementRemote(ctx context.Context, id string) error {
	artifact, err := s.store.GetBackupArtifact(id, "", true)
	if err != nil {
		return err
	}
	if err := s.verifyAdoptionLonghornRemote(ctx, artifact.RunID, artifact.Manifest); err != nil {
		return err
	}
	backend, err := s.store.GetBackupBackendForUse(artifact.BackendID, "", true)
	if err != nil {
		return err
	}
	objects, err := newDataObjectBackend(model.BackupBackendAsDataBackend(backend))
	if err != nil {
		return err
	}
	key, err := adoptionLonghornConfigKey(objects, artifact.Manifest)
	if err != nil {
		return err
	}
	info, found, err := objects.headObjectInfo(ctx, key)
	if err != nil {
		return err
	}
	if !found || info.ETag == "" || info.Size <= 0 {
		return errors.New("backup configuration is absent or empty")
	}
	return nil
}
