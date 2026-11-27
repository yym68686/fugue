package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/imagetracking"
	"fugue/internal/model"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

type appImageTrackingRequest struct {
	ImageRef *string `json:"image_ref,omitempty"`
	Enabled  *bool   `json:"enabled,omitempty"`
}

type appImageTrackingResponse struct {
	AppID    string                  `json:"app_id"`
	Tracking *model.AppImageTracking `json:"tracking,omitempty"`
}

type appImageTrackingHistoryResponse struct {
	AppID    string                        `json:"app_id"`
	Tracking *model.AppImageTracking       `json:"tracking,omitempty"`
	Checks   []model.AppImageTrackingCheck `json:"checks"`
}

type appImageTrackingDiagnosis = imagetracking.Diagnosis

type appImageTrackingDiagnosisResponse struct {
	AppID     string                    `json:"app_id"`
	Diagnosis appImageTrackingDiagnosis `json:"diagnosis"`
}

type appImageSyncResponse = imagetracking.SyncResult

func (s *Server) handleGetAppImageTracking(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	tracking, err := s.imageTrackingCommands().Get(principal, app.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			httpx.WriteJSON(w, http.StatusOK, appImageTrackingResponse{AppID: app.ID})
			return
		}
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, appImageTrackingResponse{
		AppID:    app.ID,
		Tracking: &tracking,
	})
}

func (s *Server) handleGetAppImageTrackingHistory(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	var trackingPtr *model.AppImageTracking
	tracking, err := s.imageTrackingCommands().Get(principal, app.ID)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			s.writeStoreError(w, err)
			return
		}
	} else {
		trackingPtr = &tracking
	}
	checks, err := s.imageTrackingCommands().History(principal, app.ID, parseLimitQuery(r, 50))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, appImageTrackingHistoryResponse{
		AppID:    app.ID,
		Tracking: trackingPtr,
		Checks:   checks,
	})
}

func (s *Server) handleGetAppImageTrackingDiagnosis(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	diagnosis, err := s.buildAppImageTrackingDiagnosis(r.Context(), principal, app, true)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.image_tracking.diagnosis.read", "app", app.ID, app.TenantID, map[string]string{
		"app_id": app.ID,
	})
	httpx.WriteJSON(w, http.StatusOK, appImageTrackingDiagnosisResponse{
		AppID:     app.ID,
		Diagnosis: diagnosis,
	})
}

func (s *Server) handlePutAppImageTracking(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or app.deploy scope")
		return
	}
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	var req appImageTrackingRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	imageRef := ""
	if req.ImageRef != nil {
		imageRef = strings.TrimSpace(*req.ImageRef)
	}
	if imageRef == "" {
		httpx.WriteError(w, http.StatusBadRequest, "image_ref is required")
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	tracking, err := s.imageTrackingCommands().Configure(app, imageRef, enabled)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.image_tracking.upsert", "app", app.ID, app.TenantID, map[string]string{
		"app_id":    app.ID,
		"image_ref": imageRef,
		"enabled":   fmt.Sprintf("%t", enabled),
	})
	httpx.WriteJSON(w, http.StatusOK, appImageTrackingResponse{
		AppID:    app.ID,
		Tracking: &tracking,
	})
}

func (s *Server) handleSyncAppImage(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.deploy scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	var req imagetracking.SyncRequest
	if r.ContentLength != 0 {
		if err := httpx.DecodeJSON(r, &req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	result, err := s.imageTrackingCommands().Sync(r.Context(), principal, app, req)
	if err != nil {
		var resolverErr *imagetracking.ResolverError
		switch {
		case errors.Is(err, imagetracking.ErrDisabled), errors.Is(err, imagetracking.ErrReferenceMismatch):
			httpx.WriteError(w, http.StatusConflict, err.Error())
		case errors.As(err, &resolverErr):
			httpx.WriteError(w, http.StatusBadGateway, resolverErr.Error())
		default:
			s.writeStoreError(w, err)
		}
		return
	}
	if result.Changed {
		s.appendAudit(principal, "app.image_tracking.sync", "operation", result.Operation.ID, app.TenantID, map[string]string{
			"app_id": app.ID, "image_ref": result.Tracking.ImageRef, "digest": result.Digest,
			"delivery_id": strings.TrimSpace(req.DeliveryID), "event": imagetracking.ManualEvent(req.Event),
		})
	}
	if result.Operation != nil {
		operation := sanitizeOperationForAPI(*result.Operation)
		result.Operation = &operation
	}
	status := http.StatusOK
	if result.Changed || result.RolloutPending {
		status = http.StatusAccepted
	}
	httpx.WriteJSON(w, status, result)
}

func (s *Server) imageTrackingCommands() *imagetracking.Manual {
	resolver := s.resolveRemoteImageDigest
	if resolver == nil {
		resolver = sourceimport.ResolveRemoteImageDigest
	}
	return imagetracking.NewManual(s.store.ImageTracking(), s.store, s.log, resolver, s.latestActiveOperationForApp, store.ErrNotFound, store.ErrConflict)
}

func (s *Server) latestActiveOperationForApp(app model.App) (*model.Operation, error) {
	ops, err := s.store.ListOperationsFiltered(app.TenantID, true, store.OperationListFilter{
		AppID: app.ID,
		Statuses: []string{
			model.OperationStatusPending,
			model.OperationStatusRunning,
			model.OperationStatusWaitingAgent,
		},
		Limit: 1,
	})
	if err != nil {
		return nil, err
	}
	if len(ops) == 0 {
		return nil, nil
	}
	op := ops[len(ops)-1]
	return &op, nil
}

func (s *Server) buildAppImageTrackingDiagnosis(ctx context.Context, principal model.Principal, app model.App, resolveRemote bool) (appImageTrackingDiagnosis, error) {
	diagnosis, err := s.imageTrackingCommands().Diagnose(ctx, principal, app, resolveRemote)
	if diagnosis.ActiveOperation != nil {
		operation := sanitizeOperationForAPI(*diagnosis.ActiveOperation)
		diagnosis.ActiveOperation = &operation
	}
	return diagnosis, err
}
