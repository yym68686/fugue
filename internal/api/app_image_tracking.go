package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
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

type appImageSyncRequest struct {
	ImageRef   *string `json:"image_ref,omitempty"`
	Event      string  `json:"event,omitempty"`
	DeliveryID string  `json:"delivery_id,omitempty"`
}

type appImageSyncResponse struct {
	AppID          string                  `json:"app_id"`
	Tracking       *model.AppImageTracking `json:"tracking,omitempty"`
	Operation      *model.Operation        `json:"operation,omitempty"`
	Digest         string                  `json:"digest,omitempty"`
	Changed        bool                    `json:"changed"`
	AlreadyCurrent bool                    `json:"already_current"`
	Message        string                  `json:"message,omitempty"`
}

func (s *Server) handleGetAppImageTracking(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	tracking, err := s.store.GetAppImageTracking(principal.TenantID, principal.IsPlatformAdmin(), app.ID)
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
	tracking, err := s.store.UpsertAppImageTracking(model.AppImageTracking{
		TenantID: app.TenantID,
		AppID:    app.ID,
		ImageRef: imageRef,
		Enabled:  enabled,
	})
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
	var req appImageSyncRequest
	if r.ContentLength != 0 {
		if err := httpx.DecodeJSON(r, &req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	tracking, err := s.store.GetAppImageTracking(principal.TenantID, principal.IsPlatformAdmin(), app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !tracking.Enabled {
		httpx.WriteError(w, http.StatusConflict, "image tracking is disabled")
		return
	}
	if req.ImageRef != nil && strings.TrimSpace(*req.ImageRef) != "" && strings.TrimSpace(*req.ImageRef) != strings.TrimSpace(tracking.ImageRef) {
		httpx.WriteError(w, http.StatusConflict, "requested image_ref does not match app image tracking")
		return
	}

	digest, err := s.resolveTrackedImageDigest(r, tracking.ImageRef)
	if err != nil {
		if updated, recordErr := s.store.RecordAppImageTrackingCheck(tracking.ID, "", req.DeliveryID, req.Event, err.Error()); recordErr == nil {
			tracking = updated
		}
		httpx.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}

	if model.ImageDigestsMatch(model.AppTrackedImageDigest(app, tracking.ImageRef), digest) {
		if updated, err := s.store.RecordAppImageTrackingCheck(tracking.ID, digest, req.DeliveryID, eventNameOrDefault(req.Event), ""); err == nil {
			tracking = updated
		}
		httpx.WriteJSON(w, http.StatusOK, appImageSyncResponse{
			AppID:          app.ID,
			Tracking:       &tracking,
			Digest:         digest,
			AlreadyCurrent: true,
			Message:        "tracked image digest is already deployed",
		})
		return
	}

	hasActiveOp, err := s.store.HasActiveOperationByApp(app.TenantID, true, app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if hasActiveOp {
		if updated, err := s.store.RecordAppImageTrackingCheck(tracking.ID, digest, req.DeliveryID, eventNameOrDefault(req.Event), "app has an active operation"); err == nil {
			tracking = updated
		}
		httpx.WriteJSON(w, http.StatusAccepted, appImageSyncResponse{
			AppID:    app.ID,
			Tracking: &tracking,
			Digest:   digest,
			Message:  "app has an active operation; image sync was not queued",
		})
		return
	}

	op, err := s.store.QueueAppImageTrackingImport(app, tracking, principal.ActorType, principal.ActorID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	tracking, err = s.store.RecordAppImageTrackingQueued(tracking.ID, digest, op.ID, req.DeliveryID, eventNameOrDefault(req.Event))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.image_tracking.sync", "operation", op.ID, app.TenantID, map[string]string{
		"app_id":      app.ID,
		"image_ref":   tracking.ImageRef,
		"digest":      digest,
		"delivery_id": strings.TrimSpace(req.DeliveryID),
		"event":       eventNameOrDefault(req.Event),
	})
	sanitized := sanitizeOperationForAPI(op)
	httpx.WriteJSON(w, http.StatusAccepted, appImageSyncResponse{
		AppID:     app.ID,
		Tracking:  &tracking,
		Operation: &sanitized,
		Digest:    digest,
		Changed:   true,
		Message:   "tracked image digest changed; import queued",
	})
}

func (s *Server) resolveTrackedImageDigest(r *http.Request, imageRef string) (string, error) {
	resolver := s.resolveRemoteImageDigest
	if resolver == nil {
		resolver = sourceimport.ResolveRemoteImageDigest
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	digest, err := resolver(ctx, imageRef)
	if err != nil {
		return "", err
	}
	digest = strings.TrimSpace(digest)
	if digest == "" {
		return "", fmt.Errorf("image digest resolver returned empty digest")
	}
	return digest, nil
}

func eventNameOrDefault(event string) string {
	event = strings.TrimSpace(event)
	if event == "" {
		return "manual"
	}
	return event
}
