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

type appImageTrackingHistoryResponse struct {
	AppID    string                        `json:"app_id"`
	Tracking *model.AppImageTracking       `json:"tracking,omitempty"`
	Checks   []model.AppImageTrackingCheck `json:"checks"`
}

type appImageTrackingDiagnosis struct {
	Category         string                        `json:"category"`
	Summary          string                        `json:"summary"`
	Hint             string                        `json:"hint,omitempty"`
	AppID            string                        `json:"app_id"`
	Tracking         *model.AppImageTracking       `json:"tracking,omitempty"`
	LatestCheck      *model.AppImageTrackingCheck  `json:"latest_check,omitempty"`
	RecentChecks     []model.AppImageTrackingCheck `json:"recent_checks"`
	RemoteDigest     string                        `json:"remote_digest,omitempty"`
	CurrentAppDigest string                        `json:"current_app_digest,omitempty"`
	ActiveOperation  *model.Operation              `json:"active_operation,omitempty"`
	Evidence         []string                      `json:"evidence"`
	Warnings         []string                      `json:"warnings"`
}

type appImageTrackingDiagnosisResponse struct {
	AppID     string                    `json:"app_id"`
	Diagnosis appImageTrackingDiagnosis `json:"diagnosis"`
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
	RolloutPending bool                    `json:"rollout_pending,omitempty"`
	AppPhase       string                  `json:"app_phase,omitempty"`
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

func (s *Server) handleGetAppImageTrackingHistory(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	var trackingPtr *model.AppImageTracking
	tracking, err := s.store.GetAppImageTracking(principal.TenantID, principal.IsPlatformAdmin(), app.ID)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			s.writeStoreError(w, err)
			return
		}
	} else {
		trackingPtr = &tracking
	}
	checks, err := s.store.ListAppImageTrackingChecks(model.AppImageTrackingCheckFilter{
		TenantID:      principal.TenantID,
		PlatformAdmin: principal.IsPlatformAdmin(),
		AppID:         app.ID,
		Limit:         parseLimitQuery(r, 50),
	})
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
	startedAt := time.Now().UTC()

	digest, err := s.resolveTrackedImageDigest(r, tracking.ImageRef)
	if err != nil {
		if updated, recordErr := s.store.RecordAppImageTrackingCheck(tracking.ID, "", req.DeliveryID, req.Event, err.Error()); recordErr == nil {
			tracking = updated
		}
		s.recordAppImageSyncDecision(app, tracking, appImageTrackingDecisionInputAPI{
			Decision:      model.AppImageTrackingDecisionResolverError,
			SkipReason:    "image digest resolver failed",
			ResolverError: err.Error(),
			DeliveryID:    req.DeliveryID,
			Event:         eventNameOrDefault(req.Event),
			StartedAt:     startedAt,
		})
		httpx.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}

	if model.ImageDigestsMatch(model.AppTrackedImageDigest(app, tracking.ImageRef), digest) {
		if updated, err := s.store.RecordAppImageTrackingCheck(tracking.ID, digest, req.DeliveryID, eventNameOrDefault(req.Event), ""); err == nil {
			tracking = updated
		}
		s.recordAppImageSyncDecision(app, tracking, appImageTrackingDecisionInputAPI{
			ObservedDigest: digest,
			Decision:       model.AppImageTrackingDecisionAlreadyDeployed,
			SkipReason:     "tracked digest already matches app desired source",
			DeliveryID:     req.DeliveryID,
			Event:          eventNameOrDefault(req.Event),
			StartedAt:      startedAt,
		})

		activeOp, err := s.latestActiveOperationForApp(app)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		rolloutPending := appImageSyncRolloutPending(app)
		status := http.StatusOK
		message := "tracked image digest already matches app desired source"
		if activeOp != nil {
			status = http.StatusAccepted
			rolloutPending = true
			message = "tracked image digest already matches app desired source; active operation is still in progress"
		} else if rolloutPending {
			status = http.StatusAccepted
			message = "tracked image digest already matches app desired source; app rollout is still pending"
		}

		httpx.WriteJSON(w, status, appImageSyncResponse{
			AppID:          app.ID,
			Tracking:       &tracking,
			Operation:      activeOp,
			Digest:         digest,
			AlreadyCurrent: true,
			RolloutPending: rolloutPending,
			AppPhase:       strings.TrimSpace(app.Status.Phase),
			Message:        message,
		})
		return
	}

	activeOp, err := s.latestActiveOperationForApp(app)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if activeOp != nil {
		if updated, err := s.store.RecordAppImageTrackingCheck(tracking.ID, digest, req.DeliveryID, eventNameOrDefault(req.Event), "app has an active operation"); err == nil {
			tracking = updated
		}
		s.recordAppImageSyncDecision(app, tracking, appImageTrackingDecisionInputAPI{
			ObservedDigest:    digest,
			Decision:          model.AppImageTrackingDecisionActiveOperation,
			SkipReason:        "app has an active operation",
			ActiveOperationID: activeOp.ID,
			DeliveryID:        req.DeliveryID,
			Event:             eventNameOrDefault(req.Event),
			StartedAt:         startedAt,
		})
		httpx.WriteJSON(w, http.StatusAccepted, appImageSyncResponse{
			AppID:          app.ID,
			Tracking:       &tracking,
			Operation:      activeOp,
			Digest:         digest,
			RolloutPending: true,
			AppPhase:       strings.TrimSpace(app.Status.Phase),
			Message:        "app has an active operation; image sync was not queued",
		})
		return
	}

	op, err := s.store.QueueAppImageTrackingImport(app, tracking, principal.ActorType, principal.ActorID)
	if err != nil {
		decision := model.AppImageTrackingDecisionQueueError
		skipReason := "queue image tracking import failed"
		if errors.Is(err, store.ErrConflict) {
			decision = model.AppImageTrackingDecisionQueueConflict
			skipReason = "app already has a conflicting pending operation"
		}
		s.recordAppImageSyncDecision(app, tracking, appImageTrackingDecisionInputAPI{
			ObservedDigest: digest,
			Decision:       decision,
			SkipReason:     skipReason,
			ResolverError:  err.Error(),
			DeliveryID:     req.DeliveryID,
			Event:          eventNameOrDefault(req.Event),
			StartedAt:      startedAt,
		})
		s.writeStoreError(w, err)
		return
	}
	tracking, err = s.store.RecordAppImageTrackingQueued(tracking.ID, digest, op.ID, req.DeliveryID, eventNameOrDefault(req.Event))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.recordAppImageSyncDecision(app, tracking, appImageTrackingDecisionInputAPI{
		ObservedDigest: digest,
		Decision:       model.AppImageTrackingDecisionQueued,
		OperationID:    op.ID,
		DeliveryID:     req.DeliveryID,
		Event:          eventNameOrDefault(req.Event),
		StartedAt:      startedAt,
	})
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

type appImageTrackingDecisionInputAPI struct {
	ObservedDigest    string
	Decision          string
	SkipReason        string
	OperationID       string
	ActiveOperationID string
	ResolverError     string
	DeliveryID        string
	Event             string
	StartedAt         time.Time
}

func (s *Server) recordAppImageSyncDecision(app model.App, tracking model.AppImageTracking, input appImageTrackingDecisionInputAPI) {
	checkedAt := time.Now().UTC()
	startedAt := input.StartedAt
	if startedAt.IsZero() {
		startedAt = checkedAt
	}
	duration := checkedAt.Sub(startedAt)
	if duration < 0 {
		duration = 0
	}
	_, _ = s.store.CreateAppImageTrackingCheck(model.AppImageTrackingCheck{
		TenantID:             tracking.TenantID,
		AppID:                tracking.AppID,
		TrackingID:           tracking.ID,
		ImageRef:             tracking.ImageRef,
		ObservedDigest:       input.ObservedDigest,
		CurrentAppDigest:     model.AppTrackedImageDigest(app, tracking.ImageRef),
		LastQueuedDigest:     tracking.LastQueuedDigest,
		LastDeployedDigest:   tracking.LastDeployedDigest,
		Decision:             strings.TrimSpace(input.Decision),
		SkipReason:           strings.TrimSpace(input.SkipReason),
		OperationID:          strings.TrimSpace(input.OperationID),
		ActiveOperationID:    strings.TrimSpace(input.ActiveOperationID),
		ResolverError:        strings.TrimSpace(input.ResolverError),
		DeliveryID:           strings.TrimSpace(input.DeliveryID),
		Event:                eventNameOrDefault(input.Event),
		DurationMilliseconds: duration.Milliseconds(),
		CheckedAt:            checkedAt,
	})
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
	op := sanitizeOperationForAPI(ops[len(ops)-1])
	return &op, nil
}

func (s *Server) buildAppImageTrackingDiagnosis(ctx context.Context, principal model.Principal, app model.App, resolveRemote bool) (appImageTrackingDiagnosis, error) {
	diagnosis := appImageTrackingDiagnosis{
		Category:     "not_configured",
		Summary:      "image tracking is not configured",
		Hint:         "Configure app release tracking before expecting automatic image updates.",
		AppID:        app.ID,
		RecentChecks: []model.AppImageTrackingCheck{},
		Evidence:     []string{},
		Warnings:     []string{},
	}
	tracking, err := s.store.GetAppImageTracking(principal.TenantID, principal.IsPlatformAdmin(), app.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return diagnosis, nil
		}
		return appImageTrackingDiagnosis{}, err
	}
	diagnosis.Tracking = &tracking
	diagnosis.Category = "unknown"
	diagnosis.Summary = "image tracking state is available"
	diagnosis.Hint = ""
	diagnosis.CurrentAppDigest = model.AppTrackedImageDigest(app, tracking.ImageRef)
	diagnosis.Evidence = append(diagnosis.Evidence, fmt.Sprintf("tracking image_ref=%s enabled=%t", tracking.ImageRef, tracking.Enabled))
	if diagnosis.CurrentAppDigest != "" {
		diagnosis.Evidence = append(diagnosis.Evidence, "current app digest="+diagnosis.CurrentAppDigest)
	}

	checks, err := s.store.ListAppImageTrackingChecks(model.AppImageTrackingCheckFilter{
		TenantID:      principal.TenantID,
		PlatformAdmin: principal.IsPlatformAdmin(),
		AppID:         app.ID,
		TrackingID:    tracking.ID,
		Limit:         10,
	})
	if err != nil {
		return appImageTrackingDiagnosis{}, err
	}
	diagnosis.RecentChecks = checks
	if len(checks) > 0 {
		latest := checks[0]
		diagnosis.LatestCheck = &latest
		diagnosis.Evidence = append(diagnosis.Evidence, fmt.Sprintf("latest check decision=%s at=%s", latest.Decision, latest.CheckedAt.Format(time.RFC3339)))
		if strings.TrimSpace(latest.SkipReason) != "" {
			diagnosis.Evidence = append(diagnosis.Evidence, "latest skip reason="+latest.SkipReason)
		}
	}

	if !tracking.Enabled {
		diagnosis.Category = "disabled"
		diagnosis.Summary = "image tracking is disabled"
		diagnosis.Hint = "Enable tracking before expecting automatic image imports."
		return diagnosis, nil
	}
	if app.Spec.Replicas <= 0 {
		diagnosis.Category = model.AppImageTrackingDecisionReplicasZero
		diagnosis.Summary = "image tracking is skipped because app replicas are zero"
		diagnosis.Hint = "Scale the app above zero before expecting automatic image imports."
		return diagnosis, nil
	}

	activeOp, err := s.latestActiveOperationForApp(app)
	if err != nil {
		return appImageTrackingDiagnosis{}, err
	}
	if activeOp != nil {
		diagnosis.ActiveOperation = activeOp
		diagnosis.Evidence = append(diagnosis.Evidence, "active operation="+activeOp.ID)
	}

	if resolveRemote {
		digest, err := s.resolveTrackedImageDigestContext(ctx, tracking.ImageRef)
		if err != nil {
			diagnosis.Category = model.AppImageTrackingDecisionResolverError
			diagnosis.Summary = "image tracking could not resolve the remote digest"
			diagnosis.Hint = "Check registry credentials, registry availability, and the tracked image reference."
			diagnosis.Warnings = append(diagnosis.Warnings, err.Error())
			return diagnosis, nil
		}
		diagnosis.RemoteDigest = digest
		diagnosis.Evidence = append(diagnosis.Evidence, "remote digest="+digest)
	}

	if diagnosis.RemoteDigest != "" && model.ImageDigestsMatch(diagnosis.CurrentAppDigest, diagnosis.RemoteDigest) {
		diagnosis.Category = model.AppImageTrackingDecisionAlreadyDeployed
		diagnosis.Summary = "tracked image digest already matches the app desired source"
		return diagnosis, nil
	}
	if activeOp != nil {
		diagnosis.Category = model.AppImageTrackingDecisionActiveOperation
		diagnosis.Summary = "image tracking is waiting for an active app operation to finish"
		diagnosis.Hint = "Inspect the active operation before expecting image tracking to queue another import."
		return diagnosis, nil
	}
	if diagnosis.RemoteDigest != "" && strings.TrimSpace(diagnosis.RemoteDigest) == strings.TrimSpace(tracking.LastQueuedDigest) && !imageTrackingRetryReadyForAPI(tracking, time.Now().UTC()) {
		diagnosis.Category = model.AppImageTrackingDecisionRetrySuppressed
		diagnosis.Summary = "image tracking saw the same queued digest and is waiting for retry delay"
		return diagnosis, nil
	}
	if diagnosis.LatestCheck != nil {
		diagnosis.Category = diagnosis.LatestCheck.Decision
		diagnosis.Summary = imageTrackingDiagnosisSummaryForDecision(*diagnosis.LatestCheck)
		return diagnosis, nil
	}
	if diagnosis.RemoteDigest != "" {
		diagnosis.Category = "changed"
		diagnosis.Summary = "remote digest differs from the app desired source and no blocking condition was found"
		diagnosis.Hint = "The next controller poll should queue an image import."
		return diagnosis, nil
	}
	diagnosis.Category = "waiting_for_check"
	diagnosis.Summary = "image tracking is enabled but no check history is available yet"
	return diagnosis, nil
}

func imageTrackingDiagnosisSummaryForDecision(check model.AppImageTrackingCheck) string {
	switch strings.TrimSpace(check.Decision) {
	case model.AppImageTrackingDecisionQueued:
		return "latest image tracking check queued an import"
	case model.AppImageTrackingDecisionAlreadyDeployed:
		return "latest image tracking check found the app already on the tracked digest"
	case model.AppImageTrackingDecisionReplicasZero:
		return "latest image tracking check skipped because app replicas are zero"
	case model.AppImageTrackingDecisionActiveOperation:
		return "latest image tracking check skipped because an active app operation exists"
	case model.AppImageTrackingDecisionRetrySuppressed:
		return "latest image tracking check skipped because the same digest was already queued recently"
	case model.AppImageTrackingDecisionResolverError:
		return "latest image tracking check failed to resolve the remote digest"
	case model.AppImageTrackingDecisionQueueConflict:
		return "latest image tracking check could not queue because the app had a conflicting operation"
	case model.AppImageTrackingDecisionQueueError:
		return "latest image tracking check failed while queueing the import"
	default:
		if strings.TrimSpace(check.SkipReason) != "" {
			return check.SkipReason
		}
		return "latest image tracking check did not identify a queueable update"
	}
}

func appImageSyncRolloutPending(app model.App) bool {
	if app.Spec.Replicas <= 0 {
		return false
	}
	if app.Status.CurrentReplicas < app.Spec.Replicas {
		return true
	}
	phase := strings.ToLower(strings.TrimSpace(app.Status.Phase))
	return phase != "" && phase != "deployed"
}

func (s *Server) resolveTrackedImageDigest(r *http.Request, imageRef string) (string, error) {
	return s.resolveTrackedImageDigestContext(r.Context(), imageRef)
}

func (s *Server) resolveTrackedImageDigestContext(ctx context.Context, imageRef string) (string, error) {
	resolver := s.resolveRemoteImageDigest
	if resolver == nil {
		resolver = sourceimport.ResolveRemoteImageDigest
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
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

func imageTrackingRetryReadyForAPI(tracking model.AppImageTracking, now time.Time) bool {
	if tracking.LastTriggeredAt == nil {
		return true
	}
	return !tracking.LastTriggeredAt.Add(5 * time.Minute).After(now)
}

func eventNameOrDefault(event string) string {
	event = strings.TrimSpace(event)
	if event == "" {
		return "manual"
	}
	return event
}
