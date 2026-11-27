package imagetracking

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"fugue/internal/model"
)

// ManualRepository is the capability used by the interactive command path.
// Only this service translates a user request into tracking and release writes.
type ManualRepository interface {
	GetAppImageTracking(string, bool, string) (model.AppImageTracking, error)
	ListAppImageTrackingChecks(model.AppImageTrackingCheckFilter) ([]model.AppImageTrackingCheck, error)
	UpsertAppImageTracking(model.AppImageTracking) (model.AppImageTracking, error)
	RecordAppImageTrackingCheck(string, string, string, string, string) (model.AppImageTracking, error)
	QueueAppImageTrackingImport(model.App, model.AppImageTracking, string, string) (model.Operation, error)
	RecordAppImageTrackingQueued(string, string, string, string, string) (model.AppImageTracking, error)
	CreateAppImageTrackingCheck(model.AppImageTrackingCheck) (model.AppImageTrackingCheck, error)
}

type ReleaseEvidence interface {
	CreateReleaseAttempt(model.ReleaseAttempt) (model.ReleaseAttempt, error)
	RecordReleaseStep(model.ReleaseStep) (model.ReleaseStep, error)
}

type Manual struct {
	store                       ManualRepository
	evidence                    ReleaseEvidence
	logger                      *log.Logger
	resolveDigest               func(context.Context, string) (string, error)
	latestActiveOperationForApp func(model.App) (*model.Operation, error)
	notFound, conflict          error
}

func NewManual(store ManualRepository, evidence ReleaseEvidence, logger *log.Logger, resolve func(context.Context, string) (string, error), activeOperation func(model.App) (*model.Operation, error), notFound, conflict error) *Manual {
	return &Manual{store: store, evidence: evidence, logger: logger, resolveDigest: resolve, latestActiveOperationForApp: activeOperation, notFound: notFound, conflict: conflict}
}

func (s *Manual) Configure(app model.App, imageRef string, enabled bool) (model.AppImageTracking, error) {
	return s.store.UpsertAppImageTracking(model.AppImageTracking{TenantID: app.TenantID, AppID: app.ID, ImageRef: imageRef, Enabled: enabled})
}

func (s *Manual) Get(principal model.Principal, appID string) (model.AppImageTracking, error) {
	return s.store.GetAppImageTracking(principal.TenantID, principal.IsPlatformAdmin(), appID)
}

func (s *Manual) History(principal model.Principal, appID string, limit int) ([]model.AppImageTrackingCheck, error) {
	return s.store.ListAppImageTrackingChecks(model.AppImageTrackingCheckFilter{TenantID: principal.TenantID, PlatformAdmin: principal.IsPlatformAdmin(), AppID: appID, Limit: limit})
}

var (
	ErrDisabled          = errors.New("image tracking is disabled")
	ErrReferenceMismatch = errors.New("requested image_ref does not match app image tracking")
)

type ResolverError struct{ cause error }

func (e *ResolverError) Error() string { return e.cause.Error() }

type SyncRequest struct {
	ImageRef   *string `json:"image_ref,omitempty"`
	Event      string  `json:"event,omitempty"`
	DeliveryID string  `json:"delivery_id,omitempty"`
}

type SyncResult struct {
	AppID          string                  `json:"app_id"`
	Tracking       *model.AppImageTracking `json:"tracking,omitempty"`
	Operation      *model.Operation        `json:"operation,omitempty"`
	ReleaseAttempt *model.ReleaseAttempt   `json:"release_attempt,omitempty"`
	Digest         string                  `json:"digest,omitempty"`
	Changed        bool                    `json:"changed"`
	AlreadyCurrent bool                    `json:"already_current"`
	RolloutPending bool                    `json:"rollout_pending,omitempty"`
	AppPhase       string                  `json:"app_phase,omitempty"`
	Message        string                  `json:"message,omitempty"`
}

type Diagnosis struct {
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

func (s *Manual) Sync(ctx context.Context, principal model.Principal, app model.App, req SyncRequest) (SyncResult, error) {
	tracking, err := s.store.GetAppImageTracking(principal.TenantID, principal.IsPlatformAdmin(), app.ID)
	if err != nil {
		return SyncResult{}, err
	}
	if !tracking.Enabled {
		return SyncResult{}, ErrDisabled
	}
	if req.ImageRef != nil && strings.TrimSpace(*req.ImageRef) != "" && strings.TrimSpace(*req.ImageRef) != strings.TrimSpace(tracking.ImageRef) {
		return SyncResult{}, ErrReferenceMismatch
	}
	startedAt := time.Now().UTC()

	digest, err := s.resolveTrackedImageDigestContext(ctx, tracking.ImageRef)
	if err != nil {
		if updated, recordErr := s.store.RecordAppImageTrackingCheck(tracking.ID, "", req.DeliveryID, req.Event, err.Error()); recordErr == nil {
			tracking = updated
		}
		s.recordAppImageSyncDecision(app, tracking, appImageTrackingDecisionInputAPI{
			Decision:      model.AppImageTrackingDecisionResolverError,
			SkipReason:    "image digest resolver failed",
			ResolverError: err.Error(),
			DeliveryID:    req.DeliveryID,
			Event:         ManualEvent(req.Event),
			StartedAt:     startedAt,
		})
		return SyncResult{}, &ResolverError{err}
	}

	if model.ImageDigestsMatch(model.AppTrackedImageDigest(app, tracking.ImageRef), digest) {
		if updated, err := s.store.RecordAppImageTrackingCheck(tracking.ID, digest, req.DeliveryID, ManualEvent(req.Event), ""); err == nil {
			tracking = updated
		}
		s.recordAppImageSyncDecision(app, tracking, appImageTrackingDecisionInputAPI{
			ObservedDigest: digest,
			Decision:       model.AppImageTrackingDecisionAlreadyDeployed,
			SkipReason:     "tracked digest already matches app desired source",
			DeliveryID:     req.DeliveryID,
			Event:          ManualEvent(req.Event),
			StartedAt:      startedAt,
		})

		activeOp, err := s.latestActiveOperationForApp(app)
		if err != nil {
			return SyncResult{}, err
		}
		rolloutPending := appImageSyncRolloutPending(app)
		message := "tracked image digest already matches app desired source"
		if activeOp != nil {
			rolloutPending = true
			message = "tracked image digest already matches app desired source; active operation is still in progress"
		} else if rolloutPending {
			message = "tracked image digest already matches app desired source; app rollout is still pending"
		}

		return SyncResult{
			AppID:          app.ID,
			Tracking:       &tracking,
			Operation:      activeOp,
			Digest:         digest,
			AlreadyCurrent: true,
			RolloutPending: rolloutPending,
			AppPhase:       strings.TrimSpace(app.Status.Phase),
			Message:        message,
		}, nil
	}

	activeOp, err := s.latestActiveOperationForApp(app)
	if err != nil {
		return SyncResult{}, err
	}
	if activeOp != nil {
		if updated, err := s.store.RecordAppImageTrackingCheck(tracking.ID, digest, req.DeliveryID, ManualEvent(req.Event), "app has an active operation"); err == nil {
			tracking = updated
		}
		s.recordAppImageSyncDecision(app, tracking, appImageTrackingDecisionInputAPI{
			ObservedDigest:    digest,
			Decision:          model.AppImageTrackingDecisionActiveOperation,
			SkipReason:        "app has an active operation",
			ActiveOperationID: activeOp.ID,
			DeliveryID:        req.DeliveryID,
			Event:             ManualEvent(req.Event),
			StartedAt:         startedAt,
		})
		return SyncResult{
			AppID:          app.ID,
			Tracking:       &tracking,
			Operation:      activeOp,
			Digest:         digest,
			RolloutPending: true,
			AppPhase:       strings.TrimSpace(app.Status.Phase),
			Message:        "app has an active operation; image sync was not queued",
		}, nil
	}

	op, err := s.store.QueueAppImageTrackingImport(app, tracking, principal.ActorType, principal.ActorID)
	if err != nil {
		decision := model.AppImageTrackingDecisionQueueError
		skipReason := "queue image tracking import failed"
		if errors.Is(err, s.conflict) {
			decision = model.AppImageTrackingDecisionQueueConflict
			skipReason = "app already has a conflicting pending operation"
		}
		s.recordAppImageSyncDecision(app, tracking, appImageTrackingDecisionInputAPI{
			ObservedDigest: digest,
			Decision:       decision,
			SkipReason:     skipReason,
			ResolverError:  err.Error(),
			DeliveryID:     req.DeliveryID,
			Event:          ManualEvent(req.Event),
			StartedAt:      startedAt,
		})
		return SyncResult{}, err
	}
	tracking, err = s.store.RecordAppImageTrackingQueued(tracking.ID, digest, op.ID, req.DeliveryID, ManualEvent(req.Event))
	if err != nil {
		return SyncResult{}, err
	}
	releaseAttempt := s.createImageTrackingReleaseAttemptBestEffort(app, tracking, op, principal, digest, model.ReleaseAttemptTriggerImageTrackingManualSync)
	s.recordAppImageSyncDecision(app, tracking, appImageTrackingDecisionInputAPI{
		ObservedDigest: digest,
		Decision:       model.AppImageTrackingDecisionQueued,
		OperationID:    op.ID,
		DeliveryID:     req.DeliveryID,
		Event:          ManualEvent(req.Event),
		StartedAt:      startedAt,
	})
	return SyncResult{
		AppID:          app.ID,
		Tracking:       &tracking,
		Operation:      &op,
		ReleaseAttempt: releaseAttempt,
		Digest:         digest,
		Changed:        true,
		Message:        "tracked image digest changed; import queued",
	}, nil
}

func (s *Manual) createImageTrackingReleaseAttemptBestEffort(app model.App, tracking model.AppImageTracking, op model.Operation, principal model.Principal, digest, triggerType string) *model.ReleaseAttempt {
	attempt, err := s.evidence.CreateReleaseAttempt(model.ReleaseAttempt{
		TenantID:          app.TenantID,
		ProjectID:         app.ProjectID,
		AppID:             app.ID,
		TriggerType:       triggerType,
		TriggerActorType:  principal.ActorType,
		TriggerActorID:    principal.ActorID,
		SourceOperationID: op.ID,
		RootOperationID:   op.ID,
		ImageRef:          tracking.ImageRef,
		TargetDigest:      digest,
		PreviousDigest:    tracking.LastDeployedDigest,
		Status:            model.ReleaseAttemptStatusImporting,
		Confidence:        model.OperationEvidenceConfidenceEvidenceBacked,
		Summary:           "tracked image digest changed; import queued",
	})
	if err != nil {
		if s.logger != nil {
			s.logger.Printf("create image tracking release attempt failed app=%s op=%s: %v", app.ID, op.ID, err)
		}
		return nil
	}
	_, _ = s.evidence.RecordReleaseStep(model.ReleaseStep{
		TenantID:         app.TenantID,
		ReleaseAttemptID: attempt.ID,
		OperationID:      op.ID,
		Type:             model.ReleaseStepTypeTriggerReceived,
		Status:           model.ReleaseStepStatusCompleted,
		Summary:          "image tracking sync requested",
	})
	_, _ = s.evidence.RecordReleaseStep(model.ReleaseStep{
		TenantID:         app.TenantID,
		ReleaseAttemptID: attempt.ID,
		OperationID:      op.ID,
		Type:             model.ReleaseStepTypeImageTrackingCheck,
		Status:           model.ReleaseStepStatusCompleted,
		Summary:          "tracked digest changed",
		Payload: map[string]any{
			"image_ref":       tracking.ImageRef,
			"observed_digest": digest,
			"previous_digest": tracking.LastDeployedDigest,
			"tracking_id":     tracking.ID,
		},
	})
	_, _ = s.evidence.RecordReleaseStep(model.ReleaseStep{
		TenantID:         app.TenantID,
		ReleaseAttemptID: attempt.ID,
		OperationID:      op.ID,
		Type:             model.ReleaseStepTypeImageImport,
		Status:           model.ReleaseStepStatusPending,
		Summary:          "image import queued",
	})
	return &attempt
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

func (s *Manual) recordAppImageSyncDecision(app model.App, tracking model.AppImageTracking, input appImageTrackingDecisionInputAPI) {
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
		Event:                ManualEvent(input.Event),
		DurationMilliseconds: duration.Milliseconds(),
		CheckedAt:            checkedAt,
	})
}

func (s *Manual) Diagnose(ctx context.Context, principal model.Principal, app model.App, resolveRemote bool) (Diagnosis, error) {
	diagnosis := Diagnosis{
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
		if errors.Is(err, s.notFound) {
			return diagnosis, nil
		}
		return Diagnosis{}, err
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
		return Diagnosis{}, err
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
		return Diagnosis{}, err
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

func (s *Manual) resolveTrackedImageDigestContext(ctx context.Context, imageRef string) (string, error) {
	resolver := s.resolveDigest
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

func ManualEvent(event string) string {
	event = strings.TrimSpace(event)
	if event == "" {
		return "manual"
	}
	return event
}
