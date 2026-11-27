package imagetracking

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Poller) Sync(ctx context.Context) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.metrics.markImageTrackingSyncStarted()
	defer func(startedAt time.Time) {
		s.metrics.markImageTrackingSyncFinished(startedAt, err)
	}(time.Now().UTC())

	enabled := true
	trackings, err := s.store.ListAppImageTrackings(model.AppImageTrackingFilter{
		PlatformAdmin: true,
		Enabled:       &enabled,
	})
	if err != nil {
		return fmt.Errorf("list image trackings: %w", err)
	}

	for _, tracking := range trackings {
		if err := ctx.Err(); err != nil {
			return err
		}
		startedAt := s.now()
		app, err := s.getApp(tracking.AppID)
		if err != nil {
			if !errors.Is(err, s.errNotFound) && s.logger != nil {
				s.logger.Printf("image tracking load app failed app=%s: %v", tracking.AppID, err)
			}
			continue
		}
		if app.Spec.Replicas <= 0 {
			s.RecordDecision(DecisionInput{
				App:        app,
				Tracking:   tracking,
				Decision:   model.AppImageTrackingDecisionReplicasZero,
				SkipReason: "app replicas are zero",
				Event:      "poll",
				StartedAt:  startedAt,
			})
			continue
		}
		activeOpID, err := s.latestImageTrackingActiveOperationID(app)
		if err != nil {
			if s.logger != nil {
				s.logger.Printf("image tracking active operation check failed app=%s image=%s: %v", app.ID, tracking.ImageRef, err)
			}
			continue
		}
		if activeOpID != "" {
			s.RecordDecision(DecisionInput{
				App:               app,
				Tracking:          tracking,
				Decision:          model.AppImageTrackingDecisionActiveOperation,
				SkipReason:        "app has an active operation",
				ActiveOperationID: activeOpID,
				Event:             "poll",
				StartedAt:         startedAt,
			})
			continue
		}

		digest, err := s.resolveTrackedImageDigest(ctx, tracking.ImageRef)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			if updated, recordErr := s.store.RecordAppImageTrackingCheck(tracking.ID, "", "", "poll", err.Error()); recordErr != nil && s.logger != nil {
				s.logger.Printf("image tracking check record failed app=%s image=%s: %v", app.ID, tracking.ImageRef, recordErr)
			} else {
				tracking = updated
			}
			if s.logger != nil {
				s.logger.Printf("image tracking digest check failed app=%s image=%s: %v", app.ID, tracking.ImageRef, err)
			}
			s.RecordDecision(DecisionInput{
				App:           app,
				Tracking:      tracking,
				Decision:      model.AppImageTrackingDecisionResolverError,
				SkipReason:    "image digest resolver failed",
				ResolverError: err.Error(),
				Event:         "poll",
				StartedAt:     startedAt,
			})
			continue
		}
		if model.ImageDigestsMatch(model.AppTrackedImageDigest(app, tracking.ImageRef), digest) {
			if updated, err := s.store.RecordAppImageTrackingCheck(tracking.ID, digest, "", "poll", ""); err != nil && s.logger != nil {
				s.logger.Printf("image tracking no-op record failed app=%s image=%s digest=%s: %v", app.ID, tracking.ImageRef, digest, err)
			} else {
				tracking = updated
			}
			s.RecordDecision(DecisionInput{
				App:            app,
				Tracking:       tracking,
				ObservedDigest: digest,
				Decision:       model.AppImageTrackingDecisionAlreadyDeployed,
				SkipReason:     "tracked digest already matches app desired source",
				Event:          "poll",
				StartedAt:      startedAt,
			})
			continue
		}
		retryReason, err := s.RetrySuppression(ctx, app, tracking, digest)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			s.RecordDecision(DecisionInput{
				App: app, Tracking: tracking, ObservedDigest: digest,
				Decision: model.AppImageTrackingDecisionRetrySuppressed, SkipReason: "retry state unavailable: " + err.Error(),
				Event: "poll", StartedAt: startedAt,
			})
			continue
		}
		if retryReason != "" {
			if updated, err := s.store.RecordAppImageTrackingCheck(tracking.ID, digest, "", "poll", ""); err == nil {
				tracking = updated
			} else if s.logger != nil {
				s.logger.Printf("image tracking retry-suppressed record failed app=%s image=%s digest=%s: %v", app.ID, tracking.ImageRef, digest, err)
			}
			s.RecordDecision(DecisionInput{
				App:            app,
				Tracking:       tracking,
				ObservedDigest: digest,
				Decision:       model.AppImageTrackingDecisionRetrySuppressed,
				SkipReason:     retryReason,
				Event:          "poll",
				StartedAt:      startedAt,
			})
			continue
		}

		op, err := s.store.QueueAppImageTrackingImport(app, tracking, model.ActorTypeBootstrap, model.OperationRequestedByImageTracking)
		if err != nil {
			if errors.Is(err, s.errConflict) {
				s.RecordDecision(DecisionInput{
					App:            app,
					Tracking:       tracking,
					ObservedDigest: digest,
					Decision:       model.AppImageTrackingDecisionQueueConflict,
					SkipReason:     "app already has a conflicting pending operation",
					Event:          "poll",
					StartedAt:      startedAt,
				})
				continue
			}
			if updated, recordErr := s.store.RecordAppImageTrackingCheck(tracking.ID, digest, "", "poll", err.Error()); recordErr != nil && s.logger != nil {
				s.logger.Printf("image tracking queue failure record failed app=%s image=%s digest=%s: %v", app.ID, tracking.ImageRef, digest, recordErr)
			} else {
				tracking = updated
			}
			if s.logger != nil {
				s.logger.Printf("image tracking queue failed app=%s image=%s digest=%s: %v", app.ID, tracking.ImageRef, digest, err)
			}
			s.RecordDecision(DecisionInput{
				App:            app,
				Tracking:       tracking,
				ObservedDigest: digest,
				Decision:       model.AppImageTrackingDecisionQueueError,
				SkipReason:     "queue image tracking import failed",
				ResolverError:  err.Error(),
				Event:          "poll",
				StartedAt:      startedAt,
			})
			continue
		}
		if updated, err := s.store.RecordAppImageTrackingQueued(tracking.ID, digest, op.ID, "", "poll"); err != nil && s.logger != nil {
			s.logger.Printf("image tracking queued record failed app=%s image=%s digest=%s op=%s: %v", app.ID, tracking.ImageRef, digest, op.ID, err)
		} else {
			tracking = updated
		}
		s.recordRelease(app, tracking, op, digest, model.ReleaseAttemptTriggerImageTrackingAuto, "image tracking poll queued import")
		s.RecordDecision(DecisionInput{
			App:            app,
			Tracking:       tracking,
			ObservedDigest: digest,
			Decision:       model.AppImageTrackingDecisionQueued,
			OperationID:    op.ID,
			Event:          "poll",
			StartedAt:      startedAt,
		})
		if s.logger != nil {
			s.logger.Printf("image tracking queued import app=%s image=%s old_digest=%s new_digest=%s op=%s", app.ID, tracking.ImageRef, tracking.LastDeployedDigest, digest, op.ID)
		}
	}
	return nil
}

type DecisionInput struct {
	App               model.App
	Tracking          model.AppImageTracking
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

func (s *Poller) RecordDecision(input DecisionInput) {
	if s == nil || s.store == nil {
		return
	}
	checkedAt := s.now().UTC()
	startedAt := input.StartedAt
	if startedAt.IsZero() {
		startedAt = checkedAt
	}
	duration := checkedAt.Sub(startedAt)
	if duration < 0 {
		duration = 0
	}
	check := model.AppImageTrackingCheck{
		TenantID:                 input.Tracking.TenantID,
		AppID:                    input.Tracking.AppID,
		TrackingID:               input.Tracking.ID,
		ImageRef:                 input.Tracking.ImageRef,
		ObservedDigest:           input.ObservedDigest,
		CurrentAppDigest:         model.AppTrackedImageDigest(input.App, input.Tracking.ImageRef),
		LastQueuedDigest:         input.Tracking.LastQueuedDigest,
		LastDeployedDigest:       input.Tracking.LastDeployedDigest,
		Decision:                 strings.TrimSpace(input.Decision),
		SkipReason:               strings.TrimSpace(input.SkipReason),
		OperationID:              strings.TrimSpace(input.OperationID),
		ActiveOperationID:        strings.TrimSpace(input.ActiveOperationID),
		ResolverError:            strings.TrimSpace(input.ResolverError),
		DeliveryID:               strings.TrimSpace(input.DeliveryID),
		Event:                    imageTrackingEventNameOrDefault(input.Event),
		DurationMilliseconds:     duration.Milliseconds(),
		ControllerPod:            ControllerPodName(),
		ControllerLeaderIdentity: strings.TrimSpace(s.leaderIdentity),
		CheckedAt:                checkedAt,
	}
	stored, err := s.store.CreateAppImageTrackingCheck(check)
	if err != nil {
		if s.logger != nil {
			s.logger.Printf("image tracking decision record failed app=%s image=%s decision=%s: %v", input.Tracking.AppID, input.Tracking.ImageRef, input.Decision, err)
		}
		s.metrics.observeImageTrackingDecision(check)
		return
	}
	s.metrics.observeImageTrackingDecision(stored)
	s.logImageTrackingDecision(stored)
}

func (s *Poller) logImageTrackingDecision(check model.AppImageTrackingCheck) {
	if s == nil || s.logger == nil {
		return
	}
	payload := map[string]any{
		"component":            "fugue-controller",
		"event":                "image_tracking_decision",
		"tenant_id":            check.TenantID,
		"app_id":               check.AppID,
		"tracking_id":          check.TrackingID,
		"image_ref":            check.ImageRef,
		"observed_digest":      check.ObservedDigest,
		"current_app_digest":   check.CurrentAppDigest,
		"last_queued_digest":   check.LastQueuedDigest,
		"last_deployed_digest": check.LastDeployedDigest,
		"decision":             check.Decision,
		"skip_reason":          check.SkipReason,
		"operation_id":         check.OperationID,
		"active_operation_id":  check.ActiveOperationID,
		"resolver_error":       check.ResolverError,
		"duration_ms":          check.DurationMilliseconds,
	}
	if encoded, err := json.Marshal(payload); err == nil {
		s.logger.Print(string(encoded))
	}
}

func (s *Poller) latestImageTrackingActiveOperationID(app model.App) (string, error) {
	return s.activeOperationID(app)
}

func ControllerPodName() string {
	if value := strings.TrimSpace(os.Getenv("POD_NAME")); value != "" {
		return value
	}
	if value := strings.TrimSpace(os.Getenv("HOSTNAME")); value != "" {
		return value
	}
	host, err := os.Hostname()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(host)
}

func imageTrackingEventNameOrDefault(event string) string {
	if value := strings.TrimSpace(event); value != "" {
		return value
	}
	return "poll"
}

func (s *Poller) resolveTrackedImageDigest(ctx context.Context, imageRef string) (string, error) {
	resolver := s.resolveDigest
	if resolver == nil {
		return "", fmt.Errorf("image digest resolver is not configured")
	}
	checkCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	digest, err := resolver(checkCtx, imageRef)
	if err != nil {
		return "", err
	}
	digest = strings.TrimSpace(digest)
	if digest == "" {
		return "", fmt.Errorf("image digest resolver returned empty digest")
	}
	return digest, nil
}
