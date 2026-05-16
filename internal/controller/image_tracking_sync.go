package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Service) syncTrackedAppImages(ctx context.Context) error {
	enabled := true
	trackings, err := s.Store.ListAppImageTrackings(model.AppImageTrackingFilter{
		PlatformAdmin: true,
		Enabled:       &enabled,
	})
	if err != nil {
		return fmt.Errorf("list image trackings: %w", err)
	}

	for _, tracking := range trackings {
		app, err := s.Store.GetApp(tracking.AppID)
		if err != nil {
			if !errors.Is(err, store.ErrNotFound) && s.Logger != nil {
				s.Logger.Printf("image tracking load app failed app=%s: %v", tracking.AppID, err)
			}
			continue
		}
		if app.Spec.Replicas <= 0 {
			continue
		}
		hasActiveOp, err := s.Store.HasActiveOperationByApp(app.TenantID, true, app.ID)
		if err != nil {
			if s.Logger != nil {
				s.Logger.Printf("image tracking active operation check failed app=%s image=%s: %v", app.ID, tracking.ImageRef, err)
			}
			continue
		}
		if hasActiveOp {
			continue
		}

		digest, err := s.resolveTrackedImageDigest(ctx, tracking.ImageRef)
		if err != nil {
			if _, recordErr := s.Store.RecordAppImageTrackingCheck(tracking.ID, "", "", "poll", err.Error()); recordErr != nil && s.Logger != nil {
				s.Logger.Printf("image tracking check record failed app=%s image=%s: %v", app.ID, tracking.ImageRef, recordErr)
			}
			if s.Logger != nil {
				s.Logger.Printf("image tracking digest check failed app=%s image=%s: %v", app.ID, tracking.ImageRef, err)
			}
			continue
		}
		if strings.TrimSpace(digest) == strings.TrimSpace(tracking.LastDeployedDigest) {
			if _, err := s.Store.RecordAppImageTrackingCheck(tracking.ID, digest, "", "poll", ""); err != nil && s.Logger != nil {
				s.Logger.Printf("image tracking no-op record failed app=%s image=%s digest=%s: %v", app.ID, tracking.ImageRef, digest, err)
			}
			continue
		}
		if strings.TrimSpace(digest) == strings.TrimSpace(tracking.LastQueuedDigest) && !imageTrackingRetryReady(tracking, s.currentTime()) {
			continue
		}

		op, err := s.Store.QueueAppImageTrackingImport(app, tracking, model.ActorTypeBootstrap, model.OperationRequestedByImageTracking)
		if err != nil {
			if errors.Is(err, store.ErrConflict) {
				continue
			}
			if _, recordErr := s.Store.RecordAppImageTrackingCheck(tracking.ID, digest, "", "poll", err.Error()); recordErr != nil && s.Logger != nil {
				s.Logger.Printf("image tracking queue failure record failed app=%s image=%s digest=%s: %v", app.ID, tracking.ImageRef, digest, recordErr)
			}
			if s.Logger != nil {
				s.Logger.Printf("image tracking queue failed app=%s image=%s digest=%s: %v", app.ID, tracking.ImageRef, digest, err)
			}
			continue
		}
		if _, err := s.Store.RecordAppImageTrackingQueued(tracking.ID, digest, op.ID, "", "poll"); err != nil && s.Logger != nil {
			s.Logger.Printf("image tracking queued record failed app=%s image=%s digest=%s op=%s: %v", app.ID, tracking.ImageRef, digest, op.ID, err)
		}
		if s.Logger != nil {
			s.Logger.Printf("image tracking queued import app=%s image=%s old_digest=%s new_digest=%s op=%s", app.ID, tracking.ImageRef, tracking.LastDeployedDigest, digest, op.ID)
		}
	}
	return nil
}

func (s *Service) resolveTrackedImageDigest(ctx context.Context, imageRef string) (string, error) {
	resolver := s.resolveRemoteImageDigest
	if resolver == nil {
		return "", fmt.Errorf("image digest resolver is not configured")
	}
	checkCtx, cancel := context.WithTimeout(ctx, s.Config.ImageTrackingTimeout)
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

func (s *Service) currentTime() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func imageTrackingRetryReady(tracking model.AppImageTracking, now time.Time) bool {
	if tracking.LastTriggeredAt == nil {
		return true
	}
	return !tracking.LastTriggeredAt.Add(5 * time.Minute).After(now)
}
