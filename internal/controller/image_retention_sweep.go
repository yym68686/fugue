package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Service) runManagedAppImageRetentionSweep(ctx context.Context) error {
	if s == nil || s.Config.ImageRetentionSweepInterval <= 0 {
		return nil
	}
	timeout := s.Config.ImageRetentionSweepTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	sweepCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return s.sweepManagedAppImageRetention(sweepCtx)
}

func (s *Service) sweepManagedAppImageRetention(ctx context.Context) error {
	if s == nil || s.Store == nil || strings.TrimSpace(s.registryPushBase) == "" {
		return nil
	}
	if s.imageStoreDistributedMode() {
		return s.sweepDistributedImageRetention(ctx)
	}
	if s.inspectManagedImage == nil {
		return nil
	}

	apps, err := s.Store.ListAppsMetadata("", true)
	if err != nil {
		return fmt.Errorf("list apps: %w", err)
	}
	if len(apps) == 0 {
		return nil
	}
	ops, err := s.Store.ListOperations("", true)
	if err != nil {
		return fmt.Errorf("list operations: %w", err)
	}

	opsByAppID := make(map[string][]model.Operation)
	for _, op := range ops {
		appID := strings.TrimSpace(op.AppID)
		if appID == "" {
			continue
		}
		opsByAppID[appID] = append(opsByAppID[appID], op)
	}

	liveRefs := s.liveManagedImageRefSet(ctx, apps)
	tenantIDs := make(map[string]struct{})
	var errs []error
	for _, app := range apps {
		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("stop app image retention sweep: %w", err))
			break
		}
		if tenantID := strings.TrimSpace(app.TenantID); tenantID != "" {
			tenantIDs[tenantID] = struct{}{}
		}
		if err := s.pruneExcessManagedAppImagesWithSnapshot(ctx, app, opsByAppID[app.ID], apps, ops, liveRefs); err != nil {
			errs = append(errs, fmt.Errorf("prune app %s images: %w", strings.TrimSpace(app.ID), err))
			if isContextStopped(ctx, err) {
				break
			}
		}
	}

	if s.syncBillingImageStorage {
		for tenantID := range tenantIDs {
			if err := ctx.Err(); err != nil {
				errs = append(errs, fmt.Errorf("stop tenant billing image storage sync: %w", err))
				break
			}
			if err := s.syncTenantBillingImageStorage(ctx, tenantID); err != nil {
				errs = append(errs, fmt.Errorf("sync tenant %s billing image storage: %w", tenantID, err))
				if isContextStopped(ctx, err) {
					break
				}
			}
		}
	}

	return errors.Join(errs...)
}

func (s *Service) sweepDistributedImageRetention(ctx context.Context) error {
	if s == nil || s.Store == nil {
		return nil
	}
	if err := s.sweepExpiredDistributedImagePins(ctx); err != nil {
		return err
	}
	apps, err := s.Store.ListAppsMetadata("", true)
	if err != nil {
		return fmt.Errorf("list apps: %w", err)
	}
	var errs []error
	for _, app := range apps {
		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("stop distributed image retention sweep: %w", err))
			break
		}
		if err := s.scheduleDistributedImagePruneForApp(ctx, app); err != nil {
			errs = append(errs, fmt.Errorf("schedule app %s distributed image prune: %w", strings.TrimSpace(app.ID), err))
			if isContextStopped(ctx, err) {
				break
			}
		}
	}
	return errors.Join(errs...)
}

func isContextStopped(ctx context.Context, err error) bool {
	if ctx != nil && ctx.Err() != nil {
		return true
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
