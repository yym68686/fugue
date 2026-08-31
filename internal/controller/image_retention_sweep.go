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
	gates, err := s.Store.MigrationArtifactRetirementGates()
	if err != nil {
		return fmt.Errorf("snapshot migration artifact retirement gates: %w", err)
	}
	if err := s.sweepExpiredDistributedImagePins(ctx, gates); err != nil {
		return err
	}
	deletedApps, err := s.Store.ListDeletedAppsMetadata("", true)
	if err != nil {
		return fmt.Errorf("list deleted apps for distributed image cleanup: %w", err)
	}
	apps, err := s.Store.ListAppsMetadata("", true)
	if err != nil {
		return fmt.Errorf("list apps: %w", err)
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
	deletedAppsReadyForRetirement := make([]model.App, 0, len(deletedApps))
	for _, app := range deletedApps {
		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("stop deleted app image cleanup sweep: %w", err))
			break
		}
		if tenantID := strings.TrimSpace(app.TenantID); tenantID != "" {
			tenantIDs[tenantID] = struct{}{}
		}
		if gate, blocked := gates[strings.TrimSpace(app.ID)]; blocked {
			_ = s.Store.RecordMigrationArtifactRetirementBlocked(app.ID, "distributed image cleanup blocked: "+gate.Reason)
			continue
		}
		if err := s.deleteDeletedAppDistributedImagePins(ctx, app); err != nil {
			errs = append(errs, fmt.Errorf("remove deleted app %s distributed image pins: %w", strings.TrimSpace(app.ID), err))
			if isContextStopped(ctx, err) {
				break
			}
			continue
		}
		deletedAppsReadyForRetirement = append(deletedAppsReadyForRetirement, app)
	}
	if len(deletedAppsReadyForRetirement) > 0 && ctx.Err() == nil {
		protected, err := s.controllerImageCacheProtectedSet(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("snapshot deleted app distributed image protections: %w", err))
		} else {
			now := time.Now().UTC()
			for _, app := range deletedAppsReadyForRetirement {
				if err := s.retireDeletedAppDistributedImagesWithProtection(ctx, app, protected, now); err != nil {
					errs = append(errs, fmt.Errorf("retire deleted app %s distributed images: %w", strings.TrimSpace(app.ID), err))
					if isContextStopped(ctx, err) {
						break
					}
				}
			}
		}
	}
	for _, app := range apps {
		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("stop distributed image retention sweep: %w", err))
			break
		}
		if tenantID := strings.TrimSpace(app.TenantID); tenantID != "" {
			tenantIDs[tenantID] = struct{}{}
		}
		if gate, blocked := gates[strings.TrimSpace(app.ID)]; blocked {
			_ = s.Store.RecordMigrationArtifactRetirementBlocked(app.ID, "distributed image retention blocked: "+gate.Reason)
			if s.Logger != nil {
				s.Logger.Printf("preserve old distributed image artifacts for %s: retention is blocked: %s", app.ID, gate.Reason)
			}
			continue
		}
		plan, err := s.reconcileDistributedImageRetentionForAppAfterMigrationGate(ctx, app, opsByAppID[app.ID], liveRefs)
		if err != nil {
			errs = append(errs, fmt.Errorf("reconcile app %s distributed image retention: %w", strings.TrimSpace(app.ID), err))
			if isContextStopped(ctx, err) {
				break
			}
			continue
		}
		if s.Logger != nil && len(plan.DropImageIDs) > 0 {
			s.Logger.Printf("distributed image retention reconciled %s", distributedImageRetentionPlanSummary(plan))
		}
	}
	if s.syncBillingImageStorage {
		for tenantID := range tenantIDs {
			if err := ctx.Err(); err != nil {
				errs = append(errs, fmt.Errorf("stop distributed tenant billing image storage sync: %w", err))
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

func isContextStopped(ctx context.Context, err error) bool {
	if ctx != nil && ctx.Err() != nil {
		return true
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
