package controller

import (
	"context"
	"errors"
	"strings"

	"fugue/internal/appimages"
	"fugue/internal/model"
)

func (s *Service) pruneExcessManagedAppImages(ctx context.Context, app model.App) error {
	if s == nil || s.Store == nil || strings.TrimSpace(s.registryPushBase) == "" {
		return nil
	}
	if s.imageStoreDistributedMode() {
		targetOps, err := s.Store.ListOperationsByApp(app.TenantID, true, app.ID)
		if err != nil {
			return err
		}
		allApps, err := s.Store.ListAppsMetadata("", true)
		if err != nil {
			return err
		}
		scan := s.liveManagedImageRefScanWithLookup(ctx, allApps, allApps)
		if !scan.Complete {
			return nil
		}
		_, err = s.reconcileDistributedImageRetentionForApp(ctx, app, targetOps, scan.Refs)
		return err
	}
	if s.inspectManagedImage == nil {
		return nil
	}

	targetOps, err := s.Store.ListOperationsByApp(app.TenantID, true, app.ID)
	if err != nil {
		return err
	}
	allApps, err := s.Store.ListAppsMetadata("", true)
	if err != nil {
		return err
	}
	allOps, err := s.Store.ListOperations("", true)
	if err != nil {
		return err
	}
	scan := s.liveManagedImageRefScanWithLookup(ctx, allApps, allApps)
	if !scan.Complete {
		return nil
	}
	return s.pruneExcessManagedAppImagesWithSnapshot(ctx, app, targetOps, allApps, allOps, scan.Refs)
}

func (s *Service) pruneExcessManagedAppImagesWithSnapshot(
	ctx context.Context,
	app model.App,
	targetOps []model.Operation,
	allApps []model.App,
	allOps []model.Operation,
	liveRefs map[string]struct{},
) error {
	if s == nil || strings.TrimSpace(s.registryPushBase) == "" {
		return nil
	}
	// A migration keeps both the source and target artifacts live until the
	// target-side evidence envelope has been verified.  This guard sits at the
	// lowest shared retention entry point so scheduled sweeps, post-deploy
	// maintenance, and direct controller calls cannot bypass the cutover gate.
	if blocked, reason, err := s.Store.MigrationArtifactsRetirementBlocked(app.ID); err != nil {
		return err
	} else if blocked {
		_ = s.Store.RecordMigrationArtifactRetirementBlocked(app.ID, "managed image retention blocked: "+reason)
		if s.Logger != nil {
			s.Logger.Printf("preserve old app artifacts for %s: managed image retention is blocked: %s", app.ID, reason)
		}
		return nil
	}
	if s.imageStoreDistributedMode() {
		_, err := s.reconcileDistributedImageRetentionForApp(ctx, app, targetOps, liveRefs)
		return err
	}
	if s.inspectManagedImage == nil {
		return nil
	}
	deleteImage := s.deleteManagedImage
	if deleteImage == nil {
		deleteImage = appimages.DeleteRemoteImage
	}

	imageRefs, err := appimages.ExcessManagedImageRefs(
		ctx,
		s.inspectManagedImage,
		app,
		targetOps,
		s.registryPushBase,
		s.registryPullBase,
		app.Spec.ImageMirrorLimit,
	)
	if err != nil || len(imageRefs) == 0 {
		return err
	}

	remainingRefs := appimages.ManagedImageRefSet(
		appsExcludingID(allApps, app.ID),
		operationsExcludingAppID(allOps, app.ID),
		s.registryPushBase,
		s.registryPullBase,
	)
	mergeManagedImageRefSets(remainingRefs, liveRefs)

	var errs []error
	for _, imageRef := range imageRefs {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if _, inUse := remainingRefs[imageRef]; inUse {
			continue
		}
		digestInUse, err := s.managedImageDigestInUse(ctx, imageRef, liveRefs)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if digestInUse {
			continue
		}
		result, err := deleteImage(ctx, imageRef)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if result.Deleted {
			s.markRegistryGCNeeded(ctx, "managed app image retention deleted "+imageRef)
		}
	}
	return errors.Join(errs...)
}

func mergeManagedImageRefSets(target, source map[string]struct{}) {
	for imageRef := range source {
		target[imageRef] = struct{}{}
	}
}

func appsExcludingID(apps []model.App, appID string) []model.App {
	filtered := make([]model.App, 0, len(apps))
	for _, candidate := range apps {
		if candidate.ID == appID {
			continue
		}
		filtered = append(filtered, candidate)
	}
	return filtered
}

func operationsExcludingAppID(ops []model.Operation, appID string) []model.Operation {
	filtered := make([]model.Operation, 0, len(ops))
	for _, candidate := range ops {
		if candidate.AppID == appID {
			continue
		}
		filtered = append(filtered, candidate)
	}
	return filtered
}
