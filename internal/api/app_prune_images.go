package api

import (
	"context"
	"errors"

	"fugue/internal/appimages"
	"fugue/internal/model"
)

func (s *Server) pruneExcessManagedAppImages(ctx context.Context, app model.App) error {
	if s == nil || s.store == nil || !s.appImageInventoryConfigured() {
		return nil
	}

	targetOps, err := s.store.ListOperationsByApp(app.TenantID, true, app.ID)
	if err != nil {
		return err
	}
	allApps, err := s.store.ListAppsMetadata("", true)
	if err != nil {
		return err
	}
	allOps, err := s.store.ListOperations("", true)
	if err != nil {
		return err
	}

	imageRefs, err := appimages.ExcessManagedImageRefs(
		ctx,
		func(ctx context.Context, imageRef string) (bool, map[string]int64, error) {
			result, err := s.appImageRegistry.InspectImage(ctx, imageRef)
			if err != nil {
				return false, nil, err
			}
			return result.Exists, result.BlobSizes, nil
		},
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

	var errs []error
	gcNeeded := false
	for _, imageRef := range imageRefs {
		if _, inUse := remainingRefs[imageRef]; inUse {
			continue
		}
		if _, err := s.appImageRegistry.DeleteImage(ctx, imageRef); err != nil {
			errs = append(errs, err)
			continue
		}
		gcNeeded = true
	}
	if gcNeeded {
		if err := s.runAppImageRegistryGarbageCollect(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
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
