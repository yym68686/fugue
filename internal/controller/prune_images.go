package controller

import (
	"context"
	"errors"
	"strings"

	"fugue/internal/appimages"
	"fugue/internal/model"
)

func (s *Service) pruneExcessManagedAppImages(ctx context.Context, app model.App) error {
	if s == nil || s.Store == nil || s.inspectManagedImage == nil || strings.TrimSpace(s.registryPushBase) == "" {
		return nil
	}

	deleteImage := s.deleteManagedImage
	if deleteImage == nil {
		deleteImage = appimages.DeleteRemoteImage
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

	var errs []error
	for _, imageRef := range imageRefs {
		if _, inUse := remainingRefs[imageRef]; inUse {
			continue
		}
		if _, err := deleteImage(ctx, imageRef); err != nil {
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
