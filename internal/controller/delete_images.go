package controller

import (
	"context"
	"errors"
	"strings"

	"fugue/internal/appimages"
	"fugue/internal/model"
)

func (s *Service) cleanupDeletedAppImages(ctx context.Context, app model.App) error {
	if s == nil || s.Store == nil || strings.TrimSpace(s.registryPushBase) == "" {
		return nil
	}

	targetOps, err := s.Store.ListOperationsByApp(app.TenantID, true, app.ID)
	if err != nil {
		return err
	}
	remainingApps, err := s.Store.ListAppsMetadata("", true)
	if err != nil {
		return err
	}
	remainingOps, err := s.Store.ListOperations("", true)
	if err != nil {
		return err
	}

	remainingRefs := appimages.ManagedImageRefSet(
		remainingApps,
		remainingOps,
		s.registryPushBase,
		s.registryPullBase,
	)
	mergeManagedImageRefSets(remainingRefs, s.liveManagedImageRefSet(ctx, append(append([]model.App(nil), remainingApps...), app)))

	imageRefs := appimages.ManagedImageRefs(
		app,
		targetOps,
		s.registryPushBase,
		s.registryPullBase,
	)
	if len(imageRefs) == 0 {
		return nil
	}

	deleteImage := s.deleteManagedImage
	if deleteImage == nil {
		deleteImage = appimages.DeleteRemoteImage
	}

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
