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
	remainingApps, err := s.Store.ListApps("", true)
	if err != nil {
		return err
	}
	remainingOps, err := s.Store.ListOperations("", true)
	if err != nil {
		return err
	}

	imageRefs := appimages.DeletableManagedImageRefs(
		app,
		targetOps,
		remainingApps,
		remainingOps,
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
		if _, err := deleteImage(ctx, imageRef); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
