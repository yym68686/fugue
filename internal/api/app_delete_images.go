package api

import (
	"context"
	"errors"

	"fugue/internal/appimages"
	"fugue/internal/model"
)

func (s *Server) cleanupDeletedAppImages(ctx context.Context, app model.App) error {
	if s == nil || s.store == nil || !s.appImageInventoryConfigured() {
		return nil
	}

	targetOps, err := s.store.ListOperationsByApp(app.TenantID, true, app.ID)
	if err != nil {
		return err
	}
	remainingApps, err := s.store.ListApps("", true)
	if err != nil {
		return err
	}
	remainingOps, err := s.store.ListOperations("", true)
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

	var errs []error
	gcNeeded := false
	for _, imageRef := range imageRefs {
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
