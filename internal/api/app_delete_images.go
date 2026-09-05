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
	if blocked, reason, err := s.store.MigrationArtifactsRetirementBlocked(app.ID); err != nil {
		return err
	} else if blocked {
		_ = s.store.RecordMigrationArtifactRetirementBlocked(app.ID, "image cleanup blocked: "+reason)
		return nil
	}

	targetOps, err := s.store.ListOperationsByApp(app.TenantID, true, app.ID)
	if err != nil {
		return err
	}
	remainingApps, err := s.store.ListAppsMetadata("", true)
	if err != nil {
		return err
	}
	remainingOps, err := s.store.ListOperations("", true)
	if err != nil {
		return err
	}

	remainingRefs := appimages.ManagedImageRefSet(
		remainingApps,
		remainingOps,
		s.registryPushBase,
		s.registryPullBase,
	)
	liveLookupApps := append(append([]model.App(nil), remainingApps...), app)
	liveScan := s.liveManagedImageReferenceScan(ctx, remainingApps, liveLookupApps)
	if !liveScan.Complete {
		_ = s.store.RecordMigrationArtifactRetirementBlocked(app.ID, "image cleanup blocked: live reference scan incomplete")
		if s.log != nil {
			s.log.Printf("preserve old app artifacts for %s: live reference scan incomplete", app.ID)
		}
		return nil
	}
	for _, reference := range liveScan.References {
		remainingRefs[reference.ImageRef] = struct{}{}
	}

	imageRefs := appimages.ManagedImageRefs(
		app,
		targetOps,
		s.registryPushBase,
		s.registryPullBase,
	)
	if len(imageRefs) == 0 {
		return nil
	}

	var errs []error
	gcNeeded := false
	for _, imageRef := range imageRefs {
		if _, inUse := remainingRefs[imageRef]; inUse {
			continue
		}
		digestInUse, err := s.managedImageDigestInUse(ctx, imageRef, remainingRefs)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if digestInUse {
			continue
		}
		if _, err := s.appImageRegistry.DeleteImage(ctx, imageRef); err != nil {
			errs = append(errs, err)
			continue
		}
		gcNeeded = true
	}
	if gcNeeded {
		if err := s.requestAppImageRegistryGarbageCollect(ctx, "API app deletion removed managed image manifests"); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
