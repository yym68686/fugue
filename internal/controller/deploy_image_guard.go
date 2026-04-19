package controller

import (
	"context"
	"fmt"
	"strings"

	"fugue/internal/appimages"
	"fugue/internal/model"
)

func (s *Service) ensureManagedDeployImageReady(ctx context.Context, app model.App) error {
	if s == nil || app.Spec.Replicas <= 0 {
		return nil
	}

	managedImageRef := strings.TrimSpace(s.managedDeployImageRef(app))
	if managedImageRef == "" {
		return nil
	}
	if s.inspectManagedImage == nil {
		return fmt.Errorf("deploy blocked because managed image %s could not be confirmed: image inspector is not configured", managedImageRef)
	}

	exists, err := s.inspectManagedImageWithRetry(ctx, managedImageRef)
	if err != nil {
		return fmt.Errorf("inspect managed image %s before deploy: %w", managedImageRef, err)
	}
	if !exists {
		return fmt.Errorf("deploy blocked because managed image %s is still missing from the registry", managedImageRef)
	}
	runtimeImageRef := strings.TrimSpace(app.Spec.Image)
	if runtimeImageRef == "" || runtimeImageRef == managedImageRef {
		return nil
	}
	exists, err = s.inspectManagedImageWithRetry(ctx, runtimeImageRef)
	if err != nil {
		return fmt.Errorf("inspect runtime image %s before deploy: %w", runtimeImageRef, err)
	}
	if !exists {
		return fmt.Errorf("deploy blocked because runtime image %s is still missing from the registry", runtimeImageRef)
	}
	return nil
}

func (s *Service) managedDeployImageRef(app model.App) string {
	if s == nil {
		return ""
	}
	if app.Source != nil {
		if ref := strings.TrimSpace(app.Source.ResolvedImageRef); ref != "" {
			return ref
		}
		if ref := strings.TrimSpace(managedRegistryRefFromRuntimeImageRef(app.Spec.Image, s.registryPushBase, s.registryPullBase)); ref != "" {
			return ref
		}
		if strings.TrimSpace(s.registryPushBase) != "" {
			if ref := strings.TrimSpace(appimages.ManagedImageRefForSource(app, app.Source, app.Spec.Image, s.registryPushBase, s.registryPullBase)); ref != "" {
				return ref
			}
		}
	}
	return managedRegistryRefFromRuntimeImageRef(app.Spec.Image, s.registryPushBase, s.registryPullBase)
}
