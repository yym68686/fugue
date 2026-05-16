package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fugue/internal/appimages"
	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

type deployImageTarget struct {
	RuntimeID       string
	ClusterNodeName string
}

func (s *Service) ensureDeployableImage(ctx context.Context, op model.Operation, app model.App, scheduling runtimepkg.SchedulingConstraints) error {
	if s == nil || app.Spec.Replicas <= 0 {
		return nil
	}

	managedImageRef := strings.TrimSpace(s.managedDeployImageRef(app))
	if managedImageRef == "" {
		return nil
	}
	target := s.deployImageTarget(app, scheduling)

	exists, err := s.deployImageRefAvailable(ctx, app, target, managedImageRef)
	if err != nil {
		return fmt.Errorf("inspect managed image %s before deploy: %w", managedImageRef, err)
	}
	if !exists {
		return s.handleMissingDeployImage(ctx, op, app, target, managedImageRef, "managed image")
	}
	runtimeImageRef := strings.TrimSpace(app.Spec.Image)
	if runtimeImageRef == "" || runtimeImageRef == managedImageRef {
		return nil
	}
	runtimeInspectionRef := strings.TrimSpace(s.runtimeImageInspectionRef(runtimeImageRef))
	if runtimeInspectionRef == "" {
		runtimeInspectionRef = runtimeImageRef
	}
	if runtimeInspectionRef == managedImageRef {
		s.scheduleImageHydration(ctx, app, target, runtimeImageRef)
		return nil
	}
	exists, err = s.deployImageRefAvailable(ctx, app, target, runtimeInspectionRef, runtimeImageRef)
	if err != nil {
		return fmt.Errorf("inspect runtime image %s before deploy using %s: %w", runtimeImageRef, runtimeInspectionRef, err)
	}
	if !exists {
		return s.handleMissingDeployImage(ctx, op, app, target, runtimeImageRef, "runtime image")
	}
	return nil
}

func (s *Service) ensureManagedDeployImageReady(ctx context.Context, app model.App) error {
	return s.ensureDeployableImage(ctx, model.Operation{}, app, runtimepkg.SchedulingConstraints{})
}

func (s *Service) deployImageRefAvailable(ctx context.Context, app model.App, target deployImageTarget, refs ...string) (bool, error) {
	refs = compactImageRefs(refs)
	if len(refs) == 0 {
		return true, nil
	}
	if s.inspectManagedImage != nil {
		for _, ref := range refs {
			exists, err := s.inspectManagedImageWithRetry(ctx, ref)
			if err != nil {
				return false, err
			}
			if exists {
				s.scheduleImageHydration(ctx, app, target, ref)
				return true, nil
			}
		}
	}
	locations, err := s.presentImageLocations(app, refs...)
	if err != nil {
		return false, err
	}
	if len(locations) == 0 {
		return false, nil
	}
	if imageLocationPresentOnTarget(locations, target) {
		return true, nil
	}
	s.scheduleImageHydration(ctx, app, target, refs[0])
	return true, nil
}

func (s *Service) presentImageLocations(app model.App, refs ...string) ([]model.ImageLocation, error) {
	if s == nil || s.Store == nil {
		return nil, nil
	}
	seen := map[string]struct{}{}
	out := []model.ImageLocation{}
	for _, ref := range compactImageRefs(refs) {
		locations, err := s.Store.ListImageLocations(model.ImageLocationFilter{
			TenantID: strings.TrimSpace(app.TenantID),
			AppID:    strings.TrimSpace(app.ID),
			ImageRef: ref,
			Status:   model.ImageLocationStatusPresent,
		})
		if err != nil {
			return nil, err
		}
		if len(locations) == 0 {
			locations, err = s.Store.ListImageLocations(model.ImageLocationFilter{
				TenantID: strings.TrimSpace(app.TenantID),
				ImageRef: ref,
				Status:   model.ImageLocationStatusPresent,
			})
			if err != nil {
				return nil, err
			}
		}
		for _, location := range locations {
			key := strings.TrimSpace(location.ID)
			if key == "" {
				key = strings.Join([]string{location.ImageRef, location.Digest, location.NodeID, location.RuntimeID, location.ClusterNodeName}, "\x00")
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, location)
		}
	}
	return out, nil
}

func (s *Service) handleMissingDeployImage(ctx context.Context, op model.Operation, app model.App, target deployImageTarget, imageRef, label string) error {
	imageRef = strings.TrimSpace(imageRef)
	if imageRef == "" {
		return nil
	}
	if existing, ok := s.activeImportOperationForApp(app); ok {
		if strings.TrimSpace(op.ID) != "" {
			_, _ = s.Store.FailOperation(op.ID, fmt.Sprintf("%s %s is missing; waiting for rebuild operation %s", label, imageRef, existing.ID))
			return errOperationNoLongerActive
		}
		return fmt.Errorf("deploy blocked because %s %s is missing; rebuild operation %s is already active", label, imageRef, existing.ID)
	}
	source := model.AppBuildSource(app)
	if !deployImageSourceCanRebuild(source) {
		return fmt.Errorf("deploy blocked because %s %s is missing and app has no rebuildable source", label, imageRef)
	}
	if strings.TrimSpace(op.ID) == "" {
		return fmt.Errorf("deploy blocked because %s %s is missing and needs rebuild", label, imageRef)
	}
	if _, err := s.Store.FailOperation(op.ID, fmt.Sprintf("%s %s is missing; queued image rebuild", label, imageRef)); err != nil {
		return fmt.Errorf("mark deploy operation waiting for rebuild: %w", err)
	}
	spec := app.Spec
	if spec.Replicas <= 0 {
		spec.Replicas = 1
	}
	buildSource := model.CloneAppSource(source)
	buildSource.ResolvedImageRef = ""
	originSource := model.AppOriginSource(app)
	if originSource == nil {
		originSource = model.CloneAppSource(buildSource)
	}
	rebuildOp, err := s.Store.CreateOperation(model.Operation{
		TenantID:            app.TenantID,
		Type:                model.OperationTypeImport,
		RequestedByType:     model.ActorTypeSystem,
		RequestedByID:       model.OperationRequestedByImageRebuild,
		AppID:               app.ID,
		TargetRuntimeID:     strings.TrimSpace(target.RuntimeID),
		DesiredSpec:         &spec,
		DesiredSource:       buildSource,
		DesiredOriginSource: originSource,
	})
	if err != nil {
		return fmt.Errorf("queue image rebuild for missing %s %s: %w", label, imageRef, err)
	}
	if s.Logger != nil {
		s.Logger.Printf("queued image rebuild operation %s for app=%s missing_%s=%s target_runtime=%s target_node=%s", rebuildOp.ID, app.ID, strings.ReplaceAll(label, " ", "_"), imageRef, target.RuntimeID, target.ClusterNodeName)
	}
	_ = ctx
	return errOperationNoLongerActive
}

func (s *Service) activeImportOperationForApp(app model.App) (model.Operation, bool) {
	if s == nil || s.Store == nil {
		return model.Operation{}, false
	}
	ops, err := s.Store.ListOperationsByApp(app.TenantID, true, app.ID)
	if err != nil {
		return model.Operation{}, false
	}
	for _, candidate := range ops {
		if strings.TrimSpace(candidate.Type) != model.OperationTypeImport {
			continue
		}
		switch strings.TrimSpace(candidate.Status) {
		case model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent:
			return candidate, true
		}
	}
	return model.Operation{}, false
}

func deployImageSourceCanRebuild(source *model.AppSource) bool {
	if source == nil {
		return false
	}
	switch strings.TrimSpace(source.Type) {
	case model.AppSourceTypeDockerImage:
		return strings.TrimSpace(source.ImageRef) != ""
	case model.AppSourceTypeGitHubPublic, model.AppSourceTypeGitHubPrivate:
		return strings.TrimSpace(source.RepoURL) != ""
	case model.AppSourceTypeUpload:
		return strings.TrimSpace(source.UploadID) != ""
	default:
		return false
	}
}

func (s *Service) deployImageTarget(app model.App, scheduling runtimepkg.SchedulingConstraints) deployImageTarget {
	target := deployImageTarget{RuntimeID: strings.TrimSpace(app.Spec.RuntimeID)}
	if scheduling.NodeSelector != nil {
		target.ClusterNodeName = strings.TrimSpace(scheduling.NodeSelector[kubeHostnameLabelKey])
	}
	if target.ClusterNodeName != "" || target.RuntimeID == "" || s == nil || s.Store == nil {
		return target
	}
	runtimeObj, err := s.Store.GetRuntime(target.RuntimeID)
	if err == nil {
		target.ClusterNodeName = strings.TrimSpace(runtimeObj.ClusterNodeName)
	}
	return target
}

func imageLocationPresentOnTarget(locations []model.ImageLocation, target deployImageTarget) bool {
	for _, location := range locations {
		if target.ClusterNodeName != "" && strings.TrimSpace(location.ClusterNodeName) == target.ClusterNodeName {
			return true
		}
		if target.RuntimeID != "" && strings.TrimSpace(location.RuntimeID) == target.RuntimeID {
			return true
		}
	}
	return false
}

func (s *Service) scheduleImageHydration(ctx context.Context, app model.App, target deployImageTarget, imageRef string) {
	if s == nil || s.Store == nil || strings.TrimSpace(imageRef) == "" {
		return
	}
	if strings.TrimSpace(target.ClusterNodeName) == "" && strings.TrimSpace(target.RuntimeID) == "" {
		return
	}
	_, err := s.Store.CreateNodeUpdateTask(model.Principal{
		ActorType: model.ActorTypeSystem,
		ActorID:   "fugue-controller/image-hydrate",
		TenantID:  strings.TrimSpace(app.TenantID),
		Scopes:    map[string]struct{}{"platform.admin": {}},
	}, "", target.ClusterNodeName, target.RuntimeID, model.NodeUpdateTaskTypePrepullAppImages, map[string]string{
		"images":    imageRef,
		"image_ref": imageRef,
		"app_id":    strings.TrimSpace(app.ID),
	})
	if err != nil && s.Logger != nil {
		s.Logger.Printf("schedule image hydrate task app=%s image=%s runtime=%s node=%s failed: %v", app.ID, imageRef, target.RuntimeID, target.ClusterNodeName, err)
	}
	_ = ctx
}

func compactImageRefs(refs []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			continue
		}
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	return out
}

func (s *Service) recordImportedImageLocation(app model.App, op model.Operation, refs ...string) {
	if s == nil || s.Store == nil {
		return
	}
	now := time.Now().UTC()
	if s.now != nil {
		now = s.now().UTC()
	}
	for _, ref := range compactImageRefs(refs) {
		if _, err := s.Store.UpsertImageLocation(model.ImageLocation{
			TenantID:          strings.TrimSpace(app.TenantID),
			AppID:             strings.TrimSpace(app.ID),
			ImageRef:          ref,
			SourceOperationID: strings.TrimSpace(op.ID),
			Status:            model.ImageLocationStatusPresent,
			LastSeenAt:        &now,
		}); err != nil && s.Logger != nil {
			s.Logger.Printf("record imported image location app=%s op=%s image=%s failed: %v", app.ID, op.ID, ref, err)
		}
	}
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

func (s *Service) runtimeImageInspectionRef(runtimeImageRef string) string {
	if s == nil {
		return ""
	}
	return managedRegistryRefFromRuntimeImageRef(runtimeImageRef, s.registryPushBase, s.registryPullBase)
}
