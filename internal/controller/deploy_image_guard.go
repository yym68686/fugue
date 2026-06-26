package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/appimages"
	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

type deployImageTarget struct {
	RuntimeID       string
	ClusterNodeName string
}

var errDeployImageReplicationPending = errors.New("deploy image replication is pending")

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
		if errors.Is(err, errDeployImageReplicationPending) {
			return s.handlePendingDeployImageReplication(op, err)
		}
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
		if errors.Is(err, errDeployImageReplicationPending) {
			return s.handlePendingDeployImageReplication(op, err)
		}
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
	if s.imageStoreDistributedMode() {
		exists, checked, err := s.deployImageReplicaAvailable(ctx, app, target, refs...)
		if err != nil {
			return false, err
		}
		if exists {
			return true, nil
		}
		if !s.imageStoreRegistryFallbackEnabled() {
			if !checked {
				locations, err := s.presentImageLocations(app, refs...)
				if err != nil {
					return false, err
				}
				if len(locations) > 0 {
					if imageLocationPresentOnTarget(locations, target) {
						return true, nil
					}
					s.scheduleImageHydration(ctx, app, target, refs[0])
					return true, nil
				}
			}
			return false, nil
		}
	}
	var inspectErr error
	if s.inspectManagedImage != nil {
		for _, ref := range refs {
			if !s.shouldInspectControllerImageRef(ref) {
				continue
			}
			exists, err := s.inspectManagedImageWithRetry(ctx, ref)
			if err != nil {
				if !s.nodeLocalBuilderRegistryEnabled() {
					return false, err
				}
				if inspectErr == nil {
					inspectErr = err
				}
				if s.Logger != nil {
					s.Logger.Printf("inspect deploy image failed; checking node-local image location evidence app=%s image=%s: %v", app.ID, ref, err)
				}
				continue
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
		if inspectErr != nil {
			return false, inspectErr
		}
		return false, nil
	}
	if imageLocationPresentOnTarget(locations, target) {
		return true, nil
	}
	s.scheduleImageHydration(ctx, app, target, refs[0])
	return true, nil
}

func (s *Service) nodeLocalBuilderRegistryEnabled() bool {
	if s == nil {
		return false
	}
	builderPushBase := strings.Trim(strings.TrimSpace(s.builderRegistryPushBase), "/")
	registryPushBase := strings.Trim(strings.TrimSpace(s.registryPushBase), "/")
	return builderPushBase != "" && builderPushBase != registryPushBase
}

func (s *Service) shouldInspectControllerImageRef(imageRef string) bool {
	imageRef = strings.TrimSpace(imageRef)
	if imageRef == "" {
		return false
	}
	if s == nil {
		return true
	}
	pushBase := strings.Trim(strings.TrimSpace(s.registryPushBase), "/")
	pullBase := strings.Trim(strings.TrimSpace(s.registryPullBase), "/")
	if pushBase != "" && pullBase != "" && pullBase != pushBase && strings.HasPrefix(imageRef, pullBase+"/") {
		return false
	}
	return true
}

func (s *Service) presentImageLocations(app model.App, refs ...string) ([]model.ImageLocation, error) {
	if s == nil || s.Store == nil {
		return nil, nil
	}
	seen := map[string]struct{}{}
	out := []model.ImageLocation{}
	appendLocation := func(location model.ImageLocation) {
		key := strings.TrimSpace(location.ID)
		if key == "" {
			key = strings.Join([]string{location.ImageRef, location.Digest, location.NodeID, location.RuntimeID, location.ClusterNodeName}, "\x00")
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, location)
	}
	for _, ref := range compactImageRefs(refs) {
		images, err := s.distributedImagesForRef(app, ref)
		if err != nil {
			return nil, err
		}
		for _, image := range images {
			replicas, err := s.Store.ListImageReplicas(model.ImageReplicaFilter{
				ImageID:  image.ID,
				TenantID: image.TenantID,
				Status:   model.ImageReplicaStatusPresent,
			})
			if err != nil {
				return nil, err
			}
			for _, replica := range healthyImageReplicas(replicas, time.Now().UTC()) {
				appendLocation(imageLocationFromDistributedReplica(image, replica))
			}
		}
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
			appendLocation(location)
		}
	}
	return out, nil
}

func (s *Service) deployImageReplicaAvailable(ctx context.Context, app model.App, target deployImageTarget, refs ...string) (bool, bool, error) {
	images, err := s.distributedImagesForRefs(app, refs...)
	if err != nil {
		return false, false, err
	}
	if len(images) == 0 {
		return false, false, nil
	}
	now := time.Now().UTC()
	for _, image := range images {
		replicas, err := s.Store.ListImageReplicas(model.ImageReplicaFilter{
			ImageID:  image.ID,
			TenantID: image.TenantID,
			Status:   model.ImageReplicaStatusPresent,
		})
		if err != nil {
			return false, true, err
		}
		healthy := healthyImageReplicas(replicas, now)
		locations, err := s.presentImageLocations(app, image.ImageRef, image.CanonicalDigest)
		if err != nil {
			return false, true, err
		}
		if imageLocationPresentOnTarget(locations, target) {
			return true, true, nil
		}
		if len(healthy) == 0 {
			if s.imageStoreStrictDistributedMode() && strings.TrimSpace(image.LifecycleState) == model.ImageLifecycleAvailable {
				image.LifecycleState = model.ImageLifecycleLost
				if _, err := s.Store.UpsertImage(image); err != nil && s.Logger != nil {
					s.Logger.Printf("mark lost distributed image failed image=%s ref=%s: %v", image.ID, image.ImageRef, err)
				}
			}
			continue
		}
		if imageLocationPresentOnTarget(imageLocationsFromReplicas(image, healthy), target) {
			return true, true, nil
		}
		scheduled, err := s.scheduleDeployTargetImageReplica(ctx, image, healthy[0], target)
		if err != nil {
			return false, true, err
		}
		if s.imageStoreStrictDistributedMode() {
			if scheduled {
				return false, true, fmt.Errorf("%w: image %s is being replicated to runtime=%s node=%s", errDeployImageReplicationPending, image.ImageRef, target.RuntimeID, target.ClusterNodeName)
			}
			return false, true, fmt.Errorf("%w: no image-cache updater can prepare image %s for runtime=%s node=%s", errDeployImageReplicationPending, image.ImageRef, target.RuntimeID, target.ClusterNodeName)
		}
		return true, true, nil
	}
	return false, true, nil
}

func (s *Service) distributedImagesForRefs(app model.App, refs ...string) ([]model.Image, error) {
	seen := map[string]struct{}{}
	out := []model.Image{}
	for _, ref := range compactImageRefs(refs) {
		images, err := s.distributedImagesForRef(app, ref)
		if err != nil {
			return nil, err
		}
		for _, image := range images {
			if strings.TrimSpace(image.ID) == "" {
				continue
			}
			if _, ok := seen[image.ID]; ok {
				continue
			}
			seen[image.ID] = struct{}{}
			out = append(out, image)
		}
	}
	return out, nil
}

func (s *Service) distributedImagesForRef(app model.App, ref string) ([]model.Image, error) {
	ref = strings.TrimSpace(ref)
	if s == nil || s.Store == nil || ref == "" {
		return nil, nil
	}
	seen := map[string]struct{}{}
	out := []model.Image{}
	appendImage := func(image model.Image) {
		if strings.TrimSpace(image.ID) == "" {
			return
		}
		if _, ok := seen[image.ID]; ok {
			return
		}
		seen[image.ID] = struct{}{}
		out = append(out, image)
	}
	for _, filter := range []model.ImageFilter{
		{TenantID: app.TenantID, AppID: app.ID, ImageRef: ref},
		{TenantID: app.TenantID, ImageRef: ref},
		{TenantID: app.TenantID, AppID: app.ID, CanonicalDigest: imageDigest(ref)},
		{TenantID: app.TenantID, CanonicalDigest: imageDigest(ref)},
	} {
		if filter.ImageRef == "" && filter.CanonicalDigest == "" {
			continue
		}
		images, err := s.Store.ListImages(filter)
		if err != nil {
			return nil, err
		}
		for _, image := range images {
			appendImage(image)
		}
	}
	for _, aliasFilter := range []model.ImageAliasFilter{
		{TenantID: app.TenantID, AliasRef: ref},
		{TenantID: app.TenantID, Digest: imageDigest(ref)},
	} {
		if aliasFilter.AliasRef == "" && aliasFilter.Digest == "" {
			continue
		}
		aliases, err := s.Store.ListImageAliases(aliasFilter)
		if err != nil {
			return nil, err
		}
		for _, alias := range aliases {
			image, err := s.Store.GetImage(alias.ImageID, app.TenantID, false)
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					continue
				}
				return nil, err
			}
			appendImage(image)
		}
	}
	return out, nil
}

func imageLocationsFromReplicas(image model.Image, replicas []model.ImageReplica) []model.ImageLocation {
	out := make([]model.ImageLocation, 0, len(replicas))
	for _, replica := range replicas {
		out = append(out, imageLocationFromDistributedReplica(image, replica))
	}
	return out
}

func (s *Service) handlePendingDeployImageReplication(op model.Operation, cause error) error {
	message := strings.TrimSpace(cause.Error())
	if strings.TrimSpace(op.ID) == "" {
		return cause
	}
	if _, err := s.Store.FailOperation(op.ID, message); err != nil {
		return fmt.Errorf("mark deploy operation pending image replication: %w", err)
	}
	return errOperationNoLongerActive
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
		if target.ClusterNodeName != "" {
			target.RuntimeID = ""
		}
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
	imageRef = s.nodeHydrationImageRef(imageRef)
	if imageRef == "" {
		return
	}
	if strings.TrimSpace(target.ClusterNodeName) == "" && strings.TrimSpace(target.RuntimeID) == "" {
		return
	}
	supported, err := s.Store.NodeUpdaterTargetSupportsTask("", target.ClusterNodeName, target.RuntimeID, model.NodeUpdateTaskTypePrepullAppImages)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return
		}
		if s.Logger != nil {
			s.Logger.Printf("inspect image hydrate target app=%s image=%s runtime=%s node=%s failed: %v", app.ID, imageRef, target.RuntimeID, target.ClusterNodeName, err)
		}
		return
	}
	if !supported {
		return
	}
	_, err = s.Store.CreateNodeUpdateTask(model.Principal{
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

func (s *Service) nodeHydrationImageRef(imageRef string) string {
	imageRef = strings.TrimSpace(imageRef)
	if s == nil || imageRef == "" {
		return imageRef
	}
	managedRef := strings.TrimSpace(managedRegistryRefFromRuntimeImageRef(imageRef, s.registryPushBase, s.registryPullBase))
	if managedRef == "" {
		return imageRef
	}
	runtimeRef := strings.TrimSpace(appimages.RuntimeImageRefFromManagedRef(managedRef, s.registryPushBase, s.registryPullBase))
	if runtimeRef == "" {
		return imageRef
	}
	return runtimeRef
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

func (s *Service) recordImportedImageLocationOnTarget(app model.App, op model.Operation, target deployImageTarget, cacheEndpoint string, refs ...string) {
	if s == nil || s.Store == nil || strings.TrimSpace(cacheEndpoint) == "" {
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
			RuntimeID:         strings.TrimSpace(target.RuntimeID),
			ClusterNodeName:   strings.TrimSpace(target.ClusterNodeName),
			CacheEndpoint:     strings.TrimRight(strings.TrimSpace(cacheEndpoint), "/"),
			Status:            model.ImageLocationStatusPresent,
			LastSeenAt:        &now,
		}); err != nil && s.Logger != nil {
			s.Logger.Printf("record target image location app=%s op=%s image=%s runtime=%s node=%s failed: %v", app.ID, op.ID, ref, target.RuntimeID, target.ClusterNodeName, err)
		}
	}
}

func (s *Service) recordImportedDistributedImage(ctx context.Context, app model.App, op model.Operation, managedImageRef, runtimeImageRef string, destination importImageDestination) error {
	if s == nil || s.Store == nil || !s.imageStoreDistributedMode() {
		return nil
	}
	now := time.Now().UTC()
	if s.now != nil {
		now = s.now().UTC()
	}
	digest, digestRef := s.resolveImportedImageDigest(ctx, managedImageRef, runtimeImageRef)
	image, err := s.Store.UpsertImage(model.Image{
		TenantID:                 strings.TrimSpace(app.TenantID),
		AppID:                    strings.TrimSpace(app.ID),
		ImageRef:                 strings.TrimSpace(managedImageRef),
		CanonicalDigest:          digest,
		SourceOperationID:        strings.TrimSpace(op.ID),
		LifecycleState:           model.ImageLifecycleAvailable,
		RequiredReplicaCount:     s.imageTargetReplicaCount(model.Image{}),
		MinAvailableReplicaCount: s.imageMinReplicaCount(),
	})
	if err != nil {
		return fmt.Errorf("record distributed image: %w", err)
	}
	for _, aliasRef := range compactImageRefs([]string{managedImageRef, runtimeImageRef, digestRef}) {
		if _, err := s.Store.UpsertImageAlias(model.ImageAlias{
			ImageID:  image.ID,
			TenantID: image.TenantID,
			AliasRef: aliasRef,
			Digest:   digest,
		}); err != nil {
			return fmt.Errorf("record distributed image alias %s: %w", aliasRef, err)
		}
	}
	for _, pin := range []model.ImagePin{
		{
			ImageID:     image.ID,
			TenantID:    image.TenantID,
			AppID:       strings.TrimSpace(app.ID),
			OperationID: strings.TrimSpace(op.ID),
			Reason:      model.ImagePinReasonCurrentDeploy,
			MinReplicas: s.imageMinReplicaCount(),
		},
		{
			ImageID:     image.ID,
			TenantID:    image.TenantID,
			AppID:       strings.TrimSpace(app.ID),
			OperationID: strings.TrimSpace(op.ID),
			Reason:      model.ImagePinReasonRollbackWindow,
			MinReplicas: maxInt(1, s.imageMinReplicaCount()),
			ExpiresAt:   timePointer(now.Add(7 * 24 * time.Hour)),
		},
	} {
		if _, err := s.Store.UpsertImagePin(pin); err != nil {
			return fmt.Errorf("record distributed image pin %s: %w", pin.Reason, err)
		}
	}
	if strings.TrimSpace(destination.CacheEndpoint) != "" {
		leaseExpiresAt := now.Add(s.imageReplicaLeaseTTL())
		replica, err := s.Store.UpsertImageReplica(model.ImageReplica{
			ImageID:         image.ID,
			TenantID:        image.TenantID,
			AppID:           image.AppID,
			Digest:          digest,
			RuntimeID:       strings.TrimSpace(destination.Target.RuntimeID),
			ClusterNodeName: strings.TrimSpace(destination.Target.ClusterNodeName),
			CacheEndpoint:   strings.TrimRight(strings.TrimSpace(destination.CacheEndpoint), "/"),
			Status:          model.ImageReplicaStatusPresent,
			LastVerifiedAt:  &now,
			LeaseExpiresAt:  &leaseExpiresAt,
		})
		if err != nil {
			return fmt.Errorf("record distributed first replica: %w", err)
		}
		_, _ = s.Store.UpsertImageLocation(imageLocationFromDistributedReplica(image, replica))
	} else if s.imageStoreStrictDistributedMode() {
		return fmt.Errorf("distributed image import for app %s produced no verified image-cache replica for %s", app.ID, managedImageRef)
	}
	replicas, err := s.Store.ListImageReplicas(model.ImageReplicaFilter{
		ImageID:  image.ID,
		TenantID: image.TenantID,
		Status:   model.ImageReplicaStatusPresent,
	})
	if err != nil {
		return fmt.Errorf("verify distributed image write quorum: %w", err)
	}
	if len(healthyImageReplicas(replicas, now)) == 0 {
		if s.imageStoreStrictDistributedMode() {
			return fmt.Errorf("distributed image import for app %s has no healthy source replica for %s", app.ID, managedImageRef)
		}
		if s.Logger != nil {
			s.Logger.Printf("distributed image import completed without a healthy source replica app=%s op=%s image=%s", app.ID, op.ID, managedImageRef)
		}
	}
	if err := s.ensureImageReplicaPolicy(ctx, image); err != nil && s.Logger != nil {
		s.Logger.Printf("schedule distributed image replicas app=%s op=%s image=%s failed: %v", app.ID, op.ID, image.ID, err)
	}
	return nil
}

func (s *Service) resolveImportedImageDigest(ctx context.Context, refs ...string) (string, string) {
	for _, ref := range compactImageRefs(refs) {
		if digest := imageDigest(ref); digest != "" {
			return digest, imageRefWithDigest(ref, digest)
		}
	}
	if s == nil || s.resolveManagedImageDigestRef == nil || s.imageStoreStrictDistributedMode() {
		return "", ""
	}
	resolveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for _, ref := range compactImageRefs(refs) {
		resolved, err := s.resolveManagedImageDigestRef(resolveCtx, ref)
		if err != nil {
			if s.Logger != nil {
				s.Logger.Printf("resolve imported distributed image digest failed image=%s: %v", ref, err)
			}
			continue
		}
		if digest := imageDigest(resolved); digest != "" {
			return digest, imageRefWithDigest(ref, digest)
		}
	}
	return "", ""
}

func imageRefWithDigest(ref, digest string) string {
	ref = strings.TrimSpace(ref)
	digest = normalizeControllerImageDigest(digest)
	if ref == "" || digest == "" {
		return ""
	}
	if index := strings.LastIndex(ref, "@sha256:"); index >= 0 {
		ref = ref[:index]
	} else if slash := strings.LastIndex(ref, "/"); slash >= 0 {
		if colon := strings.LastIndex(ref, ":"); colon > slash {
			ref = ref[:colon]
		}
	}
	if ref == "" {
		return ""
	}
	return ref + "@" + digest
}

func normalizeControllerImageDigest(raw string) string {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "@sha256:") {
		raw = strings.TrimPrefix(raw, "@")
	}
	if strings.HasPrefix(raw, "sha256:") && len(raw) == len("sha256:")+64 {
		return strings.ToLower(raw)
	}
	return imageDigest(raw)
}

func imageLocationFromDistributedReplica(image model.Image, replica model.ImageReplica) model.ImageLocation {
	return model.ImageLocation{
		TenantID:        replica.TenantID,
		AppID:           replica.AppID,
		ImageRef:        firstNonEmptyControllerString(image.ImageRef, image.CanonicalDigest),
		Digest:          firstNonEmptyControllerString(replica.Digest, image.CanonicalDigest),
		NodeID:          replica.NodeID,
		RuntimeID:       replica.RuntimeID,
		ClusterNodeName: replica.ClusterNodeName,
		CacheEndpoint:   replica.CacheEndpoint,
		Status:          imageLocationStatusFromReplicaStatus(replica.Status),
		LastSeenAt:      replica.LastVerifiedAt,
		SizeBytes:       replica.SizeBytes,
		LastError:       replica.LastError,
	}
}

func imageLocationStatusFromReplicaStatus(status string) string {
	switch strings.TrimSpace(status) {
	case model.ImageReplicaStatusPresent:
		return model.ImageLocationStatusPresent
	case model.ImageReplicaStatusCopying, model.ImageReplicaStatusVerifying, model.ImageReplicaStatusPlanned:
		return model.ImageLocationStatusPulling
	case model.ImageReplicaStatusMissing, model.ImageReplicaStatusStale:
		return model.ImageLocationStatusMissing
	case model.ImageReplicaStatusFailed:
		return model.ImageLocationStatusFailed
	default:
		return model.ImageLocationStatusPresent
	}
}

func firstNonEmptyControllerString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func timePointer(value time.Time) *time.Time {
	return &value
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
