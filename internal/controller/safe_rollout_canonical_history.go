package controller

import (
	"context"
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

// A promoted independent revision can advance both desired-state projections
// while the original canonical workload keeps serving cluster consumers. Its
// controller-recorded ready key, live template and an exact historical snapshot
// can recover the baseline without inventing a handoff or skipping drain gates.
func (s *Service) recoverRecordedCanonicalSnapshot(ctx context.Context, managed runtime.ManagedAppObject, live kubeDeployment, app model.App, scheduling runtime.SchedulingConstraints) (model.App, bool, error) {
	key := strings.TrimSpace(managed.Status.CurrentReleaseKey)
	if s.Store == nil || !model.AppSafeZeroDowntimeRolloutEnabled(app.Spec) || key == "" ||
		key != live.Metadata.Annotations[runtime.FugueAnnotationReleaseKey] || key != live.Spec.Template.Metadata.Annotations[runtime.FugueAnnotationReleaseKey] ||
		parseManagedAppStatusTimestamp(managed.Status.CurrentReleaseStartedAt) == nil || parseManagedAppStatusTimestamp(managed.Status.CurrentReleaseReadyAt) == nil ||
		managed.Status.ObservedGeneration < managed.Metadata.Generation || live.Metadata.UID == "" ||
		live.Spec.Template.Metadata.Labels[runtime.FugueLabelAppID] != app.ID || live.Spec.Template.Metadata.Labels[runtime.FugueLabelTenantID] != app.TenantID ||
		live.Metadata.Name != runtime.RuntimeAppResourceName(app) || live.Spec.Replicas == nil || !managedDeploymentStatusReady(live, *live.Spec.Replicas) {
		return app, false, nil
	}
	history, err := s.Store.ListAppReleaseMetadataContext(ctx, model.AppReleaseFilter{TenantID: app.TenantID, AppID: app.ID, Role: model.AppReleaseRolePrevious, ActiveOnly: true, PlatformAdmin: true})
	if err != nil {
		return app, false, fmt.Errorf("read recorded canonical history: %w", err)
	}
	reads := 0
	for _, meta := range history {
		if meta.ResolvedImageRef == "" || !s.migrationImageRefsEquivalent(app, meta.ResolvedImageRef, deploymentPrimaryContainerImage(live)) {
			continue
		}
		reads++
		if reads > 32 {
			break
		}
		release, err := s.Store.GetAppRelease(app.TenantID, true, meta.ID)
		if err != nil {
			return app, false, err
		}
		if release.AppID != app.ID || release.TenantID != app.TenantID || release.Role != model.AppReleaseRolePrevious ||
			(release.Status != model.AppReleaseStatusServing && release.Status != model.AppReleaseStatusDraining) || release.SpecSnapshot == nil {
			continue
		}
		source := app
		source.Spec = *cloneControllerAppSpec(release.SpecSnapshot)
		if normalized, changed := s.normalizeManagedAppRuntimeImageRefs(source); changed {
			source = normalized
		}
		source = s.appWithResolvedLaunchOverride(ctx, source)
		if source.Spec.RuntimeID != app.Spec.RuntimeID || !s.migrationImageRefsEquivalent(source, source.Spec.Image, deploymentPrimaryContainerImage(live)) ||
			s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(source), scheduling) != key {
			continue
		}
		return source, true, nil
	}
	return app, false, nil
}
