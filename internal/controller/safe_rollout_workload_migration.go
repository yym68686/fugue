package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

// Migration captures existing resources; it never reapplies a historical
// executable spec, replaces a Pod, or turns missing resources into a binding.
func (s *Service) migrateSafeRolloutWorkload(ctx context.Context, app model.App, release model.AppRelease) (model.AppRelease, error) {
	if release.RevisionWorkload != nil || !store.AppReleaseAwaitingDrain(release) || release.SpecSnapshot == nil || !s.Config.KubectlApply {
		return release, nil
	}
	historical := app
	historical.Spec = *cloneControllerAppSpec(release.SpecSnapshot)
	if historical.Spec.RuntimeID != release.RuntimeID || !s.migrationImageRefsEquivalent(historical, historical.Spec.Image, release.ResolvedImageRef) {
		return release, fmt.Errorf("historical executable spec differs from its source operation")
	}
	historical = s.Renderer.PrepareApp(s.appWithResolvedLaunchOverride(ctx, historical))
	base, err := s.managedSchedulingConstraints(historical.Spec.RuntimeID)
	if err != nil {
		return release, err
	}
	revision := safeRolloutCandidateRevision(release.ID)
	options := runtime.RenderOptions{StrictDrain: s.Renderer.StrictDrain, Revision: revision}
	target := release
	target.DeploymentName = runtime.RuntimeAppResourceNameWithOptions(historical, options)
	target.ServiceName = runtime.RuntimeAppServiceNameWithOptions(historical, options)
	client, err := s.kubeClient()
	if err != nil {
		return release, err
	}
	live, found, err := client.getDeployment(ctx, runtime.NamespaceForTenant(app.TenantID), target.DeploymentName)
	if err != nil || !found {
		return release, fmt.Errorf("historical revision Deployment is unavailable")
	}
	// Keep its observed placement rather than choosing a new node for an old
	// workload. It must still satisfy the runtime's required node labels.
	scheduling := runtime.SchedulingConstraints{NodeSelector: live.Spec.Template.Spec.NodeSelector, Tolerations: live.Spec.Template.Spec.Tolerations}
	if !desiredStringMapSubset(scheduling.NodeSelector, base.NodeSelector) {
		return release, fmt.Errorf("historical placement differs from its runtime")
	}
	objects := s.Renderer.BuildManagedAppRevisionChildObjects(historical, scheduling, nil, nil, revision)
	id, err := s.Store.FindAppReleaseWorkloadSource(ctx, release)
	if err != nil {
		return release, err
	}
	op, err := s.Store.GetOperation(id)
	if err != nil {
		return release, err
	}
	if op.Type != model.OperationTypeDeploy || op.AppID != app.ID || op.TenantID != app.TenantID || op.DesiredSpec == nil || op.CompletedAt == nil ||
		release.Status == model.AppReleaseStatusDraining && op.Status != model.OperationStatusCompleted ||
		release.Status == model.AppReleaseStatusFailed && op.Status != model.OperationStatusFailed {
		return release, fmt.Errorf("historical revision has no matching terminal deploy")
	}
	if !s.safeRolloutResumeSpecMatches(historical, *op.DesiredSpec) {
		return release, fmt.Errorf("historical executable spec differs from its source operation")
	}
	identity, err := s.captureSafeRolloutWorkload(ctx, client, op, historical, target, objects)
	if err != nil {
		return release, err
	}
	bound, err := s.Store.MigrateAppReleaseWorkload(ctx, release, op, identity)
	if err != nil {
		return release, err
	}
	evidence, _ := json.Marshal(identity)
	s.appendSafeRolloutAuditEvent(app, "app.release.workload.migrated", bound.ID, map[string]string{
		"operation_id": op.ID, "revision_workload": string(evidence),
	})
	return bound, nil
}

func historicalWorkloadCreatedDuringOperation(metadata map[string]any, op model.Operation) bool {
	if op.Status != model.OperationStatusCompleted && op.Status != model.OperationStatusFailed {
		return true
	}
	created, err := time.Parse(time.RFC3339Nano, objectStringField(metadata, "creationTimestamp"))
	return err == nil && !op.CreatedAt.IsZero() && op.CompletedAt != nil &&
		!created.Before(op.CreatedAt.Truncate(time.Second)) && !created.After(*op.CompletedAt)
}
