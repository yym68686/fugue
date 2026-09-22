package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
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
	historical = s.Renderer.PrepareApp(historical)
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
	if err != nil {
		return release, err
	}
	if !found {
		return release, store.ErrNotFound
	}
	historical = s.Renderer.PrepareApp(s.appWithResolvedLaunchOverride(ctx, historical))
	// Keep its observed placement rather than choosing a new node for an old
	// workload. It must still satisfy the runtime's required node labels.
	scheduling := runtime.SchedulingConstraints{NodeSelector: live.Spec.Template.Spec.NodeSelector, Tolerations: live.Spec.Template.Spec.Tolerations}
	if !desiredStringMapSubset(scheduling.NodeSelector, base.NodeSelector) {
		return release, fmt.Errorf("historical placement differs from its runtime")
	}
	objects := s.Renderer.BuildManagedAppRevisionChildObjects(historical, scheduling, nil, nil, revision)
	if err := retainHistoricalDrainImage(objects, live); err != nil {
		return release, err
	}
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
	if err := s.prepareHistoricalRevisionLabels(ctx, client, op, historical, target, objects); err != nil {
		return release, err
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

func (s *Service) prepareHistoricalRevisionLabels(ctx context.Context, client *kubeClient, op model.Operation, app model.App, release model.AppRelease, objects []map[string]any) error {
	ns := runtime.NamespaceForTenant(app.TenantID)
	dep, found, err := client.getRawObject(ctx, deploymentAPIPath(ns, release.DeploymentName))
	if err != nil {
		return err
	}
	if !found {
		return store.ErrNotFound
	}
	path := "/api/v1/namespaces/" + url.PathEscape(ns) + "/services/" + url.PathEscape(release.ServiceName)
	svc, found, err := client.getRawObject(ctx, path)
	if err != nil {
		return err
	}
	if !found {
		return store.ErrNotFound
	}
	label := objectStringMapValue(nestedObjectValue(dep, "spec", "template", "metadata", "labels"))[runtime.FugueLabelAppWorkload]
	selector := objectStringMapValue(nestedObjectValue(svc, "spec", "selector"))
	marker := objectStringMapValue(objectMapField(dep, "metadata")["annotations"])[appWorkloadMigrationAnnotation]
	if label == release.DeploymentName && selector[runtime.FugueLabelAppWorkload] == release.DeploymentName && marker == "" {
		return nil
	}
	if label != "" && label != release.DeploymentName || selector[runtime.FugueLabelAppWorkload] != "" && selector[runtime.FugueLabelAppWorkload] != release.DeploymentName {
		return fmt.Errorf("historical resource declares a different workload")
	}
	// Validate all executable and ownership fields before the derived-label
	// migration. Only the absent workload label is supplied on verification copies.
	var expectedDep, expectedSvc map[string]any
	for _, o := range objects {
		if o["kind"] == "Deployment" && objectStringField(objectMapField(o, "metadata"), "name") == release.DeploymentName {
			expectedDep = o
		}
		if o["kind"] == "Service" && objectStringField(objectMapField(o, "metadata"), "name") == release.ServiceName {
			expectedSvc = o
		}
	}
	if expectedDep == nil || expectedSvc == nil {
		return fmt.Errorf("historical expected resource pair unavailable")
	}
	depCopy, svcCopy := normalizeKubeMap(dep), normalizeKubeMap(svc)
	labels := objectStringMapValue(nestedObjectValue(depCopy, "spec", "template", "metadata", "labels"))
	labels[runtime.FugueLabelAppWorkload] = release.DeploymentName
	objectMapValue(nestedObjectValue(depCopy, "spec", "template", "metadata"))["labels"] = labels
	wantSelector := objectStringMapValue(nestedObjectValue(expectedSvc, "spec", "selector"))
	copySelector := objectStringMapValue(nestedObjectValue(svcCopy, "spec", "selector"))
	copySelector[runtime.FugueLabelAppWorkload] = release.DeploymentName
	objectMapField(svcCopy, "spec")["selector"] = copySelector
	identity, err := s.safeRolloutWorkloadIdentity(op, app, release, depCopy, svcCopy, expectedDep, expectedSvc)
	if err != nil {
		return err
	}
	if err := client.prepareAppServiceWorkload(ctx, ns, release.DeploymentName, wantSelector, dep); err != nil {
		return err
	}
	current, found, err := client.getRawObject(ctx, path)
	if err != nil {
		return err
	}
	if !found {
		return store.ErrNotFound
	}
	if objectStringField(objectMapField(current, "metadata"), "uid") != identity.ServiceUID || !normalizedKubeValueEqual(svc["spec"], current["spec"]) {
		return fmt.Errorf("historical Service changed during label migration")
	}
	if selector[runtime.FugueLabelAppWorkload] == "" {
		if err := client.patchAppWorkloadObject(ctx, path, current, map[string]any{"spec": map[string]any{"selector": wantSelector}}); err != nil {
			return err
		}
	}
	return nil
}

// A helper code rollout is independent of an existing application's executable
// identity. Freeze only the image actually observed on the historical revision;
// the normal two-read template validation still checks every other field.
func retainHistoricalDrainImage(objects []map[string]any, live kubeDeployment) error {
	images := map[string]string{}
	for _, pair := range []struct {
		field      string
		containers []kubeContainerSpec
	}{{"containers", live.Spec.Template.Spec.Containers}, {"initContainers", live.Spec.Template.Spec.InitContainers}} {
		for _, c := range pair.containers {
			if c.Name == "fugue-drain-agent" {
				if c.Image == "" || len(images) > 0 {
					return fmt.Errorf("historical drain helper identity is ambiguous")
				}
				images[pair.field] = c.Image
			}
		}
	}
	for _, obj := range objects {
		if obj["kind"] != "Deployment" || objectStringField(objectMapField(obj, "metadata"), "name") != live.Metadata.Name {
			continue
		}
		count := 0
		for _, field := range []string{"containers", "initContainers"} {
			for _, c := range mapSlice(nestedObjectValue(obj, "spec", "template", "spec", field)) {
				if c["name"] == "fugue-drain-agent" {
					image := images[field]
					if image == "" {
						return fmt.Errorf("historical drain helper layout differs")
					}
					c["image"] = image
					count++
				}
			}
		}
		if count != len(images) {
			return fmt.Errorf("historical drain helper membership differs")
		}
		return nil
	}
	return fmt.Errorf("historical revision template unavailable")
}

func historicalWorkloadCreatedDuringOperation(metadata map[string]any, op model.Operation) bool {
	if op.Status != model.OperationStatusCompleted && op.Status != model.OperationStatusFailed {
		return true
	}
	created, err := time.Parse(time.RFC3339Nano, objectStringField(metadata, "creationTimestamp"))
	return err == nil && !op.CreatedAt.IsZero() && op.CompletedAt != nil &&
		!created.Before(op.CreatedAt.Truncate(time.Second)) && !created.After(*op.CompletedAt)
}
