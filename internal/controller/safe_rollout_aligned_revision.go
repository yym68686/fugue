package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"time"

	"fugue/internal/model"
	"fugue/internal/releaseretirement"
	retirement "fugue/internal/releaseretirement/contract"
	"fugue/internal/runtime"
)

const alignedRevisionQuiescedAnnotation = "fugue.pro/canonical-revision-quiesced"

// A release and its original candidate are different lifecycles. After the
// canonical target takes over, reclaim the redundant running replicas without
// retiring the serving release or erasing its immutable provenance. Keeping the
// zero-replica Deployment/Service also makes recovery after a controller restart
// idempotent and leaves normal superseded-release retirement in charge of GC.
func (s *Service) quiesceAlignedReleaseRevision(ctx context.Context, app model.App, release model.AppRelease, policy model.AppTrafficPolicy) error {
	if !s.Config.KubectlApply || !retirement.HasAlignedRevision(release) {
		return nil
	}
	b := release.RevisionWorkload
	if b.Namespace != runtime.NamespaceForTenant(app.TenantID) || b.DeploymentUID == "" || b.ServiceUID == "" || b.ReleaseKey == "" ||
		b.RuntimeID != app.Spec.RuntimeID || release.RuntimeID != app.Spec.RuntimeID || !s.migrationImageRefsEquivalent(app, release.ResolvedImageRef, app.Spec.Image) {
		return fmt.Errorf("aligned revision execution identity differs")
	}
	client, err := s.kubeClient()
	if err != nil {
		return err
	}
	path := deploymentAPIPath(b.Namespace, b.DeploymentName)
	object, found, err := client.getRawObject(ctx, path)
	if err != nil || !found {
		return err
	}
	m := objectMapField(object, "metadata")
	if objectStringField(m, "uid") != b.DeploymentUID || objectStringField(m, "deletionTimestamp") != "" ||
		!appWorkloadOwnerMatches(m, map[string]string{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID}) ||
		objectStringMapValue(m["labels"])[runtime.FugueLabelAppReleaseID] != release.ID ||
		objectStringMapValue(nestedObjectValue(object, "spec", "template", "metadata", "annotations"))[runtime.FugueAnnotationReleaseKey] != b.ReleaseKey {
		return fmt.Errorf("aligned revision Deployment identity changed")
	}
	if replicas, ok := nestedObjectValue(object, "spec", "replicas").(float64); ok && replicas == 0 {
		return nil
	}
	since := release.UpdatedAt
	if policy.UpdatedAt.After(since) {
		since = policy.UpdatedAt
	}
	grace := 0
	if c := app.Spec.Continuity; c != nil && c.ZeroDowntime != nil {
		grace = max(c.ZeroDowntime.RetireGraceSeconds, c.ZeroDowntime.RollbackWindowSeconds)
	}
	if time.Now().Before(since.Add(time.Duration(grace)*time.Second)) || release.RetentionUntil != nil && time.Now().Before(*release.RetentionUntil) {
		return fmt.Errorf("aligned revision retention has not expired")
	}
	canonical, err := s.verifyAlignedReleaseCanonical(ctx, client, app, release, policy)
	if err != nil {
		return err
	}
	metrics, err := (kubeSafeRolloutDrainObserver{service: s}).queryReleaseDrainMetrics(ctx, app, release, since, true)
	if err != nil {
		return err
	}
	if !metrics.Ready || metrics.Workload == nil || metrics.ActiveConnections != 0 || metrics.ObserverErrors != 0 || metrics.Workload.RuntimeStopped {
		return fmt.Errorf("aligned revision drain is not confirmed")
	}
	proof, err := s.observeSafeRolloutRetirementTraffic(ctx, app, release, since)
	if err != nil || !proof.Ready {
		return fmt.Errorf("canonical publication changed after revision drain: %v", err)
	}
	freshCanonical, err := s.verifyAlignedReleaseCanonical(ctx, client, app, release, policy)
	if err != nil || !reflect.DeepEqual(canonical, freshCanonical) {
		return fmt.Errorf("canonical target changed during revision drain: %v", err)
	}
	target := release
	target.DeploymentName, target.ServiceName = b.DeploymentName, b.ServiceName
	fresh, err := s.recaptureReleaseDrainWorkload(ctx, client, app, target, *metrics.Workload)
	if err != nil || !reflect.DeepEqual(*metrics.Workload, fresh) || time.Since(metrics.ObservedAt) > 15*time.Second || ctx.Err() != nil {
		return fmt.Errorf("aligned revision changed after drain: %v", err)
	}
	if last, err := s.verifyAlignedReleaseCanonical(ctx, client, app, release, policy); err != nil || !reflect.DeepEqual(canonical, last) {
		return fmt.Errorf("canonical target changed before quiescing: %v", err)
	}
	_, err = client.doRequest(ctx, http.MethodPatch, path, "application/merge-patch+json", map[string]any{
		"metadata": map[string]any{"uid": fresh.DeploymentUID, "resourceVersion": fresh.DeploymentVersion,
			"annotations": map[string]any{alignedRevisionQuiescedAnnotation: release.ID}},
		"spec": map[string]any{"replicas": 0},
	}, nil)
	if err != nil {
		return fmt.Errorf("quiesce aligned revision: %w", err)
	}
	releaseretirement.NoteProgress(ctx)
	s.appendSafeRolloutAuditEvent(app, "app.release.aligned_revision.quiesced", release.ID, map[string]string{
		"operation_id": b.OperationID, "deployment_name": b.DeploymentName, "deployment_uid": b.DeploymentUID,
		"canonical_deployment": release.DeploymentName, "reason": "canonical publication and original Pod drain verified; serving release retained",
	})
	return nil
}

// Capture the complete serving execution identity and endpoint set, so drift
// during the drain window invalidates permission to quiesce the old revision.
func (s *Service) verifyAlignedReleaseCanonical(ctx context.Context, client *kubeClient, app model.App, release model.AppRelease, policy model.AppTrafficPolicy) (map[string]any, error) {
	if app.Spec.Replicas <= 0 || release.DeploymentName != runtime.RuntimeAppResourceName(app) || release.ServiceName != runtime.RuntimeAppServiceName(app) ||
		policy.Mode != model.AppTrafficModeSingle || policy.StableReleaseID != release.ID || policy.CandidateReleaseID != "" || policy.StableWeight != 100 || policy.CandidateWeight != 0 {
		return nil, fmt.Errorf("aligned revision has no exclusive canonical target")
	}
	current, err := s.Store.GetAppRelease(app.TenantID, true, release.ID)
	if err != nil || !sameReleaseDrainTarget(release, current) {
		return nil, fmt.Errorf("serving release changed")
	}
	freshPolicy, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	if err != nil || !reflect.DeepEqual(policy, freshPolicy) {
		return nil, fmt.Errorf("serving traffic policy changed")
	}
	freshApp, err := s.Store.GetApp(app.ID)
	if err != nil || freshApp.TenantID != app.TenantID || !reflect.DeepEqual(app.Spec, freshApp.Spec) {
		return nil, fmt.Errorf("application intent changed")
	}
	if active, err := s.appHasActiveOperation(app); err != nil || active {
		return nil, fmt.Errorf("application operation is active or unknown")
	}
	object, found, err := client.getRawObject(ctx, deploymentAPIPath(release.RevisionWorkload.Namespace, release.DeploymentName))
	if err != nil || !found {
		return nil, fmt.Errorf("canonical Deployment unavailable")
	}
	m := objectMapField(object, "metadata")
	var dep kubeDeployment
	raw, _ := json.Marshal(object)
	if json.Unmarshal(raw, &dep) != nil || dep.Spec.Replicas == nil || *dep.Spec.Replicas != app.Spec.Replicas || !managedDeploymentStatusReady(dep, app.Spec.Replicas) || objectStringField(m, "uid") == "" || objectStringField(m, "deletionTimestamp") != "" ||
		!appWorkloadOwnerMatches(m, map[string]string{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID}) ||
		objectStringMapValue(m["annotations"])[safeCanonicalHandoffAnnotation] != "" || !s.migrationImageRefsEquivalent(app, deploymentPrimaryContainerImage(dep), app.Spec.Image) {
		return nil, fmt.Errorf("canonical Deployment is not ready or its identity changed")
	}
	canonical := model.AppRelease{AppID: app.ID, TenantID: app.TenantID, DeploymentName: release.DeploymentName, ResolvedImageRef: app.Spec.Image,
		RevisionWorkload: &model.AppReleaseWorkload{DeploymentName: release.DeploymentName, DeploymentUID: dep.Metadata.UID, DeploymentGeneration: dep.Metadata.Generation,
			ReleaseKey: objectStringMapValue(nestedObjectValue(object, "spec", "template", "metadata", "annotations"))[runtime.FugueAnnotationReleaseKey]}}
	workload, err := s.captureDrainWorkload(ctx, client, app, canonical, false, true)
	if err != nil {
		return nil, fmt.Errorf("canonical Pod identity is not confirmed: %w", err)
	}
	if referenced, err := s.canonicalServicesReferenceRelease(ctx, client, app, release); err != nil || referenced {
		return nil, fmt.Errorf("canonical Service still references the original revision")
	}
	service, found, err := client.getRawObject(ctx, "/api/v1/namespaces/"+url.PathEscape(release.RevisionWorkload.Namespace)+"/services/"+url.PathEscape(release.ServiceName))
	if err != nil || !found || objectStringMapValue(nestedObjectValue(service, "spec", "selector"))[runtime.FugueLabelAppWorkload] != release.DeploymentName {
		return nil, fmt.Errorf("canonical Service target unavailable")
	}
	sm := objectMapField(service, "metadata")
	if objectStringField(sm, "uid") == "" || objectStringField(sm, "deletionTimestamp") != "" ||
		!appWorkloadOwnerMatches(sm, map[string]string{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID}) {
		return nil, fmt.Errorf("canonical Service ownership or lifecycle differs")
	}
	if err := safeCanonicalEndpoints(ctx, client, release.RevisionWorkload.Namespace, release.ServiceName, workload.Pods); err != nil {
		return nil, fmt.Errorf("canonical endpoints do not exclusively match the serving Pods: %w", err)
	}
	return map[string]any{"uid": objectStringField(m, "uid"), "template": nestedObjectValue(object, "spec", "template"), "service": service, "workload": workload}, nil
}
