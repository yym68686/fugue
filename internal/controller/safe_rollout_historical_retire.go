package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

// An original executable snapshot may have been overwritten by old canonical
// writers. Recover only current, positively stopped resources into a tombstone;
// never create a live binding or reinterpret runtime state as configuration.
func (s *Service) retireUnboundStoppedRevision(ctx context.Context, app model.App, previous, stable model.AppRelease, policy model.AppTrafficPolicy) error {
	if !s.Config.KubectlApply || previous.RevisionWorkload != nil || previous.Role != model.AppReleaseRolePrevious || previous.Status != model.AppReleaseStatusDraining {
		return nil
	}
	if previous.RetentionUntil != nil && time.Now().Before(*previous.RetentionUntil) {
		return nil
	}
	if policy.Mode != model.AppTrafficModeSingle || policy.StableReleaseID != stable.ID || policy.CandidateReleaseID != "" || policy.StableWeight != 100 || policy.CandidateWeight != 0 {
		return errors.New("historical traffic not withdrawn")
	}
	if continuity := app.Spec.Continuity; continuity != nil && continuity.ZeroDowntime != nil {
		since := previous.UpdatedAt
		if policy.UpdatedAt.After(since) {
			since = policy.UpdatedAt
		}
		if time.Now().Before(since.Add(time.Duration(continuity.ZeroDowntime.RetireGraceSeconds) * time.Second)) {
			return nil
		}
	}
	id, err := s.Store.FindAppReleaseWorkloadSource(ctx, previous)
	if err != nil {
		return err
	}
	op, err := s.Store.GetOperation(id)
	if err != nil {
		return err
	}
	if op.Type != model.OperationTypeDeploy || op.Status != model.OperationStatusCompleted || op.AppID != app.ID || op.TenantID != app.TenantID || op.CompletedAt == nil || previous.PromotedAt == nil || previous.PromotedAt.After(*op.CompletedAt) {
		return errors.New("historical retirement source differs")
	}
	client, err := s.kubeClient()
	if err != nil {
		return err
	}
	observed, err := s.observeUnboundHistoricalTarget(ctx, client, app, previous, op)
	if err != nil {
		return err
	}
	if stable.DeploymentName == observed.DeploymentName || stable.ServiceName == observed.ServiceName {
		return errors.New("stable still uses historical resources")
	}
	workload, err := s.captureReleaseDrainWorkloadMode(ctx, client, app, observed, true)
	if err != nil {
		return err
	}
	proof, err := s.edgeBundleObserverForSafeRollout().WaitForSafeRolloutEdgeRouteBundle(ctx, app, stable, 0, policy.UpdatedAt)
	if err != nil || !proof.Ready {
		return errors.New("stable traffic proof unavailable")
	}
	reports, err := s.observeStoppedReleaseRuntime(ctx, client, app, observed, workload)
	if err != nil {
		return err
	}
	proof, err = s.edgeBundleObserverForSafeRollout().WaitForSafeRolloutEdgeRouteBundle(ctx, app, stable, 0, policy.UpdatedAt)
	if err != nil || !proof.Ready {
		return errors.New("stable traffic proof lost")
	}
	for _, report := range reports {
		if time.Since(report.FinishedAt) > 30*time.Second {
			return errors.New("historical runtime proof expired")
		}
	}
	fresh, err := s.captureReleaseDrainWorkloadMode(ctx, client, app, observed, true)
	if err != nil || !reflect.DeepEqual(fresh, workload) {
		return errors.New("historical workload changed during observation")
	}
	retired, err := s.Store.RetireStoppedHistoricalAppRelease(ctx, previous, stable, policy, op, *observed.RevisionWorkload)
	if err != nil {
		return err
	}
	evidence, _ := json.Marshal(map[string]any{"source": "verified_cri_runtime", "runtime_stopped": true, "ready": true, "active_connections": 0, "observer_errors": 0, "revision_workload": retired.RevisionWorkload, "workload": workload, "runtime_receipts": reports, "intent_snapshot_verified": false})
	s.appendSafeRolloutAuditEvent(app, "app.release.historical_stopped_retired", retired.ID, map[string]string{"operation_id": op.ID, "drain_evidence": string(evidence), "reason": "current runtime positively stopped; historical executable snapshot was not reconstructed"})
	return s.cleanupSafeRolloutRetiredResources(ctx, app, retired)
}

func (s *Service) observeUnboundHistoricalTarget(ctx context.Context, client *kubeClient, app model.App, release model.AppRelease, op model.Operation) (model.AppRelease, error) {
	options := runtime.RenderOptions{StrictDrain: s.Renderer.StrictDrain, Revision: safeRolloutCandidateRevision(release.ID)}
	name := runtime.RuntimeAppResourceNameWithOptions(app, options)
	serviceName := runtime.RuntimeAppServiceNameWithOptions(app, options)
	ns := runtime.NamespaceForTenant(app.TenantID)
	dep, found, err := client.getRawObject(ctx, deploymentAPIPath(ns, name))
	if err != nil {
		return release, err
	}
	if !found {
		return release, errors.New("historical Deployment absent")
	}
	svc, found, err := client.getRawObject(ctx, "/api/v1/namespaces/"+url.PathEscape(ns)+"/services/"+url.PathEscape(serviceName))
	if err != nil {
		return release, err
	}
	if !found {
		return release, errors.New("historical Service absent")
	}
	for _, obj := range []map[string]any{dep, svc} {
		m := objectMapField(obj, "metadata")
		if objectStringField(m, "uid") == "" || objectStringField(m, "namespace") != ns || objectStringField(m, "deletionTimestamp") != "" || !appWorkloadOwnerMatches(m, map[string]string{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID}) || objectStringMapValue(m["labels"])[runtime.FugueLabelAppReleaseID] != release.ID || !historicalWorkloadCreatedDuringOperation(m, op) {
			return release, errors.New("historical resource owner or lifetime differs")
		}
	}
	dm := objectMapField(dep, "metadata")
	key := objectStringMapValue(dm["annotations"])[runtime.FugueAnnotationReleaseKey]
	generation, ok := dm["generation"].(float64)
	if !ok || generation < 1 || generation != float64(int64(generation)) || key == "" || key != objectStringMapValue(nestedObjectValue(dep, "spec", "template", "metadata", "annotations"))[runtime.FugueAnnotationReleaseKey] {
		return release, errors.New("historical workload executable identity unavailable")
	}
	containers := mapSlice(nestedObjectValue(dep, "spec", "template", "spec", "containers"))
	if len(containers) == 0 || !s.migrationImageRefsEquivalent(app, objectStringField(containers[0], "image"), release.ResolvedImageRef) {
		return release, errors.New("historical image differs")
	}
	selector := objectStringMapValue(nestedObjectValue(svc, "spec", "selector"))
	template := objectStringMapValue(nestedObjectValue(dep, "spec", "template", "metadata", "labels"))
	if selector[runtime.FugueLabelAppReleaseID] != release.ID || len(selector) == 0 {
		return release, errors.New("historical Service lacks release isolation")
	}
	for k, v := range selector {
		if strings.TrimSpace(v) == "" || template[k] != v {
			return release, fmt.Errorf("historical selector differs at %s", k)
		}
	}
	if value := selector[runtime.FugueLabelAppWorkload]; value != "" && value != name {
		return release, errors.New("historical Service has conflicting workload identity")
	}
	release.DeploymentName, release.ServiceName = name, serviceName
	release.RevisionWorkload = &model.AppReleaseWorkload{OperationID: op.ID, Namespace: ns, DeploymentName: name, DeploymentUID: objectStringField(dm, "uid"), DeploymentGeneration: int64(generation), ServiceName: serviceName, ServiceUID: objectStringField(objectMapField(svc, "metadata"), "uid"), ReleaseKey: key, RuntimeID: release.RuntimeID, ImageRef: release.ResolvedImageRef}
	return release, nil
}
