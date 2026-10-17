package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

// A canonical replica is a later convergence step. Once the promoted immutable
// revision serves every published endpoint, lack of capacity for that redundant
// replica must not invalidate the already applied code release. This read-only
// proof grants no permission to replace or retire any workload.
func (s *Service) verifyPromotedServingDuringCanonicalWait(ctx context.Context, app model.App, state *safeRolloutState, scheduling runtime.SchedulingConstraints) error {
	if state == nil || !state.Enabled || !state.StableAlignmentAllowed || !s.Config.KubectlApply {
		return fmt.Errorf("no promoted safe rollout")
	}
	policy, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	if err != nil || policy.Mode != model.AppTrafficModeSingle || policy.StableReleaseID != state.Candidate.ID ||
		policy.StableWeight != 100 || policy.CandidateWeight != 0 || policy.CandidateReleaseID != "" {
		return fmt.Errorf("promoted traffic policy differs")
	}
	release, err := s.Store.GetAppRelease(app.TenantID, true, policy.StableReleaseID)
	if err != nil || release.Role != model.AppReleaseRoleStable || release.Status != model.AppReleaseStatusServing ||
		release.RevisionWorkload == nil || !s.safeRolloutBoundReleaseMatchesApp(app, release) {
		return fmt.Errorf("promoted executable identity unavailable")
	}
	client, err := s.kubeClient()
	if err != nil {
		return err
	}
	namespace, canonicalName := runtime.NamespaceForTenant(app.TenantID), runtime.RuntimeAppResourceName(app)
	canonical, found, err := client.getRawObject(ctx, deploymentAPIPath(namespace, canonicalName))
	if err != nil || !found {
		return fmt.Errorf("canonical handoff unavailable")
	}
	metadata := objectMapField(canonical, "metadata")
	var marker safeCanonicalHandoff
	if json.Unmarshal([]byte(objectStringMapValue(metadata["annotations"])[safeCanonicalHandoffAnnotation]), &marker) != nil ||
		marker.ReleaseID != release.ID || marker.DeploymentUID == "" || marker.DeploymentUID != objectStringField(metadata, "uid") ||
		!appWorkloadOwnerMatches(metadata, map[string]string{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID}) || objectStringField(metadata, "deletionTimestamp") != "" {
		return fmt.Errorf("canonical handoff identity differs")
	}
	expectedKey := s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(app), scheduling)
	if expectedKey == "" || objectStringMapValue(nestedObjectValue(canonical, "spec", "template", "metadata", "annotations"))[runtime.FugueAnnotationReleaseKey] != expectedKey {
		return fmt.Errorf("canonical pending template differs")
	}
	candidate := release
	candidate.DeploymentName, candidate.ServiceName = release.RevisionWorkload.DeploymentName, release.RevisionWorkload.ServiceName
	if candidate.DeploymentName == canonicalName {
		return fmt.Errorf("no independent serving revision")
	}
	before, err := s.captureReleaseDrainWorkload(ctx, client, app, candidate)
	if err != nil {
		return err
	}
	if gate := s.evaluateSafeRolloutCandidateRetireGate(ctx, app, release, s.safeRolloutPlanForApp(app).Gate); gate.Status == model.AppReleaseGateStatusFail {
		return fmt.Errorf("promoted release health gate failed")
	}
	proof, err := s.edgeBundleObserverForSafeRollout().WaitForSafeRolloutEdgeRouteBundle(ctx, app, release, 0, time.Now().UTC())
	if err != nil || !proof.Ready {
		return fmt.Errorf("promoted publication is not confirmed")
	}
	services := 0
	for _, object := range s.Renderer.BuildManagedAppChildObjects(app, scheduling, nil) {
		if object["kind"] != "Service" || objectStringMapValue(nestedObjectValue(object, "spec", "selector"))[runtime.FugueLabelAppWorkload] != canonicalName {
			continue
		}
		name := objectStringField(objectMapField(object, "metadata"), "name")
		live, exists, err := client.getRawObject(ctx, "/api/v1/namespaces/"+url.PathEscape(namespace)+"/services/"+url.PathEscape(name))
		if err != nil || !exists {
			return fmt.Errorf("canonical Service unavailable")
		}
		m := objectMapField(live, "metadata")
		selector := objectStringMapValue(nestedObjectValue(live, "spec", "selector"))
		if !appWorkloadOwnerMatches(m, map[string]string{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID}) ||
			objectStringField(m, "uid") == "" || objectStringField(m, "deletionTimestamp") != "" ||
			selector[runtime.FugueLabelAppReleaseID] != release.ID || selector[runtime.FugueLabelAppWorkload] != candidate.DeploymentName {
			return fmt.Errorf("canonical Service is not bound to promoted revision")
		}
		if err := safeCanonicalEndpoints(ctx, client, namespace, name, before.Pods); err != nil {
			return err
		}
		services++
	}
	if services == 0 {
		return fmt.Errorf("no canonical Service evidence")
	}
	after, err := s.captureReleaseDrainWorkload(ctx, client, app, candidate)
	if err != nil || !reflect.DeepEqual(before, after) {
		return fmt.Errorf("promoted workload changed during observation")
	}
	currentPolicy, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	if err != nil || !reflect.DeepEqual(policy, currentPolicy) {
		return fmt.Errorf("promoted policy changed during observation")
	}
	currentRelease, err := s.Store.GetAppRelease(app.TenantID, true, release.ID)
	if err != nil || !sameReleaseDrainTarget(release, currentRelease) {
		return fmt.Errorf("promoted release changed during observation")
	}
	return ctx.Err()
}
