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

// A resumed promotion can hold an operation open while canonical connections
// drain. Its rollback and handoff releases are still in use by that operation.
// Older, independent revisions can use the normal retirement proofs under the
// same operation, without waiting for the canonical Deployment to change.
func (s *Service) retireHistoricalRevisionsDuringHandoff(ctx context.Context, op model.Operation, state *safeRolloutState) {
	if !s.Config.KubectlApply || !state.Resumed || !state.AlreadyPromoted || !state.ServingVerified ||
		state.Candidate.RevisionWorkload == nil || state.Candidate.RevisionWorkload.OperationID != op.ID ||
		state.StableRelease.ID == "" || state.StableRelease.CreatedAt.IsZero() {
		return
	}
	app := state.CandidateApp
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if held, err := s.Store.AppRecoveryHeld(app.ID); err != nil || held {
		return
	}
	client, err := s.kubeClient()
	if err != nil {
		return
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	deploymentName := runtime.RuntimeAppResourceName(app)
	serviceName := runtime.RuntimeAppServiceName(app)
	canonical, found, err := client.getRawObject(ctx, deploymentAPIPath(namespace, deploymentName))
	if err != nil || !found {
		return
	}
	metadata := objectMapField(canonical, "metadata")
	var handoff safeCanonicalHandoff
	if json.Unmarshal([]byte(objectStringMapValue(metadata["annotations"])[safeCanonicalHandoffAnnotation]), &handoff) != nil ||
		handoff.ReleaseID != state.Candidate.ID || handoff.SourceReleaseID == "" ||
		handoff.ReleaseKey == "" || handoff.Image == "" ||
		handoff.DeploymentUID == "" || handoff.DeploymentUID != objectStringField(metadata, "uid") ||
		!appWorkloadOwnerMatches(metadata, map[string]string{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID}) ||
		objectStringField(metadata, "deletionTimestamp") != "" {
		return
	}
	if err := s.ensureOperationStillActiveContext(ctx, op.ID); err != nil {
		return
	}
	err = s.scanDrainingAppReleaseRetirements(ctx, app, func(previous, stable model.AppRelease) bool {
		return historicalHandoffRetirementEligible(previous, stable, state, handoff, deploymentName, serviceName, time.Now().UTC())
	})
	if err != nil && ctx.Err() == nil && s.Logger != nil {
		s.Logger.Printf("retire historical releases during canonical handoff app=%s: %v", app.ID, err)
	}
}

func historicalHandoffRetirementEligible(previous, stable model.AppRelease, state *safeRolloutState, handoff safeCanonicalHandoff, canonicalDeployment, canonicalService string, now time.Time) bool {
	binding := previous.RevisionWorkload
	return stable.ID == state.Candidate.ID &&
		previous.Role == model.AppReleaseRolePrevious && previous.Status == model.AppReleaseStatusDraining &&
		previous.ID != state.StableRelease.ID && previous.ID != stable.RollbackTargetID &&
		previous.ID != handoff.SourceReleaseID && previous.ID != handoff.PreviousTargetReleaseID &&
		!previous.CreatedAt.IsZero() && previous.CreatedAt.Before(state.StableRelease.CreatedAt) &&
		previous.RetentionUntil != nil && !now.Before(*previous.RetentionUntil) &&
		binding != nil && binding.DeploymentName != "" && binding.DeploymentName != canonicalDeployment &&
		binding.ServiceName != "" && binding.ServiceName != canonicalService
}

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
