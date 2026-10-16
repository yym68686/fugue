package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

var errSafeCanonicalHandoffPending = errors.New("canonical handoff awaits positive drain or readiness proof")

const safeCanonicalHandoffAnnotation = "fugue.io/safe-canonical-handoff"

// Constraint policy is independent of executable convergence. Only an exact
// policy-only update may complete while the existing promoted executable and
// original drain cohort are retained. Mixed deployment changes still fail closed.
func safeRolloutPolicyUpdateCanComplete(op model.Operation, current model.App, applyErr error) bool {
	if !errors.Is(applyErr, errSafeCanonicalHandoffPending) || op.Type != model.OperationTypeDeploy ||
		op.DesiredSpec == nil || op.DesiredSource != nil ||
		!model.AppSafeZeroDowntimeRolloutEnabled(current.Spec) || !model.AppSafeZeroDowntimeRolloutEnabled(*op.DesiredSpec) {
		return false
	}
	previous, desired := cloneControllerAppSpec(&current.Spec), cloneControllerAppSpec(op.DesiredSpec)
	if reflect.DeepEqual(previous.Continuity, desired.Continuity) {
		return false
	}
	desired.Continuity = model.CloneAppContinuityPolicy(previous.Continuity)
	return reflect.DeepEqual(previous, desired)
}

type safeCanonicalHandoff struct {
	ReleaseID               string `json:"release_id"`
	SourceReleaseID         string `json:"source_release_id,omitempty"`
	PreviousTargetReleaseID string `json:"previous_target_release_id,omitempty"`
	DeploymentUID           string `json:"deployment_uid"`
	ReleaseKey              string `json:"release_key"`
	Image                   string `json:"image"`
}

// A long original connection may outlive more than one independent revision.
// Only the immediate, durable promotion predecessor can advance a handoff;
// the original source identity never advances with its traffic destination.
func (s *Service) canonicalHandoffSourceReleaseID(app model.App, handoff safeCanonicalHandoff, promoted model.AppRelease) (string, error) {
	recorded, err := s.Store.GetAppRelease(app.TenantID, true, handoff.ReleaseID)
	if err != nil {
		return "", err
	}
	if recorded.AppID != app.ID || recorded.TenantID != app.TenantID || recorded.RevisionWorkload == nil {
		return "", fmt.Errorf("canonical handoff recorded target has no verified binding")
	}
	if promoted.ID != recorded.ID && (promoted.RollbackTargetID != recorded.ID ||
		recorded.Role != model.AppReleaseRolePrevious || recorded.Status != model.AppReleaseStatusDraining) {
		return "", fmt.Errorf("canonical handoff target is not the verified promotion successor")
	}
	sourceID := handoff.SourceReleaseID
	if sourceID == "" {
		sourceID = recorded.RollbackTargetID
	}
	if sourceID == "" || sourceID == promoted.ID || (sourceID == recorded.ID && handoff.SourceReleaseID == "") {
		return "", fmt.Errorf("canonical handoff has no source release identity")
	}
	return sourceID, nil
}

func (s *Service) verifiedCanonicalHandoffSource(app model.App, handoff safeCanonicalHandoff, deployment kubeDeployment) (safeCanonicalHandoff, error) {
	liveKey := deployment.Metadata.Annotations[runtime.FugueAnnotationReleaseKey]
	image := deploymentPrimaryContainerImage(deployment)
	if handoff.DeploymentUID != deployment.Metadata.UID || liveKey == "" ||
		deployment.Spec.Template.Metadata.Annotations[runtime.FugueAnnotationReleaseKey] != liveKey {
		return handoff, fmt.Errorf("canonical handoff source identity differs")
	}
	if liveKey == handoff.ReleaseKey {
		if !s.migrationImageRefsEquivalent(app, handoff.Image, image) {
			return handoff, fmt.Errorf("canonical handoff source image differs")
		}
		return handoff, nil
	}
	// The prior target may have replaced the original after positive drain,
	// while its Service restoration was interrupted. Only its exact executable
	// snapshot can become the next drain cohort, never an arbitrary live image.
	previous, err := s.Store.GetAppRelease(app.TenantID, true, handoff.ReleaseID)
	if err != nil || previous.AppID != app.ID || previous.TenantID != app.TenantID || previous.SpecSnapshot == nil || previous.RevisionWorkload == nil {
		return handoff, fmt.Errorf("canonical handoff replacement source unavailable")
	}
	source := app
	source.Spec = *cloneControllerAppSpec(previous.SpecSnapshot)
	if normalized, changed := s.normalizeManagedAppRuntimeImageRefs(source); changed {
		source = normalized
	}
	scheduling := runtime.SchedulingConstraints{NodeSelector: deployment.Spec.Template.Spec.NodeSelector, Tolerations: deployment.Spec.Template.Spec.Tolerations}
	if s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(source), scheduling) != liveKey || !s.migrationImageRefsEquivalent(source, source.Spec.Image, image) {
		return handoff, fmt.Errorf("canonical handoff replacement is not the prior promoted executable")
	}
	handoff.SourceReleaseID, handoff.ReleaseKey, handoff.Image = previous.ID, liveKey, image
	return handoff, nil
}

// Promotion commits the desired snapshot before the old canonical workload is
// drained. Recover its exact source snapshot without treating the marker itself
// as permission to replace Pods; the ordinary rollout and drain guards still run.
func (s *Service) recoverSafeCanonicalHandoffSource(deployment kubeDeployment, current, desired model.App) (model.App, runtime.SchedulingConstraints, bool, error) {
	var scheduling runtime.SchedulingConstraints
	marker := deployment.Metadata.Annotations[safeCanonicalHandoffAnnotation]
	if marker == "" || s.Store == nil || !model.AppSafeZeroDowntimeRolloutEnabled(desired.Spec) {
		return current, scheduling, false, nil
	}
	var handoff safeCanonicalHandoff
	liveKey := deployment.Metadata.Annotations[runtime.FugueAnnotationReleaseKey]
	if json.Unmarshal([]byte(marker), &handoff) != nil || handoff.ReleaseKey == "" {
		return current, scheduling, false, fmt.Errorf("canonical handoff source identity differs")
	}
	handoff, err := s.verifiedCanonicalHandoffSource(current, handoff, deployment)
	if err != nil {
		return current, scheduling, false, err
	}
	policy, err := s.Store.GetAppTrafficPolicy(current.TenantID, true, current.ID)
	if err != nil {
		return current, scheduling, false, err
	}
	if policy.Mode != model.AppTrafficModeSingle || policy.StableWeight != 100 || policy.CandidateWeight != 0 ||
		policy.CandidateReleaseID != "" || policy.StableReleaseID == "" {
		return current, scheduling, false, fmt.Errorf("canonical handoff promoted policy differs")
	}
	promoted, err := s.Store.GetAppRelease(current.TenantID, true, policy.StableReleaseID)
	if err != nil {
		return current, scheduling, false, err
	}
	if promoted.RevisionWorkload == nil || promoted.DeploymentName == deployment.Metadata.Name ||
		promoted.Role != model.AppReleaseRoleStable || promoted.Status != model.AppReleaseStatusServing ||
		(!s.safeRolloutBoundReleaseMatchesApp(desired, promoted) &&
			!(promoted.ID == handoff.ReleaseID && s.safeRolloutBoundReleaseMatchesApp(current, promoted))) {
		return current, scheduling, false, fmt.Errorf("canonical handoff promoted executable differs")
	}
	scheduling = runtime.SchedulingConstraints{
		NodeSelector: clonePlacementStringMap(deployment.Spec.Template.Spec.NodeSelector),
		Tolerations:  append([]runtime.Toleration(nil), deployment.Spec.Template.Spec.Tolerations...),
	}
	sourceID, err := s.canonicalHandoffSourceReleaseID(current, handoff, promoted)
	if err != nil {
		return current, scheduling, false, err
	}
	release, err := s.Store.GetAppRelease(current.TenantID, true, sourceID)
	if err != nil {
		return current, scheduling, false, err
	}
	if release.SpecSnapshot != nil && release.AppID == current.ID && release.TenantID == current.TenantID {
		source := current
		source.Spec = *cloneControllerAppSpec(release.SpecSnapshot)
		if normalized, changed := s.normalizeManagedAppRuntimeImageRefs(source); changed {
			source = normalized
		}
		key := strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(source), scheduling))
		if key == liveKey && s.migrationImageRefsEquivalent(source, source.Spec.Image, handoff.Image) {
			return source, scheduling, true, nil
		}
	}
	return current, scheduling, false, fmt.Errorf("canonical handoff has no verified source snapshot")
}

// A previous selector CAS may be interrupted by another safe promotion. Only
// selectors from the exact bounded rollback chain can finish that handoff.
func (s *Service) canonicalHandoffPreviousTargetSelectors(ctx context.Context, client *kubeClient, app model.App, handoff safeCanonicalHandoff, promoted model.AppRelease) ([]map[string]string, error) {
	if handoff.PreviousTargetReleaseID == "" {
		return nil, nil
	}
	var selectors []map[string]string
	seen := map[string]bool{}
	id := promoted.RollbackTargetID
	for depth := 0; depth < 32 && id != "" && !seen[id]; depth++ {
		seen[id] = true
		previous, err := s.Store.GetAppRelease(app.TenantID, true, id)
		if err != nil {
			return nil, err
		}
		binding := previous.RevisionWorkload
		withdrawn := previous.Role == model.AppReleaseRolePrevious && previous.Status == model.AppReleaseStatusDraining || previous.Role == model.AppReleaseRoleRetired && previous.Status == model.AppReleaseStatusRetired
		if previous.AppID != app.ID || previous.TenantID != app.TenantID || binding == nil || !withdrawn {
			return nil, fmt.Errorf("canonical handoff previous target has no verified binding")
		}
		service, found, err := client.getRawObject(ctx, "/api/v1/namespaces/"+url.PathEscape(runtime.NamespaceForTenant(app.TenantID))+"/services/"+url.PathEscape(binding.ServiceName))
		if err != nil {
			return nil, err
		}
		if found || previous.Status != model.AppReleaseStatusRetired {
			metadata := objectMapField(service, "metadata")
			selector := objectStringMapValue(nestedObjectValue(service, "spec", "selector"))
			if !found || objectStringField(metadata, "uid") != binding.ServiceUID || objectStringField(metadata, "deletionTimestamp") != "" ||
				!appWorkloadOwnerMatches(metadata, map[string]string{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID}) ||
				selector[runtime.FugueLabelAppReleaseID] != previous.ID || selector[runtime.FugueLabelAppWorkload] != binding.DeploymentName {
				return nil, fmt.Errorf("canonical handoff previous target Service identity differs")
			}
			selectors = append(selectors, selector)
		}
		if id == handoff.PreviousTargetReleaseID {
			return selectors, nil
		}
		id = previous.RollbackTargetID
	}
	return nil, fmt.Errorf("canonical handoff previous target is outside the verified promotion chain")
}

// Deployment annotations advance metadata.generation. Wait for Kubernetes to
// acknowledge the unchanged source template before moving Service selectors;
// a successful marker CAS alone is not workload readiness.
func (s *Service) captureCanonicalAfterMarker(ctx context.Context, client *kubeClient, app model.App, original model.AppRelease) (releaseDrainWorkload, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		observed, err := s.captureDrainWorkload(waitCtx, client, app, original, false, true)
		if err == nil {
			return observed, nil
		}
		select {
		case <-waitCtx.Done():
			return releaseDrainWorkload{}, fmt.Errorf("%w: refresh source after marker: %v", errSafeCanonicalHandoffPending, err)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// A promotion and its canonical Service handoff are separate durable writes.
// Keep a previous target alive if reconciliation stopped between those writes.
func (s *Service) canonicalServicesReferenceRelease(ctx context.Context, client *kubeClient, app model.App, release model.AppRelease) (bool, error) {
	if release.RevisionWorkload == nil {
		return false, nil
	}
	name := runtime.RuntimeAppResourceName(app)
	objects := s.Renderer.BuildManagedAppChildObjects(app, runtime.SchedulingConstraints{}, nil)
	for _, object := range objects {
		if object["kind"] != "Service" || objectStringMapValue(nestedObjectValue(object, "spec", "selector"))[runtime.FugueLabelAppWorkload] != name {
			continue
		}
		serviceName := objectStringField(objectMapField(object, "metadata"), "name")
		service, found, err := client.getRawObject(ctx, "/api/v1/namespaces/"+url.PathEscape(runtime.NamespaceForTenant(app.TenantID))+"/services/"+url.PathEscape(serviceName))
		if err != nil {
			return false, err
		}
		if found {
			selector := objectStringMapValue(nestedObjectValue(service, "spec", "selector"))
			if selector[runtime.FugueLabelAppReleaseID] == release.ID || selector[runtime.FugueLabelAppWorkload] == release.RevisionWorkload.DeploymentName {
				return true, nil
			}
		}
	}
	return false, nil
}

// A Service can have consumers outside managed Edge (including autonomous
// origins). Withdraw new connections from the canonical workload while it is
// still running, then positively observe every original Pod before changing its
// template. A bounded observation timeout retains both running workloads.
func (s *Service) prepareSafeCanonicalHandoff(ctx context.Context, client *kubeClient, app model.App, objects []map[string]any) (result []map[string]any, resultErr error) {
	if client == nil || s.Store == nil || !s.Config.KubectlApply || !model.AppSafeZeroDowntimeRolloutEnabled(app.Spec) {
		return objects, nil
	}
	namespace, name := runtime.NamespaceForTenant(app.TenantID), runtime.RuntimeAppResourceName(app)
	desired := firstManagedAppDeploymentObject(objects, name)
	if desired == nil {
		return objects, nil
	}
	live, found, err := client.getRawObject(ctx, deploymentAPIPath(namespace, name))
	if err != nil || !found {
		return objects, err
	}
	metadata := objectMapField(live, "metadata")
	marker := objectStringMapValue(metadata["annotations"])[safeCanonicalHandoffAnnotation]
	// Once a handoff is persisted, an unavailable observation keeps the promoted
	// desired state and both workloads. Do not turn a retryable observation error
	// into a failed deploy that discards the desired replacement.
	defer func() {
		if marker != "" && resultErr != nil && ctx.Err() == nil && !errors.Is(resultErr, errSafeCanonicalHandoffPending) {
			resultErr = fmt.Errorf("%w: %v", errSafeCanonicalHandoffPending, resultErr)
		}
	}()

	desiredKey := objectStringMapValue(nestedObjectValue(desired, "spec", "template", "metadata", "annotations"))[runtime.FugueAnnotationReleaseKey]
	liveKey := objectStringMapValue(nestedObjectValue(live, "spec", "template", "metadata", "annotations"))[runtime.FugueAnnotationReleaseKey]
	if marker == "" && desiredKey == liveKey {
		return objects, nil
	}
	policy, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	if errors.Is(err, store.ErrNotFound) && marker == "" {
		return objects, nil
	}
	if err != nil {
		return nil, err
	}
	// Before promotion the operation already keeps the canonical template.
	// A persisted handoff may never be silently bypassed by reconciliation.
	if policy.Mode != model.AppTrafficModeSingle || policy.StableWeight != 100 || policy.CandidateWeight != 0 || policy.CandidateReleaseID != "" {
		if marker != "" {
			return nil, fmt.Errorf("canonical handoff retained: stable traffic policy changed")
		}
		return objects, nil
	}
	release, err := s.Store.GetAppRelease(app.TenantID, true, policy.StableReleaseID)
	if err != nil {
		return nil, err
	}
	binding := release.RevisionWorkload
	if binding == nil || release.DeploymentName == name || release.Role != model.AppReleaseRoleStable || release.Status != model.AppReleaseStatusServing {
		if marker != "" {
			return nil, fmt.Errorf("canonical handoff retained: independent promoted release missing")
		}
		return objects, nil
	}
	if !s.safeRolloutBoundReleaseMatchesApp(app, release) || desiredKey == "" || liveKey == "" {
		return nil, fmt.Errorf("canonical handoff retained: desired release differs from promoted executable")
	}
	candidate := release
	candidate.DeploymentName, candidate.ServiceName = binding.DeploymentName, binding.ServiceName
	candidateWorkload, err := s.captureReleaseDrainWorkload(ctx, client, app, candidate)
	if err != nil {
		return nil, fmt.Errorf("verify handoff destination: %w", err)
	}
	target, found, err := client.getRawObject(ctx, "/api/v1/namespaces/"+url.PathEscape(namespace)+"/services/"+url.PathEscape(binding.ServiceName))
	if err != nil || !found {
		return nil, fmt.Errorf("canonical handoff destination Service unavailable")
	}

	gate := s.evaluateSafeRolloutCandidateRetireGate(ctx, app, release, s.safeRolloutPlanForApp(app).Gate)
	if gate.Status == model.AppReleaseGateStatusFail {
		return nil, fmt.Errorf("%w: promoted release health gate failed", errSafeCanonicalHandoffPending)
	}
	proof, err := s.edgeBundleObserverForSafeRollout().WaitForSafeRolloutEdgeRouteBundle(ctx, app, release, 0, time.Now().UTC())
	if err != nil || !proof.Ready {
		return nil, fmt.Errorf("%w: promoted traffic publication is unconfirmed", errSafeCanonicalHandoffPending)
	}
	selector := objectStringMapValue(nestedObjectValue(target, "spec", "selector"))
	if selector[runtime.FugueLabelAppWorkload] != binding.DeploymentName || selector[runtime.FugueLabelAppReleaseID] != release.ID {
		return nil, fmt.Errorf("canonical handoff destination selector differs")
	}
	var handoff safeCanonicalHandoff
	if marker != "" {
		if err := json.Unmarshal([]byte(marker), &handoff); err != nil || handoff.DeploymentUID != objectStringField(metadata, "uid") || handoff.ReleaseKey == "" || handoff.Image == "" {
			return nil, fmt.Errorf("canonical handoff identity changed")
		}
	} else {
		containers := mapSlice(nestedObjectValue(live, "spec", "template", "spec", "containers"))
		if len(containers) == 0 {
			return nil, fmt.Errorf("canonical handoff source container missing")
		}
		handoff = safeCanonicalHandoff{ReleaseID: release.ID, SourceReleaseID: release.RollbackTargetID, DeploymentUID: objectStringField(metadata, "uid"), ReleaseKey: liveKey, Image: objectStringField(containers[0], "image")}
	}
	advanceTarget := marker != "" && handoff.ReleaseID != release.ID
	if advanceTarget {
		var observed kubeDeployment
		raw, _ := json.Marshal(live)
		if err := json.Unmarshal(raw, &observed); err != nil {
			return nil, err
		}
		handoff, err = s.verifiedCanonicalHandoffSource(app, handoff, observed)
		if err != nil {
			return nil, err
		}
		sourceID, err := s.canonicalHandoffSourceReleaseID(app, handoff, release)
		if err != nil {
			return nil, err
		}
		handoff.SourceReleaseID = sourceID
		if handoff.PreviousTargetReleaseID != "" {
			pending, err := s.Store.GetAppRelease(app.TenantID, true, handoff.PreviousTargetReleaseID)
			if err != nil {
				return nil, err
			}
			retained, err := s.canonicalServicesReferenceRelease(ctx, client, app, pending)
			if err != nil {
				return nil, err
			}
			if !retained {
				handoff.PreviousTargetReleaseID = handoff.ReleaseID
			}
		} else {
			handoff.PreviousTargetReleaseID = handoff.ReleaseID
		}
		handoff.ReleaseID = release.ID
	}
	previousSelectors, err := s.canonicalHandoffPreviousTargetSelectors(ctx, client, app, handoff, release)
	if err != nil {
		return nil, err
	}
	if !appWorkloadOwnerMatches(metadata, map[string]string{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID}) || objectStringField(metadata, "deletionTimestamp") != "" {
		return nil, fmt.Errorf("canonical handoff source ownership changed")
	}
	var deployment kubeDeployment
	raw, _ := json.Marshal(live)
	if err := json.Unmarshal(raw, &deployment); err != nil {
		return nil, err
	}
	// A prior pass has already drained and replaced the canonical Deployment.
	// Restore canonical Service selectors only after that exact new template is ready.
	if marker != "" && liveKey == desiredKey && managedDeploymentStatusReady(deployment, app.Spec.Replicas) && s.migrationImageRefsEquivalent(app, deploymentPrimaryContainerImage(deployment), app.Spec.Image) {
		if err := s.restoreSafeCanonicalServices(ctx, client, app, objects, live, selector); err != nil {
			return nil, err
		}
		if err := client.patchAppWorkloadObject(ctx, deploymentAPIPath(namespace, name), live, map[string]any{"metadata": map[string]any{"annotations": map[string]any{safeCanonicalHandoffAnnotation: nil}}}); err != nil {
			return nil, err
		}
		return objects, nil
	}
	if liveKey != handoff.ReleaseKey {
		return nil, fmt.Errorf("%w: replacement readiness", errSafeCanonicalHandoffPending)
	}
	original := model.AppRelease{AppID: app.ID, TenantID: app.TenantID, DeploymentName: name, ResolvedImageRef: handoff.Image, RevisionWorkload: &model.AppReleaseWorkload{DeploymentName: name, DeploymentUID: handoff.DeploymentUID, ReleaseKey: handoff.ReleaseKey, DeploymentGeneration: deployment.Metadata.Generation}}
	before, err := s.captureDrainWorkload(ctx, client, app, original, false, true)
	if err != nil {
		return nil, fmt.Errorf("capture original canonical workload: %w", err)
	}
	if marker == "" || advanceTarget {
		encoded, _ := json.Marshal(handoff)
		marker = string(encoded)
		if err := client.patchAppWorkloadObject(ctx, deploymentAPIPath(namespace, name), live, map[string]any{"metadata": map[string]any{"annotations": map[string]any{safeCanonicalHandoffAnnotation: marker}}}); err != nil {
			return nil, err
		}
		before, err = s.captureCanonicalAfterMarker(ctx, client, app, original)
		if err != nil {
			return nil, err
		}
	}
	services := []string{}
	for _, object := range objects {
		if object["kind"] != "Service" {
			continue
		}
		ownSelector := objectStringMapValue(nestedObjectValue(object, "spec", "selector"))
		if ownSelector[runtime.FugueLabelAppWorkload] != name {
			continue
		}
		serviceName := objectStringField(objectMapField(object, "metadata"), "name")
		path := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/services/" + url.PathEscape(serviceName)
		current, exists, e := client.getRawObject(ctx, path)
		if e != nil || !exists {
			return nil, fmt.Errorf("canonical handoff source Service unavailable")
		}
		cm := objectMapField(current, "metadata")
		if objectStringField(cm, "uid") == "" || objectStringField(cm, "resourceVersion") == "" {
			return nil, fmt.Errorf("canonical Service identity fence unavailable")
		}
		if !appWorkloadOwnerMatches(cm, map[string]string{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID}) || objectStringField(cm, "deletionTimestamp") != "" {
			return nil, fmt.Errorf("canonical handoff Service ownership differs")
		}
		existing := objectStringMapValue(nestedObjectValue(current, "spec", "selector"))
		previousMatch := false
		for _, prior := range previousSelectors {
			previousMatch = previousMatch || reflect.DeepEqual(existing, prior)
		}
		if !reflect.DeepEqual(existing, ownSelector) && !reflect.DeepEqual(existing, selector) && !previousMatch {
			return nil, fmt.Errorf("canonical handoff Service has an unexpected selector")
		}
		if !reflect.DeepEqual(existing, selector) {
			// JSON patch replaces the selector as a whole; merge-patch would retain
			// obsolete selector keys and can silently produce an empty endpoint set.
			patch := []map[string]any{{"op": "test", "path": "/metadata/uid", "value": objectStringField(cm, "uid")}, {"op": "test", "path": "/metadata/resourceVersion", "value": objectStringField(cm, "resourceVersion")}, {"op": "test", "path": "/spec/selector", "value": existing}, {"op": "replace", "path": "/spec/selector", "value": selector}}
			if _, e := client.doRequest(ctx, http.MethodPatch, path, "application/json-patch+json", patch, nil); e != nil {
				return nil, e
			}
		}
		objectMapField(object, "spec")["selector"] = selector
		services = append(services, serviceName)
	}
	if len(services) == 0 {
		return nil, fmt.Errorf("canonical handoff has no managed Service")
	}
	// Existing connections stay on original Pods; all newly published endpoints
	// must belong to the immutable candidate before zero can be meaningful.
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var quietSince time.Time
	quiet := 2 * time.Second
	for _, p := range before.Pods {
		if q := time.Duration(p.QuietMS) * time.Millisecond; q > quiet {
			quiet = q
		}
	}
	for {
		if err := waitCtx.Err(); err != nil {
			return nil, fmt.Errorf("%w: %v", errSafeCanonicalHandoffPending, err)
		}
		ready := true
		for _, serviceName := range services {
			if err := safeCanonicalEndpoints(waitCtx, client, namespace, serviceName, candidateWorkload.Pods); err != nil {
				ready = false
				break
			}
		}
		active := 0
		if ready {
			for _, p := range before.Pods {
				receipt, e := client.observeReleasePodDrain(waitCtx, namespace, p)
				if e != nil {
					return nil, e
				}
				active += *receipt.ActiveConnections
			}
		}
		if ready && active == 0 {
			if quietSince.IsZero() {
				quietSince = time.Now()
			}
			if time.Since(quietSince) >= quiet {
				break
			}
		} else {
			quietSince = time.Time{}
		}
		select {
		case <-waitCtx.Done():
			return nil, errSafeCanonicalHandoffPending
		case <-time.After(200 * time.Millisecond):
		}
	}
	after, err := s.captureDrainWorkload(ctx, client, app, original, false, true)
	if err != nil || !reflect.DeepEqual(before, after) {
		return nil, fmt.Errorf("canonical workload changed during drain observation")
	}

	latestCandidate, err := s.captureReleaseDrainWorkload(ctx, client, app, candidate)
	if err != nil || !reflect.DeepEqual(candidateWorkload, latestCandidate) {
		return nil, fmt.Errorf("promoted workload changed during canonical handoff")
	}
	for _, serviceName := range services {
		service, found, err := client.getRawObject(ctx, "/api/v1/namespaces/"+url.PathEscape(namespace)+"/services/"+url.PathEscape(serviceName))
		if err != nil || !found || !reflect.DeepEqual(selector, objectStringMapValue(nestedObjectValue(service, "spec", "selector"))) {
			return nil, fmt.Errorf("canonical handoff selector changed during observation")
		}
		if err := safeCanonicalEndpoints(ctx, client, namespace, serviceName, candidateWorkload.Pods); err != nil {
			return nil, err
		}
	}
	currentPolicy, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	if err != nil || !reflect.DeepEqual(policy, currentPolicy) {
		return nil, fmt.Errorf("stable policy changed during canonical handoff")
	}
	currentRelease, err := s.Store.GetAppRelease(app.TenantID, true, release.ID)
	if err != nil || !sameReleaseDrainTarget(release, currentRelease) {
		return nil, fmt.Errorf("promoted release changed during canonical handoff")
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return objects, nil
}

func safeCanonicalEndpoints(ctx context.Context, client *kubeClient, namespace, name string, pods []releaseDrainPod) error {
	expected := map[string]bool{}
	for _, p := range pods {
		if p.IP == "" {
			return fmt.Errorf("candidate Pod has no IP")
		}
		expected[p.IP] = true
	}
	slices, available, err := client.listEndpointSlicesForServiceWithAvailability(ctx, namespace, name)
	if err != nil || !available || len(expected) == 0 {
		return fmt.Errorf("canonical handoff endpoint proof unavailable")
	}
	seen := map[string]bool{}
	for _, slice := range slices {
		for _, endpoint := range slice.Endpoints {
			for _, address := range endpoint.Addresses {
				if !expected[address] || endpoint.Conditions.Ready == nil || !*endpoint.Conditions.Ready || endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating {
					return fmt.Errorf("canonical handoff endpoints have not converged")
				}
				seen[address] = true
			}
		}
	}
	if !reflect.DeepEqual(expected, seen) {
		return fmt.Errorf("canonical handoff candidate endpoints incomplete")
	}
	return nil
}

// JSON patch replaces the entire selector, including keys introduced by another
// field manager. Server-side apply alone may preserve a revision label that the
// canonical Pods do not have, leaving the Service without matching endpoints.
// Keep the resumable marker until the exact canonical Pods are published.
func (s *Service) restoreSafeCanonicalServices(ctx context.Context, client *kubeClient, app model.App, objects []map[string]any, live map[string]any, candidateSelector map[string]string) error {
	namespace, name := runtime.NamespaceForTenant(app.TenantID), runtime.RuntimeAppResourceName(app)
	metadata := objectMapField(live, "metadata")
	var deployment kubeDeployment
	raw, _ := json.Marshal(live)
	if err := json.Unmarshal(raw, &deployment); err != nil {
		return err
	}
	canonical := model.AppRelease{AppID: app.ID, TenantID: app.TenantID, DeploymentName: name, ResolvedImageRef: app.Spec.Image, RevisionWorkload: &model.AppReleaseWorkload{DeploymentName: name, DeploymentUID: objectStringField(metadata, "uid"), ReleaseKey: objectStringMapValue(nestedObjectValue(live, "spec", "template", "metadata", "annotations"))[runtime.FugueAnnotationReleaseKey], DeploymentGeneration: deployment.Metadata.Generation}}
	before, err := s.captureDrainWorkload(ctx, client, app, canonical, false, true)
	if err != nil {
		return fmt.Errorf("verify replacement canonical Pods: %w", err)
	}
	services := []string{}
	for _, object := range objects {
		if object["kind"] != "Service" {
			continue
		}
		selector := objectStringMapValue(nestedObjectValue(object, "spec", "selector"))
		if selector[runtime.FugueLabelAppWorkload] != name {
			continue
		}
		serviceName := objectStringField(objectMapField(object, "metadata"), "name")
		path := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/services/" + url.PathEscape(serviceName)
		current, found, err := client.getRawObject(ctx, path)
		if err != nil || !found {
			return fmt.Errorf("restore canonical Service unavailable")
		}
		cm := objectMapField(current, "metadata")
		if objectStringField(cm, "uid") == "" || objectStringField(cm, "resourceVersion") == "" || objectStringField(cm, "deletionTimestamp") != "" || !appWorkloadOwnerMatches(cm, map[string]string{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID}) {
			return fmt.Errorf("restore canonical Service identity changed")
		}
		existing := objectStringMapValue(nestedObjectValue(current, "spec", "selector"))
		if !reflect.DeepEqual(existing, selector) {
			if !reflect.DeepEqual(existing, candidateSelector) {
				return fmt.Errorf("restore canonical Service selector changed")
			}
			patch := []map[string]any{{"op": "test", "path": "/metadata/uid", "value": objectStringField(cm, "uid")}, {"op": "test", "path": "/metadata/resourceVersion", "value": objectStringField(cm, "resourceVersion")}, {"op": "test", "path": "/spec/selector", "value": existing}, {"op": "replace", "path": "/spec/selector", "value": selector}}
			if _, err := client.doRequest(ctx, http.MethodPatch, path, "application/json-patch+json", patch, nil); err != nil {
				return err
			}
		}
		services = append(services, serviceName)
	}
	if len(services) == 0 {
		return fmt.Errorf("restore canonical handoff has no managed Service")
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		ready := true
		for _, serviceName := range services {
			if err := safeCanonicalEndpoints(waitCtx, client, namespace, serviceName, before.Pods); err != nil {
				ready = false
				break
			}
		}
		if ready {
			break
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("%w: canonical endpoint restoration", errSafeCanonicalHandoffPending)
		case <-time.After(200 * time.Millisecond):
		}
	}
	after, err := s.captureDrainWorkload(ctx, client, app, canonical, false, true)
	if err != nil || !reflect.DeepEqual(before, after) {
		return fmt.Errorf("replacement canonical Pods changed during service restoration")
	}
	return nil
}
