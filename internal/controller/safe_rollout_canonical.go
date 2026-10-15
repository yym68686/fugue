package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

var errSafeCanonicalHandoffPending = errors.New("canonical handoff awaits positive drain or readiness proof")

const safeCanonicalHandoffAnnotation = "fugue.io/safe-canonical-handoff"

type safeCanonicalHandoff struct {
	ReleaseID     string `json:"release_id"`
	DeploymentUID string `json:"deployment_uid"`
	ReleaseKey    string `json:"release_key"`
	Image         string `json:"image"`
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
		if err := json.Unmarshal([]byte(marker), &handoff); err != nil || handoff.ReleaseID != release.ID || handoff.DeploymentUID != objectStringField(metadata, "uid") || handoff.ReleaseKey == "" || handoff.Image == "" {
			return nil, fmt.Errorf("canonical handoff identity changed")
		}
	} else {
		containers := mapSlice(nestedObjectValue(live, "spec", "template", "spec", "containers"))
		if len(containers) == 0 {
			return nil, fmt.Errorf("canonical handoff source container missing")
		}
		handoff = safeCanonicalHandoff{ReleaseID: release.ID, DeploymentUID: objectStringField(metadata, "uid"), ReleaseKey: liveKey, Image: objectStringField(containers[0], "image")}
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
	if marker == "" {
		encoded, _ := json.Marshal(handoff)
		marker = string(encoded)
		if err := client.patchAppWorkloadObject(ctx, deploymentAPIPath(namespace, name), live, map[string]any{"metadata": map[string]any{"annotations": map[string]any{safeCanonicalHandoffAnnotation: marker}}}); err != nil {
			return nil, err
		}
		before, err = s.captureDrainWorkload(ctx, client, app, original, false, true)
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
		if !reflect.DeepEqual(existing, ownSelector) && !reflect.DeepEqual(existing, selector) {
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
