package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/releaseflow"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

// This is compiler input evidence, not an assertion that the release is serving.
// Store only resource identities and readiness, never a workload's environment.
type releaseRuntimeReadiness struct {
	ReleaseID            string                       `json:"release_id"`
	Namespace            string                       `json:"namespace"`
	DeploymentName       string                       `json:"deployment_name"`
	DeploymentUID        string                       `json:"deployment_uid"`
	DeploymentGeneration int64                        `json:"deployment_generation"`
	ObservedGeneration   int64                        `json:"observed_generation"`
	ServiceName          string                       `json:"service_name"`
	ServiceUID           string                       `json:"service_uid"`
	DesiredReplicas      int                          `json:"desired_replicas"`
	ReadyReplicas        int                          `json:"ready_replicas"`
	ReadyEndpoints       int                          `json:"ready_endpoints"`
	EndpointPods         []releaseEndpointPodIdentity `json:"endpoint_pods,omitempty"`
	Ready                bool                         `json:"ready"`
	Reason               string                       `json:"reason,omitempty"`
}

func (s *Server) captureReleaseRuntimeReadiness(ctx context.Context, projection *platformIntentProjectionResponse, business store.RouteBusinessSnapshot) error {
	if len(projection.RuntimeSnapshot.Releases) == 0 {
		return nil
	}
	apps := make(map[string]model.App, len(business.Apps))
	for _, a := range business.Apps {
		apps[a.ID] = a
	}
	runtimes := make(map[string]model.Runtime, len(business.Runtimes))
	for _, r := range business.Runtimes {
		runtimes[r.ID] = r
	}
	releases := make(map[string]model.AppRelease, len(business.Releases))
	for _, r := range business.Releases {
		releases[r.ID] = r
	}
	stable := map[string]string{}
	for _, p := range business.TrafficPolicies {
		stable[p.AppID] = p.StableReleaseID
	}
	managed := func(r model.AppRelease) bool {
		if r.RuntimeID == model.DefaultManagedRuntimeID {
			return true
		}
		v, ok := runtimes[r.RuntimeID]
		return ok && (v.Type == model.RuntimeTypeManagedOwned || v.Type == model.RuntimeTypeManagedShared)
	}
	required := false
	for _, r := range projection.RuntimeSnapshot.Releases {
		if managed(releases[r.ID]) {
			required = true
			break
		}
	}
	if !required {
		return nil
	}
	client, err := s.managedAppStatusClient()
	if err != nil {
		return fmt.Errorf("release readiness observer unavailable: %w", err)
	}
	defer client.closeIdleConnections()
	cluster, err := client.getClusterID(ctx)
	if err != nil {
		return fmt.Errorf("release readiness cluster unavailable: %w", err)
	}
	observedAt := time.Now().UTC()
	snapshot, err := client.readRuntimeSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("release readiness snapshot unavailable: %w", err)
	}
	if err := client.captureReleaseEndpointOwners(ctx, &snapshot); err != nil {
		return fmt.Errorf("release endpoint ownership unavailable: %w", err)
	}
	confirmed, err := client.getClusterID(ctx)
	if err != nil || cluster == "" || confirmed != cluster {
		return fmt.Errorf("release readiness cluster identity changed or unavailable")
	}
	proofs := []releaseRuntimeReadiness{}
	for i, old := range projection.RuntimeSnapshot.Releases {
		release := releases[old.ID]
		if !managed(release) {
			continue
		}
		app := apps[release.AppID]
		fact, proof := s.observeReleaseRuntimeReadiness(app, release, stable[release.AppID] == release.ID, snapshot, observedAt)
		// The route graph, release ids and desired weights are already frozen.
		// Only independently captured runtime facts may be replaced here.
		projection.RuntimeSnapshot.Releases[i] = fact
		proofs = append(proofs, proof)
	}
	raw, err := json.Marshal(struct {
		Schema     string                    `json:"schema"`
		ClusterID  string                    `json:"cluster_id"`
		ObservedAt time.Time                 `json:"observed_at"`
		Releases   []releaseRuntimeReadiness `json:"releases"`
	}{"fugue.release-runtime-readiness/v1", cluster, observedAt, proofs})
	if err != nil {
		return err
	}
	var value map[string]any
	if err = json.Unmarshal(raw, &value); err != nil {
		return err
	}
	if projection.RuntimeSnapshot.Facts == nil {
		projection.RuntimeSnapshot.Facts = map[string]any{}
	}
	projection.RuntimeSnapshot.Facts["release_readiness"] = value
	projection.CapturedAt = time.Now().UTC()
	projection.RuntimeSnapshot.CapturedAt = &projection.CapturedAt
	return nil
}

func (s *Server) observeReleaseRuntimeReadiness(app model.App, release model.AppRelease, stable bool, snapshot managedAppKubeSnapshot, observedAt time.Time) (platformconfig.ReleaseObservation, releaseRuntimeReadiness) {
	fact := platformconfig.ReleaseObservation{ID: release.ID, AppID: release.AppID, TenantID: release.TenantID, RuntimeID: release.RuntimeID, UpstreamURL: release.UpstreamURL, DeploymentGeneration: firstNonEmpty(release.ResolvedImageRef, release.SourceRef), ObservedAt: observedAt, Status: model.EdgeRouteStatusUnavailable}
	proof := releaseRuntimeReadiness{ReleaseID: release.ID, Namespace: runtime.NamespaceForTenant(release.TenantID)}
	fail := func(reason string) (platformconfig.ReleaseObservation, releaseRuntimeReadiness) {
		fact.StatusReason = reason
		proof.Reason = reason
		return fact, proof
	}
	if app.ID == "" || app.ID != release.AppID || app.TenantID != release.TenantID || release.RuntimeID == "" || release.ResolvedImageRef == "" {
		return fail("release owner, runtime or image identity unavailable")
	}
	if release.Status != model.AppReleaseStatusReady && release.Status != model.AppReleaseStatusServing {
		return fail("release lifecycle does not permit traffic")
	}
	if stable {
		release = releaseflow.CanonicalStableTarget(app, release)
	}
	proof.DeploymentName, proof.ServiceName = release.DeploymentName, release.ServiceName
	if release.DeploymentName == "" || release.ServiceName == "" {
		return fail("release workload identity unavailable")
	}
	if spec := release.SpecSnapshot; spec != nil {
		if spec.RuntimeID != release.RuntimeID || !s.observedRuntimeImageRefsEquivalent(app, spec.Image, release.ResolvedImageRef) {
			return fail("release snapshot identity differs")
		}
	} else if !stable || release.RuntimeID != app.Spec.RuntimeID || !s.observedRuntimeImageRefsEquivalent(app, app.Spec.Image, release.ResolvedImageRef) {
		return fail("release workload snapshot unavailable")
	}
	canonical := stable && release.Role == model.AppReleaseRoleStable && release.DeploymentName == runtime.RuntimeAppResourceName(app) && release.ServiceName == runtime.RuntimeAppServiceName(app)
	owner := func(labels map[string]string) bool {
		return labels[runtime.FugueLabelAppID] == app.ID && labels[runtime.FugueLabelTenantID] == app.TenantID && (labels[runtime.FugueLabelAppReleaseID] == release.ID || canonical && labels[runtime.FugueLabelAppReleaseID] == "")
	}
	if _, ok := snapshot.namespaces[proof.Namespace]; !ok {
		return fail("release namespace absent")
	}
	deployment, found := snapshot.deployments[kubeNamespacedKey(proof.Namespace, release.DeploymentName)]
	if !found {
		return fail("release deployment absent")
	}
	proof.DeploymentUID = deployment.Metadata.UID
	proof.DeploymentGeneration = deployment.Metadata.Generation
	proof.ObservedGeneration = deployment.Status.ObservedGeneration
	// Replica count is a runtime target (including autoscaling), not the
	// immutable code snapshot's historical initial count.
	if deployment.Spec.Replicas != nil {
		proof.DesiredReplicas = *deployment.Spec.Replicas
	}
	proof.ReadyReplicas = minObservedReplicaCount(deployment.Status.UpdatedReplicas, deployment.Status.ReadyReplicas, deployment.Status.AvailableReplicas)
	if proof.DeploymentUID == "" || deployment.Metadata.DeletionTimestamp != "" || !owner(deployment.Metadata.Labels) || !owner(deployment.Spec.Template.Metadata.Labels) {
		return fail("release deployment ownership differs")
	}
	if !deploymentCurrentCohortComplete(deployment) || deployment.Spec.Replicas == nil || !s.observedRuntimeImageRefsEquivalent(app, release.ResolvedImageRef, firstDeploymentContainerImage(deployment)) {
		return fail("release deployment image or current generation is not ready")
	}
	serviceKey := kubeNamespacedKey(proof.Namespace, release.ServiceName)
	service, found := snapshot.serviceDetails[serviceKey]
	if !found {
		return fail("release service absent")
	}
	proof.ServiceUID = service.Metadata.UID
	if proof.ServiceUID == "" || service.Metadata.DeletionTimestamp != "" || !owner(service.Metadata.Labels) || !owner(service.Spec.Selector) || service.Spec.Selector[runtime.FugueLabelAppWorkload] != release.DeploymentName {
		return fail("release service ownership differs")
	}
	for k, v := range service.Spec.Selector {
		if deployment.Spec.Template.Metadata.Labels[k] != v {
			return fail("release service selects a different workload")
		}
	}
	upstream, err := url.Parse(release.UpstreamURL)
	if err != nil || upstream.Scheme != "http" || upstream.User != nil || upstream.RawQuery != "" || upstream.Fragment != "" || upstream.Path != "" && upstream.Path != "/" || upstream.Hostname() != release.ServiceName+"."+proof.Namespace+".svc.cluster.local" {
		return fail("release upstream does not name its service")
	}
	port := 80
	if upstream.Port() != "" {
		port, err = strconv.Atoi(upstream.Port())
		if err != nil {
			return fail("release upstream port invalid")
		}
	}
	portMatches := false
	for _, p := range service.Spec.Ports {
		portMatches = portMatches || p.Port == port
	}
	if !portMatches {
		return fail("release upstream port differs from service")
	}
	if binding := release.RevisionWorkload; binding != nil && release.DeploymentName == binding.DeploymentName {
		if binding.Namespace != proof.Namespace || binding.DeploymentUID != proof.DeploymentUID || binding.DeploymentGeneration > proof.DeploymentGeneration || binding.ServiceName != proof.ServiceName || binding.ServiceUID != proof.ServiceUID || binding.ReleaseKey != deployment.Spec.Template.Metadata.Annotations.ReleaseKey || binding.RuntimeID != release.RuntimeID || !s.observedRuntimeImageRefsEquivalent(app, binding.ImageRef, release.ResolvedImageRef) {
			return fail("release revision workload differs from immutable binding")
		}
	}
	endpointPods, err := s.observeReleaseEndpointPods(app, release, deployment, service, snapshot, owner)
	if err != nil {
		return fail(err.Error())
	}
	proof.ReadyEndpoints = len(endpointPods)
	proof.EndpointPods = endpointPods
	if proof.ReadyEndpoints < proof.DesiredReplicas {
		return fail("release service-owned endpoint Pods are not ready")
	}
	fact.Status = model.EdgeRouteStatusActive
	fact.StatusReason = ""
	proof.Ready = true
	return fact, proof
}
