package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/appimages"
	"fugue/internal/model"
	"fugue/internal/releaseproof"
	"fugue/internal/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Ordinary in-place deployments must pass the same Service -> EndpointSlice ->
// Pod -> ReplicaSet -> Deployment proof as the traffic compiler before completion
// can auto-switch the stable release. Aggregate Ready counts are not that proof.
// No resource or serving configuration is changed by this observer.
func (s *Service) managedAppTrafficReady(ctx context.Context, client *kubeClient, app model.App) (bool, string, error) {
	ns := runtime.NamespaceForTenant(app.TenantID)
	depName, svcName := runtime.RuntimeAppResourceName(app), runtime.RuntimeAppServiceName(app)
	snapshot := releaseproof.Snapshot{
		Namespaces: map[string]struct{}{}, Deployments: map[string]releaseproof.Deployment{},
		Services: map[string]releaseproof.Service{}, EndpointSlicesAvailable: true,
		EndpointSlices: map[string][]releaseproof.EndpointSlice{}, Pods: map[string]releaseproof.Pod{}, ReplicaSets: map[string]releaseproof.ReplicaSet{},
	}
	read := func(path string, out any) error {
		_, err := client.doJSON(ctx, http.MethodGet, path, nil, out)
		return err
	}
	var namespace releaseproof.MetadataEvidence
	if err := read("/api/v1/namespaces/"+url.PathEscape(ns), &namespace); err != nil {
		return false, "", err
	}
	if namespace.Metadata.Name != ns || namespace.Metadata.UID == "" || namespace.Metadata.DeletionTimestamp != "" {
		return false, "waiting for traffic namespace identity", nil
	}
	snapshot.Namespaces[ns] = struct{}{}
	var dep releaseproof.Deployment
	if err := read(deploymentAPIPath(ns, depName), &dep); err != nil {
		return false, "", err
	}
	var svc releaseproof.Service
	svcPath := "/api/v1/namespaces/" + url.PathEscape(ns) + "/services/" + url.PathEscape(svcName)
	if err := read(svcPath, &svc); err != nil {
		return false, "", err
	}
	if dep.Metadata.Name != depName || dep.Metadata.Namespace != ns || svc.Metadata.Name != svcName || svc.Metadata.Namespace != ns {
		return false, "waiting for traffic resource identities", nil
	}
	if dep.Metadata.ResourceVersion == "" || svc.Metadata.ResourceVersion == "" {
		return false, "waiting for traffic resource versions", nil
	}
	snapshot.Deployments[releaseproof.NamespacedKey(ns, depName)] = dep
	snapshot.Services[releaseproof.NamespacedKey(ns, svcName)] = svc
	var slices struct {
		Items    []releaseproof.EndpointSlice `json:"items"`
		Metadata struct {
			Continue string `json:"continue"`
		} `json:"metadata"`
	}
	if err := read("/apis/discovery.k8s.io/v1/namespaces/"+url.PathEscape(ns)+"/endpointslices?labelSelector="+url.QueryEscape("kubernetes.io/service-name="+svcName), &slices); err != nil {
		return false, "", err
	}
	if slices.Metadata.Continue != "" {
		return false, "", fmt.Errorf("traffic endpoint inventory is incomplete")
	}
	sk := releaseproof.NamespacedKey(ns, svcName)
	snapshot.EndpointSlices[sk] = slices.Items
	for _, slice := range slices.Items {
		if slice.Metadata.Labels["kubernetes.io/service-name"] != svcName {
			return false, "waiting for traffic EndpointSlice identity", nil
		}
		for _, ep := range slice.Endpoints {
			target := ep.TargetRef
			if target.Kind != "Pod" || target.Namespace != ns || len(validation.IsDNS1123Subdomain(target.Name)) != 0 {
				return false, "waiting for traffic endpoint Pod identity", nil
			}
			pk := releaseproof.NamespacedKey(ns, target.Name)
			if _, found := snapshot.Pods[pk]; found {
				continue
			}
			var pod releaseproof.Pod
			if err := read("/api/v1/namespaces/"+url.PathEscape(ns)+"/pods/"+url.PathEscape(target.Name), &pod); err != nil {
				return false, "", err
			}
			snapshot.Pods[pk] = pod
			owner, ok := releaseproof.SingleControllerOwner(pod.Metadata.OwnerReferences, "apps/v1", "ReplicaSet")
			if !ok || len(validation.IsDNS1123Subdomain(owner.Name)) != 0 {
				return false, "waiting for traffic ReplicaSet identity", nil
			}
			rk := releaseproof.NamespacedKey(ns, owner.Name)
			if _, found := snapshot.ReplicaSets[rk]; found {
				continue
			}
			var rs releaseproof.ReplicaSet
			if err := read("/apis/apps/v1/namespaces/"+url.PathEscape(ns)+"/replicasets/"+url.PathEscape(owner.Name), &rs); err != nil {
				return false, "", err
			}
			snapshot.ReplicaSets[rk] = rs
		}
	}
	// This identity is local to observation, never persisted as a release. The
	// canonical workload accepts only its normal empty release label, not an
	// independent revision. Safe rollouts have their own bound revision gate.
	port := 80
	if app.Route != nil && app.Route.ServicePort > 0 {
		port = app.Route.ServicePort
	} else if len(app.Spec.Ports) > 0 {
		port = app.Spec.Ports[0]
	}
	release := model.AppRelease{ID: "pending-canonical-deploy", TenantID: app.TenantID, AppID: app.ID,
		Role: model.AppReleaseRoleStable, Status: model.AppReleaseStatusReady, RuntimeID: app.Spec.RuntimeID,
		ResolvedImageRef: app.Spec.Image, SpecSnapshot: &app.Spec, DeploymentName: depName, ServiceName: svcName,
		UpstreamURL: fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", svcName, ns, port),
	}
	equivalent := func(a model.App, expected, actual string) bool {
		norm := func(ref string) string {
			return appimages.NormalizeRuntimeImageRefForSource(strings.TrimSpace(ref), model.AppBuildSource(a), s.registryPushBase, s.registryPullBase)
		}
		expected, actual = norm(expected), norm(actual)
		return expected != "" && expected == actual
	}
	_, proof := releaseproof.Observe(app, release, true, snapshot, time.Now().UTC(), equivalent)
	if !proof.Ready {
		return false, "waiting for traffic readiness: " + proof.Reason, nil
	}
	// Pin the target across the non-atomic multi-resource reads. A concurrent
	// replacement/rollout must be observed again, not authorized by mixed evidence.
	var confirmed releaseproof.Deployment
	if err := read(deploymentAPIPath(ns, depName), &confirmed); err != nil {
		return false, "", err
	}
	var confirmedSvc releaseproof.Service
	if err := read(svcPath, &confirmedSvc); err != nil {
		return false, "", err
	}
	if dep.Metadata.UID != confirmed.Metadata.UID || dep.Metadata.ResourceVersion != confirmed.Metadata.ResourceVersion || svc.Metadata.UID != confirmedSvc.Metadata.UID || svc.Metadata.ResourceVersion != confirmedSvc.Metadata.ResourceVersion {
		return false, "waiting for stable traffic resource observation", nil
	}
	return true, "traffic ownership and readiness verified", nil
}
