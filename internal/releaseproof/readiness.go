// Package releaseproof shares the exact runtime ownership and readiness gate
// between deployment completion and traffic compilation. It observes facts only;
// it cannot mutate desired configuration, release weights or serving artifacts.
package releaseproof

import (
	"fmt"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/releaseflow"
	"fugue/internal/runtime"
)

type Snapshot struct {
	Namespaces              map[string]struct{}
	Deployments             map[string]Deployment
	Services                map[string]Service
	EndpointSlicesAvailable bool
	EndpointSlices          map[string][]EndpointSlice
	Pods                    map[string]Pod
	ReplicaSets             map[string]ReplicaSet
}

func NamespacedKey(namespace, name string) string {
	return strings.TrimSpace(namespace) + "/" + strings.TrimSpace(name)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

type MetadataEvidence struct {
	Metadata struct {
		UID               string             `json:"uid"`
		ResourceVersion   string             `json:"resourceVersion"`
		Name              string             `json:"name"`
		Namespace         string             `json:"namespace"`
		DeletionTimestamp string             `json:"deletionTimestamp"`
		Labels            map[string]string  `json:"labels"`
		Annotations       ReleaseAnnotations `json:"annotations"`
		OwnerReferences   []OwnerReference   `json:"ownerReferences"`
	} `json:"metadata"`
}
type Service struct {
	MetadataEvidence
	Spec struct {
		Selector map[string]string `json:"selector"`
		Ports    []struct {
			Port int `json:"port"`
		} `json:"ports"`
	} `json:"spec"`
}
type EndpointSlice struct {
	MetadataEvidence
	Endpoints []Endpoint `json:"endpoints"`
}

type Deployment struct {
	Metadata struct {
		UID               string            `json:"uid"`
		ResourceVersion   string            `json:"resourceVersion"`
		Name              string            `json:"name"`
		Namespace         string            `json:"namespace"`
		Generation        int64             `json:"generation"`
		Labels            map[string]string `json:"labels"`
		DeletionTimestamp string            `json:"deletionTimestamp"`
	} `json:"metadata"`
	Spec struct {
		Replicas *int `json:"replicas"`
		Template struct {
			Metadata struct {
				Labels      map[string]string  `json:"labels"`
				Annotations ReleaseAnnotations `json:"annotations"`
			} `json:"metadata"`
			Spec struct {
				Containers []struct {
					Name  string `json:"name"`
					Image string `json:"image"`
				} `json:"containers"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
	Status struct {
		Replicas           int   `json:"replicas"`
		UpdatedReplicas    int   `json:"updatedReplicas"`
		ReadyReplicas      int   `json:"readyReplicas"`
		AvailableReplicas  int   `json:"availableReplicas"`
		ObservedGeneration int64 `json:"observedGeneration"`
	} `json:"status"`
}

type OwnerReference struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
	Controller *bool  `json:"controller"`
}

type ReleaseAnnotations struct {
	ReleaseKey string `json:"fugue.pro/release-key"`
}

type Endpoint struct {
	Addresses  []string `json:"addresses"`
	Conditions struct {
		Ready       *bool `json:"ready"`
		Serving     *bool `json:"serving"`
		Terminating *bool `json:"terminating"`
	} `json:"conditions"`
	TargetRef struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Name       string `json:"name"`
		Namespace  string `json:"namespace"`
		UID        string `json:"uid"`
	} `json:"targetRef"`
}

type ReplicaSet struct {
	MetadataEvidence
	Spec struct {
		Template struct {
			MetadataEvidence
		} `json:"template"`
	} `json:"spec"`
}

// Decode only readiness and identity fields; executable settings and secrets
// from the Pod payload are neither retained nor included in compiler facts.
type Pod struct {
	MetadataEvidence
	Spec struct {
		Containers []struct {
			Name  string `json:"name"`
			Image string `json:"image"`
		} `json:"containers"`
	} `json:"spec"`
	Status struct {
		Phase  string `json:"phase"`
		PodIP  string `json:"podIP"`
		PodIPs []struct {
			IP string `json:"ip"`
		} `json:"podIPs"`
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
	} `json:"status"`
}

type EndpointPodIdentity struct {
	PodName        string   `json:"pod_name"`
	PodUID         string   `json:"pod_uid"`
	ReplicaSetName string   `json:"replica_set_name"`
	ReplicaSetUID  string   `json:"replica_set_uid"`
	ReleaseKey     string   `json:"release_key"`
	Addresses      []string `json:"addresses"`
}

type Readiness struct {
	ReleaseID            string                `json:"release_id"`
	Namespace            string                `json:"namespace"`
	DeploymentName       string                `json:"deployment_name"`
	DeploymentUID        string                `json:"deployment_uid"`
	DeploymentGeneration int64                 `json:"deployment_generation"`
	ObservedGeneration   int64                 `json:"observed_generation"`
	ServiceName          string                `json:"service_name"`
	ServiceUID           string                `json:"service_uid"`
	DesiredReplicas      int                   `json:"desired_replicas"`
	ReadyReplicas        int                   `json:"ready_replicas"`
	ReadyEndpoints       int                   `json:"ready_endpoints"`
	EndpointPods         []EndpointPodIdentity `json:"endpoint_pods,omitempty"`
	Ready                bool                  `json:"ready"`
	Reason               string                `json:"reason,omitempty"`
}

func Observe(app model.App, release model.AppRelease, stable bool, snapshot Snapshot, observedAt time.Time, equivalent func(model.App, string, string) bool) (platformconfig.ReleaseObservation, Readiness) {
	fact := platformconfig.ReleaseObservation{ID: release.ID, AppID: release.AppID, TenantID: release.TenantID, RuntimeID: release.RuntimeID, UpstreamURL: release.UpstreamURL, DeploymentGeneration: firstNonEmpty(release.ResolvedImageRef, release.SourceRef), ObservedAt: observedAt, Status: model.EdgeRouteStatusUnavailable}
	proof := Readiness{ReleaseID: release.ID, Namespace: runtime.NamespaceForTenant(release.TenantID)}
	fail := func(reason string) (platformconfig.ReleaseObservation, Readiness) {
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
		if spec.RuntimeID != release.RuntimeID || !equivalent(app, spec.Image, release.ResolvedImageRef) {
			return fail("release snapshot identity differs")
		}
	} else if !stable || release.RuntimeID != app.Spec.RuntimeID || !equivalent(app, app.Spec.Image, release.ResolvedImageRef) {
		return fail("release workload snapshot unavailable")
	}
	canonical := stable && release.Role == model.AppReleaseRoleStable && release.DeploymentName == runtime.RuntimeAppResourceName(app) && release.ServiceName == runtime.RuntimeAppServiceName(app)
	owner := func(labels map[string]string) bool {
		return labels[runtime.FugueLabelAppID] == app.ID && labels[runtime.FugueLabelTenantID] == app.TenantID && (labels[runtime.FugueLabelAppReleaseID] == release.ID || canonical && labels[runtime.FugueLabelAppReleaseID] == "")
	}
	if _, ok := snapshot.Namespaces[proof.Namespace]; !ok {
		return fail("release namespace absent")
	}
	deployment, found := snapshot.Deployments[NamespacedKey(proof.Namespace, release.DeploymentName)]
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
	if !DeploymentCurrentCohortComplete(deployment) || deployment.Spec.Replicas == nil || !equivalent(app, release.ResolvedImageRef, firstDeploymentContainerImage(deployment)) {
		return fail("release deployment image or current generation is not ready")
	}
	serviceKey := NamespacedKey(proof.Namespace, release.ServiceName)
	service, found := snapshot.Services[serviceKey]
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
		if binding.Namespace != proof.Namespace || binding.DeploymentUID != proof.DeploymentUID || binding.DeploymentGeneration > proof.DeploymentGeneration || binding.ServiceName != proof.ServiceName || binding.ServiceUID != proof.ServiceUID || binding.ReleaseKey != deployment.Spec.Template.Metadata.Annotations.ReleaseKey || binding.RuntimeID != release.RuntimeID || !equivalent(app, binding.ImageRef, release.ResolvedImageRef) {
			return fail("release revision workload differs from immutable binding")
		}
	}
	endpointPods, err := observeReleaseEndpointPods(app, release, deployment, service, snapshot, owner, equivalent)
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

func SingleControllerOwner(refs []OwnerReference, apiVersion, kind string) (OwnerReference, bool) {
	var result OwnerReference
	count := 0
	for _, ref := range refs {
		if ref.Controller != nil && *ref.Controller {
			count++
			result = ref
		}
	}
	return result, count == 1 && result.APIVersion == apiVersion && result.Kind == kind && result.Name != "" && result.UID != ""
}

func observeReleaseEndpointPods(app model.App, release model.AppRelease, deployment Deployment, service Service, snapshot Snapshot, owner func(map[string]string) bool, equivalent func(model.App, string, string) bool) ([]EndpointPodIdentity, error) {
	ns := runtime.NamespaceForTenant(app.TenantID)
	slices := snapshot.EndpointSlices[NamespacedKey(ns, service.Metadata.Name)]
	key := deployment.Spec.Template.Metadata.Annotations.ReleaseKey
	if !snapshot.EndpointSlicesAvailable || len(slices) == 0 || snapshot.Pods == nil || snapshot.ReplicaSets == nil || key == "" {
		return nil, fmt.Errorf("release endpoint ownership evidence unavailable")
	}
	ready := map[string]EndpointPodIdentity{}
	addresses := map[string]map[string]struct{}{}
	addressOwner := map[netip.Addr]string{}
	for _, slice := range slices {
		ref, valid := SingleControllerOwner(slice.Metadata.OwnerReferences, "v1", "Service")
		if !valid || ref.Name != service.Metadata.Name || ref.UID != service.Metadata.UID || slice.Metadata.Namespace != ns || slice.Metadata.UID == "" || slice.Metadata.DeletionTimestamp != "" {
			return nil, fmt.Errorf("release EndpointSlice Service ownership differs")
		}
		for _, endpoint := range slice.Endpoints {
			target := endpoint.TargetRef
			pod, found := snapshot.Pods[NamespacedKey(ns, target.Name)]
			if !found || target.Kind != "Pod" || target.APIVersion != "" && target.APIVersion != "v1" || target.Namespace != ns || target.UID == "" || target.UID != pod.Metadata.UID || pod.Metadata.Namespace != ns || !owner(pod.Metadata.Labels) || len(endpoint.Addresses) == 0 {
				return nil, fmt.Errorf("release endpoint Pod identity differs")
			}
			for label, value := range service.Spec.Selector {
				if pod.Metadata.Labels[label] != value {
					return nil, fmt.Errorf("release endpoint Pod does not match Service selector")
				}
			}
			rsRef, valid := SingleControllerOwner(pod.Metadata.OwnerReferences, "apps/v1", "ReplicaSet")
			rs, found := snapshot.ReplicaSets[NamespacedKey(ns, rsRef.Name)]
			if !valid || !found || rsRef.UID != rs.Metadata.UID || rs.Metadata.Namespace != ns || rs.Metadata.DeletionTimestamp != "" || !owner(rs.Metadata.Labels) || !owner(rs.Spec.Template.Metadata.Labels) {
				return nil, fmt.Errorf("release endpoint ReplicaSet identity differs")
			}
			depRef, valid := SingleControllerOwner(rs.Metadata.OwnerReferences, "apps/v1", "Deployment")
			if !valid {
				return nil, fmt.Errorf("release endpoint ReplicaSet Deployment controller owner invalid: replica_set=%s", rs.Metadata.Name)
			}
			for _, check := range []struct{ field, expected, observed string }{
				{"deployment_name", deployment.Metadata.Name, depRef.Name},
				{"deployment_uid", deployment.Metadata.UID, depRef.UID},
				{"replica_set_workload_label", deployment.Metadata.Name, rs.Spec.Template.Metadata.Labels[runtime.FugueLabelAppWorkload]},
				{"pod_release_key", key, pod.Metadata.Annotations.ReleaseKey},
				{"replica_set_release_key", key, rs.Spec.Template.Metadata.Annotations.ReleaseKey},
			} {
				if check.expected != check.observed {
					return nil, fmt.Errorf("release endpoint identity differs: field=%s expected=%s observed=%s pod=%s pod_uid=%s pod_rv=%s replica_set=%s replica_set_uid=%s replica_set_rv=%s deployment=%s deployment_rv=%s", check.field, check.expected, check.observed, pod.Metadata.Name, pod.Metadata.UID, pod.Metadata.ResourceVersion, rs.Metadata.Name, rs.Metadata.UID, rs.Metadata.ResourceVersion, deployment.Metadata.Name, deployment.Metadata.ResourceVersion)
				}
			}
			podIPs := map[netip.Addr]struct{}{}
			if ip, err := netip.ParseAddr(pod.Status.PodIP); err == nil {
				podIPs[ip.Unmap()] = struct{}{}
			}
			for _, item := range pod.Status.PodIPs {
				if ip, err := netip.ParseAddr(item.IP); err == nil {
					podIPs[ip.Unmap()] = struct{}{}
				}
			}
			for _, address := range endpoint.Addresses {
				ip, err := netip.ParseAddr(address)
				if err != nil {
					return nil, fmt.Errorf("release endpoint address is not an IP")
				}
				if _, found := podIPs[ip.Unmap()]; !found {
					return nil, fmt.Errorf("release endpoint address differs from Pod")
				}
				if previous := addressOwner[ip.Unmap()]; previous != "" && previous != target.UID {
					return nil, fmt.Errorf("release endpoint address has multiple Pod owners")
				}
				addressOwner[ip.Unmap()] = target.UID
			}
			if endpoint.Conditions.Ready == nil || !*endpoint.Conditions.Ready {
				continue
			}
			podReady := false
			for _, condition := range pod.Status.Conditions {
				if condition.Type == "Ready" {
					podReady = condition.Status == "True"
				}
			}
			if endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating || endpoint.Conditions.Serving != nil && !*endpoint.Conditions.Serving || pod.Metadata.DeletionTimestamp != "" || pod.Status.Phase != "Running" || !podReady || len(pod.Spec.Containers) == 0 || !equivalent(app, pod.Spec.Containers[0].Image, release.ResolvedImageRef) {
				return nil, fmt.Errorf("release endpoint Pod is not currently ready")
			}
			ready[target.UID] = EndpointPodIdentity{PodName: target.Name, PodUID: target.UID, ReplicaSetName: rsRef.Name, ReplicaSetUID: rsRef.UID, ReleaseKey: key}
			if addresses[target.UID] == nil {
				addresses[target.UID] = map[string]struct{}{}
			}
			for _, address := range endpoint.Addresses {
				ip, _ := netip.ParseAddr(address)
				addresses[target.UID][ip.Unmap().String()] = struct{}{}
			}
		}
	}
	result := make([]EndpointPodIdentity, 0, len(ready))
	for uid, pod := range ready {
		for ip := range addresses[uid] {
			pod.Addresses = append(pod.Addresses, ip)
		}
		sort.Strings(pod.Addresses)
		result = append(result, pod)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].PodUID < result[j].PodUID })
	return result, nil
}
func DeploymentCurrentCohortComplete(deployment Deployment) bool {
	if deployment.Metadata.Generation <= 0 ||
		deployment.Status.ObservedGeneration < deployment.Metadata.Generation ||
		deployment.Spec.Replicas == nil {
		return false
	}
	desired := *deployment.Spec.Replicas
	return desired > 0 &&
		deployment.Status.Replicas <= desired &&
		deployment.Status.UpdatedReplicas == desired &&
		deployment.Status.ReadyReplicas >= desired &&
		deployment.Status.AvailableReplicas >= desired
}

func minObservedReplicaCount(values ...int) int {
	if len(values) == 0 {
		return 0
	}
	minimum := values[0]
	for _, value := range values[1:] {
		if value < minimum {
			minimum = value
		}
	}
	if minimum < 0 {
		return 0
	}
	return minimum
}

func firstDeploymentContainerImage(deployment Deployment) string {
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if image := strings.TrimSpace(container.Image); image != "" {
			return image
		}
	}
	return ""
}
