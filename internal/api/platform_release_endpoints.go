package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"net/url"
	"sort"

	"fugue/internal/model"
	"fugue/internal/runtime"

	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/util/validation"
)

type kubeOwnerReferenceEvidence struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
	Controller *bool  `json:"controller"`
}

type kubeReleaseAnnotations struct {
	ReleaseKey string `json:"fugue.pro/release-key"`
}

type kubeReleaseEndpointEvidence struct {
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

type kubeReleaseReplicaSetEvidence struct {
	kubeMetadataEvidence
	Spec struct {
		Template struct {
			kubeMetadataEvidence
		} `json:"template"`
	} `json:"spec"`
}

// Decode only readiness and identity fields; executable settings and secrets
// from the Pod payload are neither retained nor included in compiler facts.
type kubeReleasePodEvidence struct {
	kubeMetadataEvidence
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

type releaseEndpointPodIdentity struct {
	PodName        string   `json:"pod_name"`
	PodUID         string   `json:"pod_uid"`
	ReplicaSetName string   `json:"replica_set_name"`
	ReplicaSetUID  string   `json:"replica_set_uid"`
	ReleaseKey     string   `json:"release_key"`
	Addresses      []string `json:"addresses"`
}

func (c *managedAppStatusClient) captureReleaseEndpointOwners(ctx context.Context, snapshot *managedAppKubeSnapshot) error {
	pods, err := listTypedKubeResources[kubeReleasePodEvidence](ctx, c, "/api/v1/pods")
	if err != nil {
		return err
	}
	byPod := make(map[string]kubeReleasePodEvidence, len(pods))
	for _, pod := range pods {
		key := kubeNamespacedKey(pod.Metadata.Namespace, pod.Metadata.Name)
		if _, exists := byPod[key]; exists || pod.Metadata.Namespace == "" || pod.Metadata.Name == "" {
			return fmt.Errorf("ambiguous Pod inventory")
		}
		byPod[key] = pod
	}
	// Only ReplicaSets named by observed endpoint Pods can participate in the
	// ownership proof. Historical ReplicaSets are not part of that authority.
	// Keep negative endpoints too: the evaluator validates their ownership before
	// deciding readiness and must not silently drop an invalid member.
	type reference struct{ namespace, name string }
	references := map[string]reference{}
	for _, slices := range snapshot.releaseEndpointSlices {
		for _, slice := range slices {
			for _, endpoint := range slice.Endpoints {
				target := endpoint.TargetRef
				pod, found := byPod[kubeNamespacedKey(target.Namespace, target.Name)]
				if !found || target.Kind != "Pod" || target.APIVersion != "" && target.APIVersion != "v1" || target.Namespace != slice.Metadata.Namespace || target.UID == "" || pod.Metadata.UID != target.UID {
					continue // The unchanged evaluator rejects this endpoint.
				}
				owner, valid := singleKubeControllerOwner(pod.Metadata.OwnerReferences, "apps/v1", "ReplicaSet")
				if !valid {
					continue
				}
				if len(validation.IsDNS1123Label(pod.Metadata.Namespace)) != 0 || len(validation.IsDNS1123Subdomain(owner.Name)) != 0 {
					return fmt.Errorf("invalid ReplicaSet owner reference")
				}
				references[kubeNamespacedKey(pod.Metadata.Namespace, owner.Name)] = reference{pod.Metadata.Namespace, owner.Name}
			}
		}
	}
	keys := make([]string, 0, len(references))
	for key := range references {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	replicas := make([]kubeReleaseReplicaSetEvidence, len(keys))
	group, readCtx := errgroup.WithContext(ctx)
	group.SetLimit(4)
	for index, key := range keys {
		ref := references[key]
		group.Go(func() error {
			if err := readCtx.Err(); err != nil {
				return err
			}
			path := "/apis/apps/v1/namespaces/" + url.PathEscape(ref.namespace) + "/replicasets/" + url.PathEscape(ref.name)
			var rs kubeReleaseReplicaSetEvidence
			if err := c.doJSON(readCtx, path, &rs); err != nil {
				var status *kubeStatusError
				if errors.As(err, &status) && status.StatusCode == http.StatusNotFound {
					return nil // Missing owner remains absent and cannot make a release ready.
				}
				return err
			}
			if rs.Metadata.Namespace != ref.namespace || rs.Metadata.Name != ref.name {
				return fmt.Errorf("ReplicaSet response identity differs from request")
			}
			replicas[index] = rs
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	byReplicaSet := make(map[string]kubeReleaseReplicaSetEvidence, len(replicas))
	for _, rs := range replicas {
		if rs.Metadata.Name != "" {
			byReplicaSet[kubeNamespacedKey(rs.Metadata.Namespace, rs.Metadata.Name)] = rs
		}
	}
	// No partially assembled ownership evidence escapes on a failed read. UID,
	// labels, release key, image and controller checks remain in the evaluator.
	snapshot.releasePods, snapshot.releaseReplicaSets = byPod, byReplicaSet
	return nil
}

func singleKubeControllerOwner(refs []kubeOwnerReferenceEvidence, apiVersion, kind string) (kubeOwnerReferenceEvidence, bool) {
	var result kubeOwnerReferenceEvidence
	count := 0
	for _, ref := range refs {
		if ref.Controller != nil && *ref.Controller {
			count++
			result = ref
		}
	}
	return result, count == 1 && result.APIVersion == apiVersion && result.Kind == kind && result.Name != "" && result.UID != ""
}

func (s *Server) observeReleaseEndpointPods(app model.App, release model.AppRelease, deployment kubeDeploymentRuntimeEvidence, service kubeServiceRuntimeEvidence, snapshot managedAppKubeSnapshot, owner func(map[string]string) bool) ([]releaseEndpointPodIdentity, error) {
	ns := runtime.NamespaceForTenant(app.TenantID)
	slices := snapshot.releaseEndpointSlices[kubeNamespacedKey(ns, service.Metadata.Name)]
	key := deployment.Spec.Template.Metadata.Annotations.ReleaseKey
	if !snapshot.endpointSlicesAvailable || len(slices) == 0 || snapshot.releasePods == nil || snapshot.releaseReplicaSets == nil || key == "" {
		return nil, fmt.Errorf("release endpoint ownership evidence unavailable")
	}
	ready := map[string]releaseEndpointPodIdentity{}
	addresses := map[string]map[string]struct{}{}
	addressOwner := map[netip.Addr]string{}
	for _, slice := range slices {
		ref, valid := singleKubeControllerOwner(slice.Metadata.OwnerReferences, "v1", "Service")
		if !valid || ref.Name != service.Metadata.Name || ref.UID != service.Metadata.UID || slice.Metadata.Namespace != ns || slice.Metadata.UID == "" || slice.Metadata.DeletionTimestamp != "" {
			return nil, fmt.Errorf("release EndpointSlice Service ownership differs")
		}
		for _, endpoint := range slice.Endpoints {
			target := endpoint.TargetRef
			pod, found := snapshot.releasePods[kubeNamespacedKey(ns, target.Name)]
			if !found || target.Kind != "Pod" || target.APIVersion != "" && target.APIVersion != "v1" || target.Namespace != ns || target.UID == "" || target.UID != pod.Metadata.UID || pod.Metadata.Namespace != ns || !owner(pod.Metadata.Labels) || len(endpoint.Addresses) == 0 {
				return nil, fmt.Errorf("release endpoint Pod identity differs")
			}
			for label, value := range service.Spec.Selector {
				if pod.Metadata.Labels[label] != value {
					return nil, fmt.Errorf("release endpoint Pod does not match Service selector")
				}
			}
			rsRef, valid := singleKubeControllerOwner(pod.Metadata.OwnerReferences, "apps/v1", "ReplicaSet")
			rs, found := snapshot.releaseReplicaSets[kubeNamespacedKey(ns, rsRef.Name)]
			if !valid || !found || rsRef.UID != rs.Metadata.UID || rs.Metadata.Namespace != ns || rs.Metadata.DeletionTimestamp != "" || !owner(rs.Metadata.Labels) || !owner(rs.Spec.Template.Metadata.Labels) {
				return nil, fmt.Errorf("release endpoint ReplicaSet identity differs")
			}
			depRef, valid := singleKubeControllerOwner(rs.Metadata.OwnerReferences, "apps/v1", "Deployment")
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
			if endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating || endpoint.Conditions.Serving != nil && !*endpoint.Conditions.Serving || pod.Metadata.DeletionTimestamp != "" || pod.Status.Phase != "Running" || !podReady || len(pod.Spec.Containers) == 0 || !s.observedRuntimeImageRefsEquivalent(app, pod.Spec.Containers[0].Image, release.ResolvedImageRef) {
				return nil, fmt.Errorf("release endpoint Pod is not currently ready")
			}
			ready[target.UID] = releaseEndpointPodIdentity{PodName: target.Name, PodUID: target.UID, ReplicaSetName: rsRef.Name, ReplicaSetUID: rsRef.UID, ReleaseKey: key}
			if addresses[target.UID] == nil {
				addresses[target.UID] = map[string]struct{}{}
			}
			for _, address := range endpoint.Addresses {
				ip, _ := netip.ParseAddr(address)
				addresses[target.UID][ip.Unmap().String()] = struct{}{}
			}
		}
	}
	result := make([]releaseEndpointPodIdentity, 0, len(ready))
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
